use std::fmt::Write as _;
use std::sync::{Arc, Mutex};

use anyhow::{anyhow, Result};
use futures::future::join_all;
use log::debug;
use tokio::sync::OnceCell;

use crate::airflow::client::FlowrsClient;
use crate::airflow::model::common::{DagId, DagRunId, Log, LogSource, TaskId, TaskTryGantt};
use crate::app::model::popup::error::ErrorPopup;
use crate::app::state::App;

/// Handle fetching task logs for all attempts of a task instance.
///
/// `env_name` identifies which environment initiated this request, ensuring
/// results are written to the correct environment even if the active one changes.
pub async fn handle_update_task_logs(
    app: &Arc<Mutex<App>>,
    client: &Arc<FlowrsClient>,
    dag_id: &DagId,
    dag_run_id: &DagRunId,
    task_id: &TaskId,
    task_try: u32,
    env_name: &str,
) {
    debug!("Getting logs for task: {task_id}, try number {task_try}");

    let has_loki = client.has_loki();
    let (force_loki, cached) = {
        let mut app_lock = app.lock().unwrap();
        app_lock.logs.loki_available = has_loki;
        let cached = app_lock
            .environment_state
            .environments
            .get(env_name)
            .and_then(|env| {
                env.task_logs
                    .get(&(dag_id.clone(), dag_run_id.clone(), task_id.clone()))
            })
            .cloned()
            .unwrap_or_default();
        (app_lock.logs.force_loki && has_loki, cached)
    };

    // Tries older than the current one are immutable once a newer try exists,
    // and so is a Loki log whose try ended a while ago, so those are reused.
    let tries: OnceCell<Result<Vec<TaskTryGantt>, String>> = OnceCell::new();
    let logs = join_all((1..=task_try).map(|i| {
        let cached = cached
            .get(i as usize - 1)
            .filter(|log| is_reusable(log, i == task_try, force_loki))
            .cloned();
        let tries = &tries;
        async move {
            if let Some(log) = cached {
                return Ok(log);
            }
            let ids = TryIds {
                dag_id,
                dag_run_id,
                task_id,
                try_number: i,
            };
            fetch_try(client, &ids, force_loki, tries).await
        }
    }))
    .await;

    // Keep the tries in order: stop at the first one that failed, so later
    // tries do not shift into its tab.
    let mut collected_logs: Vec<Log> = Vec::new();
    let mut errors = Vec::new();
    for log in logs {
        match log {
            Ok(log) => {
                debug!("Got log: {log:?}");
                collected_logs.push(log);
            }
            Err(e) => {
                debug!("Error getting logs: {e}");
                errors.push(e.to_string());
                break;
            }
        }
    }

    let mut app = app.lock().unwrap();

    if !errors.is_empty() {
        app.logs.error_popup = Some(ErrorPopup::from_strings(errors));
    }

    // Store logs in the originating environment, not the active one
    if !collected_logs.is_empty() {
        if let Some(env) = app.environment_state.environments.get_mut(env_name) {
            env.replace_task_logs(dag_id, dag_run_id, task_id, collected_logs);
        }
    }

    // Only sync panel data if this environment is still active
    if app.environment_state.active_environment.as_deref() == Some(env_name) {
        app.sync_panel(&crate::app::state::Panel::Logs);
    }
}

struct TryIds<'a> {
    dag_id: &'a DagId,
    dag_run_id: &'a DagRunId,
    task_id: &'a TaskId,
    try_number: u32,
}

/// Whether a cached log can be shown again without asking the server.
fn is_reusable(log: &Log, is_latest_try: bool, force_loki: bool) -> bool {
    match log.source {
        // A forced Loki log only stands in while Loki is forced, and vice versa.
        LogSource::Loki { forced, complete } => forced == force_loki && complete,
        LogSource::Airflow | LogSource::AirflowUnavailable => !force_loki && !is_latest_try,
    }
}

/// Fetch one try from Airflow, falling back to Loki when Airflow cannot serve
/// it; or straight from Loki when the user forced it.
async fn fetch_try(
    client: &FlowrsClient,
    ids: &TryIds<'_>,
    force_loki: bool,
    tries: &OnceCell<Result<Vec<TaskTryGantt>, String>>,
) -> Result<Log> {
    if force_loki {
        return fetch_from_loki(client, ids, true, tries).await;
    }
    let airflow = client
        .get_task_logs(ids.dag_id, ids.dag_run_id, ids.task_id, ids.try_number)
        .await;
    if !client.has_loki() {
        return airflow;
    }
    match airflow {
        Ok(log) if log.source != LogSource::AirflowUnavailable => Ok(log),
        Ok(mut unavailable) => match fetch_from_loki(client, ids, false, tries).await {
            Ok(log) => Ok(log),
            Err(e) => {
                let _ = write!(unavailable.content, "\n── Loki fallback failed: {e:#} ──\n");
                Ok(unavailable)
            }
        },
        Err(airflow_error) => {
            fetch_from_loki(client, ids, false, tries)
                .await
                .map_err(|loki_error| {
                    anyhow!("{airflow_error:#}; Loki fallback also failed: {loki_error:#}")
                })
        }
    }
}

async fn fetch_from_loki(
    client: &FlowrsClient,
    ids: &TryIds<'_>,
    forced: bool,
    tries: &OnceCell<Result<Vec<TaskTryGantt>, String>>,
) -> Result<Log> {
    // The Loki window comes from the try's start and end, which only the
    // tries endpoint has for tries older than the current one.
    let tries = tries
        .get_or_init(|| async {
            client
                .list_task_instance_tries(ids.dag_id, ids.dag_run_id, ids.task_id)
                .await
                .map_err(|e| format!("{e:#}"))
        })
        .await
        .as_ref()
        .map_err(|e| anyhow!("could not look up the try's start and end: {e}"))?;
    let task_try = tries
        .iter()
        .find(|t| t.try_number == ids.try_number)
        .ok_or_else(|| anyhow!("Airflow does not list try {}", ids.try_number))?;
    client
        .get_task_logs_from_loki(ids.dag_id, ids.task_id, task_try, forced)
        .await
}

#[cfg(test)]
mod tests {
    use super::*;

    fn log(source: LogSource) -> Log {
        Log {
            continuation_token: None,
            content: String::new(),
            source,
        }
    }

    #[test]
    fn older_airflow_tries_are_reused_unless_loki_is_forced() {
        assert!(is_reusable(&log(LogSource::Airflow), false, false));
        assert!(!is_reusable(&log(LogSource::Airflow), true, false));
        assert!(!is_reusable(&log(LogSource::Airflow), false, true));
    }

    #[test]
    fn complete_loki_logs_are_reused_in_the_matching_mode() {
        let fallback = log(LogSource::Loki {
            forced: false,
            complete: true,
        });
        assert!(is_reusable(&fallback, true, false));
        assert!(!is_reusable(&fallback, true, true));

        let forced = log(LogSource::Loki {
            forced: true,
            complete: true,
        });
        assert!(is_reusable(&forced, true, true));
        assert!(!is_reusable(&forced, false, false));

        let incomplete = log(LogSource::Loki {
            forced: false,
            complete: false,
        });
        assert!(!is_reusable(&incomplete, false, false));
    }
}
