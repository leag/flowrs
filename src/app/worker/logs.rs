use std::sync::{Arc, Mutex};

use futures::future::join_all;
use log::debug;

use crate::airflow::client::FlowrsClient;
use crate::airflow::model::common::{DagId, DagRunId, Log, TaskId};
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

    // Tries older than the current one are immutable once a newer try exists,
    // so reuse what is cached and only fetch missing tries plus the current one.
    let mut collected_logs: Vec<Log> = {
        let app_lock = app.lock().unwrap();
        app_lock
            .environment_state
            .environments
            .get(env_name)
            .and_then(|env| {
                env.task_logs
                    .get(&(dag_id.clone(), dag_run_id.clone(), task_id.clone()))
            })
            .map(|cached| {
                let keep = cached.len().min(task_try.saturating_sub(1) as usize);
                cached[..keep].to_vec()
            })
            .unwrap_or_default()
    };
    let first_try = u32::try_from(collected_logs.len()).unwrap_or(u32::MAX) + 1;

    let logs = join_all(
        (first_try..=task_try).map(|i| client.get_task_logs(dag_id, dag_run_id, task_id, i)),
    )
    .await;

    // Collect logs and errors outside the lock
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
