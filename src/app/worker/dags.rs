use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use log::warn;

use crate::airflow::client::FlowrsClient;
use crate::airflow::model::common::{DagId, DagRunState, DagStatsResponse};
use crate::app::model::dagruns::popup::trigger::TriggerDagRunPopUp;
use crate::app::model::dagruns::popup::DagRunPopUp;
use crate::app::model::dagruns::DagCodeView;
use crate::app::model::dags::popup::DagPopUp;
use crate::app::state::{App, Panel};

/// Handle updating DAGs and their statistics from the Airflow server.
/// On cold start (empty cache), fetches DAGs first then stats sequentially so
/// stats use fresh IDs. On warm cache, fetches both concurrently using cached
/// DAG IDs; new DAGs pick up stats on the next refresh.
///
/// `env_name` identifies which environment initiated this request, ensuring
/// results are written to the correct environment even if the active one changes.
pub async fn handle_update_dags_and_stats(
    app: &Arc<Mutex<App>>,
    client: &Arc<FlowrsClient>,
    env_name: &str,
) {
    // Snapshot cached DAG IDs from the originating environment for the stats request
    let cached_dag_ids: Vec<DagId> = {
        let app_lock = app.lock().unwrap();
        app_lock
            .environment_state
            .environments
            .get(env_name)
            .map(|env| env.dags.iter().map(|dag| dag.dag_id.clone()).collect())
            .unwrap_or_default()
    };

    if cached_dag_ids.is_empty() {
        // Cold start: fetch DAGs first, then stats with fresh IDs
        let dag_list_result = client.list_dags().await;

        let dag_ids: Vec<DagId> = {
            let mut app = app.lock().unwrap();
            match dag_list_result {
                Ok(dag_list) => {
                    let ids: Vec<DagId> = dag_list.dags.iter().map(|d| d.dag_id.clone()).collect();
                    if let Some(env) = app.environment_state.environments.get_mut(env_name) {
                        env.replace_dags(dag_list.dags);
                    }
                    ids
                }
                Err(e) => {
                    app.dags.popup.show_error(vec![e.to_string()]);
                    vec![]
                }
            }
        };

        if !dag_ids.is_empty() {
            let summaries = fetch_dag_summaries(client, &dag_ids).await;
            let mut app = app.lock().unwrap();
            apply_dag_summaries(&mut app, env_name, summaries);
        }
    } else {
        // Warm cache: fetch DAG list and stats concurrently using cached IDs
        let (dag_list_result, summaries) = tokio::join!(
            client.list_dags(),
            fetch_dag_summaries(client, &cached_dag_ids)
        );

        let mut app = app.lock().unwrap();

        match dag_list_result {
            Ok(dag_list) => {
                if let Some(env) = app.environment_state.environments.get_mut(env_name) {
                    env.replace_dags(dag_list.dags);
                }
            }
            Err(e) => {
                app.dags.popup.show_error(vec![e.to_string()]);
            }
        }

        apply_dag_summaries(&mut app, env_name, summaries);
    }

    // Only sync panel data if this environment is still the active one,
    // otherwise we'd overwrite the UI with stale data from a different server
    let mut app = app.lock().unwrap();
    if app.environment_state.active_environment.as_deref() == Some(env_name) {
        app.sync_panel(&crate::app::state::Panel::Dag);
    }
}

/// Per-DAG summary data fetched alongside the DAG list: run-state counts and
/// the state of the most recent run.
type DagSummaries = (
    anyhow::Result<DagStatsResponse>,
    anyhow::Result<HashMap<DagId, DagRunState>>,
);

/// Fetch stats and latest-run states for `dag_ids` concurrently.
async fn fetch_dag_summaries(client: &FlowrsClient, dag_ids: &[DagId]) -> DagSummaries {
    let refs: Vec<&str> = dag_ids.iter().map(AsRef::as_ref).collect();
    tokio::join!(
        client.get_dag_stats(refs),
        client.list_latest_dagrun_states(dag_ids)
    )
}

/// Write fetched summaries into the environment; failures are logged, not shown.
fn apply_dag_summaries(app: &mut App, env_name: &str, (stats_result, latest_result): DagSummaries) {
    let Some(env) = app.environment_state.environments.get_mut(env_name) else {
        return;
    };
    match stats_result {
        Ok(dag_stats) => {
            for dag_stats in dag_stats.dags {
                env.update_dag_stats(&DagId::from(dag_stats.dag_id.clone()), dag_stats.stats);
            }
        }
        Err(e) => log::error!("Failed to fetch dag stats: {e}"),
    }
    match latest_result {
        Ok(states) => env.replace_latest_run_states(states),
        Err(e) => log::error!("Failed to fetch latest dag run states: {e}"),
    }
}

/// Handle toggling the paused state of a DAG.
pub async fn handle_toggle_dag(
    app: &Arc<Mutex<App>>,
    client: &Arc<FlowrsClient>,
    dag_id: &DagId,
    is_paused: bool,
) {
    let dag = client.toggle_dag(dag_id, is_paused).await;
    if let Err(e) = dag {
        let mut app = app.lock().unwrap();
        app.dags.popup.show_error(vec![e.to_string()]);
    }
}

/// Handle fetching the DAG source code.
pub async fn handle_get_dag_code(
    app: &Arc<Mutex<App>>,
    client: &Arc<FlowrsClient>,
    dag_id: &DagId,
) {
    let current_dag = {
        let app_lock = app.lock().unwrap();
        app_lock
            .environment_state
            .get_active_environment()
            .and_then(|env| env.dags.iter().find(|d| d.dag_id == *dag_id).cloned())
    };

    if let Some(current_dag) = current_dag {
        let dag_code = client.get_dag_code(&current_dag).await;
        let mut app = app.lock().unwrap();
        match dag_code {
            Ok(dag_code) => {
                let view = Some(DagCodeView::new(&dag_code));
                match app.active_panel {
                    Panel::Dag => app.dags.dag_code = view,
                    Panel::DAGRun => app.dagruns.dag_code = view,
                    _ => {}
                }
            }
            Err(e) => match app.active_panel {
                Panel::DAGRun => app.dagruns.popup.show_error(vec![e.to_string()]),
                _ => app.dags.popup.show_error(vec![e.to_string()]),
            },
        }
    } else {
        let mut app = app.lock().unwrap();
        let error = vec!["DAG not found".to_string()];
        match app.active_panel {
            Panel::DAGRun => app.dagruns.popup.show_error(error),
            _ => app.dags.popup.show_error(error),
        }
    }
}

/// Fetch a fresh copy of a DAG's param schema, then open the trigger popup
/// with it on the panel that asked. The schema is re-fetched on every popup
/// open so changes on the Airflow side show up; the cached copy is only the
/// fallback when the fetch fails.
pub async fn handle_get_dag_params(
    app: &Arc<Mutex<App>>,
    client: &Arc<FlowrsClient>,
    dag_id: &DagId,
    env_name: &str,
) {
    match client.get_dag_params(dag_id).await {
        Ok(params) => {
            let mut app = app.lock().unwrap();
            if let Some(env) = app.environment_state.environments.get_mut(env_name) {
                match params {
                    Some(params) => env.update_dag_params(dag_id, params),
                    // The DAG no longer has params: drop any stale schema.
                    None => {
                        env.dag_params.remove(dag_id);
                    }
                }
            }
        }
        Err(e) => warn!("Failed to fetch dag params for {dag_id}: {e}"),
    }

    let mut app = app.lock().unwrap();
    let params = app
        .environment_state
        .environments
        .get(env_name)
        .and_then(|env| env.dag_params.get(dag_id).cloned());
    let popup = TriggerDagRunPopUp::new(dag_id.clone(), params.as_deref());
    match app.active_panel {
        Panel::DAGRun => app.dagruns.popup.show_custom(DagRunPopUp::Trigger(popup)),
        _ => app.dags.popup.show_custom(DagPopUp::Trigger(popup)),
    }
}
