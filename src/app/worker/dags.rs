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

/// Handle updating DAGs and their summaries from the Airflow server.
///
/// The DAG list, the run-state stats and the latest-run states are three
/// independent requests. Each one is written to the environment and pushed to
/// the panel as soon as it lands, so a slow request (the latest-run lookup can
/// page several times) never delays the list itself.
///
/// On cold start (empty cache) the DAG list is fetched first so the summaries
/// use fresh IDs; on a warm cache all three run concurrently using cached IDs,
/// and new DAGs pick up their summaries on the next refresh.
///
/// `env_name` identifies which environment initiated this request, ensuring
/// results are written to the correct environment even if the active one changes.
pub async fn handle_update_dags_and_stats(
    app: &Arc<Mutex<App>>,
    client: &Arc<FlowrsClient>,
    env_name: &str,
) {
    // Snapshot cached DAG IDs from the originating environment for the summary requests
    let cached_dag_ids: Vec<DagId> = {
        let app_lock = app.lock().unwrap();
        app_lock
            .environment_state
            .environments
            .get(env_name)
            .map(|env| env.dags.iter().map(|dag| dag.dag_id.clone()).collect())
            .unwrap_or_default()
    };

    let (dag_ids, refresh_list) = if cached_dag_ids.is_empty() {
        let ids = fetch_and_apply_dag_list(app, client, env_name).await;
        (ids, false)
    } else {
        (cached_dag_ids, true)
    };

    if dag_ids.is_empty() {
        return;
    }

    let refs: Vec<&str> = dag_ids.iter().map(AsRef::as_ref).collect();
    tokio::join!(
        async {
            if refresh_list {
                fetch_and_apply_dag_list(app, client, env_name).await;
            }
        },
        async {
            let result = client.get_dag_stats(refs).await;
            let mut app = app.lock().unwrap();
            apply_dag_stats(&mut app, env_name, result);
            sync_dag_panel(&mut app, env_name);
        },
        async {
            let result = client.list_latest_dagrun_states(&dag_ids).await;
            let mut app = app.lock().unwrap();
            apply_latest_run_states(&mut app, env_name, result);
            sync_dag_panel(&mut app, env_name);
        },
    );
}

/// Fetch the DAG list, store it and refresh the panel. Returns the fetched IDs
/// (empty on failure, after showing the error in the panel).
async fn fetch_and_apply_dag_list(
    app: &Arc<Mutex<App>>,
    client: &FlowrsClient,
    env_name: &str,
) -> Vec<DagId> {
    let result = client.list_dags().await;
    let mut app = app.lock().unwrap();
    let ids = match result {
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
    };
    sync_dag_panel(&mut app, env_name);
    ids
}

fn apply_dag_stats(app: &mut App, env_name: &str, result: anyhow::Result<DagStatsResponse>) {
    match result {
        Ok(dag_stats) => {
            if let Some(env) = app.environment_state.environments.get_mut(env_name) {
                for dag_stats in dag_stats.dags {
                    env.update_dag_stats(&DagId::from(dag_stats.dag_id.clone()), dag_stats.stats);
                }
            }
        }
        Err(e) => log::error!("Failed to fetch dag stats: {e}"),
    }
}

fn apply_latest_run_states(
    app: &mut App,
    env_name: &str,
    result: anyhow::Result<HashMap<DagId, DagRunState>>,
) {
    match result {
        Ok(states) => {
            if let Some(env) = app.environment_state.environments.get_mut(env_name) {
                env.replace_latest_run_states(states);
            }
        }
        Err(e) => log::error!("Failed to fetch latest dag run states: {e}"),
    }
}

/// Push environment data to the DAG panel, but only if this environment is
/// still the active one; otherwise we'd overwrite the UI with stale data from
/// a different server.
fn sync_dag_panel(app: &mut App, env_name: &str) {
    if app.environment_state.active_environment.as_deref() == Some(env_name) {
        app.sync_panel(&Panel::Dag);
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
