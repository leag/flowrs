use std::collections::{HashMap, HashSet};

use anyhow::Result;

use crate::airflow::client::convert_v1::v1_dagrun_collection_to_list;
use crate::airflow::client::convert_v2::v2_dagrun_list_to_list;
use crate::airflow::client::FlowrsClient;
use crate::airflow::model::common::{DagId, DagRunList, DagRunState};

/// Page size for the recent DAG runs query. Airflow caps page size at 100 by default.
const RECENT_RUNS_PAGE_SIZE: usize = 100;
/// Upper bound on pages fetched when looking up the latest run per DAG.
const RECENT_RUNS_MAX_PAGES: usize = 5;

impl FlowrsClient {
    pub async fn list_dagruns(&self, dag_id: &str) -> Result<DagRunList> {
        match self {
            Self::V1(client) => {
                let response = client.fetch_dagruns(dag_id).await?;
                Ok(v1_dagrun_collection_to_list(response))
            }
            Self::V2(client) => {
                let response = client.fetch_dagruns(dag_id).await?;
                Ok(v2_dagrun_list_to_list(response))
            }
        }
    }

    async fn list_recent_dagruns(&self, limit: usize, offset: usize) -> Result<DagRunList> {
        match self {
            Self::V1(client) => {
                let response = client.fetch_recent_dagruns(limit, offset).await?;
                Ok(v1_dagrun_collection_to_list(response))
            }
            Self::V2(client) => {
                let response = client.fetch_recent_dagruns(limit, offset).await?;
                Ok(v2_dagrun_list_to_list(response))
            }
        }
    }

    /// Resolve the state of the most recent run for each of `dag_ids`.
    /// Walks the newest-first list of runs across all DAGs, stopping once every
    /// requested DAG has been seen or the page budget is exhausted. DAGs without
    /// any run are absent from the result.
    pub async fn list_latest_dagrun_states(
        &self,
        dag_ids: &[DagId],
    ) -> Result<HashMap<DagId, DagRunState>> {
        let mut pending: HashSet<&DagId> = dag_ids.iter().collect();
        let mut latest = HashMap::new();
        let mut offset = 0;
        for _ in 0..RECENT_RUNS_MAX_PAGES {
            if pending.is_empty() {
                break;
            }
            let page = self
                .list_recent_dagruns(RECENT_RUNS_PAGE_SIZE, offset)
                .await?;
            let fetched = page.dag_runs.len();
            for run in page.dag_runs {
                if pending.remove(&run.dag_id) {
                    latest.insert(run.dag_id, run.state);
                }
            }
            if fetched < RECENT_RUNS_PAGE_SIZE {
                break;
            }
            offset += fetched;
        }
        Ok(latest)
    }

    pub async fn mark_dag_run(&self, dag_id: &str, dag_run_id: &str, status: &str) -> Result<()> {
        match self {
            Self::V1(client) => client.patch_dag_run(dag_id, dag_run_id, status).await?,
            Self::V2(client) => client.patch_dag_run(dag_id, dag_run_id, status).await?,
        }
        Ok(())
    }

    pub async fn clear_dagrun(&self, dag_id: &str, dag_run_id: &str) -> Result<()> {
        match self {
            Self::V1(client) => client.post_clear_dagrun(dag_id, dag_run_id).await?,
            Self::V2(client) => client.post_clear_dagrun(dag_id, dag_run_id).await?,
        }
        Ok(())
    }

    pub async fn trigger_dag_run(
        &self,
        dag_id: &str,
        logical_date: Option<&str>,
        conf: Option<serde_json::Value>,
    ) -> Result<()> {
        match self {
            Self::V1(client) => {
                client
                    .post_trigger_dag_run(dag_id, logical_date, conf)
                    .await?;
            }
            Self::V2(client) => {
                client
                    .post_trigger_dag_run(dag_id, logical_date, conf)
                    .await?;
            }
        }
        Ok(())
    }
}
