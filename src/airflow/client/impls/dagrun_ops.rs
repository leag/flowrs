use std::collections::{HashMap, HashSet};

use anyhow::Result;
use futures::{stream, StreamExt};

use crate::airflow::client::convert_v1::v1_dagrun_collection_to_list;
use crate::airflow::client::convert_v2::v2_dagrun_list_to_list;
use crate::airflow::client::FlowrsClient;
use crate::airflow::model::common::{DagId, DagRunList, DagRunState};

/// Page size for the batch DAG runs query. Airflow caps page size at 100 by default.
const BATCH_RUNS_PAGE_SIZE: usize = 100;
/// Pages walked through the batch query before falling back to per-DAG lookups.
const BATCH_RUNS_MAX_PAGES: usize = 3;
/// Concurrency of the per-DAG fallback lookups.
const LATEST_RUN_CONCURRENCY: usize = 8;

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

    async fn list_dagruns_batch(
        &self,
        dag_ids: &[&str],
        page_limit: usize,
        page_offset: usize,
    ) -> Result<DagRunList> {
        match self {
            Self::V1(client) => {
                let response = client
                    .fetch_dagruns_batch(dag_ids, page_limit, page_offset)
                    .await?;
                Ok(v1_dagrun_collection_to_list(response))
            }
            Self::V2(client) => {
                let response = client
                    .fetch_dagruns_batch(dag_ids, page_limit, page_offset)
                    .await?;
                Ok(v2_dagrun_list_to_list(response))
            }
        }
    }

    async fn get_latest_dagrun_state(&self, dag_id: &DagId) -> Result<Option<DagRunState>> {
        let list = match self {
            Self::V1(client) => {
                v1_dagrun_collection_to_list(client.fetch_latest_dagrun(dag_id.as_ref()).await?)
            }
            Self::V2(client) => {
                v2_dagrun_list_to_list(client.fetch_latest_dagrun(dag_id.as_ref()).await?)
            }
        };
        Ok(list.dag_runs.into_iter().next().map(|run| run.state))
    }

    /// Resolve the state of the most recent run for each of `dag_ids`.
    ///
    /// Walks the newest-first batch list filtered to those DAGs, which resolves
    /// most of them in one page; any DAG still unresolved after the page budget
    /// gets a dedicated single-run lookup, so every requested DAG ends up in the
    /// result. DAGs without any run map to [`DagRunState::Unknown`].
    pub async fn list_latest_dagrun_states(
        &self,
        dag_ids: &[DagId],
    ) -> Result<HashMap<DagId, DagRunState>> {
        let mut pending: HashSet<&DagId> = dag_ids.iter().collect();
        let mut latest = HashMap::with_capacity(dag_ids.len());
        let refs: Vec<&str> = dag_ids.iter().map(AsRef::as_ref).collect();
        let mut offset = 0;
        for _ in 0..BATCH_RUNS_MAX_PAGES {
            if pending.is_empty() {
                break;
            }
            let page = self
                .list_dagruns_batch(&refs, BATCH_RUNS_PAGE_SIZE, offset)
                .await?;
            let fetched = page.dag_runs.len();
            for run in page.dag_runs {
                if pending.remove(&run.dag_id) {
                    latest.insert(run.dag_id, run.state);
                }
            }
            offset += fetched;
            let total = usize::try_from(page.total_entries).unwrap_or(usize::MAX);
            if fetched == 0 || offset >= total {
                break;
            }
        }

        // Anything left is either run-less or buried under busier DAGs' runs;
        // resolve each one directly so the answer is definitive.
        let fallback: Vec<(DagId, Result<Option<DagRunState>>)> =
            stream::iter(pending.into_iter().cloned())
                .map(|dag_id| async move {
                    let state = self.get_latest_dagrun_state(&dag_id).await;
                    (dag_id, state)
                })
                .buffer_unordered(LATEST_RUN_CONCURRENCY)
                .collect()
                .await;
        for (dag_id, state) in fallback {
            latest.insert(dag_id, state?.unwrap_or(DagRunState::Unknown));
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
