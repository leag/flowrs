use anyhow::Result;
use futures::future::try_join_all;

use crate::airflow::client::convert_v1::v1_dagstats_to_response;
use crate::airflow::client::convert_v2::v2_dagstats_to_response;
use crate::airflow::client::FlowrsClient;
use crate::airflow::model::common::DagStatsResponse;

/// DAG ids per `dagStats` request. Ids travel in the query string, so a large
/// deployment would otherwise exceed the webserver's request-line limit.
const STATS_CHUNK_SIZE: usize = 50;

impl FlowrsClient {
    /// Fetch run-state counts for `dag_ids`, chunking the request so the query
    /// string stays within webserver limits. Chunks are requested concurrently.
    pub async fn get_dag_stats(&self, dag_ids: Vec<&str>) -> Result<DagStatsResponse> {
        let chunks = try_join_all(
            dag_ids
                .chunks(STATS_CHUNK_SIZE)
                .map(|chunk| async move { self.get_dag_stats_chunk(chunk.to_vec()).await }),
        )
        .await?;
        let mut merged = DagStatsResponse {
            dags: Vec::with_capacity(dag_ids.len()),
            total_entries: 0,
        };
        for chunk in chunks {
            merged.dags.extend(chunk.dags);
            merged.total_entries += chunk.total_entries;
        }
        Ok(merged)
    }

    async fn get_dag_stats_chunk(&self, dag_ids: Vec<&str>) -> Result<DagStatsResponse> {
        match self {
            Self::V1(client) => {
                let response = client.fetch_dag_stats(dag_ids).await?;
                Ok(v1_dagstats_to_response(response))
            }
            Self::V2(client) => {
                let response = client.fetch_dag_stats(dag_ids).await?;
                Ok(v2_dagstats_to_response(response))
            }
        }
    }
}
