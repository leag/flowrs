use log::debug;
use reqwest::Method;

use super::model;
use super::V2Client;
use crate::client::read_json;
use crate::error::Result;

impl V2Client {
    pub async fn fetch_dagruns(&self, dag_id: &str) -> Result<model::dagrun::DagRunList> {
        let request = self
            .base_api(Method::GET, &format!("dags/{dag_id}/dagRuns"))
            .await?
            .query(&[("order_by", "-run_after"), ("limit", "50")]);
        let response = self.execute(request).await?;
        read_json(response, "DAG runs response").await
    }

    /// Fetch one page of runs for the given DAGs, newest first, via the
    /// batch list endpoint so the page only contains runs of those DAGs.
    pub async fn fetch_dagruns_batch(
        &self,
        dag_ids: &[&str],
        page_limit: usize,
        page_offset: usize,
    ) -> Result<model::dagrun::DagRunList> {
        let request = self
            .base_api(Method::POST, "dags/~/dagRuns/list")
            .await?
            .json(&serde_json::json!({
                "dag_ids": dag_ids,
                "order_by": "-run_after",
                "page_limit": page_limit,
                "page_offset": page_offset,
            }));
        let response = self.execute(request).await?;
        read_json(response, "DAG runs batch response").await
    }

    /// Fetch only the newest run of a single DAG.
    pub async fn fetch_latest_dagrun(&self, dag_id: &str) -> Result<model::dagrun::DagRunList> {
        let request = self
            .base_api(Method::GET, &format!("dags/{dag_id}/dagRuns"))
            .await?
            .query(&[("order_by", "-run_after"), ("limit", "1")]);
        let response = self.execute(request).await?;
        read_json(response, "latest DAG run response").await
    }

    pub async fn patch_dag_run(&self, dag_id: &str, dag_run_id: &str, status: &str) -> Result<()> {
        let request = self
            .base_api(
                Method::PATCH,
                &format!("dags/{dag_id}/dagRuns/{dag_run_id}"),
            )
            .await?
            .json(&serde_json::json!({"state": status}));
        self.execute(request).await?;
        Ok(())
    }

    pub async fn post_clear_dagrun(&self, dag_id: &str, dag_run_id: &str) -> Result<()> {
        let request = self
            .base_api(
                Method::POST,
                &format!("dags/{dag_id}/dagRuns/{dag_run_id}/clear"),
            )
            .await?
            .json(&serde_json::json!({"dry_run": false}));
        self.execute(request).await?;
        Ok(())
    }

    pub async fn post_trigger_dag_run(
        &self,
        dag_id: &str,
        logical_date: Option<&str>,
        conf: Option<serde_json::Value>,
    ) -> Result<()> {
        let mut body = serde_json::json!({"logical_date": logical_date});
        if let Some(conf) = conf {
            body["conf"] = conf;
        }

        let request = self
            .base_api(Method::POST, &format!("dags/{dag_id}/dagRuns"))
            .await?
            .json(&body);
        let resp = self.execute(request).await?;
        debug!("{resp:?}");
        Ok(())
    }
}
