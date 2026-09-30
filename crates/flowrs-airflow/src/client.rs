pub mod auth;
pub mod base;
pub mod v1;
pub mod v2;

use reqwest::Response;
use serde::de::DeserializeOwned;

use crate::error::{AirflowError, Result};

pub use base::BaseClient;
pub use v1::V1Client;
pub use v2::V2Client;

/// Deserialize a response body, keeping a snippet of it in the error on failure.
///
/// The body is read as text first rather than using `Response::json`, because some
/// deployments return payloads that do not match the documented schema (older v2.x
/// versions omit `dag_display_name`, for instance) and the raw body is what makes
/// those failures diagnosable.
pub(crate) async fn read_json<T: DeserializeOwned>(response: Response, context: &str) -> Result<T> {
    let body = response.text().await?;
    serde_json::from_str(&body).map_err(|e| AirflowError::decode(context, &body, e))
}

/// Whether a plain-text log holds anything from the task itself.
///
/// When Airflow cannot reach the worker that ran a try (with the Kubernetes
/// executor: the pod is gone), the log it returns only reports where it looked:
/// `***`-prefixed notes, `::group::`/`::endgroup::` markers and nothing else.
pub fn text_has_task_output(content: &str) -> bool {
    content.lines().any(|line| !is_source_detail_line(line))
}

fn is_source_detail_line(line: &str) -> bool {
    let line = line.trim();
    line.is_empty()
        || line.starts_with("***")
        || line.starts_with("::group::")
        || line.starts_with("::endgroup::")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn source_details_alone_are_not_task_output() {
        let content = "::group::Log message source details\n*** Could not read served logs: timeout\n*** Reading from k8s pod logs failed\n::endgroup::\n";
        assert!(!text_has_task_output(content));
        assert!(!text_has_task_output(""));
        assert!(text_has_task_output(
            "*** Found local files\n[2026-09-30] INFO - hello"
        ));
    }
}
