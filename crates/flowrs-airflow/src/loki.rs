//! Reading task logs from Loki through Grafana's datasource proxy.
//!
//! Airflow serves a try's log from the worker that ran it. With the Kubernetes
//! executor that worker is a pod, and once the pod is gone (finished, evicted, or
//! lost to a spot interruption) Airflow can only report where it looked. If the
//! cluster ships pod output to Loki, the log is still there, keyed by the labels
//! the Kubernetes executor puts on the pod.

use std::collections::{BTreeMap, HashSet};
use std::fmt;
use std::fmt::Write as _;
use std::time::Duration;

use log::debug;
use reqwest::{Method, Url};
use serde::Deserialize;
use time::OffsetDateTime;

use crate::client::read_json;
use crate::config::GrafanaConfig;
use crate::error::{AirflowError, Result, SNIPPET_LEN};

/// Lines requested per `query_range` call. Airflow error events can embed a whole
/// pod manifest, so a single line may be 100 KB or more; keeping batches modest
/// bounds the size of one response.
const BATCH_SIZE: usize = 1000;

/// Identifies one try of a task instance, plus the time window it ran in.
#[derive(Debug, Clone)]
pub struct TaskLogQuery<'a> {
    pub dag_id: &'a str,
    pub task_id: &'a str,
    pub try_number: u32,
    pub start: OffsetDateTime,
    pub end: OffsetDateTime,
}

/// One log line and the labels of the stream it came from.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LokiEntry {
    pub timestamp_ns: i128,
    pub line: String,
    /// Index into [`LokiLogs::streams`].
    pub stream: usize,
}

/// The lines of a try, oldest first.
#[derive(Debug, Clone, Default)]
pub struct LokiLogs {
    pub entries: Vec<LokiEntry>,
    pub streams: Vec<BTreeMap<String, String>>,
    /// `true` when `max_lines` was reached before the window was exhausted.
    pub truncated: bool,
}

/// Client for the Loki datasource behind a Grafana instance.
pub struct LokiClient {
    http: reqwest::Client,
    query_url: Url,
    config: GrafanaConfig,
    batch_size: usize,
}

impl fmt::Debug for LokiClient {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("LokiClient")
            .field("query_url", &self.query_url)
            .field("config", &self.config)
            .finish_non_exhaustive()
    }
}

impl LokiClient {
    pub fn new(config: GrafanaConfig, timeout_secs: u64, insecure: bool) -> Result<Self> {
        let mut base = Url::parse(&config.url)
            .map_err(|e| AirflowError::invalid_url(config.url.clone(), e))?;
        if !base.path().ends_with('/') {
            let with_slash = format!("{}/", base.path());
            base.set_path(&with_slash);
        }
        let path = format!(
            "api/datasources/proxy/uid/{}/loki/api/v1/query_range",
            config.loki_datasource_uid
        );
        let query_url = base
            .join(&path)
            .map_err(|e| AirflowError::invalid_url(path, e))?;

        let http = reqwest::Client::builder()
            .timeout(Duration::from_secs(timeout_secs))
            .use_rustls_tls()
            .danger_accept_invalid_certs(insecure)
            .build()?;

        Ok(Self {
            http,
            query_url,
            config,
            batch_size: BATCH_SIZE,
        })
    }

    /// The `LogQL` stream selector (and line filter) for one try of a task.
    ///
    /// `run_id` is deliberately not part of it: Kubernetes sanitises the label
    /// value and appends a hash, so it never equals Airflow's run id. dag, task
    /// and try plus the try's time window identify the pod well enough.
    pub fn task_selector(&self, dag_id: &str, task_id: &str, try_number: u32) -> String {
        let mut matchers = vec![
            format!("dag_id=\"{}\"", escape_label_value(dag_id)),
            format!("task_id=\"{}\"", escape_label_value(task_id)),
            format!("try_number=\"{try_number}\""),
        ];
        let excluded: Vec<String> = self
            .config
            .exclude_containers
            .iter()
            .map(|c| regex::escape(c))
            .collect();
        if !excluded.is_empty() {
            matchers.push(format!(
                "container!~\"{}\"",
                escape_label_value(&excluded.join("|"))
            ));
        }
        let mut query = format!("{{{}}}", matchers.join(", "));
        if let Some(pattern) = self
            .config
            .exclude_lines
            .as_deref()
            .filter(|p| !p.is_empty())
        {
            let _ = write!(query, " !~ \"{}\"", escape_label_value(pattern));
        }
        query
    }

    /// Fetch every line of a try, oldest first, up to the configured `max_lines`.
    ///
    /// Loki pages by time, so each call resumes at the last timestamp seen.
    /// Resuming at that timestamp (rather than one nanosecond later) and skipping
    /// the lines already collected there keeps lines that share a timestamp
    /// across a page boundary.
    pub async fn fetch_task_logs(&self, query: &TaskLogQuery<'_>) -> Result<LokiLogs> {
        let selector = self.task_selector(query.dag_id, query.task_id, query.try_number);
        let end_ns = query.end.unix_timestamp_nanos();
        let mut start_ns = query.start.unix_timestamp_nanos();
        let max_lines = self.config.max_lines.max(1);

        let mut logs = LokiLogs::default();
        let mut stream_index: BTreeMap<BTreeMap<String, String>, usize> = BTreeMap::new();
        // Lines already collected at the current `start_ns`, keyed by stream and text.
        let mut seen_at_start: HashSet<(usize, String)> = HashSet::new();

        loop {
            let remaining = max_lines - logs.entries.len();
            let limit = remaining.min(self.batch_size) + seen_at_start.len();
            let streams = self.query_range(&selector, start_ns, end_ns, limit).await?;

            let mut batch: Vec<LokiEntry> = Vec::new();
            for stream in streams {
                let index = *stream_index
                    .entry(stream.stream.clone())
                    .or_insert_with(|| {
                        logs.streams.push(stream.stream.clone());
                        logs.streams.len() - 1
                    });
                for value in stream.values {
                    if let Some(entry) = value.into_entry(index) {
                        batch.push(entry);
                    }
                }
            }
            let received = batch.len();
            batch.sort_by_key(|e| e.timestamp_ns);

            let mut fresh = 0;
            for entry in batch {
                if entry.timestamp_ns == start_ns
                    && seen_at_start.contains(&(entry.stream, entry.line.clone()))
                {
                    continue;
                }
                if logs.entries.len() >= max_lines {
                    logs.truncated = true;
                    break;
                }
                logs.entries.push(entry);
                fresh += 1;
            }

            if logs.truncated || received < limit || fresh == 0 {
                break;
            }
            if logs.entries.len() >= max_lines {
                logs.truncated = true;
                break;
            }

            let last_ts = logs.entries.last().map_or(start_ns, |e| e.timestamp_ns);
            if last_ts != start_ns {
                seen_at_start.clear();
                start_ns = last_ts;
            }
            seen_at_start.extend(
                logs.entries
                    .iter()
                    .rev()
                    .take_while(|e| e.timestamp_ns == start_ns)
                    .map(|e| (e.stream, e.line.clone())),
            );
        }

        debug!(
            "Loki returned {} lines for {}.{} try {}",
            logs.entries.len(),
            query.dag_id,
            query.task_id,
            query.try_number
        );
        Ok(logs)
    }

    async fn query_range(
        &self,
        query: &str,
        start_ns: i128,
        end_ns: i128,
        limit: usize,
    ) -> Result<Vec<StreamResult>> {
        let request = self
            .http
            .request(Method::GET, self.query_url.clone())
            .query(&[
                ("query", query.to_string()),
                ("start", start_ns.to_string()),
                ("end", end_ns.to_string()),
                ("limit", limit.to_string()),
                ("direction", "forward".to_string()),
            ]);
        let request = self.authenticate(request)?.build()?;
        let method = request.method().clone();
        let url = request.url().clone();
        debug!("🔗 Loki request: {url}");

        let response = self.http.execute(request).await?;
        let status = response.status();
        if !status.is_success() {
            let body = response.text().await.unwrap_or_default();
            let body: String = body.chars().take(SNIPPET_LEN).collect();
            return Err(AirflowError::status(&method, &url, status, &body));
        }
        let response: QueryResponse = read_json(response, "Loki query_range response").await?;
        if response.data.result_type != "streams" {
            return Ok(Vec::new());
        }
        Ok(response.data.result)
    }

    fn authenticate(&self, request: reqwest::RequestBuilder) -> Result<reqwest::RequestBuilder> {
        if let Some(token) = self.config.token.as_deref() {
            return Ok(request.bearer_auth(resolve_secret(token)?));
        }
        match (&self.config.username, &self.config.password) {
            (Some(username), password) => {
                let password = password.as_deref().map(resolve_secret).transpose()?;
                Ok(request.basic_auth(resolve_secret(username)?, password))
            }
            (None, _) => Ok(request),
        }
    }
}

/// Resolve `$NAME` or `${NAME}` from the environment; anything else is literal.
fn resolve_secret(value: &str) -> Result<String> {
    let name = value
        .strip_prefix("${")
        .and_then(|v| v.strip_suffix('}'))
        .or_else(|| value.strip_prefix('$'));
    match name {
        Some(name) if !name.is_empty() => std::env::var(name).map_err(|_| AirflowError::Auth {
            provider: "Grafana",
            message: format!("environment variable {name} is not set"),
        }),
        _ => Ok(value.to_string()),
    }
}

/// Escape a value for a double-quoted `LogQL` string.
fn escape_label_value(value: &str) -> String {
    value.replace('\\', "\\\\").replace('"', "\\\"")
}

#[derive(Debug, Deserialize)]
struct QueryResponse {
    data: QueryData,
}

#[derive(Debug, Deserialize)]
struct QueryData {
    #[serde(rename = "resultType")]
    result_type: String,
    #[serde(default)]
    result: Vec<StreamResult>,
}

#[derive(Debug, Deserialize)]
struct StreamResult {
    #[serde(default)]
    stream: BTreeMap<String, String>,
    #[serde(default)]
    values: Vec<StreamValue>,
}

/// `[timestamp_ns, line]`, optionally followed by structured metadata.
#[derive(Debug, Deserialize)]
struct StreamValue(Vec<serde_json::Value>);

impl StreamValue {
    fn into_entry(self, stream: usize) -> Option<LokiEntry> {
        let mut values = self.0.into_iter();
        let timestamp_ns = values.next()?.as_str()?.parse().ok()?;
        let serde_json::Value::String(line) = values.next()? else {
            return None;
        };
        Some(LokiEntry {
            timestamp_ns,
            line,
            stream,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use time::macros::datetime;
    use wiremock::matchers::{method, path, query_param};
    use wiremock::{Mock, MockServer, ResponseTemplate};

    fn config(url: &str) -> GrafanaConfig {
        GrafanaConfig {
            url: url.to_string(),
            username: Some("user".to_string()),
            password: Some("pass".to_string()),
            token: None,
            loki_datasource_uid: "abc123".to_string(),
            exclude_containers: vec!["vault-agent-init".to_string()],
            exclude_lines: None,
            max_lines: 20_000,
        }
    }

    fn query() -> TaskLogQuery<'static> {
        TaskLogQuery {
            dag_id: "my_dag",
            task_id: "extract.my_task",
            try_number: 3,
            start: datetime!(2026-09-30 16:00 UTC),
            end: datetime!(2026-09-30 17:00 UTC),
        }
    }

    fn streams(entries: &[(&str, i128, &str)]) -> serde_json::Value {
        let mut by_stream: BTreeMap<&str, Vec<serde_json::Value>> = BTreeMap::new();
        for (container, ts, line) in entries {
            by_stream
                .entry(container)
                .or_default()
                .push(serde_json::json!([ts.to_string(), line]));
        }
        let result: Vec<_> = by_stream
            .into_iter()
            .map(|(container, values)| {
                serde_json::json!({"stream": {"container": container, "pod": "p1"}, "values": values})
            })
            .collect();
        serde_json::json!({"status": "success", "data": {"resultType": "streams", "result": result}})
    }

    #[test]
    fn selector_uses_task_labels_and_excludes_containers() {
        let mut cfg = config("http://grafana");
        cfg.exclude_containers = vec!["vault-agent-init".into(), "istio.proxy".into()];
        cfg.exclude_lines = Some("pip install".into());
        let client = LokiClient::new(cfg, 30, false).unwrap();
        assert_eq!(
            client.task_selector("d", "g.t\"x", 2),
            r#"{dag_id="d", task_id="g.t\"x", try_number="2", container!~"vault\\-agent\\-init|istio\\.proxy"} !~ "pip install""#
        );
    }

    #[test]
    fn query_url_goes_through_the_datasource_proxy() {
        let client = LokiClient::new(config("https://grafana.example.com/sub"), 30, false).unwrap();
        assert_eq!(
            client.query_url.as_str(),
            "https://grafana.example.com/sub/api/datasources/proxy/uid/abc123/loki/api/v1/query_range"
        );
    }

    #[test]
    fn resolves_secrets_from_the_environment() {
        assert_eq!(resolve_secret("literal").unwrap(), "literal");
        assert!(resolve_secret("$FLOWRS_TEST_SURELY_UNSET_VAR").is_err());
        let path = std::env::var("PATH").unwrap();
        assert_eq!(resolve_secret("${PATH}").unwrap(), path);
        assert_eq!(resolve_secret("$PATH").unwrap(), path);
    }

    #[tokio::test]
    async fn fetches_and_merges_streams_in_time_order() {
        let server = MockServer::start().await;
        Mock::given(method("GET"))
            .and(path(
                "/api/datasources/proxy/uid/abc123/loki/api/v1/query_range",
            ))
            .and(query_param("direction", "forward"))
            .respond_with(ResponseTemplate::new(200).set_body_json(streams(&[
                ("base", 1, "a"),
                ("base", 3, "c"),
                ("other", 2, "b"),
            ])))
            .mount(&server)
            .await;

        let client = LokiClient::new(config(&server.uri()), 30, false).unwrap();
        let logs = client.fetch_task_logs(&query()).await.unwrap();
        let lines: Vec<_> = logs.entries.iter().map(|e| e.line.as_str()).collect();
        assert_eq!(lines, ["a", "b", "c"]);
        assert_eq!(logs.streams.len(), 2);
        assert!(!logs.truncated);

        let request = &server.received_requests().await.unwrap()[0];
        assert!(request.headers.get("authorization").is_some());
    }

    #[tokio::test]
    async fn paginates_by_timestamp_without_losing_ties() {
        let server = MockServer::start().await;
        let start = query().start.unix_timestamp_nanos();
        // First page: exactly `limit` lines, the last two sharing a timestamp.
        Mock::given(method("GET"))
            .and(query_param("start", start.to_string()))
            .respond_with(ResponseTemplate::new(200).set_body_json(streams(&[
                ("base", start + 1, "a"),
                ("base", start + 2, "b"),
                ("base", start + 2, "c"),
            ])))
            .mount(&server)
            .await;
        // Second page resumes at that timestamp; Loki repeats b and c, and has d.
        Mock::given(method("GET"))
            .and(query_param("start", (start + 2).to_string()))
            .respond_with(ResponseTemplate::new(200).set_body_json(streams(&[
                ("base", start + 2, "b"),
                ("base", start + 2, "c"),
                ("base", start + 2, "d"),
            ])))
            .mount(&server)
            .await;

        let mut client = LokiClient::new(config(&server.uri()), 30, false).unwrap();
        // A full first page forces a second request.
        client.batch_size = 3;
        let logs = client.fetch_task_logs(&query()).await.unwrap();
        let lines: Vec<_> = logs.entries.iter().map(|e| e.line.as_str()).collect();
        assert_eq!(lines, ["a", "b", "c", "d"]);
        assert!(!logs.truncated);
    }

    #[tokio::test]
    async fn stops_at_max_lines() {
        let server = MockServer::start().await;
        Mock::given(method("GET"))
            .respond_with(ResponseTemplate::new(200).set_body_json(streams(&[
                ("base", 1, "a"),
                ("base", 2, "b"),
                ("base", 3, "c"),
            ])))
            .mount(&server)
            .await;
        let mut cfg = config(&server.uri());
        cfg.max_lines = 2;
        let client = LokiClient::new(cfg, 30, false).unwrap();
        let logs = client.fetch_task_logs(&query()).await.unwrap();
        assert_eq!(logs.entries.len(), 2);
        assert!(logs.truncated);
    }

    #[tokio::test]
    async fn surfaces_grafana_errors() {
        let server = MockServer::start().await;
        Mock::given(method("GET"))
            .respond_with(
                ResponseTemplate::new(401).set_body_string("invalid username or password"),
            )
            .mount(&server)
            .await;
        let client = LokiClient::new(config(&server.uri()), 30, false).unwrap();
        let error = client.fetch_task_logs(&query()).await.unwrap_err();
        assert!(
            error.to_string().contains("invalid username or password"),
            "got: {error}"
        );
    }
}
