//! Turning the raw lines Loki holds for a try into readable log text.
//!
//! A try usually spans two pods. The Airflow worker (Kubernetes executor)
//! writes JSON records: `{"timestamp": …, "level": …, "event": …, "logger": …}`.
//! When the task is a `KubernetesPodOperator`, the job runs in a second pod;
//! its own output is in Loki under that pod, and the worker's `PodManager`
//! also relays it as events prefixed with the container name (`[base] …`).
//! Every line is tagged with the role of its pod, and the relayed copies are
//! dropped when the job pod's own lines are present.

use std::collections::{BTreeMap, BTreeSet};
use std::fmt::Write as _;

use flowrs_airflow::loki::LokiLogs;
use serde_json::{Map, Value};
use time::OffsetDateTime;

use crate::airflow::model::common::{Log, LogSource};

/// Events longer than this are cut. Airflow error events embed the whole pod
/// manifest as a string, easily 100 KB on a single line.
const MAX_EVENT_CHARS: usize = 2000;

/// The event the Airflow supervisor logs when a try's process exits.
const TASK_FINISHED_EVENT: &str = "Task finished";

/// Fields rendered in the line prefix, or not worth showing at all.
const SKIPPED_FIELDS: [&str; 4] = ["timestamp", "level", "event", "logger"];

#[derive(Debug, Clone, Copy)]
pub(crate) struct LokiLogContext {
    pub forced: bool,
    /// The try has ended long enough ago that Loki will not receive more lines.
    pub complete: bool,
    pub start: OffsetDateTime,
    pub end: OffsetDateTime,
}

pub(crate) fn loki_logs_to_log(logs: &LokiLogs, ctx: LokiLogContext) -> Log {
    let mut content = String::new();

    let origin = if ctx.forced {
        "Log from Loki"
    } else {
        "Log from Loki (Airflow could not serve it)"
    };
    let _ = writeln!(content, "── {origin} ──");
    let roles: Vec<PodRole> = logs.streams.iter().map(PodRole::of).collect();
    let pods: BTreeSet<(PodRole, String, String)> = logs
        .streams
        .iter()
        .zip(&roles)
        .map(|(labels, role)| {
            let label = |name: &str| labels.get(name).cloned().unwrap_or_else(|| "?".into());
            (*role, label("pod"), label("node_name"))
        })
        .collect();
    for (role, pod, node) in &pods {
        let _ = writeln!(content, "── {} pod {pod} on node {node} ──", role.name());
    }

    if logs.entries.is_empty() {
        let _ = writeln!(
            content,
            "No lines in Loki for this try between {} and {} (UTC).",
            ctx.start, ctx.end
        );
        return log(content, ctx);
    }

    // The worker's relayed copy of the job's output is only kept when the job
    // pod's own lines did not make it to Loki.
    let has_job_lines = logs
        .entries
        .iter()
        .any(|entry| roles.get(entry.stream) == Some(&PodRole::Job));

    let mut finished = false;
    for entry in &logs.entries {
        let role = roles.get(entry.stream).copied().unwrap_or(PodRole::Other);
        let line = format_line(&entry.line, entry.timestamp_ns);
        if line.is_relay && has_job_lines && role == PodRole::Worker {
            continue;
        }
        finished |= line.is_finish;
        content.push_str(role.tag());
        content.push_str(&line.text);
        content.push('\n');
    }

    if logs.truncated {
        let _ = writeln!(
            content,
            "── stopped after {} lines (grafana.max_lines) ──",
            logs.entries.len()
        );
    } else if ctx.complete && !finished {
        let _ = writeln!(
            content,
            "⚠ No \"{TASK_FINISHED_EVENT}\" event: the pod stopped mid-run (for example a spot node interruption)."
        );
    }
    log(content, ctx)
}

fn log(content: String, ctx: LokiLogContext) -> Log {
    Log {
        continuation_token: None,
        content,
        source: LogSource::Loki {
            forced: ctx.forced,
            complete: ctx.complete,
        },
    }
}

/// Which part of a try a pod plays, from the labels Airflow puts on it.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
enum PodRole {
    /// Runs the Airflow task runner (`KubernetesExecutor`).
    Worker,
    /// Launched by a `KubernetesPodOperator` to run the job itself.
    Job,
    Other,
}

impl PodRole {
    fn of(labels: &BTreeMap<String, String>) -> Self {
        let is = |name: &str, value: &str| labels.get(name).is_some_and(|v| v == value);
        if is("kubernetes_pod_operator", "True") {
            Self::Job
        } else if is("component", "worker") || is("kubernetes_executor", "True") {
            Self::Worker
        } else {
            Self::Other
        }
    }

    const fn name(self) -> &'static str {
        match self {
            Self::Worker => "worker",
            Self::Job => "job",
            Self::Other => "other",
        }
    }

    /// Line prefix; the logs panel colors lines by it.
    const fn tag(self) -> &'static str {
        match self {
            Self::Worker => "[worker] ",
            Self::Job => "[job]    ",
            Self::Other => "",
        }
    }
}

struct FormattedLine {
    text: String,
    /// The supervisor's "Task finished" event.
    is_finish: bool,
    /// Job output the worker's `PodManager` relayed as `[container] …`.
    is_relay: bool,
}

/// Render one Loki line.
fn format_line(raw: &str, timestamp_ns: i128) -> FormattedLine {
    let Ok(Value::Object(record)) = serde_json::from_str::<Value>(raw) else {
        return FormattedLine {
            text: format!("{} {}", format_ns(timestamp_ns), truncate(raw.trim_end())),
            is_finish: false,
            is_relay: false,
        };
    };

    let timestamp = record
        .get("timestamp")
        .and_then(Value::as_str)
        .map_or_else(|| format_ns(timestamp_ns), str::to_string);
    let level = record.get("level").and_then(Value::as_str).unwrap_or("");
    let event = record.get("event").map(value_text).unwrap_or_default();
    let is_finish = event.trim() == TASK_FINISHED_EVENT;
    let is_relay = split_container_prefix(&event).is_some()
        && record
            .get("logger")
            .and_then(Value::as_str)
            .is_some_and(|logger| logger.contains("pod_manager"));

    let mut line = format!(
        "{timestamp} {:<7} {}",
        level.to_uppercase(),
        unwrap_container_output(&event)
    );
    push_extra_fields(&mut line, &record);
    FormattedLine {
        text: truncate(&line),
        is_finish,
        is_relay,
    }
}

/// `[base] {"event": …, "level": …}` → `[base] LEVEL event key=value`.
fn unwrap_container_output(event: &str) -> String {
    let Some((prefix, rest)) = split_container_prefix(event) else {
        return event.to_string();
    };
    match serde_json::from_str::<Value>(rest) {
        Ok(Value::Object(inner)) if inner.contains_key("event") => {
            let level = inner.get("level").and_then(Value::as_str).unwrap_or("");
            let event = inner.get("event").map(value_text).unwrap_or_default();
            let mut line = if level.is_empty() {
                format!("{prefix} {event}")
            } else {
                format!("{prefix} {} {event}", level.to_uppercase())
            };
            push_extra_fields(&mut line, &inner);
            line
        }
        _ => event.to_string(),
    }
}

/// Split `[name] rest` into (`[name]`, `rest`).
fn split_container_prefix(event: &str) -> Option<(&str, &str)> {
    if !event.starts_with('[') {
        return None;
    }
    let close = event.find("] ")?;
    let name = &event[1..close];
    if name.is_empty() || name.contains(char::is_whitespace) {
        return None;
    }
    Some((&event[..=close], event[close + 2..].trim_start()))
}

fn push_extra_fields(line: &mut String, record: &Map<String, Value>) {
    for (key, value) in record {
        if SKIPPED_FIELDS.contains(&key.as_str()) || value.is_null() {
            continue;
        }
        let _ = write!(line, " {key}={}", value_text(value));
    }
}

fn value_text(value: &Value) -> String {
    match value {
        Value::String(s) => s.clone(),
        other => other.to_string(),
    }
}

fn truncate(line: &str) -> String {
    let mut chars = line.char_indices();
    match chars.nth(MAX_EVENT_CHARS) {
        None => line.to_string(),
        Some((cut, _)) => {
            let rest = line[cut..].chars().count();
            format!("{} … [{rest} more chars]", &line[..cut])
        }
    }
}

fn format_ns(timestamp_ns: i128) -> String {
    OffsetDateTime::from_unix_timestamp_nanos(timestamp_ns).map_or_else(
        |_| timestamp_ns.to_string(),
        |ts| {
            format!(
                "{:04}-{:02}-{:02}T{:02}:{:02}:{:02}.{:06}",
                ts.year(),
                u8::from(ts.month()),
                ts.day(),
                ts.hour(),
                ts.minute(),
                ts.second(),
                ts.microsecond()
            )
        },
    )
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;

    use flowrs_airflow::loki::LokiEntry;
    use time::macros::datetime;

    use super::*;

    fn ctx(complete: bool) -> LokiLogContext {
        LokiLogContext {
            forced: false,
            complete,
            start: datetime!(2026-09-30 16:00 UTC),
            end: datetime!(2026-09-30 17:00 UTC),
        }
    }

    fn logs(lines: &[&str]) -> LokiLogs {
        LokiLogs {
            entries: lines
                .iter()
                .enumerate()
                .map(|(i, line)| LokiEntry {
                    timestamp_ns: i128::try_from(i).unwrap(),
                    line: (*line).to_string(),
                    stream: 0,
                })
                .collect(),
            streams: vec![BTreeMap::from([
                ("pod".to_string(), "pod-1".to_string()),
                ("node_name".to_string(), "spot-node".to_string()),
            ])],
            truncated: false,
        }
    }

    #[test]
    fn unwraps_nested_container_json() {
        let raw = r#"{"timestamp":"2026-09-30T16:15:07.585793","level":"info","event":"[base] {\"event\": \"Extracting\", \"level\": \"info\", \"name\": \"tap-x\"}","logger":"airflow.providers.cncf.kubernetes.utils.pod_manager.PodManager"}"#;
        let line = format_line(raw, 0);
        assert_eq!(
            line.text,
            "2026-09-30T16:15:07.585793 INFO    [base] INFO Extracting name=tap-x"
        );
        assert!(!line.is_finish);
        assert!(line.is_relay);
    }

    #[test]
    fn keeps_non_json_container_output() {
        let raw = r#"{"timestamp":"t","level":"info","event":"[base] plain output"}"#;
        assert_eq!(format_line(raw, 0).text, "t INFO    [base] plain output");
    }

    #[test]
    fn shows_task_finished_fields() {
        let raw = r#"{"timestamp":"t","level":"info","event":"Task finished","exit_code":0,"duration":12.5,"final_state":"success","logger":"supervisor"}"#;
        let FormattedLine {
            text: line,
            is_finish: finish,
            ..
        } = format_line(raw, 0);
        assert_eq!(
            line,
            "t INFO    Task finished duration=12.5 exit_code=0 final_state=success"
        );
        assert!(finish);
    }

    #[test]
    fn truncates_huge_lines() {
        let raw = format!(
            r#"{{"timestamp":"t","level":"error","event":"{}"}}"#,
            "x".repeat(5000)
        );
        let line = format_line(&raw, 0).text;
        assert!(line.len() < 2100, "len {}", line.len());
        assert!(line.ends_with("more chars]"));
    }

    #[test]
    fn non_json_lines_get_the_loki_timestamp() {
        let line = format_line("hello", 1_790_000_000_000_000_000).text;
        assert!(line.ends_with(" hello"));
        assert!(line.starts_with("2026-"));
    }

    #[test]
    fn warns_when_a_complete_try_never_finished() {
        let log = loki_logs_to_log(&logs(&[r#"{"event":"Starting"}"#]), ctx(true));
        assert!(log.content.contains("pod pod-1 on node spot-node"));
        assert!(log.content.contains("stopped mid-run"));
        assert_eq!(
            log.source,
            LogSource::Loki {
                forced: false,
                complete: true
            }
        );

        let finished = loki_logs_to_log(
            &logs(&[r#"{"event":"Starting"}"#, r#"{"event":"Task finished"}"#]),
            ctx(true),
        );
        assert!(!finished.content.contains("stopped mid-run"));

        // A try that may still be running is not flagged.
        let running = loki_logs_to_log(&logs(&[r#"{"event":"Starting"}"#]), ctx(false));
        assert!(!running.content.contains("stopped mid-run"));
    }

    fn stream(pod: &str, role_label: (&str, &str)) -> BTreeMap<String, String> {
        BTreeMap::from([
            ("pod".to_string(), pod.to_string()),
            ("node_name".to_string(), "spot-node".to_string()),
            (role_label.0.to_string(), role_label.1.to_string()),
        ])
    }

    fn entry(timestamp_ns: i128, stream: usize, line: &str) -> LokiEntry {
        LokiEntry {
            timestamp_ns,
            line: line.to_string(),
            stream,
        }
    }

    const RELAY: &str = r#"{"timestamp":"t2","level":"info","event":"[base] {\"event\": \"Extracting\"}","logger":"airflow.providers.cncf.kubernetes.utils.pod_manager.PodManager"}"#;

    #[test]
    fn tags_lines_by_pod_role_and_drops_relayed_job_output() {
        let logs = LokiLogs {
            entries: vec![
                entry(
                    1,
                    0,
                    r#"{"timestamp":"t1","level":"info","event":"Executing workload"}"#,
                ),
                entry(
                    2,
                    1,
                    r#"{"timestamp":"t2","level":"info","event":"Extracting"}"#,
                ),
                entry(3, 0, RELAY),
            ],
            streams: vec![
                stream("worker-pod", ("component", "worker")),
                stream("job-pod", ("kubernetes_pod_operator", "True")),
            ],
            truncated: false,
        };
        let content = loki_logs_to_log(&logs, ctx(false)).content;
        assert!(content.contains("── worker pod worker-pod on node spot-node ──"));
        assert!(content.contains("── job pod job-pod on node spot-node ──"));
        assert!(content.contains("[worker] t1 INFO    Executing workload"));
        assert!(content.contains("[job]    t2 INFO    Extracting"));
        assert!(!content.contains("[base]"), "relay kept: {content}");
    }

    #[test]
    fn keeps_relayed_output_when_the_job_pod_has_no_lines() {
        let logs = LokiLogs {
            entries: vec![entry(1, 0, RELAY)],
            streams: vec![stream("worker-pod", ("component", "worker"))],
            truncated: false,
        };
        let content = loki_logs_to_log(&logs, ctx(false)).content;
        assert!(
            content.contains("[worker] t2 INFO    [base] Extracting"),
            "{content}"
        );
    }

    #[test]
    fn explains_an_empty_result() {
        let log = loki_logs_to_log(&LokiLogs::default(), ctx(true));
        assert!(log.content.contains("No lines in Loki"));
    }
}
