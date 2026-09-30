//! Turning the raw lines Loki holds for a try into readable log text.
//!
//! Each Loki line is a JSON record written by the Airflow worker:
//! `{"timestamp": …, "level": …, "event": …, "logger": …, …}`. Output of the
//! job pod itself arrives as an event prefixed with its container name
//! (`[base] …`), frequently another structlog JSON document, which is unwrapped.

use std::collections::BTreeSet;
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
    let pods: BTreeSet<(String, String)> = logs
        .streams
        .iter()
        .map(|labels| {
            let label = |name: &str| labels.get(name).cloned().unwrap_or_else(|| "?".into());
            (label("pod"), label("node_name"))
        })
        .collect();
    for (pod, node) in &pods {
        let _ = writeln!(content, "── pod {pod} on node {node} ──");
    }

    if logs.entries.is_empty() {
        let _ = writeln!(
            content,
            "No lines in Loki for this try between {} and {} (UTC).",
            ctx.start, ctx.end
        );
        return log(content, ctx);
    }

    let mut finished = false;
    for entry in &logs.entries {
        let (line, is_finish) = format_line(&entry.line, entry.timestamp_ns);
        finished |= is_finish;
        content.push_str(&line);
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

/// Render one Loki line; the flag tells whether it is the supervisor's
/// "Task finished" event.
fn format_line(raw: &str, timestamp_ns: i128) -> (String, bool) {
    let Ok(Value::Object(record)) = serde_json::from_str::<Value>(raw) else {
        return (
            format!("{} {}", format_ns(timestamp_ns), truncate(raw.trim_end())),
            false,
        );
    };

    let timestamp = record
        .get("timestamp")
        .and_then(Value::as_str)
        .map_or_else(|| format_ns(timestamp_ns), str::to_string);
    let level = record.get("level").and_then(Value::as_str).unwrap_or("");
    let event = record.get("event").map(value_text).unwrap_or_default();
    let is_finish = event.trim() == TASK_FINISHED_EVENT;

    let mut line = format!(
        "{timestamp} {:<7} {}",
        level.to_uppercase(),
        unwrap_container_output(&event)
    );
    push_extra_fields(&mut line, &record);
    (truncate(&line), is_finish)
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
        let raw = r#"{"timestamp":"2026-09-30T16:15:07.585793","level":"info","event":"[base] {\"event\": \"Extracting\", \"level\": \"info\", \"name\": \"tap-x\"}","logger":"airflow.PodManager"}"#;
        let (line, finish) = format_line(raw, 0);
        assert_eq!(
            line,
            "2026-09-30T16:15:07.585793 INFO    [base] INFO Extracting name=tap-x"
        );
        assert!(!finish);
    }

    #[test]
    fn keeps_non_json_container_output() {
        let raw = r#"{"timestamp":"t","level":"info","event":"[base] plain output"}"#;
        assert_eq!(format_line(raw, 0).0, "t INFO    [base] plain output");
    }

    #[test]
    fn shows_task_finished_fields() {
        let raw = r#"{"timestamp":"t","level":"info","event":"Task finished","exit_code":0,"duration":12.5,"final_state":"success","logger":"supervisor"}"#;
        let (line, finish) = format_line(raw, 0);
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
        let (line, _) = format_line(&raw, 0);
        assert!(line.len() < 2100, "len {}", line.len());
        assert!(line.ends_with("more chars]"));
    }

    #[test]
    fn non_json_lines_get_the_loki_timestamp() {
        let (line, _) = format_line("hello", 1_790_000_000_000_000_000);
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

    #[test]
    fn explains_an_empty_result() {
        let log = loki_logs_to_log(&LokiLogs::default(), ctx(true));
        assert!(log.content.contains("No lines in Loki"));
    }
}
