use serde::{Deserialize, Serialize};
use std::{
    collections::BTreeMap,
    fmt::{Display, Formatter},
};

/// An individual structured log message with timestamp and event
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StructuredLogMessage {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub timestamp: Option<String>,
    pub event: String,
    #[serde(flatten)]
    pub additional_fields: BTreeMap<String, serde_json::Value>,
}

/// Log content can be either structured messages or plain text lines
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(untagged)]
pub enum LogContent {
    Structured(Vec<StructuredLogMessage>),
    Plain(Vec<String>),
}

impl LogContent {
    /// Whether the log holds anything from the task itself, as opposed to only
    /// Airflow's "Log message source details" group reporting where it looked
    /// (which is all it returns once the worker that ran the try is gone).
    pub fn has_task_output(&self) -> bool {
        match self {
            Self::Structured(messages) => messages.iter().any(|msg| {
                let event = msg.event.trim();
                !(event.is_empty()
                    || event.starts_with("::group::")
                    || event.starts_with("::endgroup::")
                    || msg.additional_fields.contains_key("sources"))
            }),
            Self::Plain(lines) => lines
                .iter()
                .any(|line| crate::client::text_has_task_output(line)),
        }
    }
}

impl Display for LogContent {
    /// Convert log content to a single string representation
    fn fmt(&self, f: &mut Formatter) -> std::fmt::Result {
        match self {
            Self::Structured(messages) => {
                for msg in messages {
                    if let Some(timestamp) = &msg.timestamp {
                        write!(f, "{timestamp} | ")?;
                    }
                    for (key, value) in &msg.additional_fields {
                        write!(f, "{key}: {value} | ")?;
                    }
                    write!(f, "{} | ", msg.event)?;
                    writeln!(f)?;
                }
            }
            Self::Plain(lines) => {
                for line in lines {
                    writeln!(f, "{line}")?;
                }
            }
        }
        Ok(())
    }
}

#[derive(Debug, Serialize, Deserialize)]
pub struct Log {
    #[serde(rename = "continuation_token")]
    pub continuation_token: Option<String>,
    pub content: LogContent,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn source_details_group_alone_is_not_task_output() {
        let content: LogContent = serde_json::from_value(serde_json::json!([
            {"timestamp": null, "event": "::group::Log message source details",
             "sources": ["Could not read served logs: HTTPConnectionPool(host='pod', port=8793)",
                         "Reading from k8s pod logs failed: ('Cannot find pod for ti %s', ...)"]},
            {"event": "::endgroup::"}
        ]))
        .unwrap();
        assert!(!content.has_task_output());
        assert!(!LogContent::Structured(vec![]).has_task_output());
    }

    #[test]
    fn task_events_are_task_output() {
        let content: LogContent = serde_json::from_value(serde_json::json!([
            {"event": "::group::Log message source details", "sources": ["/logs/x.log"]},
            {"event": "::endgroup::"},
            {"timestamp": "2026-09-30T16:15:07Z", "event": "Task started", "level": "info"}
        ]))
        .unwrap();
        assert!(content.has_task_output());
    }
}
