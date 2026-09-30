use serde::{Deserialize, Serialize};

/// Common Log model used by the application
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Log {
    pub continuation_token: Option<String>,
    pub content: String,
    #[serde(default)]
    pub source: LogSource,
}

/// Where a try's log content came from.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
pub enum LogSource {
    /// Served by Airflow.
    #[default]
    Airflow,
    /// Airflow answered, but only with where it looked for the log: the worker
    /// that ran the try is gone.
    AirflowUnavailable,
    /// Read from Loki through Grafana.
    Loki {
        /// The user asked for Loki rather than falling back to it.
        forced: bool,
        /// The try ended long enough ago that Loki will not receive more lines.
        complete: bool,
    },
}

impl LogSource {
    pub const fn is_loki(self) -> bool {
        matches!(self, Self::Loki { .. })
    }
}
