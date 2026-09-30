use std::fmt::{Display, Formatter};

use clap::ValueEnum;
use serde::{Deserialize, Serialize};
use strum::EnumIter;

use crate::auth::AirflowAuth;

#[derive(Deserialize, Serialize, Debug, Clone, PartialEq, Eq, Default)]
pub enum AirflowVersion {
    #[default]
    V2,
    V3,
}

impl AirflowVersion {
    pub const fn api_path(&self) -> &str {
        match self {
            Self::V2 => "api/v1",
            Self::V3 => "api/v2",
        }
    }
}

#[derive(Deserialize, Serialize, Debug, Clone, PartialEq, Eq, ValueEnum, EnumIter)]
pub enum ManagedService {
    Conveyor,
    Mwaa,
    Astronomer,
    Gcc,
}

impl Display for ManagedService {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Conveyor => write!(f, "Conveyor"),
            Self::Mwaa => write!(f, "MWAA"),
            Self::Astronomer => write!(f, "Astronomer"),
            Self::Gcc => write!(f, "Google Cloud Composer"),
        }
    }
}

#[derive(Deserialize, Serialize, Debug, Clone, Default)]
pub struct GccConfig {
    pub regions: Vec<String>,
    /// GCP project IDs to search for Composer environments.
    /// `None` means search all accessible projects.
    pub projects: Option<Vec<String>>,
}

#[derive(Deserialize, Serialize, Debug, Clone)]
pub struct AirflowConfig {
    pub name: String,
    pub endpoint: String,
    pub auth: AirflowAuth,
    pub managed: Option<ManagedService>,
    #[serde(default)]
    pub version: AirflowVersion,
    /// Request timeout in seconds. Defaults to 30 seconds if not specified.
    #[serde(default = "default_timeout")]
    pub timeout_secs: u64,
    /// Whether to allow insecure SSL connections.
    #[serde(default)]
    pub insecure: bool,
    /// Grafana/Loki access used to read task logs Airflow can no longer serve.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub grafana: Option<GrafanaConfig>,
}

/// Where to find task logs in Loki, queried through Grafana's datasource proxy.
///
/// Credential fields accept either a literal value or `$NAME` / `${NAME}`, which
/// is read from the environment when a query is made, so secrets can stay out of
/// the config file. Set `username` and `password` for basic auth, or `token` for
/// a Grafana service-account token.
#[derive(Deserialize, Serialize, Clone)]
pub struct GrafanaConfig {
    /// Grafana base URL, e.g. `https://grafana.example.com`.
    pub url: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub username: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub password: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub token: Option<String>,
    /// UID of the Loki datasource holding the Airflow worker logs.
    pub loki_datasource_uid: String,
    /// Containers whose output is left out of task logs (sidecars, init containers).
    #[serde(default = "default_exclude_containers")]
    pub exclude_containers: Vec<String>,
    /// Optional regex; log lines matching it are dropped server-side.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub exclude_lines: Option<String>,
    /// Upper bound on the number of lines fetched for a single try.
    #[serde(default = "default_max_lines")]
    pub max_lines: usize,
}

impl std::fmt::Debug for GrafanaConfig {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("GrafanaConfig")
            .field("url", &self.url)
            .field("username", &self.username)
            .field("password", &self.password.as_ref().map(|_| "<redacted>"))
            .field("token", &self.token.as_ref().map(|_| "<redacted>"))
            .field("loki_datasource_uid", &self.loki_datasource_uid)
            .field("exclude_containers", &self.exclude_containers)
            .field("exclude_lines", &self.exclude_lines)
            .field("max_lines", &self.max_lines)
            .finish()
    }
}

fn default_exclude_containers() -> Vec<String> {
    vec!["vault-agent-init".to_string()]
}

const fn default_max_lines() -> usize {
    20_000
}

pub const fn default_timeout() -> u64 {
    30
}
