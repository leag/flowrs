use anyhow::{anyhow, Result};
use flowrs_airflow::loki::{LokiClient, TaskLogQuery};
use time::{Duration, OffsetDateTime};

use crate::airflow::client::convert_loki::{loki_logs_to_log, LokiLogContext};
use crate::airflow::client::convert_v1::v1_log_to_log;
use crate::airflow::client::convert_v2::v2_log_to_log;
use crate::airflow::client::FlowrsClient;
use crate::airflow::model::common::{Log, TaskTryGantt};

/// Loki is queried from a little before the try started, to catch pod startup
/// output logged before Airflow recorded the start.
const WINDOW_BEFORE: Duration = Duration::minutes(2);
/// ...and until a little after it ended, since pod output keeps arriving (and
/// being shipped) after Airflow marks the try done.
const WINDOW_AFTER: Duration = Duration::minutes(5);

impl FlowrsClient {
    pub async fn get_task_logs(
        &self,
        dag_id: &str,
        dag_run_id: &str,
        task_id: &str,
        task_try: u32,
    ) -> Result<Log> {
        match self {
            Self::V1(client) => {
                let response = client
                    .fetch_task_logs(dag_id, dag_run_id, task_id, task_try)
                    .await?;
                Ok(v1_log_to_log(response))
            }
            Self::V2(client) => {
                let response = client
                    .fetch_task_logs(dag_id, dag_run_id, task_id, task_try)
                    .await?;
                Ok(v2_log_to_log(response))
            }
        }
    }

    fn loki(&self) -> Option<&LokiClient> {
        match self {
            Self::V1(client) => client.loki(),
            Self::V2(client) => client.loki(),
        }
    }

    /// Whether this server has a `grafana` section, so logs can be read from Loki.
    pub fn has_loki(&self) -> bool {
        self.loki().is_some()
    }

    /// Read one try's log from Loki, bounded by the try's start and end time.
    pub async fn get_task_logs_from_loki(
        &self,
        dag_id: &str,
        task_id: &str,
        task_try: &TaskTryGantt,
        forced: bool,
    ) -> Result<Log> {
        let loki = self
            .loki()
            .ok_or_else(|| anyhow!("no grafana section is configured for this server"))?;
        let window = loki_window(task_try, OffsetDateTime::now_utc()).ok_or_else(|| {
            anyhow!(
                "try {} of {task_id} has not started, so there is nothing in Loki yet",
                task_try.try_number
            )
        })?;
        let logs = loki
            .fetch_task_logs(&TaskLogQuery {
                dag_id,
                task_id,
                try_number: task_try.try_number,
                start: window.start,
                end: window.end,
            })
            .await?;
        Ok(loki_logs_to_log(
            &logs,
            LokiLogContext {
                forced,
                complete: window.complete,
                start: window.start,
                end: window.end,
            },
        ))
    }
}

#[derive(Debug, PartialEq, Eq)]
struct LokiWindow {
    start: OffsetDateTime,
    end: OffsetDateTime,
    complete: bool,
}

/// The time range to search Loki for a try, or `None` if it has not started.
fn loki_window(task_try: &TaskTryGantt, now: OffsetDateTime) -> Option<LokiWindow> {
    let started = task_try.start_date.or(task_try.queued_when)?;
    let start = started - WINDOW_BEFORE;
    match task_try.end_date {
        Some(ended) => {
            let end = ended + WINDOW_AFTER;
            Some(LokiWindow {
                start,
                end: end.min(now),
                complete: end <= now,
            })
        }
        None => Some(LokiWindow {
            start,
            end: now,
            complete: false,
        }),
    }
}

#[cfg(test)]
mod tests {
    use time::macros::datetime;

    use super::*;

    fn task_try(
        start_date: Option<OffsetDateTime>,
        end_date: Option<OffsetDateTime>,
    ) -> TaskTryGantt {
        TaskTryGantt {
            try_number: 3,
            start_date,
            end_date,
            ..TaskTryGantt::default()
        }
    }

    #[test]
    fn finished_try_gets_margins_and_is_complete() {
        let window = loki_window(
            &task_try(
                Some(datetime!(2026-09-30 16:00 UTC)),
                Some(datetime!(2026-09-30 16:30 UTC)),
            ),
            datetime!(2026-09-30 18:00 UTC),
        )
        .unwrap();
        assert_eq!(
            window,
            LokiWindow {
                start: datetime!(2026-09-30 15:58 UTC),
                end: datetime!(2026-09-30 16:35 UTC),
                complete: true,
            }
        );
    }

    #[test]
    fn recently_finished_try_is_not_complete_yet() {
        let window = loki_window(
            &task_try(
                Some(datetime!(2026-09-30 16:00 UTC)),
                Some(datetime!(2026-09-30 16:30 UTC)),
            ),
            datetime!(2026-09-30 16:32 UTC),
        )
        .unwrap();
        assert_eq!(window.end, datetime!(2026-09-30 16:32 UTC));
        assert!(!window.complete);
    }

    #[test]
    fn running_try_extends_to_now() {
        let now = datetime!(2026-09-30 16:10 UTC);
        let window =
            loki_window(&task_try(Some(datetime!(2026-09-30 16:00 UTC)), None), now).unwrap();
        assert_eq!(window.end, now);
        assert!(!window.complete);
    }

    #[test]
    fn unstarted_try_has_no_window() {
        assert!(loki_window(&task_try(None, None), datetime!(2026-09-30 16:10 UTC)).is_none());
    }
}
