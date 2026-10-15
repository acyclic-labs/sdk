//! Durable timer data shared by guest scheduling and the harness. This module
//! owns no scheduler, clock, waiting loop or second copy of retained timer state.

use serde::{Deserialize, Serialize};
use ts_rs::{Config, TS};

/// Existing timer stream namespace; each task's records remain the source of truth.
pub const TIMER_STREAM_PREFIX: &str = "harness/v2/timers";

/// Exact existing harness record. Generic identity types let the harness retain
/// its nominal TaskId/OperationId without converting or allocating those fields.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize, TS)]
#[serde(deny_unknown_fields)]
pub struct TimerRecord<Task = String, Operation = String> {
    /// Task identity; default authoring projection uses canonical UUID text.
    pub task_id: Task,
    /// Stable operation identity, fixed before scheduling.
    pub operation_id: Operation,
    /// Positive Unix deadline in milliseconds; readiness is now >= deadline.
    #[ts(type = "bigint")]
    pub deadline_unix_ms: u64,
}

/// Invalid durable timer input. The record never embeds credentials or a clock.
#[derive(Clone, Debug, PartialEq, Eq, thiserror::Error)]
pub enum TimerError {
    /// IDs must retain the harness's canonical lowercase hyphenated UUID spelling.
    #[error("noncanonical timer task or operation identity")]
    Identity,
    /// The harness rejects zero deadlines during replay.
    #[error("timer deadline must be positive")]
    Deadline,
    /// Existing Stream path/key bounds rejected a derived identity.
    #[error(transparent)]
    Stream(#[from] acyclic_stream::StreamError),
}

impl TimerRecord {
    /// Validate the public string projection before retaining a scheduling effect.
    pub fn validate(&self) -> Result<(), TimerError> {
        for identity in [&self.task_id, &self.operation_id] {
            let parsed = uuid::Uuid::parse_str(identity).map_err(|_| TimerError::Identity)?;
            if parsed.hyphenated().to_string() != *identity { return Err(TimerError::Identity); }
        }
        if self.deadline_unix_ms == 0 { return Err(TimerError::Deadline); }
        Ok(())
    }

    /// Locate the original retained task timer stream, not a scheduler-owned copy.
    pub fn stream_path(&self) -> Result<acyclic_stream::StreamPath, TimerError> {
        self.validate()?;
        Ok(acyclic_stream::StreamPath::new(format!("{TIMER_STREAM_PREFIX}/{}", self.task_id))?)
    }

    /// Stable logical timer ID, independent of deadline, fire time and retries.
    pub fn timer_id(&self) -> Result<String, TimerError> {
        self.validate()?;
        Ok(format!("{TIMER_STREAM_PREFIX}/{}/{}", self.task_id, self.operation_id))
    }

    /// Inbox append retry key, separated from retention's account-scoped retry key.
    /// Reusing a timer for another payload/target must fail the provider's exact
    /// request check instead of silently treating retention as a completed fire.
    pub fn fire_key(&self) -> Result<acyclic_stream::IdempotencyKey, TimerError> {
        Ok(acyclic_stream::IdempotencyKey::new(format!("timer-fire:{}", self.timer_id()?).into_bytes())?)
    }
}

/// Generate the shared timer record authoring shape from the same Rust declaration.
pub fn typescript() -> String {
    let config = Config::default();
    let output = format!("// Generated from acyclic_actors::timers. Do not edit.\nexport {}\n", <TimerRecord as TS>::decl(&config));
    output.lines().map(str::trim_end).collect::<Vec<_>>().join("\n") + "\n"
}
