//! Durable timer data shared by guest scheduling and the harness. This module
//! owns no scheduler, clock, waiting loop or second copy of retained timer state.

use serde::{Deserialize, Serialize};
use ts_rs::{Config, TS};

/// Existing timer stream namespace; each task's records remain the source of truth.
pub const TIMER_STREAM_PREFIX: &str = "harness/v2/timers";

/// Maximum encoded source record; canonical UUIDs and a u64 deadline fit below this.
pub const MAX_TIMER_RECORD_BYTES: usize = 256;

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
    /// Malformed or oversized source record.
    #[error("invalid timer record JSON")]
    Record,
    /// Existing Stream path/key bounds rejected a derived identity.
    #[error(transparent)]
    Stream(#[from] acyclic_stream::StreamError),
}

impl TimerRecord {
    /// Construct the canonical source record before retention.
    pub fn new(
        task_id: String,
        operation_id: String,
        deadline_unix_ms: u64,
    ) -> Result<Self, TimerError> {
        let record = Self {
            task_id,
            operation_id,
            deadline_unix_ms,
        };
        record.validate()?;
        Ok(record)
    }

    /// Decode bounded untrusted timer input and apply the shared invariants.
    pub fn decode(bytes: &[u8]) -> Result<Self, TimerError> {
        if bytes.len() > MAX_TIMER_RECORD_BYTES {
            return Err(TimerError::Record);
        }
        let record: Self = serde_json::from_slice(bytes).map_err(|_| TimerError::Record)?;
        record.validate()?;
        Ok(record)
    }

    /// Encode the exact existing harness JSON layout without a second source record.
    pub fn encode(&self) -> Result<Vec<u8>, TimerError> {
        self.validate()?;
        let mut value = serde_json::to_value(self).map_err(|_| TimerError::Record)?;
        value.sort_all_objects();
        serde_json::to_vec(&value).map_err(|_| TimerError::Record)
    }

    /// Validate the public string projection before retaining a scheduling effect.
    pub fn validate(&self) -> Result<(), TimerError> {
        for identity in [&self.task_id, &self.operation_id] {
            let parsed = uuid::Uuid::parse_str(identity).map_err(|_| TimerError::Identity)?;
            if parsed.hyphenated().to_string() != *identity {
                return Err(TimerError::Identity);
            }
        }
        if self.deadline_unix_ms == 0 {
            return Err(TimerError::Deadline);
        }
        Ok(())
    }

    /// Locate the original retained task timer stream, not a scheduler-owned copy.
    pub fn stream_path(&self) -> Result<acyclic_stream::StreamPath, TimerError> {
        self.validate()?;
        Ok(acyclic_stream::StreamPath::new(format!(
            "{TIMER_STREAM_PREFIX}/{}",
            self.task_id
        ))?)
    }

    /// Stable logical timer ID, independent of deadline, fire time and retries.
    pub fn timer_id(&self) -> Result<String, TimerError> {
        self.validate()?;
        Ok(format!(
            "{TIMER_STREAM_PREFIX}/{}/{}",
            self.task_id, self.operation_id
        ))
    }

    /// Inbox append retry key, separated from retention's account-scoped retry key.
    /// Reusing a timer for another payload/target must fail the provider's exact
    /// request check instead of silently treating retention as a completed fire.
    pub fn fire_key(&self) -> Result<acyclic_stream::IdempotencyKey, TimerError> {
        Ok(acyclic_stream::IdempotencyKey::new(
            format!("timer-fire:{}", self.timer_id()?).into_bytes(),
        )?)
    }

    /// Same retention identity as the harness's event_key("timers", task, operation).
    pub fn retention_key(&self) -> Result<acyclic_stream::IdempotencyKey, TimerError> {
        Ok(acyclic_stream::IdempotencyKey::new(
            blake3::hash(self.timer_id()?.as_bytes())
                .as_bytes()
                .to_vec(),
        )?)
    }
}

/// Retain a scheduling request in the existing timer source stream.
pub async fn schedule<P: acyclic_stream::StreamProvider>(
    provider: &P,
    timer: &TimerRecord,
) -> Result<acyclic_stream::AppendOutcome, TimerError> {
    Ok(provider
        .append(acyclic_stream::AppendRequest {
            path: timer.stream_path()?,
            records: vec![timer.encode()?.into()],
            if_tail: None,
            idempotency_key: Some(timer.retention_key()?),
        })
        .await?)
}

/// Absolute-deadline alarm spelling of schedule; replacement uses a fresh operation ID.
pub use schedule as set_alarm;

/// Append a due timer to its admitted inbox. Clock and dispatch belong to the runtime.
/// Retrying uses the identical timer key and payload; no Invoke or waiting loop exists.
pub async fn fire<P: acyclic_stream::StreamProvider>(
    provider: &P,
    timer: &TimerRecord,
    inbox: acyclic_stream::StreamPath,
    now_unix_ms: u64,
) -> Result<Option<acyclic_stream::AppendOutcome>, TimerError> {
    timer.validate()?;
    if now_unix_ms < timer.deadline_unix_ms {
        return Ok(None);
    }
    Ok(Some(
        provider
            .append(acyclic_stream::AppendRequest {
                path: inbox,
                records: vec![timer.encode()?.into()],
                if_tail: None,
                idempotency_key: Some(timer.fire_key()?),
            })
            .await?,
    ))
}

/// Generate the shared timer record authoring shape from the same Rust declaration.
pub fn typescript() -> String {
    let config = Config::default();
    let output = format!(
        "// Generated from acyclic_actors::timers. Do not edit.\nexport const TIMER_STREAM_PREFIX = {TIMER_STREAM_PREFIX:?};\nexport {}\n",
        <TimerRecord as TS>::decl(&config)
    );
    output
        .lines()
        .map(str::trim_end)
        .collect::<Vec<_>>()
        .join("\n")
        + "\n"
}
