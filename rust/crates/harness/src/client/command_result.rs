//! Settled command outcomes, their process exit codes and lossless result encoding.
use crate::{Error, Result, wire};
use serde_json::Value;
#[cfg(test)]
use serde_json::json;

/// Settled task outcome with stable process exit codes.
#[derive(Clone, Copy, Debug, Eq, PartialEq, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "snake_case")]
#[cfg_attr(feature = "wasm", derive(tsify::Tsify))]
pub enum HeadlessOutcome {
    /// The task completed, or is idle awaiting another prompt.
    Completed,
    /// Execution failed.
    Failed,
    /// Cancellation settled.
    Cancelled,
    /// Execution needs external input or approval.
    Blocked,
    /// The service cannot establish whether an operation completed.
    Indeterminate,
}

impl HeadlessOutcome {
    fn from_error(error: &Error) -> Self {
        if matches!(error, Error::GoalBlocked(_)) {
            return Self::Blocked;
        }
        match error.code() {
            wire::ErrorCode::Indeterminate => Self::Indeterminate,
            wire::ErrorCode::InteractionCancelled | wire::ErrorCode::Cancelled => Self::Cancelled,
            wire::ErrorCode::InteractionDeclined
            | wire::ErrorCode::InteractionExpired
            | wire::ErrorCode::InteractionDenied => Self::Blocked,
            wire::ErrorCode::Unspecified
            | wire::ErrorCode::NotFound
            | wire::ErrorCode::Conflict
            | wire::ErrorCode::Unsupported
            | wire::ErrorCode::Invalid
            | wire::ErrorCode::Unauthorized
            | wire::ErrorCode::Unavailable
            | wire::ErrorCode::Storage => Self::Failed,
        }
    }

    fn of(result: &Result<Value>) -> Self {
        result
            .as_ref()
            .map_or_else(Self::from_error, |_| Self::Completed)
    }

    /// Stable CLI contract: completed=0, failed=1, cancelled=2, blocked=3, indeterminate=4.
    #[must_use]
    pub const fn exit_code(self) -> u8 {
        match self {
            Self::Completed => 0,
            Self::Failed => 1,
            Self::Cancelled => 2,
            Self::Blocked => 3,
            Self::Indeterminate => 4,
        }
    }
}

/// Encode the complete settled outcome for command files and live query projections.
pub fn encode_command_result(sequence: u64, result: &Result<Value>) -> Result<Vec<u8>> {
    use serde::{Serializer as _, ser::SerializeMap as _};
    let codec = |error: serde_json::Error| Error::Invalid(error.to_string());
    let mut output = Vec::new();
    let completion = HeadlessOutcome::of(result);
    let encoded = (|| -> serde_json::Result<()> {
        let mut serializer = serde_json::Serializer::new(&mut output);
        let mut map = serializer.serialize_map(Some(3))?;
        map.serialize_entry("sequence", &sequence)?;
        map.serialize_entry("completion", &completion)?;
        match result {
            Ok(value) => map.serialize_entry("ok", value)?,
            Err(error) => map.serialize_entry("error", &error.to_string())?,
        }
        map.end()
    })();
    encoded.map_err(codec)?;
    Ok(output)
}

#[cfg(test)]
mod tests {
    use super::*;

    // Preparation and history surface a fired cancellation as `Error::Cancelled`;
    // headless clients must settle it as cancelled, never as a failure.
    #[test]
    fn cancellation_settles_as_cancelled_and_storage_failure_as_failed() {
        assert_eq!(
            HeadlessOutcome::from_error(&Error::Cancelled),
            HeadlessOutcome::Cancelled
        );
        assert_eq!(
            HeadlessOutcome::from_error(&Error::Storage("provider write failed".into())),
            HeadlessOutcome::Failed
        );
    }

    #[test]
    fn command_results_preserve_escaped_unicode_output() -> Result<()> {
        for stdout in ["x".repeat(256), "\"\n".repeat(128), "λ".repeat(128)] {
            let bytes = encode_command_result(3, &Ok(json!({"stdout": &stdout})))?;
            let value: Value =
                serde_json::from_slice(&bytes).map_err(|e| Error::Invalid(e.to_string()))?;
            assert_eq!(value["sequence"], 3);
            assert_eq!(value["ok"]["stdout"], stdout);
            assert_eq!(value["completion"], "completed");
        }
        Ok(())
    }
}
