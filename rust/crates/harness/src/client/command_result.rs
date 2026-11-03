//! Settled command outcomes, their process exit codes and their bounded
//! result encoding.
use crate::{Error, Result};
pub use acyclic_harness_core::HeadlessOutcome;
use serde_json::{Value, json};

// Stop the serializer at the encoded bound, including JSON escaping and wrapper
// fields. Overflow still publishes a terminal outcome; it cannot strand a claim.
struct Output {
    bytes: Vec<u8>,
    maximum: u64,
    overflow: bool,
}

impl std::io::Write for Output {
    fn write(&mut self, bytes: &[u8]) -> std::io::Result<usize> {
        if (self.bytes.len() as u64)
            .checked_add(bytes.len() as u64)
            .is_none_or(|n| n > self.maximum)
        {
            self.overflow = true;
            return Err(std::io::ErrorKind::FileTooLarge.into());
        }
        self.bytes.extend_from_slice(bytes);
        Ok(bytes.len())
    }
    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}

/// Encode a bounded outcome for command files or a live query projection.
/// Both use the same terminal omission envelope when the encoded result is too large.
pub fn encode_command_result(
    sequence: u64,
    result: &Result<Value>,
    maximum: u64,
) -> Result<Vec<u8>> {
    use serde::{Serializer as _, ser::SerializeMap as _};
    let codec = |error: serde_json::Error| Error::Invalid(error.to_string());
    let mut output = Output {
        bytes: Vec::new(),
        maximum,
        overflow: false,
    };
    let completion = result
        .as_ref()
        .map_or_else(HeadlessOutcome::from_error, |_| HeadlessOutcome::Completed);
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
    if output.overflow {
        output.bytes.clear();
        output.overflow = false;
        serde_json::to_writer(
            &mut output,
            &json!({"sequence":sequence,"output_omitted":true,
                "completion":completion}),
        )
        .map_err(codec)?;
    } else {
        encoded.map_err(codec)?;
    }
    Ok(output.bytes)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn cleanup_preserves_encoded_terminal_outcomes() -> Result<()> {
        use acyclic_harness_core::{InteractionRejection, OperationId, goal::GoalBlockedReason};

        for cleanup in 0..3 {
            for (original, outcomes) in [
                (
                    Error::GoalBlocked(GoalBlockedReason::Declared),
                    [("blocked", 3), ("indeterminate", 4), ("failed", 1)],
                ),
                (
                    Error::GoalBlocked(GoalBlockedReason::TaskViewUnreachable),
                    [("blocked", 3), ("indeterminate", 4), ("failed", 1)],
                ),
                (
                    Error::InteractionRejected(InteractionRejection::Denied),
                    [("blocked", 3), ("indeterminate", 4), ("failed", 1)],
                ),
                (
                    Error::InteractionRejected(InteractionRejection::Declined),
                    [("blocked", 3), ("indeterminate", 4), ("failed", 1)],
                ),
                (
                    Error::InteractionRejected(InteractionRejection::Expired),
                    [("blocked", 3), ("indeterminate", 4), ("failed", 1)],
                ),
                (
                    Error::InteractionRejected(InteractionRejection::Cancelled),
                    [("cancelled", 2); 3],
                ),
                (Error::Cancelled, [("cancelled", 2); 3]),
                (
                    Error::Unsupported("missing capability".into()),
                    [("failed", 1), ("indeterminate", 4), ("failed", 1)],
                ),
                (
                    Error::Storage("provider write failed".into()),
                    [("failed", 1); 3],
                ),
                (
                    Error::Indeterminate(OperationId::from_bytes([7; 16])),
                    [("indeterminate", 4); 3],
                ),
            ] {
                let (terminal, exit) = outcomes[cleanup];
                let cleanup = match cleanup {
                    1 => Error::Indeterminate(OperationId::from_bytes([8; 16])),
                    2 => Error::Storage("retirement write failed".into()),
                    _ => Error::Conflict("retirement failed".into()),
                };
                let mut result = Error::after_cleanup::<Value>(Err(original), Err(cleanup));
                for depth in 0..3 {
                    // Exercise both the full and omitted envelopes through the public encoder.
                    for maximum in [96, 8192] {
                        let bytes = encode_command_result(3, &result, maximum)?;
                        let value: Value = serde_json::from_slice(&bytes)
                            .map_err(|e| Error::Invalid(e.to_string()))?;
                        assert_eq!(value["sequence"], 3);
                        assert_eq!(
                            value["completion"], terminal,
                            "cleanup must retain the original terminal outcome"
                        );
                        assert_eq!(
                            HeadlessOutcome::from_error(result.as_ref().unwrap_err()).exit_code(),
                            exit
                        );
                        if depth > 0 && maximum == 96 {
                            assert_eq!(value["output_omitted"], true);
                        } else if maximum == 8192 {
                            assert!(value["error"].is_string());
                        }
                        assert!(value.get("ok").is_none());
                    }
                    result = Error::after_cleanup(result, Err(Error::Conflict("x".repeat(1024))));
                }
            }
        }
        Ok(())
    }

    #[test]
    fn an_oversized_result_settles_as_an_omitted_terminal_outcome() -> Result<()> {
        for stdout in ["x".repeat(256), "\"\n".repeat(128), "λ".repeat(128)] {
            let bytes = encode_command_result(3, &Ok(json!({"stdout": stdout})), 64)?;
            assert!(bytes.len() <= 64);
            let value: Value =
                serde_json::from_slice(&bytes).map_err(|e| Error::Invalid(e.to_string()))?;
            assert_eq!(value["sequence"], 3);
            assert_eq!(value["output_omitted"], true);
            assert_eq!(value["completion"], "completed");
        }
        Ok(())
    }
}
