//! Settled command outcomes, their process exit codes and their bounded
//! result encoding.
use crate::{Error, Result};
pub use acyclic_harness_core::HeadlessOutcome;
use acyclic_harness_core::{ErrorClassification, OperationId};
use serde::{Deserialize, Serialize};
use serde_json::Value;

struct Diagnostic<'a>(&'a Error);

impl Serialize for Diagnostic<'_> {
    fn serialize<S: serde::Serializer>(
        &self,
        serializer: S,
    ) -> std::result::Result<S::Ok, S::Error> {
        serializer.collect_str(self.0)
    }
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Envelope {
    version: u8,
    sequence: u64,
    operation: OperationId,
    completion: HeadlessOutcome,
    #[serde(default, deserialize_with = "present_value")]
    ok: Option<Value>,
    error: Option<ErrorClassification>,
    diagnostic: Option<String>,
    #[serde(default)]
    output_omitted: bool,
}

// JSON null is a successful value, distinct from an absent result field.
fn present_value<'de, D: serde::Deserializer<'de>>(
    decoder: D,
) -> std::result::Result<Option<Value>, D::Error> {
    Value::deserialize(decoder).map(Some)
}

/// Admit one original request's terminal projection. Malformed, mismatched and
/// incomplete replies cannot become successful results or replace its identity.
pub fn decode_command_result(operation: OperationId, sequence: u64, bytes: &[u8]) -> Result<Value> {
    let unavailable = |cause| Error::reply_unavailable(operation, cause);
    let envelope: Envelope = serde_json::from_slice(bytes)
        .map_err(|error| unavailable(Error::Invalid(error.to_string())))?;
    if envelope.version != 1 || envelope.operation != operation || envelope.sequence != sequence {
        return Err(unavailable(Error::Invalid(
            "command result identity or version mismatch".into(),
        )));
    }
    match envelope {
        Envelope {
            completion: HeadlessOutcome::Completed,
            ok: Some(value),
            error: None,
            diagnostic: None,
            output_omitted: false,
            ..
        } => Ok(value),
        Envelope {
            completion: HeadlessOutcome::Completed,
            ok: None,
            error: None,
            diagnostic: None,
            output_omitted: true,
            ..
        } => Err(Error::OutputOmitted(operation)),
        Envelope {
            completion,
            ok: None,
            error: Some(classification),
            diagnostic,
            output_omitted,
            ..
        } if diagnostic.is_some() != output_omitted => {
            let error = classification
                .into_error(operation, diagnostic)
                .map_err(unavailable)?;
            if HeadlessOutcome::from_error(&error) != completion {
                return Err(unavailable(Error::Invalid(
                    "command result outcome mismatch".into(),
                )));
            }
            Err(error)
        }
        _ => Err(unavailable(Error::Invalid(
            "command result has no single terminal outcome".into(),
        ))),
    }
}

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
    operation: OperationId,
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
    let classification = result.as_ref().err().map(|error| error.classification());
    let encode = |output: &mut Output, omitted: bool| -> serde_json::Result<()> {
        let mut serializer = serde_json::Serializer::new(output);
        let mut map = serializer.serialize_map(None)?;
        map.serialize_entry("version", &1)?;
        map.serialize_entry("sequence", &sequence)?;
        map.serialize_entry("operation", &operation)?;
        map.serialize_entry("completion", &completion)?;
        if let Some(classification) = classification {
            map.serialize_entry("error", &classification)?;
        }
        if omitted {
            map.serialize_entry("output_omitted", &true)?;
        } else {
            match result {
                Ok(value) => map.serialize_entry("ok", value)?,
                Err(error) => map.serialize_entry("diagnostic", &Diagnostic(error))?,
            }
        }
        map.end()
    };
    let encoded = encode(&mut output, false);
    if output.overflow {
        output.bytes.clear();
        output.overflow = false;
        encode(&mut output, true).map_err(codec)?;
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
                #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
                (
                    Error::NativeBoundary {
                        code: acyclic_harness_core::wire::ErrorCode::Invalid,
                        reason: "original native boundary failure",
                    },
                    [("failed", 1), ("indeterminate", 4), ("failed", 1)],
                ),
                #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
                (
                    Error::NativeJson {
                        code: acyclic_harness_core::wire::ErrorCode::Invalid,
                        source: serde_json::from_slice::<Value>(b"{").unwrap_err(),
                        custody: None,
                    },
                    [("failed", 1), ("indeterminate", 4), ("failed", 1)],
                ),
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
                    // An interaction refusal is settled; uncertain cleanup takes precedence.
                    [("cancelled", 2), ("indeterminate", 4), ("failed", 1)],
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
                    let operation = OperationId::from_bytes([3; 16]);
                    let full = encode_command_result(3, operation, &result, u64::MAX)?;
                    let mut omitted: Value = serde_json::from_slice(&full)
                        .map_err(|error| Error::Invalid(error.to_string()))?;
                    omitted.as_object_mut().unwrap().remove("diagnostic");
                    omitted["output_omitted"] = Value::Bool(true);
                    let header = serde_json::to_vec(&omitted)
                        .map_err(|error| Error::Invalid(error.to_string()))?
                        .len() as u64;
                    // Exercise both the full and omitted envelopes through the public encoder.
                    for maximum in [header, u64::MAX] {
                        let bytes = encode_command_result(3, operation, &result, maximum)?;
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
                        if depth > 0 && maximum == header {
                            assert_eq!(value["output_omitted"], true);
                        } else if maximum == u64::MAX {
                            assert!(value["diagnostic"].is_string());
                        }
                        assert!(value.get("ok").is_none());
                        let mut received = decode_command_result(operation, 3, &bytes).unwrap_err();
                        assert_eq!(
                            received.classification(),
                            result.as_ref().unwrap_err().classification(),
                            "cleanup receiver must preserve every original semantic fact",
                        );
                        for _ in 0..2 {
                            received = Error::after_cleanup::<()>(
                                Err(received),
                                Err(Error::Conflict("retirement refused".into())),
                            )
                            .unwrap_err();
                            assert_eq!(
                                received.request_operation_id(),
                                Some(operation),
                                "cleanup must retain the original request identity"
                            );
                            assert_eq!(
                                received.operation_id(),
                                result.as_ref().unwrap_err().operation_id(),
                                "cleanup must retain the distinct uncertain effect identity"
                            );
                        }
                    }
                    result = Error::after_cleanup(result, Err(Error::Conflict("x".repeat(1024))));
                }
            }
        }
        Ok(())
    }
}
