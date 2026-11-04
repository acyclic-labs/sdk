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
