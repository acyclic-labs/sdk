//! Protobuf JSON conversion over the caller's canonical descriptor.
//!
//! Callers retain descriptor ownership, transport limits, typed error mapping,
//! semantic validation and request/response identity checks.

use prost::Message;
use prost_reflect::{DynamicMessage, MessageDescriptor};

/// Conversion stage, mapped into the original caller's error vocabulary.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Error {
    /// The complete JSON input exceeds the caller's admitted byte limit.
    Limit,
    /// Generated message bytes do not decode through their owning descriptor.
    Wire,
    /// JSON fields or values contradict the descriptor.
    Json,
    /// A second JSON value or non-whitespace bytes follow the message.
    Trailing,
}

/// Project generated binary bytes through their actual Protobuf JSON descriptor.
///
/// # Errors
/// Returns the original conversion stage; does not perform transport admission.
pub fn encode(descriptor: MessageDescriptor, bytes: &[u8]) -> Result<Vec<u8>, Error> {
    let message = DynamicMessage::decode(descriptor, bytes).map_err(|_| Error::Wire)?;
    serde_json::to_vec(&message).map_err(|_| Error::Json)
}

/// Decode one complete bounded JSON message through its actual descriptor.
///
/// # Errors
/// Refuses oversize input before parsing, unknown fields, malformed values and
/// trailing input. It does not admit a service operation or validate its result.
pub fn decode(
    descriptor: MessageDescriptor,
    bytes: &[u8],
    maximum: usize,
) -> Result<DynamicMessage, Error> {
    if bytes.len() > maximum {
        return Err(Error::Limit);
    }
    let mut json = serde_json::Deserializer::from_slice(bytes);
    let message =
        DynamicMessage::deserialize(descriptor, &mut json).map_err(|_| Error::Json)?;
    json.end().map_err(|_| Error::Trailing)?;
    Ok(message)
}
