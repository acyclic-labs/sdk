//! Bounded canonical protobuf codecs for retained Stream v1 facts.
use crate::wire_codec;
use acyclic_stream_contract_core::request::validation::{validate_envelope, validate_observation};
use acyclic_stream_contract_core::{CommittedEnvelope, IdempotencyObservation, StreamError};
use acyclic_stream_wire::wire;
use prost::Message;

/// Fixed header of a retained idempotency observation.
pub const OBSERVATION_MAGIC: &[u8] = b"ACYCLIC-STREAM-OBSERVATION-V1\0";
/// Fixed header of a retained committed envelope.
pub const ENVELOPE_MAGIC: &[u8] = b"ACYCLIC-STREAM-ENVELOPE-V1\0";
/// Domain prefix an observation's version follows.
pub const OBSERVATION_DOMAIN: &[u8] = b"ACYCLIC-STREAM-OBSERVATION-V";
/// Domain prefix an envelope's version follows.
pub const ENVELOPE_DOMAIN: &[u8] = b"ACYCLIC-STREAM-ENVELOPE-V";
/// The only retained fact version.
pub const FACT_VERSION: &[u8] = b"1\0";

/// Admit a binary format's domain and version before interpreting its payload.
///
/// The returned slice borrows the input. Checking fixed headers allocates and
/// copies nothing, and takes constant work independent of the payload length.
///
/// # Errors
/// Returns [`StreamError::InvalidArgument`] for another domain or a header
/// truncated before its version ends, and [`StreamError::Unsupported`] for an
/// old or unknown version.
pub fn header<'a>(
    encoded: &'a [u8],
    domain: &[u8],
    version: &[u8],
) -> Result<&'a [u8], StreamError> {
    acyclic_auth::format::header(
        encoded,
        domain,
        version,
        || StreamError::InvalidArgument,
        || StreamError::Unsupported,
    )
}

/// Encodes a complete terminal retry observation, including conflict outcomes.
pub fn encode_observation(
    value: &IdempotencyObservation,
    maximum_bytes: usize,
) -> Result<Vec<u8>, StreamError> {
    validate_observation(value)?;
    encode(
        &wire_codec::observation_wire(value.clone()),
        maximum_bytes,
        OBSERVATION_MAGIC,
    )
}

/// Decodes canonical bytes for one complete terminal retry observation.
pub fn decode_observation(
    input: &[u8],
    maximum_bytes: usize,
) -> Result<IdempotencyObservation, StreamError> {
    observation_from_wire(decode::<wire::IdempotencyObservation>(
        header(input, OBSERVATION_DOMAIN, FACT_VERSION)?,
        maximum_bytes.saturating_sub(OBSERVATION_MAGIC.len()),
    )?)
}

/// Admits a decoded observation before it is retained.
pub fn observation_from_wire(
    value: wire::IdempotencyObservation,
) -> Result<IdempotencyObservation, StreamError> {
    let value =
        wire_codec::observation_from_wire(value).map_err(|_| StreamError::InvalidArgument)?;
    validate_observation(&value)?;
    Ok(value)
}

/// Encodes a complete immutable successful envelope, including record facts.
pub fn encode_envelope(
    value: &CommittedEnvelope,
    maximum_bytes: usize,
) -> Result<Vec<u8>, StreamError> {
    validate_envelope(value)?;
    encode(
        &wire_codec::envelope_to_wire(value.clone()),
        maximum_bytes,
        ENVELOPE_MAGIC,
    )
}

/// Decodes canonical bytes for one complete immutable successful envelope.
pub fn decode_envelope(
    input: &[u8],
    maximum_bytes: usize,
) -> Result<CommittedEnvelope, StreamError> {
    envelope_from_wire(decode::<wire::CommittedEnvelope>(
        header(input, ENVELOPE_DOMAIN, FACT_VERSION)?,
        maximum_bytes.saturating_sub(ENVELOPE_MAGIC.len()),
    )?)
}

/// Admits a decoded envelope before it is retained.
pub fn envelope_from_wire(
    value: wire::CommittedEnvelope,
) -> Result<CommittedEnvelope, StreamError> {
    let value = wire_codec::envelope_from_wire(value).map_err(|_| StreamError::InvalidArgument)?;
    validate_envelope(&value)?;
    Ok(value)
}

fn bound(length: usize, maximum: usize) -> Result<(), StreamError> {
    if maximum == 0 || length > maximum {
        return Err(StreamError::LimitExceeded);
    }
    Ok(())
}
/// Encodes `value` behind `magic`, refusing more than `maximum` bytes.
pub fn encode<T: Message>(value: &T, maximum: usize, magic: &[u8]) -> Result<Vec<u8>, StreamError> {
    let length = magic
        .len()
        .checked_add(value.encoded_len())
        .ok_or(StreamError::LimitExceeded)?;
    bound(length, maximum)?;
    let mut output = Vec::with_capacity(length);
    output.extend_from_slice(magic);
    value.encode_raw(&mut output);
    Ok(output)
}
/// Decodes exactly `input`, refusing unknown or duplicate fields.
pub fn decode<T: Message + Default>(input: &[u8], maximum: usize) -> Result<T, StreamError> {
    bound(input.len(), maximum)?;
    let value = T::decode(input).map_err(|_| StreamError::InvalidArgument)?;
    // Persistence fails closed on unknown fields, duplicate fields and alternate
    if value.encode_to_vec() != input {
        return Err(StreamError::InvalidArgument);
    }
    Ok(value)
}
