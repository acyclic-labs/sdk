//! Bounded canonical protobuf codecs for retained Stream v2 facts.
//!
//! These bytes are the existing public messages, without a private container.
//! The storage owner supplies checksums, account binding and durable publication;
//! decoding does not establish authenticity, request-digest binding or acceptance.
use crate::{
    AppendOutcome, CommitConflict, CommitId, CommitOutcome, CommittedEnvelope, CommittedMutation,
    IdempotencyObservation, IdempotencyOutcome, MAX_ITEMS, MAX_RECORD_BYTES, Record, StreamError,
    StreamPath, wire, wire_codec,
};
use prost::Message;

/// Encodes a complete terminal retry observation, including conflict outcomes.
/// # Errors
/// Rejects malformed facts, zero bounds and output exceeding `maximum_bytes`.
pub fn encode_observation(
    value: &IdempotencyObservation,
    maximum_bytes: usize,
) -> Result<Vec<u8>, StreamError> {
    validate_observation(value)?;
    encode(&wire_codec::observation_wire(value.clone()), maximum_bytes)
}

/// Decodes canonical bytes for one complete terminal retry observation.
/// # Errors
/// Rejects malformed/noncanonical bytes, invalid facts and exceeded bounds.
pub fn decode_observation(
    input: &[u8],
    maximum_bytes: usize,
) -> Result<IdempotencyObservation, StreamError> {
    let value = wire_codec::observation_from_wire(decode::<wire::IdempotencyObservation>(
        input,
        maximum_bytes,
    )?)
    .map_err(|_| StreamError::InvalidArgument)?;
    validate_observation(&value)?;
    Ok(value)
}

/// Encodes a complete immutable successful envelope, including record facts.
/// # Errors
/// Rejects malformed facts, zero bounds and output exceeding `maximum_bytes`.
pub fn encode_envelope(
    value: &CommittedEnvelope,
    maximum_bytes: usize,
) -> Result<Vec<u8>, StreamError> {
    validate_envelope(value)?;
    encode(&wire_codec::envelope_wire(value.clone()), maximum_bytes)
}

/// Decodes canonical bytes for one complete immutable successful envelope.
/// # Errors
/// Rejects malformed/noncanonical bytes, invalid facts and exceeded bounds.
pub fn decode_envelope(
    input: &[u8],
    maximum_bytes: usize,
) -> Result<CommittedEnvelope, StreamError> {
    let value =
        wire_codec::envelope_from_wire(decode::<wire::CommittedEnvelope>(input, maximum_bytes)?)
            .map_err(|_| StreamError::InvalidArgument)?;
    validate_envelope(&value)?;
    Ok(value)
}

fn bound(length: usize, maximum: usize) -> Result<(), StreamError> {
    if maximum == 0 || length > maximum {
        return Err(StreamError::LimitExceeded);
    }
    Ok(())
}
fn encode<T: Message>(value: &T, maximum: usize) -> Result<Vec<u8>, StreamError> {
    bound(value.encoded_len(), maximum)?;
    Ok(value.encode_to_vec())
}
fn decode<T: Message + Default>(input: &[u8], maximum: usize) -> Result<T, StreamError> {
    bound(input.len(), maximum)?;
    let value = T::decode(input).map_err(|_| StreamError::InvalidArgument)?;
    // Persistence fails closed on unknown fields, duplicate fields and alternate
    // protobuf encodings instead of silently dropping durable information.
    if value.encode_to_vec() != input {
        return Err(StreamError::InvalidArgument);
    }
    Ok(value)
}

fn validate_observation(value: &IdempotencyObservation) -> Result<(), StreamError> {
    match &value.outcome {
        IdempotencyOutcome::Append(AppendOutcome::Committed(receipt)) => {
            if receipt.start >= receipt.end
                || receipt.end != receipt.tail
                || receipt.end - receipt.start > MAX_ITEMS as u64
            {
                return Err(StreamError::InvalidArgument);
            }
        }
        IdempotencyOutcome::Fork(receipt) => {
            if receipt.source == receipt.destination || receipt.forked_at != receipt.tail {
                return Err(StreamError::InvalidArgument);
            }
        }
        IdempotencyOutcome::Commit(CommitOutcome::Committed(envelope)) => {
            validate_envelope(envelope)?;
        }
        IdempotencyOutcome::Commit(CommitOutcome::Conflict(conflicts)) => {
            if conflicts.is_empty() || conflicts.len() > MAX_ITEMS {
                return Err(StreamError::InvalidArgument);
            }
            let mut previous = None;
            for conflict in conflicts {
                let path = match conflict {
                    CommitConflict::Tail {
                        path,
                        expected,
                        actual,
                    } => {
                        if *actual == Some(*expected) {
                            return Err(StreamError::InvalidArgument);
                        }
                        path
                    }
                    CommitConflict::Exists { path } => path,
                };
                if previous.is_some_and(|previous| previous >= path) {
                    return Err(StreamError::InvalidArgument);
                }
                previous = Some(path);
            }
        }
        IdempotencyOutcome::Append(AppendOutcome::TailConflict { .. }) => {}
    }
    Ok(())
}

fn validate_envelope(value: &CommittedEnvelope) -> Result<(), StreamError> {
    if value.mutations.is_empty() || value.mutations.len() > MAX_ITEMS {
        return Err(StreamError::InvalidArgument);
    }
    let mut previous: Option<&StreamPath> = None;
    let mut time = None;
    for mutation in &value.mutations {
        let (path, start, end, records) = match mutation {
            CommittedMutation::Append(append) => {
                if append.start >= append.end || append.end != append.tail {
                    return Err(StreamError::InvalidArgument);
                }
                (&append.path, append.start, append.end, &append.records)
            }
            CommittedMutation::Fork(fork) => {
                if fork.source == fork.destination || fork.forked_at > fork.tail {
                    return Err(StreamError::InvalidArgument);
                }
                (&fork.destination, fork.forked_at, fork.tail, &fork.records)
            }
        };
        if previous.is_some_and(|previous| previous >= path) {
            return Err(StreamError::InvalidArgument);
        }
        previous = Some(path);
        validate_records(records, start, end, value.commit_id, &mut time)?;
    }
    Ok(())
}

fn validate_records(
    records: &[Record],
    start: u64,
    end: u64,
    commit_id: CommitId,
    time: &mut Option<u64>,
) -> Result<(), StreamError> {
    if records.len() > MAX_ITEMS || end - start != records.len() as u64 {
        return Err(StreamError::InvalidArgument);
    }
    for (index, record) in records.iter().enumerate() {
        if record.sequence != start + index as u64
            || record.commit_id != commit_id
            || record.value.len() > MAX_RECORD_BYTES
            || time.is_some_and(|time| time != record.committed_at_micros)
        {
            return Err(StreamError::InvalidArgument);
        }
        *time = Some(record.committed_at_micros);
    }
    Ok(())
}

#[cfg(test)]
mod tests;
