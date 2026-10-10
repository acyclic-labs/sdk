//! Shared domain/wire conversion for every Stream transport and persistence adapter.

use bytes::Bytes;

use crate::{
    AppendOutcome, AppendReceipt, CommitCondition, CommitConflict, CommitId, CommitMutation,
    CommitOutcome, CommittedAppend, CommittedEnvelope, CommittedFork, CommittedMutation,
    ForkReceipt, IdempotencyKey, IdempotencyObservation, IdempotencyOutcome, Record, StreamError,
    StreamPath, wire,
};

pub(crate) fn path(value: String) -> Result<StreamPath, StreamError> {
    StreamPath::new(value)
}

pub(crate) fn optional_key(value: Option<Bytes>) -> Result<Option<IdempotencyKey>, StreamError> {
    value.map(IdempotencyKey::new).transpose()
}

#[cfg(feature = "grpc")]
/// Converts an optional wire idempotency key, rejecting an omitted key.
pub fn required_key(value: Option<Bytes>) -> Result<IdempotencyKey, StreamError> {
    value
        .ok_or(StreamError::InvalidArgument)
        .and_then(IdempotencyKey::new)
}

pub(crate) fn condition_wire(value: CommitCondition) -> wire::CommitCondition {
    let condition = match value {
        CommitCondition::Tail { path, expected } => {
            wire::commit_condition::Condition::Tail(wire::TailCondition {
                path: path.to_string(),
                expected,
            })
        }
        CommitCondition::Absent { path } => {
            wire::commit_condition::Condition::Absent(wire::AbsentCondition {
                path: path.to_string(),
            })
        }
    };
    wire::CommitCondition {
        condition: Some(condition),
    }
}

pub(crate) fn condition_from_wire(
    value: wire::CommitCondition,
) -> Result<CommitCondition, StreamError> {
    match value.condition.ok_or(StreamError::InvalidArgument)? {
        wire::commit_condition::Condition::Tail(value) => Ok(CommitCondition::Tail {
            path: path(value.path)?,
            expected: value.expected,
        }),
        wire::commit_condition::Condition::Absent(value) => Ok(CommitCondition::Absent {
            path: path(value.path)?,
        }),
    }
}

pub(crate) fn mutation_wire(value: CommitMutation) -> wire::CommitMutation {
    let mutation = match value {
        CommitMutation::Append { path, records } => {
            wire::commit_mutation::Mutation::Append(wire::AppendMutation {
                path: path.to_string(),
                records,
            })
        }
        CommitMutation::Fork {
            source,
            destination,
            at_tail,
            records,
        } => wire::commit_mutation::Mutation::Fork(wire::ForkMutation {
            source: source.to_string(),
            destination: destination.to_string(),
            at_tail,
            records,
        }),
    };
    wire::CommitMutation {
        mutation: Some(mutation),
    }
}

pub(crate) fn mutation_from_wire(
    value: wire::CommitMutation,
) -> Result<CommitMutation, StreamError> {
    match value.mutation.ok_or(StreamError::InvalidArgument)? {
        wire::commit_mutation::Mutation::Append(value) => Ok(CommitMutation::Append {
            path: path(value.path)?,
            records: value.records,
        }),
        wire::commit_mutation::Mutation::Fork(value) => Ok(CommitMutation::Fork {
            source: path(value.source)?,
            destination: path(value.destination)?,
            at_tail: value.at_tail,
            records: value.records,
        }),
    }
}

pub(crate) fn observation_from_wire(
    value: wire::IdempotencyObservation,
) -> Result<IdempotencyObservation, StreamError> {
    observation_with_path(value, &path)
}

pub(crate) fn observation_with_path(
    value: wire::IdempotencyObservation,
    decode_path: &impl Fn(String) -> Result<StreamPath, StreamError>,
) -> Result<IdempotencyObservation, StreamError> {
    let request_digest = <[u8; 32]>::try_from(value.request_digest.as_ref())
        .map_err(|_| StreamError::Unavailable)?;
    let outcome = match value.outcome.ok_or(StreamError::Unavailable)? {
        wire::idempotency_observation::Outcome::Append(value) => {
            IdempotencyOutcome::Append(append_outcome_from_wire(value)?)
        }
        wire::idempotency_observation::Outcome::Fork(value) => {
            IdempotencyOutcome::Fork(ForkReceipt {
                source: decode_path(value.source)?,
                destination: decode_path(value.destination)?,
                forked_at: value.forked_at,
                tail: value.tail,
                commit_id: commit_id(&value.commit_id)?,
            })
        }
        wire::idempotency_observation::Outcome::Commit(value) => {
            IdempotencyOutcome::Commit(commit_outcome_with_path(value, decode_path)?)
        }
    };
    Ok(IdempotencyObservation {
        idempotency_key: IdempotencyKey::new(value.idempotency_key)?,
        request_digest,
        outcome,
    })
}

/// Converts an idempotency observation into its canonical wire representation.
pub fn observation_wire(value: IdempotencyObservation) -> wire::IdempotencyObservation {
    let outcome = match value.outcome {
        IdempotencyOutcome::Append(value) => {
            wire::idempotency_observation::Outcome::Append(append_outcome_wire(value))
        }
        IdempotencyOutcome::Fork(value) => {
            wire::idempotency_observation::Outcome::Fork(fork_receipt_wire(&value))
        }
        IdempotencyOutcome::Commit(value) => {
            wire::idempotency_observation::Outcome::Commit(commit_outcome_wire(value))
        }
    };
    wire::IdempotencyObservation {
        idempotency_key: Bytes::copy_from_slice(value.idempotency_key.as_bytes()),
        request_digest: Bytes::copy_from_slice(&value.request_digest),
        outcome: Some(outcome),
    }
}

pub(crate) fn append_outcome_from_wire(
    value: wire::AppendResponse,
) -> Result<AppendOutcome, StreamError> {
    match value.outcome.ok_or(StreamError::Unavailable)? {
        wire::append_response::Outcome::Committed(receipt) => {
            Ok(AppendOutcome::Committed(append_receipt(&receipt)?))
        }
        wire::append_response::Outcome::Conflict(conflict) => Ok(AppendOutcome::TailConflict {
            actual_tail: conflict.actual_tail,
        }),
    }
}

pub(crate) fn append_outcome_wire(value: AppendOutcome) -> wire::AppendResponse {
    let outcome = match value {
        AppendOutcome::Committed(receipt) => {
            wire::append_response::Outcome::Committed(append_receipt_wire(&receipt))
        }
        AppendOutcome::TailConflict { actual_tail } => {
            wire::append_response::Outcome::Conflict(wire::TailConflict { actual_tail })
        }
    };
    wire::AppendResponse {
        outcome: Some(outcome),
    }
}

#[cfg(all(feature = "grpc", not(target_arch = "wasm32")))]
pub(crate) fn commit_outcome_from_wire(
    value: wire::CommitResponse,
) -> Result<CommitOutcome, StreamError> {
    commit_outcome_with_path(value, &path)
}

fn commit_outcome_with_path(
    value: wire::CommitResponse,
    decode_path: &impl Fn(String) -> Result<StreamPath, StreamError>,
) -> Result<CommitOutcome, StreamError> {
    match value.outcome.ok_or(StreamError::Unavailable)? {
        wire::commit_response::Outcome::Committed(envelope) => Ok(CommitOutcome::Committed(
            envelope_with_path(envelope, decode_path)?,
        )),
        wire::commit_response::Outcome::Conflict(conflicts) => Ok(CommitOutcome::Conflict(
            conflicts
                .conflicts
                .into_iter()
                .map(|conflict| conflict_with_path(conflict, decode_path))
                .collect::<Result<_, _>>()?,
        )),
    }
}

pub(crate) fn commit_outcome_wire(value: CommitOutcome) -> wire::CommitResponse {
    let outcome = match value {
        CommitOutcome::Committed(envelope) => {
            wire::commit_response::Outcome::Committed(envelope_wire(envelope))
        }
        CommitOutcome::Conflict(conflicts) => {
            wire::commit_response::Outcome::Conflict(wire::CommitConflicts {
                conflicts: conflicts.into_iter().map(conflict_wire).collect(),
            })
        }
    };
    wire::CommitResponse {
        outcome: Some(outcome),
    }
}

/// Converts a wire commit-id byte sequence into the fixed-width domain id.
pub fn commit_id(value: &[u8]) -> Result<CommitId, StreamError> {
    let bytes = <[u8; 32]>::try_from(value).map_err(|_| StreamError::Unavailable)?;
    Ok(CommitId::from_bytes(bytes))
}

pub(crate) fn record(value: wire::Record) -> Result<Record, StreamError> {
    Ok(Record {
        sequence: value.sequence,
        value: value.value,
        commit_id: commit_id(&value.commit_id)?,
        committed_at_micros: value.committed_at_micros,
    })
}

pub(crate) fn append_receipt(value: &wire::AppendReceipt) -> Result<AppendReceipt, StreamError> {
    Ok(AppendReceipt {
        start: value.start,
        end: value.end,
        tail: value.tail,
        commit_id: commit_id(&value.commit_id)?,
    })
}

/// Converts a domain record into its canonical wire representation.
pub fn record_wire(value: Record) -> wire::Record {
    wire::Record {
        sequence: value.sequence,
        value: value.value,
        commit_id: Bytes::copy_from_slice(value.commit_id.as_bytes()),
        committed_at_micros: value.committed_at_micros,
    }
}

pub(crate) fn append_receipt_wire(value: &AppendReceipt) -> wire::AppendReceipt {
    wire::AppendReceipt {
        start: value.start,
        end: value.end,
        tail: value.tail,
        commit_id: Bytes::copy_from_slice(value.commit_id.as_bytes()),
    }
}

pub(crate) fn fork_receipt_wire(value: &ForkReceipt) -> wire::ForkReceipt {
    wire::ForkReceipt {
        source: value.source.to_string(),
        destination: value.destination.to_string(),
        forked_at: value.forked_at,
        tail: value.tail,
        commit_id: Bytes::copy_from_slice(value.commit_id.as_bytes()),
    }
}

/// Decodes an append request from its generated wire representation.
pub fn append_from_wire(value: wire::AppendRequest) -> Result<crate::AppendRequest, StreamError> {
    Ok(crate::AppendRequest {
        path: path(value.path)?,
        records: value.records,
        if_tail: value.if_tail,
        idempotency_key: optional_key(value.idempotency_key)?,
    })
}

/// Decodes a fork request from its generated wire representation.
pub fn fork_from_wire(value: wire::ForkRequest) -> Result<crate::ForkRequest, StreamError> {
    Ok(crate::ForkRequest {
        source: path(value.source)?,
        destination: path(value.destination)?,
        at_tail: value.at_tail,
        idempotency_key: optional_key(value.idempotency_key)?,
    })
}

/// Decodes a read request from its generated wire representation.
pub fn read_from_wire(value: wire::ReadRequest) -> Result<crate::ReadRequest, StreamError> {
    Ok(crate::ReadRequest {
        path: path(value.path)?,
        from: value.from,
        limit: value.limit,
    })
}

#[cfg(all(feature = "wasm", target_arch = "wasm32"))]
pub(crate) fn follow_from_wire(
    value: wire::FollowRequest,
) -> Result<(StreamPath, u64), StreamError> {
    Ok((path(value.path)?, value.from))
}

/// Decodes a children request from its generated wire representation.
pub fn children_from_wire(
    value: wire::ChildrenRequest,
) -> Result<crate::ChildrenRequest, StreamError> {
    Ok(crate::ChildrenRequest {
        parent: value.parent.map(path).transpose()?,
        limit: value.limit,
    })
}

/// Decodes a paginated children request from its generated wire representation.
pub fn children_page_from_wire(
    value: wire::ChildrenPageRequest,
) -> Result<crate::ChildrenPageRequest, StreamError> {
    Ok(crate::ChildrenPageRequest {
        parent: value.parent.map(path).transpose()?,
        after: value.after.map(path).transpose()?,
        hierarchy_version: value
            .hierarchy_version
            .map(|bytes| {
                let array: [u8; 32] = bytes
                    .as_ref()
                    .try_into()
                    .map_err(|_| StreamError::InvalidArgument)?;
                Ok(crate::CommitId::from_bytes(array))
            })
            .transpose()?,
        limit: value.limit,
    })
}

#[cfg(any(
    all(feature = "grpc", not(target_arch = "wasm32")),
    all(feature = "wasm", target_arch = "wasm32")
))]
/// Converts a domain children page into its generated wire representation.
pub fn children_page_to_wire(value: crate::ChildrenPage) -> wire::ChildrenPageResponse {
    wire::ChildrenPageResponse {
        hierarchy_version: Bytes::copy_from_slice(value.hierarchy_version.as_bytes()),
        children: value
            .children
            .into_iter()
            .map(|child| wire::Child {
                path: child.path.to_string(),
            })
            .collect(),
        next_after: value.next_after.map(|path| path.to_string()),
    }
}

/// Decodes a commit request from its generated wire representation.
pub fn commit_from_wire(value: wire::CommitRequest) -> Result<crate::CommitRequest, StreamError> {
    Ok(crate::CommitRequest {
        conditions: value
            .conditions
            .into_iter()
            .map(condition_from_wire)
            .collect::<Result<_, _>>()?,
        mutations: value
            .mutations
            .into_iter()
            .map(mutation_from_wire)
            .collect::<Result<_, _>>()?,
        idempotency_key: IdempotencyKey::new(value.idempotency_key)?,
    })
}

pub(crate) fn commit_to_wire(value: &crate::CommitRequest) -> wire::CommitRequest {
    wire::CommitRequest {
        conditions: value
            .conditions
            .iter()
            .cloned()
            .map(condition_wire)
            .collect(),
        mutations: value.mutations.iter().cloned().map(mutation_wire).collect(),
        idempotency_key: Bytes::copy_from_slice(value.idempotency_key.as_bytes()),
        deadline_unix_millis: None,
    }
}

#[cfg(any(
    all(feature = "grpc", not(target_arch = "wasm32")),
    all(feature = "wasm", target_arch = "wasm32")
))]
/// Converts an append outcome into its generated wire representation.
pub fn append_outcome_to_wire(value: crate::AppendOutcome) -> wire::AppendResponse {
    let outcome = match value {
        crate::AppendOutcome::Committed(receipt) => {
            wire::append_response::Outcome::Committed(wire::AppendReceipt {
                start: receipt.start,
                end: receipt.end,
                tail: receipt.tail,
                commit_id: Bytes::copy_from_slice(receipt.commit_id.as_bytes()),
            })
        }
        crate::AppendOutcome::TailConflict { actual_tail } => {
            wire::append_response::Outcome::Conflict(wire::TailConflict { actual_tail })
        }
    };
    wire::AppendResponse {
        outcome: Some(outcome),
    }
}

#[cfg(any(
    all(feature = "grpc", not(target_arch = "wasm32")),
    all(feature = "wasm", target_arch = "wasm32")
))]
/// Converts a fork receipt into its generated wire representation.
pub fn fork_receipt_to_wire(value: &crate::ForkReceipt) -> wire::ForkReceipt {
    wire::ForkReceipt {
        source: value.source.to_string(),
        destination: value.destination.to_string(),
        forked_at: value.forked_at,
        tail: value.tail,
        commit_id: Bytes::copy_from_slice(value.commit_id.as_bytes()),
    }
}

pub(crate) fn envelope_from_wire(
    value: wire::CommittedEnvelope,
) -> Result<CommittedEnvelope, StreamError> {
    envelope_with_path(value, path)
}

pub(crate) fn envelope_with_path(
    value: wire::CommittedEnvelope,
    decode_path: impl Fn(String) -> Result<StreamPath, StreamError>,
) -> Result<CommittedEnvelope, StreamError> {
    Ok(CommittedEnvelope {
        commit_id: commit_id(&value.commit_id)?,
        mutations: value
            .mutations
            .into_iter()
            .map(|mutation| committed_mutation_with_path(mutation, &decode_path))
            .collect::<Result<_, _>>()?,
    })
}

pub(crate) fn envelope_wire(value: CommittedEnvelope) -> wire::CommittedEnvelope {
    wire::CommittedEnvelope {
        commit_id: Bytes::copy_from_slice(value.commit_id.as_bytes()),
        mutations: value
            .mutations
            .into_iter()
            .map(committed_mutation_wire)
            .collect(),
    }
}

#[cfg(any(
    all(feature = "grpc", not(target_arch = "wasm32")),
    all(feature = "wasm", target_arch = "wasm32")
))]
/// Converts a commit outcome into its generated wire representation.
pub fn commit_outcome_to_wire(value: crate::CommitOutcome) -> wire::CommitResponse {
    let outcome = match value {
        crate::CommitOutcome::Committed(envelope) => {
            wire::commit_response::Outcome::Committed(envelope_to_wire(envelope))
        }
        crate::CommitOutcome::Conflict(conflicts) => {
            wire::commit_response::Outcome::Conflict(wire::CommitConflicts {
                conflicts: conflicts.into_iter().map(conflict_to_wire).collect(),
            })
        }
    };
    wire::CommitResponse {
        outcome: Some(outcome),
    }
}

#[cfg(any(
    all(feature = "grpc", not(target_arch = "wasm32")),
    all(feature = "wasm", target_arch = "wasm32")
))]
/// Converts a committed envelope into its generated wire representation.
pub fn envelope_to_wire(value: crate::CommittedEnvelope) -> wire::CommittedEnvelope {
    wire::CommittedEnvelope {
        commit_id: Bytes::copy_from_slice(value.commit_id.as_bytes()),
        mutations: value
            .mutations
            .into_iter()
            .map(committed_mutation_to_wire)
            .collect(),
    }
}

fn committed_mutation_with_path(
    value: wire::CommittedMutation,
    decode_path: &impl Fn(String) -> Result<StreamPath, StreamError>,
) -> Result<CommittedMutation, StreamError> {
    match value.mutation.ok_or(StreamError::Unavailable)? {
        wire::committed_mutation::Mutation::Append(value) => {
            Ok(CommittedMutation::Append(CommittedAppend {
                path: decode_path(value.path)?,
                start: value.start,
                end: value.end,
                tail: value.tail,
                records: value
                    .records
                    .into_iter()
                    .map(record)
                    .collect::<Result<_, _>>()?,
            }))
        }
        wire::committed_mutation::Mutation::Fork(value) => {
            Ok(CommittedMutation::Fork(CommittedFork {
                source: decode_path(value.source)?,
                destination: decode_path(value.destination)?,
                forked_at: value.forked_at,
                tail: value.tail,
                records: value
                    .records
                    .into_iter()
                    .map(record)
                    .collect::<Result<_, _>>()?,
            }))
        }
    }
}

pub(crate) fn committed_mutation_wire(value: CommittedMutation) -> wire::CommittedMutation {
    let mutation = match value {
        CommittedMutation::Append(value) => {
            wire::committed_mutation::Mutation::Append(wire::CommittedAppend {
                path: value.path.to_string(),
                start: value.start,
                end: value.end,
                tail: value.tail,
                records: value.records.into_iter().map(record_wire).collect(),
            })
        }
        CommittedMutation::Fork(value) => {
            wire::committed_mutation::Mutation::Fork(wire::CommittedFork {
                source: value.source.to_string(),
                destination: value.destination.to_string(),
                forked_at: value.forked_at,
                tail: value.tail,
                records: value.records.into_iter().map(record_wire).collect(),
            })
        }
    };
    wire::CommittedMutation {
        mutation: Some(mutation),
    }
}

#[cfg(any(
    all(feature = "grpc", not(target_arch = "wasm32")),
    all(feature = "wasm", target_arch = "wasm32")
))]
fn record_to_wire(value: crate::Record) -> wire::Record {
    wire::Record {
        sequence: value.sequence,
        value: value.value,
        commit_id: Bytes::copy_from_slice(value.commit_id.as_bytes()),
        committed_at_micros: value.committed_at_micros,
    }
}

#[cfg(any(
    all(feature = "grpc", not(target_arch = "wasm32")),
    all(feature = "wasm", target_arch = "wasm32")
))]
fn committed_mutation_to_wire(value: crate::CommittedMutation) -> wire::CommittedMutation {
    let mutation = match value {
        crate::CommittedMutation::Append(value) => {
            wire::committed_mutation::Mutation::Append(wire::CommittedAppend {
                path: value.path.to_string(),
                start: value.start,
                end: value.end,
                tail: value.tail,
                records: value.records.into_iter().map(record_to_wire).collect(),
            })
        }
        crate::CommittedMutation::Fork(value) => {
            wire::committed_mutation::Mutation::Fork(wire::CommittedFork {
                source: value.source.to_string(),
                destination: value.destination.to_string(),
                forked_at: value.forked_at,
                tail: value.tail,
                records: value.records.into_iter().map(record_to_wire).collect(),
            })
        }
    };
    wire::CommittedMutation {
        mutation: Some(mutation),
    }
}

fn conflict_with_path(
    value: wire::CommitConflict,
    decode_path: &impl Fn(String) -> Result<StreamPath, StreamError>,
) -> Result<CommitConflict, StreamError> {
    match value.conflict.ok_or(StreamError::Unavailable)? {
        wire::commit_conflict::Conflict::Tail(value) => Ok(CommitConflict::Tail {
            path: decode_path(value.path)?,
            expected: value.expected,
            actual: value.actual,
        }),
        wire::commit_conflict::Conflict::Exists(value) => Ok(CommitConflict::Exists {
            path: decode_path(value.path)?,
        }),
    }
}

pub(crate) fn conflict_wire(value: CommitConflict) -> wire::CommitConflict {
    let conflict = match value {
        CommitConflict::Tail {
            path,
            expected,
            actual,
        } => wire::commit_conflict::Conflict::Tail(wire::TailCommitConflict {
            path: path.to_string(),
            expected,
            actual,
        }),
        CommitConflict::Exists { path } => {
            wire::commit_conflict::Conflict::Exists(wire::ExistsCommitConflict {
                path: path.to_string(),
            })
        }
    };
    wire::CommitConflict {
        conflict: Some(conflict),
    }
}

#[cfg(any(
    all(feature = "grpc", not(target_arch = "wasm32")),
    all(feature = "wasm", target_arch = "wasm32")
))]
fn conflict_to_wire(value: crate::CommitConflict) -> wire::CommitConflict {
    let conflict = match value {
        crate::CommitConflict::Tail {
            path,
            expected,
            actual,
        } => wire::commit_conflict::Conflict::Tail(wire::TailCommitConflict {
            path: path.to_string(),
            expected,
            actual,
        }),
        crate::CommitConflict::Exists { path } => {
            wire::commit_conflict::Conflict::Exists(wire::ExistsCommitConflict {
                path: path.to_string(),
            })
        }
    };
    wire::CommitConflict {
        conflict: Some(conflict),
    }
}

#[cfg(all(feature = "wasm", target_arch = "wasm32"))]
pub(crate) fn observation_to_wire(
    value: crate::IdempotencyObservation,
) -> wire::InspectIdempotencyResponse {
    let outcome = match value.outcome {
        crate::IdempotencyOutcome::Append(value) => {
            wire::idempotency_observation::Outcome::Append(append_outcome_to_wire(value))
        }
        crate::IdempotencyOutcome::Fork(value) => {
            wire::idempotency_observation::Outcome::Fork(fork_receipt_to_wire(&value))
        }
        crate::IdempotencyOutcome::Commit(value) => {
            wire::idempotency_observation::Outcome::Commit(commit_outcome_to_wire(value))
        }
    };
    wire::InspectIdempotencyResponse {
        observation: Some(wire::IdempotencyObservation {
            idempotency_key: Bytes::copy_from_slice(value.idempotency_key.as_bytes()),
            request_digest: Bytes::copy_from_slice(&value.request_digest),
            outcome: Some(outcome),
        }),
    }
}

/// Builds an uncompressed Read or Follow frame carrying consecutive records.
#[must_use]
pub fn read_response_wire(records: Vec<wire::Record>) -> wire::ReadResponse {
    use prost::Message;
    let data = wire::RecordBatch { records }.encode_to_vec();
    wire::ReadResponse {
        codec: wire::Codec::None as i32,
        decoded_length: data.len() as u64,
        data: data.into(),
    }
}

/// Decodes one Read or Follow frame to its encoded `RecordBatch`.
///
/// The declared length is checked against [`crate::MAX_COMMAND_BYTES`] before any
/// allocation or decompression, and the decoded length must match it exactly.
pub fn read_response_batch(value: wire::ReadResponse) -> Result<Bytes, StreamError> {
    let length = usize::try_from(value.decoded_length)
        .ok()
        .filter(|length| *length <= crate::MAX_COMMAND_BYTES)
        .ok_or(StreamError::Unavailable)?;
    match wire::Codec::try_from(value.codec) {
        Ok(wire::Codec::None) if value.data.len() == length => Ok(value.data),
        Ok(wire::Codec::Zstd) => zstd(&value.data, length)
            .map(Bytes::from)
            .ok_or(StreamError::Unavailable),
        _ => Err(StreamError::Unavailable),
    }
}

/// Decodes one Read or Follow frame to its records, in frame order.
pub fn read_response_records(value: wire::ReadResponse) -> Result<Vec<wire::Record>, StreamError> {
    use prost::Message;
    wire::RecordBatch::decode(read_response_batch(value)?)
        .map(|batch| batch.records)
        .map_err(|_| StreamError::Unavailable)
}

/// Decompresses one or more complete Zstandard frames to exactly `length` bytes.
///
/// Empty data is not a Zstandard stream; an empty frame is sent as `CODEC_NONE`.
fn zstd(mut input: &[u8], length: usize) -> Option<Vec<u8>> {
    use std::io::Read;
    if input.is_empty() {
        return None;
    }
    let mut output = Vec::with_capacity(length);
    while !input.is_empty() {
        let allowed = (length - output.len()) as u64 + 1;
        let decoder = ruzstd::decoding::StreamingDecoder::new(&mut input).ok()?;
        decoder.take(allowed).read_to_end(&mut output).ok()?;
        if output.len() > length {
            return None;
        }
    }
    (output.len() == length).then_some(output)
}

#[cfg(test)]
pub(crate) fn zstd_read_response(records: Vec<wire::Record>) -> wire::ReadResponse {
    let plain = read_response_wire(records);
    wire::ReadResponse {
        codec: wire::Codec::Zstd as i32,
        decoded_length: plain.decoded_length,
        data: ruzstd::encoding::compress_to_vec(
            &plain.data[..],
            ruzstd::encoding::CompressionLevel::Fastest,
        )
        .into(),
    }
}

#[cfg(test)]
mod read_response_tests {
    use super::{read_response_records, read_response_wire, wire, zstd_read_response};

    fn records(from: u64, count: u64) -> Vec<wire::Record> {
        (from..from + count)
            .map(|sequence| wire::Record {
                sequence,
                value: vec![u8::try_from(sequence % 251).unwrap_or(0); 100].into(),
                commit_id: vec![1; 32].into(),
                committed_at_micros: sequence,
            })
            .collect()
    }

    #[test]
    fn compressed_frame_decodes_to_the_same_records() {
        for count in [0, 1, 64] {
            let expected = records(5, count);
            let frame = zstd_read_response(expected.clone());
            assert!(
                count < 64 || frame.data.len() < usize::try_from(frame.decoded_length).unwrap_or(0)
            );
            assert_eq!(read_response_records(frame).ok(), Some(expected.clone()));
            assert_eq!(
                read_response_records(read_response_wire(expected.clone())).ok(),
                Some(expected)
            );
        }
    }

    #[test]
    fn empty_zstd_data_is_refused_and_an_empty_plain_frame_is_accepted() {
        let empty = wire::ReadResponse {
            codec: wire::Codec::Zstd as i32,
            data: Default::default(),
            decoded_length: 0,
        };
        assert!(read_response_records(empty).is_err());
        assert_eq!(
            read_response_records(read_response_wire(Vec::new())).ok(),
            Some(Vec::new())
        );
    }

    #[test]
    fn mixed_frames_reassemble_in_order() {
        let expected = records(0, 30);
        let frames = expected
            .chunks(7)
            .enumerate()
            .map(|(index, chunk)| {
                if index % 2 == 0 {
                    zstd_read_response(chunk.to_vec())
                } else {
                    read_response_wire(chunk.to_vec())
                }
            })
            .collect::<Vec<_>>();
        let mut result = Vec::new();
        for frame in frames {
            result.extend(read_response_records(frame).expect("valid frame"));
        }
        assert_eq!(result, expected);
    }

    #[test]
    fn oversized_declared_length_is_refused_before_decompressing() {
        let maximum = crate::MAX_COMMAND_BYTES as u64;
        for length in [maximum + 1, u64::MAX] {
            for codec in [wire::Codec::None, wire::Codec::Zstd] {
                let frame = wire::ReadResponse {
                    codec: codec as i32,
                    data: vec![0xff; 16].into(),
                    decoded_length: length,
                };
                assert!(read_response_records(frame).is_err());
            }
        }
        let mut expanding = zstd_read_response(records(0, 64));
        expanding.decoded_length = 100;
        assert!(read_response_records(expanding).is_err());
    }

    #[test]
    fn mismatched_length_unknown_codec_and_corrupt_data_are_refused() {
        let mut long = zstd_read_response(records(0, 3));
        long.decoded_length += 1;
        assert!(read_response_records(long).is_err());
        let mut plain = read_response_wire(records(0, 3));
        plain.decoded_length -= 1;
        assert!(read_response_records(plain).is_err());
        for codec in [0, 3] {
            let mut unknown = read_response_wire(records(0, 3));
            unknown.codec = codec;
            assert!(read_response_records(unknown).is_err());
        }
        let mut corrupt = zstd_read_response(records(0, 30));
        corrupt.data.truncate(corrupt.data.len() / 2);
        assert!(read_response_records(corrupt).is_err());
    }
}
