//! Shared domain/wire conversion for every Stream transport and persistence adapter.

use bytes::Bytes;

use crate::{
    AppendOutcome, AppendReceipt, CommitCondition, CommitConflict, CommitId,
    CommitMutation, CommitOutcome, CommittedAppend, CommittedEnvelope, CommittedFork,
    CommittedMutation, ChildrenPage, ChildrenPageRequest, ForkReceipt, IdempotencyKey,
    IdempotencyObservation,
    IdempotencyOutcome, Record, StreamError, StreamPath, MAX_ITEMS, MAX_RECORD_BYTES, wire,
};

pub(crate) fn path(value: String) -> Result<StreamPath, StreamError> {
    StreamPath::new(value)
}

/// Converts an optional protobuf idempotency key with canonical validation.
pub fn optional_key(value: Option<Bytes>) -> Result<Option<IdempotencyKey>, StreamError> {
    value.map(IdempotencyKey::new).transpose()
}

#[cfg(any(feature = "grpc", feature = "local"))]
/// Converts a required protobuf idempotency key with canonical validation.
pub fn required_key(value: Option<Bytes>) -> Result<IdempotencyKey, StreamError> {
    optional_key(value)?.ok_or(StreamError::InvalidArgument)
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
    let request_digest = <[u8; 32]>::try_from(value.request_digest.as_ref())
        .map_err(|_| StreamError::Unavailable)?;
    let outcome = match value.outcome.ok_or(StreamError::Unavailable)? {
        wire::idempotency_observation::Outcome::Append(value) => {
            IdempotencyOutcome::Append(append_outcome_from_wire(value)?)
        }
        wire::idempotency_observation::Outcome::Fork(value) => {
            IdempotencyOutcome::Fork(ForkReceipt {
                source: path(value.source)?,
                destination: path(value.destination)?,
                forked_at: value.forked_at,
                tail: value.tail,
                commit_id: commit_id(&value.commit_id)?,
            })
        }
        wire::idempotency_observation::Outcome::Commit(value) => {
            IdempotencyOutcome::Commit(commit_outcome_from_wire(value)?)
        }
    };
    Ok(IdempotencyObservation {
        idempotency_key: IdempotencyKey::new(value.idempotency_key)?,
        request_digest,
        outcome,
    })
}

/// Converts a validated idempotency observation into its protobuf response.
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

/// Converts a validated append outcome into its protobuf response.
pub fn append_outcome_wire(value: AppendOutcome) -> wire::AppendResponse {
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

pub(crate) fn commit_outcome_from_wire(
    value: wire::CommitResponse,
) -> Result<CommitOutcome, StreamError> {
    match value.outcome.ok_or(StreamError::Unavailable)? {
        wire::commit_response::Outcome::Committed(envelope) => {
            Ok(CommitOutcome::Committed(envelope_from_wire(envelope)?))
        }
        wire::commit_response::Outcome::Conflict(conflicts) => Ok(CommitOutcome::Conflict(
            conflicts
                .conflicts
                .into_iter()
                .map(conflict_from_wire)
                .collect::<Result<_, _>>()?,
        )),
    }
}

/// Converts a validated commit outcome into its protobuf response.
pub fn commit_outcome_wire(value: CommitOutcome) -> wire::CommitResponse {
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

/// Parses a canonical 32-byte commit identity.
pub fn commit_id(value: &[u8]) -> Result<CommitId, StreamError> {
    let bytes = <[u8; 32]>::try_from(value).map_err(|_| StreamError::Unavailable)?;
    Ok(CommitId::from_bytes(bytes))
}

pub(crate) fn record(value: wire::Record) -> Result<Record, StreamError> {
    checked_record(Record {
        sequence: value.sequence,
        value: value.value,
        commit_id: commit_id(&value.commit_id)?,
        committed_at_micros: value.committed_at_micros,
    })
}

/// Rejects a record body that exceeds the canonical wire bound.
pub(crate) fn checked_record(value: Record) -> Result<Record, StreamError> {
    if value.value.len() > MAX_RECORD_BYTES {
        return Err(StreamError::Unavailable);
    }
    Ok(value)
}

/// Validates a children-page request before transport dispatch.
pub(crate) fn validate_children_page_request(
    request: &ChildrenPageRequest,
) -> Result<(), StreamError> {
    if request.limit == 0 || usize::try_from(request.limit).map_or(true, |limit| limit > MAX_ITEMS) {
        return Err(StreamError::LimitExceeded);
    }
    if request.after.as_ref().is_some_and(|after| {
        request.hierarchy_version.is_none()
            || !is_direct_child(request.parent.as_ref(), after)
    }) {
        return Err(StreamError::InvalidArgument);
    }
    Ok(())
}

/// Parses a remote path, normalizing malformed response data to `Unavailable`.
pub(crate) fn response_path(value: String) -> Result<StreamPath, StreamError> {
    path(value).map_err(|_| StreamError::Unavailable)
}

/// Validates one remote hierarchy page against its request cursor.
pub(crate) fn checked_children_page(
    request: &ChildrenPageRequest,
    page: ChildrenPage,
) -> Result<ChildrenPage, StreamError> {
    validate_children_page_request(request)?;
    if request
        .hierarchy_version
        .is_some_and(|version| version != page.hierarchy_version)
    {
        return Err(StreamError::HierarchyChanged);
    }
    if page.children.len() > request.limit as usize {
        return Err(StreamError::Unavailable);
    }
    let mut previous = request.after.as_ref();
    for child in &page.children {
        if !is_direct_child(request.parent.as_ref(), &child.path)
            || previous.is_some_and(|previous| previous >= &child.path)
        {
            return Err(StreamError::Unavailable);
        }
        previous = Some(&child.path);
    }
    if let Some(next_after) = &page.next_after {
        if page.children.last().is_none_or(|child| child.path != *next_after) {
            return Err(StreamError::Unavailable);
        }
    }
    Ok(page)
}

fn is_direct_child(parent: Option<&StreamPath>, child: &StreamPath) -> bool {
    match parent {
        Some(parent) => child.parent().as_ref() == Some(parent),
        None => child.parent().is_none(),
    }
}

pub(crate) fn append_receipt(value: &wire::AppendReceipt) -> Result<AppendReceipt, StreamError> {
    Ok(AppendReceipt {
        start: value.start,
        end: value.end,
        tail: value.tail,
        commit_id: commit_id(&value.commit_id)?,
    })
}

/// Converts a validated record into its protobuf representation.
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

/// Converts a validated fork receipt into its protobuf representation.
pub fn fork_receipt_wire(value: &ForkReceipt) -> wire::ForkReceipt {
    wire::ForkReceipt {
        source: value.source.to_string(),
        destination: value.destination.to_string(),
        forked_at: value.forked_at,
        tail: value.tail,
        commit_id: Bytes::copy_from_slice(value.commit_id.as_bytes()),
    }
}

/// Converts an append request using the canonical Stream validation rules.
pub fn append_from_wire(
    value: wire::AppendRequest,
) -> Result<crate::AppendRequest, StreamError> {
    Ok(crate::AppendRequest {
        path: path(value.path)?,
        records: value.records,
        if_tail: value.if_tail,
        idempotency_key: optional_key(value.idempotency_key)?,
    })
}

/// Converts a fork request using the canonical Stream validation rules.
pub fn fork_from_wire(value: wire::ForkRequest) -> Result<crate::ForkRequest, StreamError> {
    Ok(crate::ForkRequest {
        source: path(value.source)?,
        destination: path(value.destination)?,
        at_tail: value.at_tail,
        idempotency_key: optional_key(value.idempotency_key)?,
    })
}

/// Converts a read request using the canonical Stream validation rules.
pub fn read_from_wire(value: wire::ReadRequest) -> Result<crate::ReadRequest, StreamError> {
    Ok(crate::ReadRequest {
        path: path(value.path)?,
        from: value.from,
        limit: value.limit,
    })
}

#[cfg(target_arch = "wasm32")]
pub(crate) fn follow_from_wire(
    value: wire::FollowRequest,
) -> Result<(StreamPath, u64), StreamError> {
    Ok((path(value.path)?, value.from))
}

/// Converts a children request using the canonical Stream validation rules.
pub fn children_from_wire(
    value: wire::ChildrenRequest,
) -> Result<crate::ChildrenRequest, StreamError> {
    Ok(crate::ChildrenRequest {
        parent: value.parent.map(path).transpose()?,
        limit: value.limit,
    })
}

/// Converts a children-page request using the canonical Stream validation rules.
pub fn children_page_from_wire(
    value: wire::ChildrenPageRequest,
) -> Result<crate::ChildrenPageRequest, StreamError> {
    let request = crate::ChildrenPageRequest {
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
    };
    validate_children_page_request(&request)?;
    Ok(request)
}

/// Converts a validated children page into its protobuf response.
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

/// Converts a commit request using the canonical Stream validation rules.
pub fn commit_from_wire(
    value: wire::CommitRequest,
) -> Result<crate::CommitRequest, StreamError> {
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

/// Binding-facing alias for append_outcome_wire.
pub use append_outcome_wire as append_outcome_to_wire;

/// Binding-facing alias for fork_receipt_wire.
pub use fork_receipt_wire as fork_receipt_to_wire;

pub(crate) fn envelope_from_wire(
    value: wire::CommittedEnvelope,
) -> Result<CommittedEnvelope, StreamError> {
    Ok(CommittedEnvelope {
        commit_id: commit_id(&value.commit_id)?,
        mutations: value
            .mutations
            .into_iter()
            .map(committed_mutation)
            .collect::<Result<_, _>>()?,
    })
}

/// Converts a validated committed envelope into its protobuf representation.
pub fn envelope_wire(value: CommittedEnvelope) -> wire::CommittedEnvelope {
    wire::CommittedEnvelope {
        commit_id: Bytes::copy_from_slice(value.commit_id.as_bytes()),
        mutations: value
            .mutations
            .into_iter()
            .map(committed_mutation_wire)
            .collect(),
    }
}

/// Binding-facing alias for commit_outcome_wire.
pub use commit_outcome_wire as commit_outcome_to_wire;

/// Binding-facing alias for envelope_wire.
pub use envelope_wire as envelope_to_wire;

pub(crate) fn committed_mutation(
    value: wire::CommittedMutation,
) -> Result<CommittedMutation, StreamError> {
    match value.mutation.ok_or(StreamError::Unavailable)? {
        wire::committed_mutation::Mutation::Append(value) => {
            Ok(CommittedMutation::Append(CommittedAppend {
                path: path(value.path)?,
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
                source: path(value.source)?,
                destination: path(value.destination)?,
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

pub(crate) fn conflict_from_wire(
    value: wire::CommitConflict,
) -> Result<CommitConflict, StreamError> {
    match value.conflict.ok_or(StreamError::Unavailable)? {
        wire::commit_conflict::Conflict::Tail(value) => Ok(CommitConflict::Tail {
            path: path(value.path)?,
            expected: value.expected,
            actual: value.actual,
        }),
        wire::commit_conflict::Conflict::Exists(value) => Ok(CommitConflict::Exists {
            path: path(value.path)?,
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

#[cfg(target_arch = "wasm32")]
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

#[cfg(test)]
mod tests {
    use crate::Child;
    use super::*;

    #[test]
    fn checked_cursor_increment_rejects_u64_max() {
        let next = u64::MAX
            .checked_add(1)
            .ok_or(StreamError::Unavailable);
        assert_eq!(next, Err(StreamError::Unavailable));
    }

    #[test]
    fn checked_record_rejects_an_oversized_body() {
        let record = Record {
            sequence: 0,
            value: Bytes::from(vec![0; MAX_RECORD_BYTES + 1]),
            commit_id: CommitId::from_bytes([1; 32]),
            committed_at_micros: 0,
        };
        assert_eq!(checked_record(record), Err(StreamError::Unavailable));
    }

    #[test]
    fn checked_children_page_rejects_limits_and_non_direct_children() -> Result<(), StreamError> {
        let request = ChildrenPageRequest {
            parent: Some(StreamPath::new("runs")?),
            after: None,
            hierarchy_version: None,
            limit: 1,
        };
        let too_many = ChildrenPage {
            hierarchy_version: CommitId::from_bytes([1; 32]),
            children: vec![
                Child {
                    path: StreamPath::new("runs/a")?,
                },
                Child {
                    path: StreamPath::new("runs/b")?,
                },
            ],
            next_after: None,
        };
        assert_eq!(
            checked_children_page(&request, too_many),
            Err(StreamError::Unavailable)
        );

        let non_direct = ChildrenPage {
            hierarchy_version: CommitId::from_bytes([1; 32]),
            children: vec![Child {
                path: StreamPath::new("runs/a/leaf")?,
            }],
            next_after: None,
        };
        assert_eq!(
            checked_children_page(&request, non_direct),
            Err(StreamError::Unavailable)
        );
        Ok(())
    }

    #[test]
    fn checked_children_page_rejects_drift_order_and_bad_continuation() -> Result<(), StreamError> {
        let version = CommitId::from_bytes([1; 32]);
        let request = ChildrenPageRequest {
            parent: Some(StreamPath::new("runs")?),
            after: Some(StreamPath::new("runs/a")?),
            hierarchy_version: Some(version),
            limit: 1,
        };
        let drifted = ChildrenPage {
            hierarchy_version: CommitId::from_bytes([2; 32]),
            children: vec![Child {
                path: StreamPath::new("runs/b")?,
            }],
            next_after: Some(StreamPath::new("runs/b")?),
        };
        assert_eq!(
            checked_children_page(&request, drifted),
            Err(StreamError::HierarchyChanged)
        );

        let out_of_order = ChildrenPage {
            hierarchy_version: version,
            children: vec![Child {
                path: StreamPath::new("runs/a")?,
            }],
            next_after: Some(StreamPath::new("runs/a")?),
        };
        assert_eq!(
            checked_children_page(&request, out_of_order),
            Err(StreamError::Unavailable)
        );

        let missing_children = ChildrenPage {
            hierarchy_version: version,
            children: Vec::new(),
            next_after: Some(StreamPath::new("runs/b")?),
        };
        assert_eq!(
            checked_children_page(&request, missing_children),
            Err(StreamError::Unavailable)
        );
        Ok(())
    }
}
