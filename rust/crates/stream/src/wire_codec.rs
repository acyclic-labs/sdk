//! Shared domain/wire conversion for every Stream transport and persistence adapter.

use bytes::Bytes;

use crate::{
    AppendOutcome, AppendReceipt, CommitCondition, CommitConflict, CommitId, CommitMutation,
    CommitOutcome, CommittedAppend, CommittedDelete, CommittedEnvelope, CommittedFork,
    CommittedMutation, CommittedTrim, DeleteReceipt, ForkReceipt, IdempotencyKey,
    IdempotencyObservation, IdempotencyOutcome, Record, StreamError, StreamPath, TrimReceipt, wire,
};

pub(crate) fn path(value: String) -> Result<StreamPath, StreamError> {
    StreamPath::new(value)
}

pub(crate) fn optional_key(value: Option<Bytes>) -> Result<Option<IdempotencyKey>, StreamError> {
    value.map(IdempotencyKey::new).transpose()
}

pub(crate) fn required_key(value: Option<Bytes>) -> Result<IdempotencyKey, StreamError> {
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
        CommitMutation::Trim { path, before } => {
            wire::commit_mutation::Mutation::Trim(wire::TrimMutation {
                path: path.to_string(),
                before,
            })
        }
        CommitMutation::Delete { path } => {
            wire::commit_mutation::Mutation::Delete(wire::DeleteMutation {
                path: path.to_string(),
            })
        }
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
        wire::commit_mutation::Mutation::Trim(value) => Ok(CommitMutation::Trim {
            path: path(value.path)?,
            before: value.before,
        }),
        wire::commit_mutation::Mutation::Delete(value) => Ok(CommitMutation::Delete {
            path: path(value.path)?,
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
        wire::idempotency_observation::Outcome::Trim(value) => {
            IdempotencyOutcome::Trim(TrimReceipt {
                path: path(value.path)?,
                trim_point: value.trim_point,
                commit_id: commit_id(&value.commit_id)?,
            })
        }
        wire::idempotency_observation::Outcome::Delete(value) => {
            IdempotencyOutcome::Delete(DeleteReceipt {
                path: path(value.path)?,
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

pub(crate) fn observation_wire(value: IdempotencyObservation) -> wire::IdempotencyObservation {
    let outcome = match value.outcome {
        IdempotencyOutcome::Append(value) => {
            wire::idempotency_observation::Outcome::Append(append_outcome_wire(value))
        }
        IdempotencyOutcome::Fork(value) => {
            wire::idempotency_observation::Outcome::Fork(fork_receipt_wire(&value))
        }
        IdempotencyOutcome::Trim(value) => {
            wire::idempotency_observation::Outcome::Trim(trim_receipt_wire(&value))
        }
        IdempotencyOutcome::Delete(value) => {
            wire::idempotency_observation::Outcome::Delete(delete_receipt_wire(&value))
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

pub(crate) fn commit_id(value: &[u8]) -> Result<CommitId, StreamError> {
    let bytes = <[u8; 32]>::try_from(value).map_err(|_| StreamError::Unavailable)?;
    Ok(CommitId::from_bytes(bytes))
}

pub(crate) fn record(value: wire::Record) -> Result<Record, StreamError> {
    Ok(Record {
        sequence: value.sequence,
        value: value.value,
        commit_id: commit_id(&value.commit_id)?,
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

pub(crate) fn record_wire(value: Record) -> wire::Record {
    wire::Record {
        sequence: value.sequence,
        value: value.value,
        commit_id: Bytes::copy_from_slice(value.commit_id.as_bytes()),
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

pub(crate) fn trim_receipt_wire(value: &TrimReceipt) -> wire::TrimReceipt {
    wire::TrimReceipt {
        path: value.path.to_string(),
        trim_point: value.trim_point,
        commit_id: Bytes::copy_from_slice(value.commit_id.as_bytes()),
    }
}

pub(crate) fn delete_receipt_wire(value: &DeleteReceipt) -> wire::DeleteReceipt {
    wire::DeleteReceipt {
        path: value.path.to_string(),
        commit_id: Bytes::copy_from_slice(value.commit_id.as_bytes()),
    }
}

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
        wire::committed_mutation::Mutation::Trim(value) => {
            Ok(CommittedMutation::Trim(CommittedTrim {
                path: path(value.path)?,
                trim_point: value.trim_point,
            }))
        }
        wire::committed_mutation::Mutation::Delete(value) => {
            Ok(CommittedMutation::Delete(CommittedDelete {
                path: path(value.path)?,
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
        CommittedMutation::Trim(value) => {
            wire::committed_mutation::Mutation::Trim(wire::CommittedTrim {
                path: value.path.to_string(),
                trim_point: value.trim_point,
            })
        }
        CommittedMutation::Delete(value) => {
            wire::committed_mutation::Mutation::Delete(wire::CommittedDelete {
                path: value.path.to_string(),
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
        wire::commit_conflict::Conflict::Retired(value) => Ok(CommitConflict::Retired {
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
        CommitConflict::Retired { path } => {
            wire::commit_conflict::Conflict::Retired(wire::RetiredCommitConflict {
                path: path.to_string(),
            })
        }
    };
    wire::CommitConflict {
        conflict: Some(conflict),
    }
}
