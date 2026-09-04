//! Shared domain/wire conversion for every Stream transport and persistence adapter.

use bytes::Bytes;

use crate::{CommitCondition, CommitMutation, IdempotencyKey, StreamError, StreamPath, wire};

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
        } => wire::commit_mutation::Mutation::Fork(wire::ForkMutation {
            source: source.to_string(),
            destination: destination.to_string(),
            at_tail,
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
