//! Canonical mutation facts prepared against pre-commit observations.

use crate::OwnedBytes;

use crate::{
    CommitCondition, CommitConflict, CommitId,
    CommitMutation, CommitRequest, CommittedAppend, CommittedEnvelope,
    CommittedFork, CommittedMutation, ForkRequest, Record, StreamError, StreamPath,
};
use crate::memory::{MemoryObserver, MemoryOutput, MemoryResult};

pub(crate) fn fork_cut(
    request: &ForkRequest,
    source_tail: Option<u64>,
    destination_tail: Option<u64>,
) -> Result<u64, StreamError> {
    if destination_tail.is_some() {
        return Err(StreamError::AlreadyExists);
    }
    let source_tail = source_tail.ok_or(StreamError::NotFound)?;
    let cut = request.at_tail.unwrap_or(source_tail);
    if cut > source_tail {
        return Err(StreamError::PrefixNotRetained);
    }
    Ok(cut)
}

pub(crate) fn append_fact(
    path: &StreamPath,
    start: u64,
    bodies: &[OwnedBytes],
    commit_id: CommitId,
    committed_at_micros: u64,
    observer: &MemoryObserver,
) -> MemoryResult<MemoryOutput<CommittedAppend>> {
    let records = records(start, bodies, commit_id, committed_at_micros, observer)?;
    let end = start + records.value.len() as u64;
    Ok(MemoryOutput {
        value: CommittedAppend {
            path: observer.retain_path(path)?, start, end, tail: end, records: records.value,
        },
        allocation: records.allocation,
    })
}

/// The closed completion ingress has one already allocated record slot.
#[cfg(not(target_arch = "wasm32"))]
pub(crate) fn single_append_template(request: &CommitRequest) -> Result<(&StreamPath, u64), StreamError> {
    let [CommitCondition::Tail { path: first, expected: first_tail },
        CommitCondition::Tail { path: second, expected: second_tail }] = request.conditions.as_slice()
        else { return Err(StreamError::InvalidArgument); };
    let [CommitMutation::Append { path, records }] = request.mutations.as_slice()
        else { return Err(StreamError::InvalidArgument); };
    if records.len() != 1 || !records[0].is_empty() || first == second {
        return Err(StreamError::InvalidArgument);
    }
    let start = if path == first { *first_tail } else if path == second { *second_tail } else {
        return Err(StreamError::InvalidArgument);
    };
    start.checked_add(1).ok_or(StreamError::LimitExceeded)?;
    Ok((path, start))
}

/// Rebinds only scalar metadata and an immutable body alias in the real fact vector allocated by `mutations`.
#[cfg(not(target_arch = "wasm32"))]
pub(crate) fn fill_single_append(
    envelope: &mut CommittedEnvelope,
    record: &Record,
    mut visited: impl FnMut(),
) -> Result<(), StreamError> {
    let [CommittedMutation::Append(append)] = envelope.mutations.as_mut_slice()
        else { return Err(StreamError::InvalidArgument); };
    visited();
    let [slot] = append.records.as_mut_slice() else { return Err(StreamError::InvalidArgument); };
    visited();
    let end = record.sequence.checked_add(1).ok_or(StreamError::LimitExceeded)?;
    *slot = record.clone();
    append.start = record.sequence;
    append.end = end;
    append.tail = end;
    envelope.commit_id = record.commit_id;
    Ok(())
}

pub(crate) fn conditions(
    conditions: &[CommitCondition],
    tail: impl Fn(&StreamPath) -> Option<u64>,
    observer: &MemoryObserver,
) -> MemoryResult<MemoryOutput<Vec<CommitConflict>>> {
    let mut count = 0;
    for condition in conditions {
        observer.admit_item()?;
        count += usize::from(match condition {
            CommitCondition::Tail { path, expected } => tail(path) != Some(*expected),
            CommitCondition::Absent { path } => tail(path).is_some(),
        });
        observer.record_item()?;
    }
    let mut output = observer.vec(count)?;
    for condition in conditions {
        observer.admit_item()?;
        let conflict = match condition {
            CommitCondition::Tail { path, expected } => {
                let actual = tail(path);
                if actual != Some(*expected) {
                    Some(CommitConflict::Tail {
                        path: observer.retain_path(path)?, expected: *expected, actual,
                    })
                } else { None }
            }
            CommitCondition::Absent { path } => {
                if tail(path).is_some() {
                    Some(CommitConflict::Exists { path: observer.retain_path(path)? })
                } else { None }
            }
        };
        if let Some(conflict) = conflict { output.value.push(conflict); }
        observer.record_item()?;
    }
    Ok(output)
}

pub(crate) fn authority(
    request: &CommitRequest,
    tail: impl Fn(&StreamPath) -> Option<u64>,
    observer: &MemoryObserver,
) -> MemoryResult<()> {
    for mutation in &request.mutations {
        observer.admit_item()?;
        let error = if let CommitMutation::Fork { source, at_tail, .. } = mutation {
            match tail(source) {
                None => Some(StreamError::NotFound),
                Some(tail) if *at_tail > tail => Some(StreamError::PrefixNotRetained),
                Some(_) => None,
            }
        } else { None };
        observer.record_item()?;
        if let Some(error) = error { return Err(error.into()); }
    }
    Ok(())
}

pub(crate) fn mutations(
    mutations: &[CommitMutation],
    tail: impl Fn(&StreamPath) -> Option<u64>,
    commit_id: CommitId,
    committed_at_micros: u64,
    observer: &MemoryObserver,
) -> MemoryResult<MemoryOutput<Vec<CommittedMutation>>> {
    let mut output = observer.vec(mutations.len())?;
    for mutation in mutations {
        observer.admit_item()?;
        let committed = match mutation {
            CommitMutation::Append { path, records: bodies } => {
                let append = append_fact(path, tail(path).unwrap_or(0), bodies, commit_id, committed_at_micros, observer)?;
                output.allocation.absorb(append.allocation)?;
                CommittedMutation::Append(append.value)
            }
            CommitMutation::Fork { source, destination, at_tail, records: bodies } => {
                let source_tail = tail(source).ok_or(StreamError::NotFound)?;
                if *at_tail > source_tail { return Err(StreamError::PrefixNotRetained.into()); }
                let records = records(
                    *at_tail, bodies, commit_id, committed_at_micros, observer,
                )?;
                let tail = at_tail + records.value.len() as u64;
                output.allocation.absorb(records.allocation)?;
                CommittedMutation::Fork(CommittedFork {
                    source: observer.retain_path(source)?, destination: observer.retain_path(destination)?,
                    forked_at: *at_tail, tail, records: records.value,
                })
            }
        };
        output.value.push(committed);
        observer.record_item()?;
    }
    Ok(output)
}

pub(crate) fn records(
    start: u64,
    bodies: &[OwnedBytes],
    commit_id: CommitId,
    committed_at_micros: u64,
    observer: &MemoryObserver,
) -> MemoryResult<MemoryOutput<Vec<Record>>> {
    let count = bodies.len();
    // Reserve the exclusive tail too: a record at u64::MAX is unrepresentable.
    start.checked_add(u64::try_from(count).map_err(|_| StreamError::LimitExceeded)?)
        .ok_or(StreamError::LimitExceeded)?;
    let mut output = observer.vec(count)?;
    for (index, value) in bodies.iter().enumerate() {
        observer.admit_item()?;
        output.value.push(Record {
            sequence: start + index as u64,
            value: value.clone(),
            commit_id,
            committed_at_micros,
        });
        observer.record_item()?;
    }
    Ok(output)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn rejects_unrepresentable_exclusive_tails() {
        assert!(matches!(records(
            u64::MAX, &[OwnedBytes::from_static(b"overflow")], CommitId::default(), 0,
            &MemoryObserver::default(),
        ), Err(crate::memory::MemoryFailure::Provider(StreamError::LimitExceeded))));
    }
}
