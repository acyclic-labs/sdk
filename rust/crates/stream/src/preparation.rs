//! Shared mutation facts, identities and conditions for stream backends.
use crate::AppendRecord;
use crate::{
    CommitCondition, CommitConflict, CommitId, CommitMutation, CommitRequest, CommittedAppend,
    CommittedFork, CommittedMutation, ForkRequest, Record, StreamError, StreamPath,
};
use sha2::{Digest, Sha256};

/// Identity belongs to the global durable decision order, never a transient
/// working set. Both warm and cold planners use the same domain and ordinal.
pub(crate) fn commit_identity(decision: u64, digest: [u8; 32]) -> CommitId {
    let mut hash = Sha256::new();
    hash.update(b"acyclic.stream.commit.v1\0");
    hash.update(decision.to_le_bytes());
    hash.update(digest);
    CommitId::from_bytes(hash.finalize().into())
}

pub(crate) fn committed_time(previous_micros: u64, sampled_millis: u64) -> u64 {
    previous_micros.max(sampled_millis.saturating_mul(1_000))
}

/// Evaluate against the backend's authoritative head. The preceding commit is
/// loaded lazily, only for a Head condition whose tail already matches.
pub(crate) fn condition_conflict(
    condition: &CommitCondition,
    actual: Option<u64>,
    preceding: impl FnOnce() -> Result<CommitId, StreamError>,
) -> Result<Option<CommitConflict>, StreamError> {
    let path = crate::request::condition_path(condition);
    Ok(match condition {
        CommitCondition::Tail { expected, .. }
            if actual.unwrap_or(0) != *expected =>
        {
            Some(CommitConflict::Tail {
                path: path.clone(),
                expected: *expected,
                actual,
            })
        }
        CommitCondition::Head { expected, .. }
            if actual != Some(*expected) =>
        {
            Some(CommitConflict::Tail {
                path: path.clone(),
                expected: *expected,
                actual,
            })
        }
        CommitCondition::Absent { .. } if actual.is_some() => {
            Some(CommitConflict::Exists { path: path.clone() })
        }
        CommitCondition::Head { commit_id, .. } => {
            #[cfg(all(acyclic_loom, feature = "local"))]
            if crate::verification::fault_is(crate::verification::Fault::IgnoreHead) {
                return Ok(None);
            }
            let actual = preceding()?;
            (actual != *commit_id).then(|| CommitConflict::Head {
                path: path.clone(),
                expected: *commit_id,
                actual,
            })
        }
        _ => None,
    })
}

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
    path: StreamPath,
    start: u64,
    records: Vec<Record>,
) -> Result<CommittedAppend, StreamError> {
    let end = start
        .checked_add(u64::try_from(records.len()).map_err(|_| StreamError::LimitExceeded)?)
        .ok_or(StreamError::LimitExceeded)?;
    Ok(CommittedAppend {
        path,
        start,
        end,
        tail: end,
        records,
    })
}

pub(crate) fn authority(
    request: &CommitRequest,
    mut tail: impl FnMut(&StreamPath) -> Result<Option<u64>, StreamError>,
) -> Result<(), StreamError> {
    for mutation in &request.mutations {
        if let CommitMutation::Fork {
            source, at_tail, ..
        } = mutation
        {
            let source_tail = tail(source)?.ok_or(StreamError::NotFound)?;
            if *at_tail > source_tail {
                return Err(StreamError::PrefixNotRetained);
            }
        }
    }
    Ok(())
}

/// Preserve pre-commit source cuts and mutation order independently of the
/// backend's record storage. Both output containers are admitted by the caller;
/// the semantic planner never allocates a second unaccounted vector.
pub(crate) fn facts(
    source: &[CommitMutation],
    mut tail: impl FnMut(&StreamPath) -> Result<Option<u64>, StreamError>,
    mut build: impl FnMut(u64, &[AppendRecord]) -> Result<Vec<Record>, StreamError>,
    mut output: Vec<CommittedMutation>,
) -> Result<Vec<CommittedMutation>, StreamError> {
    if !output.is_empty() || output.capacity() < source.len() {
        return Err(StreamError::InvalidArgument);
    }
    for mutation in source {
        let (start, bodies) = match mutation {
            CommitMutation::Append { path, records } => (tail(path)?.unwrap_or(0), records),
            CommitMutation::Fork {
                at_tail, records, ..
            } => (*at_tail, records),
        };
        let records = build(start, bodies)?;
        output.push(match mutation {
            CommitMutation::Append { path, .. } => {
                CommittedMutation::Append(append_fact(path.clone(), start, records)?)
            }
            CommitMutation::Fork {
                source,
                destination,
                at_tail,
                exclude,
                ..
            } => {
                let tail = start
                    .checked_add(
                        u64::try_from(records.len()).map_err(|_| StreamError::LimitExceeded)?,
                    )
                    .ok_or(StreamError::LimitExceeded)?;
                CommittedMutation::Fork(CommittedFork {
                    exclude: exclude.clone(),
source: source.clone(),
                    destination: destination.clone(),
                    forked_at: *at_tail,
                    tail,
                    records,
                })
            }
        });
    }
    Ok(output)
}

pub(crate) fn records(
    start: u64,
    bodies: &[AppendRecord],
    commit_id: CommitId,
    committed_at_micros: u64,
    mut records: Vec<Record>,
) -> Result<Vec<Record>, StreamError> {
    if !records.is_empty() || records.capacity() < bodies.len() {
        return Err(StreamError::InvalidArgument);
    }
    // Reserve the exclusive tail too: a record at u64::MAX cannot be represented.
    start
        .checked_add(u64::try_from(bodies.len()).map_err(|_| StreamError::LimitExceeded)?)
        .ok_or(StreamError::LimitExceeded)?;
    for (index, value) in bodies.iter().cloned().enumerate() {
        let sequence = start
            .checked_add(u64::try_from(index).map_err(|_| StreamError::LimitExceeded)?)
            .ok_or(StreamError::LimitExceeded)?;
        records.push(Record {
            sequence,
            value: value.value,
            headers: value.headers,
            commit_id,
            committed_at_micros,
        });
    }
    Ok(records)
}

#[cfg(test)]
mod tests;
