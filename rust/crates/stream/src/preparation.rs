//! Shared mutation facts, identities and conditions for stream backends.
use crate::AppendRecord;
use crate::request::fact_path;
use crate::{
    CommitCondition, CommitConflict, CommitId, CommitMutation, CommitRequest, CommittedAppend,
    CommittedFork, CommittedMutation, ForkRequest, Record, RelativePath, StreamError, StreamPath,
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
    let path = crate::CommitCondition::path(condition);
    Ok(match condition {
        CommitCondition::Present { .. } if actual.is_none() => {
            Some(CommitConflict::Missing { path: path.clone() })
        }
        CommitCondition::Tail { expected, .. } if actual.unwrap_or(0) != *expected => {
            Some(CommitConflict::Tail {
                path: path.clone(),
                expected: *expected,
                actual,
            })
        }
        CommitCondition::Head { expected, .. } if actual != Some(*expected) => {
            Some(CommitConflict::Tail {
                path: path.clone(),
                expected: *expected,
                actual,
            })
        }
        CommitCondition::Absent { .. } if actual.is_some() => {
            #[cfg(all(loom, feature = "local"))]
            if crate::verification::fault_is(crate::verification::Fault::IgnoreAbsent) {
                return Ok(None);
            }
            Some(CommitConflict::Exists { path: path.clone() })
        }
        CommitCondition::Head { commit_id, .. } => {
            #[cfg(all(loom, feature = "local"))]
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

/// Settles every effect against the authoritative pre-commit state. Every
/// failed condition of a hard effect is pushed into `conflicts`; when there is
/// none, each soft effect is skipped at its first failed condition and its
/// index pushed into `skipped`. The caller admitted `conflicts` with capacity
/// for [`hard_conditions`] and `skipped` for [`soft`]; neither grows, and no
/// soft condition is evaluated for a commit that conflicts.
pub(crate) fn settle(
    request: &CommitRequest,
    mut conflict: impl FnMut(&CommitCondition) -> Result<Option<CommitConflict>, StreamError>,
    mut conflicts: Vec<CommitConflict>,
    mut skipped: Vec<u32>,
) -> Result<(Vec<CommitConflict>, Vec<u32>), StreamError> {
    if !conflicts.is_empty()
        || conflicts.capacity() < hard_conditions(request)
        || !skipped.is_empty()
        || skipped.capacity() < soft(request)
    {
        return Err(StreamError::InvalidArgument);
    }
    for effect in request.effects.iter().filter(|effect| !effect.soft) {
        for condition in &effect.conditions {
            if let Some(failed) = conflict(condition)? {
                conflicts.push(failed);
            }
        }
    }
    if !conflicts.is_empty() {
        return Ok((conflicts, skipped));
    }
    for (index, effect) in request.effects.iter().enumerate() {
        if !effect.soft {
            continue;
        }
        for condition in &effect.conditions {
            if conflict(condition)?.is_some() {
                skipped.push(u32::try_from(index).map_err(|_| StreamError::LimitExceeded)?);
                break;
            }
        }
    }
    Ok((conflicts, skipped))
}

/// Whether `request` has a shape a commit may take: at least one mutation,
/// and only [admissible](crate::CommitEffect::admissible) effects.
pub(crate) fn shaped(request: &CommitRequest) -> bool {
    request.mutations().next().is_some()
        && request.effects.iter().all(crate::CommitEffect::admissible)
}

/// The number of conditions on hard effects: the conflicts [`settle`] can report.
pub(crate) fn hard_conditions(request: &CommitRequest) -> usize {
    request
        .effects
        .iter()
        .filter(|effect| !effect.soft)
        .map(|effect| effect.conditions.len())
        .sum()
}

/// The number of soft effects: the capacity [`settle`] and [`skipped`] need.
pub(crate) fn soft(request: &CommitRequest) -> usize {
    request.effects.iter().filter(|effect| effect.soft).count()
}

/// The number of mutations a commit that skipped `skipped` applies: the
/// capacity [`applied`] needs.
pub(crate) fn applied_len(request: &CommitRequest, skipped: &[u32]) -> usize {
    effects_applied(request, skipped)
        .map(|effect| effect.mutations.len())
        .sum()
}

fn effects_applied<'r: 's, 's>(
    request: &'r CommitRequest,
    skipped: &'s [u32],
) -> impl Iterator<Item = &'r crate::CommitEffect> + 's {
    request
        .effects
        .iter()
        .enumerate()
        .filter(move |(index, _)| {
            u32::try_from(*index).map_or(true, |index| skipped.binary_search(&index).is_err())
        })
        .map(|(_, effect)| effect)
}

/// The mutations a settled commit applies: those of every effect it did not
/// skip, in canonical path order, as references into the request. `applied`
/// was admitted by the caller with capacity for [`applied_len`]; it is filled
/// and sorted in place, and nothing else is copied.
pub(crate) fn applied<'a>(
    request: &'a CommitRequest,
    skipped: &[u32],
    mut applied: Vec<&'a CommitMutation>,
) -> Result<Vec<&'a CommitMutation>, StreamError> {
    if !applied.is_empty() || applied.capacity() < applied_len(request, skipped) {
        return Err(StreamError::InvalidArgument);
    }
    for effect in effects_applied(request, skipped) {
        applied.extend(effect.mutations.iter());
    }
    applied.sort_unstable_by(|left, right| {
        crate::CommitMutation::path(left).cmp(crate::CommitMutation::path(right))
    });
    Ok(applied)
}

/// The soft effects of `request` that its committed `envelope` skipped: an
/// effect applied iff its mutations have facts, and all or none of them do,
/// so its first path decides. A replayed commit reports exactly what the
/// original reported. `skipped` is admitted by the caller as in [`settle`].
pub(crate) fn skipped(
    request: &CommitRequest,
    envelope: &crate::CommittedEnvelope,
    mut skipped: Vec<u32>,
) -> Result<Vec<u32>, StreamError> {
    if !skipped.is_empty() || skipped.capacity() < soft(request) {
        return Err(StreamError::InvalidArgument);
    }
    for (index, effect) in request.effects.iter().enumerate() {
        if !effect.soft {
            continue;
        }
        let first = effect
            .mutations
            .first()
            .ok_or(StreamError::InvalidArgument)?;
        if envelope
            .mutations
            .binary_search_by(|fact| fact_path(fact).cmp(crate::CommitMutation::path(first)))
            .is_err()
        {
            skipped.push(u32::try_from(index).map_err(|_| StreamError::LimitExceeded)?);
        }
    }
    Ok(skipped)
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

/// Checks every applied fork's source cut against the pre-commit state.
pub(crate) fn authority(
    applied: &[&CommitMutation],
    mut tail: impl FnMut(&StreamPath) -> Result<Option<u64>, StreamError>,
) -> Result<(), StreamError> {
    for mutation in applied {
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
/// backend's record storage. All output containers are admitted by the caller;
/// the semantic planner never allocates a second unaccounted vector.
pub(crate) fn facts(
    source: &[&CommitMutation],
    mut tail: impl FnMut(&StreamPath) -> Result<Option<u64>, StreamError>,
    mut build: impl FnMut(
        &StreamPath,
        u64,
        &[AppendRecord],
        &[RelativePath],
    ) -> Result<(Vec<Record>, Vec<RelativePath>), StreamError>,
    mut output: Vec<CommittedMutation>,
) -> Result<Vec<CommittedMutation>, StreamError> {
    if !output.is_empty() || output.capacity() < source.len() {
        return Err(StreamError::InvalidArgument);
    }
    for mutation in source.iter().copied() {
        // A fork's own records belong to its destination.
        let (origin, start, bodies, exclusions) = match mutation {
            CommitMutation::Append { path, records } => {
                (path, tail(path)?.unwrap_or(0), records, &[][..])
            }
            CommitMutation::Fork {
                destination,
                at_tail,
                records,
                exclude,
                ..
            } => (destination, *at_tail, records, exclude.as_slice()),
        };
        let (records, exclude) = build(origin, start, bodies, exclusions)?;
        output.push(match mutation {
            CommitMutation::Append { path, .. } => {
                CommittedMutation::Append(append_fact(path.clone(), start, records)?)
            }
            CommitMutation::Fork {
                source,
                destination,
                at_tail,
                ..
            } => {
                let tail = start
                    .checked_add(
                        u64::try_from(records.len()).map_err(|_| StreamError::LimitExceeded)?,
                    )
                    .ok_or(StreamError::LimitExceeded)?;
                CommittedMutation::Fork(CommittedFork {
                    exclude,
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
    origin: &StreamPath,
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
            origin: origin.clone(),
        });
    }
    Ok(records)
}

#[cfg(test)]
mod tests;
