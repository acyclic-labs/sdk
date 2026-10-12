//! Canonical mutation facts prepared against pre-commit observations.

use std::collections::BTreeMap;

use crate::OwnedBytes;

use crate::{
    AppendOutcome, AppendReceipt, AppendRequest, CommitCondition, CommitConflict, CommitId,
    CommitMutation, CommitOutcome, CommitRequest, CommittedAppend, CommittedEnvelope,
    CommittedFork, CommittedMutation, ForkReceipt, ForkRequest, Record, StreamError, StreamPath,
};
use crate::memory::{MemoryObserver, MemoryOutput, MemoryResult, provider_result};

/// Pre-commit existence and exclusive tails.
pub type ObservedTails = BTreeMap<StreamPath, Option<u64>>;

/// Prepares an append against the observed tail (absence is tail zero).
pub fn append(
    request: &AppendRequest,
    observed_tail: Option<u64>,
    commit_id: CommitId,
    committed_at_micros: u64,
) -> Result<(AppendOutcome, Option<CommittedEnvelope>), StreamError> {
    crate::request::append_digest(request)?;
    let start = observed_tail.unwrap_or(0);
    if request.if_tail.is_some_and(|expected| expected != start) {
        return Ok((AppendOutcome::TailConflict { actual_tail: start }, None));
    }
    let fact = provider_result(append_fact(
        &request.path,
        start,
        request.records.clone(),
        commit_id,
        committed_at_micros, &MemoryObserver::default(),
    ))?.value;
    let receipt = AppendReceipt {
        start: fact.start,
        end: fact.end,
        tail: fact.tail,
        commit_id,
    };
    Ok((
        AppendOutcome::Committed(receipt),
        Some(CommittedEnvelope {
            commit_id,
            mutations: vec![CommittedMutation::Append(fact)],
        }),
    ))
}

/// Prepares an immutable-prefix fork against pre-commit source and destination existence.
pub fn fork(
    request: &ForkRequest,
    source_tail: Option<u64>,
    destination_tail: Option<u64>,
    commit_id: CommitId,
) -> Result<(ForkReceipt, CommittedEnvelope), StreamError> {
    crate::request::fork_digest(request)?;
    let forked_at = fork_cut(request, source_tail, destination_tail)?;
    let receipt = ForkReceipt {
        source: request.source.clone(),
        destination: request.destination.clone(),
        forked_at,
        tail: forked_at,
        commit_id,
    };
    let envelope = CommittedEnvelope {
        commit_id,
        mutations: vec![CommittedMutation::Fork(CommittedFork {
            source: request.source.clone(),
            destination: request.destination.clone(),
            forked_at,
            tail: forked_at,
            records: Vec::new(),
        })],
    };
    Ok((receipt, envelope))
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

pub(crate) fn append_fact<I>(
    path: &StreamPath,
    start: u64,
    bodies: I,
    commit_id: CommitId,
    committed_at_micros: u64,
    observer: &MemoryObserver,
) -> MemoryResult<MemoryOutput<CommittedAppend>>
where
    I: IntoIterator<Item = OwnedBytes>,
    I::IntoIter: ExactSizeIterator,
{
    let records = records_with_observer(start, bodies, commit_id, committed_at_micros, observer)?;
    let end = start.checked_add(records.value.len() as u64).ok_or(StreamError::LimitExceeded)?;
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

/// Normalizes and validates a commit, checks exact conditions and source authority, and constructs its immutable envelope using the accepted ID and timestamp.
pub fn commit(
    request: &mut CommitRequest,
    observed: &ObservedTails,
    commit_id: CommitId,
    committed_at_micros: u64,
) -> Result<CommitOutcome, StreamError> {
    crate::request::commit_digest(request)?;
    for condition in &request.conditions {
        let path = match condition {
            CommitCondition::Tail { path, .. } | CommitCondition::Absent { path } => path,
        };
        if !observed.contains_key(path) {
            return Err(StreamError::InvalidArgument);
        }
    }
    for mutation in &request.mutations {
        let paths = match mutation {
            CommitMutation::Append { path, .. } => [Some(path), None],
            CommitMutation::Fork {
                source,
                destination,
                ..
            } => [Some(source), Some(destination)],
        };
        if paths
            .into_iter()
            .flatten()
            .any(|path| !observed.contains_key(path))
        {
            return Err(StreamError::InvalidArgument);
        }
    }
    let tail = |path: &StreamPath| observed.get(path).copied().flatten();
    let conflicts = provider_result(conditions(&request.conditions, tail, &MemoryObserver::default()))?.value;
    if !conflicts.is_empty() {
        return Ok(CommitOutcome::Conflict(conflicts));
    }
    provider_result(authority(request, tail, &MemoryObserver::default()))?;
    Ok(CommitOutcome::Committed(CommittedEnvelope {
        commit_id,
        mutations: provider_result(mutations(&request.mutations, tail, commit_id, committed_at_micros, &MemoryObserver::default()))?.value,
    }))
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
                let start = tail(path).unwrap_or(0);
                let records = records_with_observer(
                    start, bodies.iter().cloned(), commit_id, committed_at_micros, observer,
                )?;
                let end = start + records.value.len() as u64;
                output.allocation.absorb(records.allocation)?;
                CommittedMutation::Append(CommittedAppend {
                    path: observer.retain_path(path)?, start, end, tail: end, records: records.value,
                })
            }
            CommitMutation::Fork { source, destination, at_tail, records: bodies } => {
                let source_tail = tail(source).ok_or(StreamError::NotFound)?;
                if *at_tail > source_tail { return Err(StreamError::PrefixNotRetained.into()); }
                let records = records_with_observer(
                    *at_tail, bodies.iter().cloned(), commit_id, committed_at_micros, observer,
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

pub(crate) fn records_with_observer<I>(
    start: u64,
    bodies: I,
    commit_id: CommitId,
    committed_at_micros: u64,
    observer: &MemoryObserver,
) -> MemoryResult<MemoryOutput<Vec<Record>>>
where
    I: IntoIterator<Item = OwnedBytes>,
    I::IntoIter: ExactSizeIterator,
{
    let mut bodies = bodies.into_iter();
    let count = bodies.len();
    // Reserve the exclusive tail too: a record at u64::MAX is unrepresentable.
    start.checked_add(u64::try_from(count).map_err(|_| StreamError::LimitExceeded)?)
        .ok_or(StreamError::LimitExceeded)?;
    let mut output = observer.vec(count)?;
    for index in 0..count {
        // A borrowed iterator clones its immutable alias only after admission;
        // an owned Vec iterator simply transfers it, without an avoidable clone.
        observer.admit_item()?;
        let value = bodies.next().ok_or(StreamError::Unavailable)?;
        output.value.push(Record {
            sequence: start + index as u64,
            value,
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
    use crate::{IdempotencyKey, MemoryStream, StreamProvider};

    fn key(value: &'static [u8]) -> Result<IdempotencyKey, StreamError> {
        IdempotencyKey::new(OwnedBytes::from_static(value))
    }

    #[tokio::test]
    async fn prepared_append_and_fork_match_provider_envelopes() -> Result<(), StreamError> {
        let provider = MemoryStream::default();
        let request = AppendRequest {
            path: StreamPath::new("source")?,
            records: vec![OwnedBytes::from_static(b"one")],
            if_tail: Some(0),
            idempotency_key: Some(key(b"append")?),
        };
        let outcome = provider.append(request.clone()).await?;
        let AppendOutcome::Committed(receipt) = &outcome else {
            return Err(StreamError::Unavailable);
        };
        let envelope = provider.read_commit(receipt.commit_id).await?;
        let Some(CommittedMutation::Append(fact)) = envelope.mutations.first() else {
            return Err(StreamError::Unavailable);
        };
        assert_eq!(
            append(
                &request,
                None,
                receipt.commit_id,
                fact.records
                    .first()
                    .ok_or(StreamError::Unavailable)?
                    .committed_at_micros
            )?,
            (outcome, Some(envelope))
        );
        let request = ForkRequest {
            source: request.path,
            destination: StreamPath::new("fork")?,
            at_tail: None,
            idempotency_key: Some(key(b"fork")?),
        };
        let receipt = provider.fork(request.clone()).await?;
        let envelope = provider.read_commit(receipt.commit_id).await?;
        assert_eq!(
            fork(&request, Some(1), None, receipt.commit_id)?,
            (receipt, envelope)
        );
        assert_eq!(
            fork(&request, Some(1), Some(0), CommitId::default()),
            Err(StreamError::AlreadyExists)
        );
        assert_eq!(
            fork(&request, None, None, CommitId::default()),
            Err(StreamError::NotFound)
        );
        Ok(())
    }

    #[tokio::test]
    async fn coordinated_preparation_matches_precommit_fork_and_append() -> Result<(), StreamError>
    {
        let provider = MemoryStream::default();
        let source = StreamPath::new("z-source")?;
        let destination = StreamPath::new("a-fork")?;
        provider
            .append(AppendRequest {
                path: source.clone(),
                records: vec![OwnedBytes::from_static(b"before")],
                if_tail: None,
                idempotency_key: None,
            })
            .await?;
        let mut request = CommitRequest {
            conditions: vec![
                CommitCondition::Tail {
                    path: source.clone(),
                    expected: 1,
                },
                CommitCondition::Absent {
                    path: destination.clone(),
                },
            ],
            mutations: vec![
                CommitMutation::Append {
                    path: source.clone(),
                    records: vec![OwnedBytes::from_static(b"new-source")],
                },
                CommitMutation::Fork {
                    source: source.clone(),
                    destination: destination.clone(),
                    at_tail: 1,
                    records: vec![OwnedBytes::from_static(b"new-fork")],
                },
            ],
            idempotency_key: key(b"coordinated")?,
        };
        let outcome = provider.commit(request.clone()).await?;
        let CommitOutcome::Committed(envelope) = &outcome else {
            return Err(StreamError::Unavailable);
        };
        let Some(CommittedMutation::Fork(fact)) = envelope.mutations.first() else {
            return Err(StreamError::Unavailable);
        };
        let observed = [(source.clone(), Some(1)), (destination.clone(), None)].into();
        assert_eq!(
            commit(
                &mut request,
                &observed,
                envelope.commit_id,
                fact.records
                    .first()
                    .ok_or(StreamError::Unavailable)?
                    .committed_at_micros
            )?,
            outcome
        );
        let conflict = commit(
            &mut request,
            &[(source, Some(2)), (destination, None)].into(),
            CommitId::default(),
            0,
        )?;
        assert!(matches!(conflict, CommitOutcome::Conflict(_)));
        assert_eq!(
            commit(&mut request, &ObservedTails::new(), CommitId::default(), 0),
            Err(StreamError::InvalidArgument)
        );
        Ok(())
    }

    #[test]
    fn preparation_rejects_unrepresentable_exclusive_tails() -> Result<(), StreamError> {
        let request = AppendRequest {
            path: StreamPath::new("full")?,
            records: vec![OwnedBytes::from_static(b"overflow")],
            if_tail: Some(u64::MAX),
            idempotency_key: None,
        };
        assert_eq!(
            append(&request, Some(u64::MAX), CommitId::default(), 0),
            Err(StreamError::LimitExceeded)
        );
        assert!(matches!(
            append(&request, Some(0), CommitId::default(), 0)?,
            (AppendOutcome::TailConflict { actual_tail: 0 }, None)
        ));
        Ok(())
    }
}
