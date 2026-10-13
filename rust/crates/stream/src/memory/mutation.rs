fn reserve_commit(state: &State, limits: MemoryLimits) -> Result<(), StreamError> {
    if state.commits.len().saturating_add(reserved(state).0) >= limits.commits {
        Err(StreamError::Capacity)
    } else {
        Ok(())
    }
}

fn reserve_append(
    state: &State,
    path: &StreamPath,
    records: &[Bytes],
    limits: MemoryLimits,
) -> Result<(), StreamError> {
    reserve_paths(state, path, limits)?;
    reserve_commit(state, limits)?;
    let bytes = records.iter().map(Bytes::len).sum::<usize>();
    reserve_records(state, records.len(), bytes, limits)
}

fn commit_identity(decision: u64, digest: [u8; 32]) -> CommitId {
    let mut hash = Sha256::new();
    hash.update(b"acyclic.stream.commit.v1\0");
    hash.update(decision.to_le_bytes());
    hash.update(digest);
    CommitId::from_bytes(hash.finalize().into())
}

fn replay(
    state: &State,
    key: Option<&crate::IdempotencyKey>,
    digest: [u8; 32],
) -> Result<Option<IdempotencyOutcome>, StreamError> {
    let Some(key) = key else { return Ok(None) };
    let Some(replay) = state.replay(key.as_bytes()) else {
        return Ok(None);
    };
    if replay.digest != digest {
        return Err(StreamError::IdempotencyMismatch);
    }
    Ok(Some(replay.result.clone()))
}

fn replay_append(
    state: &State,
    key: Option<&crate::IdempotencyKey>,
    digest: [u8; 32],
) -> Result<Option<AppendOutcome>, StreamError> {
    match replay(state, key, digest)? {
        Some(IdempotencyOutcome::Append(result)) => Ok(Some(result)),
        Some(_) => Err(StreamError::IdempotencyMismatch),
        None => Ok(None),
    }
}

fn replay_fork(
    state: &State,
    key: Option<&crate::IdempotencyKey>,
    digest: [u8; 32],
) -> Result<Option<ForkReceipt>, StreamError> {
    match replay(state, key, digest)? {
        Some(IdempotencyOutcome::Fork(result)) => Ok(Some(result)),
        Some(_) => Err(StreamError::IdempotencyMismatch),
        None => Ok(None),
    }
}

fn admit_replay(
    state: &State,
    key: Option<&crate::IdempotencyKey>,
    limits: MemoryLimits,
) -> Result<(), StreamError> {
    if key.is_some()
        && state.replays.len().saturating_add(reserved(state).0) >= limits.idempotency_results
    {
        Err(StreamError::Capacity)
    } else {
        Ok(())
    }
}

/// Retains a mutation envelope and its replay result indefinitely.
fn retain(
    state: &mut State,
    envelope: Option<CommittedEnvelope>,
    key: Option<crate::IdempotencyKey>,
    digest: [u8; 32],
    result: IdempotencyOutcome,
) {
    let envelope = if matches!(
        &result,
        IdempotencyOutcome::Commit(CommitOutcome::Committed(_))
    ) {
        None
    } else {
        envelope
    };
    let entry = Retained {
        replay: key.map(|key| key.0),
        digest,
        result,
        envelope,
        _allocation: Default::default(),
    };
    let index = state.retained.len();
    if let Some(envelope) = entry.envelope() {
        state.commits.insert(envelope.commit_id, index);
    }
    if let Some(key) = &entry.replay {
        state.replays.insert(key.clone(), index);
    }
    if entry.envelope().is_some() || entry.replay.is_some() {
        state.retained.push_back(entry);
    }
}

fn validate_commit_authority(state: &State, request: &CommitRequest) -> Result<(), StreamError> {
    crate::preparation::authority(request, |path| state.paths.get(path).map(|path| path.tail))
}
