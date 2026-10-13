fn reserve_commit(state: &State, limits: MemoryLimits) -> Result<(), StreamError> {
    if state.decision.saturating_add(reserved(state).0 as u64) >= limits.commits as u64 {
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

fn replay<'a>(
    state: &'a State,
    key: Option<&crate::IdempotencyKey>,
    digest: [u8; 32],
) -> Result<Option<&'a IdempotencyOutcome>, StreamError> {
    let Some(key) = key else { return Ok(None) };
    let Some(replay) = state.replay(key.as_bytes()) else {
        return Ok(None);
    };
    if replay.digest != digest {
        return Err(StreamError::IdempotencyMismatch);
    }
    Ok(Some(&replay.result))
}

fn replay_append(
    state: &State,
    key: Option<&crate::IdempotencyKey>,
    digest: [u8; 32],
) -> Result<Option<AppendOutcome>, StreamError> {
    match replay(state, key, digest)? {
        Some(IdempotencyOutcome::Append(result)) => Ok(Some(result.clone())),
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
        Some(IdempotencyOutcome::Fork(result)) => Ok(Some(result.clone())),
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
        && state.replay_count().saturating_add(reserved(state).0) >= limits.idempotency_results
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
    if entry.envelope().is_some() || entry.replay.is_some() {
        state.retained.push(entry);
    }
}
