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
    records: &[AppendRecord],
    limits: MemoryLimits,
) -> Result<(), StreamError> {
    reserve_paths(state, path, limits)?;
    reserve_commit(state, limits)?;
    let bytes = records
        .iter()
        .map(AppendRecord::payload_bytes)
        .sum::<usize>();
    reserve_records(state, records.len(), bytes, limits)
}

fn commit_identity(decision: u64, digest: [u8; 32]) -> CommitId {
    let mut hash = Sha256::new();
    hash.update(b"acyclic.stream.commit.v1\0");
    hash.update(decision.to_le_bytes());
    hash.update(digest);
    CommitId::from_bytes(hash.finalize().into())
}

impl MemoryStream {
    fn replay_outcome<'a>(
        &self,
        state: &'a State,
        key: Option<&crate::IdempotencyKey>,
        digest: [u8; 32],
    ) -> Result<Option<&'a IdempotencyOutcome>, StreamError> {
        let Some(key) = key else { return Ok(None) };
        let Some(replay) = self.replay(state, key.as_bytes())? else {
            return Ok(None);
        };
        if replay.digest != digest {
            return Err(StreamError::IdempotencyMismatch);
        }
        Ok(Some(&replay.result))
    }

    fn replay_append(
        &self,
        state: &State,
        key: Option<&crate::IdempotencyKey>,
        digest: [u8; 32],
    ) -> Result<Option<AppendOutcome>, StreamError> {
        match self.replay_outcome(state, key, digest)? {
            Some(IdempotencyOutcome::Append(result)) => Ok(Some(result.clone())),
            Some(_) => Err(StreamError::IdempotencyMismatch),
            None => Ok(None),
        }
    }

    fn replay_fork(
        &self,
        state: &State,
        key: Option<&crate::IdempotencyKey>,
        digest: [u8; 32],
    ) -> Result<Option<ForkReceipt>, StreamError> {
        match self.replay_outcome(state, key, digest)? {
            Some(IdempotencyOutcome::Fork(result)) => Ok(Some(result.clone())),
            Some(_) => Err(StreamError::IdempotencyMismatch),
            None => Ok(None),
        }
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
    probe: &mut impl FnMut(),
    envelope: Option<CommittedEnvelope>,
    key: Option<crate::IdempotencyKey>,
    digest: [u8; 32],
    result: IdempotencyOutcome,
    allocation: Allocation,
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
        replay_left: None,
        replay_right: None,
        replay_height: 1,
        digest,
        result,
        envelope,
        _allocation: allocation,
    };
    if entry.envelope().is_some() || entry.replay.is_some() {
        let keyed = entry.replay.is_some();
        let index = state.retained.len();
        state.retained.push(entry);
        if keyed {
            state.replay_root = Some(state.insert_replay(state.replay_root, index, probe));
            state.replay_count += 1;
        }
    }
}
