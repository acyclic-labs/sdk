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

impl MemoryStream {
    fn replay_outcome<'a>(
        &self,
        state: &'a State,
        key: &crate::IdempotencyKey,
        digest: [u8; 32],
    ) -> Result<Option<&'a IdempotencyOutcome>, StreamError> {
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
        key: &crate::IdempotencyKey,
        digest: [u8; 32],
    ) -> Result<Option<AppendOutcome>, StreamError> {
        match self.replay_outcome(state, key, digest)? {
            Some(IdempotencyOutcome::Append(result)) => {
                let mut result = result.clone();
                if let AppendOutcome::Committed(receipt) = &mut result {
                    receipt.revision = self.revisions.at(receipt.revision.index);
                }
                Ok(Some(result))
            }
            Some(_) => Err(StreamError::IdempotencyMismatch),
            None => Ok(None),
        }
    }

    fn replay_fork(
        &self,
        state: &State,
        key: &crate::IdempotencyKey,
        digest: [u8; 32],
    ) -> Result<Option<ForkReceipt>, StreamError> {
        match self.replay_outcome(state, key, digest)? {
            Some(IdempotencyOutcome::Fork(result)) => Ok(Some(ForkReceipt {
                revision: self.revisions.at(result.revision.index),
                ..result.clone()
            })),
            Some(_) => Err(StreamError::IdempotencyMismatch),
            None => Ok(None),
        }
    }
}

fn admit_replay(state: &State, limits: MemoryLimits) -> Result<(), StreamError> {
    if state.replay_count().saturating_add(reserved(state).0) >= limits.idempotency_results {
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
    (allocation, mut fact): (Allocation, RetainedFactRef),
) {
    let data = fact
        .get_mut()
        .unwrap_or_else(|| unreachable!("retained reservation is exclusively owned"));
    data.previous = state.retained.last().map(|entry| entry.fact.clone());
    data.replay = key.map(|key| key.0);
    data.digest = digest;
    data.index = state.decision;
    data.result = result;
    data.envelope = envelope;
    data._allocation = allocation;
    let header = if data.previous.is_some() {
        data._header.take()
    } else {
        None
    };
    let entry = Retained {
        fact,
        replay_left: None,
        replay_right: None,
        replay_height: 1,
        commit_branch: None,
    };
    if entry.envelope().is_some() || entry.replay.is_some() {
        if let Some(previous) = &entry.fact.previous {
            previous
                .next
                .set(RetainedLink {
                    fact: entry.fact.downgrade(),
                    _allocation: header,
                })
                .unwrap_or_else(|_| unreachable!("the retained tail has one successor"));
        }
        let keyed = entry.replay.is_some();
        let committed = entry.envelope().is_some();
        let index = state.retained.len();
        state.retained.push(entry);
        if committed {
            state.insert_commit(index, probe);
        }
        if keyed {
            state.replay_root = Some(state.insert_replay(state.replay_root, index, probe));
            state.replay_count += 1;
        }
    }
}
