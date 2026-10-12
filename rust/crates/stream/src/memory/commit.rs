use super::*;

impl MemoryStream {
    pub(crate) async fn commit_value(
        &self,
        mut request: CommitRequest,
        deadline: Option<u64>,
        committed_at: Option<u64>,
    ) -> Result<CommitOutcome, StreamError> {
        let result = provider_result(
            self.apply_commit(
                &mut request,
                deadline,
                None,
                None::<&mut fn() -> std::future::Ready<Result<(), std::convert::Infallible>>>,
                committed_at,
            )
            .await,
        )?;
        let (outcome, error, _) = result.unwrap_or_else(|never| match never {});
        assert!(error.is_none(), "ordinary commits have no fallible meter");
        Ok(outcome.value)
    }

    #[cfg(not(target_arch = "wasm32"))]
    pub(super) fn commit_future(
        &self,
        mut request: CommitRequest,
        deadline: Option<u64>,
        meter: Arc<dyn NativeReadMeter>,
    ) -> NativeProviderFuture<'_, CommitOutcome> {
        let original = Arc::clone(&meter);
        NativeProviderFuture::new(&original, async move {
            match self
                .apply_commit(
                    &mut request,
                    deadline,
                    Some(&meter),
                    None::<&mut fn() -> std::future::Ready<Result<(), StreamError>>>,
                    None,
                )
                .await
            {
                Ok(Ok((outcome, error, _))) => match error {
                    Some(error) => Err(error),
                    None => native(Ok(outcome), &meter),
                },
                Ok(Err(error)) => Ok(Err(error)),
                Err(error) => native(Err(error), &meter),
            }
        })
    }

    pub(crate) async fn apply_commit<E, F, Fut, C>(
        &self,
        request: &mut CommitRequest,
        deadline_unix_millis: Option<u64>,
        meter: WireMeter<'_>,
        mut before: Option<&mut F>,
        committed_at: Option<u64>,
    ) -> MemoryResult<
        Result<
            (
                NativeStreamOutput<CommitOutcome>,
                Option<std::io::Error>,
                Option<C>,
            ),
            E,
        >,
    >
    where
        E: Send,
        F: FnMut() -> Fut + Send,
        Fut: std::future::Future<Output = Result<C, E>> + Send,
        C: Send,
    {
        check(meter)?;
        normalize_commit(request, meter)?;
        validate_commit_shape(request, meter)?;
        let digest = commit_digest(request, meter, None)?;
        let bytes = request
            .mutations
            .iter()
            .map(|mutation| match mutation {
                CommitMutation::Append { records, .. } | CommitMutation::Fork { records, .. } => {
                    records.iter().map(OwnedBytes::len).sum::<usize>()
                }
            })
            .sum();
        boundary(meter, true)?;
        let mut write = crate::memory::bytes(meter, true, bytes)?;
        loop {
            let mut state = self.state.write().await;
            if before.is_some() {
                state.prepared_capacity.current_revision()?;
            }
            let mut attempt = self.prepare_commit_attempt(
                &mut state,
                request,
                deadline_unix_millis,
                digest,
                meter,
                committed_at,
            )?;
            if before.is_some() && attempt.decision.is_some() {
                let mut slots = 0_u64;
                let CommitOutcome::Committed(envelope) = &attempt.outcome.value else {
                    unreachable!("a new canonical decision has a committed envelope")
                };
                for mutation in &envelope.mutations {
                    admit_item(meter)?;
                    let records = match mutation {
                        CommittedMutation::Append(append) => &append.records,
                        CommittedMutation::Fork(fork) => &fork.records,
                    };
                    slots += 1 + records.len() as u64;
                    record_item(meter)?;
                }
                #[cfg(not(target_arch = "wasm32"))]
                if let Some(meter) = meter {
                    meter.admit_examination(slots * 4, 0)?;
                }
            }
            let continuation = if let Some(before) = before.as_deref_mut() {
                let version = CommitStateVersion::capture(&state)?;
                drop(state);
                let continuation = match before().await {
                    Ok(value) => value,
                    Err(error) => return Ok(Err(error)),
                };
                state = match self.state.try_write() {
                    Ok(state) => state,
                    Err(_) => {
                        // Release the continuation before waiting on contention.
                        drop(continuation);
                        drop(attempt);
                        drop(self.state.write().await);
                        continue;
                    }
                };
                if !version.matches(&state)? {
                    drop(state);
                    drop(continuation);
                    drop(attempt);
                    continue;
                }
                Some(continuation)
            } else {
                None
            };
            if continuation.is_some() && attempt.decision.is_some() {
                let committed_at = state.last_committed_at_micros.max(
                    committed_at
                        .unwrap_or_else(|| self.clock.now_unix_millis())
                        .saturating_mul(1_000),
                );
                attempt.bind_timestamp(committed_at, meter)?;
            }
            let MemoryCommitAttempt {
                outcome,
                publication,
                decision: _,
                committed_at_micros,
            } = attempt;
            let error = if let Some(prepared) = publication {
                check(meter)?;
                if deadline_unix_millis
                    .is_some_and(|deadline| self.clock.now_unix_millis() >= deadline)
                {
                    return Err(StreamError::DeadlineElapsed.into());
                }
                // Publish without awaiting after the guard and state recheck.
                publish(&mut state, prepared, digest);
                state.last_committed_at_micros = committed_at_micros;
                record_bytes(&mut write, bytes).err()
            } else {
                None
            };
            return Ok(Ok((outcome, error, continuation)));
        }
    }

    pub(super) fn prepare_commit_attempt(
        &self,
        state: &mut State,
        request: &CommitRequest,
        deadline_unix_millis: Option<u64>,
        digest: [u8; 32],
        meter: WireMeter<'_>,
        committed_at: Option<u64>,
    ) -> MemoryResult<MemoryCommitAttempt> {
        admit_item(meter)?;
        let replay = state.replays.get(request.idempotency_key.as_bytes());
        record_item(meter)?;
        if let Some(replay) = replay {
            if replay.digest != digest {
                return Err(StreamError::IdempotencyMismatch.into());
            }
            let IdempotencyOutcome::Commit(outcome) = &replay.result else {
                return Err(StreamError::IdempotencyMismatch.into());
            };
            return Ok(MemoryCommitAttempt {
                outcome: clone_commit_outcome(outcome, meter, false)?,
                publication: None,
                decision: None,
                committed_at_micros: state.last_committed_at_micros,
            });
        }
        if deadline_unix_millis.is_some_and(|deadline| self.clock.now_unix_millis() >= deadline) {
            return Err(StreamError::DeadlineElapsed.into());
        }
        admit_replay(state, Some(&request.idempotency_key), self.limits)?;
        let conflicts = crate::preparation::conditions(
            &request.conditions,
            |path| state.paths.get(path).map(|stream| stream.tail),
            meter,
        )?;
        let entry = std::mem::size_of::<(OwnedBytes, Arc<Replay>)>();
        let mut resident = adopt(meter, entry)?;
        let (outcome, paths, mutations, decision, committed_at_micros) =
            if conflicts.value.is_empty() {
                drop(conflicts);
                crate::preparation::authority(
                    request,
                    |path| state.paths.get(path).map(|stream| stream.tail),
                    meter,
                )?;
                let missing =
                    missing_paths(state, request.mutations.iter().map(mutation_path), meter)?;
                let (records, bytes) = request.mutations.iter().fold(
                    (0_usize, 0_usize),
                    |(count, bytes), mutation| {
                        let records = match mutation {
                            CommitMutation::Append { records, .. }
                            | CommitMutation::Fork { records, .. } => records,
                        };
                        (
                            count.saturating_add(records.len()),
                            bytes.saturating_add(records.iter().map(OwnedBytes::len).sum()),
                        )
                    },
                );
                if reserve(state, &missing.value, records, bytes, self.limits).is_err() {
                    recount(state, meter)?;
                    reserve(state, &missing.value, records, bytes, self.limits)?;
                }
                let decision = state
                    .commits
                    .len()
                    .checked_add(1)
                    .and_then(|count| u64::try_from(count).ok())
                    .ok_or(StreamError::Capacity)?;
                let commit_id = planned_commit_id(decision, digest, meter)?;
                let committed_at_micros = state.last_committed_at_micros.max(
                    committed_at
                        .unwrap_or_else(|| self.clock.now_unix_millis())
                        .saturating_mul(1_000),
                );
                let facts = crate::preparation::mutations(
                    &request.mutations,
                    |path| state.paths.get(path).map(|stream| stream.tail),
                    commit_id,
                    committed_at_micros,
                    meter,
                )?;
                let envelope = facts.map(|mutations| CommittedEnvelope {
                    commit_id,
                    mutations,
                });
                let mutations = prepare_mutations(state, &envelope.value, meter)?;
                let paths = prepare_paths(&missing.value, &envelope.value, meter)?;
                absorb(
                    &mut resident,
                    adopt(meter, std::mem::size_of::<(CommitId, Arc<Replay>)>())?,
                )?;
                let outcome = envelope.map(CommitOutcome::Committed);
                (
                    outcome,
                    paths,
                    mutations,
                    Some(decision),
                    committed_at_micros,
                )
            } else {
                let outcome = conflicts.map(CommitOutcome::Conflict);
                (
                    outcome,
                    vec(meter, 0)?,
                    vec(meter, 0)?,
                    None,
                    state.last_committed_at_micros,
                )
            };
        let replay = clone_commit_outcome(&outcome.value, meter, false)?;
        let cached = cache_replay(
            meter,
            None,
            IdempotencyOutcome::Commit(replay.value),
            replay.allocation,
        )?;
        let key = request.idempotency_key.0.clone();
        prepare_retained_capacity(state, meter)?;
        let publication = PreparedPublication {
            cached,
            key: Some(key),
            paths,
            mutations,
            resident,
        };
        Ok(MemoryCommitAttempt {
            outcome,
            publication: Some(publication),
            decision,
            committed_at_micros,
        })
    }

    pub(crate) async fn apply_append(
        &self,
        request: &AppendRequest,
        meter: WireMeter<'_>,
        committed_at: Option<u64>,
    ) -> MemoryResult<(NativeStreamOutput<AppendOutcome>, Option<std::io::Error>)> {
        check(meter)?;
        validate_records(&request.records, meter)?;
        let encoded_len =
            crate::wire_codec::CanonicalWire::Append(request).encoded_len(None, meter)?;
        if encoded_len > MAX_COMMAND_BYTES {
            return Err(StreamError::LimitExceeded.into());
        }
        let digest = append_digest(request, meter)?;
        boundary(meter, true)?;
        let bytes = request.records.iter().map(OwnedBytes::len).sum();
        let mut write = crate::memory::bytes(meter, true, bytes)?;
        let mut state = self.state.write().await;
        admit_item(meter)?;
        let replay = request
            .idempotency_key
            .as_ref()
            .and_then(|key| state.replays.get(key.as_bytes()));
        record_item(meter)?;
        if let Some(replay) = replay {
            if replay.digest != digest {
                return Err(StreamError::IdempotencyMismatch.into());
            }
            let IdempotencyOutcome::Append(outcome) = &replay.result else {
                return Err(StreamError::IdempotencyMismatch.into());
            };
            return Ok((
                NativeStreamOutput {
                    value: outcome.clone(),
                    allocation: allocate(meter, 0)?,
                },
                None,
            ));
        }
        admit_replay(&state, request.idempotency_key.as_ref(), self.limits)?;
        admit_item(meter)?;
        let actual_tail = state.paths.get(&request.path).map_or(0, |path| path.tail);
        record_item(meter)?;
        let mut resident = allocate(meter, 0)?;
        if request.idempotency_key.is_some() {
            absorb(
                &mut resident,
                adopt(meter, std::mem::size_of::<(OwnedBytes, Arc<Replay>)>())?,
            )?;
        }
        let (outcome, envelope, cached_allocation, paths, mutations, decision, committed_at) =
            if request
                .if_tail
                .is_some_and(|expected| expected != actual_tail)
            {
                (
                    AppendOutcome::TailConflict { actual_tail },
                    None,
                    Allocation::default(),
                    vec(meter, 0)?,
                    vec(meter, 0)?,
                    None,
                    state.last_committed_at_micros,
                )
            } else {
                let missing = missing_paths(&state, std::iter::once(&request.path), meter)?;
                if reserve(
                    &state,
                    &missing.value,
                    request.records.len(),
                    bytes,
                    self.limits,
                )
                .is_err()
                {
                    recount(&mut state, meter)?;
                    reserve(
                        &state,
                        &missing.value,
                        request.records.len(),
                        bytes,
                        self.limits,
                    )?;
                }
                let decision = state
                    .commits
                    .len()
                    .checked_add(1)
                    .and_then(|count| u64::try_from(count).ok())
                    .ok_or(StreamError::Capacity)?;
                let commit_id = planned_commit_id(decision, digest, meter)?;
                let committed_at = state.last_committed_at_micros.max(
                    committed_at
                        .unwrap_or_else(|| self.clock.now_unix_millis())
                        .saturating_mul(1_000),
                );
                let mut facts = vec(meter, 1)?;
                let fact = crate::preparation::append_fact(
                    &request.path,
                    actual_tail,
                    &request.records,
                    commit_id,
                    committed_at,
                    meter,
                )?;
                let outcome = AppendOutcome::Committed(AppendReceipt {
                    start: fact.value.start,
                    end: fact.value.end,
                    tail: fact.value.tail,
                    commit_id,
                });
                absorb(&mut facts.allocation, fact.allocation)?;
                facts.value.push(CommittedMutation::Append(fact.value));
                let envelope = CommittedEnvelope {
                    commit_id,
                    mutations: facts.value,
                };
                let mutations = prepare_mutations(&state, &envelope, meter)?;
                let paths = prepare_paths(&missing.value, &envelope, meter)?;
                absorb(
                    &mut resident,
                    adopt(meter, std::mem::size_of::<(CommitId, Arc<Replay>)>())?,
                )?;
                (
                    outcome,
                    Some(envelope),
                    facts.allocation,
                    paths,
                    mutations,
                    Some(decision),
                    committed_at,
                )
            };
        let key = request.idempotency_key.as_ref().map(|key| key.0.clone());
        if envelope.is_some() || key.is_some() {
            prepare_retained_capacity(&mut state, meter)?;
        }
        let output = NativeStreamOutput {
            value: outcome.clone(),
            allocation: allocate(meter, 0)?,
        };
        let prepared = PreparedPublication {
            cached: cache_replay(
                meter,
                envelope,
                IdempotencyOutcome::Append(outcome),
                cached_allocation,
            )?,
            key,
            paths,
            mutations,
            resident,
        };
        check(meter)?;
        publish(&mut state, prepared, digest);
        state.last_committed_at_micros = committed_at;
        let error = if decision.is_some() {
            record_bytes(&mut write, bytes).err()
        } else {
            None
        };
        Ok((output, error))
    }
}

pub(super) struct MemoryCommitAttempt {
    pub(super) outcome: NativeStreamOutput<CommitOutcome>,
    pub(super) publication: Option<PreparedPublication>,
    pub(super) decision: Option<u64>,
    pub(super) committed_at_micros: u64,
}

impl MemoryCommitAttempt {
    pub(super) fn bind_timestamp(
        &mut self,
        committed_at: u64,
        meter: WireMeter<'_>,
    ) -> MemoryResult<()> {
        pub(super) fn envelope(
            envelope: &mut CommittedEnvelope,
            committed_at: u64,
            meter: WireMeter<'_>,
        ) -> MemoryResult<()> {
            for mutation in &mut envelope.mutations {
                record_item(meter)?;
                let records = match mutation {
                    CommittedMutation::Append(append) => &mut append.records,
                    CommittedMutation::Fork(fork) => &mut fork.records,
                };
                for record in records {
                    record.committed_at_micros = committed_at;
                    record_item(meter)?;
                }
            }
            Ok(())
        }
        let CommitOutcome::Committed(output) = &mut self.outcome.value else {
            return Err(StreamError::Unavailable.into());
        };
        let publication = self.publication.as_mut().ok_or(StreamError::Unavailable)?;
        let cached = Arc::get_mut(&mut publication.cached).ok_or(StreamError::Unavailable)?;
        let replay = cached.envelope_mut().ok_or(StreamError::Unavailable)?;
        envelope(output, committed_at, meter)?;
        envelope(replay, committed_at, meter)?;
        for mutation in &mut publication.mutations.value {
            record_item(meter)?;
            let history = Arc::get_mut(&mut mutation.history).ok_or(StreamError::Unavailable)?;
            if let History::Batch { records, .. } = history {
                let records = Arc::get_mut(records).ok_or(StreamError::Unavailable)?;
                for record in &mut records.value {
                    record.committed_at_micros = committed_at;
                    record_item(meter)?;
                }
            }
        }
        self.committed_at_micros = committed_at;
        Ok(())
    }
}
