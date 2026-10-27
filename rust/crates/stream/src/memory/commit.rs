struct CommitPlan {
    envelope: CommittedEnvelope,
    result: CommitOutcome,
    histories: Vec<Arc<History>>,
    paths: Vec<(StreamPath, PathState)>,
    resident: Allocation,
    output_claim: Allocation,
    _scratch: Allocation,
}

impl CommitPlan {
    fn bind(&mut self, commit_id: CommitId, time: u64) -> Result<(), StreamError> {
        bind_envelope(&mut self.envelope, commit_id, time);
        if let CommitOutcome::Committed(receipt) = &mut self.result {
            receipt.commit_id = commit_id;
            receipt.committed_at_micros = time;
            for fork in &mut receipt.forks {
                fork.commit_id = commit_id;
            }
        }
        bind_histories(&mut self.histories, commit_id, time)
    }

    fn publish(
        self,
        state: &mut State,
        build: &mut NamespaceBuilder<'_>,
        probe: &mut impl FnMut(),
        key: IdempotencyKey,
        digest: [u8; 32],
        time: u64,
    ) -> Result<(CommitOutcome, Allocation), StreamError> {
        publish(state, self.paths, self.histories, &self.envelope, build)?;
        state.decision += 1;
        state.last_committed_at_micros = time;
        retain(
            state,
            probe,
            None,
            Some(key),
            digest,
            IdempotencyOutcome::Commit(self.envelope),
            self.resident,
        );
        Ok((self.result, self.output_claim))
    }
}

fn bind_histories(
    histories: &mut [Arc<History>],
    commit_id: CommitId,
    time: u64,
) -> Result<(), StreamError> {
    for history in histories {
        if let History::Batch { records, .. } =
            Arc::get_mut(history).ok_or(StreamError::Unavailable)?
        {
            for record in &mut Arc::get_mut(records)
                .ok_or(StreamError::Unavailable)?
                .records
            {
                record.commit_id = commit_id;
                record.committed_at_micros = time;
            }
        }
    }
    Ok(())
}

impl MemoryStream {
    fn records(
        &self,
        bodies: &[AppendRecord],
        start: u64,
        commit_id: CommitId,
        time: u64,
    ) -> Result<(Vec<Record>, Allocation), StreamError> {
        start
            .checked_add(bodies.len() as u64)
            .ok_or(StreamError::LimitExceeded)?;
        let (buffer, claim) = self.vector(bodies.len())?;
        #[cfg(not(target_arch = "wasm32"))]
        if let Some(work) = &self.work {
            work.record(work.meter.admit_examination(
                bodies.len() as u64,
                (bodies.len() * std::mem::size_of::<Record>()) as u64,
            ))?;
        }
        let mut records = crate::preparation::records(start, bodies, commit_id, time, buffer)?;
        #[cfg(not(target_arch = "wasm32"))]
        if let Some(work) = &self.work {
            for record in &mut records {
                record.headers = work.record(record.headers.bind_native(&work.meter))?;
            }
        }
        #[cfg(not(target_arch = "wasm32"))]
        if let Some(work) = &self.work {
            work.record(work.meter.record_examination(
                bodies.len() as u64,
                (bodies.len() * std::mem::size_of::<Record>()) as u64,
            ))?;
        }
        Ok((records, claim))
    }

    fn facts(
        &self,
        state: &State,
        source: &[CommitMutation],
        commit_id: CommitId,
        time: u64,
    ) -> Result<(Vec<CommittedMutation>, Allocation), StreamError> {
        let (output, mut allocation) = self.vector(source.len())?;
        let facts = crate::preparation::facts(
            source,
            |path| Ok(state.paths.get(path).map(|path| path.tail)),
            |start, bodies| {
                let (records, claim) = self.records(bodies, start, commit_id, time)?;
                self.absorb(&mut allocation, claim)?;
                Ok(records)
            },
            output,
        )?;
        Ok((facts, allocation))
    }
    fn histories(
        &self,
        state: &State,
        facts: &[CommittedMutation],
    ) -> Result<(Vec<Arc<History>>, Allocation), StreamError> {
        let (mut histories, allocation) = self.vector(facts.len())?;
        for fact in facts {
            histories.push(self.history(state, fact)?);
        }
        Ok((histories, allocation))
    }

    fn history(
        &self,
        state: &State,
        fact: &CommittedMutation,
    ) -> Result<Arc<History>, StreamError> {
        let parent = match fact {
            CommittedMutation::Append(append) => state
                .paths
                .get(&append.path)
                .and_then(|path| path.history.clone()),
            CommittedMutation::Fork(fork) => {
                let source = state
                    .paths
                    .get(&fork.source)
                    .ok_or(StreamError::NotFound)?
                    .history
                    .clone();
                let jump = self.history_jump(source.as_ref(), fork.forked_at)?;
                Some(Arc::new(History::Prefix {
                    source,
                    jump,
                    tail: fork.forked_at,
                    _allocation: self.allocate(std::mem::size_of::<History>())?,
                }))
            }
        };
        let source = fact_records(fact);
        if source.is_empty() {
            return parent.ok_or(StreamError::Unavailable);
        }
        let (mut records, mut claim) = self.vector(source.len())?;
        #[cfg(not(target_arch = "wasm32"))]
        if let Some(work) = &self.work {
            work.record(work.meter.admit_examination(
                source.len() as u64,
                (source.len() * std::mem::size_of::<Record>()) as u64,
            ))?;
        }
        records.extend(source.iter().cloned());
        #[cfg(not(target_arch = "wasm32"))]
        if let Some(work) = &self.work {
            work.record(work.meter.record_examination(
                source.len() as u64,
                (source.len() * std::mem::size_of::<Record>()) as u64,
            ))?;
        }
        self.absorb(
            &mut claim,
            self.allocate(std::mem::size_of::<RecordBatch>())?,
        )?;
        let records = Arc::new(RecordBatch {
            records,
            _allocation: claim,
        });
        let jump = self.history_jump(
            parent.as_ref(),
            records
                .records
                .first()
                .ok_or(StreamError::Unavailable)?
                .sequence,
        )?;
        Ok(Arc::new(History::Batch {
            parent,
            jump,
            records,
            _allocation: self.allocate(std::mem::size_of::<History>())?,
        }))
    }

    fn history_jump(
        &self,
        parent: Option<&Arc<History>>,
        boundary: u64,
    ) -> Result<HistoryJump, StreamError> {
        HistoryJump::new(parent, boundary, || {
            #[cfg(not(target_arch = "wasm32"))]
            if let Some(work) = &self.work {
                work.record(inspect(&work.meter, || ()))?;
            }
            Ok(())
        })
    }

    fn paths<'a>(
        &self,
        state: &'a State,
        destinations: impl Iterator<Item = &'a StreamPath> + Clone + 'a,
    ) -> Result<(Vec<(StreamPath, PathState)>, Allocation), StreamError> {
        let count = missing_paths(state, destinations.clone()).count();
        let (mut paths, allocation) = self.vector(count)?;
        if count == 0 {
            return Ok((paths, allocation));
        }
        for (destination, end) in missing_paths(state, destinations) {
            paths.push((
                destination
                    .as_str()
                    .get(..end)
                    .and_then(|ancestor| destination.ancestor(ancestor))
                    .ok_or(StreamError::InvalidPath)?,
                PathState {
                    history: None,
                    prefix: [None, None],
                    tail: 0,
                },
            ));
        }
        Ok((paths, allocation))
    }

    fn reserve_retained(&self, state: &mut State) -> Result<(), StreamError> {
        let needed = state
            .retained
            .len()
            .checked_add(reserved(state).0)
            .and_then(|len| len.checked_add(1))
            .ok_or(StreamError::Capacity)?;
        if needed > state.retained.capacity() {
            let (mut buffer, claim) = self.vector(needed.next_power_of_two())?;
            #[cfg(not(target_arch = "wasm32"))]
            let copied = (state.retained.len() * std::mem::size_of::<Retained>()) as u64;
            #[cfg(not(target_arch = "wasm32"))]
            if let Some(work) = &self.work {
                work.record(work.meter.admit_codec(0, copied, 0))?;
            }
            buffer.append(&mut state.retained);
            state.retained = buffer;
            state.retained_allocation = claim;
            #[cfg(not(target_arch = "wasm32"))]
            if let Some(work) = &self.work {
                work.record(work.meter.record_codec(0, copied, 0))?;
            }
        }
        Ok(())
    }

    fn plan_commit(
        &self,
        state: &mut State,
        request: &CommitRequest,
        commit_id: CommitId,
        time: u64,
    ) -> Result<CommitPlan, StreamError> {
        let (facts, facts_claim) = self.facts(state, &request.mutations, commit_id, time)?;
        let (histories, mut scratch) = self.histories(state, &facts)?;
        let (paths, paths_claim) =
            self.paths(state, request.mutations.iter().map(mutation_path))?;
        let envelope = CommittedEnvelope {
            commit_id,
            committed_at_micros: time,
            mutations: facts,
        };
        let (receipt, output_claim) = self.receipt(&envelope)?;
        let result = CommitOutcome::Committed(receipt);
        let resident = facts_claim;
        self.absorb(&mut scratch, paths_claim)?;
        Ok(CommitPlan {
            envelope,
            result,
            histories,
            paths,
            resident,
            output_claim,
            _scratch: scratch,
        })
    }

    fn commit_locked(
        &self,
        state: &mut State,
        request: CommitRequest,
        digest: [u8; 32],
        deadline: Option<u64>,
    ) -> Result<(CommitOutcome, Allocation), StreamError> {
        self.plan_commit_locked(state, request, digest, deadline)?
            .install(self, state)
    }

    fn plan_commit_locked<'a>(
        &'a self,
        state: &mut State,
        request: CommitRequest,
        digest: [u8; 32],
        deadline: Option<u64>,
    ) -> Result<
        Decision<
            (CommitOutcome, Allocation),
            impl FnOnce(&mut State, bool) -> Result<(CommitOutcome, Allocation), StreamError> + use<'a>,
        >,
        StreamError,
    > {
        if let Some(replay) = self.replay(state, request.idempotency_key.as_bytes())? {
            if replay.digest != digest {
                return Err(StreamError::IdempotencyMismatch);
            }
            let IdempotencyOutcome::Commit(result) = &replay.result else {
                return Err(StreamError::IdempotencyMismatch);
            };
            return self.receipt(result)
                .map(|(receipt, claim)| Decision::Returned((CommitOutcome::Committed(receipt), claim)));
        }
        let deadline = deadline.into_iter().chain(request.deadline_unix_millis).min();
        let check_deadline = move || {
            if deadline.is_some_and(|deadline| self.clock.now_unix_millis() >= deadline) {
                Err(StreamError::DeadlineElapsed)
            } else {
                Ok(())
            }
        };
        check_deadline()?;
        let mut probes =
            self.reserve_replay_probes(u64::from(state.replay_height(state.replay_root)))?;
        let (mut conflicts, output_claim) = self.vector(request.conditions.len())?;
        for condition in &request.conditions {
            if let Some(conflict) = self.condition_conflict(state, condition)? {
                conflicts.push(conflict);
            }
        }
        if !conflicts.is_empty() {
            check_deadline()?;
            let result = CommitOutcome::Conflict(conflicts);
            // Old behaviour, for negative controls only: the conflict spends the key.
            #[cfg(all(loom, feature = "local"))]
            if crate::verification::fault_is(crate::verification::Fault::RetainConflictKey) {
                admit_replay(state, Some(&request.idempotency_key), self.limits)?;
                retain(
                    state,
                    &mut || self.record_replay_probe(&mut probes),
                    None,
                    Some(request.idempotency_key),
                    digest,
                    // Spending a refused key is the injected defect. Its invalid
                    // retained family cannot be emitted by successful commits.
                    IdempotencyOutcome::Append(AppendOutcome::TailConflict { actual_tail: 0 }),
                    Default::default(),
                );
            }
            return Ok(Decision::Returned((result, output_claim)));
        }
        drop((conflicts, output_claim));
        crate::preparation::authority(&request, |path| {
            Ok(state.paths.get(path).map(|path| path.tail))
        })?;
        admit_replay(state, Some(&request.idempotency_key), self.limits)?;
        reserve_coordinated(state, &request, self.limits)?;
        self.reserve_commit_index(&mut probes)?;
        let decision = state.decision.checked_add(1).ok_or(StreamError::Capacity)?;
        #[cfg(not(target_arch = "wasm32"))]
        if let Some(work) = &self.work {
            work.record(work.meter.admit_codec(0, 0, identity_bytes()))?;
        }
        let commit_id = commit_identity(decision, digest);
        #[cfg(not(target_arch = "wasm32"))]
        if let Some(work) = &self.work {
            work.record(work.meter.record_codec(0, 0, identity_bytes()))?;
        }
        self.reserve_retained(state)?;
        let mut plan = self.plan_commit(state, &request, commit_id, 0)?;
        Ok(Decision::Accepted {
            bytes: envelope_payload_bytes(&plan.envelope),
            install: move |state: &mut State, durable: bool| {
                if !durable {
                    check_deadline()?;
                }
                let time = crate::preparation::committed_time(
                    state.last_committed_at_micros,
                    self.commit_clock.now_unix_millis(),
                );
                plan.bind(commit_id, time)?;
                plan.publish(
                    state,
                    &mut NamespaceBuilder::new(&|bytes| self.allocate(bytes)),
                    &mut || self.record_replay_probe(&mut probes),
                    request.idempotency_key,
                    digest,
                    time,
                )
            },
        })
    }
}

fn fact_records(fact: &CommittedMutation) -> &Vec<Record> {
    match fact {
        CommittedMutation::Append(append) => &append.records,
        CommittedMutation::Fork(fork) => &fork.records,
    }
}

impl MemoryStream {
    fn condition_conflict(
        &self,
        state: &State,
        condition: &CommitCondition,
    ) -> Result<Option<CommitConflict>, StreamError> {
        let path = condition_path(condition);
        let actual = state.paths.get(path).map(|path| path.tail);
        crate::preparation::condition_conflict(condition, actual, || {
            let stream = state.paths.get(path).ok_or(StreamError::Unavailable)?;
            self.preceding_commit(stream)
        })
    }

    fn preceding_commit(&self, path: &PathState) -> Result<CommitId, StreamError> {
        let sequence = path
            .tail
            .checked_sub(1)
            .ok_or(StreamError::InvalidArgument)?;
        let mut walk = HistoryWalk::new(path.history.as_ref(), path.tail, sequence);
        loop {
            #[cfg(not(target_arch = "wasm32"))]
            let next = if let Some(work) = &self.work {
                work.record(inspect(&work.meter, || walk.next()))?
            } else {
                walk.next()
            };
            #[cfg(target_arch = "wasm32")]
            let next = walk.next();
            let Some((History::Batch { records, .. }, _)) = next else {
                if next.is_none() {
                    return Err(StreamError::Unavailable);
                }
                continue;
            };
            let lookup = || {
                let first = records.records.first()?.sequence;
                let offset = usize::try_from(sequence.checked_sub(first)?).ok()?;
                records.records.get(offset).map(|record| record.commit_id)
            };
            #[cfg(not(target_arch = "wasm32"))]
            let commit = if let Some(work) = &self.work {
                work.record(inspect(&work.meter, lookup))?
            } else {
                lookup()
            };
            #[cfg(target_arch = "wasm32")]
            let commit = lookup();
            if let Some(commit) = commit {
                return Ok(commit);
            }
        }
    }
}

#[cfg(not(target_arch = "wasm32"))]
fn identity_bytes() -> u64 {
    b"acyclic.stream.commit.v1\0".len() as u64 + 8 + 32
}

fn envelope_payload_bytes(envelope: &CommittedEnvelope) -> usize {
    envelope.mutations.iter().flat_map(fact_records)
        .map(|record| record.payload_bytes()).sum()
}

fn bind_envelope(envelope: &mut CommittedEnvelope, commit_id: CommitId, time: u64) {
    envelope.commit_id = commit_id;
    envelope.committed_at_micros = time;
    for mutation in &mut envelope.mutations {
        let records = match mutation {
            CommittedMutation::Append(append) => &mut append.records,
            CommittedMutation::Fork(fork) => &mut fork.records,
        };
        for record in records {
            record.commit_id = commit_id;
            record.committed_at_micros = time;
        }
    }
}

fn publish(
    state: &mut State,
    paths: Vec<(StreamPath, PathState)>,
    histories: Vec<Arc<History>>,
    envelope: &CommittedEnvelope,
    build: &mut NamespaceBuilder<'_>,
) -> Result<(), StreamError> {
    let mut next = state.paths.clone();
    for (path, data) in paths {
        next.put_data(&path, data, None, build)?;
    }
    for (fact, history) in envelope.mutations.iter().zip(histories) {
        let mut origin = None;
        let (path, tail) = match fact {
            CommittedMutation::Append(append) => (&append.path, append.tail),
            CommittedMutation::Fork(fork) => {
                let subtree = state
                    .paths
                    .node(&fork.source)
                    .ok_or(StreamError::NotFound)?
                    .clone();
                #[cfg(all(loom, feature = "local"))]
                let subtree =
                    if crate::verification::fault_is(crate::verification::Fault::RootOnlyFork) {
                        build.node(subtree.data.clone(), None, subtree.origin.clone())?
                    } else {
                        subtree
                    };
                #[cfg(all(loom, feature = "local"))]
                let excludes = if crate::verification::fault_is(
                    crate::verification::Fault::IgnoreForkExclusions,
                ) {
                    &[][..]
                } else {
                    &fork.exclude[..]
                };
                #[cfg(not(all(loom, feature = "local")))]
                let excludes = &fork.exclude[..];
                let subtree = Namespace::excluded(&subtree, excludes, build)?;
                origin = Some(ForkMarker {
                    source: fork.source.clone(),
                    snapshot: subtree.clone(),
                    forked_at: fork.forked_at,
                    commit_id: envelope.commit_id,
                    decision: state.decision.checked_add(1).ok_or(StreamError::Capacity)?,
                });
                next.put(&fork.destination, subtree, build)?;
                (&fork.destination, fork.tail)
            }
        };
        // Root prefix and histories are derived from the same old cut even
        // when this envelope also appends to the source.
        let data = fact_data(&state.paths, fact, history, tail)?;
        next.put_data(path, data, origin, build)?;
    }
    if next.len() != state.paths.len() {
        state.hierarchy_version = envelope.commit_id;
    }
    state.path_bytes = next.bytes();
    state.paths = next;
    for fact in &envelope.mutations {
        let (path, tail) = match fact {
            CommittedMutation::Append(append) => (&append.path, append.tail),
            CommittedMutation::Fork(fork) => (&fork.destination, fork.tail),
        };
        #[cfg(all(loom, feature = "local"))]
        let notify = !crate::verification::fault_is(crate::verification::Fault::OmitAppendWake);
        #[cfg(not(all(loom, feature = "local")))]
        let notify = true;
        if notify && let Some(signals) = state.signals.get(path) {
            signals.changed.send_replace(tail);
            signals
                .follows
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .publish(fact_records(fact), tail);
        }
        state.record_count += fact_records(fact).len();
        state.payload_bytes += fact_records(fact)
            .iter()
            .map(|record| record.payload_bytes())
            .sum::<usize>();
    }
    Ok(())
}
fn fact_data(
    paths: &Namespace,
    fact: &CommittedMutation,
    history: Arc<History>,
    tail: u64,
) -> Result<PathState, StreamError> {
    let source = match fact {
        CommittedMutation::Append(append) => &append.path,
        CommittedMutation::Fork(fork) => &fork.source,
    };
    let mut prefix = paths
        .get(source)
        .map_or([None, None], |path| path.prefix.clone());
    if let CommittedMutation::Fork(fork) = fact {
        for (slot, sequence) in prefix.iter_mut().zip([0_u64, 1]) {
            if sequence >= fork.forked_at {
                *slot = None;
            }
        }
    }
    if let History::Batch { records, .. } = history.as_ref()
        && let Some(first) = records.records.first()
    {
        for (slot, sequence) in prefix.iter_mut().zip([0_u64, 1]) {
            if slot.is_none()
                && let Some(index) = sequence
                    .checked_sub(first.sequence)
                    .and_then(|index| usize::try_from(index).ok())
                    .filter(|&index| index < records.records.len())
            {
                *slot = Some(HistoryWindow {
                    records: Arc::clone(records),
                    start: index,
                    end: index + 1,
                });
            }
        }
    }
    Ok(PathState {
        prefix,
        history: Some(history),
        tail,
    })
}
