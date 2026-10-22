struct CommitPlan {
    result: CommitOutcome,
    cached: CommitOutcome,
    histories: Vec<Arc<History>>,
    paths: Vec<(StreamPath, PathState)>,
    resident: Allocation,
    output_claim: Allocation,
    _scratch: Allocation,
}

impl CommitPlan {
    fn bind(&mut self, commit_id: CommitId, time: u64) -> Result<(), StreamError> {
        for output in [&mut self.result, &mut self.cached] {
            if let CommitOutcome::Committed(envelope) = output {
                bind_envelope(envelope, commit_id, time);
            }
        }
        for history in &mut self.histories {
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

    fn publish(
        self,
        state: &mut State,
        probe: &mut impl FnMut(),
        key: IdempotencyKey,
        digest: [u8; 32],
        time: u64,
    ) -> Result<(CommitOutcome, Allocation), StreamError> {
        let CommitOutcome::Committed(envelope) = &self.result else {
            unreachable!()
        };
        publish(state, self.paths, self.histories, envelope)?;
        state.decision += 1;
        state.last_committed_at_micros = time;
        retain(
            state,
            probe,
            None,
            Some(key),
            digest,
            IdempotencyOutcome::Commit(self.cached),
            self.resident,
        );
        Ok((self.result, self.output_claim))
    }
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
        let (mut facts, mut allocation) = self.vector(source.len())?;
        for mutation in source {
            let (start, bodies) = match mutation {
                CommitMutation::Append { path, records } => {
                    (state.paths.get(path).map_or(0, |path| path.tail), records)
                }
                CommitMutation::Fork {
                    at_tail, records, ..
                } => (*at_tail, records),
            };
            let (records, claim) = self.records(bodies, start, commit_id, time)?;
            self.absorb(&mut allocation, claim)?;
            facts.push(match mutation {
                CommitMutation::Append { path, .. } => CommittedMutation::Append(
                    crate::preparation::append_fact(path.clone(), start, records)?,
                ),
                CommitMutation::Fork {
                    source,
                    destination,
                    at_tail,
                    ..
                } => CommittedMutation::Fork(CommittedFork {
                    source: source.clone(),
                    destination: destination.clone(),
                    forked_at: *at_tail,
                    tail: start + records.len() as u64,
                    records,
                }),
            });
        }
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
            CommittedMutation::Fork(fork) => Some(Arc::new(History::Prefix {
                source: state
                    .paths
                    .get(&fork.source)
                    .ok_or(StreamError::NotFound)?
                    .history
                    .clone(),
                tail: fork.forked_at,
                _allocation: self.allocate(std::mem::size_of::<History>())?,
            })),
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
        Ok(Arc::new(History::Batch {
            parent,
            records,
            _allocation: self.allocate(std::mem::size_of::<History>())?,
        }))
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
            let claim = self.adopt(std::mem::size_of::<(StreamPath, PathState)>())?;
            let (changed, _) = watch::channel(0);
            paths.push((
                destination
                    .as_str()
                    .get(..end)
                    .and_then(|ancestor| destination.ancestor(ancestor))
                    .ok_or(StreamError::InvalidPath)?,
                PathState {
                    children: ChildIndex::default(),
                    next_sibling: None,
                    history: None,
                    prefix: [None, None],
                    tail: 0,
                    changed,
                    follows: None,
                    _allocation: claim,
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
        let (facts, output_claim) = self.facts(state, &request.mutations, commit_id, time)?;
        let (histories, mut scratch) = self.histories(state, &facts)?;
        let (paths, paths_claim) =
            self.paths(state, request.mutations.iter().map(mutation_path))?;
        let result = CommitOutcome::Committed(CommittedEnvelope {
            commit_id,
            mutations: facts,
        });
        let (cached, resident) = self.copy_commit(&result)?;
        self.absorb(&mut scratch, paths_claim)?;
        Ok(CommitPlan {
            result,
            cached,
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
        if let Some(replay) = self.replay(state, request.idempotency_key.as_bytes())? {
            if replay.digest != digest {
                return Err(StreamError::IdempotencyMismatch);
            }
            let IdempotencyOutcome::Commit(result) = &replay.result else {
                return Err(StreamError::IdempotencyMismatch);
            };
            return self.copy_commit(result);
        }
        let check_deadline = || {
            if deadline.is_some_and(|deadline| self.clock.now_unix_millis() >= deadline) {
                Err(StreamError::DeadlineElapsed)
            } else {
                Ok(())
            }
        };
        check_deadline()?;
        admit_replay(state, Some(&request.idempotency_key), self.limits)?;
        let mut probes =
            self.reserve_replay_probes(u64::from(state.replay_height(state.replay_root)))?;
        let (mut conflicts, output_claim) = self.vector(request.conditions.len())?;
        for condition in &request.conditions {
            if let Some(conflict) = self.condition_conflict(state, condition)? {
                conflicts.push(conflict);
            }
        }
        if !conflicts.is_empty() {
            let result = CommitOutcome::Conflict(conflicts);
            let (cached, resident) = self.copy_commit(&result)?;
            self.reserve_retained(state)?;
            return self.publish(0, || {
                check_deadline()?;
                retain(
                    state,
                    &mut || self.record_replay_probe(&mut probes),
                    None,
                    Some(request.idempotency_key),
                    digest,
                    IdempotencyOutcome::Commit(cached),
                    resident,
                );
                Ok((result, output_claim))
            });
        }
        drop((conflicts, output_claim));
        crate::preparation::authority(&request, |path| {
            state.paths.get(path).map(|path| path.tail)
        })?;
        reserve_coordinated(state, &request, self.limits)?;
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
        self.publish(envelope_payload_bytes(&plan.result), || {
            check_deadline()?;
            let time = state
                .last_committed_at_micros
                .max(self.commit_clock.now_unix_millis().saturating_mul(1_000));
            plan.bind(commit_id, time)?;
            plan.publish(
                state,
                &mut || self.record_replay_probe(&mut probes),
                request.idempotency_key,
                digest,
                time,
            )
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
        Ok(match condition {
            CommitCondition::Tail { expected, .. } | CommitCondition::Head { expected, .. }
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
                let stream = state.paths.get(path).ok_or(StreamError::Unavailable)?;
                let actual = self.preceding_commit(stream)?;
                (actual != *commit_id).then(|| CommitConflict::Head {
                    path: path.clone(),
                    expected: *commit_id,
                    actual,
                })
            }
            _ => None,
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

fn envelope_payload_bytes(result: &CommitOutcome) -> usize {
    match result {
        CommitOutcome::Committed(envelope) => envelope
            .mutations
            .iter()
            .flat_map(fact_records)
            .map(|record| record.payload_bytes())
            .sum(),
        CommitOutcome::Conflict(_) => 0,
    }
}

fn bind_envelope(envelope: &mut CommittedEnvelope, commit_id: CommitId, time: u64) {
    envelope.commit_id = commit_id;
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
) -> Result<(), StreamError> {
    let hierarchy_changed = !paths.is_empty();
    for (path, stream) in paths {
        state.path_bytes += path.as_str().len();
        state.paths.insert(path.clone(), stream);
        state.index_child(path)?;
        state.hierarchy_version = envelope.commit_id;
    }
    for (fact, history) in envelope.mutations.iter().zip(histories) {
        publish_fact(state, fact, history)?;
    }
    if hierarchy_changed && let Some(changed) = &state.hierarchy_changed {
        changed.send_replace(());
    }
    Ok(())
}

fn publish_fact(
    state: &mut State,
    fact: &CommittedMutation,
    history: Arc<History>,
) -> Result<(), StreamError> {
    let (path, tail) = match fact {
        CommittedMutation::Append(append) => (&append.path, append.tail),
        CommittedMutation::Fork(fork) => (&fork.destination, fork.tail),
    };
    let source = match fact {
        CommittedMutation::Append(append) => &append.path,
        CommittedMutation::Fork(fork) => &fork.source,
    };
    let mut prefix = state
        .paths
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
    let stream = state.paths.get_mut(path).ok_or(StreamError::Unavailable)?;
    stream.prefix = prefix;
    stream.history = Some(history);
    stream.tail = tail;
    #[cfg(all(acyclic_loom, feature = "local"))]
    let notify = !crate::verification::fault_is(crate::verification::Fault::OmitAppendWake);
    #[cfg(not(all(acyclic_loom, feature = "local")))]
    let notify = true;
    if notify {
        stream.changed.send_replace(tail);
        if let Some(index) = &stream.follows {
            index
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .publish(fact_records(fact), tail);
        }
    }
    state.record_count += fact_records(fact).len();
    state.payload_bytes += fact_records(fact)
        .iter()
        .map(|record| record.payload_bytes())
        .sum::<usize>();
    Ok(())
}
