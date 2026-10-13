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
    fn bind(&mut self, commit_id: CommitId, time: u64) {
        for output in [&mut self.result, &mut self.cached] {
            if let CommitOutcome::Committed(envelope) = output {
                bind_envelope(envelope, commit_id, time);
            }
        }
        for history in &mut self.histories {
            if let History::Batch { records, .. } =
                Arc::get_mut(history).expect("unpublished history")
            {
                for record in &mut Arc::get_mut(records).expect("unpublished records").records {
                    record.commit_id = commit_id;
                    record.committed_at_micros = time;
                }
            }
        }
    }

    fn publish(
        self,
        state: &mut State,
        key: IdempotencyKey,
        digest: [u8; 32],
        time: u64,
    ) -> (CommitOutcome, Allocation) {
        let CommitOutcome::Committed(envelope) = &self.result else {
            unreachable!()
        };
        publish(state, self.paths, self.histories, envelope);
        state.decision += 1;
        state.last_committed_at_micros = time;
        retain(
            state,
            None,
            Some(key),
            digest,
            IdempotencyOutcome::Commit(self.cached),
        );
        state
            .retained
            .back_mut()
            .expect("retained commit")
            ._allocation = self.resident;
        (self.result, self.output_claim)
    }
}

impl MemoryStream {
    fn records(
        &self,
        bodies: &[Bytes],
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
        let records = crate::preparation::records(start, bodies, commit_id, time, buffer)?;
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
            return Ok(parent.expect("empty fork prefix"));
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
        let (mut paths, allocation) =
            self.vector(missing_paths(state, destinations.clone()).count())?;
        for (destination, end) in missing_paths(state, destinations) {
            let claim = self.adopt(std::mem::size_of::<(StreamPath, PathState)>())?;
            let (changed, _) = watch::channel(0);
            paths.push((
                StreamPath(destination.0.slice(..end)),
                PathState {
                    history: None,
                    tail: 0,
                    changed,
                    _allocation: claim,
                },
            ));
        }
        Ok((paths, allocation))
    }

    fn retained(
        &self,
        state: &mut State,
        key: bool,
        committed: bool,
    ) -> Result<Allocation, StreamError> {
        let allocation = self.retained_claim(key, committed)?;
        self.reserve_retained(state)?;
        Ok(allocation)
    }

    fn retained_claim(&self, key: bool, committed: bool) -> Result<Allocation, StreamError> {
        self.adopt(
            usize::from(key) * std::mem::size_of::<(Bytes, usize)>()
                + usize::from(committed) * std::mem::size_of::<(CommitId, usize)>(),
        )
    }

    fn reserve_retained(&self, state: &mut State) -> Result<(), StreamError> {
        let needed = state
            .retained
            .len()
            .checked_add(reserved(state).0)
            .and_then(|len| len.checked_add(1))
            .ok_or(StreamError::Capacity)?;
        if needed > state.retained.capacity() {
            let (mut buffer, claim) = self.retention_buffer(needed.next_power_of_two())?;
            buffer.append(&mut state.retained);
            state.retained = buffer;
            state.retained_allocation = claim;
        }
        Ok(())
    }

    fn retention_buffer(
        &self,
        capacity: usize,
    ) -> Result<(VecDeque<Retained>, Allocation), StreamError> {
        let claim = self.allocate(capacity * std::mem::size_of::<Retained>())?;
        #[cfg(not(target_arch = "wasm32"))]
        let mut claim = claim;
        let mut buffer = VecDeque::new();
        buffer
            .try_reserve_exact(capacity)
            .map_err(|_| StreamError::Capacity)?;
        #[cfg(not(target_arch = "wasm32"))]
        if let Some(claim) = &mut claim {
            self.work.as_ref().expect("native claim context").record(
                claim.reconcile_capacity(buffer.capacity() * std::mem::size_of::<Retained>()),
            )?;
        }
        Ok((buffer, claim))
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
        let (cached, mut resident) = self.copy_commit(&result)?;
        self.absorb(&mut resident, self.retained_claim(true, true)?)?;
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
        if let Some(replay) = state.replay(request.idempotency_key.as_bytes()) {
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
        let (mut conflicts, output_claim) = self.vector(request.conditions.len())?;
        conflicts.extend(
            request
                .conditions
                .iter()
                .filter_map(|condition| condition_conflict(state, condition)),
        );
        if !conflicts.is_empty() {
            let result = CommitOutcome::Conflict(conflicts);
            let (cached, mut resident) = self.copy_commit(&result)?;
            self.absorb(&mut resident, self.retained(state, true, false)?)?;
            return self.publish(0, || {
                check_deadline()?;
                retain(
                    state,
                    None,
                    Some(request.idempotency_key),
                    digest,
                    IdempotencyOutcome::Commit(cached),
                );
                state
                    .retained
                    .back_mut()
                    .expect("retained conflict")
                    ._allocation = resident;
                Ok((result, output_claim))
            });
        }
        drop(conflicts);
        drop(output_claim);
        validate_commit_authority(state, &request)?;
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
            plan.bind(commit_id, time);
            Ok(plan.publish(state, request.idempotency_key, digest, time))
        })
    }
}

fn fact_records(fact: &CommittedMutation) -> &Vec<Record> {
    match fact {
        CommittedMutation::Append(append) => &append.records,
        CommittedMutation::Fork(fork) => &fork.records,
    }
}

fn condition_conflict(state: &State, condition: &CommitCondition) -> Option<CommitConflict> {
    let path = condition_path(condition);
    let actual = state.paths.get(path).map(|path| path.tail);
    match condition {
        CommitCondition::Tail { expected, .. } if actual != Some(*expected) => {
            Some(CommitConflict::Tail {
                path: path.clone(),
                expected: *expected,
                actual,
            })
        }
        CommitCondition::Absent { .. } if actual.is_some() => {
            Some(CommitConflict::Exists { path: path.clone() })
        }
        _ => None,
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
            .map(|record| record.value.len())
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
) {
    for (path, stream) in paths {
        state.path_bytes += path.as_str().len();
        state.paths.insert(path, stream);
        state.hierarchy_version = envelope.commit_id;
    }
    for (fact, history) in envelope.mutations.iter().zip(histories) {
        publish_fact(state, fact, history);
    }
}

fn publish_fact(state: &mut State, fact: &CommittedMutation, history: Arc<History>) {
    let (path, tail) = match fact {
        CommittedMutation::Append(append) => (&append.path, append.tail),
        CommittedMutation::Fork(fork) => (&fork.destination, fork.tail),
    };
    let stream = state.paths.get_mut(path).expect("prepared destination");
    stream.history = Some(history);
    stream.tail = tail;
    stream.changed.send_replace(tail);
    state.record_count += fact_records(fact).len();
    state.payload_bytes += fact_records(fact)
        .iter()
        .map(|record| record.value.len())
        .sum::<usize>();
}
