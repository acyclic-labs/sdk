impl MemoryStream {
    pub(crate) async fn append_inner(
        &self,
        request: AppendRequest,
    ) -> Result<(AppendOutcome, Allocation), StreamError> {
        let mut state = self.state.write().await;
        self.plan_append_locked(&mut state, &request)?
            .install(self, &mut state)
    }

    fn plan_append_locked<'a>(
        &'a self,
        state: &mut State,
        request: &'a AppendRequest,
    ) -> Result<
        Decision<
            (AppendOutcome, Allocation),
            impl FnOnce(&mut State, bool) -> Result<(AppendOutcome, Allocation), StreamError> + use<'a>,
        >,
        StreamError,
    > {
        #[cfg(not(target_arch = "wasm32"))]
        let _input = self.adopt(
            request.records.capacity() * std::mem::size_of::<AppendRecord>()
                + request
                    .records
                    .iter()
                    .map(|record| record.headers.owned_bytes())
                    .sum::<usize>(),
        )?;
        let bytes = request
            .records
            .iter()
            .map(AppendRecord::payload_bytes)
            .sum::<usize>();
        let digest = self.append_request_digest(&request)?;
        if let Some(result) = self.replay_append(&state, &request.idempotency_key, digest)? {
            return Ok(Decision::Returned((result, Default::default())));
        }
        let mut probes =
            self.reserve_replay_probes(u64::from(state.replay_height(state.replay_root)))?;
        let start = state.paths.get(&request.path).map_or(0, |path| path.tail);
        if request.if_tail.is_some_and(|expected| expected != start) {
            let result = AppendOutcome::TailConflict { actual_tail: start };
            return Ok(Decision::Returned((result, Default::default())));
        }
        admit_replay(&state, self.limits)?;
        reserve_append(&state, &request.path, &request.records, self.limits)?;
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
        let time = crate::preparation::committed_time(
            state.last_committed_at_micros,
            self.commit_clock.now_unix_millis(),
        );
        let (records, mut allocation) = self.records(&request.records, start, commit_id, time)?;
        let fact = crate::preparation::append_fact(request.path.clone(), start, records)?;
        let outcome = AppendOutcome::Committed(AppendReceipt {
            start,
            end: fact.end,
            tail: fact.tail,
            commit_id,
            revision: self.revisions.at(decision),
        });
        let (mut facts, claim) = self.vector(1)?;
        self.absorb(&mut allocation, claim)?;
        facts.push(CommittedMutation::Append(fact));
        let (mut histories, _histories) = self.histories(&state, &facts)?;
        let (paths, _paths) = self.paths(&state, std::iter::once(&request.path))?;
        let retained = self.reserve_retained(state)?;
        let mut envelope = CommittedEnvelope {
            commit_id,
            committed_at_micros: 0,
            mutations: facts,
        };
        bind_envelope(&mut envelope, commit_id, time);
        bind_histories(&mut histories, commit_id, time)?;
        let namespace = stage_namespace(
            state,
            paths,
            histories,
            &envelope,
            &mut NamespaceBuilder::new(&|bytes| self.allocate(bytes)),
        )?;
        Ok(Decision::Accepted {
            bytes,
            install: move |state: &mut State, _durable| {
                #[cfg(not(target_arch = "wasm32"))]
                let _input = _input;
                let _holds = (_histories, _paths);
                self.install_retained(state, retained)?;
                install_namespace(state, namespace, &envelope)?;
                state.decision = decision;
                state.last_committed_at_micros = time;
                retain(
                    state,
                    &mut || self.record_replay_probe(&mut probes),
                    Some(envelope),
                    Some(request.idempotency_key.clone()),
                    digest,
                    IdempotencyOutcome::Append(outcome.clone()),
                    allocation,
                );
                Ok((outcome, Default::default()))
            },
        })
    }
    fn append_request_digest(&self, request: &AppendRequest) -> Result<[u8; 32], StreamError> {
        #[cfg(not(target_arch = "wasm32"))]
        let hashed = b"acyclic.stream.request.v1\0".len() as u64
            + b"append".len() as u64
            + 24
            + request.path.as_str().len() as u64
            + request
                .records
                .iter()
                .map(AppendRecord::hash_bytes)
                .sum::<u64>();
        #[cfg(not(target_arch = "wasm32"))]
        if let Some(work) = &self.work {
            work.record(work.meter.admit_codec(0, 0, hashed))?;
        }
        let digest = append_digest(request)?;
        #[cfg(not(target_arch = "wasm32"))]
        if let Some(work) = &self.work {
            work.record(work.meter.record_codec(0, 0, hashed))?;
        }
        Ok(digest)
    }
}
