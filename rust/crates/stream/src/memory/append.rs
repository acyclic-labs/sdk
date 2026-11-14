impl MemoryStream {
    pub(crate) async fn append_inner(
        &self,
        request: AppendRequest,
    ) -> Result<(AppendOutcome, Allocation), StreamError> {
        let _publication = self.publication.lock().await;
        let mut state = self.state.write().await;
        self.plan_append_locked(&mut state, &request)?
            .install(self, &mut state, false)
    }

    fn plan_append_locked<'a>(
        &'a self,
        state: &mut State,
        request: &'a AppendRequest,
    ) -> Result<
        Decision<
            (AppendOutcome, Allocation),
            impl FnOnce(
                &mut State,
                bool,
                Option<u64>,
            ) -> Result<(AppendOutcome, Allocation), StreamError>
            + use<'a>,
        >,
        StreamError,
    > {
        #[cfg(not(target_arch = "wasm32"))]
        let _input = self.append_claim(request)?;
        let bytes = request
            .records
            .iter()
            .map(AppendRecord::payload_bytes)
            .sum::<usize>();
        let digest = self.append_request_digest(request)?;
        let scope = state.ephemeral.append_scope(state, request)?;
        if let Some(result) = self.replay_append(state, &request.idempotency_key, digest)? {
            return Ok(Decision::Returned {
                result: (result, Default::default()),
                ephemeral: matches!(scope, Scope::Ephemeral(_)),
            });
        }
        let durable = matches!(scope, Scope::Durable);
        let mut probes = if durable {
            self.replay_insertion_probes(request.idempotency_key.as_bytes())?
        } else {
            ReplayProbes::default()
        };
        let start = state.path(&request.path).map_or(0, |path| path.tail);
        if request.if_tail.is_some_and(|expected| expected != start) {
            let result = AppendOutcome::TailConflict { actual_tail: start };
            return Ok(Decision::Returned {
                result: (result, Default::default()),
                ephemeral: matches!(scope, Scope::Ephemeral(_)),
            });
        }
        // The write's own authority time; the installed rule decides with it
        // before any reservation.
        let time = self.admitted_time(state, &request.records)?;
        if durable {
            admit_replay(state, self.limits)?;
        }
        reserve_append(state, &request.path, &request.records, self.limits)?;
        if durable {
            self.reserve_commit_index(&mut probes)?;
        }
        #[cfg(not(target_arch = "wasm32"))]
        if let Some(work) = &self.work {
            work.record(work.meter.admit_codec(0, 0, identity_bytes()))?;
        }
        let (decision, commit_id) = state.identity(&scope, digest)?;
        #[cfg(not(target_arch = "wasm32"))]
        if let Some(work) = &self.work {
            work.record(work.meter.record_codec(0, 0, identity_bytes()))?;
        }
        let (records, mut allocation) =
            self.records(&request.path, &request.records, start, commit_id, time)?;
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
        let (mut histories, _histories) = self.histories(state, &facts)?;
        let (paths, _paths) = self.paths(state, std::iter::once(&request.path), |prefix| {
            request.path.as_str() == prefix
        })?;
        let retained = durable.then(|| self.reserve_retained(state)).transpose()?;
        let ephemeral = self.reserve_ephemeral(&scope, &request.idempotency_key)?;
        let mut envelope = CommittedEnvelope {
            commit_id,
            committed_at_micros: 0,
            mutations: facts,
        };
        bind_envelope(&mut envelope, commit_id, time);
        bind_histories(&mut histories, commit_id, time)?;
        let mut namespace = stage_namespace(
            state,
            paths,
            histories,
            &envelope,
            &scope,
            &mut NamespaceBuilder::new(&|bytes| self.allocate(bytes))
                .with_base((!durable).then_some(&state.paths)),
        )?;
        Ok(Decision::Accepted {
            ephemeral: matches!(scope, Scope::Ephemeral(_)),
            bytes,
            install: move |state: &mut State, _durable, at_millis| {
                #[cfg(not(target_arch = "wasm32"))]
                let _input = _input;
                let _holds = (_histories, _paths);
                let time = bind_acceptance(&mut namespace, &mut envelope, state, at_millis)?;
                let retained = retained
                    .map(|retained| self.install_retained(state, retained))
                    .transpose()?;
                install_namespace(state, namespace, &envelope, &scope)?;
                state.advance(&scope, time);
                let result = IdempotencyOutcome::Append(outcome.clone());
                match retained {
                    Some(retained) => retain(
                        state,
                        &mut |items| self.record_replay_probe(&mut probes, items),
                        Some(envelope),
                        Some(request.idempotency_key.clone()),
                        digest,
                        result,
                        (allocation, retained),
                    ),
                    None => state.ephemeral.retain(
                        self,
                        &scope,
                        ephemeral
                            .unwrap_or_else(|| unreachable!("leased write reserves its indexes")),
                        RetainedFact {
                            namespace: Default::default(),
                            previous: None,
                            replay: Some(request.idempotency_key.0.clone()),
                            next: Default::default(),
                            digest,
                            result,
                            envelope: Some(envelope),
                            index: decision,
                            _allocation: allocation,
                            _header: None,
                        },
                    ),
                }
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

    #[cfg(not(target_arch = "wasm32"))]
    fn append_claim(&self, request: &AppendRequest) -> Result<Allocation, StreamError> {
        self.adopt(
            request.records.capacity() * std::mem::size_of::<AppendRecord>()
                + request
                    .records
                    .iter()
                    .map(|record| record.headers.owned_bytes())
                    .sum::<usize>(),
        )
    }
}
