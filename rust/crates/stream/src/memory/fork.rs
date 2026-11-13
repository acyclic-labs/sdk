impl MemoryStream {
    fn plan_fork_locked<'a>(
        &'a self,
        state: &mut State,
        request: &'a ForkRequest,
    ) -> Result<
        Decision<
            ForkReceipt,
            impl FnOnce(&mut State, bool, Option<u64>) -> Result<ForkReceipt, StreamError> + use<'a>,
        >,
        StreamError,
    > {
        let digest = fork_digest(request)?;
        let scope = state.ephemeral.fork_scope(request)?;
        if let Some(result) = self.replay_fork(state, &scope, &request.idempotency_key, digest)? {
            return Ok(Decision::Returned(result));
        }
        let durable = matches!(scope, Scope::Durable);
        if durable {
            admit_replay(state, self.limits)?;
        }
        let mut probes = self.replay_insertion_probes(request.idempotency_key.as_bytes())?;
        let forked_at = crate::preparation::fork_cut(
            request,
            state.tail(&request.source),
            state.tail(&request.destination),
        )?;
        reserve_fork(state, request, self.limits)?;
        reserve_commit(state, self.limits)?;
        self.reserve_commit_index(&mut probes)?;
        let (decision, commit_id) = state.identity(&scope, digest)?;
        let receipt = ForkReceipt {
            source: request.source.clone(),
            destination: request.destination.clone(),
            forked_at,
            tail: forked_at,
            commit_id,
            revision: self.revisions.at(decision),
        };
        let mut envelope = CommittedEnvelope {
            commit_id,
            committed_at_micros: 0,
            mutations: vec![CommittedMutation::Fork(CommittedFork {
                exclude: crate::request::canonical_exclusions(&request.exclude)?,
                source: receipt.source.clone(),
                destination: receipt.destination.clone(),
                forked_at,
                tail: forked_at,
                records: Vec::new(),
            })],
        };
        let (histories, _histories) = self.histories(state, &envelope.mutations)?;
        let (paths, _paths) =
            self.paths(state, std::iter::once(&receipt.destination), |prefix| {
                receipt.destination.as_str() == prefix
            })?;
        let retained = durable.then(|| self.reserve_retained(state)).transpose()?;
        let time = self.admitted_time(state, [])?;
        bind_envelope(&mut envelope, commit_id, time);
        let mut namespace = stage_namespace(
            state,
            paths,
            histories,
            &envelope,
            &mut NamespaceBuilder::new(&|bytes| self.allocate(bytes)),
        )?;
        Ok(Decision::Accepted {
            bytes: 0,
            install: move |state: &mut State, _durable, at_millis| {
                let _holds = (_histories, _paths);
                let time = bind_acceptance(&mut namespace, &mut envelope, state, at_millis)?;
                let retained = retained
                    .map(|retained| self.install_retained(state, retained))
                    .transpose()?;
                install_namespace(state, namespace, &envelope, &scope)?;
                state.advance(&scope, time);
                let result = IdempotencyOutcome::Fork(receipt.clone());
                match retained {
                    Some(retained) => retain(
                        state,
                        &mut |items| self.record_replay_probe(&mut probes, items),
                        Some(envelope),
                        Some(request.idempotency_key.clone()),
                        digest,
                        result,
                        (Default::default(), retained),
                    ),
                    None => state.ephemeral.retain(
                        &scope,
                        &request.idempotency_key,
                        digest,
                        result,
                        decision,
                        Default::default(),
                    ),
                }
                Ok(receipt)
            },
        })
    }
}
