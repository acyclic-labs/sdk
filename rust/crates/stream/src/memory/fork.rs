impl MemoryStream {
    fn plan_fork_locked<'a>(
        &'a self,
        state: &mut State,
        request: ForkRequest,
    ) -> Result<
        Decision<
            ForkReceipt,
            impl FnOnce(&mut State, bool) -> Result<ForkReceipt, StreamError> + use<'a>,
        >,
        StreamError,
    > {
        let digest = fork_digest(&request)?;
        if let Some(result) = self.replay_fork(&state, &request.idempotency_key, digest)? {
            return Ok(Decision::Returned(result));
        }
        admit_replay(&state, self.limits)?;
        let mut probes =
            self.reserve_replay_probes(u64::from(state.replay_height(state.replay_root)))?;
        let forked_at = crate::preparation::fork_cut(
            &request,
            state.paths.get(&request.source).map(|path| path.tail),
            state.paths.get(&request.destination).map(|path| path.tail),
        )?;
        reserve_fork(&state, &request, self.limits)?;
        reserve_commit(&state, self.limits)?;
        self.reserve_commit_index(&mut probes)?;
        let decision = state.decision.checked_add(1).ok_or(StreamError::Capacity)?;
        let commit_id = commit_identity(decision, digest);
        let receipt = ForkReceipt {
            source: request.source,
            destination: request.destination,
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
        let (histories, _histories) = self.histories(&state, &envelope.mutations)?;
        let (paths, _paths) = self.paths(&state, std::iter::once(&receipt.destination))?;
        self.reserve_retained(state)?;
        Ok(Decision::Accepted {
            bytes: 0,
            install: move |state: &mut State, _durable| {
                let _holds = (_histories, _paths);
                let time = crate::preparation::committed_time(
                    state.last_committed_at_micros,
                    self.commit_clock.now_unix_millis(),
                );
                bind_envelope(&mut envelope, commit_id, time);
                publish(
                    state,
                    paths,
                    histories,
                    &envelope,
                    &mut NamespaceBuilder::new(&|bytes| self.allocate(bytes)),
                )?;
                state.decision = decision;
                state.last_committed_at_micros = time;
                retain(
                    state,
                    &mut || self.record_replay_probe(&mut probes),
                    Some(envelope),
                    Some(request.idempotency_key),
                    digest,
                    IdempotencyOutcome::Fork(receipt.clone()),
                    Default::default(),
                );
                Ok(receipt)
            },
        })
    }
}
