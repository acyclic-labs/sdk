impl MemoryStream {
    pub(crate) async fn append_inner(
        &self,
        request: AppendRequest,
    ) -> Result<(AppendOutcome, Allocation), StreamError> {
        validate_records(&request.records)?;
        validate_append_size(&request)?;
        #[cfg(not(target_arch = "wasm32"))]
        let _input = self.adopt(request.records.capacity() * std::mem::size_of::<Bytes>())?;
        let bytes = request.records.iter().map(Bytes::len).sum::<usize>();
        #[cfg(not(target_arch = "wasm32"))]
        let hashed = b"acyclic.stream.request.v1\0".len() as u64
            + b"append".len() as u64
            + 24
            + request.path.as_str().len() as u64
            + 8 * request.records.len() as u64
            + bytes as u64;
        #[cfg(not(target_arch = "wasm32"))]
        if let Some(work) = &self.work {
            work.record(work.meter.admit_codec(0, 0, hashed))?;
        }
        let digest = append_digest(&request)?;
        #[cfg(not(target_arch = "wasm32"))]
        if let Some(work) = &self.work {
            work.record(work.meter.record_codec(0, 0, hashed))?;
        }
        let mut state = self.state.write().await;
        if let Some(result) = replay_append(&state, request.idempotency_key.as_ref(), digest)? {
            return Ok((result, Default::default()));
        }
        admit_replay(&state, request.idempotency_key.as_ref(), self.limits)?;
        let start = state.paths.get(&request.path).map_or(0, |path| path.tail);
        if request.if_tail.is_some_and(|expected| expected != start) {
            let result = AppendOutcome::TailConflict { actual_tail: start };
            if request.idempotency_key.is_some() {
                let allocation = self.retained(&mut state, true, false)?;
                retain(
                    &mut state,
                    None,
                    request.idempotency_key,
                    digest,
                    IdempotencyOutcome::Append(result.clone()),
                );
                state
                    .retained
                    .back_mut()
                    .expect("retained conflict")
                    ._allocation = allocation;
            }
            return Ok((result, Default::default()));
        }
        reserve_append(&state, &request.path, &request.records, self.limits)?;
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
        let time = state
            .last_committed_at_micros
            .max(self.commit_clock.now_unix_millis().saturating_mul(1_000));
        let (records, mut allocation) = self.records(&request.records, start, commit_id, time)?;
        let fact = crate::preparation::append_fact(request.path.clone(), start, records)?;
        let outcome = AppendOutcome::Committed(AppendReceipt {
            start,
            end: fact.end,
            tail: fact.tail,
            commit_id,
        });
        let (mut facts, claim) = self.vector(1)?;
        self.absorb(&mut allocation, claim)?;
        facts.push(CommittedMutation::Append(fact));
        let (histories, _histories) = self.histories(&state, &facts)?;
        let (paths, _paths) = self.paths(&state, std::iter::once(&request.path))?;
        self.absorb(
            &mut allocation,
            self.retained(&mut state, request.idempotency_key.is_some(), true)?,
        )?;
        let envelope = CommittedEnvelope {
            commit_id,
            mutations: facts,
        };
        self.publish(bytes, || {
            publish(&mut state, paths, histories, &envelope);
            state.decision = decision;
            state.last_committed_at_micros = time;
            retain(
                &mut state,
                Some(envelope),
                request.idempotency_key,
                digest,
                IdempotencyOutcome::Append(outcome.clone()),
            );
            state
                .retained
                .back_mut()
                .expect("retained append")
                ._allocation = allocation;
            Ok((outcome, Default::default()))
        })
    }
}
