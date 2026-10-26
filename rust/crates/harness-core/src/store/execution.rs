use super::*;

pub(super) fn stream_idempotency_key_with_work(
    path: &str,
    value: &IdempotencyKey,
    work: &mut Option<&mut dyn history::HistoryIo>,
) -> Result<StreamIdempotencyKey> {
    if let Some(work) = work.as_deref_mut() {
        let bytes = b"acyclic-harness-stream-append-v2"
            .len()
            .checked_add(16)
            .and_then(|bytes| bytes.checked_add(path.len()))
            .and_then(|bytes| bytes.checked_add(value.0.len()))
            .ok_or_else(|| Error::Invalid("stream retry digest length overflows".into()))?;
        work.hashed_bytes(bytes as u64)?;
        work.admit_allocation(32)?;
        work.copy_bytes(32)?;
    }
    let mut hasher = blake3::Hasher::new();
    hasher.update(b"acyclic-harness-stream-append-v2");
    hasher.update(&(path.len() as u64).to_le_bytes());
    hasher.update(path.as_bytes());
    hasher.update(&(value.0.len() as u64).to_le_bytes());
    hasher.update(value.0.as_bytes());
    let key = StreamIdempotencyKey::new(OwnedBytes::copy_from_slice(hasher.finalize().as_bytes()))
        .map_err(|error| Error::Invalid(error.to_string()))?;
    if let Some(work) = work.as_deref_mut() {
        work.retain_owned(32)?;
    }
    Ok(key)
}

impl<P: StreamProvider> StreamAggregate<P> {
    /// Plans, CAS-appends, and only then applies one command.
    pub async fn execute(
        &mut self,
        command: Command,
        work: &mut Option<&mut dyn history::HistoryIo>,
    ) -> Result<ApplyResult> {
        #[cfg(not(feature = "filesystem"))]
        if work.is_some() {
            return Err(Error::Unsupported(
                "aggregate work observers require filesystem support".into(),
            ));
        }
        self.publish(command, true, work)
            .await
            .map(|(result, _)| result)
    }

    pub(super) async fn plan_command(&self, command: &Command) -> Result<ApplyResult> {
        if let Some(effect_id) = self.reducer.archived_effect_for_command(command)
            && let Some(effect) = self
                .effect(effect_id, default_projection_read_limits(), &mut None)
                .await
                .map(|state| state.map(|(state, _)| state))?
        {
            return self
                .reducer
                .plan_with_archived_effect(command, effect_id, &effect, &mut None)
                .map(|(result, _)| result);
        }
        self.reducer
            .plan_indexed_with_retained_quota(command, false, &mut None)
            .map(|(result, _)| result)
    }

    /// Reconciles a possibly committed append without issuing another append.
    pub async fn reconcile(&mut self, command: &Command) -> Result<Option<ApplyResult>> {
        if let Some((result, _)) = self
            .replay(
                command,
                true,
                2 * acyclic_stream::MAX_RECORD_BYTES as u64,
                &mut None,
            )
            .await?
        {
            if let Action::PublishFork { seed } = &command.action {
                self.release_replayed_fork_fence(seed).await?;
            }
            return Ok(Some(result));
        }
        let key = stream_idempotency_key_with_work(
            self.path.as_str(),
            &command.idempotency_key,
            &mut None,
        )?;
        let Some(observation) = self.client.inspect_idempotency(key).await? else {
            return Ok(None);
        };
        match observation.outcome {
            IdempotencyOutcome::Commit(CommitOutcome::Conflict(_)) => {
                if let Action::PublishFork { seed } = &command.action {
                    self.release_replayed_fork_fence(seed).await?;
                }
                Err(Error::Conflict("aggregate publication was rejected".into()))
            }
            IdempotencyOutcome::Commit(CommitOutcome::Committed(_)) => Err(Error::Storage(
                "committed aggregate publication has no atomic operation location".into(),
            )),
            _ => Err(Error::Conflict(
                "retry identity is bound to another operation contract".into(),
            )),
        }
    }
}
