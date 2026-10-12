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
        #[cfg(feature = "filesystem")] owner: Option<&crate::durable_host::TaskJournalOwner<P>>,
        work: &mut Option<&mut dyn history::HistoryIo>,
    ) -> Result<ApplyResult> {
        #[cfg(feature = "filesystem")]
        let write = {
            if let Some(owner) = owner
                && matches!(command.action, Action::BindConversation { .. })
            {
                return self.execute_root_bind_with_work(command, owner, work).await;
            }
            let write = if let Some(owner) = owner {
                match &command.action {
                    Action::OpenInteraction { .. } => {
                        owner.require_interaction_grant(work)?;
                        crate::distributed::JournalWrite::Fresh
                    }
                    Action::PlanEffect {
                        provider,
                        request,
                        result_schema,
                        ..
                    } => {
                        owner.require_effect_grants(provider, true, work)?;
                        #[cfg(not(target_arch = "wasm32"))]
                        if let Some(native) =
                            work.as_deref().and_then(|io| io.original_native_work())
                        {
                            owner.validate_input_file(
                                request,
                                Some(&crate::filesystem::PreparationIo::from_native_work(native)?),
                            )?;
                            owner.validate_input_file(
                                result_schema,
                                Some(&crate::filesystem::PreparationIo::from_native_work(native)?),
                            )?;
                        } else {
                            owner.validate_input_file(request, None)?;
                            owner.validate_input_file(result_schema, None)?;
                        }
                        #[cfg(target_arch = "wasm32")]
                        {
                            owner.validate_input_file(request, None)?;
                            owner.validate_input_file(result_schema, None)?;
                        }
                        crate::distributed::JournalWrite::Fresh
                    }
                    Action::MarkEffectDispatched { effect_id, .. }
                    | Action::ResolveEffect {
                        observation: crate::core::EffectAttestation { effect_id, .. },
                    } => {
                        let mut retained = crate::wire_codec::CodecQuota::from_retained(work, 0);
                        let (effect, bytes) = self
                            .effect(*effect_id, self.effect_history_limits, retained.work())
                            .await?
                            .ok_or_else(|| {
                                guarded_boundary(
                                    retained.work(),
                                    crate::wire::ErrorCode::NotFound,
                                    "effect is not found",
                                )
                            })?;
                        retained.adopt(bytes)?;
                        owner.require_effect_grants(&effect.provider, false, retained.work())?;
                        drop(effect);
                        retained.relinquish(bytes)?;
                        if matches!(command.action, Action::ResolveEffect { .. }) {
                            crate::distributed::JournalWrite::Settlement
                        } else {
                            crate::distributed::JournalWrite::Fresh
                        }
                    }
                    _ => {
                        return Err(guarded_boundary(
                            work,
                            crate::wire::ErrorCode::Invalid,
                            "task journal action is not an admitted request or effect",
                        ));
                    }
                }
            } else {
                crate::distributed::JournalWrite::Fresh
            };
            write
        };
        #[cfg(not(feature = "filesystem"))]
        if work.is_some() {
            return Err(Error::Unsupported(
                "aggregate work observers require filesystem support".into(),
            ));
        }
        self.publish(
            command,
            #[cfg(feature = "filesystem")]
            owner,
            #[cfg(feature = "filesystem")]
            write,
            true,
            work,
        )
        .await
        .map(|(result, _)| result)
    }

    pub(super) async fn plan_command(
        &self,
        command: &Command,
        fresh_migration: bool,
    ) -> Result<ApplyResult> {
        if !fresh_migration {
            if let Some(effect_id) = self.reducer.archived_effect_for_command(command)
                && let Some(effect) = self
                    .effect(effect_id, self.effect_history_limits, &mut None)
                    .await
                    .map(|state| state.map(|(state, _)| state))?
            {
                return self
                    .reducer
                    .plan_with_archived_effect(command, effect_id, &effect, &mut None)
                    .map(|(result, _)| result);
            }
            return self
                .reducer
                .plan_indexed_with_retained_quota(command, false, &mut None)
                .map(|(result, _)| result);
        }
        if let Action::MigrateExtensionState {
            name,
            previous,
            to_version,
            content,
        } = &command.action
        {
            self.reducer.preflight_extension_migration(
                &command.scope,
                command.expected_revision,
                previous,
                name,
                *to_version,
                content.volume(),
            )?;
        }
        self.validate_admission_content(&command.action).await?;
        self.reducer
            .plan_indexed_with_retained_quota(command, true, &mut None)
            .map(|(result, _)| result)
    }

    /// Reconciles a possibly committed append without issuing another append.

    pub async fn reconcile(&mut self, command: &Command) -> Result<Option<ApplyResult>> {
        async {
            let result = self.reconcile_inner(command).await?;
            if result.is_some()
                && let crate::core::Action::PublishFork { seed } = &command.action
            {
                self.release_replayed_fork_fence(seed).await?;
            }
            Ok(result)
        }
        .await
    }

    pub(super) async fn reconcile_inner(
        &mut self,
        command: &Command,
    ) -> Result<Option<ApplyResult>> {
        if let Some((result, _)) = self
            .replay(
                command,
                true,
                2 * acyclic_stream::MAX_RECORD_BYTES as u64,
                &mut None,
            )
            .await?
        {
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
