use super::*;

fn guarded_publication_error(error: Error, operation: OperationId) -> Error {
    let _ = &error;
    #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
    if matches!(
        &error,
        Error::NativeWork { .. } | Error::NativeBoundary { .. }
    ) {
        return error;
    }
    drop(error);
    Error::Indeterminate(operation)
}

impl<P: StreamProvider> StreamAggregate<P> {
    pub(crate) async fn publish(
        &mut self,
        command: Command,
        return_event: bool,
        work: &mut Option<&mut dyn history::HistoryIo>,
    ) -> Result<(ApplyResult, u64)> {
        if let Some(replay) = self
            .replay_publication(&command, return_event, work)
            .await?
        {
            return Ok(replay);
        }

        let mut transient = crate::wire_codec::CodecQuota::from_retained(work, 0);
        let (planned, planned_quota) = self.plan_publication(&command, &mut transient).await?;
        transient.adopt(planned_quota)?;
        let ApplyResult::Applied { event } = planned else {
            transient.transfer();
            return Ok((planned, planned_quota));
        };
        self.validate_publication_content(&command, &mut transient)
            .await?;
        self.validate_causal_reference(event.causal_parent.as_ref(), transient.work())
            .await?;
        let publication = checkpoints::publication_with_work(
            &self.client,
            &self.reducer,
            &event,
            transient.work(),
        )
        .await?;
        let publication_owned = if transient.work().is_some() {
            publication.retained_owned()
        } else {
            0
        };
        transient.adopt(publication_owned)?;
        if return_event && let Some(work) = transient.work().as_deref_mut() {
            work.stage_output(publication.bytes.len() as u64, 1)?;
        }
        let mut key = Some(stream_idempotency_key_with_work(
            self.path.as_str(),
            &command.idempotency_key,
            transient.work(),
        )?);
        let fork_guard = self
            .activate_publication_fork(&command, &mut transient)
            .await?;
        let mut append_phase = crate::distributed::CommitPhase::Uncommitted;
        let appended = self
            .append_publication(
                &publication,
                &mut key,
                command.operation_id,
                &mut append_phase,
                &mut transient,
            )
            .await;
        drop(publication);
        let released_publication = transient.relinquish(publication_owned);
        drop(key);
        let released_key = transient.relinquish(0);
        Self::finish_append(
            appended,
            (released_publication, released_key),
            command.operation_id,
            append_phase,
            fork_guard.as_ref(),
            &mut transient,
        )
        .await?;
        self.apply_publication(
            event,
            command.operation_id,
            planned_quota,
            fork_guard.as_ref(),
            &mut transient,
        )
        .await
    }

    async fn validate_publication_content(
        &self,
        command: &Command,
        transient: &mut crate::wire_codec::CodecQuota<'_, '_>,
    ) -> Result<()> {
        if matches!(command.action, Action::MigrateExtensionState { .. }) {
            return Err(guarded_boundary(
                transient.work(),
                crate::wire::ErrorCode::Unsupported,
                "extension migration executor is not installed",
            ));
        }
        if transient.work().is_none() {
            self.validate_admission_content(&command.action, command.scope.id())
                .await?;
        } else {
            #[cfg(feature = "filesystem")]
            {
                if matches!(
                    command.action,
                    Action::AppendCustom { .. }
                        | Action::MigrateExtensionState { .. }
                        | Action::ConfigureExtension { .. }
                        | Action::AppendConversationMessage { .. }
                        | Action::PublishProjectMerge { .. }
                ) {
                    return Err(guarded_boundary(
                        transient.work(),
                        crate::wire::ErrorCode::Unsupported,
                        "command content has no finite admission engine",
                    ));
                }
                self.validate_task_admission_content_with_work(&command.action, transient.work())
                    .await?;
                if let Action::ResolveInteraction { resolution } = &command.action
                    && let Some(detail) = &resolution.detail
                {
                    let ticket = self.reducer.interaction(&resolution.id).ok_or_else(|| {
                        guarded_boundary(
                            transient.work(),
                            crate::wire::ErrorCode::NotFound,
                            "interaction is not found",
                        )
                    })?;
                    self.validate_interaction_part_with_work(
                        &ticket.0,
                        detail,
                        Some((resolution, command.scope.id())),
                        transient.work(),
                    )
                    .await?;
                }
            }
            #[cfg(not(feature = "filesystem"))]
            return Err(Error::Unsupported(
                "aggregate work observers require filesystem support".into(),
            ));
        }
        Ok(())
    }

    async fn append_publication(
        &self,
        publication: &operations::IndexedPublication,
        key: &mut Option<StreamIdempotencyKey>,
        operation: OperationId,
        append_phase: &mut crate::distributed::CommitPhase,
        transient: &mut crate::wire_codec::CodecQuota<'_, '_>,
    ) -> Result<bool> {
        async {
            let request = publication
                .request(
                    &self.client,
                    key.take().ok_or_else(|| {
                        guarded_boundary(
                            transient.work(),
                            crate::wire::ErrorCode::Storage,
                            "publication key is missing",
                        )
                    })?,
                    transient.work(),
                )
                .await?;
            let (outcome, retained) = crate::distributed::commit(
                request,
                &self.client,
                Some(operation),
                Some(append_phase),
                None,
            )
            .await?;
            let mut acknowledged =
                crate::wire_codec::CodecQuota::from_retained(transient.work(), retained);
            match outcome {
                CommitOutcome::Committed(envelope) => {
                    let verified = publication.verify(&envelope, acknowledged.work());
                    drop(envelope);
                    verified.map_err(|error| guarded_publication_error(error, operation))?;
                    Ok(true)
                }
                CommitOutcome::Conflict(_) => Ok(false),
            }
        }
        .await
    }

    async fn apply_publication(
        &mut self,
        event: crate::core::Event,
        operation: OperationId,
        planned_quota: u64,
        fork_guard: Option<&crate::fork::ObservedForkPublicationGuard>,
        transient: &mut crate::wire_codec::CodecQuota<'_, '_>,
    ) -> Result<(ApplyResult, u64)> {
        let (cloned, returned_owned) =
            crate::wire_codec::CodecQuota::produce(transient.work(), |copy_quota| {
                crate::core::copy_event(&event, copy_quota)
            })?;
        transient.adopt(returned_owned)?;
        let applied = self
            .reducer
            .apply_committed(cloned, transient.work())
            .and_then(|result| {
                self.reducer
                    .mark_indexed_terminal_effect_with_work(&event, transient.work())
                    .map(|()| result)
            });
        if let Some(guard) = fork_guard.map(|guard| guard.guard.as_ref()) {
            crate::fork::release_fork_guard_with_work(guard, transient.work())
                .await
                .map_err(|error| guarded_publication_error(error, operation))?;
        }
        drop(event);
        let released_planned = transient.relinquish(planned_quota);
        match applied {
            Ok(result) => {
                released_planned.map_err(|error| guarded_publication_error(error, operation))?;
                transient.transfer();
                Ok((result, returned_owned))
            }
            Err(error) => {
                let _ = released_planned;
                Err(guarded_publication_error(error, operation))
            }
        }
    }
    async fn replay_publication(
        &mut self,
        command: &Command,
        return_event: bool,
        work: &mut Option<&mut dyn history::HistoryIo>,
    ) -> Result<Option<(ApplyResult, u64)>> {
        if let Some(result) = self
            .replay(
                command,
                return_event,
                default_projection_read_limits().maximum_bytes,
                work,
            )
            .await?
        {
            let returned_owned = result.1;
            let mut returned_custody =
                crate::wire_codec::CodecQuota::from_retained(work, returned_owned);
            let value = result.0;
            if let Action::PublishFork { seed } = &command.action {
                self.fork_verifier
                    .as_ref()
                    .ok_or_else(|| {
                        guarded_boundary(
                            returned_custody.work(),
                            crate::wire::ErrorCode::Unsupported,
                            "fork seed verifier is not bound",
                        )
                    })?
                    .release_private_fence(seed, returned_custody.work())
                    .await?;
            }
            returned_custody.transfer();
            return Ok(Some((value, returned_owned)));
        }
        Ok(None)
    }

    async fn finish_append(
        appended: Result<bool>,
        released: (Result<()>, Result<()>),
        operation: OperationId,
        append_phase: crate::distributed::CommitPhase,
        fork_guard: Option<&crate::fork::ObservedForkPublicationGuard>,
        transient: &mut crate::wire_codec::CodecQuota<'_, '_>,
    ) -> Result<()> {
        let (released_publication, released_key) = released;
        match appended {
            Ok(true) => {
                released_publication
                    .map_err(|error| guarded_publication_error(error, operation))?;
                released_key.map_err(|error| guarded_publication_error(error, operation))?;
            }
            Ok(false) => {
                let _ = released_key;
                let _ = released_publication;
                if let Some(guard) = fork_guard.map(|guard| guard.guard.as_ref()) {
                    crate::fork::release_fork_guard_with_work(guard, transient.work()).await?;
                }
                return Err(guarded_boundary(
                    transient.work(),
                    crate::wire::ErrorCode::Conflict,
                    "task publication lost its owner or conversation tail",
                ));
            }
            Err(error) => {
                let _ = released_key;
                let _ = released_publication;
                if matches!(append_phase, crate::distributed::CommitPhase::Uncommitted)
                    && let Some(guard) = fork_guard.map(|guard| guard.guard.as_ref())
                {
                    let _ =
                        crate::fork::release_fork_guard_with_work(guard, transient.work()).await;
                }
                return Err(error);
            }
        }
        Ok(())
    }

    async fn activate_publication_fork(
        &self,
        command: &Command,
        transient: &mut crate::wire_codec::CodecQuota<'_, '_>,
    ) -> Result<Option<crate::fork::ObservedForkPublicationGuard>> {
        let fork_guard = if let Action::PublishFork { seed } = &command.action {
            Some(
                self.fork_verifier
                    .as_ref()
                    .ok_or_else(|| {
                        guarded_boundary(
                            transient.work(),
                            crate::wire::ErrorCode::Unsupported,
                            "fork seed verifier is not bound",
                        )
                    })?
                    .activate(seed, transient.work())
                    .await?,
            )
        } else {
            None
        };
        Ok(fork_guard)
    }

    async fn plan_publication(
        &self,
        command: &Command,
        transient: &mut crate::wire_codec::CodecQuota<'_, '_>,
    ) -> Result<(ApplyResult, u64)> {
        let (planned, planned_quota) = if transient.work().is_some() {
            self.reducer
                .plan_indexed_with_retained_quota(command, false, transient.work())?
        } else {
            (self.plan_command(command).await?, 0)
        };
        Ok((planned, planned_quota))
    }
}
