use super::*;
use crate::durable_host::TaskJournalOwner;
use acyclic_fs::{CancellationToken, MeasuredResult, OperationReceipt, WorkBudget};

fn guarded_publication_error(error: Error, operation: OperationId) -> Error {
    #[cfg(not(target_arch = "wasm32"))]
    if matches!(&error, Error::NativeWork { .. } | Error::NativeBoundary { .. }) { return error; }
    Error::Indeterminate(operation)
}

impl<P: StreamProvider> StreamAggregate<P> {

    pub(super) async fn execute_root_bind_with_work(
        &mut self, command: Command, owner: &TaskJournalOwner<P>,
        work: &mut Option<&mut dyn history::HistoryIo>,
    ) -> Result<ApplyResult> {
        if let Some(work) = work.as_deref_mut() { work.check_cancellation()?; }
        let Action::BindConversation { agent } = &command.action else {
            return Err(guarded_boundary(work, crate::wire::ErrorCode::Invalid, "measured ROOT binding requires the original BindConversation command"));
        };
        if self.reducer.authority().kind != crate::core::AggregateKind::Conversation
            || command.causal_parent.is_some()
            || command.scope.agent() != Some(*agent)
            || owner.authenticated_binding_agent()? != Some(*agent)
        {
            return Err(guarded_boundary(work, crate::wire::ErrorCode::Unauthorized, "ROOT binding differs from its original owner, actor or causal policy"));
        }
        owner.authenticate_binding_scope(self.reducer.authority(), &command.scope, work)?;
        let gate = owner.lock_root_binding().await
            .ok_or_else(|| guarded_boundary(work, crate::wire::ErrorCode::Unauthorized, "published children use their canonical child binding, not ROOT binding"))?;
        if let Some(work) = work.as_deref_mut() { work.check_cancellation()?; }
        let intent = crate::core::canonical_intent_with_work(&command, work)?;
        owner.check_root_binding(self.reducer.authority(), command.operation_id, intent)?;
        const ROOT_CHOICE_DOMAIN: &[u8] = b"harness/v2/root-bind-choice\0";
        let audience = self.reducer.authority();
        if let Some(work) = work.as_deref_mut() {
            work.hashed_bytes(
                (ROOT_CHOICE_DOMAIN.len() + b"conversation".len() + 8 + audience.id.len()) as u64)?;
        }
        let mut choice = blake3::Hasher::new();
        choice.update(ROOT_CHOICE_DOMAIN);
        choice.update(b"conversation");
        choice.update(&(audience.id.len() as u64).to_le_bytes());
        choice.update(audience.id.as_bytes());
        owner.pin_root_binding_choice(command.operation_id, intent, *choice.finalize().as_bytes())?;
        let (original_task, admission_digest, original_fence) = owner.binding_origin();
        let proof = VerifiedRootBinding {
            original_task, admission_digest,
            fence: crate::scheduler::LeaseFence {
                reservation_id: match work.as_deref_mut() {
                    Some(work) => work.clone_text(&original_fence.reservation_id)?,
                    None => original_fence.reservation_id.clone(),
                },
                placement: match work.as_deref_mut() {
                    Some(work) => work.clone_text(&original_fence.placement)?,
                    None => original_fence.placement.clone(),
                },
            },
            agent: *agent,
            audience: match work.as_deref_mut() {
                Some(work) => work.clone_authority(self.reducer.authority())?,
                None => self.reducer.authority().clone(),
            },
            bind_operation: command.operation_id,
            intent_digest: intent,
        };
        #[cfg(not(target_arch = "wasm32"))]
        let proof = {
            let custody = if let Some(observer) = work.as_deref_mut() {
                if observer.original_native_work().is_some() {
                    let bytes = (proof.fence.reservation_id.capacity() as u64)
                        .checked_add(proof.fence.placement.capacity() as u64)
                        .and_then(|bytes| bytes.checked_add(proof.audience.id.capacity() as u64))
                        .ok_or(Error::NativeBoundary {
                            code: crate::wire::ErrorCode::Invalid,
                            reason: "root binding proof custody overflows",
                        })?;
                    Some(observer.take_native_custody(bytes)?)
                } else { None }
            } else { None };
            (proof, custody)
        };
        let result = if let Some(replayed) = self.replay(&command, true, default_projection_read_limits().maximum_bytes, work).await? {
            replayed.0
        } else {
            self.publish(command, Some(owner),
                crate::distributed::JournalWrite::Fresh, true, work).await?.0
        };
        #[cfg(not(target_arch = "wasm32"))]
        {
            let (proof, custody) = proof;
            match custody {
                Some(custody) => { owner.retain_root_binding_with_native_custody(proof, custody)?; },
                None => owner.retain_root_binding(proof)?,
            }
        }
        #[cfg(target_arch = "wasm32")]
        owner.retain_root_binding(proof)?;
        drop(gate);
        Ok(result)
    }


    pub(crate) async fn publish(
        &mut self, command: Command, owner: Option<&TaskJournalOwner<P>>,
        write: crate::distributed::JournalWrite, return_event: bool,
        work: &mut Option<&mut dyn history::HistoryIo>,
    ) -> Result<(ApplyResult, u64)> {
        if let Some(owner) = owner { owner.verify(write.settlement(), work).await?; }
        if let Some(result) = self.replay(&command, return_event, default_projection_read_limits().maximum_bytes, work).await? {
            let returned_owned = result.1;
            let mut returned_custody = crate::wire_codec::CodecQuota::from_retained(work, returned_owned);
            let value = result.0;
            if let Action::PublishFork { seed } = &command.action {
                self.fork_verifier.as_ref().ok_or_else(||
                    guarded_boundary(returned_custody.work(), crate::wire::ErrorCode::Unsupported,
                        "fork seed verifier is not bound"))?
                    .release_private_fence_with_work(seed, returned_custody.work()).await?;
            }
            returned_custody.transfer();
            return Ok((value, returned_owned));
        }
        let mut transient = crate::wire_codec::CodecQuota::from_retained(work, 0);
        let (planned, planned_quota) = if transient.work().is_some() {
            self.reducer.plan_indexed_with_retained_quota(&command, false, transient.work())?
        } else {
            (self.plan_command(&command, false).await?, 0)
        };
        transient.adopt(planned_quota)?;
        let ApplyResult::Applied { event } = planned else {
            transient.transfer();
            return Ok((planned, planned_quota));
        };
        if transient.work().is_none() { self.validate_admission_content(&command.action).await?; }
        else {
            if matches!(command.action, Action::AppendCustom { .. } | Action::MigrateExtensionState { .. }
                | Action::ConfigureExtension { .. } | Action::AppendConversationMessage { .. }
                | Action::PublishProjectMerge { .. })
            {
                return Err(guarded_boundary(transient.work(), crate::wire::ErrorCode::Unsupported,
                    "command content has no finite admission engine"));
            }
            self.validate_task_admission_content_with_work(&command.action, transient.work()).await?;
            if let Action::ResolveInteraction { resolution } = &command.action
                && let Some(detail) = &resolution.detail
            {
                let ticket = self.reducer.interaction(&resolution.id).ok_or_else(||
                    guarded_boundary(transient.work(), crate::wire::ErrorCode::NotFound, "interaction is not found"))?;
                self.validate_interaction_part_with_work(&ticket.0, detail, Some(&resolution.outcome), transient.work()).await?;
            }
        }
        self.validate_causal_reference(event.causal_parent.as_ref(), transient.work()).await?;
        let publication = checkpoints::publication_with_work(
            &self.client, &self.reducer, &event, transient.work()).await?;
        let publication_owned = if transient.work().is_some() { publication.retained_owned() } else { 0 };
        transient.adopt(publication_owned)?;
        if return_event && let Some(work) = transient.work().as_deref_mut() {
            work.stage_output(publication.bytes.len() as u64, 1)?;
        }
        let mut key = Some(stream_idempotency_key_with_work(
            self.stream.path().as_str(), &command.idempotency_key, transient.work())?);
        let key_owned = if owner.is_some() && transient.work().is_some() { 32 } else { 0 };
        transient.adopt(key_owned)?;
        let fork_guard = if let Action::PublishFork { seed } = &command.action {
            Some(self.fork_verifier.as_ref().ok_or_else(||
                guarded_boundary(transient.work(), crate::wire::ErrorCode::Unsupported,
                    "fork seed verifier is not bound"))?
                .activate_with_work(seed, transient.work()).await?)
        } else { None };
        let mut append_phase = crate::distributed::CommitPhase::NotCalled;
        let appended = async {
            if let Some(owner) = owner {
                return owner.append_conversation(
                    &publication, key.as_ref().expect("publication key"), write, transient.work(), Some(&mut append_phase)).await;
            }
            let request = publication.request_with_work(
                &self.client, key.take().expect("publication key"), transient.work()).await?;
            let (outcome, retained) = crate::distributed::commit_prepared(
                &self.client, request, command.operation_id, Some(&mut append_phase), None, None,
            ).await?;
            let mut acknowledged = crate::wire_codec::CodecQuota::from_retained(transient.work(), retained);
            match outcome {
                CommitOutcome::Committed(envelope) => {
                    let verified = publication.verify_with_work(&envelope, acknowledged.work());
                    drop(envelope);
                    verified.map_err(|error| guarded_publication_error(error, command.operation_id))?;
                    Ok(true)
                }
                CommitOutcome::Conflict(_) => Ok(false),
            }
        }.await;
        drop(publication);
        let released_publication = transient.relinquish(publication_owned);
        drop(key);
        let released_key = transient.relinquish(key_owned);
        match appended {
            Ok(true) => {
                released_publication.map_err(|error| guarded_publication_error(error, command.operation_id))?;
                released_key.map_err(|error| guarded_publication_error(error, command.operation_id))?;
            },
            Ok(false) => {
                let _ = released_key;
                let _ = released_publication;
                if let Some(guard) = fork_guard.as_deref() {
                    crate::fork::release_fork_guard_with_work(guard, transient.work()).await?;
                }
                return Err(guarded_boundary(transient.work(), crate::wire::ErrorCode::Conflict,
                    "task publication lost its owner or conversation tail"));
            },
            Err(error) => {
                let _ = released_key;
                let _ = released_publication;
                if matches!(append_phase, crate::distributed::CommitPhase::NotCalled
                    | crate::distributed::CommitPhase::Conflict | crate::distributed::CommitPhase::Failed)
                    && let Some(guard) = fork_guard.as_deref() {
                    let _ = crate::fork::release_fork_guard_with_work(guard, transient.work()).await;
                }
                return Err(error);
            },
        }
        let (cloned, returned_owned) = crate::core::native_event_clone_with_custody(
            &event, transient.work())?;
        transient.adopt(returned_owned)?;
        let applied = self.reducer.apply_committed_with_work(cloned, transient.work())
            .and_then(|result| self.reducer.mark_indexed_terminal_effect_with_work(
                &event, transient.work()).map(|()| result));
        if let Some(guard) = fork_guard.as_deref() {
            crate::fork::release_fork_guard_with_work(guard, transient.work()).await
                .map_err(|error| guarded_publication_error(error, command.operation_id))?;
        }
        drop(event);
        let released_planned = transient.relinquish(planned_quota);
        match applied {
            Ok(result) => {
                released_planned.map_err(|error| guarded_publication_error(error, command.operation_id))?;
                transient.transfer();
                Ok((result, returned_owned))
            },
            Err(error) => {
                let _ = released_planned;
                Err(guarded_publication_error(error, command.operation_id))
            },
        }
    }


}
