use super::*;
use crate::durable_host::TaskJournalOwner;
use acyclic_fs::{CancellationToken, MeasuredResult, OperationReceipt, WorkBudget};

impl<P: StreamProvider> StreamAggregate<P> {
    /// Executes or authenticates the original ROOT Bind command on the ordinary
    /// conversation engine. A retry never republishes or derives a plugin ID.
    pub async fn execute_task_command_measured(
        &mut self, command: Command, owner: &TaskJournalOwner<P>,
        budget: WorkBudget, cancellation: &CancellationToken,
    ) -> MeasuredResult<OperationReceipt<ApplyResult>, Error> {
        let mut work = history::HistoryWork::new(budget, cancellation);
        let result = async {
            work.preflight()?;
            let Action::BindConversation { agent } = &command.action else {
                return Err(Error::Invalid("measured ROOT binding requires the original BindConversation command".into()));
            };
            if self.reducer.authority().kind != crate::core::AggregateKind::Conversation
                || command.causal_parent.is_some()
                || command.scope.agent() != Some(*agent)
                || owner.authenticated_binding_agent()? != Some(*agent)
            {
                return Err(Error::Unauthorized("ROOT binding differs from its original owner, actor or causal policy".into()));
            }
            owner.authenticate_binding_scope(self.reducer.authority(), &command.scope, &mut Some(&mut work))?;
            let gate = owner.lock_root_binding().await
                .ok_or_else(|| Error::Unauthorized("published children use their canonical child binding, not ROOT binding".into()))?;
            work.preflight()?;
            let intent = crate::core::canonical_intent_with_work(&command, &mut Some(&mut work))?;
            owner.check_root_binding(self.reducer.authority(), command.operation_id, intent)?;
            const ROOT_CHOICE_DOMAIN: &[u8] = b"harness/v2/root-bind-choice\0";
            let audience = self.reducer.authority();
            history::HistoryIo::hashed_bytes(&mut work,
                (ROOT_CHOICE_DOMAIN.len() + b"conversation".len() + 8 + audience.id.len()) as u64)?;
            let mut choice = blake3::Hasher::new();
            choice.update(ROOT_CHOICE_DOMAIN);
            choice.update(b"conversation");
            choice.update(&(audience.id.len() as u64).to_le_bytes());
            choice.update(audience.id.as_bytes());
            owner.pin_root_binding_choice(command.operation_id, intent, *choice.finalize().as_bytes())?;
            // Reserve the actual future immutable proof before any guarded IO.
            let (original_task, admission_digest, original_fence) = owner.binding_origin();
            let proof = VerifiedRootBinding {
                original_task, admission_digest,
                fence: crate::scheduler::LeaseFence {
                    reservation_id: history::HistoryIo::clone_text(&mut work, &original_fence.reservation_id)?,
                    placement: history::HistoryIo::clone_text(&mut work, &original_fence.placement)?,
                },
                agent: *agent,
                audience: history::HistoryIo::clone_authority(&mut work, self.reducer.authority())?,
                bind_operation: command.operation_id,
                intent_digest: intent,
            };
            // An exact immutable replay needs the original admission/scope but
            // no new publication lease. New publication retains the real fence.
            let result = if let Some(replayed) = self.replay_indexed_with_work(&command, &mut Some(&mut work)).await? {
                replayed
            } else {
                self.execute_guarded_publication_with_work(command, owner,
                    crate::distributed::JournalWrite::Fresh, &mut Some(&mut work)).await?
            };
            owner.retain_root_binding(proof)?;
            drop(gate);
            Ok(result)
        }.await;
        work.finish(result)
    }

    pub(super) async fn execute_guarded_publication_with_work(
        &mut self, command: Command, owner: &TaskJournalOwner<P>,
        write: crate::distributed::JournalWrite,
        work: &mut Option<&mut dyn history::HistoryIo>,
    ) -> Result<ApplyResult> {
        owner.verify_with_work(write.settlement(), work).await?;
        if let Some(result) = self.replay_indexed_with_work(&command, work).await? {
            if let Action::PublishFork { seed } = &command.action {
                self.fork_verifier.as_ref().ok_or_else(|| Error::Unsupported("fork seed verifier is not bound".into()))?
                    .release_private_fence_with_work(seed, work).await?;
            }
            return Ok(result);
        }
        let planned = if work.is_some() {
            self.reducer.plan_indexed_with_work(&command, false, work)?
        } else {
            self.plan_command(&command, false).await?
        };
        let ApplyResult::Applied { event } = planned else { return Ok(planned); };
        if work.is_none() { self.validate_admission_content(&command.action).await?; }
        else if !matches!(command.action, Action::BindConversation { .. } | Action::PublishFork { .. }) {
            return Err(Error::Unsupported("this measured guarded producer requires ROOT or published-child binding/fork publication".into()));
        }
        self.validate_causal_reference_with_work(event.causal_parent.as_ref(), work).await?;
        let publication = checkpoints::publication_with_work(&self.client, &self.reducer, &event, work).await?;
        let key = stream_idempotency_key(self.stream.path().as_str(), &command.idempotency_key)?;
        let fork_guard = if let Action::PublishFork { seed } = &command.action {
            Some(self.fork_verifier.as_ref().ok_or_else(|| Error::Unsupported("fork seed verifier is not bound".into()))?
                .activate_with_work(seed, work).await?)
        } else { None };
        let appended = owner.append_conversation_with_work(&publication, &key, write, work).await;
        match appended {
            Ok(true) => {},
            Ok(false) => {
                if let Some(guard) = fork_guard.as_deref() { crate::fork::release_fork_guard_with_work(guard, work).await?; }
                return Err(Error::Conflict("task publication lost its owner or conversation tail".into()));
            }
            Err(error) => {
                if !matches!(error, Error::Indeterminate(_)) {
                    if let Some(guard) = fork_guard.as_deref() { crate::fork::release_fork_guard_with_work(guard, work).await?; }
                }
                return Err(error);
            }
        }
        let reservation = if work.is_some() { crate::core::event_clone_reservation(&event)? } else { 0 };
        if let Some(work) = work.as_deref_mut() { work.retain_owned(reservation)?; }
        let applied = self.reducer.apply_committed_with_work(event.clone(), work)
            .and_then(|result| self.reducer.mark_indexed_terminal_effect(&event).map(|()| result));
        if let Some(guard) = fork_guard.as_deref() {
            crate::fork::release_fork_guard_with_work(guard, work).await
                .map_err(|_| Error::Indeterminate(command.operation_id))?;
        }
        applied.map_err(|_| Error::Indeterminate(command.operation_id))
    }

    pub(super) async fn replay_indexed_with_work(
        &mut self, command: &Command, work: &mut Option<&mut dyn history::HistoryIo>,
    ) -> Result<Option<ApplyResult>> {
        self.reducer.verify_command_scope_with_work(command, work)?;
        let (event, _) = operations::find_operation_bounded_with_work(
            &self.client, self.reducer.authority(), &self.reducer.event_verifier(),
            command.operation_id, default_projection_read_limits().maximum_bytes, work,
        ).await?;
        let Some(event) = event else {
            if self.reducer.operation_revision(command.operation_id).is_some() {
                return Err(Error::Storage("resident operation has no atomic canonical location".into()));
            }
            return Ok(None);
        };
        self.reducer.verify_retained_command_with_work(command, &event, work)?;
        if event.revision > self.reducer.revision() {
            if event.revision != crate::contract::next_revision(self.reducer.revision())? {
                return Err(Error::Conflict("aggregate must refresh to the indexed operation boundary".into()));
            }
            let indexed = effects::retirement_proof_for_event_with_work(
                &self.client, self.reducer.authority(), &event, work).await?;
            let reservation = if work.is_some() { crate::core::event_clone_reservation(&event)? } else { 0 };
            if let Some(work) = work.as_deref_mut() { work.retain_owned(reservation)?; }
            let result = self.reducer.apply_committed_with_work(event.clone(), work)?;
            if indexed { self.reducer.mark_indexed_terminal_effect(&event)?; }
            self.reducer.enforce_terminal_effect_limit()?;
            return Ok(Some(result));
        }
        Ok(Some(ApplyResult::Replayed { event }))
    }

    pub(super) async fn read_event_at_with_work(
        &self, revision: u64, work: &mut Option<&mut dyn history::HistoryIo>,
    ) -> Result<crate::core::Event> {
        let after_revision = revision.checked_sub(1)
            .ok_or_else(|| Error::Invalid("event revision must be positive".into()))?;
        let cursor = HistoryCursor {
            authority: match work.as_deref_mut() {
                Some(work) => work.clone_authority(self.reducer.authority())?,
                None => self.reducer.authority().clone(),
            },
            after_revision, through_revision: self.reducer.revision(),
        };
        let page = self.history_reader()?.read_page_with_work(&cursor,
            HistoryReadLimits { maximum_events: 1, maximum_bytes: acyclic_stream::MAX_RECORD_BYTES as u64 },
            false, work.as_mut().map(|work| &mut **work as &mut dyn history::HistoryIo)).await?;
        let cursor_bytes = cursor.authority.id.capacity() as u64;
        drop(cursor);
        let vector_bytes = (page.page.events.capacity() * std::mem::size_of::<crate::core::Event>()) as u64;
        let returned_cursor = page.page.cursor.authority.id.capacity() as u64;
        drop(page.page.cursor);
        let event = page.page.events.into_iter().next()
            .ok_or_else(|| Error::NotFound("committed event is unavailable".into()))?;
        if let Some(work) = work.as_deref_mut() { work.release_owned(cursor_bytes + returned_cursor + vector_bytes)?; }
        Ok(event)
    }

    pub(super) async fn validate_causal_reference_with_work(
        &self, reference: Option<&EventReference>, work: &mut Option<&mut dyn history::HistoryIo>,
    ) -> Result<()> {
        let Some(reference) = reference else { return Ok(()); };
        if reference.authority == *self.reducer.authority() { return Ok(()); }
        let path = reference.authority.stream_path()?;
        let record = operations::one_record_with_work(&self.client, &path,
            reference.revision.saturating_sub(1), work).await?
            .ok_or_else(|| Error::NotFound("causal event".into()))?;
        let (authority, event) = crate::wire_codec::decode_event_with_work(&record.value, work)?;
        let valid = authority == reference.authority && event.revision == reference.revision;
        let owned = if work.is_some() {
            crate::core::event_clone_reservation(&event)?
                .checked_add(authority.id.capacity() as u64)
                .ok_or_else(|| Error::Invalid("causal record custody overflows".into()))?
        } else { 0 };
        let raw = record.value.len() as u64;
        drop(record);
        drop(event);
        drop(authority);
        if let Some(work) = work.as_deref_mut() {
            work.release_record(raw)?;
            work.release_owned(owned)?;
        }
        if !valid {
            return Err(Error::Invalid("causal event reference does not resolve exactly".into()));
        }
        Ok(())
    }
}
