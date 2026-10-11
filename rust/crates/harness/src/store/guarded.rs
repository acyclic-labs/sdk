use super::*;
use crate::durable_host::TaskJournalOwner;
use acyclic_fs::{CancellationToken, MeasuredResult, OperationReceipt, WorkBudget};

fn guarded_publication_error(error: Error, operation: OperationId) -> Error {
    #[cfg(not(target_arch = "wasm32"))]
    if matches!(&error, Error::NativeWork { .. } | Error::NativeBoundary { .. }) { return error; }
    Error::Indeterminate(operation)
}

enum GuardedBoundary {
    Invalid,
    Unauthorized,
    Conflict,
    Unsupported,
    Storage,
}

fn guarded_boundary(
    work: &Option<&mut dyn history::HistoryIo>, kind: GuardedBoundary,
    reason: &'static str,
) -> Error {
    #[cfg(not(target_arch = "wasm32"))]
    if work.as_ref().is_some_and(|work| work.original_native_work().is_some()) {
        let code = match kind {
            GuardedBoundary::Invalid => crate::wire::ErrorCode::Invalid,
            GuardedBoundary::Unauthorized => crate::wire::ErrorCode::Unauthorized,
            GuardedBoundary::Conflict => crate::wire::ErrorCode::Conflict,
            GuardedBoundary::Unsupported => crate::wire::ErrorCode::Unsupported,
            GuardedBoundary::Storage => crate::wire::ErrorCode::Storage,
        };
        return Error::NativeBoundary { code, reason };
    }
    let _ = work;
    match kind {
        GuardedBoundary::Invalid => Error::Invalid(reason.into()),
        GuardedBoundary::Unauthorized => Error::Unauthorized(reason.into()),
        GuardedBoundary::Conflict => Error::Conflict(reason.into()),
        GuardedBoundary::Unsupported => Error::Unsupported(reason.into()),
        GuardedBoundary::Storage => Error::Storage(reason.into()),
    }
}

impl<P: StreamProvider> StreamAggregate<P> {
    /// Executes or authenticates the original ROOT Bind command on the ordinary conversation engine.
    pub async fn execute_task_command_measured(
        &mut self, command: Command, owner: &TaskJournalOwner<P>,
        budget: WorkBudget, cancellation: &CancellationToken,
    ) -> MeasuredResult<OperationReceipt<ApplyResult>, Error> {
        let mut work = history::HistoryWork::new(budget, cancellation);
        let result = async {
            work.preflight()?;
            self.execute_root_bind_with_work(command, owner, &mut Some(&mut work)).await
        }.await;
        work.finish(result)
    }

    /// Executes the same protected ROOT binding on the original native ledger.
    #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
    pub async fn execute_task_command_with_native_work(
        &mut self, command: Command, owner: &TaskJournalOwner<P>,
        original_work: &acyclic_fs::NativeMaterializationWork,
    ) -> Result<(ApplyResult, acyclic_fs::NativeAllocation)> {
        let mut native_io = history::native_work_io(original_work)
            .map_err(|cause| Error::NativeWork { cause, custody: None })?;
        let result = self.execute_root_bind_with_work(
            command, owner, &mut Some(&mut native_io),
        ).await?;
        native_io.return_staged_output()?;
        Ok((result, native_io.into_custody()))
    }

    async fn execute_root_bind_with_work(
        &mut self, command: Command, owner: &TaskJournalOwner<P>,
        work: &mut Option<&mut dyn history::HistoryIo>,
    ) -> Result<ApplyResult> {
        if let Some(work) = work.as_deref_mut() { work.check_cancellation()?; }
        let Action::BindConversation { agent } = &command.action else {
            return Err(guarded_boundary(work, GuardedBoundary::Invalid, "measured ROOT binding requires the original BindConversation command"));
        };
        if self.reducer.authority().kind != crate::core::AggregateKind::Conversation
            || command.causal_parent.is_some()
            || command.scope.agent() != Some(*agent)
            || owner.authenticated_binding_agent()? != Some(*agent)
        {
            return Err(guarded_boundary(work, GuardedBoundary::Unauthorized, "ROOT binding differs from its original owner, actor or causal policy"));
        }
        owner.authenticate_binding_scope(self.reducer.authority(), &command.scope, work)?;
        let gate = owner.lock_root_binding().await
            .ok_or_else(|| guarded_boundary(work, GuardedBoundary::Unauthorized, "published children use their canonical child binding, not ROOT binding"))?;
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
        let result = if let Some(replayed) = self.replay_indexed_with_work(&command, true, work).await? {
            replayed
        } else {
            self.execute_guarded_publication_with_work(command, owner,
                crate::distributed::JournalWrite::Fresh, true, work).await?
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

    pub(super) async fn append_planned_command_with_work(
        &mut self, command: &Command, event: crate::core::Event,
        planned_quota: u64, publication: operations::IndexedPublication,
        mut request: crate::distributed::PreparedCommitRequest<'_, '_>,
        fork_guard: Option<crate::fork::ObservedForkPublicationGuard>,
    ) -> Result<ApplyResult> {
        let applied = async {
            let append = match request.commit_once(&self.client, None).await {
                Ok(append) => append,
                Err(error) => {
                    if matches!(request.commit_phase(),
                        crate::distributed::CommitPhase::NotCalled
                            | crate::distributed::CommitPhase::Conflict
                            | crate::distributed::CommitPhase::Failed)
                        && let Some(guard) = fork_guard.as_deref() {
                        let _ = crate::fork::release_fork_guard_with_work(guard, request.work()).await;
                    }
                    return Err(error);
                },
            };
            let committed = match append {
                Ok(committed) => committed,
                Err(StreamError::Unavailable) => {
                    return match self.reconcile_inner_with_work(command, request.work()).await {
                        Ok(Some(result)) => {
                            if let Some(guard) = fork_guard.as_deref() {
                                crate::fork::release_fork_guard_with_work(guard, request.work()).await
                                    .map_err(|error| guarded_publication_error(error, command.operation_id))?;
                            }
                            Ok(result)
                        },
                        Ok(None) | Err(Error::Storage(_)) => Err(Error::Indeterminate(command.operation_id)),
                        Err(error) => Err(error),
                    };
                },
                Err(StreamError::IdempotencyMismatch) => {
                    if let Some(guard) = fork_guard.as_deref() {
                        crate::fork::release_fork_guard_with_work(guard, request.work()).await
                            .map_err(|error| guarded_publication_error(error, command.operation_id))?;
                    }
                    return Err(guarded_boundary(request.work(), GuardedBoundary::Conflict,
                        "retry identity is already bound to another append"));
                },
                Err(error) => {
                    if let Some(guard) = fork_guard.as_deref() {
                        crate::fork::release_fork_guard_with_work(guard, request.work()).await
                            .map_err(|error| guarded_publication_error(error, command.operation_id))?;
                    }
                    return Err(operations::stream_error_with_work(error, request.work()));
                },
            };
            let mut observed = crate::wire_codec::CodecQuota::from_retained(request.work(), committed.1);
            let outcome = committed.0;
            let applied = match &outcome {
                CommitOutcome::Committed(envelope) => {
                    publication.verify_with_work(envelope, observed.work())
                        .map_err(|error| guarded_publication_error(error, publication.operation_id))?;
                    let (cloned, _) = crate::core::native_event_clone_with_custody(
                        &event, observed.work())?;
                    self.reducer.apply_committed_with_work(cloned, observed.work())
                        .and_then(|result| self.reducer.mark_indexed_terminal_effect_with_work(
                            &event, observed.work()).map(|()| result))
                        .map_err(|error| guarded_publication_error(error, command.operation_id))
                },
                CommitOutcome::Conflict(_) => Err(guarded_boundary(
                    observed.work(), GuardedBoundary::Conflict,
                    "aggregate tail or operation identity changed")),
            };
            drop(outcome);
            drop(observed);
            if let Some(guard) = fork_guard.as_deref() {
                crate::fork::release_fork_guard_with_work(guard, request.work()).await
                    .map_err(|error| guarded_publication_error(error, command.operation_id))?;
            }
            applied
        }.await;
        let publication_bytes = publication.into_retained_owned();
        drop(event);
        let released = match request.work().as_deref_mut() {
            Some(observer) => observer.release_owned(publication_bytes + planned_quota),
            None => Ok(()),
        };
        match applied {
            Ok(result) => {
                released.map_err(|error| guarded_publication_error(error, command.operation_id))?;
                Ok(result)
            },
            Err(error) => { let _ = released; Err(error) },
        }
    }

    pub(super) async fn release_replayed_fork_fence_with_work(
        &self, seed: &crate::fork::ForkSeed,
        work: &mut Option<&mut dyn history::HistoryIo>,
    ) -> Result<()> {
        self.fork_verifier.as_ref().ok_or_else(||
            guarded_boundary(work, GuardedBoundary::Unsupported, "fork seed verifier is not bound"))?
            .release_private_fence_with_work(seed, work).await
    }

    pub(super) async fn reconcile_inner_with_work(
        &mut self, command: &Command,
        work: &mut Option<&mut dyn history::HistoryIo>,
    ) -> Result<Option<ApplyResult>> {
        if let Some(result) = self.replay_indexed_with_work(command, false, work).await? {
            return Ok(Some(result));
        }
        let key = stream_idempotency_key_with_work(self.stream.path().as_str(), &command.idempotency_key, work)?;
        let retained_key = if work.is_some()
            && !work.as_deref().is_some_and(crate::store::operations::original_native_decode_mode)
        { key.as_bytes().len() as u64 } else { 0 };
        let observation = crate::distributed::inspect_idempotency_once(
            &self.client, key, retained_key, work, None, None,
        ).await?.map_err(|error| error.into_error(work))?;
        let Some((observation, quota)) = observation else { return Ok(None); };
        let mut retained = crate::wire_codec::CodecQuota::from_retained(work, quota);
        let observation = observation;
        let result = match &observation.outcome {
            IdempotencyOutcome::Commit(CommitOutcome::Conflict(_)) => {
                if let Action::PublishFork { seed } = &command.action {
                    self.release_replayed_fork_fence_with_work(seed, retained.work()).await?;
                }
                Err(guarded_boundary(retained.work(), GuardedBoundary::Conflict, "aggregate publication was rejected"))
            },
            IdempotencyOutcome::Commit(CommitOutcome::Committed(_)) =>
                Err(guarded_boundary(retained.work(), GuardedBoundary::Storage,
                    "committed aggregate publication has no atomic operation location")),
            _ => Err(guarded_boundary(retained.work(), GuardedBoundary::Conflict,
                "retry identity is bound to another operation contract")),
        };
        drop(observation);
        drop(retained);
        result
    }

    pub(crate) async fn execute_guarded_publication_with_work(
        &mut self, command: Command, owner: &TaskJournalOwner<P>,
        write: crate::distributed::JournalWrite,
        return_event: bool,
        work: &mut Option<&mut dyn history::HistoryIo>,
    ) -> Result<ApplyResult> {
        self.execute_guarded_publication_with_retained_quota(
            command, owner, write, return_event, work).await.map(|(result, _)| result)
    }

    pub(super) async fn execute_guarded_publication_with_retained_quota(
        &mut self, command: Command, owner: &TaskJournalOwner<P>,
        write: crate::distributed::JournalWrite, return_event: bool,
        work: &mut Option<&mut dyn history::HistoryIo>,
    ) -> Result<(ApplyResult, u64)> {
        owner.verify_with_work(write.settlement(), work).await?;
        if let Some(result) = self.replay_indexed_with_retained_quota(&command, return_event, work).await? {
            let returned_owned = result.1;
            let mut returned_custody = crate::wire_codec::CodecQuota::from_retained(work, returned_owned);
            let value = result.0;
            if let Action::PublishFork { seed } = &command.action {
                self.fork_verifier.as_ref().ok_or_else(||
                    guarded_boundary(returned_custody.work(), GuardedBoundary::Unsupported,
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
        else if !matches!(command.action, Action::BindConversation { .. } | Action::PublishFork { .. }) {
            return Err(guarded_boundary(transient.work(), GuardedBoundary::Unsupported,
                "this measured guarded producer requires ROOT or published-child binding/fork publication"));
        }
        self.validate_causal_reference_with_work(event.causal_parent.as_ref(), transient.work()).await?;
        let publication = checkpoints::publication_with_work(
            &self.client, &self.reducer, &event, transient.work()).await?;
        let publication_owned = if transient.work().is_some() { publication.retained_owned() } else { 0 };
        transient.adopt(publication_owned)?;
        if return_event && let Some(work) = transient.work().as_deref_mut() {
            work.stage_output(publication.bytes.len() as u64, 1)?;
        }
        let key = stream_idempotency_key_with_work(
            self.stream.path().as_str(), &command.idempotency_key, transient.work())?;
        let key_owned = if transient.work().is_some() { 32 } else { 0 };
        transient.adopt(key_owned)?;
        let fork_guard = if let Action::PublishFork { seed } = &command.action {
            Some(self.fork_verifier.as_ref().ok_or_else(||
                guarded_boundary(transient.work(), GuardedBoundary::Unsupported,
                    "fork seed verifier is not bound"))?
                .activate_with_work(seed, transient.work()).await?)
        } else { None };
        let mut append_phase = crate::distributed::CommitPhase::NotCalled;
        let appended = owner.append_conversation(
            &publication, &key, write, transient.work(), Some(&mut append_phase)).await;
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
                return Err(guarded_boundary(transient.work(), GuardedBoundary::Conflict,
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

    pub(super) async fn replay_indexed_with_work(
        &mut self, command: &Command, return_event: bool,
        work: &mut Option<&mut dyn history::HistoryIo>,
    ) -> Result<Option<ApplyResult>> {
        self.replay_indexed_with_retained_quota(command, return_event, work).await
            .map(|result| result.map(|(result, _)| result))
    }

    pub(super) async fn replay_indexed_with_retained_quota(
        &mut self, command: &Command, return_event: bool,
        work: &mut Option<&mut dyn history::HistoryIo>,
    ) -> Result<Option<(ApplyResult, u64)>> {
        let mut owned = crate::wire_codec::CodecQuota::from_retained(work, 0);
        self.reducer.verify_command_scope_with_work(command, owned.work())?;
        let verifier = if owned.work().is_some() {
            self.reducer.event_verifier_with_work(owned.work())?
        } else { self.reducer.event_verifier() };
        let verifier_bytes = if owned.work().is_some() { verifier.logical_owned_bytes()? } else { 0 };
        let read = operations::bounded_operation_with_retained_quota(
            &self.client, self.reducer.authority(), &verifier,
            command.operation_id, default_projection_read_limits().maximum_bytes, return_event, owned.work(),
        ).await;
        drop(verifier);
        let released_verifier = match owned.work().as_deref_mut() {
            Some(work) => work.release_owned(verifier_bytes),
            None => Ok(()),
        };
        let (event, _) = match read {
            Ok(read) => { released_verifier?; read },
            Err(error) => {
                let _ = released_verifier;
                return Err(error);
            },
        };
        let Some((event, original_owned)) = event else {
            if self.reducer.operation_revision(command.operation_id).is_some() {
                return Err(guarded_boundary(owned.work(), GuardedBoundary::Storage,
                    "resident operation has no atomic canonical location"));
            }
            return Ok(None);
        };
        owned.adopt(original_owned)?;
        self.reducer.verify_retained_command_with_work(command, &event, owned.work())?;
        if event.revision > self.reducer.revision() {
            if event.revision != crate::contract::next_revision(self.reducer.revision())? {
                return Err(guarded_boundary(owned.work(), GuardedBoundary::Conflict,
                    "aggregate must refresh to the indexed operation boundary"));
            }
            let indexed = effects::retirement_proof_for_event_with_work(
                &self.client, self.reducer.authority(), &event, owned.work()).await?;
            let (cloned, returned_owned) = crate::core::native_event_clone_with_custody(&event, owned.work())?;
            owned.adopt(returned_owned)?;
            let result = self.reducer.apply_committed_with_work(cloned, owned.work())?;
            if indexed { self.reducer.mark_indexed_terminal_effect_with_work(&event, owned.work())?; }
            self.reducer.enforce_terminal_effect_limit()?;
            drop(event);
            owned.relinquish(original_owned)?;
            owned.transfer();
            return Ok(Some((result, returned_owned)));
        }
        owned.transfer();
        Ok(Some((ApplyResult::Replayed { event }, original_owned)))
    }

    pub(super) async fn read_event_at_with_work(
        &self, revision: u64, work: &mut Option<&mut dyn history::HistoryIo>,
    ) -> Result<crate::core::Event> {
        self.read_event_at_with_retained_quota(revision, work).await.map(|(event, _)| event)
    }

    pub(super) async fn read_event_at_with_retained_quota(
        &self, revision: u64, work: &mut Option<&mut dyn history::HistoryIo>,
    ) -> Result<(crate::core::Event, u64)> {
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
        let retained = page.events_retained_bytes;
        let returned_cursor = page.page.cursor.authority.id.capacity() as u64;
        drop(page.page.cursor);
        let event = page.page.events.into_iter().next();
        if let Some(work) = work.as_deref_mut() { work.release_owned(cursor_bytes + returned_cursor + vector_bytes)?; }
        let event = event.ok_or_else(|| guarded_boundary(work, GuardedBoundary::NotFound, "committed event is unavailable"))?;
        Ok((event, retained))
    }

    pub(super) async fn validate_causal_reference_with_work(
        &self, reference: Option<&EventReference>, work: &mut Option<&mut dyn history::HistoryIo>,
    ) -> Result<()> {
        let Some(reference) = reference else { return Ok(()); };
        if reference.authority == *self.reducer.authority() { return Ok(()); }
        let path = reference.authority.stream_path_with_work(work)?;
        let path_bytes = if work.is_some() { path.capacity() as u64 } else { 0 };
        let read = operations::one_record_with_work(&self.client, &path,
            reference.revision.saturating_sub(1), work).await;
        drop(path);
        let released_path = match work.as_deref_mut() {
            Some(observer) => observer.release_owned(path_bytes), None => Ok(()),
        };
        let record = match read {
            Ok(record) => { released_path?; record },
            Err(error) => { let _ = released_path; return Err(error); },
        }.ok_or_else(|| guarded_boundary(work, GuardedBoundary::NotFound, "causal event"))?;
        let raw = operations::sdk_record_retained_bytes(&record, work);
        let decoded = crate::wire_codec::decode_event_with_work_and_custody(&record.value, work);
        drop(record);
        let released_record = match work.as_deref_mut() {
            Some(observer) => observer.release_record(raw), None => Ok(()),
        };
        let ((authority, event), event_retained) = match decoded {
            Ok(decoded) => { released_record?; decoded },
            Err(error) => { let _ = released_record; return Err(error); },
        };
        let valid = authority == reference.authority && event.revision == reference.revision;
        let owned = if work.is_some() {
            event_retained
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
