use super::*;
use crate::executor::stock_boundary;

#[cfg(feature = "research")]
impl<P, A, O> crate::executor::SequencedExecutionJournal for FilesystemExecutionJournal<P, A, O>
where
    P: crate::store::JournalProvider + ?Sized,
    A: AsyncAuthorityStore + 'static,
    O: AsyncObjectStore + 'static,
{
    fn append_with_sequence<'a>(
        &'a self,
        operation: OperationId,
        key: &'a str,
        event: &'a ExecutionEvent,
        io: Option<&'a PreparationIo>,
    ) -> Result<crate::conversation::BoundedContentFuture<'a, u64>> {
        self.append_projected(operation, None, key, event, &[], None, io, move |receipt| {
            receipt
                .sequence
                .filter(|sequence| *sequence > 0)
                .ok_or(Error::Indeterminate(operation))
        })
    }
}

impl<P, A, O> ToolApprovalVerifier for FilesystemExecutionJournal<P, A, O>
where
    P: crate::store::JournalProvider + ?Sized,
    A: AsyncAuthorityStore + 'static,
    O: AsyncObjectStore + 'static,
{
    fn verify<'a>(
        &'a self,
        id: InteractionId,
        operation: OperationId,
        digest: [u8; 32],
    ) -> BoxFuture<'a, Result<()>> {
        Box::pin(self.verify_tool_approval_with_io(id, operation, digest, None))
    }

    #[cfg(not(target_arch = "wasm32"))]
    fn verify_with_native_work<'a>(
        &'a self,
        id: InteractionId,
        operation: OperationId,
        digest: [u8; 32],
        work: &'a acyclic_fs::NativeMaterializationWork,
    ) -> Result<acyclic_fs::NativeFutureBundle<'a, (), crate::Error>> {
        let io = PreparationIo::from_native_work(work)?;
        acyclic_fs::NativeFutureBundle::new(work, || async move {
            self.verify_tool_approval_with_io(id, operation, digest, Some(&io))
                .await
        })
    }
}

impl<P, A, O> ExecutionJournal for FilesystemExecutionJournal<P, A, O>
where
    P: crate::store::JournalProvider + ?Sized,
    A: AsyncAuthorityStore + 'static,
    O: AsyncObjectStore + 'static,
{
    fn output_content(
        &self,
        operation: OperationId,
    ) -> Result<Option<crate::runtime::ContentBindings>> {
        self.require_operation(operation, None)?;
        Ok(Some(crate::runtime::ContentBindings {
            volume: self.volume.clone(),
            reader: crate::filesystem::FilesystemContentVerifier::new(
                Arc::clone(&self.host),
                &self.verifier,
                &self.scope,
                super::MAXIMUM_PAYLOAD_BYTES,
                None,
            )?,
            writer: Some(crate::filesystem::FilesystemContentPublisher::new(
                Arc::clone(&self.host),
                &self.volume,
                &self.verifier,
                &self.scope,
                super::MAXIMUM_PAYLOAD_BYTES,
                None,
            )?),
        }))
    }

    fn supervision(
        &self,
    ) -> acyclic_stream::BoxProviderFuture<'_, Result<Option<crate::supervision::Control>>> {
        Box::pin(async {
            let Some((agent, run, _, _)) = &self.agent_events else {
                return Ok(None);
            };
            let checkpoint = crate::supervision::streams::Queries::new(&*self.stream, agent, *run)
                .checkpoint()
                .await?;
            Ok(checkpoint.and_then(|checkpoint| checkpoint.control()))
        })
    }

    fn run_admission(&self) -> Option<&crate::supervision::RunAdmissionSource> {
        self.run_admission.as_ref()
    }

    fn published_progress<'a>(
        &'a self,
        execution: OperationId,
        effect: OperationId,
        publication: crate::executor::ProgressPublication<'a>,
        io: Option<&'a PreparationIo>,
    ) -> Result<crate::conversation::BoundedContentFuture<'a, ()>> {
        crate::conversation::BoundedContentFuture::new(io, || async move {
            if !self
                .run_admission
                .as_ref()
                .is_some_and(|source| source.value.no_progress_turns.is_some())
            {
                return Ok(());
            }
            self.require_operation(execution, io)?;
            let (agent, run, turn, _) = self.agent_events.as_ref().ok_or_else(|| {
                stock_boundary(
                    io,
                    crate::wire::ErrorCode::Invalid,
                    "original progress has no agent binding",
                )
            })?;
            let queries = crate::supervision::streams::Queries::new(&*self.stream, agent, *run);
            if let crate::executor::ProgressPublication::NativeCapture(file) = publication {
                let event = ExecutionEvent::NativeCapture {
                    receipt: crate::model::clone_file(file, io)?,
                };
                let _receipt_hold = io
                    .map(|io| io.guard_owned(file.logical_owned_bytes()?))
                    .transpose()?;
                let key = crate::executor::with_history_io(io, |work| {
                    crate::executor::preparation_format(
                        format_args!("native-progress:{effect}:{}", file.path()),
                        work,
                    )
                })?;
                let _key_hold = io
                    .map(|io| io.guard_owned(key.capacity() as u64))
                    .transpose()?;
                return self.append(execution, &key, &event, io)?.await;
            }
            if queries.progress_in_turn(*turn).await?.0 {
                return Ok(());
            }
            let unmeasured;
            let original_file;
            let publication = match publication {
                crate::executor::ProgressPublication::NativeCapture(_) => unreachable!(),
                crate::executor::ProgressPublication::Resource(publication) => publication,
                crate::executor::ProgressPublication::Changes { volume, changes } => {
                    if !self
                        .host
                        .original_changes_progress(volume, changes, io)
                        .await?
                    {
                        return Ok(());
                    }
                    changes.generation.as_resource()
                }
                crate::executor::ProgressPublication::File(file) => {
                    let work = if let Some(io) = io {
                        io
                    } else {
                        unmeasured =
                            PreparationIo::for_unmeasured(&acyclic_fs::CancellationToken::new())?;
                        &unmeasured
                    };
                    let Some(generation) = self.host.original_file_progress(file, work).await?
                    else {
                        return Ok(());
                    };
                    let hold = work.guard_owned(generation.logical_owned_bytes()?)?;
                    original_file = (generation, hold);
                    original_file.0.as_resource()
                }
            };
            let event = ExecutionEvent::Progress {
                publication: crate::model::clone_resource(publication, io)?,
            };
            let held = io
                .map(|io| io.guard_owned(publication.logical_owned_bytes()?))
                .transpose()?;
            let event = (event, held);
            let key = crate::executor::with_history_io(io, |work| {
                crate::executor::preparation_format(format_args!("progress:{effect}"), work)
            })?;
            let _key_hold = io
                .map(|io| io.guard_owned(key.capacity() as u64))
                .transpose()?;
            let result = self.append(execution, &key, &event.0, io)?.await;
            // A concurrent original publication may have supplied the one
            // positive fact. Observe that conditional result once, without
            // retrying or inventing a tool failure for already accepted work.
            match result {
                Err(error)
                    if error.code() == crate::wire::ErrorCode::Conflict
                        && queries.progress_in_turn(*turn).await?.0 =>
                {
                    Ok(())
                }
                result => result,
            }
        })
    }

    fn settle_progress<'a>(
        &'a self,
        source: &'a crate::supervision::Position,
    ) -> acyclic_stream::BoxProviderFuture<
        'a,
        Result<Option<crate::supervision::streams::Projection>>,
    > {
        Box::pin(async move {
            let Some(policy) = self
                .run_admission
                .as_ref()
                .map(|source| &source.value)
                .filter(|policy| policy.no_progress_turns.is_some())
            else {
                return Ok(None);
            };
            let (agent, run, turn, _) = self
                .agent_events
                .as_ref()
                .ok_or_else(|| Error::Invalid("original settlement has no agent binding".into()))?;
            if source.stream
                != agent
                    .conversation()
                    .map_err(|_| Error::Invalid("invalid original conversation".into()))?
            {
                return Err(Error::Invalid(
                    "original settlement names another conversation".into(),
                ));
            }
            crate::supervision::streams::Queries::new(&*self.stream, agent, *run)
                .project_settlement(source, *turn, policy)
                .await
        })
    }

    fn model_boundary(&self) -> Option<&dyn crate::agent::ModelBoundary> {
        self.boundary.as_deref()
    }

    fn steering_snapshot(
        &self,
        operation: OperationId,
    ) -> acyclic_stream::BoxProviderFuture<'_, Result<Option<crate::agent::SteeringSnapshot>>> {
        Box::pin(async move {
            self.require_operation(operation, None)?;
            let step = {
                let mut cache = self.verified.lock().await;
                self.refresh_cached_summary(operation, &mut cache, None)
                    .await?;
                cache.summary().latest_response
            };
            let Some(step) = step else { return Ok(None) };
            let select = |event: &ExecutionEvent| {
                matches!(
                    event,
                    ExecutionEvent::ContextPrepared { .. }
                        | ExecutionEvent::ModelStarted {
                            purpose: crate::executor::ModelPurpose::Response,
                            ..
                        }
                )
            };
            let (_, records) = self
                .indexed_step(
                    crate::executor::ExecutionRecordQuery {
                        operation,
                        step,
                        call_id: None,
                        include_started: false,
                        maximum_step_records: crate::executor::ExecutionRecordQuery::step_bound(
                            Limits::default(),
                        ),
                        maximum: Some(2),
                        select: &select,
                    },
                    None,
                )
                .await?
                .ok_or_else(|| Error::Storage("admitted steering index is unavailable".into()))?;
            let mut context = None;
            let mut started = false;
            for record in &records {
                match &record.event {
                    ExecutionEvent::ContextPrepared {
                        projection,
                        steering,
                        ..
                    } => {
                        if context.replace((projection, steering)).is_some() {
                            return Err(Error::Storage(
                                "admitted response repeats its context".into(),
                            ));
                        }
                    }
                    ExecutionEvent::ModelStarted { .. } => started = true,
                    _ => {
                        return Err(Error::Storage(
                            "admitted response index selected an unrelated record".into(),
                        ));
                    }
                }
            }
            let (projection, cut) = context.filter(|_| started).ok_or_else(|| {
                Error::Storage("admitted response has no original prepared context".into())
            })?;
            let snapshot =
                crate::executor::ResponseProjection::steering_snapshot(self, projection).await?;
            if snapshot.as_ref().map(|snapshot| snapshot.through) != cut.map(|cut| cut.through) {
                return Err(Error::Storage(
                    "admitted steering differs from its frozen range".into(),
                ));
            }
            Ok(snapshot)
        })
    }

    fn cache_identity(&self) -> Option<crate::executor::ExecutionJournalCacheId> {
        Some(self.cache_identity)
    }

    fn supports_deferred_durability(&self) -> bool {
        self.can_defer_durability()
    }

    fn durability_barrier<'a>(
        &'a self,
        io: Option<&'a PreparationIo>,
    ) -> Result<crate::conversation::BoundedContentFuture<'a, ()>> {
        self.prepare_phase_seal(io)
    }

    fn acquire_execution<'a>(
        &'a self,
        operation: OperationId,
        io: Option<&PreparationIo>,
    ) -> Result<crate::executor::ExecutionAttempt<'a>> {
        self.require_operation(operation, io)?;
        let namespace = crate::executor::with_history_io(io, |work| {
            self.volume
                .identity_digest(super::MAXIMUM_PAYLOAD_BYTES, work)
        })?;
        crate::executor::execution_claim::acquire(
            self.host.filesystem.operation_claims(),
            namespace,
            operation,
            io,
        )
    }

    fn acquire_effect<'a>(
        &'a self,
        parent: OperationId,
        effect: OperationId,
        io: Option<&PreparationIo>,
    ) -> Result<crate::executor::ExecutionAttempt<'a>> {
        self.require_operation(parent, io)?;
        let volume = crate::executor::with_history_io(io, |work| {
            self.volume
                .identity_digest(super::MAXIMUM_PAYLOAD_BYTES, work)
        })?;
        let mut namespace = blake3::Hasher::new_derive_key("acyclic.execution.effects/v1");
        namespace.update(&volume);
        crate::executor::execution_claim::acquire(
            self.host.filesystem.operation_claims(),
            *namespace.finalize().as_bytes(),
            effect,
            io,
        )
    }

    fn model_cancellation(&self) -> acyclic_stream::BoxProviderFuture<'_, Result<()>> {
        Box::pin(self.follow_cancellation())
    }

    fn working_checkpoint_authority(&self) -> Option<&crate::core::AuthorityVerifier> {
        Some(&self.verifier)
    }

    fn indexed_records<'a>(
        &'a self,
        query: crate::executor::ExecutionRecordQuery<'a>,
        io: Option<&'a PreparationIo>,
    ) -> Result<Option<crate::executor::IndexedExecutionFuture<'a>>> {
        crate::conversation::BoundedContentFuture::new(io, || self.indexed_step(query, io))
            .map(Some)
    }

    fn tool_settlement<'a>(
        &'a self,
        query: crate::executor::ToolSettlementQuery<'a>,
        io: Option<&'a PreparationIo>,
    ) -> Result<
        crate::conversation::BoundedContentFuture<'a, Option<crate::executor::ToolSettlement>>,
    > {
        crate::conversation::BoundedContentFuture::new(io, || {
            self.observe_tool_settlement(query, io)
        })
    }

    #[cfg(not(target_arch = "wasm32"))]
    fn supports_native_execution(
        &self,
        work: &acyclic_fs::NativeMaterializationWork,
    ) -> Result<bool> {
        work.remaining().map_err(Error::from)?;
        Ok(true)
    }

    fn replay<'a>(
        &'a self,
        operation: OperationId,
        after: u64,
        maximum: u32,
        io: Option<&'a PreparationIo>,
    ) -> Result<crate::conversation::BoundedContentFuture<'a, Vec<ExecutionRecord>>> {
        #[cfg(all(any(test, feature = "test-support"), not(target_arch = "wasm32")))]
        acyclic_differential::record(ExecutionReplayObservation::Requested);
        crate::conversation::BoundedContentFuture::new(io, || async move {
            let records = self.records(operation, after, maximum, io).await?;
            if let Some(io) = io {
                io.remaining()?;
            }
            Ok(records)
        })
    }

    fn load<'a>(
        &'a self,
        reference: &'a FileRef,
        io: Option<&'a PreparationIo>,
    ) -> Result<crate::conversation::BoundedContentFuture<'a, acyclic_stream::OwnedBytes>> {
        crate::conversation::BoundedContentFuture::new(io, || {
            self.observe_request(
                acyclic_stream::trace::Request {
                    name: "harness.private_load",
                    mechanism: acyclic_stream::trace::Mechanism::Io,
                    parent: None,
                    guarantee: Some("authenticated_immutable_content"),
                },
                self.load_content(reference, io),
                |bytes| u64::try_from(bytes.len()).ok(),
            )
        })
    }

    fn load_range<'a>(
        &'a self,
        reference: &'a FileRef,
        range: acyclic_fs::ByteRange,
        io: Option<&'a PreparationIo>,
    ) -> Result<crate::conversation::BoundedContentFuture<'a, acyclic_stream::OwnedBytes>> {
        crate::conversation::BoundedContentFuture::new(io, || {
            self.observe_request(
                acyclic_stream::trace::Request {
                    name: "harness.private_load_range",
                    mechanism: acyclic_stream::trace::Mechanism::Io,
                    parent: None,
                    guarantee: Some("authenticated_immutable_range"),
                },
                async move {
                    if reference.volume() != &self.volume {
                        return Err(canonical_boundary(
                            io,
                            crate::wire::ErrorCode::Unauthorized,
                            "journal content belongs to another private volume",
                        ));
                    }
                    let grant = crate::executor::with_history_io(io, |work| {
                        ContentGrant::verify(
                            &self.verifier,
                            &self.scope,
                            &self.volume,
                            VolumeOperation::Read,
                            work,
                        )
                    })?;
                    let grant_hold = io
                        .map(|io| io.guard_owned(grant.logical_owned_bytes()?))
                        .transpose()?;
                    let result = self
                        .host
                        .read_content_range(reference, &grant, range, io)
                        .await;
                    drop(grant);
                    drop(grant_hold);
                    result
                },
                |bytes| u64::try_from(bytes.len()).ok(),
            )
        })
    }

    fn append<'a>(
        &'a self,
        operation: OperationId,
        key: &'a str,
        event: &'a ExecutionEvent,
        io: Option<&'a PreparationIo>,
    ) -> Result<crate::conversation::BoundedContentFuture<'a, ()>> {
        self.append_projected(operation, None, key, event, &[], None, io, |_| Ok(()))
    }

    #[cfg(feature = "research")]
    fn sequenced_appends(&self) -> Option<&dyn crate::executor::SequencedExecutionJournal> {
        Some(self)
    }

    fn append_if_tail<'a>(
        &'a self,
        operation: OperationId,
        expected_tail: u64,
        key: &'a str,
        event: &'a ExecutionEvent,
        io: Option<&'a PreparationIo>,
    ) -> Result<crate::conversation::BoundedContentFuture<'a, bool>> {
        self.append_contributed_if_tail(operation, expected_tail, key, event, &[], None, io)
    }

    fn claim_if_tail<'a>(
        &'a self,
        operation: OperationId,
        expected_tail: u64,
        key: &'a str,
        event: &'a ExecutionEvent,
        io: Option<&'a PreparationIo>,
    ) -> Result<crate::conversation::BoundedContentFuture<'a, Option<u64>>> {
        self.append_projected(
            operation,
            Some(expected_tail),
            key,
            event,
            &[],
            None,
            io,
            |receipt| Ok(receipt.committed_at_micros),
        )
    }

    fn dispatch_if_tail<'a>(
        &'a self,
        operation: OperationId,
        expected_tail: u64,
        key: &'a str,
        event: &'a mut ExecutionEvent,
        dispatch: &'a crate::tool::Dispatch,
        io: Option<&'a PreparationIo>,
    ) -> Result<crate::conversation::BoundedContentFuture<'a, bool>> {
        crate::conversation::BoundedContentFuture::new(io, || async move {
            use indexed_publication::Contribution;
            let mut contributions = [
                Contribution::Append {
                    path: &dispatch.delivery.0,
                    record: &dispatch.delivery.1,
                },
                Contribution::Append {
                    path: &dispatch.activation.0,
                    record: &dispatch.activation.1,
                },
            ];
            if dispatch.delivery.0 > dispatch.activation.0 {
                contributions.swap(0, 1);
            }
            let identity = indexed_publication::contribution_identity(&contributions, io)?;
            let ExecutionEvent::ToolStarted { publication, .. } = event else {
                return Err(crate::filesystem::stock_boundary(
                    io,
                    crate::wire::ErrorCode::Invalid,
                    "remote dispatch requires its original tool start",
                ));
            };
            if publication.is_some() && *publication != identity {
                return Err(crate::filesystem::stock_boundary(
                    io,
                    crate::wire::ErrorCode::Invalid,
                    "remote dispatch differs from its original tool start",
                ));
            }
            *publication = identity;
            self.append_contributed_if_tail(
                operation,
                expected_tail,
                key,
                event,
                &contributions,
                None,
                io,
            )?
            .await
        })
    }

    fn append_envelope_if_tail<'a>(
        &'a self,
        expected_tail: u64,
        key: &'a str,
        step: u32,
        envelope: &'a crate::conversation::envelope::Envelope,
        io: &'a PreparationIo,
    ) -> Result<crate::conversation::BoundedContentFuture<'a, bool>> {
        self.append_conversation(expected_tail, key, step, envelope, io)
    }

    fn stage_prepared_model_request<'a>(
        &'a self,
        operation: OperationId,
        key: &'a str,
        prepared: &'a crate::model::PreparedModelRequest,
        previous: Option<&'a FileRef>,
        io: Option<&'a PreparationIo>,
    ) -> Result<Option<crate::conversation::BoundedContentFuture<'a, FileRef>>> {
        if !crate::executor::transcript::limits_admit(prepared.admitted_limits(), Limits::default())
        {
            return Ok(None);
        }
        crate::conversation::BoundedContentFuture::new(io, || {
            crate::executor::transcript::stage_attested_request(
                self,
                operation,
                key,
                prepared,
                previous.or(self.preceding_request.as_ref()),
                &self.verifier,
                io,
            )
        })
        .map(Some)
    }

    fn staged_ref<'a>(
        &'a self,
        operation: OperationId,
        key: &'a str,
        io: Option<&'a PreparationIo>,
    ) -> Result<crate::conversation::BoundedContentFuture<'a, Option<FileRef>>> {
        crate::conversation::BoundedContentFuture::new(io, || {
            self.staged_ref_with_io(operation, key, io)
        })
    }
    fn stage<'a>(
        &'a self,
        operation: OperationId,
        key: &'a str,
        bytes: &'a [u8],
        media: &'static str,
        io: Option<&'a PreparationIo>,
    ) -> Result<crate::conversation::BoundedContentFuture<'a, FileRef>> {
        crate::conversation::BoundedContentFuture::new(io, || {
            self.observe_request(
                acyclic_stream::trace::Request {
                    name: "harness.private_stage",
                    mechanism: acyclic_stream::trace::Mechanism::Commit,
                    parent: None,
                    guarantee: Some("authenticated_private_publication"),
                },
                self.stage_with_io(operation, key, bytes, media, io),
                |_| u64::try_from(bytes.len()).ok(),
            )
        })
    }

    fn verify_input_file<'a>(
        &'a self,
        reference: &'a FileRef,
        io: Option<&'a PreparationIo>,
    ) -> Result<crate::conversation::BoundedContentFuture<'a, ()>> {
        crate::conversation::BoundedContentFuture::new(io, || {
            self.observe_request(
                acyclic_stream::trace::Request {
                    name: "harness.verify_input_file",
                    mechanism: acyclic_stream::trace::Mechanism::AuthCheck,
                    parent: None,
                    guarantee: Some("authenticated_original_input_file"),
                },
                self.verify_input_file_with_io(reference, io),
                |_| None,
            )
        })
    }

    fn has_file_bound_model_verification(&self) -> bool {
        true
    }

    fn verify_model_content<'a>(
        &'a self,
        content: &'a ModelContent,
        io: Option<&'a PreparationIo>,
        context: Option<&'a crate::runtime::TaskContext>,
        summary_files: &'a [&'a FileRef],
    ) -> Result<crate::conversation::BoundedContentFuture<'a, ()>> {
        crate::conversation::BoundedContentFuture::new(io, || {
            self.verify_model_content_with_io::<true>(content, io, context, summary_files)
        })
    }

    fn authenticate_publication<'a>(
        &'a self,
        operation: OperationId,
        file: &'a FileRef,
        io: Option<&'a PreparationIo>,
    ) -> Result<crate::conversation::BoundedContentFuture<'a, bool>> {
        crate::conversation::BoundedContentFuture::new(io, || {
            self.observe_request(
                acyclic_stream::trace::Request {
                    name: "harness.authenticate_publication",
                    mechanism: acyclic_stream::trace::Mechanism::AuthCheck,
                    parent: None,
                    guarantee: Some("original_publication_membership_decision"),
                },
                self.authenticate_publication_with_io(operation, file, io),
                |_| None,
            )
        })
    }

    fn authenticate_summary<'a>(
        &'a self,
        file: &'a FileRef,
        io: Option<&'a PreparationIo>,
    ) -> Result<crate::conversation::BoundedContentFuture<'a, bool>> {
        crate::conversation::BoundedContentFuture::new(io, || {
            self.authenticate_summary_with_io(file, io)
        })
    }

    fn verify_selected_context<'a>(
        &'a self,
        operation: OperationId,
        selected: &'a SelectedModelContext,
        io: Option<&'a PreparationIo>,
    ) -> Result<crate::conversation::BoundedContentFuture<'a, ()>> {
        crate::conversation::BoundedContentFuture::new(io, || {
            self.verify_selected_context_with_io(operation, selected, io)
        })
    }

    fn open_interaction<'a>(
        &'a self,
        id: InteractionId,
        interaction: &'a Interaction,
        io: Option<&'a PreparationIo>,
    ) -> Result<crate::conversation::BoundedContentFuture<'a, ()>> {
        crate::conversation::BoundedContentFuture::new(io, || async move {
            let temporary = PreparationIo::temporary(io)?;
            let active = temporary.as_ref().or(io);
            self.open_interaction_with_io(id, interaction, active).await
        })
    }

    fn tool_barrier<'a>(
        &'a self,
        operation: OperationId,
        io: Option<&'a PreparationIo>,
    ) -> Option<Result<crate::conversation::BoundedContentFuture<'a, bool>>> {
        let approvals = self.agent_approvals.as_ref()?;
        if !approvals.has_effect_barrier() {
            return None;
        }
        #[cfg(not(target_arch = "wasm32"))]
        if io.is_some_and(|io| io.original_native_work().is_some()) {
            return Some(Err(canonical_boundary(
                io,
                crate::wire::ErrorCode::Unsupported,
                "agent effect barrier has no native observation producer",
            )));
        }
        Some(crate::conversation::BoundedContentFuture::new(io, || {
            approvals.ready(operation)
        }))
    }

    fn defer_tool(&self, until: crate::agent::Until, io: Option<&PreparationIo>) -> Result<()> {
        self.approvals(io)?.defer(until)
    }

    fn preparation_deadline(
        &self,
        deadline_unix_ms: u64,
    ) -> Result<acyclic_stream::BoxProviderFuture<'static, Result<()>>> {
        let deadline = self
            .clock
            .as_ref()
            .ok_or_else(|| Error::Unsupported("execution journal has no host clock".into()))?
            .sleep_until(deadline_unix_ms);
        Ok(Box::pin(async move {
            deadline
                .await
                .map_err(|error| Error::Invalid(error.to_string()))
        }))
    }

    fn commit_tool_effects(&self, io: Option<&PreparationIo>) -> Result<()> {
        self.approvals(io)?.commit_effects()
    }

    fn interaction_outcome<'a>(
        &'a self,
        id: InteractionId,
        io: Option<&'a PreparationIo>,
    ) -> Result<crate::conversation::BoundedContentFuture<'a, Option<InteractionOutcome>>> {
        crate::conversation::BoundedContentFuture::new(io, || async move {
            let temporary = PreparationIo::temporary(io)?;
            let active = temporary.as_ref().or(io);
            let outcome = self.interaction_outcome_with_io(id, active).await?;
            #[cfg(not(target_arch = "wasm32"))]
            if let Some(temporary) = temporary {
                let retained = temporary.owned_bytes()?;
                io.ok_or_else(|| {
                    crate::executor::stock_boundary(
                        io,
                        crate::wire::ErrorCode::Invalid,
                        "native parent IO",
                    )
                })?
                .absorb_native_custody(temporary.into_native_custody(retained)?)?;
            }
            Ok(outcome)
        })
    }
}
