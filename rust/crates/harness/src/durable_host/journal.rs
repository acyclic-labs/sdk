//! Original owner checks and fenced journal publication.
use super::*;

#[cfg(feature = "filesystem")]
impl<P: StreamProvider> TaskJournalOwner<P> {
    /// Returns the authenticated task identity.
    pub fn task_id(&self) -> TaskId {
        TaskId::from_bytes(self.admission.operation_id.into_bytes())
    }

    pub(crate) fn matches_task_binding(
        &self,
        task: TaskId,
        fence: &crate::scheduler::LeaseFence,
    ) -> bool {
        self.task_id() == task && &self.fence == fence
    }

    pub(crate) fn require_mail_scope(
        &self,
        scope: &RuntimeScope,
        work: &mut Option<&mut dyn crate::store::HistoryIo>,
    ) -> Result<()> {
        crate::core::inspect_metadata(&(scope.grants(), &self.admission.grants), work)?;
        if scope.grants() != &self.admission.grants || scope.limits() != self.admission.limits {
            return Err(crate::store::guarded_boundary(work, crate::wire::ErrorCode::Unauthorized,
                "mailbox scope differs from retained admission"));
        }
        Ok(())
    }

    pub(crate) fn require_task_context_binding(
        &self,
        context: &crate::runtime::TaskContext,
        io: Option<&crate::executor::PreparationIo>,
    ) -> Result<()> {
        #[cfg(not(target_arch = "wasm32"))]
        if let Some(work) = io.and_then(crate::executor::PreparationIo::original_native_work) {
            context.require_original_native_work(work)?;
            if !self.native_custody.as_ref().is_some_and(|custody| custody.work().shares_allocation_context(work)) {
                return Err(crate::conversation::content_error(io, crate::wire::ErrorCode::Unauthorized,
                    "read owner differs from its original admission ledger"));
            }
        }
        crate::executor::with_history_io(io, |work| {
            crate::core::inspect_metadata(&(context.scope().grants(), &self.admission.grants,
                context.scope().resource_proofs(), &self.admission.resource_proofs), work)
        })?;
        if !context.original_task_binding_matches(self.task_id(), &self.fence)
            || context.id() != self.operation_id()
            || !context.original_host_binding_matches(self.host.as_ref(), self.host.policy.as_deref())
            || context.scope().limits() != self.admission.limits
            || context.scope().run_limits() != self.admission.run_limits
            || context.scope().grants() != &self.admission.grants
            || context.scope().resource_proofs() != self.admission.resource_proofs
            || context.scope().extensions() != self.admission.extensions.as_ref()
        {
            return Err(crate::conversation::content_error(io, crate::wire::ErrorCode::Unauthorized,
                "read context differs from its original owner admission"));
        }
        Ok(())
    }

    pub(crate) fn input_limits(&self) -> crate::conversation::Limits {
        self.admission.limits
    }

    pub(crate) fn validate_input_file(
        &self,
        file: &FileRef,
        io: Option<&crate::executor::PreparationIo>,
    ) -> Result<()> {
        crate::executor::with_history_io(io, |work| {
            self.admission.limits.validate_file(file, work)
        })?;
        if !read_granted(&self.admission.grants, file)? {
            return Err(Error::Unauthorized(
                "task cannot read the execution input file".into(),
            ));
        }
        Ok(())
    }

    pub(crate) fn validate_native_configuration(
        &self,
        binding: &crate::model::NativeConfigurationBinding,
        io: Option<&crate::executor::PreparationIo>,
    ) -> Result<(&crate::core::SchemaRegistry, [u8; 32])> {
        let (schemas, expected) = binding.verify_original(
            self.admission.extensions.as_ref(),
            self.host.root_scope.extension_schema_registry(),
            self.host.root_scope.extension_runtime().as_deref(),
            io,
        )?;
        let file = &binding.configuration.content;
        self.validate_input_file(file, io)?;
        if file.descriptor().byte_length() > self.admission.limits.render_bytes {
            return Err(Error::Invalid(
                "native options exceed render byte limit".into(),
            ));
        }
        Ok((schemas, expected))
    }

    /// Verifies options against the original authenticated admission and linked code.
    /// This is read authority, so it remains valid during original-fence settlement.
    pub(crate) async fn verify_native_configuration(
        &self,
        binding: &crate::model::NativeConfigurationBinding,
        io: Option<&crate::executor::PreparationIo>,
    ) -> Result<()> {
        let (schemas, expected) = self.validate_native_configuration(binding, io)?;
        let bytes = self
            .host
            .reader
            .read(
                &binding.configuration.content,
                self.admission.limits.render_bytes,
                io,
            )?
            .await?;
        schemas.verify_configuration_binding(
            &binding.configuration,
            expected,
            &bytes,
            self.admission.limits,
            io,
        )
    }

    /// Requires the volume grant retained by this task admission.
    pub fn require_volume_grant(
        &self,
        volume: &crate::conversation::VolumeRef,
        operation: crate::conversation::VolumeOperation,
        work: &mut Option<&mut dyn crate::store::HistoryIo>,
    ) -> Result<()> {
        volume.validate(work)?;
        crate::core::inspect_metadata(&self.admission.grants, work)?;
        let grant = volume.capability(operation, work)?;
        if !self.admission.grants.contains(&grant) {
            return Err(Error::Unauthorized(format!("task scope lacks {grant}")));
        }
        Ok(())
    }

    pub(crate) fn require_interaction_grant(
        &self,
        io: Option<&crate::executor::PreparationIo>,
    ) -> Result<()> {
        crate::executor::with_history_io(io, |work| {
            crate::core::inspect_metadata(&self.admission.grants, work)
        })?;
        if !self
            .admission
            .grants
            .contains(capability::INTERACTION_ROUTE)
        {
            return Err(Error::Unauthorized(
                "task scope lacks interaction:route".into(),
            ));
        }
        Ok(())
    }

    pub(crate) fn task_binding(&self) -> (TaskId, &crate::scheduler::LeaseFence) {
        (self.task_id(), &self.fence)
    }

    /// Consumes only the original journal's sealed current Pending witness and settled execution tail.
    #[cfg(not(target_arch = "wasm32"))]
    pub(crate) async fn release_quiescent_execution(
        &self,
        execution: OperationId,
        tail: u64,
        pending: &crate::filesystem::WorkflowReleaseWitness<'_>,
        io: &crate::filesystem::PreparationIo,
    ) -> Result<bool> {
        let mut io = io.history_io();
        if pending.condition().0 != (self.task_id(), &self.fence) {
            return Err(Error::Unauthorized(
                "pending workflow belongs to another task or fence".into(),
            ));
        }
        let mut coordinator = self.host.coordinator.lock().await;
        coordinator.refresh(&mut Some(&mut io)).await?;
        let operation = coordinator.observe_operation(
            self.host.verifier.audience(),
            &self.host.owner_scope,
            &self.host.verifier,
            self.operation_id(),
            &mut Some(&mut io),
        )?;
        crate::scheduler::require_execution_owner(operation, &self.fence, true, &mut Some(&mut io))?;
        if operation.cancellation_requested {
            return Err(Error::Conflict(
                "read-only close cannot settle task cancellation".into(),
            ));
        }
        if operation.phase == crate::scheduler::OperationPhase::Reconciling {
            return Ok(false);
        }
        let revision = operation.revision;
        let key = worker_key(
            "release",
            self.operation_id(),
            &self.fence,
            revision,
            &mut Some(&mut io),
        )?;
        let fence = self.fence.clone_with_work(&mut Some(&mut io))?;
        coordinator
            .apply_if_execution_quiescent(
                key,
                crate::scheduler::SchedulerEvent::LeaseReleased {
                    operation_id: self.operation_id(),
                    fence,
                },
                execution,
                tail,
                Some(pending.condition().1),
                &mut Some(&mut io),
            )
            .await?;
        Ok(true)
    }

    pub(crate) async fn suspend_quiescent_execution(
        &self,
        execution: OperationId,
        tail: u64,
        revision: u64,
        command: OperationId,
        work: &mut Option<&mut dyn crate::store::HistoryIo>,
    ) -> Result<()> {
        self.host
            .suspend_workflow_command_guarded(
                self.task_id(),
                self.fence.clone(),
                revision,
                Some(command),
                Some((execution, tail)),
                work,
            )
            .await
    }

    pub(crate) fn validate_storage(
        &self,
        verifier: &AuthorityVerifier,
        maximum_payload_bytes: u64,
        work: &mut Option<&mut dyn crate::store::HistoryIo>,
    ) -> Result<()> {
        if let Some(io) = work.as_deref() {
            io.check_cancellation()?;
        }
        if verifier.audience() != &self.host.owner
            || maximum_payload_bytes > self.admission.limits.file_bytes
        {
            return Err(Error::Unauthorized(
                "journal storage exceeds admitted task authority or limits".into(),
            ));
        }
        Ok(())
    }

    /// Returns the initial workflow identity derived from the retained task
    /// admission and its registered machine, rather than caller-selected state.
    #[must_use]
    pub const fn workflow_admission(&self) -> &crate::workflow::WorkflowAdmission {
        &self.workflow
    }

    pub(crate) fn validate_workflow_record(
        &self,
        record: &crate::workflow::WorkflowRecord,
        io: Option<&crate::executor::PreparationIo>,
    ) -> Result<()> {
        if record.prior.machine != self.workflow.initial.machine {
            return Err(Error::Conflict(
                "workflow implementation differs from task admission".into(),
            ));
        }
        let (next, transition, _, _) = self.host.machines.step(
            &record.prior,
            std::borrow::Cow::Borrowed(&record.input),
            io,
        )?;
        if record.checkpoint()? != (&next.machine, next.revision, &next.state) || transition != record.transition {
            return Err(Error::Conflict(
                "workflow transition differs from pinned machine".into(),
            ));
        }
        if let crate::workflow::MachineStatus::Completed { value } = &transition.status {
            crate::executor::with_history_io(io, |work| {
                crate::executor::validate_json_schema_value(
                    self.workflow_output.as_ref().unwrap_or(&self.admission.output_schema),
                    value,
                    "task output",
                    work,
                )
            })?;
        }
        Ok(())
    }

    pub(crate) fn stream(&self) -> Arc<P> {
        self.host.stream.clone()
    }

    pub(crate) fn operation_id(&self) -> OperationId {
        OperationId::from_bytes(self.task_id().into_bytes())
    }

    pub(crate) async fn condition(
        &self,
        write: crate::distributed::JournalWrite,
        work: &mut Option<&mut dyn crate::store::HistoryIo>,
    ) -> Result<acyclic_stream::CommitCondition> {
        self.host
            .coordinator
            .lock()
            .await
            .journal_condition(
                &self.host.owner,
                &self.host.owner_scope,
                &self.host.verifier,
                self.operation_id(),
                &self.fence,
                write,
                work,
            )
            .await
    }

    pub(crate) async fn verify_read_access(
        &self,
        work: &mut Option<&mut dyn crate::store::HistoryIo>,
    ) -> Result<()> {
        let mut coordinator = self.host.coordinator.lock().await;
        coordinator.refresh(work).await?;
        let operation = coordinator.observe_operation(&self.host.owner, &self.host.owner_scope,
            &self.host.verifier, self.operation_id(), work)?;
        if operation.spec.entrypoint.name != self.admission.task.name
            || operation.spec.entrypoint.version != self.admission.task.version
            || operation.spec.entrypoint.digest != self.admission.task.digest
            || operation.spec.entrypoint.result_schema != self.admission.output_schema
        {
            return Err(crate::store::guarded_boundary(work, crate::wire::ErrorCode::Unauthorized,
                "read owner differs from the retained admission"));
        }
        Ok(())
    }

    /// Verifies the current task fence for dispatch or settlement.
    pub async fn verify(
        &self,
        settlement: bool,
        work: &mut Option<&mut dyn crate::store::HistoryIo>,
    ) -> Result<()> {
        use crate::distributed::JournalWrite;
        self.condition(
            if settlement {
                JournalWrite::Settlement
            } else {
                JournalWrite::Fresh
            },
            work,
        )
        .await
        .map(|_| ())
    }

    pub(super) fn execution_write(
        operation: OperationId,
        event: &crate::executor::ExecutionEvent,
        work: &mut Option<&mut dyn crate::store::HistoryIo>,
    ) -> Result<crate::distributed::JournalWrite> {
        use crate::{distributed::JournalWrite, executor::ExecutionEvent};
        #[cfg(not(target_arch = "wasm32"))]
        let io = work.as_deref().and_then(|work| work.original_native_work())
            .map(crate::filesystem::PreparationIo::from_native_work).transpose()?;
        #[cfg(target_arch = "wasm32")]
        let io = None;
        Ok(match event {
            ExecutionEvent::ModelStarted {
                step,
                purpose,
                request_digest,
                ..
            } => JournalWrite::Model {
                attempt_id: purpose.attempt_operation(operation, io.as_ref())?,
                step: *step,
                request_digest: *request_digest,
            },
            ExecutionEvent::Started { .. }
            | ExecutionEvent::ContextPrepared { .. }
            | ExecutionEvent::ContextCompacted { .. }
            | ExecutionEvent::ToolStarted { .. } => JournalWrite::Fresh,
            ExecutionEvent::Model { .. }
            | ExecutionEvent::ToolCompleted { .. }
            | ExecutionEvent::ToolFailed { .. } => JournalWrite::Settlement,
        })
    }

    pub(crate) async fn verify_execution(
        &self,
        operation: OperationId,
        event: &crate::executor::ExecutionEvent,
        work: &mut Option<&mut dyn crate::store::HistoryIo>,
    ) -> Result<()> {
        self.condition(Self::execution_write(operation, event, work)?, work)
            .await
            .map(|_| ())
    }

    pub(crate) async fn verify_model_history<'a>(
        &self,
        operation: OperationId,
        records: impl Iterator<Item = &'a crate::executor::ExecutionRecord> + Clone,
        work: &mut Option<&mut dyn crate::store::HistoryIo>,
    ) -> Result<()> {
        self.host
            .verify_model_history(self.task_id(), operation, records, work)
            .await
    }

    pub(crate) fn require_effect_grants(
        &self,
        provider: &str,
        planning: bool,
        work: &mut Option<&mut dyn crate::store::HistoryIo>,
    ) -> Result<()> {
        crate::core::inspect_metadata(&self.admission.grants, work)?;
        for grant in [
            capability::EFFECT_RUN.to_owned(),
            capability::effect_provider(provider),
        ] {
            if !self.admission.grants.contains(&grant) {
                return Err(Error::Unauthorized(format!("task scope lacks {grant}")));
            }
        }
        if planning && !self.admission.grants.contains(capability::EFFECT_PLAN) {
            return Err(Error::Unauthorized("task scope lacks effect:plan".into()));
        }
        Ok(())
    }

    pub(crate) async fn append_conversation(
        &self,
        publication: &crate::store::operations::IndexedPublication,
        key: &StreamKey,
        write: crate::distributed::JournalWrite,
        work: &mut Option<&mut dyn crate::store::HistoryIo>,
        phase: Option<&mut crate::distributed::CommitPhase>,
    ) -> Result<bool> {
        let mut request = self
            .prepare_append(
                &publication.path,
                publication.expected_tail,
                key,
                &publication.bytes,
                write,
                work,
            )
            .await?;
        publication.add_index(&mut request)?;
        match crate::distributed::commit(
            request,
            &self.host.stream,
            Some(publication.operation_id),
            phase,
        )
        .await?
        .0
        {
            acyclic_stream::CommitOutcome::Conflict(_) => Ok(false),
            acyclic_stream::CommitOutcome::Committed(envelope) => {
                publication
                    .verify(&envelope, work)
                    .map_err(|_| Error::Indeterminate(publication.operation_id))?;
                Ok(true)
            }
        }
    }

    pub(crate) async fn append_execution(
        &self,
        operation: OperationId,
        expected_tail: u64,
        key: &StreamKey,
        bytes: OwnedBytes,
        event: &crate::executor::ExecutionEvent,
        work: &mut Option<&mut dyn crate::store::HistoryIo>,
    ) -> Result<bool> {
        let path = crate::distributed::execution_path(operation, work)?;
        self.append(
            path,
            expected_tail,
            key,
            bytes,
            Self::execution_write(operation, event, work)?,
            work,
        )
        .await
    }

    pub(crate) async fn append_workflow(
        &self,
        admission: bool,
        expected_tail: u64,
        key: &StreamKey,
        bytes: OwnedBytes,
        work: &mut Option<&mut dyn crate::store::HistoryIo>,
    ) -> Result<bool> {
        let class = if admission {
            "workflow-admissions"
        } else {
            "workflows"
        };
        let path =
            acyclic_stream::StreamPath::new(format!("harness/v2/{class}/{}", self.operation_id()))?;
        self.append(
            path,
            expected_tail,
            key,
            bytes,
            crate::distributed::JournalWrite::Fresh,
            work,
        )
        .await
    }

    pub(super) async fn prepare_append<'a, 'b>(
        &self,
        path: &acyclic_stream::StreamPath,
        expected_tail: u64,
        key: &StreamKey,
        bytes: &OwnedBytes,
        write: crate::distributed::JournalWrite,
        work: &'a mut Option<&'b mut dyn crate::store::history::HistoryIo>,
    ) -> Result<(
        acyclic_stream::CommitRequest,
        crate::wire_codec::CodecQuota<'a, 'b>,
    )> {
        use acyclic_stream::CommitCondition;
        if bytes.len() > acyclic_stream::MAX_RECORD_BYTES {
            return Err(crate::store::guarded_boundary(
                work,
                crate::wire::ErrorCode::Invalid,
                "owned journal record exceeds the canonical byte bound",
            ));
        }
        let owner = self.condition(write, work).await?;
        let target = match crate::store::operations::bounds_with_work(&self.host.stream, path, work).await? {
            Ok(_) => CommitCondition::Tail { path: path.clone(), expected: expected_tail },
            Err(error) if *crate::distributed::sdk_source(&error) == StreamError::NotFound && expected_tail == 0 => CommitCondition::Absent { path: path.clone() },
            Err(error) => return Err(crate::distributed::sdk_error(error, work)),
        };
        let mut hash = blake3::Hasher::new();
        for part in [
            b"harness/v2/owned-journal\0".as_slice(),
            key.as_bytes(),
            &self.task_id().into_bytes(),
        ] {
            if let Some(io) = work.as_deref_mut() {
                io.hashed_bytes(part.len() as u64)?;
            }
            hash.update(part);
        }
        if let Some(io) = work.as_deref_mut() {
            io.admit_encoding(1)?;
        }
        let encoded_fence = crate::store::history::canonical_bytes_for_bounds_with_work(
            &self.fence,
            crate::contract::JsonBounds {
                maximum_bytes: acyclic_stream::MAX_RECORD_BYTES as u64,
                maximum_items: acyclic_stream::MAX_RECORD_BYTES as u64,
                maximum_depth: 256,
            },
            work,
        )?;
        let hash_admission = match work.as_deref_mut() {
            Some(io) => io.hashed_bytes(encoded_fence.len() as u64),
            None => Ok(()),
        };
        if hash_admission.is_ok() {
            hash.update(&encoded_fence);
        }
        let retained_fence = encoded_fence.capacity() as u64;
        drop(encoded_fence);
        let release = match work.as_deref_mut() {
            Some(io) => io.release_owned(retained_fence),
            None => Ok(()),
        };
        hash_admission?;
        release?;
        let CommitCondition::Tail { expected, .. } = &owner else {
            return Err(Error::Invalid("coordinator tail condition required".into()));
        };
        for part in [
            expected.to_le_bytes().as_slice(),
            path.as_str().as_bytes(),
            &expected_tail.to_le_bytes(),
            &[u8::from(matches!(target, CommitCondition::Absent { .. }))],
        ] {
            if let Some(io) = work.as_deref_mut() {
                io.hashed_bytes(part.len() as u64)?;
            }
            hash.update(part);
        }
        if let Some(io) = work.as_deref_mut() {
            io.hashed_bytes(bytes.len() as u64)?;
        }
        let payload_digest = blake3::hash(bytes);
        if let Some(io) = work.as_deref_mut() {
            io.hashed_bytes(32)?;
        }
        hash.update(payload_digest.as_bytes());
        let (key, held) = crate::distributed::digest_key(hash.finalize().as_bytes(), work)?;
        let mut request = crate::distributed::commit_request(key, held, work);
        crate::distributed::add_condition(&mut request, owner)?;
        crate::distributed::add_condition(&mut request, target)?;
        crate::distributed::add_record(&mut request, path.clone(), bytes.clone())?;
        Ok(request)
    }

    pub(crate) async fn append(
        &self,
        path: acyclic_stream::StreamPath,
        expected_tail: u64,
        key: &StreamKey,
        bytes: OwnedBytes,
        write: crate::distributed::JournalWrite,
        work: &mut Option<&mut dyn crate::store::HistoryIo>,
    ) -> Result<bool> {
        use acyclic_stream::{CommitOutcome, CommittedMutation};
        let request = self
            .prepare_append(&path, expected_tail, key, &bytes, write, work)
            .await?;
        let outcome =
            crate::distributed::commit(request, &self.host.stream, Some(self.operation_id()), None)
                .await?
                .0;
        match outcome {
            CommitOutcome::Conflict(_) => Ok(false),
            CommitOutcome::Committed(envelope) => {
                let [CommittedMutation::Append(append)] = envelope.mutations.as_slice() else {
                    return Err(Error::Storage("invalid owned journal envelope".into()));
                };
                if append.path != path
                    || append.start != expected_tail
                    || append.end != expected_tail + 1
                    || append.tail != append.end
                    || append.records.len() != 1
                    || append.records.first().is_none_or(|record| {
                        record.sequence != expected_tail
                            || record.value != bytes
                            || record.commit_id != envelope.commit_id
                    })
                {
                    return Err(Error::Storage("invalid owned journal append".into()));
                }
                Ok(true)
            }
        }
    }
}
