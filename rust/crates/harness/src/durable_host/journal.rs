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

    /// Verifies options against the original admission and linked implementation.
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
        work: &mut Option<&mut dyn crate::store::HistoryIo>,
    ) -> Result<()> {
        crate::core::inspect_metadata(&self.admission.grants, work)?;
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

    pub(crate) async fn suspend_quiescent_execution(
        &self,
        execution: OperationId,
        tail: u64,
        revision: u64,
        command: OperationId,
        work: &mut Option<&mut dyn crate::store::HistoryIo>,
    ) -> Result<()> {
        self.host
            .suspend_workflow(
                self.task_id(),
                &self.fence,
                revision,
                Some(command),
                Some((execution, tail)),
                None,
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

    /// Returns the workflow pinned by the retained admission.
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
        if record.checkpoint()? != (&next.machine, next.revision, &next.state)
            || transition != record.transition
        {
            return Err(Error::Conflict(
                "workflow transition differs from pinned machine".into(),
            ));
        }
        if let crate::workflow::MachineStatus::Completed { value } = &transition.status {
            crate::executor::with_history_io(io, |work| {
                crate::executor::validate_json_schema_value(
                    &self.admission.output_schema,
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

    pub(super) async fn condition(
        &self,
        write: crate::distributed::JournalWrite,
        work: &mut Option<&mut dyn crate::store::HistoryIo>,
    ) -> Result<acyclic_stream::CommitCondition> {
        if !matches!(write, crate::distributed::JournalWrite::Settlement)
            && self
                .invocation_cancellation
                .as_ref()
                .is_some_and(|token| token.is_cancelled())
        {
            return Err(Error::Indeterminate(self.operation_id()));
        }
        if !matches!(write, crate::distributed::JournalWrite::Settlement)
            && self
                .admission
                .run_limits
                .deadline_epoch_ms
                .is_some_and(|deadline| self.host.clock.now_unix_millis() >= deadline)
        {
            return Err(Error::Indeterminate(self.operation_id()));
        }
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
    ) -> Result<crate::distributed::JournalWrite> {
        use crate::{distributed::JournalWrite, executor::ExecutionEvent};
        Ok(match event {
            ExecutionEvent::ModelStarted {
                step,
                purpose,
                request_digest,
                ..
            } => JournalWrite::Model {
                attempt_id: purpose.attempt_operation(operation, None)?,
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
        self.condition(Self::execution_write(operation, event)?, work)
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
        let owner = self.condition(write, work).await?;
        let acyclic_stream::CommitCondition::Tail { expected, .. } = &owner else {
            return Err(crate::scheduler::diagnostic(
                work,
                crate::wire::ErrorCode::Invalid,
                "coordinator tail condition required",
            ));
        };
        if let Some(io) = work.as_deref_mut() {
            io.hashed_bytes(publication.bytes.len() as u64)?;
        }
        let payload = blake3::hash(&publication.bytes);
        let digest = crate::executor::json::canonical_json_digest(
            &(
                "harness/v2/owned-publication",
                key.as_bytes(),
                self.task_id(),
                &self.fence,
                expected,
                publication.path.as_str(),
                publication.expected_tail,
                payload.as_bytes(),
            ),
            acyclic_stream::MAX_RECORD_BYTES as u64,
            work,
        )?;
        let (key, _) = crate::distributed::digest_key(&digest, work)?;
        let mut request = publication.request(&self.host.stream, key, work).await?;
        crate::distributed::add_condition(&mut request, owner)?;
        if !matches!(write, crate::distributed::JournalWrite::Settlement)
            && self
                .invocation_cancellation
                .as_ref()
                .is_some_and(|token| token.is_cancelled())
        {
            return Err(Error::Indeterminate(self.operation_id()));
        }
        if !matches!(write, crate::distributed::JournalWrite::Settlement)
            && self
                .admission
                .run_limits
                .deadline_epoch_ms
                .is_some_and(|deadline| self.host.clock.now_unix_millis() >= deadline)
        {
            return Err(Error::Indeterminate(self.operation_id()));
        }
        let (outcome, retained) = crate::distributed::commit(
            request,
            &self.host.stream,
            Some(publication.operation_id),
            phase,
        )
        .await?;
        let mut received = crate::wire_codec::CodecQuota::from_retained(work, retained);
        let result = match &outcome {
            acyclic_stream::CommitOutcome::Conflict(_) => Ok(false),
            acyclic_stream::CommitOutcome::Committed(envelope) => publication
                .verify(envelope, received.work())
                .map(|()| true)
                .map_err(|error| {
                    #[cfg(not(target_arch = "wasm32"))]
                    if error.is_native_failure() {
                        return error;
                    }
                    Error::Indeterminate(publication.operation_id)
                }),
        };
        drop(outcome);
        drop(received);
        result
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
        let mut quota = crate::wire_codec::CodecQuota::new(work, 0)?;
        let path = crate::distributed::execution_path(operation, quota.work())?;
        if quota.work().is_some() {
            quota.adopt(path.as_str().len() as u64)?;
        }
        self.append(
            path,
            expected_tail,
            key,
            bytes,
            Self::execution_write(operation, event)?,
            quota.work(),
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
        let mut quota = crate::wire_codec::CodecQuota::new(work, 0)?;
        let path = acyclic_stream::StreamPath::new(crate::executor::preparation_format(
            format_args!("harness/v2/{class}/{}", self.operation_id()),
            quota.work(),
        )?)?;
        if quota.work().is_some() {
            quota.adopt(path.as_str().len() as u64)?;
        }
        self.append(
            path,
            expected_tail,
            key,
            bytes,
            crate::distributed::JournalWrite::Fresh,
            quota.work(),
        )
        .await
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
        use acyclic_stream::{CommitCondition, CommitOutcome, CommittedMutation};
        let owner = self.condition(write, work).await?;
        let target =
            match crate::store::operations::bounds_with_work(&self.host.stream, &path, work).await?
            {
                Ok(_) => CommitCondition::Tail {
                    path: path.clone(),
                    expected: expected_tail,
                },
                Err(error) if error == StreamError::NotFound && expected_tail == 0 => {
                    CommitCondition::Absent { path: path.clone() }
                }
                Err(error) => {
                    return Err(crate::store::operations::stream_error_with_work(
                        error, work,
                    ));
                }
            };
        let CommitCondition::Tail { expected, .. } = &owner else {
            return Err(Error::Invalid("coordinator tail condition required".into()));
        };
        if let Some(io) = work.as_deref_mut() {
            io.hashed_bytes(bytes.len() as u64)?;
        }
        let digest = crate::executor::json::canonical_json_digest(
            &(
                "harness/v2/owned-journal",
                key.as_bytes(),
                self.task_id(),
                &self.fence,
                expected,
                path.as_str(),
                expected_tail,
                matches!(target, CommitCondition::Absent { .. }),
                blake3::hash(&bytes).as_bytes(),
            ),
            acyclic_stream::MAX_RECORD_BYTES as u64,
            work,
        )?;
        let (key, held) = crate::distributed::digest_key(&digest, work)?;
        let mut request = crate::distributed::commit_request(key, held, work);
        crate::distributed::add_condition(&mut request, owner)?;
        crate::distributed::add_condition(&mut request, target)?;
        crate::distributed::add_record(&mut request, path.clone(), bytes.clone())?;
        let (outcome, retained) =
            crate::distributed::commit(request, &self.host.stream, Some(self.operation_id()), None)
                .await?;
        let received = crate::wire_codec::CodecQuota::from_retained(work, retained);
        let result = match &outcome {
            CommitOutcome::Conflict(_) => Ok(false),
            CommitOutcome::Committed(envelope) => {
                let valid = match envelope.mutations.as_slice() {
                    [CommittedMutation::Append(append)] => {
                        append.path == path
                            && append.start == expected_tail
                            && expected_tail.checked_add(1) == Some(append.end)
                            && append.tail == append.end
                            && matches!(append.records.as_slice(), [record] if record.sequence == expected_tail && record.value == bytes && record.commit_id == envelope.commit_id)
                    }
                    _ => false,
                };
                if valid {
                    Ok(true)
                } else {
                    Err(Error::Indeterminate(self.operation_id()))
                }
            }
        };
        drop(outcome);
        drop(received);
        result
    }
}
