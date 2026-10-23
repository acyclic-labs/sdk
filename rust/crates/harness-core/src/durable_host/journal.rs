//! Original owner checks and fenced journal publication.
#[cfg(feature = "filesystem")]
use super::*;

#[cfg(feature = "filesystem")]
impl<P: StreamProvider> TaskJournalOwner<P> {
    /// Returns the authenticated task identity.
    pub fn task_id(&self) -> TaskId {
        TaskId::from_bytes(self.admission.operation_id.into_bytes())
    }

    #[cfg(not(target_arch = "wasm32"))]
    pub(crate) fn matches_task_binding(
        &self,
        task: TaskId,
        fence: &crate::scheduler::LeaseFence,
    ) -> bool {
        self.task_id() == task && &self.fence == fence
    }

    #[doc(hidden)]
    pub fn admission(&self) -> &TaskAdmissionRecord {
        &self.admission
    }

    #[cfg(all(
        feature = "filesystem-local",
        feature = "native-execution",
        not(target_arch = "wasm32")
    ))]
    #[doc(hidden)]
    pub fn deadline_epoch_ms(&self) -> Option<u64> {
        self.admission.run_limits.deadline_epoch_ms
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
            self.admission.limits.validate_file(file, work)?;
            if !crate::runtime::read_granted_with_work(&self.admission.grants, file, work)? {
                return Err(crate::conversation::content_error(
                    io,
                    crate::wire::ErrorCode::Unauthorized,
                    "task cannot read the execution input file",
                ));
            }
            Ok(())
        })
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
                None,
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
        if self.read_proof.is_some() && operation == crate::conversation::VolumeOperation::Write {
            return Err(Error::Unauthorized(
                "historical admission has no volume writer".into(),
            ));
        }
        volume.validate(work)?;
        let admitted = match operation {
            crate::conversation::VolumeOperation::Read => {
                crate::core::directory_read_granted_with_work(
                    &self.admission.grants,
                    volume,
                    "",
                    work,
                )?
            }
            crate::conversation::VolumeOperation::Write => {
                crate::core::volume_write_granted_with_work(&self.admission.grants, volume, work)?
            }
        };
        if !admitted {
            return Err(crate::fork::boundary_error(
                work,
                crate::wire::ErrorCode::Unauthorized,
                "task scope lacks volume authority",
            ));
        }
        Ok(())
    }

    pub(crate) fn require_interaction_grant(
        &self,
        work: &mut Option<&mut dyn crate::store::HistoryIo>,
    ) -> Result<()> {
        if let Some(io) = work.as_deref_mut() {
            io.check_cancellation()?;
        }
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

    #[doc(hidden)]
    pub fn task_binding(&self) -> (TaskId, &crate::scheduler::LeaseFence) {
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
                (revision, Some(command)),
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

    #[cfg(not(target_arch = "wasm32"))]
    pub(crate) async fn scope(
        &self,
        work: &acyclic_fs::NativeMaterializationWork,
    ) -> Result<(RuntimeScope, acyclic_fs::NativeAllocation)> {
        if !self.native_shared_work_matches(work) {
            return Err(Error::NativeBoundary {
                code: crate::wire::ErrorCode::Unauthorized,
                reason: "task owner uses another scope ledger",
            });
        }
        let io = crate::executor::PreparationIo::from_native_work(work)?;
        let (scope, retained) = self
            .host
            .authenticated_scope(&self.admission, &mut Some(&mut io.history_io()))
            .await?;
        Ok((scope, io.into_native_custody(retained)?))
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
        #[cfg(not(target_arch = "wasm32"))]
        let recheck = io
            .and_then(crate::executor::PreparationIo::original_native_work)
            .map(|work| crate::executor::PreparationIo::from_native_work(&work.recheck_admitted()))
            .transpose()?;
        #[cfg(not(target_arch = "wasm32"))]
        let io = recheck.as_ref().or(io);
        if record.prior.machine != self.workflow.initial.machine {
            return Err(Error::Conflict(
                "workflow implementation differs from task admission".into(),
            ));
        }
        let input = std::borrow::Cow::Borrowed(&record.input);
        self.host
            .machines
            .validate_transition(&record.prior, input, &record.transition, io)?;
        if let crate::workflow::MachineStatus::Completed { value } = &record.transition.status {
            crate::executor::with_history_io(io, |work| {
                crate::executor::validate_json_schema_value(
                    self.tool_output_schema
                        .as_ref()
                        .unwrap_or(&self.admission.output_schema),
                    value,
                    "task output",
                    work,
                )
            })?;
        }
        Ok(())
    }

    #[doc(hidden)]
    pub fn stream(&self) -> Arc<P> {
        self.host.stream.clone()
    }

    pub(crate) fn operation_id(&self) -> OperationId {
        OperationId::from_bytes(self.task_id().into_bytes())
    }

    #[doc(hidden)]
    pub async fn condition(
        &self,
        write: crate::distributed::JournalWrite,
        work: &mut Option<&mut dyn crate::store::HistoryIo>,
    ) -> Result<acyclic_stream::CommitCondition> {
        #[cfg(not(target_arch = "wasm32"))]
        let mut recheck = work
            .as_deref()
            .and_then(|io| io.original_native_work())
            .map(|native| {
                crate::executor::PreparationIo::from_native_work(&native.recheck_admitted())
                    .map(crate::store::HistoryWork::new)
            })
            .transpose()?;
        #[cfg(not(target_arch = "wasm32"))]
        let mut check_work = match recheck.as_mut() {
            Some(io) => Some(io as &mut dyn crate::store::HistoryIo),
            None => work
                .as_mut()
                .map(|io| &mut **io as &mut dyn crate::store::HistoryIo),
        };
        #[cfg(target_arch = "wasm32")]
        let mut check_work = work
            .as_mut()
            .map(|io| &mut **io as &mut dyn crate::store::HistoryIo);
        let result = async {
            let work = &mut check_work;
            #[cfg(not(target_arch = "wasm32"))]
            if work
                .as_deref()
                .and_then(|io| io.original_native_work())
                .is_some_and(|native| !self.native_shared_work_matches(native))
            {
                return Err(Error::Unauthorized(
                    "journal owner uses another metadata ledger".into(),
                ));
            }
            if self.read_proof.is_some() {
                return Err(Error::Unauthorized(
                    "historical admission has no publication authority".into(),
                ));
            }
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
        .await;
        drop(check_work);
        #[cfg(not(target_arch = "wasm32"))]
        if result.is_ok()
            && let Some(recheck) = recheck
        {
            work.as_deref_mut()
                .ok_or_else(|| Error::Invalid("journal observer is missing".into()))?
                .absorb_native_custody(recheck.into_custody()?)?;
        }
        result
    }

    /// Verifies the current task fence for dispatch or settlement.
    pub async fn verify<'a, 'w: 'a>(
        &'a self,
        settlement: bool,
        work: &'a mut Option<&'w mut dyn crate::store::HistoryIo>,
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
            | ExecutionEvent::ModelStopped { .. }
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
        if let Some(io) = work.as_deref_mut() {
            io.check_cancellation()?;
        }
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
                .map_err(|_native_error| {
                    #[cfg(not(target_arch = "wasm32"))]
                    if _native_error.is_native_failure() {
                        return _native_error;
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
            Some(key),
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
            format_args!("harness/v2/{class}/{}", self.workflow.operation_id),
            quota.work(),
        )?)?;
        if quota.work().is_some() {
            quota.adopt(path.as_str().len() as u64)?;
        }
        self.append(
            path,
            expected_tail,
            Some(key),
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
        key: Option<&StreamKey>,
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
        let payload = blake3::hash(&bytes);
        // Native journals use the payload digest itself as the caller key.
        // Reuse this hash rather than hashing the same immutable buffer twice.
        let key = key.map_or(payload.as_bytes().as_slice(), StreamKey::as_bytes);
        let digest = crate::executor::json::canonical_json_digest(
            &(
                "harness/v2/owned-journal",
                key,
                self.task_id(),
                &self.fence,
                expected,
                path.as_str(),
                expected_tail,
                matches!(target, CommitCondition::Absent { .. }),
                payload.as_bytes(),
            ),
            acyclic_stream::MAX_RECORD_BYTES as u64,
            work,
        )?;
        let (key, held) = crate::distributed::digest_key(&digest, work)?;
        let request = crate::distributed::journal_request::<true>(
            key,
            held,
            owner,
            target,
            path.clone(),
            bytes.clone(),
            work,
        )?;
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

#[cfg(feature = "filesystem")]
impl<P: StreamProvider> TaskJournalOwner<P> {
    pub(crate) async fn for_tool_workflow(
        mut self,
        context: &crate::runtime::ToolContext,
        tool: &crate::durable_tool::ResumableTool,
        invocation: &crate::tool::ToolInvocation,
        cancellation: &acyclic_fs::CancellationToken,
        io: Option<&crate::executor::PreparationIo>,
    ) -> Result<Self> {
        self.require_task_context_binding(context.task(), io)?;
        if invocation.operation_id == self.operation_id() {
            return Err(Error::Unauthorized(
                "tool workflow reused its task identity".into(),
            ));
        }
        self.invocation_cancellation = Some(cancellation.clone());
        let mut history = io.map(|io| io.history_io());
        let mut work = history
            .as_mut()
            .map(|io| io as &mut dyn crate::store::HistoryIo);
        self.verify(false, &mut work).await?;
        #[cfg(not(target_arch = "wasm32"))]
        let before = io.map(|io| io.owned_bytes()).transpose()?.unwrap_or(0);
        let workflow = tool.workflow_admission(context, invocation, io)?;
        let output_schema = crate::executor::with_history_io(io, |work| {
            crate::wire_codec::CodecQuota::produce(work, |quota| {
                crate::core::copy(&tool.definition.output_schema, quota, |value, _| {
                    crate::executor::json::json_value_logical_owned_bytes(value)
                })
            })
            .map(|(schema, _)| schema)
        })?;
        self.host
            .machines
            .validate_checkpoint(&workflow.initial, io)?;
        #[cfg(not(target_arch = "wasm32"))]
        if let Some(io) = io {
            self.native_custody
                .as_mut()
                .ok_or_else(|| {
                    Error::Unauthorized("tool workflow lacks original owner custody".into())
                })?
                .absorb(&mut io.take_native_custody(
                    io.owned_bytes()?.checked_sub(before).ok_or_else(|| {
                        Error::Invalid("tool workflow ownership underflow".into())
                    })?,
                )?)?;
        }
        self.workflow = workflow;
        self.tool_output_schema = Some(output_schema);
        Ok(self)
    }
}
