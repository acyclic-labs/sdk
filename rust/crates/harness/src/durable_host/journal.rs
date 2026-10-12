//! Journal.
use super::*;

#[cfg(feature = "filesystem")]
impl<P: StreamProvider> TaskJournalOwner<P> {
    pub(crate) async fn suspend_quiescent_execution_with_work(
        &self,
        execution: OperationId,
        tail: u64,
        revision: u64,
        command: OperationId,
        work: &mut Option<&mut dyn crate::store::history::HistoryIo>,
    ) -> Result<()> {
        self.host
            .suspend_workflow(
                self.task_id,
                &self.fence,
                revision,
                Some(command),
                Some((execution, tail)),
                None,
                work,
            )
            .await
    }

    /// Consumes only the original journal's sealed current Pending witness and settled execution tail.
    #[cfg(not(target_arch = "wasm32"))]
    pub(crate) async fn release_quiescent_execution_with_native_work(
        &self,
        execution: OperationId,
        tail: u64,
        pending: &crate::filesystem::WorkflowReleaseWitness<'_>,
        work: &acyclic_fs::NativeMaterializationWork,
    ) -> Result<bool> {
        let mut io = crate::filesystem::PreparationIo::from_native_work(work)
            .map(crate::store::HistoryWork::new)?;
        if pending.condition().0 != (self.task_id, &self.fence) {
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
        crate::scheduler::require_execution_owner(operation, &self.fence, true, &mut None)?;
        if operation.cancellation_requested {
            return Err(Error::Conflict(
                "read-only close cannot settle task cancellation".into(),
            ));
        }
        if operation.phase == crate::scheduler::OperationPhase::Reconciling {
            return Ok(false);
        }
        let revision = operation.revision;
        let key = worker_key_with_work(
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

    /// Derives a workflow for this original task's scoped tool invocation.
    pub(crate) async fn for_tool_workflow(
        mut self,
        context: &crate::runtime::ToolContext,
        tool: &crate::durable_tool::ResumableTool,
        invocation: &crate::tool::ToolInvocation,
        cancellation: &acyclic_fs::CancellationToken,
        io: Option<&crate::executor::PreparationIo>,
    ) -> Result<Self> {
        self.invocation_cancellation = Some(cancellation.clone());
        if !context
            .task()
            .original_task_binding_matches(self.task_id, &self.fence)
            || invocation.operation_id == self.operation_id()
        {
            return Err(Error::Unauthorized(
                "tool workflow differs from original task owner".into(),
            ));
        }
        #[cfg(not(target_arch = "wasm32"))]
        let native = io.and_then(crate::executor::PreparationIo::original_native_work);
        #[cfg(not(target_arch = "wasm32"))]
        if let Some(work) = native {
            context.task().require_original_native_dispatch(work)?;
            self.require_mail_scope_with_native_work(context.task().scope(), work)?;
        }
        let mut history = io.map(crate::executor::PreparationIo::history_io);
        let mut work = history
            .as_mut()
            .map(|work| work as &mut dyn crate::store::HistoryIo);
        self.require_mail_scope(context.task().scope(), &mut work)?;
        {
            let mut quota = crate::wire_codec::CodecQuota::new(&mut work, 0)?;
            let (admission, held) = self
                .host
                .read_retained_admission_counted(self.operation_id(), quota.work())
                .await?;
            quota.adopt(held)?;
            if context.task().scope().run_limits() != admission.run_limits
                || context.task().scope().extensions() != admission.extensions.as_ref()
            {
                return Err(Error::Unauthorized(
                    "tool workflow changed original admission policy".into(),
                ));
            }
            drop(admission);
        }
        self.verify(false, &mut work).await?;
        #[cfg(not(target_arch = "wasm32"))]
        let before = io
            .map(crate::executor::PreparationIo::owned_bytes)
            .transpose()?
            .unwrap_or(0);
        self.workflow = tool.workflow_admission(context, invocation, io)?;
        self.output_schema = {
            let mut quota = crate::wire_codec::CodecQuota::new(&mut work, 0)?;
            let value = crate::core::copy_value(&tool.definition.output_schema, &mut quota)?;
            quota.transfer();
            value
        };
        #[cfg(not(target_arch = "wasm32"))]
        if let Some(native) = native {
            let io = io.expect("original native funding");
            let bytes = io
                .owned_bytes()?
                .checked_sub(before)
                .ok_or(Error::Invalid("tool workflow custody underflow".into()))?;
            let mut retained = io.take_native_custody(bytes)?;
            self._native_custody
                .as_mut()
                .expect("authenticated native owner")
                .absorb(&mut retained)?;
            let bytes = self.native_owned_bytes()?;
            self._native_custody
                .as_mut()
                .expect("authenticated native owner")
                .resize(bytes)?;
            native.remaining()?;
        }
        Ok(self)
    }

    #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
    pub(crate) fn require_native_workflow_validation(&self) -> Result<()> {
        self.host
            .machines
            .require_native_work(&self.workflow.initial.machine)
    }

    pub(crate) fn validate_workflow_record(
        &self,
        record: &crate::workflow::WorkflowRecord,
        io: Option<&crate::executor::PreparationIo>,
    ) -> Result<()> {
        let conflict = |reason| {
            crate::executor::with_history_io(io, |work| {
                admission_boundary(work, crate::wire::ErrorCode::Conflict, reason)
            })
        };
        if record.prior.machine != self.workflow.initial.machine {
            return Err(conflict(
                "workflow implementation differs from task admission",
            ));
        }
        #[cfg(not(target_arch = "wasm32"))]
        let proof = if let Some(io) = io {
            let work = io.original_native_work().ok_or(Error::NativeBoundary {
                code: crate::wire::ErrorCode::Unsupported,
                reason: "workflow proof requires original native work",
            })?;
            self.host
                .machines
                .require_native_work(&record.prior.machine)?;
            let mut history = io.history_io();
            let mut observer: Option<&mut dyn crate::store::history::HistoryIo> =
                Some(&mut history);
            let mut quota = crate::wire_codec::CodecQuota::new(&mut observer, 0)?;
            let input = crate::core::copy_value(&record.input, &mut quota)?;
            quota.transfer();
            let (next, transition, custody) =
                self.host
                    .machines
                    .step_with_native_work(&record.prior, input, work)?;
            (next, transition, Some(custody))
        } else {
            let (next, transition) = self.host.machines.step(&record.prior, &record.input)?;
            (next, transition, None)
        };
        #[cfg(target_arch = "wasm32")]
        let proof = self.host.machines.step(&record.prior, &record.input)?;
        if proof.0 != record.next || proof.1 != record.transition {
            return Err(conflict("workflow transition differs from pinned machine"));
        }
        if let crate::workflow::MachineStatus::Completed { value } = &proof.1.status {
            crate::executor::with_history_io(io, |work| {
                crate::contract::validate_json_schema_value(
                    &self.output_schema,
                    value,
                    "task output",
                    work,
                )
            })?;
        }
        Ok(())
    }

    pub(crate) fn stream(&self) -> std::sync::Arc<P> {
        Arc::clone(&self.stream)
    }

    pub(crate) fn operation_id(&self) -> OperationId {
        OperationId::from_bytes(self.task_id.into_bytes())
    }

    pub(crate) async fn condition(
        &self,
        write: crate::distributed::JournalWrite,
        work: &mut Option<&mut dyn crate::store::history::HistoryIo>,
    ) -> Result<acyclic_stream::CommitCondition> {
        if let Some(io) = work.as_deref_mut() {
            io.check_cancellation()?;
        }
        if self.is_readonly() {
            return Err(admission_boundary(
                work,
                crate::wire::ErrorCode::Unauthorized,
                "historical READ custody cannot prepare a journal write",
            ));
        }
        if !matches!(write, crate::distributed::JournalWrite::Settlement)
            && self
                .deadline_epoch_ms
                .is_some_and(|deadline| self.host.clock.now_unix_millis() >= deadline)
        {
            if let Some(cancellation) = &self.invocation_cancellation {
                cancellation.cancel();
            }
            if work.is_none() {
                self.host
                    .expire_task_deadline(self.task_id, Some(&self.fence), &mut None)
                    .await?;
            }
            return Err(Error::Indeterminate(self.operation_id()));
        }
        let result = self
            .host
            .coordinator
            .lock()
            .await
            .journal_condition(
                self.host.verifier.audience(),
                &self.host.owner_scope,
                &self.host.verifier,
                self.operation_id(),
                &self.fence,
                write,
                work,
            )
            .await;
        if result.is_err() {
            if let Some(cancellation) = &self.invocation_cancellation {
                cancellation.cancel();
            }
        }
        result
    }

    /// Checks the original owner fence for dispatch or settlement.
    pub async fn verify(
        &self,
        allow_settlement: bool,
        work: &mut Option<&mut dyn crate::store::history::HistoryIo>,
    ) -> Result<()> {
        #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
        if !allow_settlement
            && work
                .as_deref()
                .and_then(|io| io.original_native_work())
                .is_some()
            && let Some(access) = self.input_created.as_ref()
            && let Some(binding) = access.current_content(work)?
        {
            if binding.binding.writer.is_none() {
                return Err(admission_boundary(
                    work,
                    crate::wire::ErrorCode::Unauthorized,
                    "current created scratch has no WRITE custody",
                ));
            }
            self.require_volume_grant(
                &binding.volume,
                crate::conversation::VolumeOperation::Write,
                work,
            )?;
        }
        let write = if allow_settlement {
            crate::distributed::JournalWrite::Settlement
        } else {
            crate::distributed::JournalWrite::Fresh
        };
        self.condition(write, work).await.map(|_| ())
    }

    pub(super) fn is_readonly(&self) -> bool {
        #[cfg(not(target_arch = "wasm32"))]
        {
            self.readonly_original.is_some()
        }
        #[cfg(target_arch = "wasm32")]
        {
            false
        }
    }

    pub(crate) async fn verify_read_access(
        &self,
        work: &mut Option<&mut dyn crate::store::history::HistoryIo>,
    ) -> Result<()> {
        if let Some(io) = work.as_deref_mut() {
            io.check_cancellation()?;
        }
        if self.is_readonly() {
            return Ok(());
        }
        self.verify(true, work).await
    }

    pub(super) fn execution_write(
        operation: OperationId,
        event: &crate::executor::ExecutionEvent,
        work: &mut Option<&mut dyn crate::store::history::HistoryIo>,
    ) -> Result<crate::distributed::JournalWrite> {
        use crate::{distributed::JournalWrite, executor::ExecutionEvent};
        Ok(match event {
            ExecutionEvent::ModelStarted {
                step,
                purpose,
                request_digest,
                ..
            } => {
                #[cfg(not(target_arch = "wasm32"))]
                let io = work
                    .as_deref()
                    .and_then(|io| io.original_native_work())
                    .map(crate::filesystem::PreparationIo::from_native_work)
                    .transpose()?;
                #[cfg(target_arch = "wasm32")]
                let io: Option<crate::filesystem::PreparationIo> = None;
                JournalWrite::Model {
                    attempt_id: purpose.attempt_operation(operation, io.as_ref())?,
                    step: *step,
                    request_digest: *request_digest,
                }
            }
            ExecutionEvent::Started { .. }
            | ExecutionEvent::ContextPrepared { .. }
            | ExecutionEvent::ContextCompacted { .. }
            | ExecutionEvent::ToolStarted { .. } => JournalWrite::Fresh,
            ExecutionEvent::Model { .. }
            | ExecutionEvent::ToolCompleted { .. }
            | ExecutionEvent::ToolFailed { .. } => JournalWrite::Settlement,
        })
    }

    pub(crate) async fn verify_execution_with_work(
        &self,
        operation: OperationId,
        event: &crate::executor::ExecutionEvent,
        work: &mut Option<&mut dyn crate::store::history::HistoryIo>,
    ) -> Result<()> {
        self.condition(Self::execution_write(operation, event, work)?, work)
            .await
            .map(|_| ())
    }

    pub(crate) async fn verify_model_history<'a>(
        &self,
        operation: OperationId,
        records: impl Iterator<Item = &'a crate::executor::ExecutionRecord> + Clone,
        work: &mut Option<&mut dyn crate::store::history::HistoryIo>,
    ) -> Result<()> {
        self.host
            .verify_model_history(self.task_id, operation, records, work)
            .await
    }

    pub(crate) async fn append_conversation(
        &self,
        publication: &crate::store::operations::IndexedPublication,
        key: &StreamKey,
        write: crate::distributed::JournalWrite,
        work: &mut Option<&mut dyn crate::store::history::HistoryIo>,
        mut phase: Option<&mut crate::distributed::CommitPhase>,
    ) -> Result<bool> {
        if let Some(phase) = phase.as_deref_mut() {
            *phase = crate::distributed::CommitPhase::Uncommitted;
        }
        let fresh = !matches!(write, crate::distributed::JournalWrite::Settlement);
        let mut request = self
            .prepare_append_with_work(
                &publication.path,
                publication.expected_tail,
                key,
                &publication.bytes,
                write,
                work,
            )
            .await?;
        publication.add_index(&mut request)?;
        if fresh
            && self
                .deadline_epoch_ms
                .is_some_and(|deadline| self.host.clock.now_unix_millis() >= deadline)
        {
            return Err(Error::Indeterminate(self.operation_id()));
        }
        let (outcome, retained) = crate::distributed::commit(
            request,
            &self.stream,
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
                    #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
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

    pub(crate) async fn append_execution_with_work(
        &self,
        operation: OperationId,
        expected_tail: u64,
        key: &StreamKey,
        bytes: OwnedBytes,
        event: &crate::executor::ExecutionEvent,
        work: &mut Option<&mut dyn crate::store::history::HistoryIo>,
    ) -> Result<bool> {
        if let Some(io) = work.as_deref_mut() {
            io.check_cancellation()?;
        }
        if bytes.len() > acyclic_stream::MAX_RECORD_BYTES {
            return Err(admission_boundary(
                work,
                crate::wire::ErrorCode::Invalid,
                "execution record exceeds canonical Stream byte bound",
            ));
        }
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

    pub(crate) async fn append_workflow_with_work(
        &self,
        admission: bool,
        expected_tail: u64,
        key: &StreamKey,
        bytes: OwnedBytes,
        work: &mut Option<&mut dyn crate::store::history::HistoryIo>,
    ) -> Result<bool> {
        let class = if admission {
            "workflow-admissions"
        } else {
            "workflows"
        };
        let mut quota = crate::wire_codec::CodecQuota::new(work, 0)?;
        let text = crate::contract::preparation_format(
            format_args!("harness/v2/{class}/{}", self.operation_id()),
            quota.work(),
        )?;
        quota.adopt(text.capacity() as u64)?;
        let stream = crate::store::operations::stream(&self.stream, &text, quota.work())?;
        self.append(
            stream.0.path().clone(),
            expected_tail,
            key,
            bytes,
            crate::distributed::JournalWrite::Fresh,
            quota.work(),
        )
        .await
    }

    pub(super) async fn prepare_append_with_work<'a, 'b>(
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
            return Err(admission_boundary(
                work,
                crate::wire::ErrorCode::Invalid,
                "owned journal record exceeds the canonical byte bound",
            ));
        }
        let owner = self.condition(write, work).await?;
        if let Some(io) = work.as_deref_mut() {
            io.begin_call()?;
        }
        let target = match self
            .stream
            .bounds(acyclic_stream::StreamPath::new(path.as_str())?)
            .await
        {
            Ok(_) => CommitCondition::Tail {
                path: path.clone(),
                expected: expected_tail,
            },
            Err(StreamError::NotFound) if expected_tail == 0 => {
                CommitCondition::Absent { path: path.clone() }
            }
            Err(error) => return Err(error.into()),
        };
        if let Some(io) = work.as_deref_mut() {
            io.check_cancellation()?;
        }
        let mut hash = blake3::Hasher::new();
        for part in [
            b"harness/v2/owned-journal\0".as_slice(),
            key.as_bytes(),
            &self.task_id.into_bytes(),
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
        work: &mut Option<&mut dyn crate::store::history::HistoryIo>,
    ) -> Result<bool> {
        use acyclic_stream::{CommitOutcome, CommittedMutation};
        let request = self
            .prepare_append_with_work(&path, expected_tail, key, &bytes, write, work)
            .await?;
        let (outcome, retained) =
            crate::distributed::commit(request, &self.stream, Some(self.operation_id()), None)
                .await?;
        let mut received = crate::wire_codec::CodecQuota::from_retained(work, retained);
        let result = match &outcome {
            CommitOutcome::Conflict(_) => Ok(false),
            CommitOutcome::Committed(envelope) => {
                let invalid = match envelope.mutations.as_slice() {
                    [CommittedMutation::Append(append)] => {
                        let valid = append.path == path
                            && append.start == expected_tail
                            && append.end == expected_tail + 1
                            && append.tail == append.end
                            && append.records.len() == 1
                            && append.records.first().is_some_and(|record| {
                                record.sequence == expected_tail
                                    && record.value == bytes
                                    && record.commit_id == envelope.commit_id
                            });
                        (!valid).then_some("invalid owned journal append")
                    }
                    _ => Some("invalid owned journal envelope"),
                };
                match invalid {
                    None => Ok(true),
                    Some(reason) => Err(admission_boundary(
                        received.work(),
                        crate::wire::ErrorCode::Storage,
                        reason,
                    )),
                }
            }
        };
        drop(outcome);
        drop(received);
        result
    }
}
