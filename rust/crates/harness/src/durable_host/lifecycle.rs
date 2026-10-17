//! Main durable task operations.
use super::*;

impl<P: StreamProvider> CoordinatorTaskHost<P> {
    /// Claims signed-owner work, optionally restricted to one original operation.
    pub async fn pull_work(
        &self,
        worker: &crate::distributed::Worker,
        selected: Option<OperationId>,
        work: &mut Option<&mut dyn crate::store::HistoryIo>,
    ) -> Result<crate::distributed::WorkPull> {
        self.coordinator
            .lock()
            .await
            .pull_owned(
                &self.owner,
                &self.owner_scope,
                &self.verifier,
                worker,
                selected,
                work,
            )
            .await
    }

    /// Recovers the retained reservation without claiming or publishing another lease.
    pub async fn recover_work(
        &self,
        task: TaskId,
        work: &mut Option<&mut dyn crate::store::HistoryIo>,
    ) -> Result<crate::distributed::WorkPull> {
        use crate::distributed::{WorkLease, WorkPull};
        use crate::scheduler::OperationPhase;
        let operation = OperationId::from_bytes(task.into_bytes());
        self.admission_run_limits(operation, work).await?;
        let mut coordinator = self.coordinator.lock().await;
        coordinator.refresh(work).await?;
        let state = coordinator.observe_operation(
            &self.owner,
            &self.owner_scope,
            &self.verifier,
            operation,
            work,
        )?;
        let Some(reservation) = &state.reservation else {
            return Ok(WorkPull::Idle);
        };
        let lease = WorkLease {
            operation: state.spec.clone_with_work(work)?,
            reservation: reservation.clone_with_work(work)?,
            checkpoint: state
                .checkpoint
                .as_ref()
                .map(|value| value.clone_with_work(work))
                .transpose()?,
            operation_revision: state.revision,
        };
        if state.cancellation_requested
            || !matches!(
                state.phase,
                OperationPhase::Admitted | OperationPhase::Running | OperationPhase::Reconciling
            )
        {
            return Ok(WorkPull::Unresolved {
                lease,
                error: crate::scheduler::diagnostic(
                    work,
                    crate::wire::ErrorCode::Conflict,
                    "retained task reservation requires reconciliation",
                ),
            });
        }
        Ok(WorkPull::Claimed(lease))
    }

    /// Starts or reattaches the exact original lease without claiming another reservation.
    pub async fn start_task(
        &self,
        lease: &crate::distributed::WorkLease,
        work: &mut Option<&mut dyn crate::store::HistoryIo>,
    ) -> Result<()> {
        let operation = lease.operation.operation_id;
        let limits = self.admission_run_limits(operation, work).await?;
        let fence = crate::scheduler::LeaseFence {
            reservation_id: match work.as_deref_mut() {
                Some(io) => io.clone_text(&lease.reservation.id)?,
                None => lease.reservation.id.clone(),
            },
            placement: match work.as_deref_mut() {
                Some(io) => io.clone_text(&lease.reservation.placement)?,
                None => lease.reservation.placement.clone(),
            },
        };
        let mut coordinator = self.coordinator.lock().await;
        coordinator.refresh(work).await?;
        let state = coordinator.observe_operation(
            &self.owner,
            &self.owner_scope,
            &self.verifier,
            operation,
            work,
        )?;
        if state.spec != lease.operation || state.reservation.as_ref() != Some(&lease.reservation) {
            return Err(crate::scheduler::diagnostic(
                work,
                crate::wire::ErrorCode::Conflict,
                "worker lease differs from retained admission",
            ));
        }
        if state.phase != crate::scheduler::OperationPhase::Reconciling
            && limits
                .deadline_epoch_ms
                .is_some_and(|deadline| self.now_unix_millis() >= deadline)
        {
            drop(coordinator);
            self.expire_task_deadline(
                TaskId::from_bytes(operation.into_bytes()),
                Some(&fence),
                work,
            )
            .await?;
            return Err(Error::Indeterminate(operation));
        }
        if state.phase == crate::scheduler::OperationPhase::Running {
            return crate::scheduler::require_execution_owner(state, &fence, false, work);
        }
        let key = worker_key("start", operation, &fence, state.revision, work)?;
        let event = if state.phase == crate::scheduler::OperationPhase::Reconciling {
            crate::scheduler::SchedulerEvent::ReconciliationResumed {
                operation_id: operation,
                fence,
            }
        } else {
            crate::scheduler::SchedulerEvent::Started {
                operation_id: operation,
                fence,
            }
        };
        coordinator.apply(key, event, work).await?;
        Ok(())
    }

    /// Settles a schema-checked result under the exact original fence.
    pub async fn settle_task(
        &self,
        task: TaskId,
        fence: &crate::scheduler::LeaseFence,
        outcome: Outcome<Value>,
        work: &mut Option<&mut dyn crate::store::HistoryIo>,
    ) -> Result<()> {
        let operation = OperationId::from_bytes(task.into_bytes());
        let io = operation_io(work)?;
        #[cfg(feature = "filesystem")]
        let mut admission_history = io.as_ref().map(|io| io.history_io());
        #[cfg(feature = "filesystem")]
        let mut admission_work = admission_history
            .as_mut()
            .map(|io| io as &mut dyn crate::store::HistoryIo);
        #[cfg(not(feature = "filesystem"))]
        let mut admission_work = None;
        let admission = self.admission(operation, &mut admission_work).await?;
        self.verify_owner(task, fence, true, work).await?;
        let digest = crate::executor::json::canonical_json_digest(
            &outcome,
            acyclic_stream::MAX_RECORD_BYTES as u64,
            work,
        )?;
        let lease_digest = crate::executor::json::canonical_json_digest(
            fence,
            acyclic_stream::MAX_RECORD_BYTES as u64,
            work,
        )?;
        let text = crate::executor::preparation_format(
            format_args!(
                "task-settle:{operation}:{}:{}",
                blake3::Hash::from_bytes(lease_digest).to_hex(),
                blake3::Hash::from_bytes(digest).to_hex()
            ),
            work,
        )?;
        let mut quota = crate::wire_codec::CodecQuota::from_retained(work, text.capacity() as u64);
        let key = IdempotencyKey::new(text)?;
        let outcome = match outcome {
            Outcome::Succeeded(value) => {
                crate::executor::validate_json_schema_value(
                    &admission.output_schema,
                    &value,
                    "task output",
                    quota.work(),
                )?;
                let bytes = canonical_payload(&value, admission.limits.file_bytes, io.as_ref())?;
                if bytes.len() as u64 > admission.limits.file_bytes {
                    return Err(crate::scheduler::diagnostic(
                        quota.work(),
                        crate::wire::ErrorCode::Invalid,
                        "task result exceeds admitted file limit",
                    ));
                }
                Outcome::Succeeded(
                    self.payloads
                        .stage(operation, key.as_str(), &bytes, io.as_ref())?
                        .await?,
                )
            }
            Outcome::Failed { message } => Outcome::Failed { message },
            Outcome::Cancelled => Outcome::Cancelled,
            Outcome::Indeterminate { operation_id } => Outcome::Indeterminate { operation_id },
        };
        self.coordinator
            .lock()
            .await
            .apply(
                key,
                crate::scheduler::SchedulerEvent::Completed {
                    operation_id: operation,
                    outcome,
                    fence: Some(fence.clone_with_work(quota.work())?),
                    execution_duration_ns: None,
                },
                quota.work(),
            )
            .await?;
        Ok(())
    }

    pub(crate) async fn verify_owner(
        &self,
        task: TaskId,
        fence: &crate::scheduler::LeaseFence,
        settlement: bool,
        work: &mut Option<&mut dyn crate::store::HistoryIo>,
    ) -> Result<()> {
        let operation = OperationId::from_bytes(task.into_bytes());
        if !settlement {
            let limits = self.admission_run_limits(operation, work).await?;
            if limits
                .deadline_epoch_ms
                .is_some_and(|deadline| self.now_unix_millis() >= deadline)
            {
                return Err(Error::Indeterminate(operation));
            }
        }
        let mut coordinator = self.coordinator.lock().await;
        coordinator.refresh(work).await?;
        crate::scheduler::require_execution_owner(
            coordinator.observe_operation(
                &self.owner,
                &self.owner_scope,
                &self.verifier,
                operation,
                work,
            )?,
            fence,
            settlement,
            work,
        )
    }

    /// Binds journal publications to the authenticated task's exact current lease.
    #[cfg(feature = "filesystem")]
    pub async fn journal_owner(
        self: &Arc<Self>,
        task_id: TaskId,
        fence: &crate::scheduler::LeaseFence,
        io: Option<&crate::executor::PreparationIo>,
    ) -> Result<TaskJournalOwner<P>> {
        self.journal_owner_record(task_id, fence, None, io)
            .await
            .map(|(owner, _)| owner)
    }

    #[cfg(feature = "filesystem")]
    pub(crate) async fn ordinary_journal_owner_with_scope(
        self: &Arc<Self>,
        task_id: TaskId,
        fence: &crate::scheduler::LeaseFence,
    ) -> Result<(TaskJournalOwner<P>, RuntimeScope)> {
        self.journal_owner_record(task_id, fence, None, None).await
    }

    #[cfg(feature = "filesystem")]
    #[allow(
        clippy::too_many_lines,
        reason = "The original native custody stays in the same journal reconstruction future."
    )]
    pub(super) async fn journal_owner_record(
        self: &Arc<Self>,
        task_id: TaskId,
        fence: &crate::scheduler::LeaseFence,
        original: Option<OperationSpec>,
        io: Option<&crate::executor::PreparationIo>,
    ) -> Result<(TaskJournalOwner<P>, RuntimeScope)> {
        let operation = OperationId::from_bytes(task_id.into_bytes());
        #[cfg(not(target_arch = "wasm32"))]
        let (admission, mut custody) = match io {
            Some(io) if io.original_native_work().is_some() => {
                let admission = self
                    .admission(operation, &mut Some(&mut io.history_io()))
                    .await?;
                let custody = io.take_native_custody(
                    std::mem::size_of::<TaskAdmissionRecord>() as u64
                        + admission.logical_owned_bytes()?,
                )?;
                (admission, Some(custody))
            }
            _ => (self.admission(operation, &mut None).await?, None),
        };
        #[cfg(target_arch = "wasm32")]
        let admission = self.admission(operation, &mut None).await?;
        let mut history = io.map(|io| io.history_io());
        let mut work = history
            .as_mut()
            .map(|io| io as &mut dyn crate::store::HistoryIo);
        let mut coordinator = self.coordinator.lock().await;
        coordinator.refresh(&mut work).await?;
        let declared_agent = coordinator
            .observe_operation(
                &self.owner,
                &self.owner_scope,
                &self.verifier,
                operation,
                &mut work,
            )?
            .spec
            .owner_scope
            .agent();
        drop(coordinator);
        if admission.parent.is_none() && declared_agent != self.owner_scope.agent() {
            return Err(Error::Unauthorized(
                "root actor differs from its declaration".into(),
            ));
        }
        let request_digest = crate::executor::json::canonical_json_digest(
            &admission,
            acyclic_stream::MAX_RECORD_BYTES as u64,
            &mut work,
        )?;
        #[cfg(not(target_arch = "wasm32"))]
        let state = match custody.as_mut() {
            Some(custody) => {
                let (state, mut held) = self.machines.initialize_with_native_work(
                    &admission.machine,
                    &admission.input,
                    custody.work(),
                )?;
                custody.absorb(&mut held)?;
                state
            }
            None => self
                .machines
                .resolve(&admission.machine, &mut work)?
                .ok_or_else(|| Error::Unsupported("pinned task machine is unavailable".into()))?
                .initialize(&admission.input)?,
        };
        #[cfg(target_arch = "wasm32")]
        let state = self
            .machines
            .resolve(&admission.machine, &mut work)?
            .ok_or_else(|| Error::Unsupported("pinned task machine is unavailable".into()))?
            .initialize(&admission.input)?;
        let machine = crate::workflow::workflow_copy_identity(&admission.machine, io)?;
        let workflow = crate::workflow::WorkflowAdmission {
            operation_id: operation,
            request_digest,
            initial: crate::workflow::MachineCheckpoint {
                machine,
                revision: 0,
                state,
            },
        };
        self.machines.validate_checkpoint(&workflow.initial, io)?;
        if let Some(io) = work.as_deref_mut() {
            io.retain_owned(
                (std::mem::size_of::<TaskJournalOwner<P>>() + 2 * std::mem::size_of::<usize>())
                    as u64,
            )?;
        }
        let fence = fence.clone_with_work(&mut work)?;
        #[cfg(not(target_arch = "wasm32"))]
        if let (Some(custody), Some(io)) = (custody.as_mut(), io) {
            let retained = (std::mem::size_of::<TaskJournalOwner<P>>()
                + 2 * std::mem::size_of::<usize>()) as u64
                + fence.reservation_id.capacity() as u64
                + fence.placement.capacity() as u64
                + workflow.initial.machine.name.capacity() as u64
                + workflow.initial.machine.version.capacity() as u64;
            custody.absorb(&mut io.take_native_custody(retained)?)?;
        }
        #[cfg(not(target_arch = "wasm32"))]
        let before_scope = io.map(|io| io.owned_bytes()).transpose()?.unwrap_or(0);
        let (scope, _scope_bytes) = self.authenticated_scope(&admission, &mut work).await?;
        let created = scope.created_access();
        let declared_agent = if admission.parent.is_none() {
            declared_agent
        } else {
            scope
                .current_created_content(&mut work)?
                .map(|binding| match binding.volume.owner() {
                    crate::conversation::VolumeOwner::Agent(agent) => Ok(*agent),
                    _ => Err(Error::Unauthorized(
                        "created scratch is not agent-owned".into(),
                    )),
                })
                .transpose()?
        };
        #[cfg(not(target_arch = "wasm32"))]
        if let (Some(custody), Some(io)) = (custody.as_mut(), io) {
            custody.absorb(
                &mut io.take_native_custody(
                    io.owned_bytes()?.checked_sub(before_scope).ok_or_else(|| {
                        Error::Invalid("created scope ownership underflows".into())
                    })?,
                )?,
            )?;
        }
        let owner = TaskJournalOwner {
            host: self.clone(),
            fence,
            workflow,
            admission: Arc::new(admission),
            input_agent: declared_agent,
            created,
            read_proof: original,
            tool_output_schema: None,
            invocation_cancellation: None,
            root_binding: std::sync::OnceLock::new(),
            root_choice: std::sync::OnceLock::new(),
            root_gate: Mutex::new(()),
            #[cfg(not(target_arch = "wasm32"))]
            native_custody: custody,
        };
        if owner.read_proof.is_some() {
            owner.verify_read_access(&mut work).await?;
        } else {
            owner.verify(true, &mut work).await?;
        }
        Ok((owner, scope))
    }

    pub(super) async fn publish_control(
        &self,
        stream: &acyclic_stream::Stream<P>,
        kind: &str,
        task_id: TaskId,
        operation_id: OperationId,
        bytes: &[u8],
    ) -> Result<()> {
        let key = Self::event_key(kind, task_id, operation_id, &mut None)?.0;
        let outcome = crate::distributed::append(
            (stream, &self.stream),
            [OwnedBytes::copy_from_slice(bytes)],
            0,
            None,
            key,
            operation_id,
            &mut None,
        )
        .await?;
        match outcome {
            AppendOutcome::Committed(receipt) if receipt.end == receipt.start + 1 => {
                let records = stream
                    .read(receipt.start, 1)
                    .await?
                    .try_collect::<Vec<_>>()
                    .await?;
                let [record] = records.as_slice() else {
                    return Err(Error::Conflict(
                        "control publication differs from its committed record".into(),
                    ));
                };
                if record.sequence != receipt.start
                    || record.commit_id != receipt.commit_id
                    || record.value.as_ref() != bytes
                {
                    return Err(Error::Conflict(
                        "control publication differs from its committed record".into(),
                    ));
                }
                Ok(())
            }
            AppendOutcome::Committed(_) => {
                Err(Error::Storage("invalid control append receipt".into()))
            }
            AppendOutcome::TailConflict { .. } => Err(Error::Conflict(
                "unconditional control append conflicted".into(),
            )),
        }
    }
}
