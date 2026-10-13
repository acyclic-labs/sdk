//! Main durable task operations.
use super::*;

impl<P: StreamProvider> CoordinatorTaskHost<P> {
    /// Claims through the same coordinator, restricted to this signed owner.
    pub async fn pull_work(
        &self,
        worker: &crate::distributed::Worker,
    ) -> Result<crate::distributed::WorkPull> {
        self.coordinator
            .lock()
            .await
            .pull_owned(
                &self.owner,
                &self.owner_scope,
                &self.verifier,
                worker,
                None,
                &mut None,
            )
            .await
    }

    /// Claims this original operation without owner-wide task selection.
    pub async fn pull_operation(
        &self,
        worker: &crate::distributed::Worker,
        operation_id: OperationId,
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
                Some(operation_id),
                work,
            )
            .await
    }

    /// Recovers the coordinator's original reservation after a lost pull response.
    /// This performs no claim, release, or publication. The returned reference is
    /// not execution authority: start/resume must still verify its exact fence.
    /// A retained cancelled or nonexecutable reservation remains unresolved.
    pub async fn recover_work(&self, task: TaskId) -> Result<crate::distributed::WorkPull> {
        use crate::distributed::{WorkLease, WorkPull};
        use crate::scheduler::OperationPhase;

        let operation_id = OperationId::from_bytes(task.into_bytes());
        self.admission(operation_id, None).await?;
        let mut coordinator = self.coordinator.lock().await;
        coordinator.refresh(&mut None).await?;
        let operation = coordinator
            .observe_operation(
                &self.owner,
                &self.owner_scope,
                &self.verifier,
                operation_id,
                &mut None,
            )?
            .clone();
        let Some(reservation) = operation.reservation else {
            return Ok(WorkPull::Idle);
        };
        let lease = WorkLease {
            operation: operation.spec,
            reservation,
            checkpoint: operation.checkpoint,
            operation_revision: operation.revision,
        };
        if operation.cancellation_requested
            || !matches!(
                operation.phase,
                OperationPhase::Admitted | OperationPhase::Running | OperationPhase::Reconciling
            )
        {
            return Ok(WorkPull::Unresolved {
                lease,
                error: Error::Conflict("retained task reservation requires reconciliation".into()),
            });
        }
        Ok(WorkPull::Claimed(lease))
    }

    /// Starts or reattaches the exact already-admitted lease. This does not
    /// acquire another reservation or infer that an uncertain provider stopped.
    pub async fn start_task(&self, lease: &crate::distributed::WorkLease, native: crate::NativeWork<'_>) -> Result<()> {
        #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
        let io = native.map(crate::filesystem::PreparationIo::from_native_work).transpose()?;
        #[cfg(not(all(feature = "filesystem", not(target_arch = "wasm32"))))]
        let io = None;
        #[cfg(feature = "filesystem")]
        let mut observer = io.as_ref().map(crate::filesystem::PreparationIo::history_io);
        #[cfg(feature = "filesystem")]
        let mut work = observer.as_mut().map(|work| work as &mut dyn crate::store::HistoryIo);
        #[cfg(not(feature = "filesystem"))]
        let mut work = None;
        let _ = native;
        let operation_id = lease.operation.operation_id;
        let fence = crate::scheduler::LeaseFence::from(&lease.reservation);
        self.admission(operation_id, io.as_ref()).await?;
        let mut coordinator = self.coordinator.lock().await;
        coordinator.refresh(&mut work).await?;
        let operation = coordinator
            .observe_operation(
                &self.owner,
                &self.owner_scope,
                &self.verifier,
                operation_id,
                &mut work,
            )?
            .clone();
        if operation.spec != lease.operation
            || operation.reservation.as_ref() != Some(&lease.reservation)
        {
            return Err(Error::Conflict(
                "worker lease differs from retained admission".into(),
            ));
        }
        if operation.phase == crate::scheduler::OperationPhase::Running {
            return crate::scheduler::require_execution_owner(&operation, &fence, false, &mut work);
        }
        let event = if operation.phase == crate::scheduler::OperationPhase::Reconciling {
            crate::scheduler::SchedulerEvent::ReconciliationResumed {
                operation_id,
                fence: fence.clone(),
            }
        } else {
            crate::scheduler::SchedulerEvent::Started {
                operation_id,
                fence: fence.clone(),
            }
        };
        coordinator
            .apply(
                worker_key("start", operation_id, &fence, operation.revision, &mut work)?,
                event,
                &mut work,
            )
            .await?;
        Ok(())
    }

    /// Publishes a schema-checked result or retains uncertainty with this exact
    /// lease. A staged result can survive a publication conflict without making
    /// that conflict a successful task completion.
    pub async fn settle_task(
        &self,
        task: TaskId,
        fence: &crate::scheduler::LeaseFence,
        outcome: Outcome<Value>,
        work: &mut Option<&mut dyn crate::store::HistoryIo>,
    ) -> Result<()> {
        #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
        let io = work.as_deref().and_then(|work| work.original_native_work())
            .map(crate::filesystem::PreparationIo::from_native_work).transpose()?;
        #[cfg(not(all(feature = "filesystem", not(target_arch = "wasm32"))))]
        let io = None;
        let operation_id = OperationId::from_bytes(task.into_bytes());
        let admission = self.admission(operation_id, io.as_ref()).await?;
        self.verify_owner(task, fence, true, work).await?;
        let digest = crate::executor::json::canonical_json_digest(
            &outcome,
            acyclic_stream::MAX_RECORD_BYTES as u64,
            work,
        )?;
        let key = IdempotencyKey::new(format!(
            "task-settle:{operation_id}:{}:{}",
            blake3::Hash::from_bytes(crate::executor::json::canonical_json_digest(
                &fence,
                acyclic_stream::MAX_RECORD_BYTES as u64,
                work
            )?)
            .to_hex(),
            blake3::Hash::from_bytes(digest).to_hex()
        ))?;
        let outcome = match outcome {
            Outcome::Succeeded(value) => {
                crate::executor::validate_json_schema_value(
                    &admission.output_schema,
                    &value,
                    "task output",
                    work,
                )?;
                let bytes = crate::store::history::canonical_bytes_with_work(&value, admission.limits.file_bytes, work)?;
                if bytes.len() as u64 > admission.limits.file_bytes {
                    return Err(Error::Invalid(
                        "task result exceeds admitted file limit".into(),
                    ));
                }
                Outcome::Succeeded(
                    self.payloads
                        .stage(operation_id, key.as_str(), &bytes, io.as_ref())?
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
                    operation_id,
                    outcome,
                    fence: Some(fence.clone_with_work(work)?),
                    execution_duration_ns: None,
                },
                work,
            )
            .await?;
        Ok(())
    }

    pub(crate) async fn verify_owner(
        &self,
        task_id: TaskId,
        fence: &crate::scheduler::LeaseFence,
        settlement: bool,
        work: &mut Option<&mut dyn crate::store::HistoryIo>,
    ) -> Result<()> {
        let mut coordinator = self.coordinator.lock().await;
        coordinator.refresh(work).await?;
        let operation = coordinator
            .observe_operation(
                &self.owner,
                &self.owner_scope,
                &self.verifier,
                OperationId::from_bytes(task_id.into_bytes()),
                work,
            )?
            .clone();
        crate::scheduler::require_execution_owner(&operation, fence, settlement, work)
    }

    /// Binds journal publications to the authenticated task's exact current lease.
    #[cfg(feature = "filesystem")]
    pub async fn journal_owner(
        self: &Arc<Self>,
        task_id: TaskId,
        fence: crate::scheduler::LeaseFence,
        io: Option<&crate::filesystem::PreparationIo>,
    ) -> Result<TaskJournalOwner<P>> {
        let mut borrowed = io.map(crate::filesystem::PreparationIo::history_io);
        let mut work = borrowed.as_mut().map(|work| work as &mut dyn crate::store::HistoryIo);
        let admission = self
            .admission(OperationId::from_bytes(task_id.into_bytes()), io)
            .await?;
        let machine = self
            .machines
            .resolve(&admission.machine, &mut work)?
            .ok_or_else(|| Error::Unsupported("pinned task machine is unavailable".into()))?;
        #[cfg(not(target_arch = "wasm32"))]
        let initial_state = match io.and_then(crate::filesystem::PreparationIo::original_native_work) {
            Some(original) => {
                let (state, custody) = self.machines.initialize_with_native_work(&admission.machine, &admission.input, original)?;
                io.expect("native observation").absorb_native_custody(custody)?;
                state
            }
            None => machine.initialize(&admission.input)?,
        };
        #[cfg(target_arch = "wasm32")]
        let initial_state = machine.initialize(&admission.input)?;
        let workflow = crate::workflow::WorkflowAdmission {
            operation_id: admission.operation_id,
            request_digest: crate::executor::json::canonical_json_digest(
                &admission,
                acyclic_stream::MAX_RECORD_BYTES as u64,
                &mut work,
            )?,
            initial: crate::workflow::MachineCheckpoint {
                machine: crate::workflow::workflow_copy_identity(&admission.machine, io)?,
                revision: 0,
                state: initial_state,
            },
        };
        self.machines.validate_checkpoint(&workflow.initial, io)?;
        if let Some(io) = io {
            io.retain_owned((std::mem::size_of::<TaskJournalOwner<P>>() + 2 * std::mem::size_of::<usize>()) as u64)?;
        }
        let owner = TaskJournalOwner {
            host: self.clone(),
            fence,
            workflow,
            workflow_output: None,
            admission: Arc::new(admission),
            invocation_stop: io.map(|io| io.cancellation().clone()),
            #[cfg(not(target_arch = "wasm32"))]
            native_custody: io.filter(|io| io.original_native_work().is_some())
                .map(|io| io.take_native_custody(io.owned_bytes()?)).transpose()?,
        };
        owner.verify(true, &mut work).await?;
        Ok(owner)
    }

    pub(super) async fn publish_control(
        &self,
        stream: &acyclic_stream::Stream<P>,
        kind: &str,
        task_id: TaskId,
        operation_id: OperationId,
        bytes: &[u8],
    ) -> Result<()> {
        if self
            .publish_control_at(stream, kind, task_id, operation_id, bytes, None)
            .await?
        {
            Ok(())
        } else {
            Err(Error::Conflict(
                "unconditional control append conflicted".into(),
            ))
        }
    }

    pub(super) async fn publish_control_at(
        &self,
        stream: &acyclic_stream::Stream<P>,
        kind: &str,
        task_id: TaskId,
        operation_id: OperationId,
        bytes: &[u8],
        expected_tail: Option<u64>,
    ) -> Result<bool> {
        let key = Self::event_key(kind, task_id, operation_id)?;
        let key = if let Some(tail) = expected_tail {
            let mut digest = blake3::Hasher::new();
            digest.update(b"harness/v2/control-tail\0");
            digest.update(key.as_bytes());
            digest.update(&tail.to_le_bytes());
            StreamKey::new(OwnedBytes::copy_from_slice(digest.finalize().as_bytes()))?
        } else {
            key
        };
        let outcome = crate::distributed::append(
            (stream, &self.stream),
            [OwnedBytes::copy_from_slice(bytes)],
            0,
            expected_tail,
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
                Ok(true)
            }
            AppendOutcome::Committed(_) => {
                Err(Error::Storage("invalid control append receipt".into()))
            }
            AppendOutcome::TailConflict { .. } => Ok(false),
        }
    }
}
