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
                &mut None,
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
        self.admission(operation_id).await?;
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
    pub async fn start_task(&self, lease: &crate::distributed::WorkLease) -> Result<()> {
        let operation_id = lease.operation.operation_id;
        let fence = crate::scheduler::LeaseFence::from(&lease.reservation);
        self.admission(operation_id).await?;
        let mut coordinator = self.coordinator.lock().await;
        coordinator.refresh(&mut None).await?;
        let operation = coordinator.observe_operation(
            &self.owner,
            &self.owner_scope,
            &self.verifier,
            operation_id,
            &mut None,
        )?;
        if operation.spec != lease.operation
            || operation.reservation.as_ref() != Some(&lease.reservation)
        {
            return Err(Error::Conflict(
                "worker lease differs from retained admission".into(),
            ));
        }
        if operation.phase == crate::scheduler::OperationPhase::Running {
            return crate::scheduler::require_execution_owner(operation, &fence, false, &mut None);
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
        let key = worker_key("start", operation_id, &fence, operation.revision, &mut None)?;
        coordinator.apply(key, event, &mut None).await?;
        Ok(())
    }

    /// Publishes a schema-checked result or retains uncertainty with this exact
    /// lease. A staged result can survive a publication conflict without making
    /// that conflict a successful task completion.
    pub async fn settle_task(
        &self,
        task: TaskId,
        fence: crate::scheduler::LeaseFence,
        outcome: Outcome<Value>,
    ) -> Result<()> {
        let operation_id = OperationId::from_bytes(task.into_bytes());
        let admission = self.admission(operation_id).await?;
        self.verify_owner(task, &fence, true).await?;
        let digest = crate::executor::json::canonical_json_digest(
            &outcome,
            acyclic_stream::MAX_RECORD_BYTES as u64,
            &mut None,
        )?;
        let key = IdempotencyKey::new(format!(
            "task-settle:{operation_id}:{}:{}",
            blake3::Hash::from_bytes(crate::executor::json::canonical_json_digest(
                &fence,
                acyclic_stream::MAX_RECORD_BYTES as u64,
                &mut None
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
                    &mut None,
                )?;
                let bytes = crate::contract::canonical_json_bytes(&value)?;
                if bytes.len() as u64 > admission.limits.file_bytes {
                    return Err(Error::Invalid(
                        "task result exceeds admitted file limit".into(),
                    ));
                }
                Outcome::Succeeded(
                    self.payloads
                        .stage(operation_id, key.as_str(), &bytes, None)?
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
                    fence: Some(fence),
                    execution_duration_ns: None,
                },
                &mut None,
            )
            .await?;
        Ok(())
    }

    pub(super) async fn verify_owner(
        &self,
        task_id: TaskId,
        fence: &crate::scheduler::LeaseFence,
        settlement: bool,
    ) -> Result<()> {
        let mut coordinator = self.coordinator.lock().await;
        coordinator.refresh(&mut None).await?;
        let operation = coordinator.observe_operation(
            &self.owner,
            &self.owner_scope,
            &self.verifier,
            OperationId::from_bytes(task_id.into_bytes()),
            &mut None,
        )?;
        crate::scheduler::require_execution_owner(operation, fence, settlement, &mut None)
    }

    /// Binds journal publications to the authenticated task's exact current lease.
    #[cfg(feature = "filesystem")]
    pub async fn journal_owner(
        self: &Arc<Self>,
        task_id: TaskId,
        fence: &crate::scheduler::LeaseFence,
        io: Option<&crate::executor::PreparationIo>,
    ) -> Result<TaskJournalOwner<P>> {
        let operation = OperationId::from_bytes(task_id.into_bytes());
        #[cfg(not(target_arch = "wasm32"))]
        let (admission, mut custody) = match io {
            Some(io) if io.original_native_work().is_some() => {
                let (admission, custody) = self.readonly_admission(operation, io).await?;
                (admission, Some(custody))
            }
            _ => (self.admission(operation).await?, None),
        };
        #[cfg(target_arch = "wasm32")]
        let admission = self.admission(operation).await?;
        #[cfg(not(target_arch = "wasm32"))]
        if let Some(native) = io.and_then(|io| io.original_native_work()) {
            if admission.resource_proofs.is_empty() {
                self.root_scope.validate_narrowing_with_native_work(
                    &admission.grants,
                    &admission.limits,
                    native,
                )?;
            }
        }
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
        let request_digest = crate::contract::canonical_json_digest(
            &admission,
            acyclic_stream::MAX_RECORD_BYTES as u64,
            &mut work,
        )?;
        #[cfg(not(target_arch = "wasm32"))]
        let state = match io.and_then(|io| io.original_native_work()) {
            Some(native) => {
                let (state, mut held) = self.machines.initialize_with_native_work(
                    &admission.machine,
                    &admission.input,
                    native,
                )?;
                custody
                    .as_mut()
                    .expect("native admission custody")
                    .absorb(&mut held)?;
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
            io.retain_owned(std::mem::size_of::<TaskJournalOwner<P>>() as u64)?;
        }
        let fence = fence.clone_with_work(&mut work)?;
        #[cfg(not(target_arch = "wasm32"))]
        if let Some(custody) = custody.as_mut() {
            let retained = std::mem::size_of::<TaskJournalOwner<P>>() as u64
                + fence.reservation_id.capacity() as u64
                + fence.placement.capacity() as u64
                + workflow.initial.machine.name.capacity() as u64
                + workflow.initial.machine.version.capacity() as u64;
            custody.absorb(
                &mut work
                    .as_deref_mut()
                    .expect("native history")
                    .take_native_custody(retained)?,
            )?;
        }
        let owner = TaskJournalOwner {
            host: self.clone(),
            fence,
            workflow,
            admission: Arc::new(admission),
            input_agent: declared_agent,
            invocation_cancellation: None,
            root_binding: std::sync::OnceLock::new(),
            root_choice: std::sync::OnceLock::new(),
            root_gate: Mutex::new(()),
            #[cfg(not(target_arch = "wasm32"))]
            native_custody: custody,
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
