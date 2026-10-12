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
            .pull_owned(&self.owner, &self.owner_scope, &self.verifier, worker)
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
            .pull_owned_operation(
                &self.owner,
                &self.owner_scope,
                &self.verifier,
                worker,
                operation_id,
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
        let operation = coordinator
            .observe_operation(
                &self.owner,
                &self.owner_scope,
                &self.verifier,
                operation_id,
                &mut None,
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
            return crate::scheduler::require_execution_owner(&operation, &fence, false);
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
                worker_key("start", operation_id, &fence, operation.revision)?,
                event,
                &mut None,
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
        fence: crate::scheduler::LeaseFence,
        outcome: Outcome<Value>,
    ) -> Result<()> {
        let operation_id = OperationId::from_bytes(task.into_bytes());
        let admission = self.admission(operation_id).await?;
        self.verify_owner(task, &fence, true).await?;
        let digest = crate::contract::canonical_json_digest(
            &outcome,
            acyclic_stream::MAX_RECORD_BYTES as u64,
            &mut None,
        )?;
        let key = IdempotencyKey::new(format!(
            "task-settle:{operation_id}:{}:{}",
            blake3::Hash::from_bytes(crate::contract::canonical_json_digest(
                &fence,
                acyclic_stream::MAX_RECORD_BYTES as u64,
                &mut None
            )?)
            .to_hex(),
            blake3::Hash::from_bytes(digest).to_hex()
        ))?;
        let outcome = match outcome {
            Outcome::Succeeded(value) => {
                crate::contract::validate_json_schema_value(
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
                        .stage(operation_id, key.as_str(), &bytes)
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
        let operation = coordinator
            .observe_operation(
                &self.owner,
                &self.owner_scope,
                &self.verifier,
                OperationId::from_bytes(task_id.into_bytes()),
                &mut None,
            )?
            .clone();
        crate::scheduler::require_execution_owner(&operation, fence, settlement)
    }

    /// Binds journal publications to the authenticated task's exact current lease.
    #[cfg(feature = "filesystem")]
    pub async fn journal_owner(
        self: &Arc<Self>,
        task_id: TaskId,
        fence: crate::scheduler::LeaseFence,
    ) -> Result<TaskJournalOwner<P>> {
        let admission = self
            .admission(OperationId::from_bytes(task_id.into_bytes()))
            .await?;
        let machine = self
            .machines
            .resolve(&admission.machine)
            .ok_or_else(|| Error::Unsupported("pinned task machine is unavailable".into()))?;
        let workflow = crate::workflow::WorkflowAdmission {
            operation_id: admission.operation_id,
            request_digest: crate::contract::canonical_json_digest(
                &admission,
                acyclic_stream::MAX_RECORD_BYTES as u64,
                &mut None,
            )?,
            initial: crate::workflow::MachineCheckpoint {
                machine: admission.machine.clone(),
                revision: 0,
                state: machine.initialize(&admission.input)?,
            },
        };
        self.machines.validate_checkpoint(&workflow.initial, None)?;
        let stream = self.stream.clone();
        let owner = TaskJournalOwner {
            host: self.clone(),
            stream,
            task_id,
            fence,
            workflow,
            maximum_payload_bytes: admission.limits.file_bytes,
            output_schema: admission.output_schema,
            input_grants: admission.grants,
            input_limits: admission.limits,
            extensions: admission.extensions,
            extension_schemas: self.root_scope.extension_schema_registry(),
            extension_runtime: self.root_scope.extension_runtime(),
        };
        owner.verify(true, &mut None).await?;
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
        let outcome = crate::distributed::append_keyed(
            (stream, &self.stream),
            OwnedBytes::copy_from_slice(bytes),
            expected_tail,
            key,
            operation_id,
            "control identity",
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
