//! Recovery.
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
                self.verifier.audience(),
                &self.owner_scope,
                &self.verifier,
                worker,
                None,
                &mut None,
            )
            .await
    }

    /// Claims this original operation under the signed owner.
    pub async fn pull_operation(
        &self,
        worker: &crate::distributed::Worker,
        operation_id: OperationId,
        work: &mut Option<&mut dyn crate::store::history::HistoryIo>,
    ) -> Result<crate::distributed::WorkPull> {
        self.coordinator
            .lock()
            .await
            .pull_owned(
                self.verifier.audience(),
                &self.owner_scope,
                &self.verifier,
                worker,
                Some(operation_id),
                work,
            )
            .await
    }

    /// Recovers the original reservation after a lost pull response.
    pub async fn recover_work(
        &self,
        task: TaskId,
        work: &mut Option<&mut dyn crate::store::history::HistoryIo>,
    ) -> Result<crate::distributed::WorkPull> {
        use crate::distributed::WorkPull;
        use crate::scheduler::OperationPhase;

        let operation_id = OperationId::from_bytes(task.into_bytes());
        let admission = self.admission_with_work(operation_id, work).await?;
        let mut coordinator = self.coordinator.lock().await;
        coordinator.refresh(work).await?;
        let operation = coordinator.observe_operation(
            self.verifier.audience(),
            &self.owner_scope,
            &self.verifier,
            operation_id,
            work,
        )?;
        let Some(lease) = operation.work_lease_with_work(work)? else {
            return Ok(WorkPull::Idle);
        };
        drop(admission);
        if operation.cancellation_requested
            || !matches!(
                operation.phase,
                OperationPhase::Admitted | OperationPhase::Running | OperationPhase::Reconciling
            )
        {
            return Ok(WorkPull::Unresolved {
                lease,
                error: guarded_boundary(
                    work,
                    crate::wire::ErrorCode::Conflict,
                    "retained task reservation requires reconciliation",
                ),
            });
        }
        Ok(WorkPull::Claimed(lease))
    }

    /// Starts or reattaches the exact already-admitted lease.
    pub async fn start_task(
        &self,
        lease: &crate::distributed::WorkLease,
        native: crate::NativeWork<'_>,
    ) -> Result<()> {
        #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
        let mut io = native
            .map(|work| {
                crate::filesystem::PreparationIo::from_native_work(work)
                    .map(crate::store::HistoryWork::new)
            })
            .transpose()?;
        #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
        let mut work = io
            .as_mut()
            .map(|io| io as &mut dyn crate::store::history::HistoryIo);
        #[cfg(not(all(feature = "filesystem", not(target_arch = "wasm32"))))]
        let mut work: Option<&mut dyn crate::store::history::HistoryIo> = None;
        let _ = native;
        let operation_id = lease.operation.operation_id;
        let admission = self.admission_with_work(operation_id, &mut work).await?;
        let mut coordinator = self.coordinator.lock().await;
        coordinator.refresh(&mut work).await?;
        let operation = coordinator.observe_operation(
            self.verifier.audience(),
            &self.owner_scope,
            &self.verifier,
            operation_id,
            &mut work,
        )?;
        if operation.spec != lease.operation
            || operation.reservation.as_ref() != Some(&lease.reservation)
        {
            return Err(guarded_boundary(
                &mut work,
                crate::wire::ErrorCode::Conflict,
                "worker lease differs from retained admission",
            ));
        }
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
        if operation.phase == crate::scheduler::OperationPhase::Running {
            return crate::scheduler::require_execution_owner(operation, &fence, false, &mut None);
        }
        let key =
            worker_key_with_work("start", operation_id, &fence, operation.revision, &mut work)?;
        let event = if operation.phase == crate::scheduler::OperationPhase::Reconciling {
            crate::scheduler::SchedulerEvent::ReconciliationResumed {
                operation_id,
                fence,
            }
        } else {
            crate::scheduler::SchedulerEvent::Started {
                operation_id,
                fence,
            }
        };
        coordinator.apply(key, event, &mut work).await?;
        drop(admission);
        Ok(())
    }

    pub(crate) async fn settle_task(
        &self,
        task: TaskId,
        fence: &crate::scheduler::LeaseFence,
        outcome: Outcome<Value>,
        work: &mut Option<&mut dyn crate::store::history::HistoryIo>,
    ) -> Result<()> {
        let fence = fence.clone_with_work(work)?;
        let operation_id = OperationId::from_bytes(task.into_bytes());
        let admission = self.admission_with_work(operation_id, work).await?;
        let settlement = matches!(&outcome, Outcome::Cancelled | Outcome::Indeterminate { .. });
        self.verify_owner(task, &fence, settlement, work).await?;
        let maximum = if matches!(&outcome, Outcome::Succeeded(_)) {
            admission.limits.file_bytes.saturating_add(14)
        } else {
            acyclic_stream::MAX_RECORD_BYTES as u64
        };
        let digest = crate::contract::canonical_json_digest_with_work(&outcome, maximum, work)?;
        let fence_digest = crate::contract::canonical_json_digest_with_work(
            &fence,
            acyclic_stream::MAX_RECORD_BYTES as u64,
            work,
        )?;
        let mut text = String::new();
        if let Some(io) = work.as_deref_mut() {
            io.admit_copy(178)?;
            io.reserve_text(&mut text, 178)?;
            io.copy_bytes(178)?;
        }
        use std::fmt::Write as _;
        write!(
            &mut text,
            "task-settle:{operation_id}:{}:{}",
            blake3::Hash::from_bytes(fence_digest).to_hex(),
            blake3::Hash::from_bytes(digest).to_hex()
        )
        .expect("writing into String");
        let key = IdempotencyKey::new(text)?;
        let outcome = match outcome {
            Outcome::Succeeded(value) => {
                crate::contract::validate_json_schema_value(
                    &admission.output_schema,
                    &value,
                    "task output",
                    work,
                )?;
                let bytes = crate::store::history::canonical_bytes_with_work(
                    &value,
                    admission.limits.file_bytes,
                    work,
                )?;
                if bytes.len() as u64 > admission.limits.file_bytes {
                    return Err(guarded_boundary(
                        work,
                        crate::wire::ErrorCode::Invalid,
                        "task result exceeds admitted file limit",
                    ));
                }
                let staged = self
                    .stage_payload_with_work(operation_id, key.as_str(), &bytes, work)
                    .await;
                let capacity = bytes.capacity() as u64;
                drop(bytes);
                let release = match work.as_deref_mut() {
                    Some(io) => io.release_owned(capacity),
                    None => Ok(()),
                };
                let reference = staged?;
                release?;
                Outcome::Succeeded(reference)
            }
            Outcome::Failed { message } => Outcome::Failed { message },
            Outcome::Cancelled => Outcome::Cancelled,
            Outcome::Indeterminate { operation_id } => Outcome::Indeterminate { operation_id },
        };
        let mut coordinator = self.coordinator.lock().await;
        coordinator.refresh(work).await?;
        if let Some(io) = work.as_deref_mut() {
            io.check_cancellation()?;
        }
        if !settlement
            && admission
                .run_limits
                .deadline_epoch_ms
                .is_some_and(|deadline| self.clock.now_unix_millis() >= deadline)
        {
            let mut text = String::new();
            if let Some(io) = work.as_deref_mut() {
                io.reserve_text(&mut text, 48)?;
            }
            write!(&mut text, "task-cancel:{operation_id}").expect("writing into String");
            coordinator
                .cancel_operation(
                    self.verifier.audience(),
                    &self.owner_scope,
                    &self.verifier,
                    operation_id,
                    IdempotencyKey::new(text)?,
                    true,
                    None,
                    work,
                )
                .await?;
            return Err(Error::Indeterminate(operation_id));
        }
        let current = coordinator.observe_operation(
            self.verifier.audience(),
            &self.owner_scope,
            &self.verifier,
            operation_id,
            work,
        )?;
        crate::scheduler::require_execution_owner(current, &fence, settlement, &mut None)?;
        coordinator
            .apply(
                key,
                crate::scheduler::SchedulerEvent::Completed {
                    operation_id,
                    outcome,
                    fence: Some(fence),
                    execution_duration_ns: None,
                },
                work,
            )
            .await?;
        Ok(())
    }

    pub(super) async fn verify_owner(
        &self,
        task_id: TaskId,
        fence: &crate::scheduler::LeaseFence,
        settlement: bool,
        work: &mut Option<&mut dyn crate::store::history::HistoryIo>,
    ) -> Result<()> {
        if !settlement {
            let operation = OperationId::from_bytes(task_id.into_bytes());
            let admitted = self
                .read_retained_admission_counted(operation, work)
                .await
                .map(|(record, _)| record)?;
            if let Some(io) = work.as_deref_mut() {
                io.check_cancellation()?;
            }
            if admitted
                .run_limits
                .deadline_epoch_ms
                .is_some_and(|deadline| self.clock.now_unix_millis() >= deadline)
            {
                self.expire_task_deadline(task_id, Some(fence), work)
                    .await?;
                return Err(Error::Indeterminate(operation));
            }
        }
        let mut coordinator = self.coordinator.lock().await;
        coordinator.refresh(work).await?;
        let operation = coordinator.observe_operation(
            self.verifier.audience(),
            &self.owner_scope,
            &self.verifier,
            OperationId::from_bytes(task_id.into_bytes()),
            work,
        )?;
        crate::scheduler::require_execution_owner(operation, fence, settlement, &mut None)
    }

    /// Reads the configured owner's exact current operation without granting dispatch, publication or another reservation.
    pub async fn observe_task(&self, task: TaskId) -> Result<crate::scheduler::OperationState> {
        let mut coordinator = self.coordinator.lock().await;
        coordinator.refresh(&mut None).await?;
        coordinator
            .observe_operation(
                self.verifier.audience(),
                &self.owner_scope,
                &self.verifier,
                OperationId::from_bytes(task.into_bytes()),
                &mut None,
            )
            .cloned()
    }

    /// Reads the original owner's current cancellation flag.
    pub async fn cancellation_requested(
        &self,
        task: TaskId,
        native: crate::NativeWork<'_>,
    ) -> Result<bool> {
        #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
        let mut io = native
            .map(|work| {
                crate::filesystem::PreparationIo::from_native_work(work)
                    .map(crate::store::HistoryWork::new)
            })
            .transpose()?;
        #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
        let mut work = io
            .as_mut()
            .map(|io| io as &mut dyn crate::store::history::HistoryIo);
        #[cfg(not(all(feature = "filesystem", not(target_arch = "wasm32"))))]
        let mut work: Option<&mut dyn crate::store::history::HistoryIo> = None;
        let _ = native;
        let mut coordinator = self.coordinator.lock().await;
        coordinator.refresh(&mut work).await?;
        let operation = coordinator.observe_operation(
            self.verifier.audience(),
            &self.owner_scope,
            &self.verifier,
            OperationId::from_bytes(task.into_bytes()),
            &mut work,
        )?;
        Ok(operation.cancellation_requested)
    }

    /// Cancels when the original admitted deadline has elapsed.
    pub async fn expire_task_deadline(
        &self,
        task: TaskId,
        fence: Option<&crate::scheduler::LeaseFence>,
        work: &mut Option<&mut dyn crate::store::history::HistoryIo>,
    ) -> Result<()> {
        let operation = OperationId::from_bytes(task.into_bytes());
        let admission = self
            .read_retained_admission_counted(operation, work)
            .await
            .map(|(record, _)| record)?;
        let deadline = admission.run_limits.deadline_epoch_ms.ok_or_else(|| {
            guarded_boundary(
                work,
                crate::wire::ErrorCode::Invalid,
                "task has no admitted deadline",
            )
        })?;
        if let Some(io) = work.as_deref_mut() {
            io.check_cancellation()?;
        }
        if self.clock.now_unix_millis() < deadline {
            return Err(guarded_boundary(
                work,
                crate::wire::ErrorCode::Conflict,
                "task deadline has not elapsed",
            ));
        }
        let mut coordinator = self.coordinator.lock().await;
        coordinator.refresh(work).await?;
        let state = coordinator.observe_operation(
            self.verifier.audience(),
            &self.owner_scope,
            &self.verifier,
            operation,
            work,
        )?;
        if state
            .outcome
            .as_ref()
            .is_some_and(|outcome| !matches!(outcome, Outcome::Indeterminate { .. }))
        {
            return Ok(());
        }
        if let Some(fence) = fence {
            if state.reservation.as_ref().is_none_or(|reservation| {
                reservation.id != fence.reservation_id || reservation.placement != fence.placement
            }) {
                return Err(guarded_boundary(
                    work,
                    crate::wire::ErrorCode::Conflict,
                    "deadline observer does not own the current reservation",
                ));
            }
        }
        if state.cancellation_requested {
            return Ok(());
        }
        let mut text = String::new();
        if let Some(io) = work.as_deref_mut() {
            io.admit_copy(48)?;
            io.reserve_text(&mut text, 48)?;
            io.copy_bytes(48)?;
        }
        use std::fmt::Write as _;
        write!(&mut text, "task-cancel:{operation}").expect("writing into String");
        coordinator
            .cancel_operation(
                self.verifier.audience(),
                &self.owner_scope,
                &self.verifier,
                operation,
                IdempotencyKey::new(text)?,
                true,
                None,
                work,
            )
            .await?;
        Ok(())
    }
}
