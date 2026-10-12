//! Observation of the existing outbox without advancing its task machine.

use super::{FilesystemTaskRuntime, TaskCommandHost, TaskCommandProgress, TaskWorkerAttempt, WorkerTurn, WorkerContext, deadline, observed};
use crate::{Error, Outcome, Result, TaskId, scheduler::LeaseFence, workflow::WorkflowJournal as _};
use acyclic_fs::{AsyncAuthorityStore, AsyncObjectStore};
use acyclic_stream::StreamProvider;

impl<P, A, O> FilesystemTaskRuntime<P, A, O>
where
    P: StreamProvider + 'static,
    A: AsyncAuthorityStore + 'static,
    O: AsyncObjectStore + 'static,
{
    #[cfg(not(target_arch = "wasm32"))]
    pub async fn reconcile_task_with_native_work(
        &self, lease: crate::distributed::WorkLease, recovery_custody: acyclic_fs::NativeAllocation,
        commands: &dyn TaskCommandHost,
        maximum_commands: u32, maximum_wait_millis: u32,
        work: &acyclic_fs::NativeMaterializationWork,
    ) -> Result<(TaskWorkerAttempt, acyclic_fs::NativeAllocation)> {
        if !work.shares_ledger(recovery_custody.work()) {
            return Ok((TaskWorkerAttempt::Unresolved {
                lease, error: Error::NativeBoundary {
                    code: crate::wire::ErrorCode::Unauthorized,
                    reason: "recovered lease custody differs from original work",
                },
            }, recovery_custody));
        }
        let io = super::super::PreparationIo::from_native_custody(recovery_custody);
        let value = self.reconcile_task(lease, commands, maximum_commands, maximum_wait_millis, Some(&io)).await;
        let custody = io.take_native_custody(io.owned_bytes()?)?;
        Ok((value, custody))
    }

    /// Reconciles the retained outbox under the exact lease and optional original IO.
    pub async fn reconcile_task(
        &self, lease: crate::distributed::WorkLease, commands: &dyn TaskCommandHost,
        maximum_commands: u32, maximum_wait_millis: u32,
        io: Option<&super::super::PreparationIo>,
    ) -> TaskWorkerAttempt {
        if maximum_commands == 0 || maximum_wait_millis == 0 {
            return TaskWorkerAttempt::Unresolved {
                lease, error: observed::boundary(io, crate::wire::ErrorCode::Invalid, "invalid reconciliation allowance"),
            };
        }
        if let Some(io) = io && let Err(error) = io.check_cancellation() {
            return TaskWorkerAttempt::Unresolved { lease, error };
        }
        match deadline::observe(
            maximum_wait_millis,
            async {
                let lease = &lease;
                let operation = lease.operation.operation_id;
                let task = TaskId::from_bytes(operation.into_bytes());
                let fence = observed::lease_fence(lease, io)?;
                self.owner_observed(task, &fence, true, io).await?;
                let admission = self.admission_observed(task, io).await?;
                if admission.run_limits.deadline_epoch_ms.is_some_and(|deadline| self.host.now_unix_millis() >= deadline) {
                    #[cfg(not(target_arch = "wasm32"))]
                    if let Some(io) = io {
                        let work = io.original_native_work().ok_or_else(|| observed::boundary(Some(io), crate::wire::ErrorCode::Unsupported, "native reconciliation requires original work"))?;
                        self.host.expire_task_deadline(task, Some(&fence), &mut Some(&mut crate::store::HistoryWork::new(
                                crate::filesystem::PreparationIo::from_native_work(work)?,
                            ))).await?;
                    } else {
                        self.host.expire_task_deadline(task, Some(&fence), &mut None).await?;
                    }
                    #[cfg(target_arch = "wasm32")]
                    self.host.expire_task_deadline(task, Some(&fence), &mut None).await?;
                }
                #[cfg(not(target_arch = "wasm32"))]
                let journal = if let Some(io) = io {
                    self.workflow_journal_with_native_work(task, &fence, io.original_native_work().ok_or_else(|| observed::boundary(Some(io), crate::wire::ErrorCode::Unsupported, "native reconciliation requires original work"))?).await?
                } else { self.workflow_journal(task, fence.clone()).await? };
                #[cfg(target_arch = "wasm32")]
                let journal = self.workflow_journal(task, fence.clone()).await?;
                #[cfg(not(target_arch = "wasm32"))]
                let present = if let Some(io) = io {
                    let work = io.original_native_work().ok_or_else(|| observed::boundary(Some(io), crate::wire::ErrorCode::Unsupported, "native journal requires original work"))?;
                    let admission = journal.admission_with_native_work(work)?.await?;
                    let present = admission.0.is_some();
                    drop(admission);
                    present
                } else { journal.admission().await?.is_some() };
                #[cfg(target_arch = "wasm32")]
                let present = journal.admission().await?.is_some();
                if !present { return self.finish_known_reconciliation(task, &fence, io).await; }
                let transition = journal.observe_latest_record(io).await?.map(|record| record.transition);
                let Some(transition) = transition else { return self.finish_known_reconciliation(task, &fence, io).await; };
                #[cfg(not(target_arch = "wasm32"))]
                let context = if let Some(io) = io {
                    let work = io.original_native_work().ok_or_else(|| observed::boundary(Some(io), crate::wire::ErrorCode::Unsupported, "native context requires original work"))?;
                    WorkerContext::Native(self.task_context_for_original_owner_with_native_work(task, &fence, true, work).await?)
                } else { WorkerContext::Ordinary(self.harness.durable_context(task, operation).await?) };
                #[cfg(target_arch = "wasm32")]
                let context = WorkerContext::Ordinary(self.harness.durable_context(task, operation).await?);
                for command in transition.commands.iter().take(maximum_commands as usize) {
                    self.owner_observed(task, &fence, true, io).await?;
                    #[cfg(not(target_arch = "wasm32"))]
                    let progress = if let Some(io) = io {
                        let work = io.original_native_work().ok_or_else(|| observed::boundary(Some(io), crate::wire::ErrorCode::Unsupported, "native command requires original work"))?;
                        let WorkerContext::Native(shared) = &context else { return Err(observed::boundary(Some(io), crate::wire::ErrorCode::Unsupported, "native command has no issued context")); };
                        if !shared.as_ref().original_task_binding_matches(task, &fence) {
                            return Err(observed::boundary(Some(io), crate::wire::ErrorCode::Unauthorized, "native reconciliation differs from original task binding"));
                        }
                        let body = shared.as_ref().read_file_with_native_work(&command.payload, work).await?;
                        let payload = crate::filesystem::retain_decoded(io.decode(std::marker::PhantomData::<serde_json::Value>, &body, crate::wire::ErrorCode::Storage)?);
                        drop(body);
                        commands.reconcile_with_native_work(
                            std::sync::Arc::clone(shared), observed::clone_fence(&fence, Some(io))?,
                            observed::clone_command(command, Some(io))?, payload, work,
                        )?.await?
                    } else {
                        let payload = crate::contract::json_from_slice(&context.context().read_file(&command.payload).await?)
                            .map_err(|error| Error::Invalid(format!("invalid command JSON: {error}")))?;
                        commands.reconcile(context.context().clone(), fence.clone(), command.clone(), payload).await?
                    };
                    #[cfg(target_arch = "wasm32")]
                    let progress = {
                        let payload = crate::contract::json_from_slice(&context.context().read_file(&command.payload).await?)
                            .map_err(|error| Error::Invalid(format!("invalid command JSON: {error}")))?;
                        commands.reconcile(context.context().clone(), fence.clone(), command.clone(), payload).await?
                    };
                    match &progress {
                        TaskCommandProgress::Ready(_) if io.is_some() => return Err(observed::boundary(io, crate::wire::ErrorCode::Unsupported, "native reconciliation returned an ordinary value")),
                        TaskCommandProgress::Ready(_) => {}
                        #[cfg(not(target_arch = "wasm32"))]
                        TaskCommandProgress::ReadyWithNativeCustody { custody, .. } => {
                            if let Some(io) = io {
                                let work = io.original_native_work().ok_or_else(|| observed::boundary(Some(io), crate::wire::ErrorCode::Unsupported, "native result requires original work"))?;
                                if !work.shares_ledger(custody.work()) {
                                    return Err(observed::boundary(Some(io), crate::wire::ErrorCode::Unauthorized, "native result differs from original work"));
                                }
                                self.host.verify_native_command_result(task, &fence, custody.work()).await?;
                            }
                        }
                        TaskCommandProgress::Pending | TaskCommandProgress::Indeterminate => return Ok(WorkerTurn::Reconciling),
                    }
                    drop(progress);
                }
                if transition.commands.len() > maximum_commands as usize { return Ok(WorkerTurn::Reconciling); }
                self.finish_known_reconciliation(task, &fence, io).await
            },
        ).await {
            Ok(Some(progress)) => TaskWorkerAttempt::Progress(progress.with_lease(lease)),
            Ok(None) => TaskWorkerAttempt::Progress(WorkerTurn::Reconciling.with_lease(lease)),
            Err(error) => TaskWorkerAttempt::Unresolved { lease, error },
        }
    }


    async fn finish_known_reconciliation(
        &self, task: TaskId, fence: &LeaseFence, io: Option<&super::super::PreparationIo>,
    ) -> Result<WorkerTurn> {
        self.owner_observed(task, fence, true, io).await?;
        #[cfg(not(target_arch = "wasm32"))]
        let cancelled = if let Some(io) = io {
            let work = io.original_native_work().ok_or_else(|| observed::boundary(Some(io), crate::wire::ErrorCode::Unsupported, "native cancellation requires original work"))?;
            self.host.cancellation_requested(task, Some(work)).await?
        } else { self.host.cancellation_requested(task, None).await? };
        #[cfg(target_arch = "wasm32")]
        let cancelled = self.host.cancellation_requested(task, None).await?;
        if cancelled {
            self.task_host().settle_task(task, fence, Outcome::Cancelled, &mut observed::history(io)?.as_mut().map(|work| work as &mut dyn crate::store::HistoryIo)).await?;
            Ok(WorkerTurn::Completed { task })
        } else { Ok(WorkerTurn::Reconciling) }
    }
}
