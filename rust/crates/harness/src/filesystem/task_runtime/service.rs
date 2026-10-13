//! Service for the filesystem task runtime.

use super::*;

impl<P, A, O> FilesystemTaskRuntime<P, A, O>
where
    P: StreamProvider + 'static,
    A: AsyncAuthorityStore + 'static,
    O: AsyncObjectStore + 'static,
{
    /// Performs one bounded discovery page followed by at most one owner-scoped admission and worker turn through the trusted command adapter.
    pub async fn worker_tick(
        &self,
        worker: &crate::distributed::Worker,
        cursor: Option<TaskWakeCursor>,
        commands: &dyn TaskCommandHost,
        maximum_events: u32,
        maximum_transitions: u32,
    ) -> Result<TaskWorkerTick> {
        if worker.id.trim().is_empty() || maximum_transitions == 0 {
            return Err(Error::Invalid(
                "invalid worker tick allowance or identity".into(),
            ));
        }
        let wake = self.poll_task_wake_page(cursor, maximum_events).await?;
        let work = match self.host.pull_work(worker).await? {
            crate::distributed::WorkPull::Idle => None,
            crate::distributed::WorkPull::Claimed(lease) => Some(
                self.resume_task(lease, commands, maximum_transitions, None)
                    .await,
            ),
            crate::distributed::WorkPull::Unresolved { lease, error } => {
                Some(TaskWorkerAttempt::Unresolved { lease, error })
            }
        };
        Ok(TaskWorkerTick { wake, work })
    }

    /// Polls and claims only the original admitted task, then uses the same fenced worker execution path.
    pub async fn run_operation(
        &self,
        worker: &crate::distributed::Worker,
        task: TaskId,
        commands: &dyn TaskCommandHost,
        maximum_transitions: u32,
        io: Option<&crate::filesystem::PreparationIo>,
    ) -> Result<Option<TaskWorkerAttempt>> {
        if let Some(io) = io {
            io.check_cancellation()?;
        }
        if worker.id.trim().is_empty() || maximum_transitions == 0 {
            #[cfg(not(target_arch = "wasm32"))]
            if io.is_some() {
                return Err(Error::NativeBoundary {
                    code: crate::wire::ErrorCode::Invalid,
                    reason: "invalid targeted worker allowance or identity",
                });
            }
            return Err(Error::Invalid(
                "invalid targeted worker allowance or identity".into(),
            ));
        }
        let admission = self.admission_observed(task, io).await?;
        let terminal = if io.is_some() {
            self.outcome_observed(task, io)
                .await?
                .is_some_and(|outcome| {
                    matches!(
                        outcome,
                        Outcome::Succeeded(_) | Outcome::Failed { .. } | Outcome::Cancelled
                    )
                })
        } else {
            self.host
                .observe_task(task)
                .await?
                .outcome
                .is_some_and(|outcome| {
                    matches!(
                        outcome,
                        Outcome::Succeeded(_) | Outcome::Failed { .. } | Outcome::Cancelled
                    )
                })
        };
        if terminal {
            return Ok(None);
        }
        let pulled = deadline::run(
            &self.host,
            task,
            None,
            admission.run_limits.deadline_epoch_ms,
            admission.operation_id,
            async {
                self.poll_task_wake(task, io).await?;
                #[cfg(not(target_arch = "wasm32"))]
                if let Some(io) = io {
                    let work = io.original_native_work().ok_or(Error::NativeBoundary {
                        code: crate::wire::ErrorCode::Unsupported,
                        reason: "worker pull requires original native work",
                    })?;
                    let mut operation_io = crate::store::HistoryWork::new(
                        crate::filesystem::PreparationIo::from_native_work(work)?,
                    );
                    crate::store::HistoryIo::retain_owned(
                        &mut operation_io,
                        std::mem::size_of::<crate::distributed::WorkPull>()
                            .max(std::mem::size_of::<crate::filesystem::TaskWorkerAttempt>())
                            as u64,
                    )?;
                    let pull = self
                        .host
                        .pull_operation(
                            worker,
                            admission.operation_id,
                            &mut Some(&mut operation_io),
                        )
                        .await?;
                    io.absorb_native_custody(operation_io.into_custody()?)?;
                    return Ok(pull);
                }
                self.host
                    .pull_operation(worker, admission.operation_id, &mut None)
                    .await
            },
            io,
            observed::pull_has_native_failure,
        )
        .await?;
        Ok(match pulled {
            crate::distributed::WorkPull::Idle => None,
            crate::distributed::WorkPull::Claimed(lease) => Some(
                self.resume_task(lease, commands, maximum_transitions, io)
                    .await,
            ),
            crate::distributed::WorkPull::Unresolved { lease, error } => {
                Some(TaskWorkerAttempt::Unresolved { lease, error })
            }
        })
    }

    /// Resumes only this original lease and returns it unchanged on failure.
    #[cfg(not(target_arch = "wasm32"))]
    pub async fn resume_task_with_native_work(
        &self,
        lease: crate::distributed::WorkLease,
        recovery_custody: acyclic_fs::NativeAllocation,
        commands: &dyn TaskCommandHost,
        maximum_transitions: u32,
        work: &acyclic_fs::NativeMaterializationWork,
    ) -> Result<(TaskWorkerAttempt, acyclic_fs::NativeAllocation)> {
        if !work.shares_ledger(recovery_custody.work()) {
            return Ok((
                TaskWorkerAttempt::Unresolved {
                    lease,
                    error: Error::NativeBoundary {
                        code: crate::wire::ErrorCode::Unauthorized,
                        reason: "recovered lease custody differs from original work",
                    },
                },
                recovery_custody,
            ));
        }
        let io = crate::filesystem::PreparationIo::from_native_custody(recovery_custody);
        let value = self
            .resume_task(lease, commands, maximum_transitions, Some(&io))
            .await;
        let custody = io.take_native_custody(io.owned_bytes()?)?;
        Ok((value, custody))
    }

    /// Resumes an explicit lease with optional original preparation IO.
    pub async fn resume_task(
        &self,
        lease: crate::distributed::WorkLease,
        commands: &dyn TaskCommandHost,
        maximum_transitions: u32,
        io: Option<&crate::filesystem::PreparationIo>,
    ) -> TaskWorkerAttempt {
        match self
            .run_task_observed(&lease, commands, maximum_transitions, io)
            .await
        {
            Ok(progress) => TaskWorkerAttempt::Progress(progress.with_lease(lease)),
            Err(error) => TaskWorkerAttempt::Unresolved { lease, error },
        }
    }
}
