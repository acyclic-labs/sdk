use super::*;

impl<P, A, O> FilesystemTaskRuntime<P, A, O>
where
    P: StreamProvider + 'static,
    A: AsyncAuthorityStore + 'static,
    O: AsyncObjectStore + 'static,
{
    /// Reopens the admitted registered task and its bound workflow without requiring a typed definition at the worker boundary.
    pub async fn open_task(&self, task: TaskId, fence: LeaseFence) -> Result<ResumableTaskSession> {
        crate::conversation::BoundedContentFuture::new(None, || async move {
            let journal = self.workflow_journal(task, fence.clone()).await?;
            self.harness
                .open_registered_task(
                    task,
                    &fence,
                    journal,
                    #[cfg(not(target_arch = "wasm32"))]
                    None,
                )
                .await
        })?
        .await
    }

    /// Opens the original registered task using its one issued shared context.
    #[cfg(not(target_arch = "wasm32"))]
    pub async fn open_task_with_native_work(
        &self,
        task: TaskId,
        fence: &LeaseFence,
        context: Arc<crate::runtime::TaskContext>,
        work: &acyclic_fs::NativeMaterializationWork,
    ) -> Result<ResumableTaskSession> {
        if !context.original_task_binding_matches(task, fence) {
            return Err(Error::NativeBoundary {
                code: crate::wire::ErrorCode::Unauthorized,
                reason: "native task context differs from original task lease",
            });
        }
        crate::NativeFutureBundle::new(work, || async move {
            let owner = context.journal_owner::<P>(fence, work)?;
            let journal = FilesystemWorkflowJournal::for_task(
                Arc::clone(&owner),
                Arc::clone(&self.filesystem),
                crate::filesystem::FilesystemJournalBinding {
                    volume: &self.volume,
                    verifier: &self.verifier,
                    scope: &self.signed,
                    maximum_bytes: self
                        .maximum_payload_bytes
                        .min(owner.input_limits().file_bytes),
                    io: Some(&super::super::PreparationIo::from_native_work(work)?),
                },
            )?;
            self.harness
                .open_registered_task(task, fence, journal, Some(context))
                .await
        })?
        .await
    }

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
        let pulled = self.host.pull_work(worker, None, &mut None).await?;
        let work = self
            .resume_pulled(pulled, commands, maximum_transitions, None)
            .await;
        Ok(TaskWorkerTick { wake, work })
    }

    /// Polls and claims only the original admitted task, then uses the same fenced worker execution path.
    pub async fn run_operation(
        &self,
        worker: &crate::distributed::Worker,
        task: TaskId,
        commands: &dyn TaskCommandHost,
        maximum_transitions: u32,
        io: Option<&super::super::PreparationIo>,
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
        let pulled = {
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
                self.host.outcome(task).await?.is_some_and(|outcome| {
                    matches!(
                        outcome,
                        Outcome::Succeeded(_) | Outcome::Failed { .. } | Outcome::Cancelled
                    )
                })
            };
            if terminal {
                return Ok(None);
            }
            deadline::run(
                &self.host,
                task,
                None,
                admission.run_limits.deadline_epoch_ms,
                admission.operation_id,
                || async {
                    self.poll_task_wake(task, io).await?;
                    #[cfg(not(target_arch = "wasm32"))]
                    if let Some(io) = io {
                        let work = io.original_native_work().ok_or(Error::NativeBoundary {
                            code: crate::wire::ErrorCode::Unsupported,
                            reason: "worker pull requires original native work",
                        })?;
                        let mut operation_io =
                            crate::filesystem::PreparationIo::from_native_work(work)
                                .map(crate::store::HistoryWork::new)?;
                        crate::store::HistoryIo::retain_owned(
                            &mut operation_io,
                            std::mem::size_of::<crate::distributed::WorkPull>()
                                .max(std::mem::size_of::<crate::filesystem::TaskWorkerAttempt>())
                                as u64,
                        )?;
                        let pull = self
                            .host
                            .pull_work(
                                worker,
                                Some(admission.operation_id),
                                &mut Some(&mut operation_io),
                            )
                            .await?;
                        io.absorb_native_custody(operation_io.into_custody()?)?;
                        return Ok(pull);
                    }
                    self.host
                        .pull_work(worker, Some(admission.operation_id), &mut None)
                        .await
                },
                io,
                observed::pull_has_native_failure,
            )
            .await?
        };

        Ok(self
            .resume_pulled(pulled, commands, maximum_transitions, io)
            .await)
    }

    fn resume_pulled<'a>(
        &'a self,
        pulled: crate::distributed::WorkPull,
        commands: &'a dyn TaskCommandHost,
        maximum_transitions: u32,
        io: Option<&'a super::super::PreparationIo>,
    ) -> impl std::future::Future<Output = Option<TaskWorkerAttempt>> + 'a {
        use futures::{FutureExt as _, future::Either};
        // Select before constructing the state machine so the pull result does
        // not remain live alongside the resumed task across its awaits.
        match pulled {
            crate::distributed::WorkPull::Claimed(lease) => Either::Left(
                self.resume_task(lease, commands, maximum_transitions, io)
                    .map(Some),
            ),
            crate::distributed::WorkPull::Idle => Either::Right(std::future::ready(None)),
            crate::distributed::WorkPull::Unresolved { lease, error } => {
                Either::Right(std::future::ready(Some(TaskWorkerAttempt::Unresolved {
                    lease,
                    error,
                })))
            }
        }
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
        let io = super::super::PreparationIo::from_native_custody(recovery_custody);
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
        io: Option<&super::super::PreparationIo>,
    ) -> TaskWorkerAttempt {
        let result = match crate::conversation::BoundedContentFuture::new(io, || {
            self.run_task_observed(&lease, commands, maximum_transitions, io)
        }) {
            Ok(future) => future.await,
            Err(error) => Err(error),
        };
        // This rejection is returned only after a model stream has been dropped
        // or before a tool dispatch. Indeterminate native work retains its lease.
        if matches!(
            &result,
            Err(Error::InteractionRejected(
                crate::InteractionRejection::Cancelled
            ))
        ) {
            let task = TaskId::from_bytes(lease.operation.operation_id.into_bytes());
            let fence = LeaseFence::from(&lease.reservation);
            // Settlement is the cold path: its own bounded allocation keeps
            // the coordinator settlement state out of every resume future.
            let settled = match crate::conversation::BoundedContentFuture::new(io, || {
                self.settle_cancelled(task, &fence, io)
            }) {
                Ok(future) => future.await,
                Err(error) => Err(error),
            };
            return match settled {
                Ok(()) => {
                    TaskWorkerAttempt::Progress(WorkerTurn::Completed { task }.with_lease(lease))
                }
                Err(error) => TaskWorkerAttempt::Unresolved { lease, error },
            };
        }
        match result {
            Ok(progress) => TaskWorkerAttempt::Progress(progress.with_lease(lease)),
            Err(error) => TaskWorkerAttempt::Unresolved { lease, error },
        }
    }

    async fn settle_cancelled(
        &self,
        task: TaskId,
        fence: &LeaseFence,
        io: Option<&super::super::PreparationIo>,
    ) -> Result<()> {
        self.host.cancel(task).await?;
        self.host
            .settle_task(
                task,
                fence,
                Outcome::Cancelled,
                &mut observed::history(io)?
                    .as_mut()
                    .map(|work| work as &mut dyn crate::store::HistoryIo),
            )
            .await
    }
}
