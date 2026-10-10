//! Observation of the existing outbox without advancing its task machine.

use super::{FilesystemTaskRuntime, TaskCommandHost, TaskCommandProgress, TaskWorkerAttempt, TaskWorkerOutcome, deadline};
use crate::{Error, Outcome, Result, TaskId, runtime::DurableTaskHost as _, scheduler::LeaseFence, workflow::WorkflowJournal as _};
use acyclic_fs::{AsyncAuthorityStore, AsyncObjectStore};
use acyclic_stream::StreamProvider;

impl<P, A, O> FilesystemTaskRuntime<P, A, O>
where
    P: StreamProvider + 'static,
    A: AsyncAuthorityStore + 'static,
    O: AsyncObjectStore + 'static,
{
    /// Reconciles only the original retained outbox under its exact lease. The
    /// wait allowance bounds observation, never extends the admitted deadline.
    /// No machine step, command dispatch, lease replacement or queue is created.
    pub async fn reconcile_task(
        &self,
        lease: crate::distributed::WorkLease,
        commands: &dyn TaskCommandHost,
        maximum_commands: u32,
        maximum_wait_millis: u32,
    ) -> TaskWorkerAttempt {
        if maximum_commands == 0 || maximum_wait_millis == 0 {
            return TaskWorkerAttempt::Unresolved {
                lease,
                error: Error::Invalid("invalid reconciliation allowance".into()),
            };
        }
        match deadline::observe(
            maximum_wait_millis,
            self.reconcile_task_owned(&lease, commands, maximum_commands),
        )
        .await
        {
            Ok(Some(progress)) => TaskWorkerAttempt::Progress(progress),
            Ok(None) => TaskWorkerAttempt::Progress(TaskWorkerOutcome::Reconciling { lease }),
            Err(error) => TaskWorkerAttempt::Unresolved { lease, error },
        }
    }

    async fn reconcile_task_owned(
        &self,
        lease: &crate::distributed::WorkLease,
        commands: &dyn TaskCommandHost,
        maximum_commands: u32,
    ) -> Result<TaskWorkerOutcome> {
        let operation = lease.operation.operation_id;
        let task = TaskId::from_bytes(operation.into_bytes());
        let fence = LeaseFence::from(&lease.reservation);
        self.host.verify_execution_owner(task, fence.clone()).await?;
        let admission = self.host.observe_admission(task).await?;
        if admission.run_limits.deadline_epoch_ms.is_some_and(|deadline| {
            self.host.now_unix_millis() >= deadline
        }) {
            self.host.expire_task_deadline(task, Some(fence.clone())).await?;
        }
        let reconciling = || TaskWorkerOutcome::Reconciling { lease: lease.clone() };
        // A missing initial journal is not permission to create an admission.
        let journal = self.workflow_journal(task, fence.clone()).await?;
        if journal.admission().await?.is_none() {
            return self.finish_known_reconciliation(task, fence, lease).await;
        }
        let Some(transition) = journal.observe_latest_outbox().await? else {
            return self.finish_known_reconciliation(task, fence, lease).await;
        };
        let context = self.task_context(task, fence.clone()).await?;
        for command in transition.commands.iter().take(maximum_commands as usize) {
            self.host.verify_execution_owner(task, fence.clone()).await?;
            let payload = crate::contract::json_from_slice(
                &context.read_file(&command.payload).await?,
            )
            .map_err(|error| Error::Invalid(format!("invalid command JSON: {error}")))?;
            match commands.reconcile(context.clone(), fence.clone(), command.clone(), payload).await? {
                TaskCommandProgress::Ready(_) => {}
                TaskCommandProgress::Pending | TaskCommandProgress::Indeterminate => {
                    return Ok(reconciling());
                }
            }
        }
        if transition.commands.len() > maximum_commands as usize {
            return Ok(reconciling());
        }
        self.finish_known_reconciliation(task, fence, lease).await
    }

    async fn finish_known_reconciliation(
        &self,
        task: TaskId,
        fence: LeaseFence,
        lease: &crate::distributed::WorkLease,
    ) -> Result<TaskWorkerOutcome> {
        self.host.verify_execution_owner(task, fence.clone()).await?;
        if self.host.observe_task(task).await?.cancellation_requested {
            self.host.settle_task(task, fence, Outcome::Cancelled).await?;
            Ok(TaskWorkerOutcome::Completed { task })
        } else {
            // Observation grants no authority to advance an uncancelled task.
            Ok(TaskWorkerOutcome::Reconciling { lease: lease.clone() })
        }
    }
}
