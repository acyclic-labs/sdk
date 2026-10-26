//! One stock package worker over the original filesystem journal and fence.
use crate::{Result, TaskId, local::LocalHarness, runtime::TaskStateProvider};
use std::future::Future;
const TRANSITIONS_PER_SLICE: u32 = 32;

/// A detached passive approval is recoverable work, never a failed effect.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum DriveState {
    Quiescent,
    AwaitingApproval,
}

impl DriveState {
    pub(crate) fn into_result(self, task: TaskId) -> Result<()> {
        match self {
            Self::Quiescent => Ok(()),
            Self::AwaitingApproval => Err(crate::Error::Indeterminate(
                crate::OperationId::from_bytes(task.into_bytes()),
            )),
        }
    }
}

pub(crate) async fn advance(
    local: &LocalHarness,
    task: TaskId,
    commands: &dyn crate::filesystem::TaskCommandHost,
) -> Result<()> {
    advance_while(local, task, commands, None, || async { Ok(true) })
        .await?
        .into_result(task)
}

#[inline(never)]
pub(crate) fn advance_while<'a, F, Fut>(
    local: &'a LocalHarness,
    task: TaskId,
    commands: &'a dyn crate::filesystem::TaskCommandHost,
    inbox: Option<&'a crate::local::approvals::Inbox>,
    continue_work: F,
) -> acyclic_stream::BoxProviderFuture<'a, Result<DriveState>>
where
    F: Fn() -> Fut + Send + Sync + 'a,
    Fut: Future<Output = Result<bool>> + Send,
{
    Box::pin(async move {
        let worker = crate::distributed::Worker {
            id: format!("local-{}", crate::identity::fresh_uuid()),
            available: Default::default(),
            labels: Default::default(),
        };
        let mut attempt = match Box::pin(
            local
                .task_runtime()
                .task_host()
                .recover_work(task, &mut None),
        )
        .await?
        {
            crate::distributed::WorkPull::Claimed(lease) => Some(
                local
                    .task_runtime()
                    .resume_task(lease, commands, TRANSITIONS_PER_SLICE, None)
                    .await,
            ),
            crate::distributed::WorkPull::Unresolved { error, .. } => {
                if local
                    .task_runtime()
                    .task_host()
                    .outcome(task)
                    .await?
                    .is_none()
                {
                    return Err(error);
                }
                None
            }
            crate::distributed::WorkPull::Idle => {
                local
                    .task_runtime()
                    .run_operation(&worker, task, commands, TRANSITIONS_PER_SLICE, None)
                    .await?
            }
        };
        loop {
            match attempt {
                Some(crate::filesystem::TaskWorkerAttempt::Unresolved { error, .. }) => {
                    return Err(error);
                }
                Some(crate::filesystem::TaskWorkerAttempt::Progress(
                    crate::filesystem::TaskWorkerOutcome::Yielded { lease },
                )) => {
                    attempt = Some(
                        local
                            .task_runtime()
                            .resume_task(lease, commands, TRANSITIONS_PER_SLICE, None)
                            .await,
                    );
                }
                Some(crate::filesystem::TaskWorkerAttempt::Progress(
                    crate::filesystem::TaskWorkerOutcome::Suspended { .. },
                ))
                | None => {
                    if !continue_work().await? {
                        break;
                    }
                    if local.service_approvals(task, commands, inbox).await?
                        == DriveState::AwaitingApproval
                    {
                        return Ok(DriveState::AwaitingApproval);
                    }
                    // Only an authenticated wake makes a passive command claimable.
                    // Without one, run_operation returns None and yields control.
                    attempt = local
                        .task_runtime()
                        .run_operation(&worker, task, commands, TRANSITIONS_PER_SLICE, None)
                        .await?;
                    if attempt.is_none() {
                        break;
                    }
                }
                _ => break,
            }
        }
        local.refresh_command_task(task).await?;
        Ok(DriveState::Quiescent)
    })
}
