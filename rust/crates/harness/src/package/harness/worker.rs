//! One stock package worker over the original filesystem journal and fence.
use crate::{Result, TaskId, local::LocalHarness, runtime::TaskStateProvider};
use std::future::Future;
const TRANSITIONS_PER_SLICE: u32 = 32;

pub(crate) async fn advance(
    local: &LocalHarness,
    task: TaskId,
    commands: &dyn crate::filesystem::TaskCommandHost,
) -> Result<()> {
    advance_while(local, task, commands, || async { Ok(true) }).await
}

pub(crate) async fn advance_while<F, Fut>(
    local: &LocalHarness,
    task: TaskId,
    commands: &dyn crate::filesystem::TaskCommandHost,
    continue_work: F,
) -> Result<()>
where
    F: Fn() -> Fut + Send + Sync,
    Fut: Future<Output = Result<bool>> + Send,
{
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
            )) => {
                if !continue_work().await? {
                    break;
                }
                // Only an authenticated wake makes a passive command claimable.
                // Without one, run_operation returns None and yields control.
                attempt = local
                    .task_runtime()
                    .run_operation(&worker, task, commands, TRANSITIONS_PER_SLICE, None)
                    .await?;
            }
            _ => break,
        }
    }
    local.refresh_command_task(task).await?;
    Ok(())
}
