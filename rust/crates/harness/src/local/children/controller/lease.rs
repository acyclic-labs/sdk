use super::*;

impl LocalHarness {
    pub(super) async fn child_files(
        &self,
        child: TaskId,
        parent: &Files,
    ) -> Result<(Arc<Files>, crate::runtime::TaskContext)> {
        use crate::distributed::{WorkPull, Worker};
        let host = self.runtime.task_host();
        let lease = match host.recover_work(child, &mut None).await? {
            WorkPull::Claimed(lease) => lease,
            WorkPull::Idle => {
                if !self.runtime.poll_task_wake(child, None).await? {
                    if let Some(slot) = host.workflow_suspension(child, &mut None).await?
                        && let Some(waiting) = slot.waiting_command
                    {
                        host.wake_command(child, slot.revision, waiting, &mut None)
                            .await?;
                    }
                }
                let worker = Worker {
                    id: "child-file-controller".into(),
                    available: Default::default(),
                    labels: Default::default(),
                };
                let WorkPull::Claimed(lease) = host
                    .pull_work(
                        &worker,
                        Some(OperationId::from_bytes(child.into_bytes())),
                        &mut None,
                    )
                    .await?
                else {
                    return Err(Error::Indeterminate(OperationId::from_bytes(
                        child.into_bytes(),
                    )));
                };
                lease
            }
            WorkPull::Unresolved { error, .. } => return Err(error),
        };
        host.start_task(&lease, &mut None).await?;
        let fence = LeaseFence::from(&lease.reservation);
        let context = self.runtime.task_context(child, fence.clone()).await?;
        let files = parent
            .for_created_task(&context, context.scope())
            .await?
            .ok_or_else(|| Error::Unauthorized("child has no created private files".into()))?;
        Ok((Arc::new(files), context))
    }
}
