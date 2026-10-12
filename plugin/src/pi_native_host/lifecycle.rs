//! Cancellation and joining the original SDK task.
use super::*;

impl PiNativeCommandHost {
    pub(crate) async fn signal_stop_child(
        &self,
        task: TaskId,
        work: &NativeMaterializationWork,
    ) -> Result<()> {
        self.child_admission(task, work).await?;
        if let Some(entry) = self.find_entry(task) {
            entry.operation_cancellation.cancel();
        }
        self.runtime
            .runtime
            .task_host()
            .cancel(task, Some(work))
            .await
    }

    pub(crate) async fn drain_stopped_child(
        &self,
        task: TaskId,
        work: &NativeMaterializationWork,
    ) -> Result<()> {
        self.child_admission(task, work).await?;
        let entry = self.registered_entry(task)?;
        let _run = entry.run.lock().await;
        let mut handles = entry.handles.lock().await;
        let effect = handles.effect.clone();
        let original = effect
            .as_ref()
            .map(|effect| (Arc::clone(&effect.owner), effect.command))
            .or_else(|| {
                handles
                    .preparation_owner
                    .as_ref()
                    .map(|(owner, _, command)| (Arc::clone(owner), *command))
            });
        if let Some((owner, command)) = original {
            handles
                .project_gate
                .release(&owner, work, work.cancellation(), command)
                .await?;
            handles
                .scratch_gate
                .release(&owner, work, work.cancellation(), command)
                .await?;
        }
        handles.project_startup_owner = None;
        handles.project = None;
        handles.project_allocation = None;
        handles.root = None;
        handles.preparation_owner = None;
        handles.effect = None;
        *entry
            .view
            .write()
            .unwrap_or_else(std::sync::PoisonError::into_inner) = None;
        drop(handles);
        if let Some(effect) = effect {
            let outcome = effect.owner.outcome_with_native_work(work).await?;
            if outcome.0.is_none() {
                let workflow = self
                    .runtime
                    .runtime
                    .workflow_journal(task, effect.context.owned_task_fence()?)
                    .await?;
                effect
                    .journal
                    .release_if_quiescent_with_native_work(work, &workflow)
                    .await?;
            }
        }
        Ok(())
    }
}
