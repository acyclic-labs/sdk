use super::*;
use crate::{filesystem::TaskCommandProgress, runtime::TaskContext};
use serde_json::Value;

pub(super) struct Commands<'a> {
    pub harness: &'a LocalHarness,
    pub inner: &'a dyn TaskCommandHost,
    pub cancellation: acyclic_fs::CancellationToken,
    pub drained: acyclic_fs::CancellationToken,
}

impl TaskCommandHost for Commands<'_> {
    fn invocation_cancellation(&self) -> Option<&acyclic_fs::CancellationToken> {
        Some(&self.cancellation)
    }
    fn execute<'a>(
        &'a self,
        context: TaskContext,
        fence: LeaseFence,
        command: WorkflowCommand,
        payload: Value,
    ) -> BoxProviderFuture<'a, Result<TaskCommandProgress>> {
        Box::pin(async move {
            let context = if matches!(
                command.kind.as_str(),
                crate::filesystem::MODEL_TASK_COMMAND_KIND
                    | crate::filesystem::TOOL_TASK_COMMAND_KIND
            ) {
                let task = context
                    .durable_task_id()
                    .ok_or_else(|| Error::Unauthorized("effect has no original task".into()))?;
                self.harness
                    .runtime
                    .task_context_with_cancellation(task, fence.clone(), Some(&self.cancellation))
                    .await?
            } else {
                context
            };
            let result = self.inner.execute(context, fence, command, payload).await;
            // Keep the parent's original lease until every active child has
            // finished admission, effects and publication. Siblings do not
            // inherit this error-only invocation cancellation token.
            if self.cancellation.is_cancelled() {
                self.drained.cancelled().await;
            }
            result
        })
    }
    fn reconcile<'a>(
        &'a self,
        context: TaskContext,
        fence: LeaseFence,
        command: WorkflowCommand,
        payload: Value,
    ) -> BoxProviderFuture<'a, Result<TaskCommandProgress>> {
        self.inner.reconcile(context, fence, command, payload)
    }
}

pub(super) async fn finish<F, T>(
    parent: impl Future<Output = Result<()>>,
    jobs: &mut FuturesUnordered<F>,
    cancellation: &acyclic_fs::CancellationToken,
    drained: &acyclic_fs::CancellationToken,
) where
    F: Future<Output = Result<T>>,
{
    cancellation.cancel();
    #[cfg(acyclic_harness_loom)]
    if crate::task_volume::model_checking::enabled(
        crate::task_volume::model_checking::Fault::DropSiblingEffects,
    ) {
        drained.cancel();
        let _ = parent.await;
        return;
    }
    let children = async {
        while jobs.next().await.is_some() {}
        drained.cancel();
    };
    let _ = futures::future::join(parent, children).await;
}

#[cfg(all(test, acyclic_harness_loom, acyclic_fs_loom))]
mod models;
