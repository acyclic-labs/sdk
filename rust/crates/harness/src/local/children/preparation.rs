use super::*;
use crate::{
    filesystem::{FilesystemForkPreparer, PreparationIo, TaskCommandHost, TaskCommandProgress},
    resource_preparation::ResourcePreparationRequest,
    runtime::{TaskContext, TaskStateProvider},
};
use acyclic_stream::BoxProviderFuture;
use serde_json::Value;

pub(super) struct Commands<'a> {
    pub harness: &'a LocalHarness,
    pub source: &'a workspace::View,
    pub cut: acyclic_fs::LazySnapshotRef,
    pub preparer: &'a FilesystemForkPreparer<LocalAuthorityBackend, LocalObjectBackend>,
    pub request: &'a ResourcePreparationRequest,
    pub workspace: &'a LocalWorkspace,
    pub io: &'a PreparationIo,
}

impl Commands<'_> {
    async fn run(
        &self,
        context: TaskContext,
        fence: LeaseFence,
        command: WorkflowCommand,
        payload: Value,
        reconcile: bool,
    ) -> Result<TaskCommandProgress> {
        let task = context
            .durable_task_id()
            .ok_or_else(|| Error::Unauthorized("child preparation has no task".into()))?;
        let host = self.harness.runtime.task_host();
        let record = crate::conversation::BoundedOperationFuture::new(Some(self.io), || {
            host.observe_admission(task)
        })?
        .await?;
        let fork = self.admitted_fork(&record.input, &command, payload)?;
        drop(record);
        let owner = Arc::new(
            crate::conversation::BoundedOperationFuture::new(Some(self.io), || {
                host.journal_owner(task, &fence, None)
            })?
            .await?
            .with_invocation_cancellation(self.io.cancellation()),
        );
        let preparation = Arc::new(
            crate::conversation::BoundedOperationFuture::new(Some(self.io), || async {
                owner
                    .resource_preparation(self.request.clone(), &mut None)
                    .await
            })?
            .await?,
        );
        if reconcile {
            return Ok(
                if crate::conversation::BoundedOperationFuture::new(Some(self.io), || {
                    self.preparer
                        .reconcile_for_task(&fork, &preparation, Some(self.io))
                })?
                .await?
                .is_some()
                {
                    TaskCommandProgress::Ready(Value::Null)
                } else {
                    TaskCommandProgress::Indeterminate
                },
            );
        }
        let (report, _) = crate::conversation::BoundedOperationFuture::new(Some(self.io), || {
            self.workspace.prepare_child(
                self.source,
                self.cut,
                self.preparer,
                &fork,
                &preparation,
                self.io,
            )
        })?
        .await?;
        ready(report)
    }

    // The admitted command's checks and decode run in a frame of their own,
    // not in the poll frame live under the child's preparation.
    #[inline(never)]
    fn admitted_fork(
        &self,
        input: &Value,
        command: &WorkflowCommand,
        payload: Value,
    ) -> Result<crate::charged_box::ChargedBox<crate::fork::ForkRequest>> {
        if *input != serde_json::to_value(self.request).map_err(storage)?
            || self.request.command.as_ref() != Some(command)
            || payload != self.request.payload
        {
            return Err(Error::Unauthorized(
                "child preparation differs from its admitted command".into(),
            ));
        }
        // The request lives through preparation and its retries, so its box
        // is paid for on the preparation's budget.
        let fork = serde_json::from_value(payload).map_err(storage)?;
        crate::charged_box::ChargedBox::new(Some(self.io), fork)
    }
}

#[inline(never)]
fn ready(
    report: crate::charged_box::ChargedBox<crate::fork::ForkReport>,
) -> Result<TaskCommandProgress> {
    Ok(TaskCommandProgress::Ready(
        serde_json::to_value(crate::Outcome::Succeeded(report.into_inner())).map_err(storage)?,
    ))
}

impl TaskCommandHost for Commands<'_> {
    fn frame_io(&self) -> Option<&PreparationIo> {
        Some(self.io)
    }
    fn execute<'a>(
        &'a self,
        context: TaskContext,
        fence: LeaseFence,
        command: WorkflowCommand,
        payload: Value,
    ) -> BoxProviderFuture<'a, Result<TaskCommandProgress>> {
        // The frame is paid for on the preparation's budget, which `run` spends.
        match crate::charged_box::charged_frame(Some(self.io), || {
            self.run(context, fence, command, payload, false)
        }) {
            Ok(frame) => frame,
            // Unbudgeted: a refused frame holds only its error.
            Err(error) => Box::pin(std::future::ready(Err(error))),
        }
    }
    fn reconcile<'a>(
        &'a self,
        context: TaskContext,
        fence: LeaseFence,
        command: WorkflowCommand,
        payload: Value,
    ) -> BoxProviderFuture<'a, Result<TaskCommandProgress>> {
        // The frame is paid for on the preparation's budget, which `run` spends.
        match crate::charged_box::charged_frame(Some(self.io), || {
            self.run(context, fence, command, payload, true)
        }) {
            Ok(frame) => frame,
            // Unbudgeted: a refused frame holds only its error.
            Err(error) => Box::pin(std::future::ready(Err(error))),
        }
    }
}

// The input files are visible through existing live queries during staging.
// Admission is allowed only after the common authority durability fence.
pub(super) async fn fenced_inputs<T>(
    prepare: impl Future<Output = Result<T>>,
    fence: impl Future<Output = Result<()>>,
) -> Result<T> {
    let prepared = acyclic_fs::deferring_authority_durability(prepare).await;
    #[cfg(acyclic_harness_loom)]
    if crate::task_volume::model_checking::enabled(
        crate::task_volume::model_checking::Fault::SkipChildInputFence,
    ) {
        return prepared;
    }
    // Fence accepted prefixes on errors too; a retry observes the same cut.
    fence.await?;
    prepared
}

#[cfg(test)]
#[cfg(all(acyclic_harness_loom, acyclic_fs_loom))]
mod models;

#[cfg(test)]
#[cfg(not(acyclic_harness_loom))]
mod tests {
    use super::*;
    use proptest::prelude::*;
    proptest! {
        #![proptest_config(ProptestConfig::with_cases(16))]
        #[test]
        fn staged_prefix_is_fenced_before_success_or_failure(prepare_fails in any::<bool>(), fence_fails in any::<bool>()) {
            let phase = std::cell::Cell::new(0);
            let result = futures::executor::block_on(fenced_inputs(async {
                phase.set(1);
                if prepare_fails { Err(Error::Storage("prepare".into())) } else { Ok(()) }
            }, async {
                assert_eq!(phase.get(), 1);
                phase.set(2);
                if fence_fails { Err(Error::Storage("fence".into())) } else { Ok(()) }
            }));
            prop_assert_eq!(phase.get(), 2);
            prop_assert_eq!(result.is_ok(), !prepare_fails && !fence_fails);
            if fence_fails { prop_assert!(matches!(result, Err(Error::Storage(ref reason)) if reason == "fence")); }
            else if prepare_fails { prop_assert!(matches!(result, Err(Error::Storage(ref reason)) if reason == "prepare")); }
        }
    }
}
