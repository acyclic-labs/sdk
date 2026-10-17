//! File control stops new mailbox turns and cancels only this live invocation.
use super::*;
use crate::{
    filesystem::{TaskCommandHost, TaskCommandProgress},
    runtime::TaskContext,
    task_volume::{
        ChildFile, ChildState, Component, Control, FileKey, FileQuerySource, Query, TaskFiles,
    },
};
use acyclic_stream::BoxProviderFuture;
use serde_json::Value;

type Files = TaskFiles<acyclic_stream::LocalStream>;

pub(super) async fn state(
    files: &Files,
    reference: &Component,
    maximum_bytes: usize,
) -> Result<ChildState> {
    let snapshot = files
        .observe_files(
            Query::File(FileKey::Child {
                reference: reference.clone(),
                file: ChildFile::Control,
            }),
            None,
            1,
            maximum_bytes,
        )
        .await
        .map_err(storage)?
        .snapshot
        .ok_or_else(|| Error::Conflict("child control snapshot missing".into()))?;
    snapshot
        .files
        .first()
        .map(|file| {
            Control::parse(&file.bytes)
                .map(|control| ChildState::Running.apply(control))
                .map_err(storage)
        })
        .transpose()
        .map(|state| state.unwrap_or_default())
}

pub(super) struct Commands<'a> {
    pub harness: &'a LocalHarness,
    pub inner: &'a dyn TaskCommandHost,
    pub files: &'a Files,
    pub reference: &'a Component,
    pub cancellation: acyclic_fs::CancellationToken,
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
        if command.kind == crate::filesystem::MAIL_RECEIVE_TASK_COMMAND_KIND
            && (self.cancellation.is_cancelled()
                || state(
                    self.files,
                    self.reference,
                    usize::try_from(context.scope().limits().render_bytes).map_err(storage)?,
                )
                .await?
                    != ChildState::Running)
        {
            return Ok(TaskCommandProgress::Pending);
        }
        if reconcile {
            return self.inner.reconcile(context, fence, command, payload).await;
        }
        let task = context
            .durable_task_id()
            .ok_or_else(|| Error::Unauthorized("child command has no task".into()))?;
        let model = command.kind == crate::filesystem::MODEL_TASK_COMMAND_KIND;
        let inherited = context.invocation_cancellation().cloned();
        let cancellation = inherited.as_ref().unwrap_or(&self.cancellation);
        // Cancellation attenuates the effect context, never the workflow
        // checkpoint owner: the latter must still durably settle this result.
        let context = if model && inherited.is_none() {
            self.harness
                .runtime
                .task_context_with_cancellation(task, fence.clone(), Some(cancellation))
                .await?
        } else {
            context
        };
        let result = if cancellation.is_cancelled() && model {
            Err(Error::InteractionRejected(
                crate::InteractionRejection::Cancelled,
            ))
        } else {
            self.inner
                .execute(context, fence.clone(), command.clone(), payload.clone())
                .await
        };
        if model
            && result
                .as_ref()
                .is_err_and(|error| stopped(cancellation.is_cancelled(), error))
        {
            #[cfg(all(test, not(acyclic_harness_loom)))]
            if matches!(
                &result,
                Err(Error::InteractionRejected(
                    crate::InteractionRejection::Cancelled
                ))
            ) {
                super::cuts::stop("model-stopped", command.operation_id)?;
            }
            let context = self
                .harness
                .runtime
                .task_context(task, fence.clone())
                .await?;
            let result = self
                .inner
                .reconcile(context, fence, command, payload)
                .await?;
            return Ok(match result {
                TaskCommandProgress::Ready(_) => {
                    TaskCommandProgress::Ready(serde_json::json!("Cancelled"))
                }
                other => other,
            });
        }
        result
    }
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
        Box::pin(self.run(context, fence, command, payload, false))
    }
    fn reconcile<'a>(
        &'a self,
        context: TaskContext,
        fence: LeaseFence,
        command: WorkflowCommand,
        payload: Value,
    ) -> BoxProviderFuture<'a, Result<TaskCommandProgress>> {
        Box::pin(self.run(context, fence, command, payload, true))
    }
}

fn stopped(cancelled: bool, error: &Error) -> bool {
    !error.is_native_failure()
        && (cancelled
            || matches!(
                error,
                Error::InteractionRejected(crate::InteractionRejection::Cancelled)
            ))
}

#[cfg(all(test, not(acyclic_harness_loom)))]
mod tests {
    use super::*;
    use proptest::prelude::*;
    proptest! {
        #[test]
        fn a_retained_stop_does_not_require_a_live_cancellation_token(
            cancelled in any::<bool>(), kind in 0usize..4, reason in ".{0,32}",
        ) {
            let error = match kind {
                0 => Error::InteractionRejected(crate::InteractionRejection::Cancelled),
                1 => Error::Storage(reason),
                2 => Error::Invalid(reason),
                _ => Error::NativeBoundary { code: crate::wire::ErrorCode::Storage, reason: "native" },
            };
            prop_assert_eq!(stopped(cancelled, &error), kind != 3 && (cancelled || kind == 0));
        }
    }
}
