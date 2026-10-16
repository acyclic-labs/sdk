//! Retain the stock command diagnostic before the worker marks it uncertain.
use super::*;
use crate::{
    filesystem::{TaskCommandHost, TaskCommandProgress},
    runtime::TaskContext,
    scheduler::LeaseFence,
    workflow::WorkflowCommand,
};

pub(super) struct Checked<'a>(pub &'a dyn TaskCommandHost);

impl TaskCommandHost for Checked<'_> {
    fn execute<'a>(
        &'a self,
        context: TaskContext,
        fence: LeaseFence,
        command: WorkflowCommand,
        payload: Value,
    ) -> BoxProviderFuture<'a, Result<TaskCommandProgress>> {
        Box::pin(async move {
            let result = self.0.execute(context, fence, command, payload).await;
            if let Err(error) = &result {
                panic!("stock checkout command: {error:?}");
            }
            result
        })
    }
}
