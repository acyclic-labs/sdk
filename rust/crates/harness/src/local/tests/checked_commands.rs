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

pub(super) fn call(index: usize, name: &str, command: &str) -> ScriptedTurn {
    ScriptedTurn::Events(vec![
        ScriptedEvent::Event(ModelEvent::ToolCall(crate::model::ToolCall::new(
            format!("checkout-{index}"),
            name.into(),
            json!({"command":command}),
        ))),
        ScriptedEvent::Event(ModelEvent::Completed {
            metadata: json!({}),
        }),
    ])
}
