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

/// Advances a scripted task until it finishes, failing only once it stops
/// making progress: no model request for a whole idle window. A wall-clock
/// bound on the whole scenario misreads a runnable scenario that a loaded
/// test run delays as stalled, while a scenario that really stops still
/// fails within two windows.
pub(super) async fn advance_until_idle(
    harness: &LocalHarness,
    task: crate::TaskId,
    provider: &ScriptedModelProvider,
) -> Result<()> {
    const IDLE: std::time::Duration = std::time::Duration::from_secs(20);
    let commands = harness.commands()?;
    let checked = Checked(&commands);
    let advance = harness.advance(task, &checked);
    tokio::pin!(advance);
    let mut requests = provider.request_count();
    loop {
        if let Ok(result) = tokio::time::timeout(IDLE, &mut advance).await {
            return result;
        }
        let made = provider.request_count();
        if made == requests {
            return Err(Error::Conflict("the scenario stalled".into()));
        }
        requests = made;
    }
}
