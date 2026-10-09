//! Actual browser execution of ordinary registered live tasks without Tokio.
#![cfg(target_arch = "wasm32")]

use acyclic_harness::{
    Capabilities, Outcome,
    conversation::Limits,
    live::TaskGroup,
    runtime::{Bindings, RuntimeScope, TaskDefinition, TaskRunLimits},
};
use std::{cell::Cell, rc::Rc};
use wasm_bindgen::JsValue;
use wasm_bindgen_futures::JsFuture;
use wasm_bindgen_test::{wasm_bindgen_test, wasm_bindgen_test_configure};

wasm_bindgen_test_configure!(run_in_browser);

fn js_error(error: impl std::fmt::Display) -> JsValue {
    JsValue::from_str(&error.to_string())
}

struct DropSignal(Rc<Cell<bool>>);

impl Drop for DropSignal {
    fn drop(&mut self) {
        self.0.set(true);
    }
}

#[wasm_bindgen_test]
async fn registered_live_tasks_use_the_browser_executor_and_release_cancelled_capacity()
-> Result<(), JsValue> {
    // Rc deliberately verifies the platform's local future boundary. Native
    // task definitions remain Send through ProviderPlatform/ProviderTask.
    let observed = Rc::new(Cell::new(0));
    let captured = observed.clone();
    let definition = TaskDefinition::<u64, u64>::live("test.live", "1", move |_, input| {
        let captured = captured.clone();
        async move {
            JsFuture::from(js_sys::Promise::resolve(&JsValue::UNDEFINED))
                .await
                .map_err(|_| acyclic_harness::Error::Storage("browser microtask failed".into()))?;
            captured.set(captured.get() + 1);
            Ok(input + 1)
        }
    })
    .map_err(js_error)?;
    let dropped = Rc::new(Cell::new(false));
    let captured = dropped.clone();
    let pending = TaskDefinition::<u64, u64>::live("test.pending", "1", move |_, _| {
        let signal = DropSignal(captured.clone());
        async move {
            let _signal = signal;
            std::future::pending().await
        }
    })
    .map_err(js_error)?;
    let mut bindings = Bindings::local();
    bindings.scope = RuntimeScope::new(
        Capabilities::new(["task:spawn:test.live@1", "task:spawn:test.pending@1"]),
        Limits::default(),
    )
    .map_err(js_error)?;
    bindings.concurrency = 1;
    bindings.tasks.register(definition).map_err(js_error)?;
    bindings.tasks.register(pending).map_err(js_error)?;
    let harness = bindings.build().map_err(js_error)?;
    let pending = harness.task::<u64, u64>("test.pending").map_err(js_error)?;
    let task = harness.spawn(&pending, 0).await.map_err(js_error)?;
    JsFuture::from(js_sys::Promise::resolve(&JsValue::UNDEFINED)).await?;
    task.cancel().await.map_err(js_error)?;
    assert_eq!(task.result().await.map_err(js_error)?, Outcome::Cancelled);
    dropped.set(false);
    let deadline =
        acyclic_stream::UnixMillisClock::now_unix_millis(&acyclic_stream::SystemUnixMillisClock)
            + 500;
    let bounded = harness
        .scoped_run_limits(TaskRunLimits {
            deadline_epoch_ms: Some(deadline),
            ..TaskRunLimits::default()
        })
        .map_err(js_error)?;
    let task = bounded.spawn(&pending, 0).await.map_err(js_error)?;
    assert!(matches!(
        task.result().await.map_err(js_error)?,
        Outcome::Failed { message } if message.contains("deadline has expired")
    ));
    assert!(dropped.get(), "deadline retained the pending task future");
    let definition = harness.task::<u64, u64>("test.live").map_err(js_error)?;
    let task = harness.spawn(&definition, 7).await.map_err(js_error)?;
    assert_eq!(
        task.result().await.map_err(js_error)?,
        Outcome::Succeeded(8)
    );
    assert_eq!(observed.get(), 1);
    Ok(())
}

#[wasm_bindgen_test]
async fn parent_group_cancels_browser_descendants_and_closes_admission() -> Result<(), JsValue> {
    let root = TaskGroup::new(1);
    let child = root.child(1);
    let grandchild = child.child(1);
    let child_dropped = Rc::new(Cell::new(false));
    let grandchild_dropped = Rc::new(Cell::new(false));
    let pending = |dropped| {
        let signal = DropSignal(dropped);
        async move {
            let _signal = signal;
            std::future::pending::<()>().await;
        }
    };
    let first = child.spawn(pending(child_dropped.clone())).await;
    let second = grandchild.spawn(pending(grandchild_dropped.clone())).await;
    JsFuture::from(js_sys::Promise::resolve(&JsValue::UNDEFINED)).await?;
    root.cancel();
    assert_eq!(first.result().await, Outcome::Cancelled);
    assert_eq!(second.result().await, Outcome::Cancelled);
    assert!(child_dropped.get() && grandchild_dropped.get());
    let polled = Rc::new(Cell::new(false));
    let captured = polled.clone();
    let rejected = grandchild
        .try_spawn(async move { captured.set(true) })
        .await;
    assert!(matches!(
        rejected,
        acyclic_harness::Admission::Rejected { .. }
    ));
    assert!(!polled.get(), "closed subtree executed rejected work");
    Ok(())
}

struct DelegatingBrowserLoop;

impl acyclic_harness::agent_loop::AgentLoop for DelegatingBrowserLoop {
    fn run(
        &self,
        context: acyclic_harness::runtime::TaskContext,
        input: acyclic_harness::agent_loop::AgentInput,
    ) -> acyclic_stream::BoxProviderFuture<
        'static,
        acyclic_harness::Result<acyclic_harness::agent_loop::AgentOutput>,
    > {
        Box::pin(async move {
            let definition = context.task::<String, String>("browser.builder.echo")?;
            let task = context.spawn(&definition, input.prompt).await?;
            match task.result().await? {
                Outcome::Succeeded(text) => {
                    Ok(acyclic_harness::agent_loop::AgentOutput::text(text))
                }
                outcome => Err(acyclic_harness::Error::Invalid(format!(
                    "unexpected browser child outcome: {outcome:?}"
                ))),
            }
        })
    }
}

#[wasm_bindgen_test]
async fn public_builder_registers_and_runs_browser_local_typed_tasks() -> Result<(), JsValue> {
    let calls = Rc::new(Cell::new(0));
    let observed = calls.clone();
    let task = TaskDefinition::live("browser.builder.echo", "1", move |_, input: String| {
        let observed = observed.clone();
        async move {
            observed.set(observed.get() + 1);
            Ok(input)
        }
    })
    .map_err(js_error)?;
    let bundle = acyclic_harness::HarnessBuilder::new()
        .agent_loop(std::sync::Arc::new(DelegatingBrowserLoop))
        .grant("task:spawn:browser.builder.echo@1")
        .task(task)
        .map_err(js_error)?
        .build()
        .map_err(js_error)?;
    let result = bundle
        .run_agent(acyclic_harness::agent_loop::AgentInput::text(
            "browser child",
        ))
        .await
        .map_err(js_error)?;
    assert!(matches!(result.outcome, Outcome::Succeeded(output) if output.text == "browser child"));
    assert_eq!(calls.get(), 1);
    assert!(!result.task_id.is_empty());
    Ok(())
}
