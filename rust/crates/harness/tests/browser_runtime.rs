//! Actual browser execution of ordinary registered live tasks without Tokio.
#![cfg(target_arch = "wasm32")]

use acyclic_harness::{
    Capabilities, Outcome,
    conversation::Limits,
    runtime::{Bindings, RuntimeScope, TaskDefinition},
};
use std::{cell::Cell, rc::Rc};
use wasm_bindgen::JsValue;
use wasm_bindgen_futures::JsFuture;
use wasm_bindgen_test::{wasm_bindgen_test, wasm_bindgen_test_configure};

wasm_bindgen_test_configure!(run_in_browser);

fn js_error(error: impl std::fmt::Display) -> JsValue {
    JsValue::from_str(&error.to_string())
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
    let pending = TaskDefinition::<u64, u64>::live("test.pending", "1", |_, _| async {
        std::future::pending().await
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
    let definition = harness.task::<u64, u64>("test.live").map_err(js_error)?;
    let task = harness.spawn(&definition, 7).await.map_err(js_error)?;
    assert_eq!(
        task.result().await.map_err(js_error)?,
        Outcome::Succeeded(8)
    );
    assert_eq!(observed.get(), 1);
    Ok(())
}
