//! Actual browser timers through the public Rust Harness runtime.
#![cfg(target_arch = "wasm32")]

wasm_bindgen_test::wasm_bindgen_test_configure!(run_in_browser);

use acyclic_harness::{
    Capabilities, OperationId, Outcome, Result,
    conversation::Limits,
    runtime::{AgentHarness, RuntimeScope, TaskDefinition, TaskRegistry, TaskRunLimits},
    tool::ToolRegistry,
};
use acyclic_stream::{SystemUnixMillisClock, UnixMillisClock as _};
use js_sys::{Function, Reflect};
use std::{cell::RefCell, rc::Rc, sync::Arc};
use tokio::sync::oneshot;
use wasm_bindgen::{JsCast as _, JsValue, closure::Closure};
use wasm_bindgen_test::wasm_bindgen_test;

// Observe, but never replace, the browser's real scheduling and cancellation.
// A timer with an absolute deadline beyond the signed browser delay bound must
// be chunked, and cancellation must clear the exact admitted browser timer.
#[derive(Default)]
struct TimerLog {
    scheduled: Vec<(JsValue, f64)>,
    cleared: Vec<JsValue>,
}

struct TimerProbe {
    owner: JsValue,
    original_set: JsValue,
    original_clear: JsValue,
    _set: Closure<dyn FnMut(JsValue, JsValue) -> JsValue>,
    _clear: Closure<dyn FnMut(JsValue)>,
    log: Rc<RefCell<TimerLog>>,
}

impl TimerProbe {
    fn new() -> Self {
        let owner: JsValue = js_sys::global().into();
        let original_set = Reflect::get(&owner, &"setTimeout".into()).expect("real browser timer");
        let original_clear =
            Reflect::get(&owner, &"clearTimeout".into()).expect("real cancellation");
        let set = original_set
            .clone()
            .dyn_into::<Function>()
            .expect("setTimeout function");
        let clear = original_clear
            .clone()
            .dyn_into::<Function>()
            .expect("clearTimeout function");
        let log = Rc::new(RefCell::new(TimerLog::default()));
        let scheduled = Rc::clone(&log);
        let set_owner = owner.clone();
        let set = Closure::new(move |callback: JsValue, delay: JsValue| {
            let id = set
                .call2(&set_owner, &callback, &delay)
                .expect("real timer schedules");
            scheduled
                .borrow_mut()
                .scheduled
                .push((id.clone(), delay.as_f64().expect("numeric delay")));
            id
        });
        let cleared = Rc::clone(&log);
        let clear_owner = owner.clone();
        let clear = Closure::new(move |id: JsValue| {
            clear.call1(&clear_owner, &id).expect("real timer clears");
            cleared.borrow_mut().cleared.push(id);
        });
        Reflect::set(&owner, &"setTimeout".into(), set.as_ref())
            .expect("install transparent observer");
        Reflect::set(&owner, &"clearTimeout".into(), clear.as_ref())
            .expect("observe real cancellation");
        Self {
            owner,
            original_set,
            original_clear,
            _set: set,
            _clear: clear,
            log,
        }
    }

    fn assert_single_clear(&self, delay: f64) {
        let log = self.log.borrow();
        let timers: Vec<_> = log
            .scheduled
            .iter()
            .filter(|(_, value)| *value == delay)
            .collect();
        assert_eq!(timers.len(), 1, "one real timer is admitted for this wait");
        assert_eq!(
            log.cleared.iter().filter(|id| **id == timers[0].0).count(),
            1
        );
    }
}

impl Drop for TimerProbe {
    fn drop(&mut self) {
        Reflect::set(&self.owner, &"setTimeout".into(), &self.original_set).expect("restore timer");
        Reflect::set(&self.owner, &"clearTimeout".into(), &self.original_clear)
            .expect("restore cancellation");
    }
}

fn runtime(definition: TaskDefinition<(), u64>, timer_grant: bool) -> Result<Arc<AgentHarness>> {
    let name = definition.identity().name.clone();
    let version = definition.identity().version.clone();
    let mut tasks = TaskRegistry::default();
    tasks.register(definition)?;
    let mut grants = vec![format!("task:spawn:{name}@{version}")];
    if timer_grant {
        grants.push("timer:wait".into());
    }
    AgentHarness::new(
        tasks,
        ToolRegistry::default(),
        RuntimeScope::new(Capabilities::new(grants), Limits::default())?,
        1,
        None,
    )
}

#[wasm_bindgen_test]
async fn live_absolute_sleep_waits_for_real_clock_and_requires_authority() -> Result<()> {
    let definition = TaskDefinition::live("browser.sleep", "1", |context, (): ()| async move {
        let deadline = SystemUnixMillisClock.now_unix_millis() + 30;
        context.sleep_until(OperationId::new(), deadline).await?;
        assert!(SystemUnixMillisClock.now_unix_millis() >= deadline);
        Ok(deadline)
    })?;
    let allowed = runtime(definition, true)?;
    let selected = allowed.task::<(), u64>("browser.sleep")?;
    assert!(matches!(
        allowed.spawn(&selected, ()).await?.result().await?,
        Outcome::Succeeded(_)
    ));
    let denied = allowed.scoped(
        Capabilities::new(["task:spawn:browser.sleep@1"]),
        Limits::default(),
    )?;
    let selected = denied.task::<(), u64>("browser.sleep")?;
    assert!(matches!(
        denied.spawn(&selected, ()).await?.result().await?,
        Outcome::Failed { .. }
    ));
    Ok(())
}

struct DropSignal(Option<oneshot::Sender<()>>);
impl Drop for DropSignal {
    fn drop(&mut self) {
        if let Some(send) = self.0.take() {
            let _ = send.send(());
        }
    }
}

#[wasm_bindgen_test]
async fn live_deadline_drops_pending_handler_and_its_browser_timer() -> Result<()> {
    let probe = TimerProbe::new();
    let (dropped, observed) = oneshot::channel();
    let dropped = Rc::new(RefCell::new(Some(dropped)));
    let definition = TaskDefinition::live("browser.deadline", "1", move |_context, (): ()| {
        let guard = DropSignal(dropped.borrow_mut().take());
        async move {
            let _guard = guard;
            std::future::pending::<Result<u64>>().await
        }
    })?;
    let parent = runtime(definition, false)?;
    let deadline = SystemUnixMillisClock.now_unix_millis() + 100;
    let bounded = parent.scoped_run_limits(TaskRunLimits {
        deadline_epoch_ms: Some(deadline),
        ..TaskRunLimits::default()
    })?;
    let task = bounded.task::<(), u64>("browser.deadline")?;
    assert!(matches!(
        bounded.spawn(&task, ()).await?.result().await?,
        Outcome::Failed { .. }
    ));
    observed.await.expect("deadline drops the pending handler");
    assert!(SystemUnixMillisClock.now_unix_millis() >= deadline);
    let log = probe.log.borrow();
    assert_eq!(log.scheduled.len(), 1);
    assert_eq!(log.cleared, vec![log.scheduled[0].0.clone()]);
    Ok(())
}

#[wasm_bindgen_test]
async fn cancellation_clears_long_live_sleep_without_wrapping_browser_delay() -> Result<()> {
    let probe = TimerProbe::new();
    let (started, observed) = oneshot::channel();
    let started = Rc::new(RefCell::new(Some(started)));
    let definition = TaskDefinition::live("browser.cancel_sleep", "1", move |context, (): ()| {
        let started = started.borrow_mut().take();
        async move {
            if let Some(started) = started {
                let _ = started.send(());
            }
            let deadline = SystemUnixMillisClock.now_unix_millis()
                + u64::from(i32::MAX.unsigned_abs())
                + 60_000;
            context.sleep_until(OperationId::new(), deadline).await?;
            Ok(1_u64)
        }
    })?;
    let harness = runtime(definition, true)?;
    let selected = harness.task::<(), u64>("browser.cancel_sleep")?;
    let task = harness.spawn(&selected, ()).await?;
    observed.await.expect("sleep handler starts");
    task.cancel().await?;
    assert_eq!(task.result().await?, Outcome::Cancelled);
    probe.assert_single_clear(f64::from(i32::MAX));
    Ok(())
}
