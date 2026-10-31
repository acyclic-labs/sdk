//! Exercise the production browser timer against a virtual host clock.
use crate::abort_guard::AbortGuard;
use std::{cell::RefCell, rc::Rc, time::Duration};
use wasm_bindgen::prelude::*;
use wasm_bindgen_test::wasm_bindgen_test;

#[wasm_bindgen(inline_js = r#"
export function timerClock() {
  const original = {
    setTimeout: globalThis.setTimeout,
    clearTimeout: globalThis.clearTimeout,
    performance: Object.getOwnPropertyDescriptor(globalThis, "performance"),
  };
  let elapsed = 0, next = 0;
  const timers = new Map();
  globalThis.setTimeout = (fire, delay) => {
    const id = ++next;
    timers.set(id, {fire, at: elapsed + delay});
    return id;
  };
  globalThis.clearTimeout = id => timers.delete(id);
  Object.defineProperty(globalThis, "performance", {
    configurable: true, value: {now: () => elapsed},
  });
  return {
    advance(milliseconds) {
      elapsed += milliseconds;
      for (const [id, timer] of [...timers]) {
        if (timer.at <= elapsed) { timers.delete(id); timer.fire(); }
      }
    },
    pending() { return timers.size; },
    restore() {
      globalThis.setTimeout = original.setTimeout;
      globalThis.clearTimeout = original.clearTimeout;
      Object.defineProperty(globalThis, "performance", original.performance);
    },
  };
}
"#)]
extern "C" {
    type Clock;
    #[wasm_bindgen(js_name = timerClock)]
    fn timer_clock() -> Clock;
    #[wasm_bindgen(method)]
    fn advance(this: &Clock, milliseconds: f64);
    #[wasm_bindgen(method)]
    fn pending(this: &Clock) -> u32;
    #[wasm_bindgen(method)]
    fn restore(this: &Clock);
}

struct Restore(Clock);
impl Drop for Restore {
    fn drop(&mut self) {
        self.0.restore();
    }
}

#[wasm_bindgen_test]
fn long_deadlines_expire_only_at_the_selected_time_and_release_timers() {
    let clock = Restore(timer_clock());
    let interval = i32::MAX as u64;
    let mut call = AbortGuard::new().unwrap();
    let signal = call.signal();
    call.timeout(Duration::from_millis(interval + 7));
    clock.0.advance(interval as f64);
    assert!(!signal.aborted());
    assert_eq!(clock.0.pending(), 1);
    clock.0.advance(6.0);
    assert!(!signal.aborted());
    clock.0.advance(1.0);
    assert!(signal.aborted());
    assert_eq!(
        signal.reason().as_string().as_deref(),
        Some("tonic_web_wasm_client::Error::TimedOut")
    );
    assert_eq!(clock.0.pending(), 0);
    drop(call);

    let mut call = AbortGuard::new().unwrap();
    let signal = call.signal();
    call.timeout(Duration::from_millis(1));
    call.timeout(Duration::MAX);
    assert_eq!(clock.0.pending(), 1);
    clock.0.advance(interval as f64);
    assert!(!signal.aborted());
    assert_eq!(clock.0.pending(), 1);
    drop(call);
    assert!(signal.aborted());
    assert_eq!(clock.0.pending(), 0);

    // The host timer must round up, including a sub-millisecond deadline.
    let mut call = AbortGuard::new().unwrap();
    let signal = call.signal();
    call.timeout(Duration::from_nanos(1));
    clock.0.advance(0.0);
    assert!(!signal.aborted());
    clock.0.advance(1.0);
    assert!(signal.aborted());
    assert_eq!(clock.0.pending(), 0);

    // Browser abort listeners may retire the request while its timer is firing.
    let call = Rc::new(RefCell::new(Some(AbortGuard::new().unwrap())));
    let signal = call.borrow().as_ref().unwrap().signal();
    let weak = Rc::downgrade(&call);
    let retire = Closure::<dyn FnMut()>::new(move || {
        weak.upgrade().unwrap().borrow_mut().take();
    });
    signal.set_onabort(Some(retire.as_ref().unchecked_ref()));
    call.borrow_mut()
        .as_mut()
        .unwrap()
        .timeout(Duration::from_millis(1));
    clock.0.advance(1.0);
    assert!(signal.aborted());
    assert!(call.borrow().is_none());
    assert_eq!(clock.0.pending(), 0);
    signal.set_onabort(None);
}
