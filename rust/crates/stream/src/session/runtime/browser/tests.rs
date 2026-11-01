use super::*;
use futures::FutureExt as _;
use wasm_bindgen::JsCast as _;
use wasm_bindgen_test::wasm_bindgen_test;

#[wasm_bindgen(inline_js = "
export function removeTimer() {
    const original = Object.getOwnPropertyDescriptor(globalThis, 'setTimeout');
    if (!Reflect.deleteProperty(globalThis, 'setTimeout')) {
        throw new TypeError('cannot withdraw the original timer');
    }
    return original;
}
export function restoreTimer(original) {
    Object.defineProperty(globalThis, 'setTimeout', original);
}
")]
extern "C" {
    #[wasm_bindgen(js_name = removeTimer)]
    fn remove_timer() -> wasm_bindgen::JsValue;
    #[wasm_bindgen(js_name = restoreTimer)]
    fn restore_timer(original: &wasm_bindgen::JsValue);
}

struct TimerOwner(wasm_bindgen::JsValue);
impl Drop for TimerOwner {
    fn drop(&mut self) {
        restore_timer(&self.0);
    }
}

/// Withdraw the real host function: the original generated import produces
/// its own ReferenceError, with no substitute callback, error or timer.
#[wasm_bindgen_test]
fn original_timer_registration_loss_is_terminal_for_current_and_late_waits() {
    let _owner = TimerOwner(remove_timer());
    assert_eq!(delay(0).now_or_never(), Some(Ok(())));
    for milliseconds in [1, i32::MAX as u64 + 1] {
        let error = delay(milliseconds)
            .now_or_never()
            .expect("registration failure must not park")
            .expect_err("registration failure must not report elapsed time");
        let super::super::SleepError::Registration(cause) = error else {
            panic!("registration must retain its original host error");
        };
        assert!(cause.is_instance_of::<js_sys::ReferenceError>());
    }
}
