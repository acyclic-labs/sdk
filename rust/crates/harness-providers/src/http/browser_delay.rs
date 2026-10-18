use js_sys::{Function, Promise, Reflect};
use wasm_bindgen::{JsCast as _, JsValue};
use wasm_bindgen_futures::JsFuture;

/// Own the original handle: browsers use numbers, Node uses objects. Cancellation must clear
/// the timer before releasing its future, rather than retain a resolver for up to an hour.
struct Timeout {
    global: JsValue,
    clear: Function,
    handle: JsValue,
}

impl Drop for Timeout {
    fn drop(&mut self) {
        let _ = self.clear.call1(&self.global, &self.handle);
    }
}

pub(super) async fn delay(duration: std::time::Duration) {
    // setTimeout takes signed 32-bit milliseconds. Split longer waits, and round fractional
    // milliseconds up so Retry-After is never shortened by the conversion.
    let mut remaining = duration.as_millis() + u128::from(duration.subsec_nanos() % 1_000_000 != 0);
    while remaining != 0 {
        let interval = remaining.min(i32::MAX as u128) as u32;
        let global = js_sys::global();
        let function = |name| {
            Reflect::get(&global, &JsValue::from_str(name))
                .ok()?
                .dyn_into::<Function>()
                .ok()
        };
        let (Some(set), Some(clear)) = (function("setTimeout"), function("clearTimeout")) else {
            // No platform timer: expire rather than leave an exchange waiting forever.
            return;
        };
        let mut handle = JsValue::UNDEFINED;
        let promise = Promise::new(&mut |resolve, reject| match set.call2(
            &global,
            &resolve,
            &JsValue::from_f64(interval.into()),
        ) {
            Ok(value) => handle = value,
            Err(error) => {
                let _ = reject.call1(&JsValue::UNDEFINED, &error);
            }
        });
        let timeout = Timeout {
            global: global.into(),
            clear,
            handle,
        };
        let elapsed = JsFuture::from(promise).await;
        drop(timeout);
        if elapsed.is_err() {
            return;
        }
        remaining -= u128::from(interval);
    }
}
