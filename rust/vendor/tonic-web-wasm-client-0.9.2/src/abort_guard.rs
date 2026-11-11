use std::{cell::RefCell, rc::Rc, time::Duration};

use js_sys::Function;
use wasm_bindgen::{
    JsCast, JsValue,
    prelude::{Closure, wasm_bindgen},
};
use web_sys::{AbortController, AbortSignal};

use crate::Error;

#[wasm_bindgen]
extern "C" {
    #[wasm_bindgen(js_name = "setTimeout")]
    fn set_timeout(handler: &Function, timeout: i32) -> JsValue;

    #[wasm_bindgen(js_name = "clearTimeout")]
    fn clear_timeout(handle: JsValue) -> JsValue;

    #[wasm_bindgen(js_namespace = performance, js_name = now)]
    fn now() -> f64;
}

/// One timer registration, rearmed only when the host's interval expires.
struct Timeout {
    handle: JsValue,
    callback: Option<Closure<dyn FnMut()>>,
    remaining: Duration,
    started: f64,
}

impl Timeout {
    fn arm(&mut self) {
        // Round up so a sub-millisecond remainder cannot expire early. Long
        // deadlines use several host intervals instead of shortening the call.
        let delay =
            self.remaining.as_millis() + u128::from(self.remaining.subsec_nanos() % 1_000_000 != 0);
        let delay = i32::try_from(delay).unwrap_or(i32::MAX);
        self.started = now();
        self.handle = set_timeout(
            self.callback.as_ref().unwrap().as_ref().unchecked_ref(),
            delay,
        );
    }
}

impl Drop for Timeout {
    fn drop(&mut self) {
        clear_timeout(self.handle.clone());
    }
}

/// A guard that cancels a fetch request when dropped.
pub struct AbortGuard {
    ctrl: AbortController,
    timeout: Option<Rc<RefCell<Timeout>>>,
}

impl AbortGuard {
    pub fn new() -> Result<Self, Error> {
        Ok(AbortGuard {
            ctrl: AbortController::new().map_err(Error::js_error)?,
            timeout: None,
        })
    }

    pub fn signal(&self) -> AbortSignal {
        self.ctrl.signal()
    }

    pub fn timeout(&mut self, timeout: Duration) {
        self.timeout = None;
        let timer = Rc::new(RefCell::new(Timeout {
            handle: JsValue::UNDEFINED,
            callback: None,
            remaining: timeout,
            started: 0.0,
        }));
        let weak = Rc::downgrade(&timer);
        let ctrl = self.ctrl.clone();
        let callback = Closure::new(move || {
            let Some(timer) = weak.upgrade() else { return };
            let expired = {
                let mut timer = timer.borrow_mut();
                let elapsed = (now() - timer.started).max(0.0);
                let elapsed = Duration::from_millis(elapsed as u64)
                    + Duration::from_nanos((elapsed.fract() * 1_000_000.0) as u64);
                timer.remaining = timer.remaining.saturating_sub(elapsed);
                if timer.remaining.is_zero() {
                    true
                } else {
                    timer.arm();
                    false
                }
            };
            // Abort listeners can drop the request synchronously. Release the
            // timer's borrow before notifying them.
            if expired {
                ctrl.abort_with_reason(&"tonic_web_wasm_client::Error::TimedOut".into());
            }
        });
        timer.borrow_mut().callback = Some(callback);
        timer.borrow_mut().arm();
        self.timeout = Some(timer);
    }
}

impl Drop for AbortGuard {
    fn drop(&mut self) {
        self.timeout = None;
        self.ctrl.abort();
    }
}
