use js_sys::Function;
use wasm_bindgen::{JsCast as _, prelude::*};

/// An `AbortController` whose signal a host callback receives, aborted when
/// the Rust side drops the call: the one cancellation of a JS host call.
pub(super) struct Cancel {
    controller: Option<JsValue>,
    abort: Function,
    pub(super) signal: JsValue,
}

impl Cancel {
    /// A settled operation no longer needs cancellation dispatch.
    pub(super) fn finish(&mut self) {
        self.controller = None;
    }
    pub(super) fn new() -> std::result::Result<Self, &'static str> {
        let constructor =
            js_sys::Reflect::get(&js_sys::global(), &JsValue::from_str("AbortController"))
                .ok()
                .and_then(|value| value.dyn_into::<Function>().ok())
                .ok_or("cancellation provider is unavailable")?;
        let controller = js_sys::Reflect::construct(&constructor, &js_sys::Array::new())
            .map_err(|_| "cancellation provider failed")?;
        let abort = js_sys::Reflect::get(&controller, &JsValue::from_str("abort"))
            .ok()
            .and_then(|value| value.dyn_into::<Function>().ok())
            .ok_or("cancellation provider is invalid")?;
        let signal = js_sys::Reflect::get(&controller, &JsValue::from_str("signal"))
            .map_err(|_| "cancellation signal is unavailable")?;
        Ok(Self {
            controller: Some(controller),
            abort,
            signal,
        })
    }
}

impl Cancel {
    pub(super) fn abort(&mut self) {
        if let Some(controller) = self.controller.take() {
            let _ = self.abort.call0(&controller);
        }
    }
}

impl Drop for Cancel {
    fn drop(&mut self) {
        self.abort();
    }
}
