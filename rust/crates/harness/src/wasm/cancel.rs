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

/// An `abort` listener on a host's signal, removed when dropped.
pub(super) struct Listening {
    signal: JsValue,
    listener: Closure<dyn FnMut()>,
}

impl Listening {
    /// Drop the operation when its owning host abandons the call.
    pub(super) async fn run<T>(
        signal: &JsValue,
        operation: impl std::future::Future<Output = T>,
    ) -> Result<T, JsValue> {
        let (operation, handle) = futures::future::abortable(operation);
        let _listening = Self::abort(signal, move || handle.abort())?;
        operation
            .await
            .map_err(|_| JsValue::from_str("operation was aborted"))
    }

    pub(super) fn abort(
        signal: &JsValue,
        mut abort: impl FnMut() + 'static,
    ) -> Result<Option<Self>, JsValue> {
        if signal.is_undefined() {
            return Ok(None);
        }
        if js_sys::Reflect::get(signal, &JsValue::from_str("aborted"))?.is_truthy() {
            abort();
            return Ok(None);
        }
        let listening = Self {
            signal: signal.clone(),
            listener: Closure::new(abort),
        };
        listening.method("addEventListener")?;
        Ok(Some(listening))
    }

    fn method(&self, name: &str) -> Result<(), JsValue> {
        js_sys::Reflect::get(&self.signal, &JsValue::from_str(name))?
            .dyn_into::<Function>()?
            .call2(
                &self.signal,
                &JsValue::from_str("abort"),
                self.listener.as_ref(),
            )
            .map(drop)
    }
}

impl Drop for Listening {
    fn drop(&mut self) {
        let _ = self.method("removeEventListener");
    }
}
