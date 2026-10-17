//! An AbortSignal cancels a pending Rust future and detaches its listener on drop.
use super::error;
use crate::cancellation::Cancellation;
use std::{future::Future, rc::Rc};
use wasm_bindgen::{JsCast as _, prelude::*};

struct Listener {
    signal: web_sys::AbortSignal,
    callback: Closure<dyn FnMut()>,
}
impl Drop for Listener {
    fn drop(&mut self) {
        let _ = self
            .signal
            .remove_event_listener_with_callback("abort", self.callback.as_ref().unchecked_ref());
    }
}

pub(super) async fn run<T>(
    future: impl Future<Output = Result<T, JsValue>>,
    signal: JsValue,
) -> Result<T, JsValue> {
    if signal.is_null() || signal.is_undefined() {
        return future.await;
    }
    let signal = signal
        .dyn_into::<web_sys::AbortSignal>()
        .map_err(|_| error("signal must be an AbortSignal"))?;
    let cancellation = Rc::new(Cancellation::default());
    let cancelled = cancellation.clone();
    let callback = Closure::<dyn FnMut()>::new(move || cancelled.cancel());
    signal.add_event_listener_with_callback("abort", callback.as_ref().unchecked_ref())?;
    let listener = Listener { signal, callback };
    if listener.signal.aborted() {
        cancellation.cancel();
    }
    let cancelled = futures::future::poll_fn(|cx| cancellation.poll(cx));
    futures::pin_mut!(future, cancelled);
    match futures::future::select(cancelled, future).await {
        futures::future::Either::Left(_) => Err(listener.signal.reason()),
        futures::future::Either::Right((result, _)) => result,
    }
}
