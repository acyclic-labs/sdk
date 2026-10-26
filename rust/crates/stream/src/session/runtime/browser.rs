use futures::channel::oneshot;
use wasm_bindgen::{closure::Closure, prelude::wasm_bindgen};

#[wasm_bindgen]
extern "C" {
    #[wasm_bindgen(js_name = setTimeout)]
    fn set_timeout(callback: &Closure<dyn FnMut()>, milliseconds: u32) -> wasm_bindgen::JsValue;
    #[wasm_bindgen(js_name = clearTimeout)]
    fn clear_timeout(timer: &wasm_bindgen::JsValue);
}
struct Timer {
    handle: wasm_bindgen::JsValue,
    _callback: Closure<dyn FnMut()>,
}
impl Drop for Timer {
    fn drop(&mut self) {
        clear_timeout(&self.handle);
    }
}
pub(crate) async fn delay(milliseconds: u32) {
    // Browsers return a number; Node/Bun may return an object. Preserve the
    // original handle so cancellation clears the actual runtime timer.
    let (sender, receiver) = oneshot::channel();
    let mut sender = Some(sender);
    let callback: Closure<dyn FnMut()> = Closure::new(move || {
        if let Some(sender) = sender.take() {
            let _ = sender.send(());
        }
    });
    let handle = set_timeout(&callback, milliseconds);
    let _timer = Timer {
        handle,
        _callback: callback,
    };
    let _ = receiver.await;
}

struct HostTurn {
    channel: web_sys::MessageChannel,
    _callback: Closure<dyn FnMut(web_sys::MessageEvent)>,
}
impl Drop for HostTurn {
    fn drop(&mut self) {
        self.channel.port1().set_onmessage(None);
        self.channel.port1().close();
        self.channel.port2().close();
    }
}
pub(crate) async fn yield_now() -> Result<(), crate::StreamError> {
    use wasm_bindgen::JsCast;
    let channel = web_sys::MessageChannel::new().map_err(|_| crate::StreamError::Unavailable)?;
    let (sender, receiver) = oneshot::channel();
    let mut sender = Some(sender);
    let callback = Closure::new(move |_: web_sys::MessageEvent| {
        if let Some(sender) = sender.take() {
            let _ = sender.send(());
        }
    });
    channel
        .port1()
        .set_onmessage(Some(callback.as_ref().unchecked_ref()));
    let turn = HostTurn {
        channel,
        _callback: callback,
    };
    turn.channel
        .port2()
        .post_message(&wasm_bindgen::JsValue::UNDEFINED)
        .map_err(|_| crate::StreamError::Unavailable)?;
    receiver.await.map_err(|_| crate::StreamError::Unavailable)
}
