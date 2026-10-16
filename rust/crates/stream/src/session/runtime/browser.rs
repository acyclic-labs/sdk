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
