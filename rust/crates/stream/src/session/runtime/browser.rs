use futures::channel::oneshot;
use wasm_bindgen::{JsCast as _, closure::Closure};

struct Timer {
    global: web_sys::WorkerGlobalScope,
    handle: i32,
    _callback: Closure<dyn FnMut()>,
}
impl Drop for Timer {
    fn drop(&mut self) {
        self.global.clear_timeout_with_handle(self.handle);
    }
}
pub(crate) async fn delay(milliseconds: u32) {
    // Window and worker globals expose the same timer operations. Cancellation
    // unregisters the callback; no polling task or detached timeout is retained.
    let global: web_sys::WorkerGlobalScope = js_sys::global().unchecked_into();
    let (sender, receiver) = oneshot::channel();
    let callback = Closure::once(move || {
        let _ = sender.send(());
    });
    let Ok(milliseconds) = i32::try_from(milliseconds) else {
        return;
    };
    let Ok(handle) = global.set_timeout_with_callback_and_timeout_and_arguments_0(
        callback.as_ref().unchecked_ref(),
        milliseconds,
    ) else {
        return;
    };
    let _timer = Timer {
        global,
        handle,
        _callback: callback,
    };
    let _ = receiver.await;
}
