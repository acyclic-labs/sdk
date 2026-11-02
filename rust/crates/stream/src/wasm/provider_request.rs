/// One canonical provider call, including cancellable cursor registration.
/// This uses the same pull owner as finite reads and follows.
#[wasm_bindgen]
pub struct WasmProviderRequest {
    state: Rc<crate::pull::PullCursor<JsValue, JsValue>>,
}

#[wasm_bindgen]
impl WasmProviderRequest {
    /// Resolve the original call, or return null after explicit cancellation.
    #[wasm_bindgen(unchecked_return_type = "Uint8Array | WasmFollow | WasmOpened | null")]
    pub async fn next(&self) -> Result<JsValue, JsValue> {
        Ok(self.state.next().await?.unwrap_or(JsValue::NULL))
    }

    /// Drop the original pending call and wake its waiter.
    #[wasm_bindgen]
    pub fn close(&self) {
        self.state.close();
    }
}

impl WasmProviderRequest {
    fn new(request: impl std::future::Future<Output = Result<JsValue, JsValue>> + 'static) -> Self {
        Self {
            state: Rc::new(crate::pull::PullCursor::new(
                futures::stream::once(request).boxed_local(),
                js_error(StreamError::unavailable()),
            )),
        }
    }
}
