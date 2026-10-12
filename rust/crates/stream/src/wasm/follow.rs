use super::*;

/// One Rust-backed live follow cursor.
#[wasm_bindgen]
pub struct WasmFollow {
    pub(super) stream: Mutex<Option<crate::RecordStream>>,
    pub(super) cancel: tokio::sync::watch::Sender<bool>,
}

#[wasm_bindgen]
impl WasmFollow {
    /// Waits for one record. Returns `null` after close or stream termination.
    #[wasm_bindgen(unchecked_return_type = "Uint8Array | null")]
    pub async fn next(&self) -> Result<JsValue, JsValue> {
        let Some(mut stream) = self.stream.lock().ok().and_then(|mut guard| guard.take()) else {
            return Ok(JsValue::NULL);
        };
        let mut cancellation = self.cancel.subscribe();
        if *cancellation.borrow() {
            return Ok(JsValue::NULL);
        }
        let next = stream.next().fuse();
        let cancelled = cancellation.changed().fuse();
        futures::pin_mut!(next, cancelled);
        match select(next, cancelled).await {
            Either::Left((Some(Ok(record)), _)) => {
                if let Ok(mut guard) = self.stream.lock() {
                    if *self.cancel.borrow() {
                        return Ok(JsValue::NULL);
                    }
                    guard.replace(stream);
                }
                Ok(JsValue::from(Uint8Array::from(
                    wire_codec::read_response_wire(vec![wire_codec::record_wire(record)])
                        .encode_to_vec()
                        .as_slice(),
                )))
            }
            Either::Left((Some(Err(error)), _)) => Err(js_error(error)),
            Either::Left((None, _)) | Either::Right((_, _)) => Ok(JsValue::NULL),
        }
    }

    /// Cancels the cursor and wakes any pending `next` call.
    #[wasm_bindgen]
    pub fn close(&self) {
        self.cancel.send_replace(true);
        if let Ok(mut guard) = self.stream.lock() {
            guard.take();
        }
    }
}
