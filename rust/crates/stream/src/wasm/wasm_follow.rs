/// One Rust-backed live follow cursor.
#[wasm_bindgen]
pub struct WasmFollow {
    state: Rc<FollowState>,
}

impl WasmFollow {
    /// Adapts a canonical record subscription to the shared browser cursor.
    pub fn from_records(records: crate::RecordStream, followed: StreamPath) -> Self {
        Self {
            state: Rc::new(FollowState::new(
                records
                    .map(move |record| {
                        record.map(|record| {
                            wire_codec::read_response_wire(vec![wire_codec::record_wire(
                                record,
                                Some(&followed),
                            )])
                            .encode_to_vec()
                        })
                    })
                    .boxed_local(),
                StreamError::Unavailable,
            )),
        }
    }
}

#[wasm_bindgen]
impl WasmFollow {
    /// Waits for one record. Returns `null` after close or stream termination.
    #[wasm_bindgen(unchecked_return_type = "Uint8Array | null")]
    pub async fn next(&self) -> Result<JsValue, JsValue> {
        Ok(match self.state.next().await.map_err(js_error)? {
            Some(bytes) => JsValue::from(Uint8Array::from(bytes.as_slice())),
            None => JsValue::NULL,
        })
    }

    /// Cancels the cursor and wakes any pending `next` call.
    #[wasm_bindgen]
    pub fn close(&self) {
        self.state.close();
    }
}
