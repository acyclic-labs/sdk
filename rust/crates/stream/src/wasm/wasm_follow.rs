/// One Rust-backed live follow cursor.
#[wasm_bindgen]
pub struct WasmFollow {
    state: Rc<FollowState>,
}

/// The original provider opening receipt and its retained pull cursor.
#[wasm_bindgen]
pub struct WasmOpened {
    opened: Vec<u8>,
    cursor: WasmFollow,
}

impl WasmOpened {
    async fn subscription(
        client: crate::session::Client,
        subscription: wire::SessionSubscription,
        acknowledge: bool,
    ) -> Result<Self, StreamError> {
        let (opened, records) = client
            .subscribe_opened_with_ack(subscription, acknowledge)
            .await?;
        Ok(Self {
            opened: opened.encode_to_vec(),
            cursor: WasmFollow {
                state: Rc::new(FollowState::new(
                    records
                        .map(|item| item.map(|frame| frame.wire().encode_to_vec()))
                        .boxed_local(),
                    StreamError::unavailable(),
                )),
            },
        })
    }
}

#[wasm_bindgen]
impl WasmOpened {
    /// Encoded canonical SessionOpened, available before pulling any records.
    pub fn opened(&self) -> Vec<u8> {
        self.opened.clone()
    }

    /// Pulls from the same cursor selected by this receipt.
    #[wasm_bindgen(unchecked_return_type = "Uint8Array | null")]
    pub async fn next(&self) -> Result<JsValue, JsValue> {
        self.cursor.next().await
    }

    /// Releases the original cursor and wakes a pending pull.
    pub fn close(&self) {
        self.cursor.close();
    }
}

impl WasmOpened {
    fn records(mut records: crate::RecordStream, path: crate::StreamPath) -> Self {
        let opened = wire_codec::opened_to_wire(&records).encode_to_vec();
        let bootstrap = records.selection.bootstrap.take().map(Ok);
        let records = futures::stream::iter(bootstrap).chain(records);
        let cursor = WasmFollow {
            state: Rc::new(FollowState::new(
                wire_codec::record_frames(records, path).boxed_local(),
                StreamError::unavailable(),
            )),
        };
        Self { opened, cursor }
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
