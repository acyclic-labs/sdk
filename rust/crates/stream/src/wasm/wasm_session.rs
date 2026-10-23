/// The canonical Streams client pool with JavaScript platform I/O callbacks.
#[wasm_bindgen]
pub struct WasmSession {
    client: crate::session::Client,
    acknowledge: bool,
}

#[wasm_bindgen]
impl WasmSession {
    /// `connect(nodeBytes, signal)` supplies one binary physical transport.
    /// Its object exposes `next()`, `send(frameBytes)` and `close()` only.
    #[wasm_bindgen(constructor)]
    #[must_use]
    pub fn new(connect: js_sys::Function, acknowledge: bool) -> Self {
        Self {
            client: session_io::client(connect),
            acknowledge,
        }
    }

    /// Adds, replaces or drops a bounded credential slot on every pooled session.
    #[wasm_bindgen(js_name = registerCredential)]
    pub fn register_credential(&self, slot: u32, credential: js_sys::Uint8Array) -> Result<(), JsValue> {
        if slot as usize >= crate::session::Limits::default().channels {
            return Err(js_error(StreamError::Capacity));
        }
        if credential.length() as usize > MAX_COMMAND_BYTES {
            return Err(js_error(StreamError::LimitExceeded));
        }
        self.client
            .register_credential(slot, bytes::Bytes::from(credential.to_vec()))
            .map_err(js_error)
    }

    /// Opens a filtered subscription using its selected credential slot.
    /// Rust owns validation, credit, watermarks and recovery.
    pub fn subscribe(&self, input: &[u8]) -> Result<WasmFollow, JsValue> {
        use futures::TryStreamExt;
        check_command_size(input)?;
        let subscription = decode::<wire::SessionSubscription>(input)?;
        let client = self.client.clone();
        let acknowledge = self.acknowledge;
        let stream = futures::stream::once(async move {
            client.subscribe_with_ack(subscription, acknowledge).await
        })
        .try_flatten()
        .map(|item| item.map(|frame| frame.encode_to_vec()))
        .boxed_local();
        Ok(WasmFollow {
            state: Rc::new(FollowState::new(stream)),
        })
    }
}
