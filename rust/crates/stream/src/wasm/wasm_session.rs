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
    pub fn register_credential(
        &self,
        slot: u32,
        credential: js_sys::Uint8Array,
    ) -> Result<(), JsValue> {
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

    /// The pool's read-your-writes floor: one encoded `Revision` per authority
    /// this client has observed. Every request that accepts a minimum carries it.
    #[wasm_bindgen(js_name = revisionFloor)]
    #[must_use]
    pub fn revision_floor(&self) -> js_sys::Array {
        self.client
            .revisions()
            .floor(&crate::RevisionCut::default())
            .iter()
            .map(|revision| {
                js_sys::Uint8Array::from(
                    crate::wire_codec::revision_to_wire(revision)
                        .encode_to_vec()
                        .as_slice(),
                )
            })
            .collect()
    }

    /// Raises the floor with one encoded `Revision` a receipt or response served.
    #[wasm_bindgen(js_name = observeRevision)]
    pub fn observe_revision(&self, input: &[u8]) -> Result<(), JsValue> {
        let revision = crate::wire_codec::issued_from_wire(Some(decode::<wire::Revision>(input)?))
            .map_err(js_error)?;
        self.client.revisions().observe(&revision);
        Ok(())
    }

    /// Accepts a direct read or tail reply: `applied` (an encoded `Revision`,
    /// absent when the reply carried none) must prove the encoded `minimum`
    /// the request sent, and then raises the floor.
    #[wasm_bindgen(js_name = servedRevision)]
    pub fn served_revision(
        &self,
        minimum: js_sys::Array,
        applied: Option<Box<[u8]>>,
    ) -> Result<(), JsValue> {
        let minimum = minimum
            .iter()
            .map(|entry| {
                use wasm_bindgen::JsCast as _;
                let bytes = entry
                    .dyn_into::<js_sys::Uint8Array>()
                    .map_err(|_| js_error(StreamError::InvalidArgument))?
                    .to_vec();
                Ok(decode::<wire::Revision>(&bytes)?)
            })
            .collect::<Result<Vec<_>, JsValue>>()?;
        let minimum = crate::wire_codec::cut_from_wire(minimum).map_err(js_error)?;
        let applied = applied
            .map(|bytes| {
                crate::wire_codec::issued_from_wire(Some(decode::<wire::Revision>(&bytes)?))
                    .map_err(js_error)
            })
            .transpose()?;
        self.client
            .revisions()
            .served(&minimum, applied.as_ref())
            .map_err(js_error)
    }

    /// Drops the floor after an authority refused it as foreign.
    #[wasm_bindgen(js_name = revisionRefused)]
    pub fn revision_refused(&self) {
        self.client
            .revisions()
            .refused(&StreamError::RevisionForeign);
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
