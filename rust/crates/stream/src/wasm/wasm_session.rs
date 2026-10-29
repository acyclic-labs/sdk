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
    pub fn new(
        connect: js_sys::Function,
        acknowledge: bool,
        revision_scope: Option<Box<[u8]>>,
    ) -> Self {
        Self {
            client: session_io::client(
                connect,
                revision_scope.map(|scope| crate::RevisionScope::new(scope.into_vec())),
            ),
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
    pub fn revision_floor(&self) -> Result<js_sys::Array, JsValue> {
        Ok(self
            .client
            .revisions()
            .floor(&crate::RevisionCut::default())
            .map_err(js_error)?
            .iter()
            .map(|revision| {
                js_sys::Uint8Array::from(
                    crate::wire_codec::revision_to_wire(revision)
                        .encode_to_vec()
                        .as_slice(),
                )
            })
            .collect())
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

    /// Captures a coherent view through the existing credited session pool.
    pub async fn heads(&self, input: &[u8], credential: u32) -> Result<Vec<u8>, JsValue> {
        check_command_size(input)?;
        let request =
            wire_codec::heads_from_wire(decode::<wire::HeadsRequest>(input)?).map_err(js_error)?;
        let view = self
            .client
            .heads(request, credential)
            .await
            .map_err(js_error)?;
        Ok(wire_codec::heads_to_wire(view).encode_to_vec())
    }

    /// Opens explicit application decisions through the same session pool.
    pub fn deliveries(&self, input: &[u8]) -> Result<WasmApplicationSubscription, JsValue> {
        use futures::TryStreamExt as _;
        check_command_size(input)?;
        let subscription = decode::<wire::SessionSubscription>(input)?;
        let client = self.client.clone();
        let source = futures::stream::once(async move {
            client
                .deliveries(subscription)
                .await
                .map(crate::session::ApplicationSubscription::into_stream)
        })
        .try_flatten()
        .boxed_local();
        Ok(WasmApplicationSubscription {
            state: Rc::new(FollowState::new(source)),
        })
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

/// Platform adapter for the canonical application-owned cursor.
#[wasm_bindgen]
pub struct WasmApplicationSubscription {
    state: Rc<FollowState<crate::session::ApplicationDelivery>>,
}

#[wasm_bindgen]
impl WasmApplicationSubscription {
    /// Waits for the preceding decision, then returns the next owned delivery.
    pub async fn next(&self) -> Result<Option<WasmApplicationDelivery>, JsValue> {
        self.state
            .next()
            .await
            .map(|delivery| delivery.map(|inner| WasmApplicationDelivery { inner }))
            .map_err(js_error)
    }
    /// Ends the interest and wakes a pending next call without application completion.
    pub fn close(&self) {
        self.state.close();
    }
}

/// One application decision; its destructor never commits its cursor.
#[wasm_bindgen]
pub struct WasmApplicationDelivery {
    inner: crate::session::ApplicationDelivery,
}

#[wasm_bindgen]
impl WasmApplicationDelivery {
    /// Encoded v1 frame; record bytes remain opaque.
    pub fn frame(&self) -> Vec<u8> {
        self.inner.frame().encode_to_vec()
    }
    /// Encoded preceding filtered progress, when present.
    pub fn before(&self) -> Option<Vec<u8>> {
        self.inner.before().map(prost::Message::encode_to_vec)
    }
    /// Encoded v1 checkpoint to persist with the consumer's decision.
    pub fn checkpoint(&self) -> Vec<u8> {
        self.inner.checkpoint().encode_to_vec()
    }
    /// Completes an already durable consumer decision.
    pub fn commit(self) -> Result<bool, JsValue> {
        self.inner.commit().map_err(js_error)
    }
    /// Completes an explicit terminal discard decision.
    pub fn discard(self) -> Result<bool, JsValue> {
        self.inner.discard().map_err(js_error)
    }
    /// Abandons custody without advancing the application checkpoint.
    pub fn close(self) {}
}
