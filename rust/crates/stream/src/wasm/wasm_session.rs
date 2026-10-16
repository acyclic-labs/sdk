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

    /// Opens an ALL/LATEST cursor; Rust owns validation, credit and recovery.
    #[wasm_bindgen(js_name = followData)]
    pub fn follow_data(&self, input: &[u8], from: u64) -> Result<WasmFollow, JsValue> {
        use futures::TryStreamExt;
        check_command_size(input)?;
        let follow = decode::<wire::SessionFollow>(input)?;
        wire_codec::session::preceding_follow(
            &follow,
            &wire::SessionCursor {
                next: from,
                opaque: bytes::Bytes::new(),
            },
        )
        .map_err(js_error)?;
        let client = self.client.clone();
        let acknowledge = self.acknowledge;
        let stream = futures::stream::once(async move {
            client.follow_data_with_ack(follow, from, acknowledge).await
        })
        .try_flatten()
        .map(|item| item.map(|data| data.encode_to_vec()))
        .boxed_local();
        Ok(WasmFollow {
            state: Rc::new(FollowState::new(stream)),
        })
    }

    /// Opens live children discovery through the same per-node session pool.
    #[wasm_bindgen]
    pub fn children(&self, input: &[u8]) -> Result<WasmFollow, JsValue> {
        use futures::TryStreamExt;
        check_command_size(input)?;
        let request = wire_codec::children_from_wire(decode::<wire::ChildrenRequest>(input)?)
            .map_err(js_error)?;
        let client = self.client.clone();
        let stream = futures::stream::once(async move { client.children(request).await })
            .try_flatten()
            .map(|child| {
                child.map(|child| {
                    wire::ChildrenResponse {
                        child: Some(wire::Child {
                            path: child.path.to_string(),
                        }),
                    }
                    .encode_to_vec()
                })
            })
            .boxed_local();
        Ok(WasmFollow {
            state: Rc::new(FollowState::new(stream)),
        })
    }
}
