/// Browser ABI over canonical Rust memory or durable providers.
#[wasm_bindgen]
pub struct WasmStream {
    provider: Box<dyn StreamProvider>,
}

#[wasm_bindgen]
impl WasmStream {
    #[wasm_bindgen(constructor)]
    #[must_use]
    pub fn new() -> Self {
        Self {
            provider: Box::new(MemoryStream::default()),
        }
    }

    /// Opens the canonical Rust provider on one durable `IndexedDB` journal.
    #[wasm_bindgen(js_name = openBrowser)]
    pub async fn open_browser(
        name: String,
        maximum_commands: u32,
        maximum_journal_bytes: u64,
    ) -> Result<WasmStream, JsValue> {
        let limits = crate::BrowserStreamLimits {
            commands: u64::from(maximum_commands),
            journal_bytes: maximum_journal_bytes,
            ..Default::default()
        };
        Ok(Self {
            provider: Box::new(
                crate::BrowserStream::open(&name, limits)
                    .await
                    .map_err(js_error)?,
            ),
        })
    }

    /// Executes one finite unary operation over canonical protobuf bytes.
    #[wasm_bindgen]
    pub async fn dispatch(&self, operation: &str, input: &[u8]) -> Result<Vec<u8>, JsValue> {
        check_command_size(input)?;
        let output = match operation {
            "inspect_idempotency" => {
                let request = decode::<wire::InspectIdempotencyRequest>(input)?;
                let key = crate::IdempotencyKey::new(request.idempotency_key).map_err(js_error)?;
                let response = self
                    .provider
                    .inspect_idempotency(key)
                    .await
                    .map_err(js_error)?;
                match response {
                    Some(value) => wire_codec::observation_to_wire(value).encode_to_vec(),
                    None => wire::InspectIdempotencyResponse { observation: None }.encode_to_vec(),
                }
            }
            "tail" => {
                let request = decode::<wire::TailRequest>(input)?;
                let path = wire_codec::path(request.path).map_err(js_error)?;
                let tail = self.provider.tail(path).await.map_err(js_error)?;
                wire::TailResponse { tail }.encode_to_vec()
            }
            "children_page" => {
                let request = decode::<wire::ChildrenPageRequest>(input)?;
                let request = wire_codec::children_page_from_wire(request).map_err(js_error)?;
                let page = self
                    .provider
                    .children_page(request)
                    .await
                    .map_err(js_error)?;
                wire_codec::children_page_to_wire(page).encode_to_vec()
            }
            "append" => {
                let request = decode::<wire::AppendRequest>(input)?;
                let request = wire_codec::append_from_wire(request).map_err(js_error)?;
                let response = self.provider.append(request).await.map_err(js_error)?;
                wire_codec::append_outcome_to_wire(response).encode_to_vec()
            }
            "fork" => {
                let request = decode::<wire::ForkRequest>(input)?;
                let request = wire_codec::fork_from_wire(request).map_err(js_error)?;
                let response = self.provider.fork(request).await.map_err(js_error)?;
                wire_codec::fork_receipt_to_wire(&response).encode_to_vec()
            }
            "commit" => dispatch_commit(self.provider.as_ref(), input).await?,
            "read_commit" => {
                let request = decode::<wire::ReadCommitRequest>(input)?;
                let commit_id = <[u8; 32]>::try_from(request.commit_id.as_ref())
                    .map_err(|_| js_error(StreamError::InvalidArgument))?;
                let response = self
                    .provider
                    .read_commit(crate::CommitId::from_bytes(commit_id))
                    .await
                    .map_err(js_error)?;
                wire_codec::envelope_to_wire(response).encode_to_vec()
            }
            _ => return Err(js_error(StreamError::InvalidArgument)),
        };
        Ok(output)
    }

    /// Reads one bounded page, returning encoded `ReadResponse` messages.
    #[wasm_bindgen(unchecked_return_type = "Uint8Array[]")]
    pub async fn read(&self, input: &[u8]) -> Result<JsValue, JsValue> {
        check_command_size(input)?;
        let request = decode::<wire::ReadRequest>(input)?;
        let request = wire_codec::read_from_wire(request).map_err(js_error)?;
        let records = self.provider.read(request).await.map_err(js_error)?;
        let mut output = Vec::new();
        futures::pin_mut!(records);
        while let Some(record) = records.next().await {
            let record = record.map_err(js_error)?;
            output.push(
                wire_codec::read_response_wire(vec![wire_codec::record_wire(record)])
                    .encode_to_vec(),
            );
        }
        Ok(bytes_array(output))
    }

    /// Lists one fixed-snapshot child page, returning encoded `ChildrenResponse` messages.
    #[wasm_bindgen(unchecked_return_type = "Uint8Array[]")]
    pub async fn children(&self, input: &[u8]) -> Result<JsValue, JsValue> {
        check_command_size(input)?;
        let request = decode::<wire::ChildrenRequest>(input)?;
        let request = wire_codec::children_from_wire(request).map_err(js_error)?;
        let children = self.provider.children(request).await.map_err(js_error)?;
        let mut output = Vec::new();
        futures::pin_mut!(children);
        while let Some(child) = children.next().await {
            let child = child.map_err(js_error)?;
            output.push(
                wire::ChildrenResponse {
                    child: Some(wire::Child {
                        path: child.path.to_string(),
                    }),
                }
                .encode_to_vec(),
            );
        }
        Ok(bytes_array(output))
    }

    /// Opens a live follow cursor backed by the canonical provider.
    #[wasm_bindgen]
    pub async fn open_follow(&self, input: &[u8]) -> Result<WasmFollow, JsValue> {
        check_command_size(input)?;
        let request = decode::<wire::FollowRequest>(input)?;
        let (path, from) = wire_codec::follow_from_wire(request).map_err(js_error)?;
        let stream = self.provider.follow(path, from).await.map_err(js_error)?;
        let (cancel, _) = tokio::sync::watch::channel(false);
        Ok(WasmFollow {
            state: Rc::new(FollowState {
                stream: Mutex::new(Some(stream)),
                cancel,
            }),
        })
    }
}
