/// Browser ABI over canonical Rust memory or durable providers.
#[wasm_bindgen]
pub struct WasmStream {
    provider: std::sync::Arc<dyn StreamProvider>,
    connections: Option<crate::session::Connections>,
    session: Option<crate::session::Client>,
}

#[wasm_bindgen]
impl WasmStream {
    #[wasm_bindgen(constructor)]
    #[must_use]
    /// Creates an in-process provider.
    pub fn new() -> Self {
        let provider: std::sync::Arc<dyn StreamProvider> =
            std::sync::Arc::new(MemoryStream::default());
        let session = crate::session::Client::in_process(
            provider.clone(),
            std::sync::Arc::new(crate::session::runtime::PlatformExecutor),
        );
        Self {
            connections: None,
            session: Some(session),
            provider,
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
        let provider: std::sync::Arc<dyn StreamProvider> = std::sync::Arc::new(
            crate::BrowserStream::open(&name, limits)
                .await
                .map_err(js_error)?,
        );
        let session = crate::session::Client::in_process(
            provider.clone(),
            std::sync::Arc::new(crate::session::runtime::PlatformExecutor),
        );
        Ok(Self {
            connections: None,
            session: Some(session),
            provider,
        })
    }

    /// Connects canonical platform RPCs and shared push follows. Credentials stay live.
    #[wasm_bindgen(js_name = connect)]
    pub fn connect(endpoint: &str, credential: js_sys::Function) -> Result<WasmStream, JsValue> {
        let remote = remote::Remote::new(endpoint, credential).map_err(js_error)?;
        Ok(Self {
            connections: Some(remote.connections()),
            session: Some(remote.session()),
            provider: std::sync::Arc::new(remote),
        })
    }

    /// Pushes the next successful handshake, including reconnects without further records.
    #[wasm_bindgen(js_name = nextConnection)]
    pub async fn next_connection(&self, after: u64) -> Result<u64, JsValue> {
        self.connections
            .as_ref()
            .ok_or_else(|| js_error(StreamError::Unsupported))?
            .changed(after)
            .await
            .map_err(js_error)
    }

    /// Opens a canonical SessionSubscription, yielding encoded SessionFrame data
    /// and watermarks. Rust owns IDs, ALL/LATEST cursors, anchors and bounded credit.
    #[wasm_bindgen(js_name = openData)]
    pub async fn open_data(&self, input: &[u8]) -> Result<WasmFollow, JsValue> {
        check_command_size(input)?;
        let subscription = decode::<wire::SessionSubscription>(input)?;
        let session = self
            .session
            .as_ref()
            .ok_or_else(|| js_error(StreamError::Unsupported))?;
        let frames = session.subscribe(subscription).await.map_err(js_error)?;
        Ok(WasmFollow {
            state: Rc::new(FollowState::new(
                frames
                    .map(|frame| frame.map(|frame| frame.encode_to_vec()))
                    .boxed_local(),
            )),
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
                if request.accepted {
                    let marks = self.provider.watermarks(path).await.map_err(js_error)?;
                    wire::TailResponse {
                        tail: marks.accepted,
                        durable: Some(marks.durable),
                    }
                    .encode_to_vec()
                } else {
                    wire::TailResponse {
                        tail: self.provider.tail(path).await.map_err(js_error)?,
                        durable: None,
                    }
                    .encode_to_vec()
                }
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
        let children = self
            .provider
            .children_page(crate::ChildrenPageRequest {
                parent: request.parent,
                limit: request.limit,
                after: None,
                hierarchy_version: None,
            })
            .await
            .map_err(js_error)?
            .children;
        let mut output = Vec::new();
        for child in children {
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
        Ok(WasmFollow {
            state: Rc::new(FollowState::new(
                stream
                    .map(|record| {
                        record.map(|record| {
                            wire_codec::read_response_wire(vec![wire_codec::record_wire(record)])
                                .encode_to_vec()
                        })
                    })
                    .boxed_local(),
            )),
        })
    }
    /// Opens live child discovery; each next yields one canonical ChildrenResponse.
    #[wasm_bindgen]
    pub async fn open_children(&self, input: &[u8]) -> Result<WasmFollow, JsValue> {
        check_command_size(input)?;
        let request = wire_codec::children_from_wire(decode::<wire::ChildrenRequest>(input)?)
            .map_err(js_error)?;
        let children = self.provider.children(request).await.map_err(js_error)?;
        Ok(WasmFollow {
            state: Rc::new(FollowState::new(
                children
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
                    .boxed_local(),
            )),
        })
    }
}

impl WasmStream {
    /// Physical connection generations used by Rust client recovery drivers.
    #[must_use]
    pub fn connections(&self) -> Option<crate::session::Connections> {
        self.connections.clone()
    }
    /// Shares the platform pool, including standalone durability updates.
    #[must_use]
    pub fn session_client(&self) -> Option<crate::session::Client> {
        self.session.clone()
    }
    /// Shares the exact provider with higher-level Rust WASM clients.
    #[must_use]
    pub fn provider(&self) -> std::sync::Arc<dyn StreamProvider> {
        self.provider.clone()
    }
}
