/// Browser ABI over canonical Rust memory or durable providers.
#[wasm_bindgen]
pub struct WasmStream {
    provider: std::sync::Arc<dyn StreamProvider>,
    connections: Option<crate::session::Connections>,
    session: Option<crate::session::Client>,
}

/// Persisted capacities of an injected journal domain.
#[cfg(feature = "wasm-journal")]
#[derive(Deserialize, Tsify)]
#[serde(deny_unknown_fields)]
#[tsify(large_number_types_as_bigints)]
struct WasmJournalStreamLimits {
    commands: u32,
    journal_bytes: u64,
    paths: u32,
    path_bytes: u32,
    records: u32,
    payload_bytes: u32,
    commits: u32,
    idempotency_results: u32,
}

#[wasm_bindgen]
impl WasmStream {
    /// Pull-based finite read or atomic checked follow for an injected host
    /// port. Returns canonical SessionFrame record/progress bytes.
    #[wasm_bindgen(js_name = openProviderCursor)]
    pub async fn open_provider_cursor(
        &self,
        operation: &str,
        input: &[u8],
        preceding: &[u8],
    ) -> Result<WasmFollow, JsValue> {
        self.provider_cursor(operation, input, preceding).await
    }

    /// Opens a command journal supplied by an event host.
    #[cfg(feature = "wasm-journal")]
    #[wasm_bindgen(js_name = openJournal)]
    pub async fn open_journal(
        journal: JsValue,
        #[wasm_bindgen(unchecked_param_type = "WasmJournalStreamLimits")] limits: JsValue,
    ) -> Result<WasmStream, JsValue> {
        let limits: WasmJournalStreamLimits = serde_wasm_bindgen::from_value(limits)
            .map_err(|_| js_error(StreamError::InvalidArgument))?;
        let limits = crate::JournalStreamLimits {
            commands: limits.commands,
            journal_bytes: limits.journal_bytes,
            memory: crate::MemoryLimits {
                paths: limits.paths as usize,
                path_bytes: limits.path_bytes as usize,
                records: limits.records as usize,
                payload_bytes: limits.payload_bytes as usize,
                commits: limits.commits as usize,
                idempotency_results: limits.idempotency_results as usize,
            },
        };
        let provider = crate::JournalStream::open(journal, limits)
            .await
            .map_err(js_error)?;
        Ok(Self::from_provider(std::sync::Arc::new(provider)))
    }

    /// A canonical provider RPC port injected by a trusted host.
    #[wasm_bindgen(js_name = connectProvider)]
    pub fn connect_provider(port: JsValue) -> Result<WasmStream, JsValue> {
        Ok(Self::from_provider(std::sync::Arc::new(
            provider_rpc::InjectedStream::new(port).map_err(js_error)?,
        )))
    }

    #[wasm_bindgen(constructor)]
    #[must_use]
    /// Creates an in-process provider.
    pub fn new() -> Self {
        let provider: std::sync::Arc<dyn StreamProvider> =
            std::sync::Arc::new(MemoryStream::default());
        Self {
            connections: None,
            session: None,
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
        Ok(Self {
            connections: None,
            session: None,
            provider,
        })
    }

    /// Binds the local session to an authenticated public trust feed supplied by
    /// the owning runtime. A bare provider has no implicit session authority.
    #[wasm_bindgen(js_name = bindSession)]
    pub fn bind_session(&mut self, authority: &WasmSessionAuthority) -> Result<(), JsValue> {
        if self.session.is_some() || self.connections.is_some() {
            return Err(js_error(StreamError::InvalidArgument));
        }
        self.session = Some(crate::session::Client::in_process(
            self.provider.clone(),
            authority.authority.clone(),
            std::sync::Arc::new(crate::runtime::PlatformExecutor),
        ));
        Ok(())
    }

    /// Adds, renews or drops a local or remote session credential slot.
    #[wasm_bindgen(js_name = registerCredential)]
    pub fn register_credential(&self, slot: u32, credential: &[u8]) -> Result<(), JsValue> {
        check_command_size(credential)?;
        self.session
            .as_ref()
            .ok_or_else(|| js_error(StreamError::Unsupported))?
            .register_credential(slot, bytes::Bytes::copy_from_slice(credential))
            .map_err(js_error)
    }

    /// Connects canonical platform RPCs and shared push follows. Credentials stay live.
    #[wasm_bindgen(js_name = connect)]
    pub fn connect(endpoint: &str, credential: js_sys::Function) -> Result<WasmStream, JsValue> {
        let remote = remote::Remote::new(endpoint, credential).map_err(js_error)?;
        let session = remote.session();
        Ok(Self::from_session(std::sync::Arc::new(remote), session))
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

    /// Invokes a canonical Actors message through the existing shared session.
    /// The caller keeps its admitted command until the whole reply is received.
    #[wasm_bindgen(js_name = actorCall)]
    pub async fn actor_call(
        &self,
        invocation: &[u8],
        credential: u32,
        maximum_reply_bytes: u32,
    ) -> Result<Vec<u8>, JsValue> {
        check_command_size(invocation)?;
        self.session
            .as_ref()
            .ok_or_else(|| js_error(StreamError::Unsupported))?
            .actor_call(
                &bytes::Bytes::copy_from_slice(invocation),
                credential,
                maximum_reply_bytes as usize,
            )
            .await
            .map(|reply| reply.to_vec())
            .map_err(js_error)
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
            "heads" => {
                let request = wire_codec::heads_from_wire(decode::<wire::HeadsRequest>(input)?)
                    .map_err(js_error)?;
                let view = self.provider.heads(request).await.map_err(js_error)?;
                wire_codec::heads_to_wire(view).encode_to_vec()
            }
            "head" => {
                let request = decode::<wire::HeadRequest>(input)?;
                let path = wire_codec::path(request.path).map_err(js_error)?;
                let head = self.provider.head(path).await.map_err(js_error)?;
                wire_codec::head_to_wire(head).encode_to_vec()
            }
            "wait_applied" => {
                let request = decode::<wire::WaitAppliedRequest>(input)?;
                let at_least = wire_codec::cut_from_wire(request.at_least).map_err(js_error)?;
                let applied = self
                    .provider
                    .wait_applied(&at_least)
                    .await
                    .map_err(js_error)?;
                wire_codec::revision_to_wire(&applied).encode_to_vec()
            }
            "tail" => {
                let request = decode::<wire::TailRequest>(input)?;
                wire_codec::tail_response(&*self.provider, request)
                    .await
                    .map_err(js_error)?
                    .encode_to_vec()
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

    /// Opens a live follow cursor backed by the canonical provider.
    #[wasm_bindgen]
    pub async fn open_follow(&self, input: &[u8]) -> Result<WasmFollow, JsValue> {
        check_command_size(input)?;
        let request = wire_codec::follow_from_wire(decode::<wire::FollowRequest>(input)?)
            .map_err(js_error)?;
        if request.progress {
            return Err(js_error(StreamError::InvalidArgument));
        }
        let stream = crate::records_only(self.provider.follow(request).await.map_err(js_error)?);
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
}

impl WasmStream {
    /// Shares a host's existing provider and authenticated session pool.
    pub fn from_session(
        provider: std::sync::Arc<dyn StreamProvider>,
        session: crate::session::Client,
    ) -> Self {
        Self {
            connections: Some(session.connections()),
            session: Some(session),
            provider,
        }
    }

    /// Adapts an existing canonical provider without choosing another backend.
    pub fn from_provider(provider: std::sync::Arc<dyn StreamProvider>) -> Self {
        Self {
            provider,
            connections: None,
            session: None,
        }
    }
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
