//! Event hosting is the portable public server, not a browser turn composition.
use super::*;
use crate::filesystem::TurnLimits;
use crate::hosting::{self, Bound, Placement, RootRequest, Server, Serving, Turns};
use acyclic_actors::{domain::ActorLimits, wire};
use acyclic_auth::{authority::PolicyDecision, contract::Principal};
use acyclic_harness_client::{AgentBinding, AgentCommand, PreparedCommand};
use acyclic_stream::{BoxProviderFuture, session::Client};
use bytes::Bytes;
use prost::Message as _;
use std::cell::RefCell;

type Binding = Bound<dyn StreamProvider, Objects>;
type Hosted = Server<dyn StreamProvider, Objects>;

struct Policy {
    policy: Function,
    admitted: Function,
}

impl hosting::Platform for Policy {
    fn policy<'a>(
        &'a self,
        principal: &'a Principal,
        command: &'a AgentCommand,
    ) -> BoxProviderFuture<'a, std::result::Result<PolicyDecision, StreamError>> {
        Box::pin(async move {
            let cancel =
                crate::wasm::cancel::Cancel::new().map_err(|_| StreamError::Unavailable)?;
            let principal =
                acyclic_auth::contract::PrincipalProto::from(principal.clone()).encode_to_vec();
            let command = to_js(command).map_err(|_| StreamError::InvalidArgument)?;
            let result = self
                .policy
                .call3(
                    &JsValue::UNDEFINED,
                    &js_sys::Uint8Array::from(principal.as_slice()),
                    &command,
                    &cancel.signal,
                )
                .map_err(|_| StreamError::Unavailable)?;
            let result = wasm_bindgen_futures::JsFuture::from(js_sys::Promise::resolve(&result))
                .await
                .map_err(|_| StreamError::Unavailable)?;
            match result.as_bool() {
                Some(true) => Ok(PolicyDecision::Allow),
                Some(false) => Ok(PolicyDecision::Deny),
                None => Err(StreamError::InvalidResponse),
            }
        })
    }
    fn runs_children(&self) -> bool {
        // This event binding has no admitted child placement/wake producer.
        false
    }
    fn admitted(&self) -> BoxProviderFuture<'_, std::result::Result<(), StreamError>> {
        Box::pin(async {
            let cancel =
                crate::wasm::cancel::Cancel::new().map_err(|_| StreamError::Unavailable)?;
            let result = self
                .admitted
                .call1(&JsValue::UNDEFINED, &cancel.signal)
                .map_err(|_| StreamError::Unavailable)?;
            wasm_bindgen_futures::JsFuture::from(js_sys::Promise::resolve(&result))
                .await
                .map_err(|_| StreamError::Unavailable)?;
            Ok(())
        })
    }
}

/// The shared admitted server driven by its platform's events and native alarms.
#[wasm_bindgen]
pub struct WasmAgentServer {
    bound: Binding,
    observation: WasmAgents,
    server: RefCell<Option<Arc<Hosted>>>,
    pending: RefCell<Option<hosting::Unsettled<dyn StreamProvider, Objects>>>,
    platform: Arc<dyn hosting::Platform>,
    authority: Arc<acyclic_stream::session::Authority>,
    binding: Vec<u8>,
    lifetime: RefCell<Option<tokio::sync::watch::Sender<()>>>,
}

#[wasm_bindgen]
impl WasmAgentServer {
    /// Bind the opener's verified admission once. The guest uses the same
    /// providers, preset and admission; the host retains all publication.
    pub fn open(
        providers: &WasmAgentProviders,
        #[wasm_bindgen(unchecked_param_type = "WasmBrowserAgentOptions")] options: JsValue,
        owner: &WasmAgentAuthority,
        registry: &WasmAgentRegistry,
        authority: &acyclic_stream::WasmSessionAuthority,
        #[wasm_bindgen(unchecked_param_type = "WasmHostingPlatform")] platform: JsValue,
    ) -> std::result::Result<WasmAgentServer, JsValue> {
        let options: WasmBrowserAgentOptions = from_js(options)?;
        let limits = options.limits;
        let field = |name: &str| js_sys::Reflect::get(&platform, &JsValue::from_str(name));
        let manifest = field("manifest")?.dyn_into::<js_sys::Uint8Array>()?;
        let manifest = snapshot(&manifest)?;
        let on_record = field("onRecord")?.dyn_into::<Function>()?;
        let policy = field("policy")?.dyn_into::<Function>()?;
        let admitted = field("admitted")?.dyn_into::<Function>()?;
        let recipient = &owner.caller.audience;
        let namespace = String::from_utf8(recipient.ns.clone())
            .map_err(|_| js_error(Error::Invalid("recipient namespace must be UTF-8".into())))?;
        let within = owner.within()?;
        let binding = wire::InvokeActorRequest {
            actor_id: "root".into(),
            method: "POST".into(),
            url: "https://agents.invalid/command".into(),
            namespace: Some(namespace),
            account_id: recipient.account.clone(),
            ..Default::default()
        }
        .encode_to_vec();
        let bound = Bound::platform_admitted(
            owner.issuer.clone(),
            Placement {
                filesystem: providers.providers.placement.clone(),
                provider: options.filesystem_provider.clone(),
                within,
            },
            Turns {
                executors: Arc::new(Providers::executors(&options, registry)?),
                limits: TurnLimits {
                    maximum_bytes: limits.file_bytes,
                    max_steps: u32::try_from(limits.model_steps).map_err(|_| {
                        js_error(Error::Invalid("model step limit exceeds u32".into()))
                    })?,
                    history_turns: HISTORY_TURNS,
                },
                actors: ActorLimits::new(
                    options.handler_timeout_ms.unwrap_or(TURN_BACKSTOP_MS),
                    1 << 20,
                    1 << 16,
                )
                .map_err(|error| js_error(Error::Invalid(error.to_string())))?,
                clock: Arc::new(SystemClock),
                approval_policy: options.preset.approval_policy.clone(),
            },
        )
        .map_err(agent_error)?;
        let manifest = acyclic_actors::guest::wire::decode_manifest(&manifest)
            .map_err(|error| js_error(Error::Invalid(error.to_string())))?;
        let bound = bound
            .hosting(Arc::new(acyclic_actors::guest::wire::Remote::new(
                manifest,
                guest::call(on_record),
            )))
            .map_err(agent_error)?;
        let observation = WasmAgents {
            agents: bound.agents().clone(),
            filesystem: bound.volumes().clone(),
            provider: options.filesystem_provider,
            issuer: owner.issuer.clone(),
            admission: owner.admission()?,
            sender: Sender::User {
                principal: options.principal,
            },
            maximum_bytes: options.limits.file_bytes,
            run: None,
        };
        Ok(Self {
            bound,
            observation,
            server: RefCell::new(None),
            pending: RefCell::new(None),
            platform: Arc::new(Policy { policy, admitted }),
            authority: authority.authority(),
            binding,
            lifetime: RefCell::new(Some(tokio::sync::watch::channel(()).0)),
        })
    }

    /// Canonical public address, for the unchanged harness client codec.
    pub fn binding(&self) -> Vec<u8> {
        self.binding.clone()
    }

    #[wasm_bindgen(js_name = validateCreate)]
    pub fn validate_create(
        &self,
        intent: &str,
        brief: Option<String>,
    ) -> std::result::Result<String, JsValue> {
        let path = self.observation.validate_create(intent, brief)?;
        let root = AgentPath::try_from(path.clone()).map_err(agent_error)?;
        self.bound
            .admit_root(&self.root_request(root.id())?)
            .map_err(agent_error)?;
        Ok(path)
    }

    #[wasm_bindgen(js_name = wakePaths)]
    pub fn wake_paths(&self, agent: String) -> std::result::Result<JsValue, JsValue> {
        self.observation.wake_paths(agent)
    }

    #[wasm_bindgen(js_name = validateAdopt)]
    pub async fn validate_adopt(&self, agent: String) -> std::result::Result<(), JsValue> {
        self.owned(async {
            let path = AgentPath::try_from(agent.clone()).map_err(agent_error)?;
            self.bound.admit_path(&path).map_err(agent_error)?;
            let state = self
                .bound
                .agents()
                .state(&path)
                .await
                .map_err(agent_error)?;
            self.bound
                .admit_agent(&path, &state.volumes)
                .map_err(agent_error)
        })
        .await
    }

    /// Root volumes and adoption are the one shared atomic root submission.
    pub async fn create(&self, intent: String) -> std::result::Result<String, JsValue> {
        self.owned(async {
            let root =
                AgentPath::try_from(self.validate_create(&intent, None)?).map_err(agent_error)?;
            if let Some(server) = self.server.borrow().as_ref() {
                if server.root() != &root {
                    return Err(JsValue::from_str("served root conflicts"));
                }
                return Ok(root.to_string());
            }
            let pending = self.pending.borrow_mut().take();
            let serving = if let Some(pending) = pending {
                if pending.prepared().child() != &root {
                    *self.pending.borrow_mut() = Some(pending);
                    return Err(JsValue::from_str("retry the original unsettled root"));
                }
                pending.resubmit().await.map_err(agent_error)?
            } else {
                hosting::serve(
                    self.bound.clone(),
                    self.platform.clone(),
                    &IntentId::try_from(intent).map_err(agent_error)?,
                    self.root_request(root.id())?,
                )
                .await
                .map_err(agent_error)?
            };
            match serving {
                Serving::Served(server) => {
                    *self.server.borrow_mut() = Some(server);
                    Ok(root.to_string())
                }
                Serving::Unknown(pending) => {
                    *self.pending.borrow_mut() = Some(pending);
                    Err(JsValue::from_str(
                        "root submission unknown; retry its original intent",
                    ))
                }
            }
        })
        .await
    }

    /// Explicit placement of a committed child; observing it never activates it.
    pub async fn adopt(&self, agent: String) -> std::result::Result<(), JsValue> {
        self.owned(async {
            let agent = AgentPath::try_from(agent).map_err(agent_error)?;
            let server = hosting::reopen(self.bound.clone(), self.platform.clone(), agent)
                .await
                .map_err(agent_error)?;
            *self.server.borrow_mut() = Some(server);
            Ok(())
        })
        .await
    }

    /// One canonical authenticated invocation through the shared session carrier.
    pub async fn call(
        &self,
        invocation: js_sys::Uint8Array,
        credential: js_sys::Uint8Array,
    ) -> std::result::Result<Vec<u8>, JsValue> {
        let invocation = snapshot(&invocation)?;
        let credential = snapshot(&credential)?;
        self.invoke(Bytes::from(invocation), Bytes::from(credential))
            .await
    }

    /// One Actor fetch the platform routed to this endpoint: the discovery or
    /// command URL, with the caller's original prepared command as its body.
    /// It becomes the exact envelope a session ActorCall carries and runs
    /// through the same authenticated call; the reply is the response body.
    #[wasm_bindgen(js_name = fetchCall)]
    pub async fn fetch_call(
        &self,
        url: String,
        body: js_sys::Uint8Array,
        credential: js_sys::Uint8Array,
    ) -> std::result::Result<Vec<u8>, JsValue> {
        let binding = AgentBinding::new(&self.binding).map_err(boundary_error)?;
        let invocation = binding
            .routed(&url, snapshot(&body)?)
            .map_err(boundary_error)?;
        let credential = snapshot(&credential)?;
        let reply = self.invoke(invocation, Bytes::from(credential)).await?;
        let reply = wire::InvokeActorResponse::decode(reply.as_slice()).map_err(boundary_error)?;
        Ok(reply.body)
    }

    /// Convenience for a trusted platform's own controls; uses that platform's
    /// credential through the same command codec and session dispatch.
    #[wasm_bindgen(unchecked_return_type = "AgentReply")]
    pub async fn command(
        &self,
        #[wasm_bindgen(unchecked_param_type = "AgentCommand")] command: JsValue,
        credential: js_sys::Uint8Array,
    ) -> std::result::Result<JsValue, JsValue> {
        let command: AgentCommand = from_js(command)?;
        let prepared = PreparedCommand::new(&command).map_err(boundary_error)?;
        let binding = AgentBinding::new(&self.binding).map_err(boundary_error)?;
        let invocation = binding.prepare(&prepared).map_err(boundary_error)?;
        let client = self.client(Bytes::from(snapshot(&credential)?))?;
        let reply = self
            .owned(async { invocation.execute(&client, 0).await.map_err(boundary_error) })
            .await?;
        to_js(&reply)
    }

    pub async fn drain(&self, agent: String) -> std::result::Result<Option<u64>, JsValue> {
        self.owned(async {
            self.served()?
                .drain(&AgentPath::try_from(agent).map_err(agent_error)?)
                .await
                .map_err(agent_error)
        })
        .await
    }

    /// Serves the same authenticated session as the native endpoint. Platform
    /// framing passes one canonical protobuf frame per pull. Closing the cursor
    /// cancels input, active calls and follows through the shared session owner.
    pub fn session(
        &self,
        next: Function,
    ) -> std::result::Result<acyclic_stream::WasmFollow, JsValue> {
        self.session_body(next, false)
    }

    /// Ordinary deployment HTTP/2 body, using standard gRPC envelopes. The
    /// platform publishes the returned status as terminal HTTP/2 trailers.
    #[wasm_bindgen(js_name = grpcSession)]
    pub fn grpc_session(
        &self,
        next: Function,
    ) -> std::result::Result<acyclic_stream::WasmFollow, JsValue> {
        self.session_body(next, true)
    }

    /// Stops this platform owner's sessions and commands permanently.
    pub fn close(&self) {
        let lifetime = self.lifetime.borrow_mut().take();
        drop(lifetime);
    }

    /// Fail the current delivery's waiters without disabling durable recovery.
    /// Subsequent events subscribe to a fresh generation; disposal stays final.
    #[wasm_bindgen(js_name = deliveryFailed)]
    pub fn delivery_failed(&self) {
        let previous = {
            let mut lifetime = self.lifetime.borrow_mut();
            lifetime
                .as_mut()
                .map(|current| std::mem::replace(current, tokio::sync::watch::channel(()).0))
        };
        drop(previous);
    }

    /// Bind platform metadata and queue waits to this same host owner. Dropping
    /// the call aborts its signal; a closed owner never invokes the callback.
    #[wasm_bindgen(js_name = withLifetime)]
    pub async fn with_lifetime(
        &self,
        operation: Function,
        #[wasm_bindgen(unchecked_param_type = "AbortSignal | undefined")] signal: JsValue,
    ) -> std::result::Result<JsValue, JsValue> {
        let (operation, handle) = futures::future::abortable(self.owned(async {
            let cancel = crate::wasm::cancel::Cancel::new()
                .map_err(|_| stream_error(StreamError::Unavailable))?;
            let result = operation.call1(&JsValue::UNDEFINED, &cancel.signal)?;
            wasm_bindgen_futures::JsFuture::from(js_sys::Promise::resolve(&result)).await
        }));
        let _listening = guest::Listening::abort(&signal, move || handle.abort())?;
        operation
            .await
            .map_err(|_| stream_error(StreamError::Unavailable))?
    }

    pub async fn read(
        &self,
        agent: String,
        stream: String,
        from: u64,
        limit: u32,
    ) -> std::result::Result<JsValue, JsValue> {
        self.owned(self.observation.read(agent, stream, from, limit))
            .await
    }

    #[wasm_bindgen(js_name = readFile)]
    pub async fn read_file(
        &self,
        agent: String,
        file: JsValue,
    ) -> std::result::Result<Vec<u8>, JsValue> {
        self.owned(async {
            self.bound
                .read_file(
                    &AgentPath::try_from(agent).map_err(agent_error)?,
                    &from_js(file)?,
                    0,
                    self.observation.maximum_bytes,
                )
                .await
                .map(|(bytes, _length)| bytes)
                .map_err(agent_error)
        })
        .await
    }

    pub async fn pause(&self, agent: String, paused: bool) -> std::result::Result<(), JsValue> {
        self.owned(self.observation.pause(agent, paused)).await
    }

    pub async fn archive(
        &self,
        agent: String,
        intent: String,
    ) -> std::result::Result<JsValue, JsValue> {
        self.owned(self.observation.archive(agent, intent)).await
    }
}

fn boundary_error(error: impl std::fmt::Display) -> JsValue {
    JsValue::from_str(&error.to_string())
}

impl WasmAgentServer {
    fn session_body(
        &self,
        next: Function,
        grpc: bool,
    ) -> std::result::Result<acyclic_stream::WasmFollow, JsValue> {
        let server = self.served()?;
        let serve = if grpc {
            acyclic_stream::serve_wasm_grpc_session
        } else {
            acyclic_stream::serve_wasm_session
        };
        serve(
            self.bound.agents().provider().clone(),
            self.authority.clone(),
            Some(Arc::new(self.bound.changes().map_err(agent_error)?)),
            server,
            "harness".into(),
            acyclic_stream::session::Limits::default(),
            next,
            Some({
                let mut lifetime = self.lifetime()?;
                Box::pin(async move {
                    let _ = lifetime.changed().await;
                })
            }),
        )
    }

    async fn owned<T>(
        &self,
        operation: impl std::future::Future<Output = std::result::Result<T, JsValue>>,
    ) -> std::result::Result<T, JsValue> {
        let mut lifetime = self.lifetime()?;
        let closed = lifetime.changed();
        futures::pin_mut!(closed, operation);
        match futures::future::select(closed, operation).await {
            futures::future::Either::Left(_) => Err(stream_error(StreamError::Unavailable)),
            futures::future::Either::Right((result, _)) => result,
        }
    }

    /// An in-process session on the served host, holding `credential` in
    /// slot 0.
    fn client(&self, credential: Bytes) -> std::result::Result<Client, JsValue> {
        let client = Client::in_process_with_products(
            self.bound.agents().provider().clone(),
            self.authority.clone(),
            None,
            self.served()?,
            Arc::new(acyclic_stream::runtime::PlatformExecutor),
            Some(self.lifetime()?),
        );
        client
            .register_credential(0, credential)
            .map_err(stream_error)?;
        Ok(client)
    }

    async fn invoke(
        &self,
        invocation: Bytes,
        credential: Bytes,
    ) -> std::result::Result<Vec<u8>, JsValue> {
        self.client(credential)?
            .actor_call(
                &invocation,
                0,
                acyclic_harness_client::MAX_AGENT_REPLY_BYTES,
            )
            .await
            .map(|reply| Vec::from(reply.as_ref()))
            .map_err(stream_error)
    }
    fn root_request(&self, agent: AgentId) -> std::result::Result<RootRequest, JsValue> {
        let volume = private_volume(self.observation.provider.clone(), &agent).map_err(js_error)?;
        Ok(RootRequest {
            profile: None,
            agent,
            fresh: vec![VolumeBinding {
                name: PRIVATE_VOLUME.into(),
                volume,
            }],
            retained: Vec::new(),
            brief: None,
            policy: None,
        })
    }
    fn served(&self) -> std::result::Result<Arc<Hosted>, JsValue> {
        if self.lifetime.borrow().is_none() {
            return Err(stream_error(StreamError::Unavailable));
        }
        self.server
            .borrow()
            .clone()
            .ok_or_else(|| JsValue::from_str("complete the original root placement first"))
    }

    fn lifetime(&self) -> std::result::Result<tokio::sync::watch::Receiver<()>, JsValue> {
        self.lifetime
            .borrow()
            .as_ref()
            .map(tokio::sync::watch::Sender::subscribe)
            .ok_or_else(|| stream_error(StreamError::Unavailable))
    }
}

fn snapshot(view: &js_sys::Uint8Array) -> std::result::Result<Vec<u8>, JsValue> {
    let (bytes, ()) = acyclic_stream::javascript_bytes::snapshot(view, |length| {
        if length > acyclic_stream::MAX_COMMAND_BYTES as u64 {
            Err(stream_error(StreamError::Capacity))
        } else {
            Ok(())
        }
    })?;
    Ok(bytes)
}

#[wasm_bindgen(typescript_custom_section)]
const PLATFORM: &str = r#"
/** Trusted platform primitives; admitted runs after credential/current policy
 * checks and before dispatch, to retain the platform's native recovery wake. */
export interface WasmHostingPlatform {
    manifest: Uint8Array;
    onRecord(delivery: Uint8Array, signal: AbortSignal, interrupt: AbortSignal): Promise<Uint8Array>;
    policy(principal: Uint8Array, command: AgentCommand, signal: AbortSignal): boolean | Promise<boolean>;
    admitted(signal: AbortSignal): Promise<void>;
}
"#;
