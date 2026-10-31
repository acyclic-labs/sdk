//! Browser host of the agent model: every agent of one browser profile on one
//! A4 runtime over IndexedDB Streams and Filesystem, with the same
//! [`AgentHost`] and [`FilesystemTurnHost`] the local harness composes. Each
//! control is one agent commit; observation reads the agents' streams.

use super::{WasmAgentAuthority, from_js, js_error};
use crate::{
    Error, OperationId, Result,
    agent::{
        AGENTS, AgentHandler, AgentHost, AgentId, AgentPath, Control, Delivered, ExecutorDriver,
        ForkAdopted, InboxRecord, IntentId, Message, Sender, VolumeBinding,
    },
    context::CompactionPolicy,
    conversation::{
        ContentAccess, ContentPublisher as _, ContentResidencyVerifier as _, FileRef, Limits,
        VolumeRef,
    },
    core::RuntimeAuthority,
    filesystem::{
        FilesystemContentPublisher, FilesystemContentVerifier, FilesystemHost, FilesystemTurnHost,
        PRIVATE_VOLUME, TurnAdmission, agent_authority, private_volume,
    },
    model::{Model, ModelProvider},
    resources::ProviderRef,
    tool::{Tool, ToolDefinition, ToolRegistry},
};
use acyclic_actors::{addressing::ActorReference, guest::Handler, local::SystemClock};
use acyclic_fs::StreamAuthorityStore;
use acyclic_stream::StreamProviderExt as _;
use acyclic_stream::{
    ChildrenPageRequest, CommitId, ReadRequest, StreamError, StreamPath, StreamProvider,
};
use futures::{TryStreamExt as _, future::AbortHandle};
use js_sys::Function;
use serde::{Deserialize, Serialize};
use std::sync::Arc;
use tsify::Tsify;
use wasm_bindgen::prelude::*;

mod actor_host;
mod guest;
mod host;
mod objects_port;
mod providers;
mod serving;
mod workspace;
use host::{HostModel, HostTool};
pub use objects_port::WasmObjects;
pub use providers::WasmAgentProviders;
use providers::{Objects, Providers};
pub use workspace::WasmMachinesWorkspace;

type Authorities = StreamAuthorityStore<dyn StreamProvider>;
type BrowserTurns = FilesystemTurnHost<dyn StreamProvider, Authorities, Objects>;
type BrowserAgents = AgentHost<dyn StreamProvider>;
type BrowserHandler = AgentHandler<dyn StreamProvider, ExecutorDriver<Arc<BrowserTurns>>>;

/// Settled turns of history in each turn's model context, as in the local host.
const HISTORY_TURNS: u32 = 64;
/// Backstop on one delivery, which awaits a whole turn, as in the local host.
const TURN_BACKSTOP_MS: u64 = 6 * 60 * 60 * 1000;

fn to_js<T: Serialize>(value: &T) -> std::result::Result<JsValue, JsValue> {
    value
        .serialize(
            &serde_wasm_bindgen::Serializer::new()
                .serialize_maps_as_objects(true)
                .serialize_large_number_types_as_bigints(true)
                .serialize_missing_as_null(true),
        )
        .map_err(|error| JsValue::from_str(&error.to_string()))
}

fn agent_error(error: crate::agent::Error) -> JsValue {
    JsValue::from_str(&error.to_string())
}

fn stream_error(error: StreamError) -> JsValue {
    acyclic_stream::wasm_error(error)
}

fn record_path(agent: String, stream: &str) -> std::result::Result<StreamPath, JsValue> {
    let agent = AgentPath::try_from(agent).map_err(agent_error)?;
    match stream {
        "inbox" => agent.inbox(),
        "conversation" => agent.conversation(),
        "status" => agent.status(),
        _ => return Err(js_error(Error::Invalid("unknown agent stream".into()))),
    }
    .map_err(agent_error)
}

fn digest16(domain: &str, parts: &[&[u8]]) -> [u8; 16] {
    let mut hasher = blake3::Hasher::new_derive_key(domain);
    for part in parts {
        hasher.update(part);
        hasher.update(&[0]);
    }
    let mut bytes = [0; 16];
    hasher.finalize_xof().fill(&mut bytes);
    bytes
}

#[derive(Deserialize, Tsify)]
#[serde(deny_unknown_fields)]
#[tsify(large_number_types_as_bigints)]
struct WasmBrowserStreamLimits {
    commands: u64,
    journal_bytes: u64,
    paths: u32,
    path_bytes: u32,
    records: u32,
    payload_bytes: u32,
    commits: u32,
    idempotency_results: u32,
}

#[derive(Deserialize, Tsify)]
#[serde(deny_unknown_fields)]
#[tsify(large_number_types_as_bigints)]
struct WasmBrowserAgentOptions {
    filesystem_database: String,
    stream_database: String,
    maximum_object_bytes: u64,
    stream_limits: WasmBrowserStreamLimits,
    #[tsify(type = "WasmProviderRefWire")]
    filesystem_provider: ProviderRef,
    #[tsify(type = "WasmLimitsInput")]
    limits: Limits,
    /// Principal recorded as the sender of every browser control.
    principal: String,
    /// The same executor, spawn and approval defaults as every hosted agent.
    /// It has no default: the opener chooses its complete preset explicitly.
    preset: crate::preset::Preset,
    /// Optional delivery backstop for event-owned hosting.
    #[serde(default)]
    #[tsify(optional)]
    handler_timeout_ms: Option<u64>,
}

/// Outcome of one control commit.
#[derive(Serialize, Tsify)]
#[serde(tag = "kind", rename_all = "snake_case")]
#[tsify(large_number_types_as_bigints)]
enum WasmDelivered {
    /// Committed exactly once at this inbox sequence (also on exact replay).
    Committed { inbox: u64 },
    /// The recipient is archived; nothing was written.
    Closed,
}

impl From<Delivered> for WasmDelivered {
    fn from(delivered: Delivered) -> Self {
        match delivered {
            Delivered::Committed { inbox } => Self::Committed { inbox },
            Delivered::Closed => Self::Closed,
        }
    }
}

/// One bounded page of direct child agents.
#[derive(Serialize, Tsify)]
struct WasmAgentPage {
    agents: Vec<String>,
    /// Hierarchy version to pass with `next_after` for the next page.
    version: Vec<u8>,
    next_after: Option<String>,
}

/// One record of an agent stream, decoded from its canonical JSON.
#[derive(Serialize, Tsify)]
#[tsify(large_number_types_as_bigints)]
struct WasmAgentRecord {
    sequence: u64,
    #[tsify(type = "unknown")]
    value: serde_json::Value,
}

/// Model and tools of every agent turn. Callbacks are trusted host
/// implementations, with the same contract as Rust providers and tools.
#[wasm_bindgen]
#[derive(Default)]
pub struct WasmAgentRegistry {
    tools: ToolRegistry,
    model: Option<(Model, Arc<dyn ModelProvider>, CompactionPolicy)>,
    workspace: Option<Workspace>,
}

struct Workspace {
    executor: Arc<dyn crate::tool::ToolExecutor>,
    tools: &'static [&'static str],
}

impl WasmAgentRegistry {
    /// Retains the opener's admitted workspace executor. Machines hosts pass
    /// their `MachinesWorkspace` with `["shell"]`; each turn supplies its own
    /// task authority. Only the declared tools enter model requests or grants.
    pub fn bind_workspace(
        &mut self,
        executor: Arc<dyn crate::tool::ToolExecutor>,
        tools: &'static [&'static str],
    ) {
        self.workspace = Some(Workspace { executor, tools });
    }

    /// Installs a production provider without translating its admitted requests,
    /// token counts, content custody or streamed events through JavaScript.
    pub fn bind_model(
        &mut self,
        model: Model,
        provider: Arc<dyn ModelProvider>,
        compaction: CompactionPolicy,
    ) -> Result<()> {
        model.validate()?;
        self.model = Some((model, provider, compaction));
        Ok(())
    }
}

#[wasm_bindgen]
impl WasmAgentRegistry {
    #[wasm_bindgen(constructor)]
    pub fn new() -> Self {
        Self::default()
    }

    /// Retains the platform's admitted Machines workspace. The same binding
    /// supplies shell execution to guests and the ordinary agent server.
    #[wasm_bindgen(js_name = bindMachinesWorkspace)]
    pub fn bind_machines_workspace(&mut self, workspace: &WasmMachinesWorkspace) {
        self.bind_workspace(workspace.executor(), &["shell"]);
    }

    /// Registers one immutable tool revision; turns may call it.
    #[wasm_bindgen(js_name = registerTool)]
    pub fn register_tool(
        &mut self,
        #[wasm_bindgen(unchecked_param_type = "ToolDefinition")] definition: JsValue,
        execute: Function,
        reconcile: Function,
        project: Function,
    ) -> std::result::Result<(), JsValue> {
        let definition: ToolDefinition = from_js(definition)?;
        let host = Arc::new(HostTool {
            execute,
            reconcile,
            project,
        });
        self.tools
            .register(
                (
                    Tool {
                        hook_names: &[],
                        definition,
                        executor: host.clone(),
                        projection: host,
                    },
                    None,
                ),
                &mut None,
                None,
            )
            .map_err(js_error)?;
        Ok(())
    }

    /// Selects the model of every turn. `generate` receives the canonical
    /// request bytes, its dispatch identity, an `AbortSignal` and the verified
    /// bytes of each file the request projects in full, and returns an async
    /// iterator; `reconcile` receives the exact retained attempt and never
    /// redispatches it. Capacity and counting are synchronous, effect-free
    /// provider operations; Rust validates their results.
    #[wasm_bindgen(js_name = configureModel)]
    pub fn configure_model(
        &mut self,
        #[wasm_bindgen(unchecked_param_type = "Model")] model: JsValue,
        #[wasm_bindgen(
            unchecked_param_type = "(request: Uint8Array, dispatch: ModelDispatch, signal: AbortSignal, files: Uint8Array[]) => AsyncIterator<ModelEvent>"
        )]
        generate: Function,
        #[wasm_bindgen(
            unchecked_param_type = "(attempt: ModelDispatch & { observed: ModelEvent[] }) => ModelEvent[] | null | Promise<ModelEvent[] | null>"
        )]
        reconcile: Function,
        #[wasm_bindgen(
            unchecked_param_type = "((model: Model) => ModelContextCapacity) | undefined"
        )]
        capacity: Option<Function>,
        #[wasm_bindgen(
            unchecked_param_type = "((request: Uint8Array, request_digest: number[]) => ModelTokenCount | Promise<ModelTokenCount>) | undefined"
        )]
        count_tokens: Option<Function>,
        #[wasm_bindgen(unchecked_param_type = "CompactionPolicy")] compaction: JsValue,
    ) -> std::result::Result<(), JsValue> {
        let model: Model = from_js(model)?;
        model.validate().map_err(js_error)?;
        self.bind_model(
            model,
            Arc::new(HostModel {
                generate,
                reconcile,
                capacity,
                count_tokens,
                content: None,
            }),
            from_js(compaction)?,
        )
        .map_err(js_error)
    }
}

/// Every agent of one browser profile. Opening starts the one runtime that
/// hosts them all; freeing the handle stops it. Durable state is the
/// storage its host injected (`IndexedDB` by default); reopening resumes
/// every agent's pending input.
#[wasm_bindgen]
pub struct WasmAgents {
    agents: Arc<BrowserAgents>,
    filesystem: Arc<FilesystemHost<Authorities, Objects>>,
    provider: ProviderRef,
    issuer: RuntimeAuthority,
    admission: TurnAdmission,
    sender: Sender,
    maximum_bytes: u64,
    run: Option<AbortHandle>,
}

impl Drop for WasmAgents {
    fn drop(&mut self) {
        if let Some(run) = &self.run {
            run.abort();
        }
    }
}

#[wasm_bindgen]
impl WasmAgents {
    /// Checks permanent creation input errors before an event host retains its
    /// recovery intent, returning its canonical path without provider IO.
    #[wasm_bindgen(js_name = validateCreate)]
    pub fn validate_create(
        &self,
        intent: &str,
        brief: Option<String>,
    ) -> std::result::Result<String, JsValue> {
        IntentId::try_from(intent).map_err(agent_error)?;
        if brief
            .as_ref()
            .is_some_and(|text| text.len() as u64 > self.maximum_bytes)
        {
            return Err(js_error(Error::Invalid(
                "agent brief exceeds file byte limit".into(),
            )));
        }
        Ok(AgentPath::root(&AgentId::hosted_root(intent)).to_string())
    }

    /// Checks the canonical path and its committed adoption before an event
    /// host retains permanent placement. Observing state does not activate it.
    #[wasm_bindgen(js_name = validateAdopt)]
    pub async fn validate_adopt(&self, agent: String) -> std::result::Result<(), JsValue> {
        let agent = AgentPath::try_from(agent).map_err(agent_error)?;
        self.agents.state(&agent).await.map_err(agent_error)?;
        Ok(())
    }

    /// Opens the providers and starts hosting every agent. `owner` holds the
    /// signing custody of every agent's authority. `report` receives each
    /// failed agent delivery, which stays queued.
    pub async fn open(
        #[wasm_bindgen(unchecked_param_type = "WasmBrowserAgentOptions")] options: JsValue,
        owner: &WasmAgentAuthority,
        registry: &WasmAgentRegistry,
        #[wasm_bindgen(
            unchecked_param_type = "((actor: string, error: string) => void) | undefined"
        )]
        report: Option<Function>,
    ) -> std::result::Result<WasmAgents, JsValue> {
        let options: WasmBrowserAgentOptions = from_js(options)?;
        let providers = Providers::indexed_db(&options).await?;
        let handler = providers.handler(&options, owner, registry, &owner.admission()?)?;
        Self::host(options, owner, providers, Arc::new(handler), report, true)
    }

    /// Hosts every agent of `providers` (shared with the guest) whose turns run in an agent
    /// guest artifact ([`guest::AgentGuest`]): `manifest` is the guest's
    /// canonical manifest and `onRecord` its entry. This host keeps
    /// scheduling, controls and publication.
    #[wasm_bindgen(js_name = openHosting)]
    pub fn open_hosting(
        providers: &WasmAgentProviders,
        #[wasm_bindgen(unchecked_param_type = "WasmBrowserAgentOptions")] options: JsValue,
        owner: &WasmAgentAuthority,
        manifest: &[u8],
        #[wasm_bindgen(
            unchecked_param_type = "(delivery: Uint8Array, signal: AbortSignal, interrupt: AbortSignal) => Promise<Uint8Array>"
        )]
        on_record: Function,
        #[wasm_bindgen(
            unchecked_param_type = "((actor: string, error: string) => void) | undefined"
        )]
        report: Option<Function>,
    ) -> std::result::Result<WasmAgents, JsValue> {
        let options: WasmBrowserAgentOptions = from_js(options)?;
        let manifest = acyclic_actors::guest::wire::decode_manifest(manifest)
            .map_err(|error| js_error(Error::Invalid(error.to_string())))?;
        let handler = acyclic_actors::guest::wire::Remote::new(manifest, guest::call(on_record));
        Self::host(
            options,
            owner,
            providers.providers.clone(),
            Arc::new(handler),
            report,
            true,
        )
    }

    /// Runs the explicitly hosted agent to quiescence and returns its committed
    /// undelivered alarm. A failed delivery retains its input and exact retry.
    /// Releasing the idle registration closes event-owned follow cursors.
    pub async fn drain(&self, agent: String) -> std::result::Result<Option<u64>, JsValue> {
        if self.run.is_some() {
            return Err(js_error(Error::Invalid(
                "drain requires an event-owned host".into(),
            )));
        }
        let agent = AgentPath::try_from(agent).map_err(agent_error)?;
        self.agents.drain_event(&agent).await.map_err(agent_error)
    }

    /// Durable wake subscriptions for one event-owned agent: its control state
    /// and inbox. Hosts route only these paths, so placement claims cannot
    /// produce unrelated actors' wakes.
    #[wasm_bindgen(js_name = wakePaths, unchecked_return_type = "string[]")]
    pub fn wake_paths(&self, agent: String) -> std::result::Result<JsValue, JsValue> {
        let agent = AgentPath::try_from(agent).map_err(agent_error)?;
        let actor = self.agents.actor(&agent).map_err(agent_error)?;
        let state = self
            .agents
            .runtime()
            .store()
            .path(&actor)
            .map_err(|error| js_error(Error::Storage(error.to_string())))?;
        to_js(&vec![
            state.to_string(),
            agent.inbox().map_err(agent_error)?.to_string(),
        ])
    }
}

impl WasmAgents {
    /// Hosts every agent of `providers` on one runtime running `handler`.
    fn host(
        options: WasmBrowserAgentOptions,
        owner: &WasmAgentAuthority,
        providers: Providers,
        handler: Arc<dyn Handler>,
        report: Option<Function>,
        background: bool,
    ) -> std::result::Result<WasmAgents, JsValue> {
        options.limits.validate(&mut None).map_err(js_error)?;
        let composer = providers.spawn_volumes();
        let Providers {
            stream, filesystem, ..
        } = providers;
        let agents = Arc::new(
            AgentHost::hosting(
                stream,
                owner.within()?,
                handler,
                Arc::new(SystemClock),
                Some(composer),
            )
            .map_err(agent_error)?,
        );
        let run = if background {
            let runner = Arc::clone(&agents);
            let (run, handle) = futures::future::abortable(async move {
                let report = |actor: &ActorReference, error: &acyclic_actors::local::Error| {
                    if let Some(report) = &report {
                        let _ = report.call2(
                            &JsValue::UNDEFINED,
                            &JsValue::from_str(&format!("{actor:?}")),
                            &JsValue::from_str(&error.to_string()),
                        );
                    }
                };
                let Err(error) = runner.run(&report).await;
                tracing::error!(%error, "browser agent runtime stopped");
            });
            wasm_bindgen_futures::spawn_local(async move {
                let _ = run.await;
            });
            Some(handle)
        } else {
            None
        };
        Ok(Self {
            agents,
            filesystem,
            provider: options.filesystem_provider,
            issuer: owner.issuer.clone(),
            admission: owner.admission()?,
            sender: Sender::User {
                principal: options.principal,
            },
            maximum_bytes: options.limits.file_bytes,
            run,
        })
    }
}

#[wasm_bindgen]
impl WasmAgents {
    /// Creates root agent `intent` with its private volume and an optional
    /// brief, its first turn. Retrying the same intent and brief replays
    /// exactly and returns the same agent path.
    pub async fn create(
        &self,
        intent: String,
        brief: Option<String>,
    ) -> std::result::Result<String, JsValue> {
        let path = AgentPath::try_from(self.validate_create(&intent, brief.clone())?)
            .map_err(agent_error)?;
        let id = path.id();
        let volume = private_volume(self.provider.clone(), &id).map_err(js_error)?;
        self.filesystem
            .create_volume(&volume)
            .await
            .map_err(js_error)?;
        let brief = match brief {
            Some(text) => Some(self.stage(&path, &volume, "brief", &intent, &text).await?),
            None => None,
        };
        let created = self
            .agents
            .create(
                &IntentId::try_from(intent).map_err(agent_error)?,
                ForkAdopted {
                    agent: id,
                    parent: None,
                    forked_at: 0,
                    conversation_tail: 0,
                    checkpoint: None,
                    volumes: vec![VolumeBinding {
                        name: PRIVATE_VOLUME.to_owned(),
                        volume,
                    }],
                    brief,
                    policy: None,
                },
            )
            .await
            .map_err(agent_error)?;
        Ok(created.to_string())
    }

    /// Sends `text` to `agent` as message `intent`: one staged body file on its
    /// private volume, then one inbox commit. Retrying the intent replays exactly.
    #[wasm_bindgen(unchecked_return_type = "WasmDelivered")]
    pub async fn send(
        &self,
        agent: String,
        intent: String,
        text: String,
    ) -> std::result::Result<JsValue, JsValue> {
        let path = AgentPath::try_from(agent).map_err(agent_error)?;
        // An archived agent refuses every message: stage no body for one.
        match self
            .agents
            .provider()
            .tail(path.closed().map_err(agent_error)?)
            .await
        {
            Err(StreamError::NotFound) => {}
            Ok(_) => return to_js(&WasmDelivered::Closed),
            Err(error) => return Err(stream_error(error)),
        }
        let volume = self.private(&path).await?;
        let body = self
            .stage(&path, &volume, "message", &intent, &text)
            .await?;
        let record = InboxRecord::Message(Message {
            from: self.sender.clone(),
            id: IntentId::try_from(intent).map_err(agent_error)?,
            body: Box::new(body),
            attachments: None,
        });
        self.deliver(self.agents.send(&path, &self.sender, &record).await)
    }

    /// Stops `agent`'s current turn; the agent stays open.
    #[wasm_bindgen(unchecked_return_type = "WasmDelivered")]
    pub async fn cancel(
        &self,
        agent: String,
        intent: String,
    ) -> std::result::Result<JsValue, JsValue> {
        let path = AgentPath::try_from(agent).map_err(agent_error)?;
        let record = InboxRecord::Cancel(Control {
            by: self.sender.clone(),
            id: IntentId::try_from(intent).map_err(agent_error)?,
        });
        self.deliver(self.agents.send(&path, &self.sender, &record).await)
    }

    /// Closes `agent` and, through its cascade, its open descendants.
    #[wasm_bindgen(unchecked_return_type = "WasmDelivered")]
    pub async fn archive(
        &self,
        agent: String,
        intent: String,
    ) -> std::result::Result<JsValue, JsValue> {
        let path = AgentPath::try_from(agent).map_err(agent_error)?;
        let id = IntentId::try_from(intent).map_err(agent_error)?;
        self.deliver(self.agents.archive(&path, &self.sender, &id).await)
    }

    /// Durably pauses or resumes `agent`. A paused agent consumes nothing.
    pub async fn pause(&self, agent: String, paused: bool) -> std::result::Result<(), JsValue> {
        let path = AgentPath::try_from(agent).map_err(agent_error)?;
        self.agents.pause(&path, paused).await.map_err(agent_error)
    }

    /// One page of the root agents (`parent` absent) or of `parent`'s children.
    /// Pass the previous page's `version` and `next_after` to continue it.
    #[wasm_bindgen(unchecked_return_type = "WasmAgentPage")]
    pub async fn list(
        &self,
        parent: Option<String>,
        after: Option<String>,
        version: Option<Vec<u8>>,
        limit: u32,
    ) -> std::result::Result<JsValue, JsValue> {
        let parent = match parent {
            Some(agent) => AgentPath::try_from(agent)
                .and_then(|agent| agent.children())
                .map_err(agent_error)?,
            None => StreamPath::new(AGENTS).map_err(stream_error)?,
        };
        let after = after
            .map(|path| StreamPath::new(path))
            .transpose()
            .map_err(stream_error)?;
        let hierarchy_version = version
            .map(|bytes| {
                <[u8; 32]>::try_from(bytes.as_slice())
                    .map(CommitId::from_bytes)
                    .map_err(|_| js_error(Error::Invalid("hierarchy version is 32 bytes".into())))
            })
            .transpose()?;
        let page = match self
            .agents
            .provider()
            .children_page(ChildrenPageRequest {
                tail_stream: None,
                parent: Some(parent),
                after,
                hierarchy_version,
                limit,
            })
            .await
        {
            Ok(page) => page,
            Err(StreamError::NotFound) => {
                return to_js(&WasmAgentPage {
                    agents: Vec::new(),
                    version: Vec::new(),
                    next_after: None,
                });
            }
            Err(error) => return Err(stream_error(error)),
        };
        to_js(&WasmAgentPage {
            agents: page
                .children
                .into_iter()
                .map(|child| child.path.as_str().to_owned())
                .collect(),
            version: Vec::from(page.hierarchy_version.as_bytes()),
            next_after: page.next_after.map(|path| path.as_str().to_owned()),
        })
    }

    /// Up to `limit` records from `from` of `agent`'s `inbox`, `conversation`
    /// or `status` stream.
    #[wasm_bindgen(unchecked_return_type = "WasmAgentRecord[]")]
    pub async fn read(
        &self,
        agent: String,
        #[wasm_bindgen(unchecked_param_type = "\"inbox\" | \"conversation\" | \"status\"")]
        stream: String,
        from: u64,
        limit: u32,
    ) -> std::result::Result<JsValue, JsValue> {
        let path = record_path(agent, &stream)?;
        let records = match self
            .agents
            .provider()
            .read(ReadRequest {
                key: None,
                path,
                from,
                limit,
                at_least: Default::default(),
            })
            .await
        {
            Ok(records) => records
                .try_collect::<Vec<_>>()
                .await
                .map_err(stream_error)?,
            Err(StreamError::NotFound) => Vec::new(),
            Err(error) => return Err(stream_error(error)),
        };
        let records = records
            .into_iter()
            .map(|record| {
                Ok(WasmAgentRecord {
                    sequence: record.sequence,
                    value: crate::agent::decode_record(&record.value).map_err(agent_error)?,
                })
            })
            .collect::<std::result::Result<Vec<_>, JsValue>>()?;
        to_js(&records)
    }

    /// Atomically replays and follows the same agent stream as `read`.
    /// The canonical Stream cursor returns encoded ReadResponse frames; close
    /// it to release the subscription and wake a pending next call.
    pub async fn follow(
        &self,
        agent: String,
        #[wasm_bindgen(unchecked_param_type = "\"inbox\" | \"conversation\" | \"status\"")]
        stream: String,
        from: u64,
    ) -> std::result::Result<acyclic_stream::WasmFollow, JsValue> {
        use prost::Message as _;
        let request = acyclic_stream::wire::FollowRequest {
            path: record_path(agent, &stream)?.to_string(),
            from,
            when_absent: acyclic_stream::wire::WhenAbsent::Wait as i32,
            ..Default::default()
        };
        acyclic_stream::WasmStream::from_provider(Arc::clone(self.agents.provider()))
            .open_follow(&request.encode_to_vec())
            .await
    }

    /// The verified bytes of `file` on one of `agent`'s volumes, such as a
    /// turn's staged output.
    #[wasm_bindgen(js_name = readFile)]
    pub async fn read_file(
        &self,
        agent: String,
        #[wasm_bindgen(unchecked_param_type = "WasmFileRefWire")] file: JsValue,
    ) -> std::result::Result<Vec<u8>, JsValue> {
        let path = AgentPath::try_from(agent).map_err(agent_error)?;
        let file: FileRef = from_js(file)?;
        let volumes = self.agents.state(&path).await.map_err(agent_error)?.volumes;
        let (issuer, scope) = self.authority(&path, &volumes).await?;
        let reader = FilesystemContentVerifier::new(
            Arc::clone(&self.filesystem),
            &issuer.verifier(&mut None).map_err(js_error)?,
            &scope,
            self.maximum_bytes,
            None,
        )
        .map_err(js_error)?;
        // Rust-owned verified bytes, copied out of the reader's buffer.
        Ok(Vec::from(
            reader
                .read(&file, self.maximum_bytes, None, None)
                .map_err(js_error)?
                .await
                .map_err(js_error)?
                .as_slice(),
        ))
    }
}

impl WasmAgents {
    fn deliver(
        &self,
        delivered: std::result::Result<Delivered, crate::agent::Error>,
    ) -> std::result::Result<JsValue, JsValue> {
        to_js(&WasmDelivered::from(delivered.map_err(agent_error)?))
    }

    /// `agent`'s private volume, as bound at its adoption.
    async fn private(&self, agent: &AgentPath) -> std::result::Result<VolumeRef, JsValue> {
        self.agents
            .state(agent)
            .await
            .map_err(agent_error)?
            .volumes
            .into_iter()
            .find(|binding| binding.name == PRIVATE_VOLUME)
            .map(|binding| binding.volume)
            .ok_or_else(|| js_error(Error::Invalid("agent has no private volume".into())))
    }

    /// The agent's conversation authority over `volumes`, as its turns hold it.
    async fn authority(
        &self,
        agent: &AgentPath,
        volumes: &[VolumeBinding],
    ) -> std::result::Result<(RuntimeAuthority, crate::core::Scope), JsValue> {
        let authority = agent_authority(&self.issuer, &self.admission, agent, volumes, &[])
            .await
            .map_err(js_error)?;
        Ok((authority.issuer, authority.scope))
    }

    /// Stages `text` once on `volume` under the agent's authority, keyed by
    /// the agent, the input `kind` and its intent.
    async fn stage(
        &self,
        agent: &AgentPath,
        volume: &VolumeRef,
        kind: &str,
        intent: &str,
        text: &str,
    ) -> std::result::Result<FileRef, JsValue> {
        let operation = OperationId::from_bytes(digest16(
            "acyclic.browser.agent.input.v1",
            &[
                agent.as_str().as_bytes(),
                kind.as_bytes(),
                intent.as_bytes(),
            ],
        ));
        let binding = VolumeBinding {
            name: PRIVATE_VOLUME.into(),
            volume: volume.clone(),
        };
        let (issuer, scope) = self
            .authority(agent, std::slice::from_ref(&binding))
            .await?;
        let publisher = FilesystemContentPublisher::new(
            Arc::clone(&self.filesystem),
            volume,
            &issuer.verifier(&mut None).map_err(js_error)?,
            &scope,
            self.maximum_bytes,
            None,
        )
        .map_err(js_error)?;
        Ok(publisher
            .stage_once(
                operation,
                &format!("inputs/{operation}.txt"),
                text.as_bytes(),
                "text/plain",
                kind,
                ContentAccess::default(),
            )
            .map_err(js_error)?
            .await
            .map_err(js_error)?
            .0)
    }
}

impl WasmAgentAuthority {
    pub(super) fn admission(&self) -> std::result::Result<TurnAdmission, JsValue> {
        let (audience, scope) = self
            .issuer
            .caller()
            .ok_or_else(|| JsValue::from_str("platform admission is required"))?;
        Ok(TurnAdmission::Admitted {
            audience: audience.clone(),
            scope: scope.clone(),
        })
    }
}
