//! Browser bindings over the ordinary task runtime and its real providers.

use super::{
    WasmLimitsInput, WasmReducer, WasmTaskRunLimitsInput, from_js, js_error, key_bytes,
    wasm_limits, wasm_run_limits,
};
use crate::{
    Admission, Error, OperationId, Result, TaskId,
    context::{ContextPipeline, ModelContextCapacity, ModelTokenCount},
    conversation::{ContentGrant, FileRef, VolumeOperation, VolumeRef},
    core::Scope,
    distributed::{SchedulerPayloadStore, WorkLease, WorkPull, Worker},
    filesystem::{
        FilesystemHost, FilesystemSchedulerPayloadStore, FilesystemTaskRuntime, StockTurnMachine,
        TaskWakeCursor, TaskWorkerAttempt, TaskWorkerOutcome,
    },
    model::{Model, ModelAttempt, ModelDispatch, ModelEvent, ModelProvider, PreparedModelRequest},
    resources::ProviderRef,
    runtime::{DurableTaskHost, RuntimeScope, TaskDefinition, TaskRegistry},
    scheduler::SessionLimits,
    tool::{
        Tool, ToolDefinition, ToolExecutor, ToolInvocation, ToolProjection, ToolRegistry,
        ToolResult,
    },
    workflow::{MachineIdentity, MachineRegistry, MachineTransition, ResumableMachine},
};
use acyclic_fs::{
    EmbeddedCapabilities, Fs,
    browser::{IndexedDbAuthorityStore, IndexedDbObjectStore},
};
use acyclic_stream::{BrowserStream, BrowserStreamLimits, MemoryLimits, StreamClient};
use js_sys::{Function, Promise};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::sync::Arc;
use tsify::Tsify;
use wasm_bindgen::prelude::*;
use wasm_bindgen_futures::JsFuture;

type BrowserRuntime =
    FilesystemTaskRuntime<BrowserStream, IndexedDbAuthorityStore, IndexedDbObjectStore>;
type BrowserFilesystem = FilesystemHost<IndexedDbAuthorityStore, IndexedDbObjectStore>;

fn to_js<T: serde::Serialize>(value: &T) -> std::result::Result<JsValue, JsValue> {
    value
        .serialize(
            &serde_wasm_bindgen::Serializer::new()
                .serialize_maps_as_objects(true)
                .serialize_large_number_types_as_bigints(true)
                .serialize_missing_as_null(true),
        )
        .map_err(|error| JsValue::from_str(&error.to_string()))
}

// Project only the admitted state FileRef. Entrypoint schemas and generic
// machine/model values must retain every literal, including descriptor-shaped
// user objects, rather than undergoing a recursive structural rewrite.
fn normalize_work_file(js: &JsValue, work: &WasmBrowserWork) -> std::result::Result<(), JsValue> {
    let (WasmBrowserWork::Unresolved { lease, .. }
    | WasmBrowserWork::Yielded { lease }
    | WasmBrowserWork::Reconciling { lease }) = work
    else {
        return Ok(());
    };
    normalize_lease_file(
        &js_sys::Reflect::get(js, &JsValue::from_str("lease"))?,
        lease,
    )
}

fn normalize_lease_file(js: &JsValue, lease: &WorkLease) -> std::result::Result<(), JsValue> {
    let field = |value: &JsValue, key| js_sys::Reflect::get(value, &JsValue::from_str(key));
    let operation = field(js, "operation")?;
    let state = field(&operation, "state")?;
    let descriptor = field(&state, "descriptor")?;
    super::set_js_field(
        &descriptor,
        "byte_length",
        &super::exact_js_number(lease.operation.state.descriptor().byte_length())?,
    )
}

fn work_to_js(work: &WasmBrowserWork) -> std::result::Result<JsValue, JsValue> {
    let js = to_js(work)?;
    normalize_work_file(&js, work)?;
    Ok(js)
}

#[derive(Serialize, Tsify)]
#[serde(tag = "kind", rename_all = "snake_case")]
enum WasmBrowserAdmission {
    Accepted { task_id: String },
    Rejected { reason: String },
    Indeterminate { operation_id: String },
}

#[derive(Serialize, Tsify)]
struct WasmBrowserWorkError {
    code: i32,
    message: String,
}

#[derive(Serialize, Tsify)]
#[serde(tag = "kind", rename_all = "snake_case")]
#[tsify(large_number_types_as_bigints)]
enum WasmBrowserRecoveredWork {
    Idle,
    Claimed {
        lease: WorkLease,
    },
    Unresolved {
        lease: WorkLease,
        error: WasmBrowserWorkError,
    },
}

#[derive(Serialize, Tsify)]
#[serde(tag = "kind", rename_all = "snake_case")]
#[tsify(large_number_types_as_bigints)]
enum WasmBrowserWork {
    Unresolved {
        lease: WorkLease,
        error: WasmBrowserWorkError,
    },
    Suspended {
        task_id: String,
        revision: u64,
    },
    Completed {
        task_id: String,
    },
    Yielded {
        lease: WorkLease,
    },
    Reconciling {
        lease: WorkLease,
    },
}

#[derive(Serialize, Tsify)]
#[tsify(large_number_types_as_bigints)]
struct WasmBrowserWake {
    #[tsify(type = "TaskWakeCursor | null")]
    cursor: Option<TaskWakeCursor>,
    through_revision: u64,
    events_read: u32,
    tasks_polled: u32,
    woken: Vec<String>,
}

#[derive(Serialize, Tsify)]
struct WasmBrowserTick {
    wake: WasmBrowserWake,
    #[tsify(type = "WasmBrowserWork | null")]
    work: Option<WasmBrowserWork>,
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
struct WasmBrowserSessionLimits {
    active_tasks: u64,
    total_tasks: u64,
    depth: u32,
    model_steps: u64,
}

#[derive(Deserialize, Tsify)]
#[serde(deny_unknown_fields)]
#[tsify(large_number_types_as_bigints)]
struct WasmBrowserTaskOptions {
    filesystem_database: String,
    stream_database: String,
    maximum_object_bytes: u64,
    stream_limits: WasmBrowserStreamLimits,
    #[tsify(type = "WasmProviderRefWire")]
    filesystem_provider: ProviderRef,
    #[tsify(type = "WasmVolumeRefWire")]
    volume: VolumeRef,
    limits: WasmLimitsInput,
    run_limits: WasmTaskRunLimitsInput,
    session_limits: WasmBrowserSessionLimits,
    concurrency: u32,
    maximum_payload_bytes: u64,
}

#[derive(Deserialize, Tsify)]
#[serde(deny_unknown_fields)]
struct WasmMachineDefinition {
    name: String,
    version: String,
    digest: Vec<u8>,
    #[tsify(type = "WasmModelJsonSchema")]
    state_schema: Value,
    #[tsify(type = "WasmModelJsonSchema")]
    input_schema: Value,
    #[tsify(type = "WasmModelJsonSchema")]
    output_schema: Value,
    requirements: Vec<String>,
}

struct HostMachine {
    identity: MachineIdentity,
    state_schema: Value,
    initialize: Function,
    transition: Function,
}
impl ResumableMachine for HostMachine {
    fn identity(&self) -> &MachineIdentity {
        &self.identity
    }
    fn state_schema(&self) -> &Value {
        &self.state_schema
    }
    fn initialize(&self, input: &Value) -> Result<Value> {
        let input =
            to_js(input).map_err(|_| Error::Invalid("machine input encoding failed".into()))?;
        let output = self
            .initialize
            .call1(&JsValue::UNDEFINED, &input)
            .map_err(|_| Error::Invalid("machine initialization failed".into()))?;
        from_js(output).map_err(|_| Error::Invalid("machine initial state is invalid".into()))
    }
    fn transition(&self, state: &Value, input: &Value) -> Result<MachineTransition> {
        let state =
            to_js(state).map_err(|_| Error::Invalid("machine state encoding failed".into()))?;
        let input =
            to_js(input).map_err(|_| Error::Invalid("machine input encoding failed".into()))?;
        let output = self
            .transition
            .call2(&JsValue::UNDEFINED, &state, &input)
            .map_err(|_| Error::Invalid("machine transition failed".into()))?;
        from_js(output).map_err(|_| Error::Invalid("machine transition is invalid".into()))
    }
}

/// Trusted host effects are explicitly composed; rejection is an uncertain
/// provider result, so the existing runner retains its reconciliation journal.
struct HostTool {
    execute: Function,
    reconcile: Function,
    project: Function,
}
impl HostTool {
    async fn call<T: serde::de::DeserializeOwned>(
        &self,
        callback: &Function,
        invocation: ToolInvocation,
    ) -> Result<T> {
        let input = to_js(&invocation)
            .map_err(|_| Error::Invalid("tool invocation encoding failed".into()))?;
        let result = callback
            .call1(&JsValue::UNDEFINED, &input)
            .map_err(|_| Error::Storage("host tool result is uncertain".into()))?;
        let result = JsFuture::from(Promise::resolve(&result))
            .await
            .map_err(|_| Error::Storage("host tool result is uncertain".into()))?;
        // The callback already ran; a malformed receipt cannot establish that
        // execution was rejected. Retain the original attempt for reconciliation.
        from_js(result).map_err(|_| Error::Storage("host tool result is uncertain".into()))
    }
}
impl ToolExecutor for HostTool {
    fn execute<'a>(
        &'a self,
        invocation: ToolInvocation,
    ) -> acyclic_stream::BoxProviderFuture<'a, Result<ToolResult>> {
        Box::pin(self.call(&self.execute, invocation))
    }
    fn reconcile<'a>(
        &'a self,
        invocation: ToolInvocation,
    ) -> acyclic_stream::BoxProviderFuture<'a, Result<Option<ToolResult>>> {
        Box::pin(self.call(&self.reconcile, invocation))
    }
}
impl ToolProjection for HostTool {
    fn project(&self, invocation: &ToolInvocation, result: &ToolResult) -> Result<Value> {
        let invocation = to_js(invocation)
            .map_err(|_| Error::Invalid("tool projection invocation encoding failed".into()))?;
        let result = to_js(result)
            .map_err(|_| Error::Invalid("tool projection result encoding failed".into()))?;
        let value = self
            .project
            .call2(&JsValue::UNDEFINED, &invocation, &result)
            .map_err(|_| Error::Invalid("tool projection failed".into()))?;
        from_js(value).map_err(|_| Error::Invalid("tool projection is invalid".into()))
    }
}

struct HostModel {
    generate: Function,
    reconcile: Function,
    capacity: Option<Function>,
    count_tokens: Option<Function>,
}

struct HostModelIterator {
    iterator: JsValue,
    controller: JsValue,
    abort: Function,
}
impl Drop for HostModelIterator {
    fn drop(&mut self) {
        let _ = self.abort.call0(&self.controller);
        if let Ok(value) = js_sys::Reflect::get(&self.iterator, &JsValue::from_str("return"))
            && let Ok(close) = value.dyn_into::<Function>()
        {
            let _ = close.call0(&self.iterator);
        }
    }
}

impl HostModel {
    fn start(
        &self,
        request: &PreparedModelRequest,
        dispatch: ModelDispatch,
    ) -> Result<HostModelIterator> {
        let constructor =
            js_sys::Reflect::get(&js_sys::global(), &JsValue::from_str("AbortController"))
                .ok()
                .and_then(|value| value.dyn_into::<Function>().ok())
                .ok_or_else(|| {
                    Error::Unsupported("browser model cancellation provider is unavailable".into())
                })?;
        let controller = js_sys::Reflect::construct(&constructor, &js_sys::Array::new())
            .map_err(|_| Error::Unsupported("browser model cancellation provider failed".into()))?;
        let abort = js_sys::Reflect::get(&controller, &JsValue::from_str("abort"))
            .ok()
            .and_then(|value| value.dyn_into::<Function>().ok())
            .ok_or_else(|| {
                Error::Unsupported("browser model cancellation provider is invalid".into())
            })?;
        let signal =
            js_sys::Reflect::get(&controller, &JsValue::from_str("signal")).map_err(|_| {
                Error::Unsupported("browser model cancellation signal is unavailable".into())
            })?;
        let mut attempt = HostModelIterator {
            iterator: JsValue::UNDEFINED,
            controller,
            abort,
        };
        attempt.iterator = self
            .generate
            .call3(
                &JsValue::UNDEFINED,
                &js_sys::Uint8Array::from(request.bytes()),
                &to_js(&dispatch)
                    .map_err(|_| Error::Invalid("model dispatch encoding failed".into()))?,
                &signal,
            )
            .map_err(|_| Error::Storage("host model dispatch is uncertain".into()))?;
        Ok(attempt)
    }
}

impl ModelProvider for HostModel {
    fn context_capacity(&self, model: &Model) -> Result<ModelContextCapacity> {
        let callback = self.capacity.as_ref().ok_or_else(|| {
            Error::Unsupported("selected model context capacity is unavailable".into())
        })?;
        let model = to_js(model)
            .map_err(|_| Error::Invalid("model capacity selection encoding failed".into()))?;
        let value = callback
            .call1(&JsValue::UNDEFINED, &model)
            .map_err(|_| Error::Invalid("model capacity callback failed".into()))?;
        let capacity: ModelContextCapacity = from_js(value)
            .map_err(|_| Error::Invalid("model capacity callback returned invalid data".into()))?;
        capacity.validate()?;
        Ok(capacity)
    }

    fn count_tokens(&self, request: &PreparedModelRequest) -> Result<ModelTokenCount> {
        let callback = self.count_tokens.as_ref().ok_or_else(|| {
            Error::Unsupported("selected model token counting is unavailable".into())
        })?;
        let digest = to_js(&request.manifest().request_digest)
            .map_err(|_| Error::Invalid("model counting digest encoding failed".into()))?;
        let value = callback
            .call2(
                &JsValue::UNDEFINED,
                &js_sys::Uint8Array::from(request.bytes()),
                &digest,
            )
            .map_err(|_| Error::Invalid("model token counting callback failed".into()))?;
        let count: ModelTokenCount = from_js(value)
            .map_err(|_| Error::Invalid("model counting callback returned invalid data".into()))?;
        count.validate(request)?;
        Ok(count)
    }

    fn generate<'a>(
        &'a self,
        request: PreparedModelRequest,
        dispatch: ModelDispatch,
    ) -> acyclic_stream::BoxProviderStream<'a, Result<ModelEvent>> {
        let request = Arc::new(request);
        // Pull exactly one event per poll. The stock executor owns admission,
        // event limits, cancellation, request identity and durable consumption.
        Box::pin(futures::stream::try_unfold(
            None,
            move |iterator: Option<HostModelIterator>| {
                let request = request.clone();
                async move {
                    let iterator = match iterator {
                        Some(iterator) => iterator,
                        None => self.start(&request, dispatch)?,
                    };
                    let next = js_sys::Reflect::get(&iterator.iterator, &JsValue::from_str("next"))
                        .ok()
                        .and_then(|value| value.dyn_into::<Function>().ok())
                        .ok_or_else(|| {
                            Error::Invalid("host model must return an async iterator".into())
                        })?;
                    let pending = next
                        .call0(&iterator.iterator)
                        .map_err(|_| Error::Storage("host model stream is uncertain".into()))?;
                    let item = JsFuture::from(Promise::resolve(&pending))
                        .await
                        .map_err(|_| Error::Storage("host model stream is uncertain".into()))?;
                    let done =
                        js_sys::Reflect::get(&item, &JsValue::from_str("done")).map_err(|_| {
                            Error::Invalid("host model iterator result is invalid".into())
                        })?;
                    if done.as_bool() == Some(true) {
                        return Ok(None);
                    }
                    if done.as_bool() != Some(false) {
                        return Err(Error::Invalid(
                            "host model iterator completion flag is invalid".into(),
                        ));
                    }
                    let value = js_sys::Reflect::get(&item, &JsValue::from_str("value"))
                        .map_err(|_| Error::Invalid("host model event is missing".into()))?;
                    let event = from_js(value)
                        .map_err(|_| Error::Invalid("host model event is invalid".into()))?;
                    Ok(Some((event, Some(iterator))))
                }
            },
        ))
    }

    fn reconcile<'a>(
        &'a self,
        attempt: ModelAttempt,
    ) -> acyclic_stream::BoxProviderFuture<'a, Result<Option<Vec<ModelEvent>>>> {
        Box::pin(async move {
            let attempt = to_js(&attempt)
                .map_err(|_| Error::Invalid("model attempt encoding failed".into()))?;
            let pending = self
                .reconcile
                .call1(&JsValue::UNDEFINED, &attempt)
                .map_err(|_| Error::Storage("host model reconciliation is uncertain".into()))?;
            let result = JsFuture::from(Promise::resolve(&pending))
                .await
                .map_err(|_| Error::Storage("host model reconciliation is uncertain".into()))?;
            from_js(result)
                .map_err(|_| Error::Invalid("host model reconciliation is invalid".into()))
        })
    }
}

/// The existing typed registries, sealed into each opened runtime. Host callbacks
/// are trusted pure machine implementations, with the same contract as Rust.
#[wasm_bindgen]
#[derive(Default)]
pub struct WasmTaskRegistry {
    tasks: TaskRegistry,
    machines: MachineRegistry,
    tools: ToolRegistry,
}
#[wasm_bindgen]
impl WasmTaskRegistry {
    #[wasm_bindgen(constructor)]
    pub fn new() -> Self {
        Self::default()
    }

    /// Callbacks share one immutable definition revision and are trusted host
    /// implementations. They receive runtime-owned invocation identities.
    #[wasm_bindgen(js_name = registerTool)]
    pub fn register_tool(
        &mut self,
        #[wasm_bindgen(unchecked_param_type = "WasmModelToolDefinitionWire")] definition: JsValue,
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
            .register(Tool {
                definition,
                executor: host.clone(),
                projection: host,
            })
            .map_err(js_error)
    }

    #[wasm_bindgen(js_name = registerMachine)]
    pub fn register_machine(
        &mut self,
        #[wasm_bindgen(unchecked_param_type = "WasmMachineDefinition")] definition: JsValue,
        initialize: Function,
        transition: Function,
    ) -> std::result::Result<(), JsValue> {
        let definition: WasmMachineDefinition = from_js(definition)?;
        let machine = Arc::new(HostMachine {
            identity: MachineIdentity {
                name: definition.name,
                version: definition.version,
                digest: key_bytes(definition.digest)?,
            },
            state_schema: definition.state_schema,
            initialize,
            transition,
        });
        let mut task = TaskDefinition::<Value, Value>::resumable(
            machine.clone(),
            definition.input_schema,
            definition.output_schema,
        )
        .map_err(js_error)?;
        for requirement in definition.requirements {
            task = task.requires(requirement).map_err(js_error)?;
        }
        self.register_definition(machine, task)
    }

    /// Registers the Rust stock-turn adapter over the ordinary model outbox.
    /// Registration starts no worker and grants no model or task authority.
    #[wasm_bindgen(js_name = registerStockTurn)]
    pub fn register_stock_turn(&mut self) -> std::result::Result<(), JsValue> {
        let machine = Arc::new(StockTurnMachine::new());
        let task = machine.wire_definition().map_err(js_error)?;
        self.register_definition(machine, task)
    }
}

impl WasmTaskRegistry {
    fn register_definition(
        &mut self,
        machine: Arc<dyn ResumableMachine>,
        task: TaskDefinition<Value, Value>,
    ) -> std::result::Result<(), JsValue> {
        // Only NotFound means this exact task key is vacant, including when an
        // occupied key has different Rust types. With exclusive synchronous
        // access, task insertion cannot conflict after machine registration.
        let identity = machine.identity();
        if !matches!(
            self.tasks
                .get_version::<Value, Value>(&identity.name, &identity.version),
            Err(Error::NotFound(_))
        ) {
            return Err(js_error(Error::Conflict(
                "task version is already registered".into(),
            )));
        }
        self.machines.register(machine).map_err(js_error)?;
        self.tasks.register(task).map_err(js_error)?;
        Ok(())
    }
}

/// Thin event-loop handle; scheduling, ownership, effects and recovery stay in
/// `FilesystemTaskRuntime`. No worker starts when this handle is opened.
#[wasm_bindgen]
pub struct WasmTaskRuntime {
    runtime: BrowserRuntime,
    filesystem: Arc<BrowserFilesystem>,
    volume: VolumeRef,
    scope: Scope,
    verifier: crate::core::AuthorityVerifier,
    maximum_payload_bytes: u64,
    model: Option<(Model, Arc<HostModel>)>,
}

#[wasm_bindgen]
impl WasmTaskRuntime {
    /// Opens existing providers with caller-supplied limits and signed authority.
    /// Volume creation and task admission remain separate explicit operations.
    pub async fn open(
        #[wasm_bindgen(unchecked_param_type = "WasmBrowserTaskOptions")] options: JsValue,
        owner: &WasmReducer,
        registry: &WasmTaskRegistry,
        signed_scope: JsValue,
    ) -> std::result::Result<WasmTaskRuntime, JsValue> {
        let options: WasmBrowserTaskOptions = from_js(options)?;
        let scope: Scope = from_js(signed_scope)?;
        let verifier = owner.issuer.verifier();
        ContentGrant::verify(&verifier, &scope, &options.volume, VolumeOperation::Write)
            .map_err(js_error)?;
        let root = RuntimeScope::new(
            scope.capabilities().clone(),
            wasm_limits(options.limits).map_err(js_error)?,
        )
        .map_err(js_error)?
        .with_run_limits(wasm_run_limits(options.run_limits).map_err(js_error)?)
        .map_err(js_error)?;
        let authority = IndexedDbAuthorityStore::open(
            &options.filesystem_database,
            options.maximum_object_bytes,
        )
        .await
        .map_err(|error| js_error(Error::Storage(error.to_string())))?;
        let objects =
            IndexedDbObjectStore::open(&options.filesystem_database, options.maximum_object_bytes)
                .await
                .map_err(|error| js_error(Error::Storage(error.to_string())))?;
        let filesystem = Arc::new(
            FilesystemHost::new(
                Fs::new(authority, objects, EmbeddedCapabilities { durable: true }),
                options.filesystem_provider,
            )
            .map_err(js_error)?,
        );
        let limits = options.stream_limits;
        let stream = StreamClient::new(Arc::new(
            BrowserStream::open(
                &options.stream_database,
                BrowserStreamLimits {
                    commands: limits.commands,
                    journal_bytes: limits.journal_bytes,
                    memory: MemoryLimits {
                        paths: limits.paths as usize,
                        path_bytes: limits.path_bytes as usize,
                        records: limits.records as usize,
                        payload_bytes: limits.payload_bytes as usize,
                        commits: limits.commits as usize,
                        idempotency_results: limits.idempotency_results as usize,
                    },
                },
            )
            .await
            .map_err(|error| js_error(Error::Storage(error.to_string())))?,
        ));
        let limits = options.session_limits;
        let runtime = FilesystemTaskRuntime::open(
            stream,
            filesystem.clone(),
            options.volume.clone(),
            verifier.clone(),
            scope.clone(),
            root,
            registry.tasks.clone(),
            registry.machines.clone(),
            registry.tools.clone(),
            SessionLimits {
                active_tasks: limits.active_tasks,
                total_tasks: limits.total_tasks,
                depth: limits.depth,
                model_steps: limits.model_steps,
            },
            options.concurrency as usize,
            options.maximum_payload_bytes,
        )
        .await
        .map_err(js_error)?;
        Ok(Self {
            runtime,
            filesystem,
            volume: options.volume,
            scope,
            verifier,
            maximum_payload_bytes: options.maximum_payload_bytes,
            model: None,
        })
    }

    /// Composes an explicit provider with the stock model command executor.
    /// Generate receives canonical request bytes, separate dispatch identity and
    /// an `AbortSignal`, and returns an async iterator;
    /// reconcile receives the exact retained attempt and never redispatches it.
    /// Optional capacity/counting callbacks are synchronous, effect-free provider
    /// operations. Counting receives exact canonical bytes and their Rust digest;
    /// Rust validates the response binding and ordered message dimensions.
    #[wasm_bindgen(js_name = configureModel)]
    pub fn configure_model(
        &mut self,
        #[wasm_bindgen(unchecked_param_type = "WasmModelWire")] model: JsValue,
        #[wasm_bindgen(
            unchecked_param_type = "(request: Uint8Array, dispatch: Pick<WasmModelAttemptWire, 'operation_id' | 'step' | 'request_digest'>, signal: AbortSignal) => AsyncIterator<WasmModelEvent>"
        )]
        generate: Function,
        #[wasm_bindgen(
            unchecked_param_type = "(attempt: WasmModelAttemptWire) => WasmModelEvent[] | null | Promise<WasmModelEvent[] | null>"
        )]
        reconcile: Function,
        #[wasm_bindgen(
            unchecked_param_type = "((model: WasmModelWire) => ModelContextCapacity) | undefined"
        )]
        capacity: Option<Function>,
        #[wasm_bindgen(
            unchecked_param_type = "((request: Uint8Array, request_digest: number[]) => ModelTokenCount) | undefined"
        )]
        count_tokens: Option<Function>,
    ) -> std::result::Result<(), JsValue> {
        let model: Model = from_js(model)?;
        model.validate().map_err(js_error)?;
        self.model = Some((
            model,
            Arc::new(HostModel {
                generate,
                reconcile,
                capacity,
                count_tokens,
            }),
        ));
        Ok(())
    }

    /// Creates only the explicitly authorized private journal volume.
    #[wasm_bindgen(js_name = initializeVolume)]
    pub async fn initialize_volume(&self) -> std::result::Result<(), JsValue> {
        ContentGrant::verify(
            &self.verifier,
            &self.scope,
            &self.volume,
            VolumeOperation::Write,
        )
        .map_err(js_error)?;
        self.filesystem
            .create_volume(&self.volume)
            .await
            .map_err(js_error)?;
        Ok(())
    }

    /// Stages an exact immutable command/input artifact through the ordinary store.
    #[wasm_bindgen(unchecked_return_type = "WasmFileRefWire")]
    pub async fn stage(
        &self,
        operation: String,
        key: String,
        bytes: Vec<u8>,
    ) -> std::result::Result<JsValue, JsValue> {
        let operation = OperationId::parse(&operation).map_err(js_error)?;
        let store = FilesystemSchedulerPayloadStore::new(
            self.filesystem.clone(),
            self.volume.clone(),
            &self.verifier,
            &self.scope,
            self.maximum_payload_bytes,
        )
        .map_err(js_error)?;
        let file: FileRef = store
            .stage(operation, &key, &bytes)
            .await
            .map_err(js_error)?;
        super::to_js_admitted(&file)
    }

    #[wasm_bindgen(unchecked_return_type = "WasmBrowserAdmission")]
    pub async fn admit(
        &self,
        operation: String,
        name: String,
        version: String,
        #[wasm_bindgen(unchecked_param_type = "WasmModelJsonValue")] input: JsValue,
        parent: Option<String>,
    ) -> std::result::Result<JsValue, JsValue> {
        let operation = OperationId::parse(&operation).map_err(js_error)?;
        let parent = parent
            .map(|id| TaskId::parse(&id))
            .transpose()
            .map_err(js_error)?;
        let definition = self
            .runtime
            .harness()
            .task_version::<Value, Value>(&name, &version)
            .map_err(js_error)?;
        let admission = self
            .runtime
            .harness()
            .admit(operation, &definition, from_js::<Value>(input)?, parent)
            .await
            .map_err(js_error)?;
        let admission = match admission {
            Admission::Accepted(task) => WasmBrowserAdmission::Accepted {
                task_id: task.identity(),
            },
            Admission::Rejected { reason } => WasmBrowserAdmission::Rejected { reason },
            Admission::Indeterminate { operation_id } => WasmBrowserAdmission::Indeterminate {
                operation_id: operation_id.to_string(),
            },
        };
        to_js(&admission)
    }

    /// Runs only this original task through the ordinary fenced worker path.
    #[wasm_bindgen(js_name = runOperation, unchecked_return_type = "WasmBrowserWork | null")]
    pub async fn run_operation(
        &self,
        #[wasm_bindgen(unchecked_param_type = "Worker")] worker: JsValue,
        task: String,
        maximum_transitions: u32,
    ) -> std::result::Result<JsValue, JsValue> {
        let worker: Worker = from_js(worker)?;
        let task = TaskId::parse(&task).map_err(js_error)?;
        match self
            .runtime
            .run_operation(&worker, task, &self.commands(), maximum_transitions)
            .await
            .map_err(js_error)?
        {
            Some(work) => work_to_js(&work_value(work)),
            None => Ok(JsValue::NULL),
        }
    }

    /// Reads the original reservation without claiming or releasing work.
    #[wasm_bindgen(js_name = recoverWork, unchecked_return_type = "WasmBrowserRecoveredWork")]
    pub async fn recover_work(&self, task: String) -> std::result::Result<JsValue, JsValue> {
        let task = TaskId::parse(&task).map_err(js_error)?;
        let work = match self
            .runtime
            .task_host()
            .recover_work(task)
            .await
            .map_err(js_error)?
        {
            WorkPull::Idle => WasmBrowserRecoveredWork::Idle,
            WorkPull::Claimed(lease) => WasmBrowserRecoveredWork::Claimed { lease },
            WorkPull::Unresolved { lease, error } => WasmBrowserRecoveredWork::Unresolved {
                lease,
                error: WasmBrowserWorkError {
                    code: error.code() as i32,
                    message: error.to_string(),
                },
            },
        };
        let js = to_js(&work)?;
        if let WasmBrowserRecoveredWork::Claimed { lease }
        | WasmBrowserRecoveredWork::Unresolved { lease, .. } = &work
        {
            normalize_lease_file(
                &js_sys::Reflect::get(&js, &JsValue::from_str("lease"))?,
                lease,
            )?;
        }
        Ok(js)
    }

    /// Executes one caller-bounded tick using the production command resolver.
    /// Missing tool/model routes stay unavailable; no fabricated default swarm.
    #[wasm_bindgen(js_name = workerTick, unchecked_return_type = "WasmBrowserTick")]
    pub async fn worker_tick(
        &self,
        #[wasm_bindgen(unchecked_param_type = "Worker")] worker: JsValue,
        #[wasm_bindgen(unchecked_param_type = "TaskWakeCursor | null")] cursor: JsValue,
        maximum_events: u32,
        maximum_transitions: u32,
    ) -> std::result::Result<JsValue, JsValue> {
        let worker: Worker = from_js(worker)?;
        let cursor: Option<TaskWakeCursor> = from_js(cursor)?;
        let tick = self
            .runtime
            .worker_tick(
                &worker,
                cursor,
                &self.commands(),
                maximum_events,
                maximum_transitions,
            )
            .await
            .map_err(js_error)?;
        let value = WasmBrowserTick {
            wake: WasmBrowserWake {
                cursor: tick.wake.cursor,
                through_revision: tick.wake.through_revision,
                events_read: tick.wake.events_read,
                tasks_polled: tick.wake.tasks_polled,
                woken: tick
                    .wake
                    .woken
                    .into_iter()
                    .map(|id| id.to_string())
                    .collect(),
            },
            work: tick.work.map(work_value),
        };
        let js = to_js(&value)?;
        if let Some(work) = &value.work {
            let js_work = js_sys::Reflect::get(&js, &JsValue::from_str("work"))?;
            normalize_work_file(&js_work, work)?;
        }
        Ok(js)
    }

    /// Resumes only the exact retained lease; another tick never substitutes it.
    #[wasm_bindgen(unchecked_return_type = "WasmBrowserWork")]
    pub async fn resume(
        &self,
        #[wasm_bindgen(unchecked_param_type = "WorkLease")] lease: JsValue,
        maximum_transitions: u32,
    ) -> std::result::Result<JsValue, JsValue> {
        let lease: WorkLease = from_js(lease)?;
        work_to_js(&work_value(
            self.runtime
                .resume_task(lease, &self.commands(), maximum_transitions)
                .await,
        ))
    }

    /// Observes only; absence does not dispatch or replay an uncertain effect.
    #[wasm_bindgen(unchecked_return_type = "Outcome<WasmModelJsonValue> | null")]
    pub async fn outcome(&self, task: String) -> std::result::Result<JsValue, JsValue> {
        let task = TaskId::parse(&task).map_err(js_error)?;
        to_js(
            &self
                .runtime
                .task_host()
                .outcome(task)
                .await
                .map_err(js_error)?,
        )
    }

    /// Publishes durable cancellation without claiming terminal completion.
    pub async fn cancel(&self, task: String) -> std::result::Result<(), JsValue> {
        let task = TaskId::parse(&task).map_err(js_error)?;
        self.runtime
            .task_host()
            .cancel(task)
            .await
            .map_err(js_error)
    }

    #[wasm_bindgen(js_name = reconcileAdmission, unchecked_return_type = "[string, ComponentIdentity] | null")]
    pub async fn reconcile_admission(
        &self,
        operation: String,
    ) -> std::result::Result<JsValue, JsValue> {
        let operation = OperationId::parse(&operation).map_err(js_error)?;
        to_js(
            &self
                .runtime
                .harness()
                .reconcile_admission(operation)
                .await
                .map_err(js_error)?,
        )
    }

    /// Returns only after receiver-owned durable bytes are verified. A lost
    /// acknowledgment permits exact redelivery; it grants no effect retry.
    pub async fn send(
        &self,
        #[wasm_bindgen(unchecked_param_type = "WorkLease")] lease: JsValue,
        recipient: String,
        message: String,
        #[wasm_bindgen(unchecked_param_type = "WasmFileRefWire")] payload: JsValue,
    ) -> std::result::Result<(), JsValue> {
        let lease: WorkLease = from_js(lease)?;
        let sender = TaskId::from_bytes(lease.operation.operation_id.into_bytes());
        let recipient = TaskId::parse(&recipient).map_err(js_error)?;
        let message = OperationId::parse(&message).map_err(js_error)?;
        self.runtime
            .task_host()
            .send_owned(
                sender,
                crate::scheduler::LeaseFence::from(&lease.reservation),
                recipient,
                message,
                from_js(payload)?,
            )
            .await
            .map_err(js_error)
    }

    /// A page observation is not a retained model-consumption acknowledgment.
    #[wasm_bindgen(unchecked_return_type = "InboxItem[]")]
    pub async fn inbox(
        &self,
        task: String,
        after: u64,
        maximum: u32,
    ) -> std::result::Result<JsValue, JsValue> {
        let task = TaskId::parse(&task).map_err(js_error)?;
        super::to_js_admitted(
            &self
                .runtime
                .task_host()
                .inbox(task, after, maximum as usize)
                .await
                .map_err(js_error)?,
        )
    }
}

impl WasmTaskRuntime {
    fn commands(
        &self,
    ) -> crate::filesystem::FilesystemTaskCommands<
        '_,
        BrowserStream,
        IndexedDbAuthorityStore,
        IndexedDbObjectStore,
    > {
        let commands = self.runtime.commands();
        match &self.model {
            Some((model, provider)) => {
                commands.with_model(model.clone(), provider.clone(), ContextPipeline::new([]))
            }
            None => commands,
        }
    }
}

fn work_value(work: TaskWorkerAttempt) -> WasmBrowserWork {
    match work {
        TaskWorkerAttempt::Unresolved { lease, error } => WasmBrowserWork::Unresolved {
            lease,
            error: WasmBrowserWorkError {
                code: error.code() as i32,
                message: error.to_string(),
            },
        },
        TaskWorkerAttempt::Progress(progress) => match progress {
            TaskWorkerOutcome::Suspended { task, revision } => WasmBrowserWork::Suspended {
                task_id: task.to_string(),
                revision,
            },
            TaskWorkerOutcome::Completed { task } => WasmBrowserWork::Completed {
                task_id: task.to_string(),
            },
            TaskWorkerOutcome::Yielded { lease } => WasmBrowserWork::Yielded { lease },
            TaskWorkerOutcome::Reconciling { lease } => WasmBrowserWork::Reconciling { lease },
        },
    }
}

#[cfg(all(test, target_arch = "wasm32"))]
#[path = "task_runtime/accounting_tests.rs"]
mod accounting_tests;
