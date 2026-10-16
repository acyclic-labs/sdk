use super::*;
use crate::{
    context::{ModelContextCapacity, ModelTokenCount},
    model::{ModelDispatch, ModelEvent, ModelProvider, PreparedModelRequest},
    runtime::{TaskDefinition, TaskRegistry},
    tool::{
        Tool, ToolDefinition, ToolExecutor, ToolInvocation, ToolProjection, ToolRegistry,
        ToolResult,
    },
    workflow::{MachineIdentity, MachineRegistry, MachineTransition, ResumableMachine},
};
use js_sys::Promise;
use wasm_bindgen_futures::JsFuture;

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
        _context: Option<crate::runtime::ToolContext>,
    ) -> acyclic_stream::BoxProviderFuture<'a, Result<ToolResult>> {
        Box::pin(self.call(&self.execute, invocation))
    }
    fn reconcile<'a>(
        &'a self,
        invocation: ToolInvocation,
        _context: Option<crate::runtime::ToolContext>,
    ) -> acyclic_stream::BoxProviderFuture<'a, Result<Option<ToolResult>>> {
        Box::pin(self.call(&self.reconcile, invocation))
    }
}
impl ToolProjection for HostTool {
    fn project(
        &self,
        invocation: &ToolInvocation,
        result: &ToolResult,
        _work: crate::NativeWork<'_>,
    ) -> Result<(Value, crate::NativeCustody)> {
        let invocation = to_js(invocation)
            .map_err(|_| Error::Invalid("tool projection invocation encoding failed".into()))?;
        let result = to_js(result)
            .map_err(|_| Error::Invalid("tool projection result encoding failed".into()))?;
        let value = self
            .project
            .call2(&JsValue::UNDEFINED, &invocation, &result)
            .map_err(|_| Error::Invalid("tool projection failed".into()))?;
        from_js(value)
            .map(|value| (value, None))
            .map_err(|_| Error::Invalid("tool projection is invalid".into()))
    }
}

pub(super) struct HostModel {
    pub(super) generate: Function,
    pub(super) reconcile: Function,
    pub(super) capacity: Option<Function>,
    pub(super) count_tokens: Option<Function>,
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

    fn count_tokens<'a>(
        &'a self,
        request: &'a PreparedModelRequest,
    ) -> acyclic_stream::BoxProviderFuture<'a, Result<ModelTokenCount>> {
        Box::pin(async move {
            let callback = self.count_tokens.as_ref().ok_or_else(|| {
                Error::Unsupported("selected model token counting is unavailable".into())
            })?;
            let digest = to_js(&request.request_digest())
                .map_err(|_| Error::Invalid("model counting digest encoding failed".into()))?;
            let value = callback
                .call2(
                    &JsValue::UNDEFINED,
                    &js_sys::Uint8Array::from(request.bytes()),
                    &digest,
                )
                .map_err(|_| Error::Invalid("model token counting callback failed".into()))?;
            let value = JsFuture::from(Promise::resolve(&value))
                .await
                .map_err(|_| Error::Storage("model token counting callback failed".into()))?;
            let count: ModelTokenCount = from_js(value).map_err(|_| {
                Error::Invalid("model counting callback returned invalid data".into())
            })?;
            count.validate(request, &mut None)?;
            Ok(count)
        })
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
        dispatch: ModelDispatch,
        observed: &'a [ModelEvent],
    ) -> acyclic_stream::BoxProviderFuture<'a, Result<Option<Vec<ModelEvent>>>> {
        Box::pin(async move {
            let attempt = to_js(&dispatch)
                .map_err(|_| Error::Invalid("model attempt encoding failed".into()))?;
            super::super::set_js_field(
                &attempt,
                "observed",
                &to_js(&observed)
                    .map_err(|_| Error::Invalid("model attempt encoding failed".into()))?,
            )
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
    pub(super) tasks: TaskRegistry,
    pub(super) machines: MachineRegistry,
    pub(super) tools: ToolRegistry,
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
        self.machines
            .register(machine, &mut None)
            .map_err(js_error)?;
        self.tasks.register(task).map_err(js_error)?;
        Ok(())
    }
}
