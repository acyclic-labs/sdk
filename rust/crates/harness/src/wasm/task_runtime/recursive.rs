//! Explicit browser consumers of the ordinary authenticated recursive workflow.
use super::*;
use crate::{
    core::Snapshot,
    filesystem::{ForkToolArguments, RecursiveForkConfig,
        RecursiveSourceSnapshot, RetainedSourceBinding, fork_preparation_request,
        recursive_fork_source, restore_recursive_fork_source},
    fork::ForkRequest,
    runtime::TaskAdmissionRecord,
    store::{HistoryCursor, HistoryReadLimits, HistoryReader, StreamAggregate},
};

#[derive(Deserialize, Tsify)]
#[serde(deny_unknown_fields)]
#[tsify(large_number_types_as_bigints)]
struct WasmRecursiveForkConfig {
    child: crate::core::Authority,
    #[tsify(type = "WasmVolumeRefWire")]
    child_project: VolumeRef,
    #[tsify(type = "WasmVolumeRefWire")]
    child_private: VolumeRef,
    limits: WasmLimitsInput,
    inherited_messages: u64,
    inherited_bytes: u64,
    inherited_references: u32,
    history_limits: HistoryReadLimits,
}

#[wasm_bindgen(typescript_custom_section)]
const RECURSIVE_TYPES: &str = r#"
export interface WasmRecursiveSourceSnapshot {
    readonly task: string;
    readonly project: WasmVolumeRefWire;
    readonly project_generation: WasmResourceRefWire & { readonly kind: 'generation' };
    readonly private_volume: WasmVolumeRefWire;
    readonly private_generation: WasmResourceRefWire & { readonly kind: 'generation' };
}
export interface WasmForkPreparationCommand {
    readonly operation_id: string;
    readonly kind: string;
    readonly payload: WasmFileRefWire;
}
export interface WasmForkPreparationLimits {
    readonly resources: number;
    readonly work: bigint;
    readonly bytes: bigint;
}
"#;

/// One immutable caller cut and its real provider binding, not a reducer registry.
#[wasm_bindgen]
pub struct WasmRecursiveForkSource {
    binding: RetainedSourceBinding<IndexedDbAuthorityStore, IndexedDbObjectStore>,
    filesystem: Arc<BrowserFilesystem>,
    caller: TaskId,
    snapshot: Snapshot,
}

#[wasm_bindgen]
impl WasmRecursiveForkSource {
    /// Retain this authenticated original cut for restart; never replace with a head.
    pub fn snapshot(&self) -> std::result::Result<JsValue, JsValue> {
        to_js(&self.snapshot)
    }

    #[wasm_bindgen(unchecked_return_type = "ForkRequest")]
    pub fn request(&self) -> std::result::Result<JsValue, JsValue> {
        to_js(&self.binding.request)
    }

    /// Creates only canonical input. The caller separately admits the registered
    /// ordinary preparation machine, and retains its original admission receipt.
    #[wasm_bindgen(js_name = preparationRequest)]
    pub fn preparation_request(&self, preparation_task: String,
        #[wasm_bindgen(unchecked_param_type = "WasmTaskAdmissionWire")] child: JsValue,
        #[wasm_bindgen(unchecked_param_type = "WasmForkPreparationCommand")] command: JsValue,
        #[wasm_bindgen(unchecked_param_type = "WasmForkPreparationLimits")] limits: JsValue,
    ) -> std::result::Result<JsValue, JsValue> {
        let child = TaskAdmissionRecord::from_canonical_value(from_js(child)?).map_err(js_error)?;
        if child.parent != Some(self.caller) {
            return Err(js_error(Error::Unauthorized("fork child has a different original caller".into())));
        }
        let request = fork_preparation_request(&self.binding.preparer, self.binding.request.clone(),
            TaskId::parse(&preparation_task).map_err(js_error)?, child, from_js(command)?, from_js(limits)?)
            .map_err(js_error)?;
        to_js(&request)
    }
}

#[wasm_bindgen]
impl WasmTaskRuntime {
    /// Opens a genuine authenticated aggregate on this exact BrowserStream.
    /// The owner and signed scope belong to the current caller, not the selected
    /// foreign project. Root bindings are explicit; children use admitted volumes.
    #[wasm_bindgen(js_name = prepareRecursiveForkSource)]
    pub async fn prepare_recursive_fork_source(&self, caller: String,
        #[wasm_bindgen(unchecked_param_type = "WasmBrowserLeaseFence")] fence: JsValue,
        caller_owner: &WasmReducer, signed_scope: JsValue,
        #[wasm_bindgen(unchecked_param_type = "WasmRecursiveSourceSnapshot")] source: JsValue,
        #[wasm_bindgen(unchecked_param_type = "WasmProviderRefWire")] stream_provider: JsValue,
        operation: String,
        #[wasm_bindgen(unchecked_param_type = "ForkToolArguments")] arguments: JsValue,
        #[wasm_bindgen(unchecked_param_type = "WasmRecursiveForkConfig")] config: JsValue,
        schemas: JsValue,
    ) -> std::result::Result<WasmRecursiveForkSource, JsValue> {
        let caller = TaskId::parse(&caller).map_err(js_error)?;
        let source: RecursiveSourceSnapshot = from_js(source)?;
        let signed: Scope = from_js(signed_scope)?;
        let context = self.recursive_caller(caller, from_js(fence)?, caller_owner, &signed, &source).await?;
        let config: WasmRecursiveForkConfig = from_js(config)?;
        let limits = wasm_limits(config.limits).map_err(js_error)?;
        if limits != context.scope().limits() {
            return Err(js_error(Error::Unauthorized("fork limits differ from original caller admission".into())));
        }
        let parent = StreamAggregate::open(&self.stream, caller_owner.reducer.authority().clone(),
            caller_owner.issuer.verifier(), super::super::schema_registry(schemas)?).await.map_err(js_error)?;
        let snapshot = parent.reducer().snapshot_with_event_limit(1).map_err(js_error)?;
        let binding = recursive_fork_source(&self.runtime, self.filesystem.clone(), &self.stream,
            &parent, caller_owner.issuer.verifier(), signed, &context, source, from_js(stream_provider)?,
            OperationId::parse(&operation).map_err(js_error)?, &from_js::<ForkToolArguments>(arguments)?,
            RecursiveForkConfig { child: config.child, child_project: config.child_project,
                child_private: config.child_private, limits, inherited_messages: config.inherited_messages,
                inherited_bytes: config.inherited_bytes, inherited_references: config.inherited_references,
                history_limits: config.history_limits }).await.map_err(js_error)?;
        Ok(WasmRecursiveForkSource { binding, filesystem: self.filesystem.clone(), caller, snapshot })
    }

    /// Restores only the supplied authenticated original WASM reducer. Its head
    /// event must match the canonical Stream; no refresh or tail substitution.
    #[wasm_bindgen(js_name = restoreRecursiveForkSource)]
    pub async fn restore_recursive_fork_source(&self, caller: String,
        #[wasm_bindgen(unchecked_param_type = "WasmBrowserLeaseFence")] fence: JsValue,
        original_owner: &WasmReducer, signed_scope: JsValue,
        #[wasm_bindgen(unchecked_param_type = "WasmRecursiveSourceSnapshot")] source: JsValue,
        #[wasm_bindgen(unchecked_param_type = "WasmProviderRefWire")] stream_provider: JsValue,
        #[wasm_bindgen(unchecked_param_type = "ForkRequest")] request: JsValue,
        #[wasm_bindgen(unchecked_param_type = "HistoryReadLimits")] history_limits: JsValue,
    ) -> std::result::Result<WasmRecursiveForkSource, JsValue> {
        let caller = TaskId::parse(&caller).map_err(js_error)?;
        let source: RecursiveSourceSnapshot = from_js(source)?;
        let signed: Scope = from_js(signed_scope)?;
        self.recursive_caller(caller, from_js(fence)?, original_owner, &signed, &source).await?;
        let history_limits: HistoryReadLimits = from_js(history_limits)?;
        let snapshot = original_owner.reducer.snapshot_with_event_limit(1).map_err(js_error)?;
        let head = snapshot.events.last().ok_or_else(|| js_error(Error::Unauthorized("fork source has no committed caller event".into())))?;
        let reader = HistoryReader::new(&self.stream, original_owner.reducer.authority(), original_owner.issuer.verifier()).map_err(js_error)?;
        let revision = original_owner.reducer.revision();
        let page = reader.read_page(&HistoryCursor { authority: original_owner.reducer.authority().clone(),
            after_revision: revision - 1, through_revision: revision }, history_limits).await.map_err(js_error)?;
        if page.events.as_slice() != std::slice::from_ref(head) {
            return Err(js_error(Error::Unauthorized("original fork source head differs from canonical Stream".into())));
        }
        let binding = restore_recursive_fork_source(self.filesystem.clone(), &self.stream,
            &original_owner.reducer, original_owner.issuer.verifier(), signed, source.project,
            from_js(stream_provider)?, from_js(request)?, history_limits).await.map_err(js_error)?;
        Ok(WasmRecursiveForkSource { binding, filesystem: self.filesystem.clone(), caller, snapshot })
    }

    /// Resumes the exact original preparation/admission lease with its selected
    /// real provider. Other task input or runtime providers fail closed.
    #[wasm_bindgen(js_name = resumeForkPreparation, unchecked_return_type = "WasmBrowserWork")]
    pub async fn resume_fork_preparation(&self, source: &WasmRecursiveForkSource,
        #[wasm_bindgen(unchecked_param_type = "WorkLease")] lease: JsValue,
        maximum_transitions: u32,
    ) -> std::result::Result<JsValue, JsValue> {
        let lease = from_js(lease)?;
        self.validate_recursive_lease(source, &lease).await?;
        let commands = self.commands().with_fork_preparer(source.binding.preparer.clone());
        work_to_js(&work_value(self.runtime.resume_task(lease, &commands, maximum_transitions).await))
    }

    #[wasm_bindgen(js_name = reconcileForkPreparation, unchecked_return_type = "WasmBrowserWork")]
    pub async fn reconcile_fork_preparation(&self, source: &WasmRecursiveForkSource,
        #[wasm_bindgen(unchecked_param_type = "WorkLease")] lease: JsValue,
        maximum_commands: u32, maximum_wait_millis: u64,
    ) -> std::result::Result<JsValue, JsValue> {
        let lease = from_js(lease)?;
        self.validate_recursive_lease(source, &lease).await?;
        let commands = self.commands().with_fork_preparer(source.binding.preparer.clone());
        work_to_js(&work_value(self.runtime.reconcile_task(lease, &commands, maximum_commands, maximum_wait_millis).await))
    }
}

impl WasmTaskRuntime {
    async fn recursive_caller(&self, caller: TaskId, fence: crate::scheduler::LeaseFence,
        owner: &WasmReducer, signed: &Scope, source: &RecursiveSourceSnapshot,
    ) -> std::result::Result<crate::runtime::TaskContext, JsValue> {
        let context = self.runtime.task_context(caller, fence).await.map_err(js_error)?;
        let admission = self.runtime.task_host().observe_admission(caller).await.map_err(js_error)?;
        let agent = owner.reducer.conversation().and_then(|conversation| conversation.agent)
            .ok_or_else(|| js_error(Error::Unauthorized("caller owner has no conversation agent".into())))?;
        owner.issuer.verifier().verify(signed).map_err(js_error)?;
        if source.task != caller || signed.agent() != Some(agent)
            || source.private_volume.owner() != &crate::conversation::VolumeOwner::Agent(agent)
            || signed.capabilities().iter().any(|grant| !context.scope().grants().contains(grant)) {
            return Err(js_error(Error::Unauthorized("recursive source is not the original caller scope".into())));
        }
        if admission.parent.is_some() {
            let inspection = self.runtime.inspect_source(caller, caller).await.map_err(js_error)?;
            if inspection.agent().map_err(js_error)? != agent
                || inspection.project().map_err(js_error)?.volume != source.project
                || inspection.private_volume().map_err(js_error)?.volume != source.private_volume {
                return Err(js_error(Error::Unauthorized("child source differs from its actual admitted bindings".into())));
            }
        }
        Ok(context)
    }

    async fn validate_recursive_lease(&self, source: &WasmRecursiveForkSource, lease: &WorkLease)
        -> std::result::Result<(), JsValue> {
        if !Arc::ptr_eq(&self.filesystem, &source.filesystem) {
            return Err(js_error(Error::Unauthorized("fork source is bound to another runtime provider".into())));
        }
        let task = crate::runtime::task_id_for_admission(lease.operation.operation_id);
        let admission = self.runtime.task_host().observe_admission(task).await.map_err(js_error)?;
        let fork = admission.input.get("payload").cloned()
            .ok_or_else(|| js_error(Error::Unauthorized("lease has no original recursive preparation input".into())))?;
        let fork: ForkRequest = serde_json::from_value(fork)
            .map_err(|error| js_error(Error::Invalid(error.to_string())))?;
        // The ordinary owner has already authenticated the exact registered
        // digest; this bridge only restricts the retained workflow's purpose.
        if admission.parent != Some(source.caller)
            || admission.machine.name != "acyclic.fork_preparation" || admission.machine.version != "1"
            || fork != source.binding.request {
            return Err(js_error(Error::Unauthorized("lease differs from original recursive preparation admission".into())));
        }
        Ok(())
    }
}
