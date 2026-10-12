//! Explicit browser consumers of the ordinary authenticated recursive workflow.
use super::*;
use crate::runtime::TaskStateProvider;
use crate::{
    core::Snapshot,
    filesystem::{ForkToolArguments, RecursiveForkConfig,
        RecursiveSourceSnapshot, RetainedSourceBinding, fork_preparation_request,
        recursive_fork_source, restore_recursive_fork_source},
    fork::ForkRequest,
    runtime::TaskAdmissionRecord,
    store::{HistoryCursor, HistoryReadLimits, HistoryReader, StreamAggregate},
};
use acyclic_fs::CancellationToken;
use crate::source_authorization::{ProjectSourceReadHandle, ProjectSourceSnapshot};

struct SourceInvocation(CancellationToken);
impl Drop for SourceInvocation {
    fn drop(&mut self) { self.0.cancel(); }
}

#[derive(Deserialize, Tsify)]
#[serde(deny_unknown_fields)]
#[tsify(large_number_types_as_bigints)]
struct WasmRecursiveForkConfig {
    child: crate::core::Authority,
    #[tsify(type = "WasmVolumeRefWire")]
    child_project: VolumeRef,
    #[tsify(type = "WasmVolumeRefWire")]
    child_private: VolumeRef,
    #[serde(default)]
    #[tsify(type = "WasmVolumeRefWire | null")]
    child_result_volume: Option<VolumeRef>,
    #[serde(default)]
    #[tsify(type = "WasmProjectSourceSnapshot | null")]
    selected_project: Option<ProjectSourceSnapshot>,
    #[serde(default)]
    #[tsify(type = "WasmForkPreparationLimits | null")]
    source_policy_limits: Option<crate::filesystem::ForkPreparationLimits>,
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
export interface WasmProjectSourceSnapshot {
    readonly project: WasmVolumeRefWire;
    readonly generation: WasmResourceRefWire & { readonly kind: 'generation' };
    readonly native_snapshot: unknown | null;
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

    /// Creates only canonical input.
    #[wasm_bindgen(js_name = preparationRequest)]
    pub fn preparation_request(&self, preparation_task: String,
        #[wasm_bindgen(unchecked_param_type = "WasmTaskAdmissionWire")] child: JsValue,
        #[wasm_bindgen(unchecked_param_type = "WasmForkPreparationCommand")] command: JsValue,
        #[wasm_bindgen(unchecked_param_type = "WasmForkPreparationLimits")] limits: JsValue,
        #[wasm_bindgen(unchecked_param_type = "readonly WasmFileRefWire[]")] input_files: JsValue,
    ) -> std::result::Result<JsValue, JsValue> {
        let child = TaskAdmissionRecord::from_canonical_value(from_js(child)?).map_err(js_error)?;
        if child.parent != Some(self.caller) {
            return Err(js_error(Error::Unauthorized("fork child has a different original caller".into())));
        }
        let request = fork_preparation_request(&self.binding.preparer, self.binding.request.clone(),
            TaskId::parse(&preparation_task).map_err(js_error)?, child, from_js(command)?, from_js(limits)?, None, from_js(input_files)?)
            .map_err(js_error)?;
        to_js(&request)
    }
}

#[wasm_bindgen]
impl WasmTaskRuntime {
    /// Opens a genuine authenticated aggregate on this exact BrowserStream.
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
        let cancellation = CancellationToken::new();
        let _guard = SourceInvocation(cancellation.clone());
        let caller = TaskId::parse(&caller).map_err(js_error)?;
        let source: RecursiveSourceSnapshot = from_js(source)?;
        let signed: Scope = from_js(signed_scope)?;
        let context = self.recursive_caller(caller, from_js(fence)?, caller_owner, &signed, &source).await?;
        let config: WasmRecursiveForkConfig = from_js(config)?;
        let limits = wasm_limits(config.limits).map_err(js_error)?;
        if limits != context.scope().limits() {
            return Err(js_error(Error::Unauthorized("fork limits differ from original caller admission".into())));
        }
        cancellation.check().map_err(|error| js_error(Error::Storage(error.to_string())))?;
        let parent = StreamAggregate::open(&self.stream, caller_owner.reducer.authority().clone(),
            caller_owner.issuer.verifier(&mut None).expect("verifier copy"), super::super::schema_registry(schemas)?, None, crate::store::default_projection_read_limits(), crate::store::DEFAULT_TERMINAL_EFFECTS, None, &mut None).await.map_err(js_error)?;
        cancellation.check().map_err(|error| js_error(Error::Storage(error.to_string())))?;
        let snapshot = parent.reducer().snapshot(1, &mut None).map(|(snapshot, _)| snapshot).map_err(js_error)?;
        let arguments: ForkToolArguments = from_js(arguments)?;
        let operation = OperationId::parse(&operation).map_err(js_error)?;
        let owner = Arc::new(self.runtime.task_host().journal_owner(caller, context.owned_task_fence().map_err(js_error)?).await.map_err(js_error)?
            .with_invocation_cancellation(&cancellation));
        let resolver = context.content_residency_verifier(owner.clone()).map_err(js_error)?;
        let source_read = if let Some(selected) = arguments.source.filter(|selected| *selected != caller) {
            let policy = self.project_source_policy.as_ref().ok_or_else(|| js_error(Error::Unauthorized("foreign source lacks configured RootService policy".into())))?;
            let selected_project = config.selected_project.ok_or_else(|| js_error(Error::Invalid("foreign source requires exact project snapshot metadata".into())))?;
            let policy_limits = config.source_policy_limits.ok_or_else(|| js_error(Error::Invalid("foreign source requires explicit finite policy limits".into())))?;
            Some(owner.authorize_project_source(&context, &self.service_issuer, policy.as_ref(), selected,
                operation, selected_project, policy_limits.work, policy_limits.bytes).await.map_err(js_error)?)
        } else {
            if config.selected_project.is_some() || config.source_policy_limits.is_some() {
                return Err(js_error(Error::Invalid("current source cannot adopt a foreign project policy descriptor".into())));
            }
            None
        };
        let binding = recursive_fork_source(&self.runtime, self.filesystem.clone(), &self.stream,
            &parent, caller_owner.issuer.verifier(&mut None).expect("verifier copy"), signed, &context, source, from_js(stream_provider)?,
            operation, &arguments,
            RecursiveForkConfig { child: config.child, child_project: config.child_project,
                child_private: config.child_private, child_result_volume: config.child_result_volume,
                limits, inherited_messages: config.inherited_messages,
                inherited_bytes: config.inherited_bytes, inherited_references: config.inherited_references,
                history_limits: config.history_limits }, source_read, resolver, &cancellation).await.map_err(js_error)?;
        Ok(WasmRecursiveForkSource { binding, filesystem: self.filesystem.clone(), caller, snapshot })
    }

    /// Restores only the supplied authenticated original WASM reducer.
    #[wasm_bindgen(js_name = restoreRecursiveForkSource)]
    pub async fn restore_recursive_fork_source(&self, caller: String,
        #[wasm_bindgen(unchecked_param_type = "WasmBrowserLeaseFence")] fence: JsValue,
        original_owner: &WasmReducer, signed_scope: JsValue,
        #[wasm_bindgen(unchecked_param_type = "WasmRecursiveSourceSnapshot")] source: JsValue,
        #[wasm_bindgen(unchecked_param_type = "WasmProviderRefWire")] stream_provider: JsValue,
        #[wasm_bindgen(unchecked_param_type = "ForkRequest")] request: JsValue,
        #[wasm_bindgen(unchecked_param_type = "HistoryReadLimits")] history_limits: JsValue,
    ) -> std::result::Result<WasmRecursiveForkSource, JsValue> {
        let cancellation = CancellationToken::new();
        let _guard = SourceInvocation(cancellation.clone());
        let caller = TaskId::parse(&caller).map_err(js_error)?;
        let source: RecursiveSourceSnapshot = from_js(source)?;
        let signed: Scope = from_js(signed_scope)?;
        let context = self.recursive_caller(caller, from_js(fence)?, original_owner, &signed, &source).await?;
        let history_limits: HistoryReadLimits = from_js(history_limits)?;
        let snapshot = original_owner.reducer.snapshot(1, &mut None).map(|(snapshot, _)| snapshot).map_err(js_error)?;
        let head = snapshot.events.last().ok_or_else(|| js_error(Error::Unauthorized("fork source has no committed caller event".into())))?;
        let reader = HistoryReader::new(&self.stream, original_owner.reducer.authority(), original_owner.issuer.verifier(&mut None).expect("verifier copy")).map_err(js_error)?;
        let revision = original_owner.reducer.revision();
        cancellation.check().map_err(|error| js_error(Error::Storage(error.to_string())))?;
        let page = reader.read_page(&HistoryCursor { authority: original_owner.reducer.authority().clone(),
            after_revision: revision - 1, through_revision: revision }, history_limits).await.map_err(js_error)?;
        cancellation.check().map_err(|error| js_error(Error::Storage(error.to_string())))?;
        if page.events.as_slice() != std::slice::from_ref(head) {
            return Err(js_error(Error::Unauthorized("original fork source head differs from canonical Stream".into())));
        }
        let request: ForkRequest = from_js(request)?;
        let owner = Arc::new(self.runtime.task_host().journal_owner(caller, context.owned_task_fence().map_err(js_error)?).await.map_err(js_error)?
            .with_invocation_cancellation(&cancellation));
        let resolver = context.content_residency_verifier(owner.clone()).map_err(js_error)?;
        let source_read: Option<ProjectSourceReadHandle> = if let Some(authorization) = &request.authorized_source {
            Some(owner.restore_project_source_authorization(&context, authorization).await.map_err(js_error)?)
        } else { None };
        let binding = restore_recursive_fork_source(self.filesystem.clone(), &self.stream,
            &original_owner.reducer, original_owner.issuer.verifier(&mut None).expect("verifier copy"), signed, source.project,
            from_js(stream_provider)?, request, history_limits, source_read, resolver, &cancellation).await.map_err(js_error)?;
        Ok(WasmRecursiveForkSource { binding, filesystem: self.filesystem.clone(), caller, snapshot })
    }

    /// Resumes the exact original preparation/admission lease with its selected real provider.
    #[wasm_bindgen(js_name = resumeForkPreparation, unchecked_return_type = "WasmBrowserWork")]
    pub async fn resume_fork_preparation(&self, source: &WasmRecursiveForkSource,
        #[wasm_bindgen(unchecked_param_type = "WorkLease")] lease: JsValue,
        maximum_transitions: u32,
    ) -> std::result::Result<JsValue, JsValue> {
        let lease = from_js(lease)?;
        self.validate_recursive_lease(source, &lease).await?;
        let commands = self.commands().with_fork_preparer(source.binding.preparer.clone());
        work_to_js(&work_value(self.runtime.resume_task(lease, &commands, maximum_transitions, None).await))
    }

    #[wasm_bindgen(js_name = reconcileForkPreparation, unchecked_return_type = "WasmBrowserWork")]
    pub async fn reconcile_fork_preparation(&self, source: &WasmRecursiveForkSource,
        #[wasm_bindgen(unchecked_param_type = "WorkLease")] lease: JsValue,
        maximum_commands: u32, maximum_wait_millis: u64,
    ) -> std::result::Result<JsValue, JsValue> {
        let lease = from_js(lease)?;
        self.validate_recursive_lease(source, &lease).await?;
        let commands = self.commands().with_fork_preparer(source.binding.preparer.clone());
        work_to_js(&work_value(self.runtime.reconcile_task(lease, &commands, maximum_commands, maximum_wait_millis, None).await))
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
        owner.issuer.verifier(&mut None).expect("verifier copy").verify(signed).map_err(js_error)?;
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
        if admission.parent != Some(source.caller)
            || admission.machine.name != "acyclic.fork_preparation" || admission.machine.version != "1"
            || fork != source.binding.request {
            return Err(js_error(Error::Unauthorized("lease differs from original recursive preparation admission".into())));
        }
        Ok(())
    }
}
