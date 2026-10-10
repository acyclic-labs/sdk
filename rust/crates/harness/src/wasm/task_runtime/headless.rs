//! Headless composition uses the exact native task/content/stock boundaries.
use super::*;
use crate::{conversation::PrivateDirectoryPage, executor::TurnInput};
use crate::resources::GenerationRef;

#[cfg(all(test, target_arch = "wasm32"))]
mod tests;

#[derive(Deserialize, Tsify)]
#[serde(deny_unknown_fields)]
struct WasmSelectedDirectory {
    #[tsify(type = "string")]
    caller: TaskId,
    #[tsify(type = "string")]
    source: TaskId,
    #[tsify(type = "WasmVolumeRefWire")]
    volume: VolumeRef,
    granted_prefix: String,
    path: String,
    #[tsify(type = "WasmResourceRefWire")]
    generation: GenerationRef,
}

#[wasm_bindgen(typescript_custom_section)]
const HEADLESS_TYPES: &str = r#"
export interface WasmHeadlessTurnInput {
    readonly operation_id: string;
    readonly input: WasmModelContent;
    readonly selected_context?: { readonly selection: { readonly checkpoint: WasmFileRefWire | null; readonly conversation_revision: bigint; readonly message_ids: readonly string[] }; readonly messages: readonly WasmModelMessageWire[] } | null;
    readonly max_steps: number;
}
export type WasmModelToolsProgress =
    | { readonly kind: 'ready'; readonly value: readonly WasmModelMessageWire[] }
    | { readonly kind: 'pending'; readonly value: string }
    | { readonly kind: 'rejected'; readonly value: 'declined' | 'cancelled' | 'expired' | 'denied' };
export interface WasmSelectedFileRead { readonly file: WasmFileRefWire; readonly bytes: Uint8Array }
"#;

#[wasm_bindgen]
impl WasmTaskRuntime {
    /// Exact owner-index metadata page; never enumerates or hydrates a swarm.
    #[wasm_bindgen(unchecked_return_type = "WasmTaskChildrenPage")]
    pub async fn children(&self, parent: String, expected_revision: Option<u64>, after_slot: Option<String>, maximum: u32) -> std::result::Result<JsValue, JsValue> {
        let page = self.runtime.children(TaskId::parse(&parent).map_err(js_error)?, expected_revision, after_slot, maximum as usize).await.map_err(js_error)?;
        to_js(&super::super::WasmTaskChildrenPage {
            revision: page.revision,
            entries: page.entries.into_iter().map(|entry| super::super::WasmTaskChild { slot: entry.slot, task_id: entry.task_id }).collect(),
            next_after: page.next_after,
        })
    }

    /// Resolves selected source metadata, never adopting source authority.
    #[wasm_bindgen(js_name = inspectSource, unchecked_return_type = "WasmTaskAdmissionWire")]
    pub async fn inspect_source(&self, caller: String, source: String) -> std::result::Result<JsValue, JsValue> {
        let inspection = self.runtime.inspect_source(TaskId::parse(&caller).map_err(js_error)?, TaskId::parse(&source).map_err(js_error)?).await.map_err(js_error)?;
        to_js(&inspection.source().canonical_value())
    }

    /// Returns the original authenticated allocation snapshot, not a live head.
    #[wasm_bindgen(js_name = sourceProject, unchecked_return_type = "TaskVolumeSnapshot")]
    pub async fn source_project(&self, caller: String, source: String) -> std::result::Result<JsValue, JsValue> {
        let inspection = self.runtime.inspect_source(TaskId::parse(&caller).map_err(js_error)?, TaskId::parse(&source).map_err(js_error)?).await.map_err(js_error)?;
        super::super::to_js_admitted(inspection.project().map_err(js_error)?)
    }

    #[wasm_bindgen(js_name = sourcePrivateVolume, unchecked_return_type = "TaskVolumeSnapshot")]
    pub async fn source_private_volume(&self, caller: String, source: String) -> std::result::Result<JsValue, JsValue> {
        let inspection = self.runtime.inspect_source(TaskId::parse(&caller).map_err(js_error)?, TaskId::parse(&source).map_err(js_error)?).await.map_err(js_error)?;
        super::super::to_js_admitted(inspection.private_volume().map_err(js_error)?)
    }

    #[wasm_bindgen(js_name = sourceAgent)]
    pub async fn source_agent(&self, caller: String, source: String) -> std::result::Result<String, JsValue> {
        let inspection = self.runtime.inspect_source(TaskId::parse(&caller).map_err(js_error)?, TaskId::parse(&source).map_err(js_error)?).await.map_err(js_error)?;
        Ok(inspection.agent().map_err(js_error)?.to_string())
    }

    #[wasm_bindgen(js_name = listSelectedDirectory, unchecked_return_type = "PrivateDirectoryPage")]
    pub async fn list_selected_directory(&self, #[wasm_bindgen(unchecked_param_type = "WasmSelectedDirectory")] selection: JsValue, after: Option<String>, maximum: u32) -> std::result::Result<JsValue, JsValue> {
        let selected: WasmSelectedDirectory = from_js(selection)?;
        let inspection = self.runtime.inspect_source(selected.caller, selected.source).await.map_err(js_error)?;
        let page: PrivateDirectoryPage = inspection.list(&selected.volume, &selected.granted_prefix, &selected.path, &selected.generation, after.as_deref(), maximum).await.map_err(js_error)?;
        super::super::to_js_admitted(&page)
    }

    #[wasm_bindgen(js_name = readSelectedPath, unchecked_return_type = "WasmSelectedFileRead")]
    pub async fn read_selected_path(&self, #[wasm_bindgen(unchecked_param_type = "WasmSelectedDirectory")] selection: JsValue) -> std::result::Result<JsValue, JsValue> {
        let selected: WasmSelectedDirectory = from_js(selection)?;
        let inspection = self.runtime.inspect_source(selected.caller, selected.source).await.map_err(js_error)?;
        let (file, bytes) = inspection.read_path(&selected.volume, &selected.granted_prefix, &selected.path, &selected.generation).await.map_err(js_error)?;
        let value = js_sys::Object::new();
        js_sys::Reflect::set(&value, &"file".into(), &super::super::to_js_admitted(&file)?)?;
        js_sys::Reflect::set(&value, &"bytes".into(), &js_sys::Uint8Array::from(bytes.as_slice()))?;
        Ok(value.into())
    }

    #[wasm_bindgen(js_name = diffSelectedFiles)]
    pub async fn diff_selected_files(&self, caller: String, source: String,
        #[wasm_bindgen(unchecked_param_type = "WasmFileRefWire")] before: JsValue,
        #[wasm_bindgen(unchecked_param_type = "WasmFileRefWire")] after: JsValue,
    ) -> std::result::Result<bool, JsValue> {
        let inspection = self.runtime.inspect_source(TaskId::parse(&caller).map_err(js_error)?, TaskId::parse(&source).map_err(js_error)?).await.map_err(js_error)?;
        inspection.diff(&from_js(before)?, &from_js(after)?).await.map_err(js_error)
    }

    /// Opens a custom loop using the configured real model and admitted tools.
    #[wasm_bindgen(js_name = headlessExecution)]
    pub async fn headless_execution(&self, task: String,
        #[wasm_bindgen(unchecked_param_type = "WasmBrowserLeaseFence")] fence: JsValue,
        turn: String,
    ) -> std::result::Result<WasmHeadlessExecution, JsValue> {
        let (model, provider, compaction) = self.model.as_ref().ok_or_else(|| js_error(Error::Unsupported("model is not bound".into())))?;
        let execution = self.runtime.stock_execution(TaskId::parse(&task).map_err(js_error)?, from_js(fence)?, OperationId::parse(&turn).map_err(js_error)?, model.clone(), provider.clone(), ContextPipeline::default()).await.map_err(js_error)?.with_compaction_policy(compaction.clone());
        Ok(WasmHeadlessExecution { execution })
    }
}

#[wasm_bindgen]
pub struct WasmHeadlessExecution {
    execution: crate::filesystem::FilesystemTaskExecution<BrowserStream, IndexedDbAuthorityStore, IndexedDbObjectStore>,
}

#[wasm_bindgen]
impl WasmHeadlessExecution {
    #[wasm_bindgen(js_name = operationId)]
    pub fn operation_id(&self) -> String { self.execution.operation_id().to_string() }

    #[wasm_bindgen(js_name = modelStep, unchecked_return_type = "WasmModelEvent[]")]
    pub async fn model_step(&self,
        #[wasm_bindgen(unchecked_param_type = "WasmHeadlessTurnInput")] input: JsValue,
        step: u32,
        #[wasm_bindgen(unchecked_param_type = "readonly WasmModelMessageWire[]")] prior_messages: JsValue,
    ) -> std::result::Result<JsValue, JsValue> {
        let input: TurnInput = from_js(input)?;
        let messages: Vec<crate::model::ModelMessage> = from_js(prior_messages)?;
        to_js(&self.execution.model_step(&input, step, &messages).await.map_err(js_error)?)
    }

    #[wasm_bindgen(js_name = resolveModelTools, unchecked_return_type = "WasmModelToolsProgress")]
    pub async fn resolve_model_tools(&self, #[wasm_bindgen(unchecked_param_type = "WasmHeadlessTurnInput")] input: JsValue, step: u32) -> std::result::Result<JsValue, JsValue> {
        let progress = self.execution.resolve_model_tools(&from_js(input)?, step).await.map_err(js_error)?;
        let value = match progress {
            crate::executor::ModelToolsProgress::Ready(messages) => serde_json::json!({ "kind": "ready", "value": messages }),
            crate::executor::ModelToolsProgress::Pending(operation) => serde_json::json!({ "kind": "pending", "value": operation }),
            crate::executor::ModelToolsProgress::Rejected(reason) => serde_json::json!({ "kind": "rejected", "value": reason.to_string() }),
        };
        to_js(&value)
    }
}
