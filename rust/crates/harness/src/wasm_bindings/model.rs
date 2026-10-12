use super::*;


/// Complete declarative inventory, without granting file or option authority.
#[derive(Serialize, Tsify)]
#[serde(rename_all = "camelCase")]
pub(super) struct WasmModelContentInventoryWire<'a> {
    #[tsify(type = "readonly WasmFileRefWire[]")]
    files: Vec<&'a FileRef>,
    #[tsify(type = "readonly NativeConfigurationBinding[]")]
    native_configurations: Vec<&'a crate::model::NativeConfigurationBinding>,
}

/// Returns all media/options refs and original-admission claims without IO.
#[wasm_bindgen(js_name = modelContentInventory, unchecked_return_type = "WasmModelContentInventoryWire")]
pub fn model_content_inventory(
    #[wasm_bindgen(unchecked_param_type = "WasmModelContentInput")] content: JsValue,
    #[wasm_bindgen(unchecked_param_type = "WasmModelLimitsInput")] limits: JsValue,
) -> Result<JsValue, JsValue> {
    let content: ModelContent = from_js(content)?;
    content
        .validate_limits(from_js(limits)?, &mut None)
        .map_err(js_error)?;
    to_js_admitted(&WasmModelContentInventoryWire {
        files: content.file_refs().collect(),
        native_configurations: content.native_configurations().collect(),
    })
}

/// Public facade input for the shared request constructor.
#[derive(Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub(super) struct WasmModelRequestInput {
    model: crate::model::Model,
    messages: Vec<ModelMessage>,
    tools: Vec<WasmToolDefinitionInput>,
    max_output_tokens: Option<u32>,
}

impl From<WasmModelRequestInput> for crate::model::ModelRequest {
    fn from(input: WasmModelRequestInput) -> Self {
        Self {
            model: input.model,
            messages: input.messages,
            tools: input.tools.into_iter().map(ToolDefinition::from).collect(),
            max_output_tokens: input.max_output_tokens,
        }
    }
}

/// Creates a direct-parent segment from the exact admitted request bytes.
#[wasm_bindgen(js_name = encodeModelPrefix)]
pub fn encode_model_prefix(
    request: Vec<u8>,
    parent: JsValue,
    parent_request: Option<Vec<u8>>,
    limits: JsValue,
) -> Result<js_sys::Uint8Array, JsValue> {
    let limits: Limits = from_js(limits)?;
    let prepare = |bytes: &[u8]| crate::model::PreparedModelRequest::decode(bytes, limits, None);
    let request = prepare(&request).map_err(js_error)?;
    let parent: Option<FileRef> = from_js(parent)?;
    let parent_request = parent_request
        .as_deref()
        .map(prepare)
        .transpose()
        .map_err(js_error)?;
    let selected = match (&parent, &parent_request) {
        (None, None) => None,
        (Some(reference), Some(prepared)) => Some((reference, prepared)),
        _ => {
            return Err(JsValue::from_str(
                "prefix parent and request must be paired",
            ));
        }
    };
    crate::model::ModelPrefix::select(&request, selected)
        .and_then(|prefix| prefix.canonical_bytes(None))
        .map(|bytes| js_sys::Uint8Array::from(bytes.as_ref()))
        .map_err(js_error)
}

/// Constructs the same canonical request bytes used by native providers.
#[wasm_bindgen(js_name = prepareModelRequest)]
pub fn prepare_model_request(request: JsValue, limits: JsValue) -> Result<Vec<u8>, JsValue> {
    let input: WasmModelRequestInput = from_js(request)?;
    let request = input.into();
    let prepared =
        crate::model::PreparedModelRequest::prepare(request, from_js(limits)?, None).map_err(js_error)?;
    Ok(prepared.bytes().to_vec())
}

/// Returns the ordinary replaceable policy used by the stock Rust executor.
#[wasm_bindgen(js_name = defaultCompactionPolicy, unchecked_return_type = "CompactionPolicy")]
pub fn default_compaction_policy() -> Result<JsValue, JsValue> {
    to_js(&crate::context::CompactionPolicy::default())
}

/// Checks a threshold against actual selected capacity, returning its finite output ceiling.
#[wasm_bindgen(js_name = validateThresholdCompaction)]
pub fn validate_threshold_compaction(
    #[wasm_bindgen(unchecked_param_type = "ThresholdCompaction")] config: JsValue,
    #[wasm_bindgen(unchecked_param_type = "ModelContextCapacity")] capacity: JsValue,
    #[wasm_bindgen(unchecked_param_type = "number | undefined")] output_tokens: JsValue,
) -> Result<u32, JsValue> {
    let config: crate::context::ThresholdCompaction = from_js(config)?;
    let output_tokens = if output_tokens.is_undefined() {
        None
    } else {
        from_js(output_tokens)?
    };
    config
        .validate(from_js(capacity)?, output_tokens)
        .map_err(js_error)
}

/// Validates provider-owned counts against the exact canonical prepared request.
#[wasm_bindgen(js_name = validateModelTokenCount)]
pub fn validate_model_token_count(
    #[wasm_bindgen(unchecked_param_type = "ModelRequest")] request: JsValue,
    #[wasm_bindgen(unchecked_param_type = "ModelTokenCount")] count: JsValue,
    limits: JsValue,
) -> Result<u64, JsValue> {
    let request: crate::model::ModelRequest = from_js(request)?;
    let prepared =
        crate::model::PreparedModelRequest::prepare(request, from_js(limits)?, None).map_err(js_error)?;
    let count: crate::context::ModelTokenCount = from_js(count)?;
    count.validate(&prepared, &mut None).map_err(js_error)
}

/// Validates provider-neutral model content under the exact native limits.
#[wasm_bindgen(
    js_name = validateModelContent,
)]
pub fn validate_model_content(
    #[wasm_bindgen(unchecked_param_type = "WasmModelContentInput")] content: JsValue,
    #[wasm_bindgen(unchecked_param_type = "WasmModelLimitsInput")] limits: JsValue,
) -> Result<(), JsValue> {
    let content: ModelContent = from_js(content)?;
    let limits: Limits = from_js(limits)?;
    content.validate_limits(limits, &mut None).map_err(js_error)
}

/// Validates a complete provider-neutral model message list with the native role, message-count, and content bounds.
#[wasm_bindgen(
    js_name = validateModelMessages,
)]
pub fn validate_model_messages(
    #[wasm_bindgen(unchecked_param_type = "readonly WasmModelMessageInput[]")] messages: JsValue,
    #[wasm_bindgen(unchecked_param_type = "WasmModelLimitsInput")] limits: JsValue,
) -> Result<(), JsValue> {
    let messages: Vec<ModelMessage> = from_js(messages)?;
    let limits: Limits = from_js(limits)?;
    limits.validate(&mut None).map_err(js_error)?;
    if messages.is_empty() || messages.len() > limits.context_messages {
        return Err(JsValue::from_str("model context count is invalid"));
    }
    for message in messages {
        message.content.validate_limits(limits, &mut None).map_err(js_error)?;
    }
    Ok(())
}

/// Validates one human-authored model input using the native content rules.
#[wasm_bindgen(
    js_name = validateUserInput,
)]
pub fn validate_user_input(
    #[wasm_bindgen(unchecked_param_type = "WasmModelContentInput")] content: JsValue,
) -> Result<(), JsValue> {
    let content: ModelContent = from_js(content)?;
    content.validate_user_input(&mut None).map_err(js_error)
}

/// Admits an already projected, provider-proven context with native model bounds.
#[wasm_bindgen(js_name = validateSelectedModelContext)]
pub fn validate_selected_model_context(selected: JsValue, limits: JsValue) -> Result<(), JsValue> {
    let selected: WasmPublicSelectedModelContext = from_js(selected)?;
    let limits: Limits = from_js(limits)?;
    SelectedModelContext {
        selection: ModelContextSelection {
            checkpoint: selected.selection.checkpoint,
            conversation_revision: selected.selection.conversation_revision,
            message_ids: selected.selection.message_ids,
        },
        messages: selected.messages,
    }
    .validate_for_dispatch(limits)
    .map_err(js_error)
}

#[derive(Clone, Debug, Default, Deserialize, Serialize, Tsify)]
#[serde(deny_unknown_fields)]
pub(super) struct WasmModelEventAdmissionState {
    #[tsify(type = "number")]
    count: usize,
    calls: Vec<String>,
    completed: bool,
    #[tsify(type = "number")]
    text_bytes: u64,
}

#[derive(Clone, Debug, Deserialize, Serialize, Tsify)]
pub(super) struct WasmModelEventAdmission {
    event: ModelEvent,
    state: WasmModelEventAdmissionState,
}

/// Admits one provider model event with the native stream rules and returns the detached state needed for the next event.
#[wasm_bindgen(
    js_name = admitModelEvent,
    unchecked_return_type = "WasmModelEventAdmission",
)]
pub fn admit_model_event(
    #[wasm_bindgen(unchecked_param_type = "WasmModelEventInput")] event: JsValue,
    #[wasm_bindgen(unchecked_param_type = "WasmModelLimitsInput")] limits: JsValue,
    #[wasm_bindgen(unchecked_param_type = "WasmModelEventAdmissionState | null")] state: JsValue,
) -> Result<JsValue, JsValue> {
    let event: ModelEvent = from_js(event)?;
    let limits: Limits = from_js(limits)?;
    limits.validate(&mut None).map_err(js_error)?;
    let state: WasmModelEventAdmissionState = if state.is_null() || state.is_undefined() {
        WasmModelEventAdmissionState::default()
    } else {
        from_js(state)?
    };
    if state.text_bytes > limits.file_bytes {
        return Err(JsValue::from_str(
            "model output admission state exceeds file limit",
        ));
    }
    let prior_text_bytes = state.text_bytes;
    let admission_state = ModelEventAdmission {
        count: state.count,
        calls: state.calls,
        completed: state.completed,
    };
    let mut admission =
        ModelEventAdmission::from_state(admission_state, limits).map_err(js_error)?;
    admission.observe(&event, limits, None).map_err(js_error)?;
    let text_bytes = match &event {
        ModelEvent::Content { delta } => prior_text_bytes
            .checked_add(delta.len() as u64)
            .ok_or_else(|| JsValue::from_str("model output size overflow"))?,
        ModelEvent::Reasoning { .. }
        | ModelEvent::ToolCall { .. }
        | ModelEvent::Completed { .. } => prior_text_bytes,
    };
    if text_bytes > limits.file_bytes {
        return Err(JsValue::from_str("assistant output exceeds file limit"));
    }
    let state = WasmModelEventAdmissionState {
        count: admission.count,
        calls: admission.calls,
        completed: admission.completed,
        text_bytes,
    };
    let output = WasmModelEventAdmission {
        event,
        state,
    };
    let js = to_js(&output)?;
    let js_state = js_sys::Reflect::get(&js, &JsValue::from_str("state"))?;
    set_js_field(
        &js_state,
        "count",
        &exact_js_number(output.state.count as u64)?,
    )?;
    set_js_field(
        &js_state,
        "text_bytes",
        &exact_js_number(output.state.text_bytes)?,
    )?;
    Ok(js)
}
