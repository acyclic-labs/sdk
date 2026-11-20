use super::*;

/// Parses a bounded frontmatter prefix without fetching or interpreting a skill body.
#[wasm_bindgen(js_name = parseSkillMetadata, unchecked_return_type = "SkillMetadata")]
pub fn parse_skill_metadata(
    prefix: &[u8],
    #[wasm_bindgen(unchecked_param_type = "PinnedContextPath")] source: JsValue,
) -> Result<JsValue, JsValue> {
    let metadata =
        crate::context::parse_skill_metadata(prefix, from_js(source)?).map_err(js_error)?;
    to_js(&metadata)
}

/// Projects a discovered revision using the native messages, placement and bounds.
#[wasm_bindgen(js_name = projectDiscoveredContext, unchecked_return_type = "Context")]
pub fn project_discovered_context(
    #[wasm_bindgen(unchecked_param_type = "DiscoveredContext")] snapshot: JsValue,
    #[wasm_bindgen(unchecked_param_type = "Context")] context: JsValue,
    #[wasm_bindgen(unchecked_param_type = "ContextPlacement")] placement: JsValue,
    #[wasm_bindgen(unchecked_param_type = "WasmModelLimitsInput")] limits: JsValue,
) -> Result<JsValue, JsValue> {
    let snapshot: crate::context::DiscoveredContext = from_js(snapshot)?;
    let context: crate::context::Context = from_js(context)?;
    let placement: crate::context::ContextPlacement = from_js(placement)?;
    let limits = from_js(limits)?;
    let context = crate::context::apply_context_projection(
        context,
        snapshot.messages(limits, None).map_err(js_error)?,
        crate::context::ContextRenderMode::Prompt,
        placement,
        limits,
        None,
    )
    .map_err(js_error)?;
    context_to_js(&context)
}

// Only typed model file parts use the admitted descriptor wire representation.
// Tool JSON may contain similar keys without being descriptors.
pub(super) fn context_to_js(context: &crate::context::Context) -> Result<JsValue, JsValue> {
    let result = to_js(context)?;
    if context.current_input_index.is_none() {
        let object: js_sys::Object = result.clone().dyn_into()?;
        if !js_sys::Reflect::delete_property(&object, &JsValue::from_str("current_input_index"))? {
            return Err(JsValue::from_str(
                "could not omit absent current input index",
            ));
        }
    }
    let metadata = js_sys::Array::new();
    for (name, file) in &context.metadata {
        let entry = js_sys::Array::new();
        entry.push(&JsValue::from_str(name));
        entry.push(&to_js_admitted(file)?);
        metadata.push(&entry);
    }
    js_sys::Reflect::set(
        &result,
        &JsValue::from_str("metadata"),
        &js_sys::Object::from_entries(&metadata)?.into(),
    )?;
    let messages: js_sys::Array =
        js_sys::Reflect::get(&result, &JsValue::from_str("messages"))?.dyn_into()?;
    for (index, message) in context.messages.iter().enumerate() {
        let index =
            u32::try_from(index).map_err(|_| JsValue::from_str("context index exceeds u32"))?;
        let content = js_sys::Reflect::get(&messages.get(index), &JsValue::from_str("content"))?;
        match &message.content {
            ModelContent::Text(_) => {}
            ModelContent::Part(part) => context_part_to_js(&content, part)?,
            ModelContent::Parts(parts) => {
                let content: js_sys::Array = content.dyn_into()?;
                for (index, part) in parts.iter().enumerate() {
                    let index = u32::try_from(index)
                        .map_err(|_| JsValue::from_str("context part index exceeds u32"))?;
                    context_part_to_js(&content.get(index), part)?;
                }
            }
        }
    }
    Ok(result)
}

pub(super) fn context_part_to_js(
    part: &JsValue,
    native: &crate::model::ModelContentPart,
) -> Result<(), JsValue> {
    match native {
        crate::model::ModelContentPart::File { file, policy } => {
            model_file_part_to_js(part, file, policy)?;
        }
        crate::model::ModelContentPart::ToolResult {
            content: crate::model::ToolResultContent::Parts { parts },
            ..
        } => {
            let content = js_sys::Reflect::get(part, &JsValue::from_str("content"))?;
            let values = js_sys::Reflect::get(&content, &JsValue::from_str("parts"))?;
            for (index, data) in parts.iter().enumerate() {
                if let crate::model::ModelDataPart::File { file, policy } = data {
                    let index = u32::try_from(index).map_err(|_| {
                        JsValue::from_str("model data index exceeds portable bound")
                    })?;
                    let value =
                        js_sys::Reflect::get(&values, &JsValue::from_f64(f64::from(index)))?;
                    model_file_part_to_js(&value, file, policy)?;
                }
            }
        }
        _ => {}
    }
    Ok(())
}

// Normalize only typed portable quantities. Opaque JSON and full-width original
// admission/event identities retain their existing serialization contracts.
pub(super) fn model_file_part_to_js(
    part: &JsValue,
    file: &FileRef,
    policy: &crate::model::FileProjectionPolicy,
) -> Result<(), JsValue> {
    js_sys::Reflect::set(part, &JsValue::from_str("file"), &to_js_admitted(file)?)?;
    if let crate::model::FileProjectionPolicy::Native(policy) = policy {
        let projected = js_sys::Reflect::get(part, &JsValue::from_str("policy"))?;
        let native = js_sys::Reflect::get(&projected, &JsValue::from_str("native"))?;
        for (name, value) in [
            ("maximum_bytes", policy.maximum_bytes),
            ("maximum_work", policy.maximum_work),
        ] {
            js_sys::Reflect::set(&native, &JsValue::from_str(name), &exact_js_number(value)?)?;
        }
        let duration = match &policy.intent {
            crate::model::NativeMediaIntent::Audio {
                maximum_duration_ms,
            }
            | crate::model::NativeMediaIntent::Video {
                maximum_duration_ms,
                ..
            } => Some(*maximum_duration_ms),
            _ => None,
        };
        if let Some(duration) = duration {
            let intent = js_sys::Reflect::get(&native, &JsValue::from_str("intent"))?;
            js_sys::Reflect::set(
                &intent,
                &JsValue::from_str("maximum_duration_ms"),
                &exact_js_number(duration)?,
            )?;
        }
        if let Some(binding) = &policy.configuration {
            let configuration = js_sys::Reflect::get(&native, &JsValue::from_str("configuration"))?;
            let options =
                js_sys::Reflect::get(&configuration, &JsValue::from_str("configuration"))?;
            js_sys::Reflect::set(
                &options,
                &JsValue::from_str("content"),
                &to_js_admitted(&binding.configuration.content)?,
            )?;
        }
    }
    Ok(())
}

/// Validates a host-approved pinned selection without granting read authority.
#[wasm_bindgen(js_name = validateContextSelection)]
pub fn validate_context_selection(
    #[wasm_bindgen(unchecked_param_type = "ContextSelection")] selection: JsValue,
    #[wasm_bindgen(unchecked_param_type = "WasmModelLimitsInput")] limits: JsValue,
) -> Result<(), JsValue> {
    let selection: crate::context::ContextSelection = from_js(selection)?;
    selection
        .validate(from_js(limits)?, &mut None)
        .map_err(js_error)
}

/// Applies a host renderer's output using the same native placement and bounds.
/// Hosts supply authorized reads; this call performs no I/O or model admission.
#[wasm_bindgen(js_name = applyContextProjection, unchecked_return_type = "Context")]
pub fn apply_context_projection(
    #[wasm_bindgen(unchecked_param_type = "Context")] context: JsValue,
    #[wasm_bindgen(unchecked_param_type = "readonly WasmModelMessageInput[]")] messages: JsValue,
    #[wasm_bindgen(unchecked_param_type = "ContextRenderMode")] mode: JsValue,
    #[wasm_bindgen(unchecked_param_type = "ContextPlacement")] placement: JsValue,
    #[wasm_bindgen(unchecked_param_type = "WasmModelLimitsInput")] limits: JsValue,
) -> Result<JsValue, JsValue> {
    let context = crate::context::apply_context_projection(
        from_js(context)?,
        from_js(messages)?,
        from_js(mode)?,
        from_js(placement)?,
        from_js(limits)?,
        None,
    )
    .map_err(js_error)?;
    context_to_js(&context)
}
