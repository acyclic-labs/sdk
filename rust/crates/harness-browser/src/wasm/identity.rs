use super::*;

/// Parses one public Harness identity with the canonical Rust contract and
/// returns its normalized UUID spelling for a branded TypeScript facade.
#[wasm_bindgen(js_name = validateIdentity)]
pub fn validate_identity(kind: &str, value: &str) -> Result<String, JsValue> {
    let normalized = match kind {
        "agent" => AgentId::parse(value).map_err(js_error)?.to_string(),
        "conversation" => ConversationId::parse(value).map_err(js_error)?.to_string(),
        "session" => SessionId::parse(value).map_err(js_error)?.to_string(),
        "turn" => TurnId::parse(value).map_err(js_error)?.to_string(),
        "task" => TaskId::parse(value).map_err(js_error)?.to_string(),
        "group" => GroupId::parse(value).map_err(js_error)?.to_string(),
        "batch" => BatchId::parse(value).map_err(js_error)?.to_string(),
        "operation" => OperationId::parse(value).map_err(js_error)?.to_string(),
        "effect" => EffectId::parse(value).map_err(js_error)?.to_string(),
        "interaction" => {
            let id = uuid::Uuid::parse_str(value)
                .map_err(|error| JsValue::from_str(&error.to_string()))?;
            if id.is_nil() {
                return Err(JsValue::from_str("interaction identity cannot be nil"));
            }
            id.to_string()
        }
        _ => return Err(JsValue::from_str("unknown Harness identity kind")),
    };
    Ok(normalized)
}

/// Validates one aggregate identity with the same path-segment policy used by every Rust stream access.
#[wasm_bindgen(js_name = validateAuthorityPathSegment)]
pub fn validate_authority_path_segment(
    #[wasm_bindgen(unchecked_param_type = "string")] value: JsValue,
) -> Result<String, JsValue> {
    let value = checked_js_string(value, "authority identity")?;
    let authority = Authority {
        kind: AggregateKind::Agent,
        id: value.clone(),
    };
    authority
        .stream_path(&mut None)
        .map(|_| value)
        .map_err(js_error)
}

/// Validates one component name with the canonical Rust byte and character
/// policy. The field-specific error text remains a thin TypeScript concern.
#[wasm_bindgen(js_name = validateComponentLabel)]
pub fn validate_component_label(
    #[wasm_bindgen(unchecked_param_type = "string")] value: JsValue,
) -> Result<String, JsValue> {
    let value = checked_js_string(value, "component label")?;
    crate::contract::validate_component_label(&value, "component label")
        .map(|_| value)
        .map_err(js_error)
}
