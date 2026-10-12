use super::*;


/// Derives a stable child operation/message identity from one admitted operation
/// and a local role label without duplicating UUID bit manipulation in hosts.
#[wasm_bindgen(js_name = deriveOperationUuid)]
pub fn derive_operation_uuid(operation: &str, label: &str) -> Result<String, JsValue> {
    uuid::Uuid::parse_str(operation).map_err(|error| JsValue::from_str(&error.to_string()))?;
    let mut digest = Sha256::new();
    digest.update(operation.as_bytes());
    digest.update(b":");
    digest.update(label.as_bytes());
    let digest = digest.finalize();
    let mut bytes = [0_u8; 16];
    bytes.copy_from_slice(
        digest
            .get(..16)
            .ok_or_else(|| JsValue::from_str("SHA-256 digest is incomplete"))?,
    );
    bytes[6] = (bytes[6] & 15) | 80;
    bytes[8] = (bytes[8] & 63) | 128;
    Ok(uuid::Uuid::from_bytes(bytes).to_string())
}

/// Uses one half of a canonical action digest as a stable local identity.
/// The digest is already SHA-256; the two halves separate approval operations
/// from their interaction tickets without a second hashing convention.
#[wasm_bindgen(js_name = uuidFromDigestHalf)]
pub fn uuid_from_digest_half(digest: &[u8], second: bool) -> Result<String, JsValue> {
    if digest.len() != 32 {
        return Err(JsValue::from_str("action digest must contain 32 bytes"));
    }
    let selected = if second {
        digest.get(16..)
    } else {
        digest.get(..16)
    }
    .ok_or_else(|| JsValue::from_str("action digest is incomplete"))?;
    let identity =
        uuid::Uuid::from_slice(selected).map_err(|error| JsValue::from_str(&error.to_string()))?;
    if identity.is_nil() {
        return Err(JsValue::from_str("derived identity cannot be nil"));
    }
    Ok(identity.to_string())
}

/// Binds the exact native request without rounding its filesystem work counters
/// through JavaScript numbers. This performs no native dispatch or path lookup.
#[cfg(feature = "filesystem")]
#[wasm_bindgen(js_name = nativeProcessApprovalDigest)]
pub fn native_process_approval_digest(
    task: &str,
    command: &str,
    request_json: &str,
) -> Result<Vec<u8>, JsValue> {
    let task = crate::TaskId::parse(task).map_err(js_error)?;
    let command = crate::OperationId::parse(command).map_err(js_error)?;
    let request: crate::filesystem::NativeProcessRequest = serde_json::from_str(request_json)
        .map_err(|error| js_error(crate::Error::Invalid(error.to_string())))?;
    request
        .approval_digest(task, command, None)
        .map(|digest| digest.to_vec())
        .map_err(js_error)
}

/// Returns the one Rust UUID spelling accepted for a conversation identity.
#[wasm_bindgen(js_name = validateConversationMessageId)]
pub fn validate_conversation_message_id(value: &str) -> Result<String, JsValue> {
    let id = uuid::Uuid::parse_str(value).map_err(|error| JsValue::from_str(&error.to_string()))?;
    if id.is_nil() || id.to_string() != value {
        return Err(JsValue::from_str(
            "conversation message ID is not a canonical UUID",
        ));
    }
    Ok(id.to_string())
}

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
    authority.stream_path(&mut None).map(|_| value).map_err(js_error)
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

pub(super) fn key_bytes(value: Vec<u8>) -> Result<[u8; 32], JsValue> {
    value
        .try_into()
        .map_err(|_| JsValue::from_str("authority key must contain exactly 32 bytes"))
}
