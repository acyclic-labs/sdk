//! Thin JavaScript host for the exact native reducer.
#![allow(
    clippy::needless_pass_by_value,
    reason = "wasm-bindgen's exported ABI owns JavaScript values and byte buffers"
)]

use crate::wire_codec::{decode_command, encode_apply_result, protocol_identity};
use crate::{
    AgentId, Capabilities, ConversationId, EffectId, OperationId, PolicyLayer, SessionId, TaskId,
    TurnId,
    conversation::{
        Attachment, ContentGrant, ConversationMessage, FileDescriptor, FileRef, Limits,
        ReferencedAttachments, TaskOutcomeRecord, VolumeOperation, VolumeRef,
        decode_attachment_manifest, encode_attachment_manifest,
    },
    core::{
        ApplyResult, Authority, AuthorityIssuer, Command, ExtensionForkPolicy, ExtensionRecord,
        Reducer, SchemaRegistry, Scope, Snapshot,
    },
    fork::{ForkReport, ForkRequest, ForkSeed, ReferenceGrant, ResourceRevision},
    interaction::{ApprovalBinding, InteractionResolution, InteractionTicket},
    merge::ProjectMergeReceipt,
    resources::{ProviderRef, ResourceRef},
};
use serde::Serialize;
use sha2::{Digest as _, Sha256};
use wasm_bindgen::prelude::*;

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

/// Parses canonical JSON without passing full-width integer literals through
/// JavaScript Number. Large serde integers are returned as `BigInt`.
#[wasm_bindgen(js_name = decodeCanonicalJson)]
pub fn decode_canonical_json(bytes: &[u8]) -> Result<JsValue, JsValue> {
    let value = parse_json_bytes(bytes)?;
    let canonical = crate::contract::canonical_json_bytes(&value).map_err(js_error)?;
    if canonical != bytes {
        return Err(JsValue::from_str("JSON is not canonical"));
    }
    to_js(&value)
}

/// Parses external JSON with exact integers but without demanding canonical
/// key order or whitespace. Callers must still apply their schema and numeric
/// range policy before presenting model-authored values to an executor.
#[wasm_bindgen(js_name = decodeJson)]
pub fn decode_json(bytes: &[u8]) -> Result<JsValue, JsValue> {
    to_js(&parse_json_bytes(bytes)?)
}

fn parse_json_bytes(bytes: &[u8]) -> Result<serde_json::Value, JsValue> {
    if bytes.len() > 16 * 1024 * 1024 {
        return Err(JsValue::from_str("JSON exceeds the Harness byte limit"));
    }
    reject_out_of_range_integer_tokens(bytes)?;
    serde_json::from_slice(bytes).map_err(|error| JsValue::from_str(&error.to_string()))
}

// serde_json without arbitrary_precision promotes an integer literal outside
// i64/u64 to f64. Reject it before parsing so the JS boundary never presents
// rounded integer data as a successful read.
fn reject_out_of_range_integer_tokens(bytes: &[u8]) -> Result<(), JsValue> {
    let mut offset = 0;
    let mut quoted = false;
    let mut escaped = false;
    while let Some(&byte) = bytes.get(offset) {
        if quoted {
            if escaped {
                escaped = false;
            } else if byte == b'\\' {
                escaped = true;
            } else if byte == b'"' {
                quoted = false;
            }
            offset += 1;
            continue;
        }
        if byte == b'"' {
            quoted = true;
            offset += 1;
            continue;
        }
        if byte != b'-' && !byte.is_ascii_digit() {
            offset += 1;
            continue;
        }
        let start = offset;
        while bytes
            .get(offset)
            .is_some_and(|byte| matches!(byte, b'0'..=b'9' | b'+' | b'-' | b'.' | b'e' | b'E'))
        {
            offset += 1;
        }
        let token = bytes
            .get(start..offset)
            .ok_or_else(|| JsValue::from_str("JSON token range is invalid"))?;
        if !token.iter().any(|part| matches!(*part, b'.' | b'e' | b'E')) {
            let text = std::str::from_utf8(token)
                .map_err(|_| JsValue::from_str("JSON integer token is not UTF-8"))?;
            if text.parse::<i64>().is_err() && text.parse::<u64>().is_err() {
                return Err(JsValue::from_str(
                    "JSON integer exceeds the exact 64-bit range",
                ));
            }
        }
    }
    Ok(())
}

/// Serializes a plain JavaScript data value through Rust's canonical JSON
/// representation. Unsafe integer Numbers are rejected before conversion;
/// callers must supply `BigInt` for exact full-width identities and counters.
#[wasm_bindgen(js_name = encodeCanonicalJson)]
pub fn encode_canonical_json(value: JsValue) -> Result<Vec<u8>, JsValue> {
    let value = js_json_value(&value)?;
    let bytes = crate::contract::canonical_json_bytes(&value).map_err(js_error)?;
    if bytes.len() > 16 * 1024 * 1024 {
        return Err(JsValue::from_str(
            "canonical JSON exceeds the Harness byte limit",
        ));
    }
    Ok(bytes)
}

fn js_json_value(value: &JsValue) -> Result<serde_json::Value, JsValue> {
    let mut ancestors = Vec::new();
    let mut nodes = 0;
    snapshot_js_json(value, 0, &mut ancestors, &mut nodes)
}

/// Hashes the same bounded canonical JSON bytes used by native admissions.
#[wasm_bindgen(js_name = digestCanonicalJson)]
pub fn digest_canonical_json(value: JsValue) -> Result<Vec<u8>, JsValue> {
    Ok(blake3::hash(&encode_canonical_json(value)?)
        .as_bytes()
        .to_vec())
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

#[expect(
    clippy::too_many_lines,
    reason = "one exhaustive traversal owns JS JSON admission"
)]
fn snapshot_js_json(
    value: &JsValue,
    depth: usize,
    ancestors: &mut Vec<JsValue>,
    nodes: &mut usize,
) -> Result<serde_json::Value, JsValue> {
    *nodes += 1;
    if depth > 128 || *nodes > 1_000_000 {
        return Err(JsValue::from_str("JSON nesting exceeds the Harness limit"));
    }
    if value.is_undefined() {
        return Err(JsValue::from_str("undefined is not canonical JSON"));
    }
    if let Some(number) = value.as_f64() {
        if !number.is_finite()
            || number == 0.0 && number.is_sign_negative()
            || number.fract() == 0.0 && number.abs() > 9_007_199_254_740_991.0
        {
            return Err(JsValue::from_str(
                "JSON contains an unsafe JavaScript Number",
            ));
        }
        let admitted = if number.fract() == 0.0 {
            let exact = number
                .to_string()
                .parse::<i64>()
                .map_err(|_| JsValue::from_str("JSON integer is outside the exact range"))?;
            serde_json::Value::from(exact)
        } else {
            serde_json::Value::Number(
                serde_json::Number::from_f64(number)
                    .ok_or_else(|| JsValue::from_str("JSON number is invalid"))?,
            )
        };
        return Ok(admitted);
    }
    if value.is_null() {
        return Ok(serde_json::Value::Null);
    }
    if value.is_bigint() {
        let spelling = JsValue::from(
            value
                .unchecked_ref::<js_sys::BigInt>()
                .to_string(10)
                .map_err(JsValue::from)?,
        )
        .as_string()
        .ok_or_else(|| JsValue::from_str("BigInt spelling is invalid"))?;
        if let Ok(signed) = spelling.parse::<i64>() {
            return Ok(serde_json::Value::from(signed));
        }
        if let Ok(unsigned) = spelling.parse::<u64>() {
            return Ok(serde_json::Value::from(unsigned));
        }
        return Err(JsValue::from_str("BigInt exceeds the exact 64-bit range"));
    }
    if let Some(boolean) = value.as_bool() {
        return Ok(serde_json::Value::Bool(boolean));
    }
    if let Some(string) = value.as_string() {
        return Ok(serde_json::Value::String(string));
    }
    if !value.is_object() {
        return Err(JsValue::from_str("value is not canonical JSON"));
    }
    if value.is_instance_of::<js_sys::Uint8Array>() {
        return Ok(serde_json::Value::Array(
            js_sys::Uint8Array::new(value)
                .to_vec()
                .into_iter()
                .map(serde_json::Value::from)
                .collect(),
        ));
    }
    if ancestors
        .iter()
        .any(|ancestor| js_sys::Object::is(ancestor, value))
    {
        return Err(JsValue::from_str("cyclic values are not canonical JSON"));
    }
    let is_map = value.is_instance_of::<js_sys::Map>();
    if !js_sys::Array::is_array(value) && !is_map {
        let prototype: JsValue = js_sys::Object::get_prototype_of(value).into();
        let ordinary: JsValue =
            js_sys::Object::get_prototype_of(&js_sys::Object::new().into()).into();
        if !prototype.is_null() && !js_sys::Object::is(&prototype, &ordinary) {
            return Err(JsValue::from_str("only plain objects are canonical JSON"));
        }
    }
    ancestors.push(value.clone());
    let snapshot = if js_sys::Array::is_array(value) {
        let items = js_sys::Array::from(value);
        let mut snapshot = Vec::with_capacity(items.length() as usize);
        for item in items.iter() {
            snapshot.push(snapshot_js_json(&item, depth + 1, ancestors, nodes)?);
        }
        serde_json::Value::Array(snapshot)
    } else if is_map {
        let mut failure = None;
        let mut snapshot = serde_json::Map::new();
        value
            .unchecked_ref::<js_sys::Map>()
            .for_each(&mut |child, key| {
                if failure.is_some() {
                    return;
                }
                let Some(key) = key.as_string() else {
                    failure = Some(JsValue::from_str("JSON object keys must be strings"));
                    return;
                };
                match snapshot_js_json(&child, depth + 1, ancestors, nodes) {
                    Ok(admitted) => {
                        snapshot.insert(key, admitted);
                    }
                    Err(error) => failure = Some(error),
                }
            });
        if let Some(error) = failure {
            return Err(error);
        }
        serde_json::Value::Object(snapshot)
    } else {
        let object: &js_sys::Object = value.unchecked_ref();
        let mut snapshot = serde_json::Map::new();
        for key in js_sys::Object::keys(object).iter() {
            let key_text = key
                .as_string()
                .ok_or_else(|| JsValue::from_str("JSON object key is invalid"))?;
            let child = js_sys::Reflect::get(value, &key)?;
            let admitted = snapshot_js_json(&child, depth + 1, ancestors, nodes)?;
            if snapshot.insert(key_text, admitted).is_some() {
                return Err(JsValue::from_str("duplicate JSON object key"));
            }
        }
        serde_json::Value::Object(snapshot)
    };
    ancestors.pop();
    Ok(snapshot)
}

/// Opaque synchronous reducer hosted in WebAssembly.
#[wasm_bindgen]
pub struct WasmReducer {
    reducer: Reducer,
    issuer: AuthorityIssuer,
}

#[derive(Serialize)]
struct ConversationPage<'a> {
    agent: Option<AgentId>,
    event_revision: u64,
    total_messages: u64,
    messages: &'a [ConversationMessage],
    next_sequence: Option<u64>,
}

fn conversation_page_data(
    reducer: &Reducer,
    after_sequence: u64,
    limit: u32,
) -> Result<ConversationPage<'_>, JsValue> {
    if limit == 0 || limit > 1_024 {
        return Err(JsValue::from_str("conversation page limit is invalid"));
    }
    let conversation = reducer
        .conversation()
        .ok_or_else(|| JsValue::from_str("aggregate is not a conversation"))?;
    let total_messages = conversation.messages.len() as u64;
    if after_sequence > total_messages {
        return Err(JsValue::from_str("conversation cursor is beyond the tail"));
    }
    let start = usize::try_from(after_sequence)
        .map_err(|_| JsValue::from_str("conversation cursor exceeds the platform limit"))?;
    let mut end = start;
    let mut bytes_used = 0_usize;
    while end < conversation.messages.len() && end - start < limit as usize {
        let message = conversation
            .messages
            .get(end)
            .ok_or_else(|| JsValue::from_str("conversation page cursor is invalid"))?;
        let size = crate::contract::canonical_json_bytes(message)
            .map_err(js_error)?
            .len();
        if bytes_used.saturating_add(size) > 8 * 1024 * 1024 - 1_024 {
            if end == start {
                return Err(JsValue::from_str(
                    "conversation message exceeds the page byte limit",
                ));
            }
            break;
        }
        bytes_used += size;
        end += 1;
    }
    Ok(ConversationPage {
        agent: conversation.agent,
        event_revision: reducer.revision(),
        total_messages,
        messages: conversation
            .messages
            .get(start..end)
            .ok_or_else(|| JsValue::from_str("conversation page range is invalid"))?,
        next_sequence: (end < conversation.messages.len()).then_some(end as u64),
    })
}

/// Pure v2 contract admission shared by native and JavaScript hosts. The
/// returned object is detached and canonically shaped by Rust serde; context
/// supplies `Limits` for messages and the open ticket for resolutions.
#[wasm_bindgen(js_name = validateContract)]
#[expect(
    clippy::too_many_lines,
    reason = "one exhaustive v2 contract admission dispatch"
)]
pub fn validate_contract(kind: &str, value: JsValue, context: JsValue) -> Result<JsValue, JsValue> {
    // serde-wasm-bindgen otherwise coerces NaN/Infinity inside generic JSON
    // fields to null before the Rust contract can reject them. Admit the
    // complete plain value before any target-type deserialization.
    match kind {
        "provider_ref" => {
            let value: ProviderRef = from_js(value)?;
            value.validate().map_err(js_error)?;
            to_js_admitted(&value)
        }
        "file_descriptor" => {
            let value: FileDescriptor = from_js(value)?;
            to_js_admitted(&value)
        }
        "approval_binding" => {
            let value: ApprovalBinding = from_js(value)?;
            value.validate().map_err(js_error)?;
            to_js_admitted(&value)
        }
        "project_merge_receipt" => {
            let value: ProjectMergeReceipt = from_js(value)?;
            value.validate_shape().map_err(js_error)?;
            to_js_admitted(&value)
        }
        "limits" => {
            let value: Limits = from_js(value)?;
            value.validate().map_err(js_error)?;
            let js = to_js(&value)?;
            for (key, bound) in [
                ("file_bytes", value.file_bytes),
                ("path_bytes", value.path_bytes as u64),
                ("attachments", value.attachments as u64),
                ("render_bytes", value.render_bytes),
                ("model_steps", value.model_steps as u64),
                ("model_events_per_step", value.model_events_per_step as u64),
                ("tool_calls_per_step", value.tool_calls_per_step as u64),
                ("context_messages", value.context_messages as u64),
            ] {
                set_js_field(&js, key, &exact_js_number(bound)?)?;
            }
            Ok(js)
        }
        "volume_ref" => {
            let value: VolumeRef = from_js(value)?;
            value.validate().map_err(js_error)?;
            to_js_admitted(&value)
        }
        "file_ref" => {
            let value: FileRef = from_js(value)?;
            value.validate().map_err(js_error)?;
            to_js_admitted(&value)
        }
        "attachments" => {
            let value: ReferencedAttachments = from_js(value)?;
            value.validate().map_err(js_error)?;
            to_js_admitted(&value)
        }
        "conversation_message" => {
            let value: ConversationMessage = from_js(value)?;
            let limits: Limits = from_js(context)?;
            limits.validate_message(&value).map_err(js_error)?;
            to_js_admitted(&value)
        }
        "task_outcome" => {
            let value: TaskOutcomeRecord = from_js(value)?;
            value.validate().map_err(js_error)?;
            to_js_admitted(&value)
        }
        "resource_ref" => {
            let value: ResourceRef = from_js(value)?;
            value.validate().map_err(js_error)?;
            to_js_admitted(&value)
        }
        "resource_revision" => {
            let value: ResourceRevision = from_js(value)?;
            value.validate().map_err(js_error)?;
            to_js_admitted(&value)
        }
        "fork_request" => {
            let value: ForkRequest = from_js(value)?;
            value.validate().map_err(js_error)?;
            to_js_admitted(&value)
        }
        "fork_report" => {
            let value: ForkReport = from_js(value)?;
            value.validate().map_err(js_error)?;
            to_js_admitted(&value)
        }
        "fork_seed" => {
            let value: ForkSeed = from_js(value)?;
            value.validate().map_err(js_error)?;
            to_js_admitted(&value)
        }
        "reference_grant" => {
            let value: ReferenceGrant = from_js(value)?;
            value.validate().map_err(js_error)?;
            to_js_admitted(&value)
        }
        "extension_record" => {
            let value: ExtensionRecord = from_js(value)?;
            value.validate().map_err(js_error)?;
            to_js_admitted(&value)
        }
        "interaction_ticket" => {
            let value: InteractionTicket = from_js(value)?;
            value.validate().map_err(js_error)?;
            to_js_admitted(&value)
        }
        "interaction_resolution" => {
            let value: InteractionResolution = from_js(value)?;
            let ticket: InteractionTicket = from_js(context)?;
            ticket.validate().map_err(js_error)?;
            value.validate(&ticket).map_err(js_error)?;
            to_js_admitted(&value)
        }
        _ => Err(JsValue::from_str("unknown Harness v2 contract kind")),
    }
}

/// Applies the same JSON Schema admission used by Rust tool execution before
/// a TypeScript facade turns an untrusted model value into a typed argument.
#[wasm_bindgen(js_name = validateToolValue)]
pub fn validate_tool_value(schema: JsValue, value: JsValue) -> Result<JsValue, JsValue> {
    let schema: serde_json::Value = from_js(schema)?;
    let value: serde_json::Value = from_js(value)?;
    crate::contract::validate_json_schema_value(&schema, &value, "tool value").map_err(js_error)?;
    to_js(&value)
}

/// Checks immutable file identity without constructing a reducer or issuer.
#[wasm_bindgen(js_name = verifyFileBytes)]
pub fn verify_file_bytes(file: JsValue, bytes: Vec<u8>) -> Result<(), JsValue> {
    let file: FileRef = from_js(file)?;
    file.validate().map_err(js_error)?;
    file.descriptor().verify(&bytes).map_err(js_error)
}

/// Stages a descriptor with Rust-owned SHA-256, media-type, and safe
/// byte-length rules, projecting its bounded length as a JS Number.
#[wasm_bindgen(js_name = fileDescriptor)]
pub fn file_descriptor(bytes: &[u8], media_type: &str) -> Result<JsValue, JsValue> {
    let descriptor = FileDescriptor::from_bytes(bytes, media_type).map_err(js_error)?;
    to_js_admitted(&descriptor)
}

/// Encodes a validated attachment list in the exact typed manifest wire form.
#[wasm_bindgen(js_name = encodeAttachmentManifest)]
pub fn encode_attachment_manifest_bytes(items: JsValue) -> Result<Vec<u8>, JsValue> {
    let items: Vec<Attachment> = from_js(items)?;
    encode_attachment_manifest(&items).map_err(js_error)
}

/// Decodes a complete canonical attachment list without a reducer instance.
#[wasm_bindgen(js_name = decodeAttachmentManifest)]
pub fn decode_attachment_manifest_bytes(
    manifest: JsValue,
    bytes: Vec<u8>,
    item_count: u32,
) -> Result<JsValue, JsValue> {
    let manifest: FileRef = from_js(manifest)?;
    to_js_admitted(&decode_attachment_manifest(&manifest, &bytes, item_count).map_err(js_error)?)
}

/// Converts a fully captured report into its canonical publishable child seed.
#[wasm_bindgen(js_name = forkSeedFromReport)]
pub fn fork_seed_from_report(report: JsValue) -> Result<JsValue, JsValue> {
    let report: ForkReport = from_js(report)?;
    to_js_admitted(&report.into_seed().map_err(js_error)?)
}

#[wasm_bindgen]
impl WasmReducer {
    /// Creates an empty reducer with explicit host-managed authority.
    #[wasm_bindgen(constructor)]
    pub fn new(
        authority: JsValue,
        issuer_id: String,
        issuer_key: Vec<u8>,
        schemas: JsValue,
    ) -> Result<Self, JsValue> {
        let authority: Authority = from_js(authority)?;
        let key = key_bytes(issuer_key)?;
        let issuer = AuthorityIssuer::new(issuer_id, key, authority.clone());
        Ok(Self {
            reducer: Reducer::new(authority, issuer.verifier(), schema_registry(schemas)?),
            issuer,
        })
    }

    /// Issues a root scope from this host's explicit authority object.
    #[wasm_bindgen(js_name = issueScope)]
    pub fn issue_scope(&self, id: String, capabilities: JsValue) -> Result<JsValue, JsValue> {
        let capabilities: Vec<String> = from_js(capabilities)?;
        to_js(&self.issuer.root(id, Capabilities::new(capabilities)))
    }

    /// Issues a root scope signed for one acting agent.
    #[wasm_bindgen(js_name = issueScopeForAgent)]
    pub fn issue_scope_for_agent(
        &self,
        agent: String,
        id: String,
        capabilities: JsValue,
    ) -> Result<JsValue, JsValue> {
        let agent = AgentId::parse(&agent).map_err(js_error)?;
        let capabilities: Vec<String> = from_js(capabilities)?;
        to_js(
            &self
                .issuer
                .root_for_agent(agent, id, Capabilities::new(capabilities)),
        )
    }

    /// Resolves named policy layers and issues only their effective grant.
    #[wasm_bindgen(js_name = issueScopeWithPolicies)]
    pub fn issue_scope_with_policies(
        &self,
        id: String,
        layers: JsValue,
    ) -> Result<JsValue, JsValue> {
        let layers: Vec<PolicyLayer> = from_js(layers)?;
        to_js(
            &self
                .issuer
                .root_with_policies(id, &layers)
                .map_err(js_error)?,
        )
    }

    /// Resolves hierarchy policy and binds its result to one acting agent.
    #[wasm_bindgen(js_name = issueScopeWithPoliciesForAgent)]
    pub fn issue_scope_with_policies_for_agent(
        &self,
        agent: String,
        id: String,
        layers: JsValue,
    ) -> Result<JsValue, JsValue> {
        let agent = AgentId::parse(&agent).map_err(js_error)?;
        let layers: Vec<PolicyLayer> = from_js(layers)?;
        to_js(
            &self
                .issuer
                .root_with_policies_for_agent(agent, id, &layers)
                .map_err(js_error)?,
        )
    }

    /// Attenuates a scope without permitting capability expansion.
    pub fn attenuate(
        &self,
        parent: JsValue,
        id: String,
        capabilities: JsValue,
    ) -> Result<JsValue, JsValue> {
        let parent: Scope = from_js(parent)?;
        let capabilities: Vec<String> = from_js(capabilities)?;
        let scope = self
            .issuer
            .attenuate(&parent, id, Capabilities::new(capabilities))
            .map_err(js_error)?;
        to_js(&scope)
    }

    /// Delegates one pinned private file from its signed owner to a reader.
    #[wasm_bindgen(js_name = delegatePrivateFileRead)]
    pub fn delegate_private_file_read(
        &self,
        owner_scope: JsValue,
        reader: String,
        id: String,
        file: JsValue,
    ) -> Result<JsValue, JsValue> {
        let owner_scope: Scope = from_js(owner_scope)?;
        let reader = AgentId::parse(&reader).map_err(js_error)?;
        let file: FileRef = from_js(file)?;
        to_js(
            &self
                .issuer
                .delegate_private_file_read(&owner_scope, reader, id, &file)
                .map_err(js_error)?,
        )
    }

    /// Delegates a segment-bounded private directory to a reader without a fork.
    #[wasm_bindgen(js_name = delegatePrivateDirectoryRead)]
    pub fn delegate_private_directory_read(
        &self,
        owner_scope: JsValue,
        reader: String,
        id: String,
        volume: JsValue,
        prefix: String,
    ) -> Result<JsValue, JsValue> {
        let owner_scope: Scope = from_js(owner_scope)?;
        let reader = AgentId::parse(&reader).map_err(js_error)?;
        let volume: VolumeRef = from_js(volume)?;
        to_js(
            &self
                .issuer
                .delegate_private_directory_read(&owner_scope, reader, id, &volume, &prefix)
                .map_err(js_error)?,
        )
    }

    /// Computes the canonical Rust capability for one validated volume operation.
    #[wasm_bindgen(js_name = volumeCapability)]
    pub fn volume_capability(&self, volume: JsValue, operation: String) -> Result<String, JsValue> {
        let volume: VolumeRef = from_js(volume)?;
        let operation = match operation.as_str() {
            "read" => VolumeOperation::Read,
            "write" => VolumeOperation::Write,
            _ => {
                return Err(js_error(crate::Error::Invalid(
                    "volume operation is invalid".into(),
                )));
            }
        };
        volume.capability(operation).map_err(js_error)
    }

    /// Computes the exact-version read capability without embedding it in a ref.
    #[wasm_bindgen(js_name = fileReadCapability)]
    pub fn file_read_capability(&self, file: JsValue) -> Result<String, JsValue> {
        let file: FileRef = from_js(file)?;
        file.read_capability().map_err(js_error)
    }

    /// Computes the canonical capability for one private directory boundary.
    #[wasm_bindgen(js_name = directoryReadCapability)]
    pub fn directory_read_capability(
        &self,
        volume: JsValue,
        prefix: String,
    ) -> Result<String, JsValue> {
        let volume: VolumeRef = from_js(volume)?;
        volume.directory_read_capability(&prefix).map_err(js_error)
    }

    /// Authenticates an owner-issued read grant for this exact immutable file.
    /// A caller-supplied capability string alone is never accepted as proof.
    #[wasm_bindgen(js_name = verifyContentRead)]
    pub fn verify_content_read(&self, scope: JsValue, file: JsValue) -> Result<(), JsValue> {
        let scope: Scope = from_js(scope)?;
        let file: FileRef = from_js(file)?;
        ContentGrant::verify_read(&self.issuer.verifier(), &scope, &file).map_err(js_error)?;
        Ok(())
    }

    /// Authenticates the signed scope before provider discovery or routing.
    #[wasm_bindgen(js_name = verifyScope)]
    pub fn verify_scope(&self, scope: JsValue) -> Result<(), JsValue> {
        let scope: Scope = from_js(scope)?;
        self.issuer.verifier().verify(&scope).map_err(js_error)
    }

    /// Authenticates a writer against the original private-volume owner.
    #[wasm_bindgen(js_name = verifyContentWrite)]
    pub fn verify_content_write(&self, scope: JsValue, volume: JsValue) -> Result<(), JsValue> {
        let scope: Scope = from_js(scope)?;
        let volume: VolumeRef = from_js(volume)?;
        ContentGrant::verify(
            &self.issuer.verifier(),
            &scope,
            &volume,
            VolumeOperation::Write,
        )
        .map_err(js_error)?;
        Ok(())
    }

    /// Stable physical namespace shared by Filesystem and Objects adapters.
    #[wasm_bindgen(js_name = volumeStorageName)]
    pub fn volume_storage_name(&self, volume: JsValue) -> Result<String, JsValue> {
        let volume: VolumeRef = from_js(volume)?;
        volume.storage_name().map_err(js_error)
    }

    /// Validates an exact content reference using the native Rust contract.
    #[wasm_bindgen(js_name = validateFileRef)]
    pub fn validate_file_ref(&self, file: JsValue) -> Result<(), JsValue> {
        let file: FileRef = from_js(file)?;
        file.validate().map_err(js_error)
    }

    /// Derives the canonical SHA-256 descriptor for staged bytes.
    #[wasm_bindgen(js_name = fileDescriptorJson)]
    pub fn file_descriptor_json(
        &self,
        bytes: Vec<u8>,
        media_type: String,
    ) -> Result<String, JsValue> {
        let descriptor = FileDescriptor::from_bytes(&bytes, media_type).map_err(js_error)?;
        String::from_utf8(crate::contract::canonical_json_bytes(&descriptor).map_err(js_error)?)
            .map_err(|error| JsValue::from_str(&error.to_string()))
    }

    /// Applies configured runtime bounds to one exact file reference.
    #[wasm_bindgen(js_name = validateFileUnderLimits)]
    pub fn validate_file_under_limits(
        &self,
        file: JsValue,
        limits: JsValue,
    ) -> Result<(), JsValue> {
        let file: FileRef = from_js(file)?;
        let limits: Limits = from_js(limits)?;
        limits.validate_file(&file).map_err(js_error)
    }

    /// Validates one ref-only canonical message under explicit runtime limits.
    #[wasm_bindgen(js_name = validateConversationMessage)]
    pub fn validate_conversation_message(
        &self,
        message: JsValue,
        limits: JsValue,
    ) -> Result<(), JsValue> {
        let message: ConversationMessage = from_js(message)?;
        let limits: Limits = from_js(limits)?;
        limits.validate_message(&message).map_err(js_error)
    }

    /// Verifies exact bytes against the pinned SHA-256 and byte-length descriptor.
    #[wasm_bindgen(js_name = verifyFileBytes)]
    pub fn verify_file_bytes(&self, file: JsValue, bytes: Vec<u8>) -> Result<(), JsValue> {
        verify_file_bytes(file, bytes)
    }

    /// Resolves a complete canonical attachment manifest under the Rust rules.
    #[wasm_bindgen(js_name = decodeAttachmentManifest)]
    pub fn decode_attachment_manifest(
        &self,
        manifest: JsValue,
        bytes: Vec<u8>,
        item_count: u32,
    ) -> Result<JsValue, JsValue> {
        decode_attachment_manifest_bytes(manifest, bytes, item_count)
    }

    /// Returns the authoritative conversation projection, never a parallel JS reducer.
    #[wasm_bindgen(js_name = conversationJson)]
    pub fn conversation_json(&self) -> Result<String, JsValue> {
        let conversation = self
            .reducer
            .conversation()
            .ok_or_else(|| JsValue::from_str("aggregate is not a conversation"))?;
        let bytes = crate::contract::canonical_json_bytes(conversation).map_err(js_error)?;
        if bytes.len() > 16 * 1024 * 1024 {
            return Err(JsValue::from_str(
                "conversation snapshot exceeds the byte limit; use pages",
            ));
        }
        String::from_utf8(bytes).map_err(|error| JsValue::from_str(&error.to_string()))
    }

    /// Returns an immutable, bounded page of canonical conversation records.
    /// `after_sequence` is an exclusive cursor in the message sequence, not
    /// the potentially larger aggregate event revision.
    #[wasm_bindgen(js_name = conversationPageJson)]
    pub fn conversation_page_json(
        &self,
        after_sequence: u64,
        limit: u32,
    ) -> Result<String, JsValue> {
        let page = conversation_page_data(&self.reducer, after_sequence, limit)?;
        let bytes = crate::contract::canonical_json_bytes(&page).map_err(js_error)?;
        if bytes.len() > 8 * 1024 * 1024 {
            return Err(JsValue::from_str(
                "conversation page exceeds the byte limit",
            ));
        }
        String::from_utf8(bytes).map_err(|error| JsValue::from_str(&error.to_string()))
    }

    /// Returns the same bounded page with Rust-owned JS integer projection:
    /// revisions and cursors stay `BigInt`, validated file lengths become Number.
    #[wasm_bindgen(js_name = conversationPage)]
    pub fn conversation_page(&self, after_sequence: u64, limit: u32) -> Result<JsValue, JsValue> {
        let page = conversation_page_data(&self.reducer, after_sequence, limit)?;
        let bytes = crate::contract::canonical_json_bytes(&page).map_err(js_error)?;
        if bytes.len() > 8 * 1024 * 1024 {
            return Err(JsValue::from_str(
                "conversation page exceeds the byte limit",
            ));
        }
        to_js_admitted(&page)
    }

    /// Applies one command through the same deterministic Rust reducer as native hosts.
    pub fn apply(&mut self, command: JsValue) -> Result<JsValue, JsValue> {
        let command: Command = from_js(command)?;
        let result: ApplyResult = self.reducer.apply(command).map_err(js_error)?;
        to_js(&result)
    }

    /// Applies one canonical Protobuf command and returns a Protobuf response.
    #[wasm_bindgen(js_name = applyWire)]
    pub fn apply_wire(&mut self, command: Vec<u8>) -> Result<Vec<u8>, JsValue> {
        let (authority, command) = decode_command(&command).map_err(js_error)?;
        if &authority != self.reducer.authority() {
            return Err(JsValue::from_str(
                "command authority does not match reducer",
            ));
        }
        let result = self.reducer.apply(command).map_err(js_error)?;
        encode_apply_result(&authority, &result).map_err(js_error)
    }

    /// Returns the exact wire identity used by this compiled core.
    #[wasm_bindgen(js_name = protocolIdentity)]
    pub fn protocol_identity(&self) -> Result<JsValue, JsValue> {
        let identity = protocol_identity();
        to_js(&crate::ProtocolIdentity {
            version: identity.version,
            descriptor_digest: identity.descriptor_digest,
        })
    }

    /// Returns a versioned integrity-checked restoration snapshot.
    pub fn snapshot(&self) -> Result<JsValue, JsValue> {
        to_js(&self.reducer.snapshot().map_err(js_error)?)
    }

    /// Restores a snapshot under explicit host-managed authority.
    pub fn restore(
        snapshot: JsValue,
        issuer_id: String,
        issuer_key: Vec<u8>,
        schemas: JsValue,
    ) -> Result<Self, JsValue> {
        let snapshot: Snapshot = from_js(snapshot)?;
        let issuer = AuthorityIssuer::new(
            issuer_id,
            key_bytes(issuer_key)?,
            snapshot.authority.clone(),
        );
        let reducer = Reducer::restore(snapshot, issuer.verifier(), schema_registry(schemas)?)
            .map_err(js_error)?;
        Ok(Self { reducer, issuer })
    }
}

#[derive(serde::Deserialize)]
struct SchemaDefinition {
    name: String,
    version: u32,
    schema: serde_json::Value,
    implementation_digest: [u8; 32],
    fork_policy: ExtensionForkPolicy,
}

fn schema_registry(value: JsValue) -> Result<SchemaRegistry, JsValue> {
    let definitions: Vec<SchemaDefinition> = from_js(value)?;
    let mut registry = SchemaRegistry::new();
    for definition in definitions {
        registry
            .register(
                definition.name,
                definition.version,
                definition.schema,
                definition.implementation_digest,
                definition.fork_policy,
            )
            .map_err(js_error)?;
    }
    Ok(registry)
}

fn key_bytes(value: Vec<u8>) -> Result<[u8; 32], JsValue> {
    value
        .try_into()
        .map_err(|_| JsValue::from_str("authority key must contain exactly 32 bytes"))
}

fn from_js<T: serde::de::DeserializeOwned>(value: JsValue) -> Result<T, JsValue> {
    // A single traversal detaches the admitted value from getters and Proxies.
    // Never validate the original object and then deserialize it again: a
    // mutable accessor could return a finite number first and Infinity later.
    let value = js_json_value(&value)?;
    serde_json::from_value(value).map_err(|error| JsValue::from_str(&error.to_string()))
}

fn to_js<T: serde::Serialize>(value: &T) -> Result<JsValue, JsValue> {
    let serializer = serde_wasm_bindgen::Serializer::new()
        .serialize_large_number_types_as_bigints(true)
        .serialize_missing_as_null(true);
    value
        .serialize(&serializer)
        .map_err(|error| JsValue::from_str(&error.to_string()))
}

// Durable JSON keeps integral literals. Only admitted, ref-only contract
// outputs pass through this projection: a validated FileDescriptor length is
// represented as Number and every other 64-bit integer remains BigInt.
// Generic events, snapshots, model values and extension payloads use to_js.
fn to_js_admitted<T: serde::Serialize>(value: &T) -> Result<JsValue, JsValue> {
    let js = to_js(value)?;
    let json =
        serde_json::to_value(value).map_err(|error| JsValue::from_str(&error.to_string()))?;
    normalize_descriptor_lengths(&js, &json)?;
    Ok(js)
}

fn exact_js_number(value: u64) -> Result<JsValue, JsValue> {
    if value > 9_007_199_254_740_991 {
        return Err(JsValue::from_str(
            "integer exceeds JavaScript Number precision",
        ));
    }
    let number = value
        .to_string()
        .parse::<f64>()
        .map_err(|_| JsValue::from_str("integer cannot be projected to JavaScript"))?;
    Ok(JsValue::from_f64(number))
}

fn set_js_field(js: &JsValue, key: &str, value: &JsValue) -> Result<(), JsValue> {
    let key = JsValue::from_str(key);
    if js.is_instance_of::<js_sys::Map>() {
        js.unchecked_ref::<js_sys::Map>().set(&key, value);
    } else if !js_sys::Reflect::set(js, &key, value)? {
        return Err(JsValue::from_str("typed JS projection is not writable"));
    }
    Ok(())
}

fn normalize_descriptor_lengths(js: &JsValue, value: &serde_json::Value) -> Result<(), JsValue> {
    match value {
        serde_json::Value::Object(fields) => {
            if fields.len() == 3
                && fields.contains_key("sha256")
                && fields.contains_key("byte_length")
                && fields.contains_key("media_type")
            {
                let descriptor: FileDescriptor =
                    serde_json::from_value(serde_json::Value::Object(fields.clone()))
                        .map_err(|error| JsValue::from_str(&error.to_string()))?;
                set_js_field(
                    js,
                    "byte_length",
                    &exact_js_number(descriptor.byte_length())?,
                )?;
            }
            for (key, child) in fields {
                let key = JsValue::from_str(key);
                let js_child = if js.is_instance_of::<js_sys::Map>() {
                    js.unchecked_ref::<js_sys::Map>().get(&key)
                } else {
                    js_sys::Reflect::get(js, &key)?
                };
                normalize_descriptor_lengths(&js_child, child)?;
            }
        }
        serde_json::Value::Array(items) => {
            for (index, child) in items.iter().enumerate() {
                let index = u32::try_from(index)
                    .map_err(|_| JsValue::from_str("JS array index exceeds u32"))?;
                let js_child = js_sys::Reflect::get(js, &JsValue::from_f64(f64::from(index)))?;
                normalize_descriptor_lengths(&js_child, child)?;
            }
        }
        _ => {}
    }
    Ok(())
}

fn js_error(error: crate::Error) -> JsValue {
    JsValue::from_str(&error.to_string())
}
