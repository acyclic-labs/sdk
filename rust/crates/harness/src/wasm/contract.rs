use super::*;

/// Validates the immutable workflow admission through its core serde type.
#[wasm_bindgen(js_name = validateWorkflowAdmission, unchecked_return_type = "WasmWorkflowAdmissionWire")]
pub fn validate_workflow_admission(
    #[wasm_bindgen(unchecked_param_type = "WasmWorkflowAdmissionWire")] value: JsValue,
) -> Result<JsValue, JsValue> {
    let value: crate::workflow::WorkflowAdmission = from_js(value)?;
    value.validate(None).map_err(js_error)?;
    value
        .serialize(
            &serde_wasm_bindgen::Serializer::new().serialize_large_number_types_as_bigints(true),
        )
        .map_err(|error| JsValue::from_str(&error.to_string()))
}

fn admit<T: serde::de::DeserializeOwned + Serialize, R>(
    value: JsValue,
    validate: impl FnOnce(&T) -> crate::Result<R>,
) -> Result<JsValue, JsValue> {
    let value: T = from_js(value)?;
    validate(&value).map_err(js_error)?;
    to_js_admitted(&value)
}

/// Pure v2 contract admission shared by native and JavaScript hosts.
#[wasm_bindgen(js_name = validateContract)]
pub fn validate_contract(kind: &str, value: JsValue, context: JsValue) -> Result<JsValue, JsValue> {
    use crate::workflow::{MachineIdentity, WorkflowAdmission};
    use crate::{conversation::PrivateDirectoryPage, runtime::ExecutionPlacement};
    match kind {
        "provider_ref" => admit(value, |v: &ProviderRef| v.validate(&mut None)),
        "file_descriptor" => admit(value, |_: &FileDescriptor| Ok(())),
        "approval_binding" => admit(value, |v: &ApprovalBinding| v.validate()),
        "project_merge_receipt" => {
            admit(value, |v: &ProjectMergeReceipt| v.validate_shape(&mut None))
        }
        "volume_ref" => admit(value, |v: &VolumeRef| v.validate(&mut None)),
        "file_ref" => admit(value, |v: &FileRef| v.validate(&mut None)),
        "attachments" => admit(value, |v: &ReferencedAttachments| v.validate(&mut None)),
        "task_outcome" => admit(value, |v: &TaskOutcomeRecord| v.validate()),
        "execution_placement" => admit(value, |v: &ExecutionPlacement| v.validate(&mut None)),
        "machine_identity" => admit(value, |v: &MachineIdentity| v.validate(&mut None)),
        "workflow_admission" => admit(value, |v: &WorkflowAdmission| v.validate(None)),
        "resource_ref" => admit(value, |v: &ResourceRef| v.validate(&mut None)),
        "private_directory_page" => admit(value, |v: &PrivateDirectoryPage| v.validate()),
        "resource_revision" => admit(value, |v: &ResourceRevision| v.validate(&mut None)),
        "fork_request" => admit(value, |v: &ForkRequest| v.validate(&mut None)),
        "fork_report" => admit(value, |v: &ForkReport| v.validate(&mut None)),
        "fork_seed" => admit(value, |v: &ForkSeed| v.validate(&mut None)),
        "reference_grant" => admit(value, |v: &ReferenceGrant| v.validate(&mut None)),
        "extension_record" => admit(value, |v: &ExtensionRecord| v.validate()),
        "extension_state_migration" => admit(value, |v: &ExtensionStateMigration| v.validate()),
        "extension_dependency" => admit(value, |v: &ExtensionDependency| v.validate()),
        "extension_configuration" => admit(value, |v: &ExtensionConfiguration| v.validate()),
        "extension_admission" => admit(value, |v: &ExtensionAdmission| v.validate(&mut None)),
        "interaction_ticket" => admit(value, |v: &InteractionTicket| v.validate(&mut None)),
        "resolution_receipt" => admit(value, |v: &ResolutionReceipt| v.validate()),
        "conversation_message" => {
            let limits: Limits = from_js(context)?;
            admit(value, |v: &ConversationMessage| limits.validate_message(v, &mut None))
        }
        "interaction_resolution" => {
            let ticket: InteractionTicket = from_js(context)?;
            ticket.validate(&mut None).map_err(js_error)?;
            admit(value, |v: &InteractionResolution| v.validate(&ticket))
        }
        "task_admission" => {
            let value: serde_json::Value = from_js(value)?;
            let request = TaskAdmissionRecord::from_canonical_value(value, &mut None)
                .map_err(js_error)?
                .0;
            to_js_admitted(&request.canonical_value())
        }
        "durable_batch_request" => {
            let value: serde_json::Value = from_js(value)?;
            let request = DurableBatchRequest::from_canonical_value(value).map_err(js_error)?;
            to_js_admitted(&request.canonical_value())
        }
        "limits" => {
            let value: Limits = from_js(value)?;
            value.validate(&mut None).map_err(js_error)?;
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
        _ => Err(JsValue::from_str("unknown Harness v2 contract kind")),
    }
}

/// Validates and reprojects one owner-retained direct-child page using the
/// same bounds and bytewise slot ordering as native Harness hosts.
#[wasm_bindgen(
    js_name = validateTaskChildrenPage,
    unchecked_return_type = "WasmTaskChildrenPage"
)]
pub fn validate_task_children_page(
    #[wasm_bindgen(unchecked_param_type = "WasmTaskChildrenPageInput")] value: JsValue,
) -> Result<JsValue, JsValue> {
    let input: WasmTaskChildrenPageInput = from_js(value)?;
    let maximum = usize::try_from(input.maximum)
        .map_err(|_| JsValue::from_str("task child page maximum is not representable"))?;
    validate_children_request(input.parent, input.after_slot.as_deref(), maximum)
        .map_err(js_error)?;
    let page = TaskChildrenPage {
        revision: input.page.revision,
        entries: input
            .page
            .entries
            .iter()
            .map(|entry| TaskChild {
                slot: entry.slot.clone(),
                task_id: entry.task_id,
            })
            .collect(),
        next_after: input.page.next_after.clone(),
    };
    validate_children_page(
        &page,
        input.expected_revision,
        input.after_slot.as_deref(),
        maximum,
    )
    .map_err(js_error)?;
    to_js(&input.page)
}

/// Applies the same JSON Schema admission used by Rust tool execution before a TypeScript facade turns an untrusted model value into a typed argument.
#[wasm_bindgen(js_name = validateToolValue)]
pub fn validate_tool_value(schema: JsValue, value: JsValue) -> Result<JsValue, JsValue> {
    let schema: serde_json::Value = from_js(schema)?;
    let value: serde_json::Value = from_js(value)?;
    crate::contract::validate_json_schema_value(&schema, &value, "tool value", &mut None)
        .map_err(js_error)?;
    to_js(&value)
}

/// Checks immutable file identity without constructing a reducer or issuer.
#[wasm_bindgen(js_name = verifyFileBytes)]
pub fn verify_file_bytes(file: JsValue, bytes: Vec<u8>) -> Result<(), JsValue> {
    let file: FileRef = from_js(file)?;
    file.validate(&mut None).map_err(js_error)?;
    file.descriptor()
        .verify(&bytes, &mut None)
        .map_err(js_error)
}

/// Stages a descriptor with Rust-owned SHA-256, media-type, and safe byte-length rules, projecting its bounded length as a JS Number.
#[wasm_bindgen(js_name = fileDescriptor)]
pub fn file_descriptor(bytes: &[u8], media_type: &str) -> Result<JsValue, JsValue> {
    let descriptor = FileDescriptor::from_bytes(bytes, media_type, &mut None).map_err(js_error)?;
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
    to_js_admitted(
        &decode_attachment_manifest(&manifest, &bytes, Some(item_count), None).map_err(js_error)?,
    )
}

/// Converts a fully captured report into its canonical publishable child seed.
#[wasm_bindgen(js_name = forkSeedFromReport)]
pub fn fork_seed_from_report(report: JsValue) -> Result<JsValue, JsValue> {
    let report: ForkReport = from_js(report)?;
    to_js_admitted(&report.into_seed(&mut None).map_err(js_error)?)
}
