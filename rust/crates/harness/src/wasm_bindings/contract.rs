use super::*;


/// Public Rust projection for the immutable workflow admission envelope.
///
/// The validator still consumes the canonical `WorkflowAdmission`; this DTO
/// only gives the generated TypeScript surface the exact serde shape and
/// bigint treatment used by the Rust value.
#[derive(Clone, Debug, Serialize, Tsify)]
#[tsify(large_number_types_as_bigints)]
pub struct WasmWorkflowAdmissionWire {
    #[tsify(type = "string")]
    operation_id: String,
    #[tsify(type = "readonly number[]")]
    request_digest: Vec<u8>,
    #[tsify(
        type = "Readonly<{ readonly machine: WasmMachineIdentityWire; readonly revision: bigint; readonly state: unknown }>"
    )]
    initial: crate::workflow::MachineCheckpoint,
}

/// Validates and projects one immutable workflow admission through Rust.
#[wasm_bindgen(js_name = validateWorkflowAdmission)]
pub fn validate_workflow_admission(
    #[wasm_bindgen(unchecked_param_type = "WasmWorkflowAdmissionWire")] value: JsValue,
) -> Result<Ts<WasmWorkflowAdmissionWire>, JsValue> {
    let value: crate::workflow::WorkflowAdmission = from_js(value)?;
    value.validate(None).map_err(js_error)?;
    WasmWorkflowAdmissionWire {
        operation_id: value.operation_id.to_string(),
        request_digest: value.request_digest.to_vec(),
        initial: value.initial,
    }
    .into_ts()
    .map_err(|error| JsValue::from_str(&error.to_string()))
}

/// Pure v2 contract admission shared by native and JavaScript hosts.
#[wasm_bindgen(js_name = validateContract)]
#[expect(
    clippy::too_many_lines,
    reason = "one exhaustive v2 contract admission dispatch"
)]
pub fn validate_contract(kind: &str, value: JsValue, context: JsValue) -> Result<JsValue, JsValue> {
    // serde-wasm-bindgen otherwise coerces NaN/Infinity inside generic JSON fields to null before the Rust contract can reject them.
    match kind {
        "provider_ref" => {
            let value: ProviderRef = from_js(value)?;
            value.validate(&mut None).map_err(js_error)?;
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
            value.validate_shape(&mut None).map_err(js_error)?;
            to_js_admitted(&value)
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
        "volume_ref" => {
            let value: VolumeRef = from_js(value)?;
            value.validate(&mut None).map_err(js_error)?;
            to_js_admitted(&value)
        }
        "file_ref" => {
            let value: FileRef = from_js(value)?;
            value.validate(&mut None).map_err(js_error)?;
            to_js_admitted(&value)
        }
        "attachments" => {
            let value: ReferencedAttachments = from_js(value)?;
            value.validate(&mut None).map_err(js_error)?;
            to_js_admitted(&value)
        }
        "conversation_message" => {
            let value: ConversationMessage = from_js(value)?;
            let limits: Limits = from_js(context)?;
            limits.validate_message(&value, &mut None).map_err(js_error)?;
            to_js_admitted(&value)
        }
        "task_outcome" => {
            let value: TaskOutcomeRecord = from_js(value)?;
            value.validate().map_err(js_error)?;
            to_js_admitted(&value)
        }
        "execution_placement" => {
            let value: crate::runtime::ExecutionPlacement = from_js(value)?;
            value.validate(&mut None).map_err(js_error)?;
            to_js_admitted(&value)
        }
        "task_admission" => {
            let value: serde_json::Value = from_js(value)?;
            let request = crate::runtime::TaskAdmissionRecord::from_canonical_value(value, &mut None)
                .map_err(js_error)?.0;
            to_js_admitted(&request.canonical_value())
        }
        "machine_identity" => {
            let value: crate::workflow::MachineIdentity = from_js(value)?;
            value.validate(&mut None).map_err(js_error)?;
            to_js_admitted(&value)
        }
        "workflow_admission" => {
            let value: crate::workflow::WorkflowAdmission = from_js(value)?;
            value.validate(None).map_err(js_error)?;
            to_js_admitted(&value)
        }
        "durable_batch_request" => {
            let value: serde_json::Value = from_js(value)?;
            let request = DurableBatchRequest::from_canonical_value(value).map_err(js_error)?;
            to_js_admitted(&request.canonical_value())
        }
        "resource_ref" => {
            let value: ResourceRef = from_js(value)?;
            value.validate(&mut None).map_err(js_error)?;
            to_js_admitted(&value)
        }
        "private_directory_page" => {
            let value: crate::conversation::PrivateDirectoryPage = from_js(value)?;
            value.validate().map_err(js_error)?;
            to_js_admitted(&value)
        }
        "resource_revision" => {
            let value: ResourceRevision = from_js(value)?;
            value.validate(&mut None).map_err(js_error)?;
            to_js_admitted(&value)
        }
        "fork_request" => {
            let value: ForkRequest = from_js(value)?;
            value.validate(&mut None).map_err(js_error)?;
            to_js_admitted(&value)
        }
        "fork_report" => {
            let value: ForkReport = from_js(value)?;
            value.validate(&mut None).map_err(js_error)?;
            to_js_admitted(&value)
        }
        "fork_seed" => {
            let value: ForkSeed = from_js(value)?;
            value.validate(&mut None).map_err(js_error)?;
            to_js_admitted(&value)
        }
        "reference_grant" => {
            let value: ReferenceGrant = from_js(value)?;
            value.validate(&mut None).map_err(js_error)?;
            to_js_admitted(&value)
        }
        "extension_record" => {
            let value: ExtensionRecord = from_js(value)?;
            value.validate().map_err(js_error)?;
            to_js_admitted(&value)
        }
        "extension_state_migration" => {
            let value: ExtensionStateMigration = from_js(value)?;
            value.validate().map_err(js_error)?;
            to_js_admitted(&value)
        }
        "extension_dependency" => {
            let value: ExtensionDependency = from_js(value)?;
            value.validate().map_err(js_error)?;
            to_js_admitted(&value)
        }
        "extension_configuration" => {
            let value: ExtensionConfiguration = from_js(value)?;
            value.validate().map_err(js_error)?;
            to_js_admitted(&value)
        }
        "extension_admission" => {
            let value: ExtensionAdmission = from_js(value)?;
            value.validate(&mut None).map_err(js_error)?;
            to_js_admitted(&value)
        }
        "interaction_ticket" => {
            let value: InteractionTicket = from_js(value)?;
            value.validate(&mut None).map_err(js_error)?;
            to_js_admitted(&value)
        }
        "interaction_resolution" => {
            let value: InteractionResolution = from_js(value)?;
            let ticket: InteractionTicket = from_js(context)?;
            ticket.validate(&mut None).map_err(js_error)?;
            value.validate(&ticket).map_err(js_error)?;
            to_js_admitted(&value)
        }
        "resolution_receipt" => {
            let value: ResolutionReceipt = from_js(value)?;
            value.validate().map_err(js_error)?;
            to_js_admitted(&value)
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
    crate::contract::validate_json_schema_value(&schema, &value, "tool value", None).map_err(js_error)?;
    to_js(&value)
}

/// Checks immutable file identity without constructing a reducer or issuer.
#[wasm_bindgen(js_name = verifyFileBytes)]
pub fn verify_file_bytes(file: JsValue, bytes: Vec<u8>) -> Result<(), JsValue> {
    let file: FileRef = from_js(file)?;
    file.validate(&mut None).map_err(js_error)?;
    file.descriptor().verify(&bytes, &mut None).map_err(js_error)
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
    to_js_admitted(&decode_attachment_manifest(&manifest, &bytes, Some(item_count), None).map_err(js_error)?)
}

/// Converts a fully captured report into its canonical publishable child seed.
#[wasm_bindgen(js_name = forkSeedFromReport)]
pub fn fork_seed_from_report(report: JsValue) -> Result<JsValue, JsValue> {
    let report: ForkReport = from_js(report)?;
    to_js_admitted(&report.into_seed(&mut None).map_err(js_error)?)
}
