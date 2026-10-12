use super::*;

#[derive(Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub(super) struct WasmTaskChildrenPageInput {
    pub(super) parent: TaskId,
    pub(super) expected_revision: Option<u64>,
    pub(super) after_slot: Option<String>,
    pub(super) maximum: u32,
    pub(super) page: WasmTaskChildrenPage,
}

#[derive(Deserialize, Tsify)]
#[serde(deny_unknown_fields)]
pub(super) struct WasmTaskIdentityInput {
    #[tsify(type = "string")]
    name: String,
    #[tsify(type = "string")]
    version: String,
    #[tsify(type = "WasmToolJsonSchema")]
    input_schema: serde_json::Value,
    #[tsify(type = "WasmToolJsonSchema")]
    output_schema: serde_json::Value,
    #[tsify(type = "readonly string[]")]
    requirements: BTreeSet<String>,
    #[tsify(type = "readonly number[]")]
    machine_digest: Vec<u8>,
}

#[derive(Deserialize, Tsify)]
#[serde(deny_unknown_fields)]
pub(super) struct WasmTaskDependencyDefinition {
    #[tsify(type = "string")]
    name: String,
    #[tsify(type = "string")]
    version: String,
    #[tsify(type = "readonly string[]")]
    requirements: Vec<String>,
}

#[derive(Deserialize, Tsify)]
#[serde(deny_unknown_fields)]
pub(super) struct WasmToolDependencyDefinition {
    #[tsify(type = "string")]
    name: String,
    #[tsify(type = "string")]
    version: String,
}

#[derive(Deserialize, Tsify)]
#[serde(deny_unknown_fields)]
pub(super) struct WasmExtensionDependencyDefinition {
    #[tsify(type = "string")]
    name: String,
    #[tsify(type = "number")]
    version: u32,
}

#[derive(Deserialize, Tsify)]
#[serde(deny_unknown_fields)]
pub(super) struct WasmTaskDependencyComponents {
    model: bool,
    context: bool,
    interactions: bool,
    policy: bool,
    host: bool,
    state: bool,
    spawner: bool,
    content: bool,
    artifacts: bool,
    content_write: bool,
    artifacts_write: bool,
}

#[derive(Deserialize, Tsify)]
#[serde(deny_unknown_fields)]
pub(super) struct WasmTaskDependencyInput {
    #[tsify(type = "readonly WasmTaskDependencyDefinition[]")]
    tasks: Vec<WasmTaskDependencyDefinition>,
    #[tsify(type = "readonly WasmToolDependencyDefinition[]")]
    tools: Vec<WasmToolDependencyDefinition>,
    components: WasmTaskDependencyComponents,
    #[tsify(type = "readonly string[]")]
    grants: Vec<String>,
    #[tsify(type = "readonly WasmExtensionDependencyDefinition[]")]
    extensions: Vec<WasmExtensionDependencyDefinition>,
}

#[derive(Deserialize, Tsify)]
#[serde(deny_unknown_fields)]
pub(super) struct WasmTaskAdmissionInput {
    #[tsify(type = "string")]
    operation_id: OperationId,
    #[tsify(type = "string")]
    name: String,
    #[tsify(type = "string")]
    version: String,
    #[tsify(type = "WasmToolJsonValue")]
    input: serde_json::Value,
    #[tsify(type = "WasmToolJsonSchema")]
    input_schema: serde_json::Value,
    #[tsify(type = "WasmToolJsonSchema")]
    output_schema: serde_json::Value,
    #[tsify(type = "readonly string[]")]
    requirements: BTreeSet<String>,
    #[tsify(type = "readonly number[]")]
    machine_digest: Vec<u8>,
    #[tsify(type = "string | null")]
    parent: Option<TaskId>,
    #[tsify(type = "readonly WasmResourceCreationProofRefWire[]")]
    resource_proofs: Vec<crate::resource_preparation::ResourceCreationProofRef>,
    #[tsify(type = "readonly string[]")]
    grants: Vec<String>,
    #[tsify(type = "WasmLimitsInput")]
    limits: Limits,
    #[tsify(type = "WasmTaskRunLimitsInput")]
    run_limits: TaskRunLimits,
    #[tsify(type = "WasmMachineIdentityWire | null")]
    policy: Option<crate::registry::ComponentIdentity>,
    #[tsify(type = "WasmExtensionAdmissionWire | null")]
    extensions: Option<ExtensionAdmission>,
    #[tsify(type = "WasmExecutionPlacementWire | null")]
    execution: Option<crate::runtime::ExecutionPlacement>,
}

#[derive(Deserialize, Tsify)]
#[serde(deny_unknown_fields)]
pub(super) struct WasmBatchAdmissionInput {
    #[tsify(type = "string")]
    group_id: GroupId,
    #[tsify(type = "string")]
    batch_id: BatchId,
    #[tsify(type = "\"collect-all\" | \"cancel-on-failure\"")]
    group_policy: BatchGroupPolicy,
    #[tsify(type = "string")]
    name: String,
    #[tsify(type = "string")]
    version: String,
    #[tsify(type = "readonly WasmToolJsonValue[]")]
    inputs: Vec<serde_json::Value>,
    #[tsify(type = "WasmToolJsonSchema")]
    input_schema: serde_json::Value,
    #[tsify(type = "WasmToolJsonSchema")]
    output_schema: serde_json::Value,
    #[tsify(type = "readonly string[]")]
    requirements: BTreeSet<String>,
    #[tsify(type = "readonly number[]")]
    machine_digest: Vec<u8>,
    #[tsify(type = "string | null")]
    parent: Option<TaskId>,
    #[tsify(type = "readonly WasmResourceCreationProofRefWire[]")]
    resource_proofs: Vec<crate::resource_preparation::ResourceCreationProofRef>,
    #[tsify(type = "readonly string[]")]
    grants: Vec<String>,
    #[tsify(type = "WasmLimitsInput")]
    limits: Limits,
    #[tsify(type = "WasmTaskRunLimitsInput")]
    run_limits: TaskRunLimits,
    #[tsify(type = "WasmExtensionAdmissionWire | null")]
    extensions: Option<ExtensionAdmission>,
    #[tsify(type = "WasmMachineIdentityWire | null")]
    policy: Option<crate::registry::ComponentIdentity>,
    #[tsify(type = "WasmExecutionPlacementWire | null")]
    execution: Option<crate::runtime::ExecutionPlacement>,
}

/// Complete public durable batch admission returned by the Rust projection.
/// The outer request uses the SDK's camelCase host shape while `canonical` and
/// each member retain the exact Rust-owned v2 wire envelopes. Keeping this
/// projection together prevents TypeScript callers from accidentally changing
/// the member list, digest, or implementation identity independently.
#[derive(Serialize)]
pub(super) struct WasmBatchAdmissionRequest {
    contract: &'static str,
    #[serde(rename = "groupId")]
    group_id: GroupId,
    #[serde(rename = "batchId")]
    batch_id: BatchId,
    #[serde(rename = "taskName")]
    task_name: String,
    revision: String,
    #[serde(rename = "implementationDigest")]
    implementation_digest: String,
    #[serde(rename = "parentTaskId")]
    parent_task_id: Option<TaskId>,
    policy: WasmGroupPolicy,
    members: Vec<serde_json::Value>,
    canonical: serde_json::Value,
    #[serde(rename = "inputDigest")]
    input_digest: Vec<u8>,
}

#[derive(Serialize)]
pub(super) struct WasmGroupPolicy {
    kind: BatchGroupPolicy,
}

/// Derives the same immutable per-slot operation as Rust durable admission.
#[wasm_bindgen(js_name = batchMemberOperationId)]
pub fn batch_member_operation_id_wasm(
    group: &str,
    batch: &str,
    index: u32,
) -> Result<String, JsValue> {
    let group = GroupId::parse(group).map_err(js_error)?;
    let batch = BatchId::parse(batch).map_err(js_error)?;
    Ok(batch_member_operation_id(group, batch, index as usize).to_string())
}

/// Derives the same pinned task registration digest used by native admission.
#[wasm_bindgen(js_name = taskIdentityDigest)]
pub fn task_identity_digest_wasm(
    name: &str,
    version: &str,
    input_schema: JsValue,
    output_schema: JsValue,
    requirements: JsValue,
    machine_digest: &[u8],
) -> Result<Vec<u8>, JsValue> {
    let input_schema: serde_json::Value = from_js(input_schema)?;
    let output_schema: serde_json::Value = from_js(output_schema)?;
    let requirements: Vec<String> = from_js(requirements)?;
    let machine_digest: Option<[u8; 32]> = match machine_digest {
        [] => None,
        bytes => Some(
            bytes
                .try_into()
                .map_err(|_| JsValue::from_str("machine digest must contain exactly 32 bytes"))?,
        ),
    };
    task_definition_digest(
        name,
        version,
        &input_schema,
        &output_schema,
        &requirements.into_iter().collect(),
        machine_digest,
    )
    .map(|digest| digest.to_vec())
    .map_err(js_error)
}

/// Derives the exact task and machine identities retained by durable
/// admission. The digest envelope and resumable machine pin are shared with
/// native Rust registration.
#[wasm_bindgen(
    js_name = taskAdmissionIdentities,
    unchecked_return_type = "WasmTaskAdmissionIdentities"
)]
pub fn task_admission_identities_wasm(
    #[wasm_bindgen(unchecked_param_type = "WasmTaskIdentityInput")] value: JsValue,
) -> Result<JsValue, JsValue> {
    let input: WasmTaskIdentityInput = from_js(value)?;
    let (task, machine) = task_admission_identities(
        &input.name,
        &input.version,
        &input.input_schema,
        &input.output_schema,
        &input.requirements,
        &input.machine_digest,
    )
    .map_err(js_error)?;
    to_js_admitted(&serde_json::json!({ "task": task, "machine": machine }))
}

/// Validates the exact task dependency graph used by the TypeScript builder.
#[wasm_bindgen(js_name = validateTaskRequirements)]
pub fn validate_task_requirements_wasm(
    #[wasm_bindgen(unchecked_param_type = "WasmTaskDependencyInput")] value: JsValue,
) -> Result<(), JsValue> {
    let input: WasmTaskDependencyInput = from_js(value)?;
    let mut tasks = std::collections::BTreeMap::new();
    for definition in input.tasks {
        let key = (definition.name, definition.version);
        if tasks
            .insert(key.clone(), definition.requirements.into_iter().collect())
            .is_some()
        {
            return Err(js_error(crate::Error::Conflict(format!(
                "conflicting task registration for {}@{}",
                key.0, key.1
            ))));
        }
    }
    let tools = input
        .tools
        .into_iter()
        .map(|definition| (definition.name, definition.version))
        .collect();
    let extensions = input
        .extensions
        .into_iter()
        .map(|definition| (definition.name, definition.version))
        .collect();
    let environment = TaskDependencyEnvironment {
        model: input.components.model,
        context: input.components.context,
        interactions: input.components.interactions,
        policy: input.components.policy,
        host: input.components.host,
        state: input.components.state,
        spawner: input.components.spawner,
        content: input.components.content,
        artifacts: input.components.artifacts,
        content_write: input.components.content_write,
        artifacts_write: input.components.artifacts_write,
        extensions,
    };
    validate_task_requirements(
        &tasks,
        &tools,
        &environment,
        &input.grants.into_iter().collect(),
        None,
    )
    .map_err(js_error)
}

/// Builds and validates the complete owner-retained task admission envelope.
#[wasm_bindgen(js_name = admitTask, unchecked_return_type = "WasmTaskAdmissionWire")]
pub fn admit_task_wasm(
    #[wasm_bindgen(unchecked_param_type = "WasmTaskAdmissionInput")] value: JsValue,
) -> Result<JsValue, JsValue> {
    let input: WasmTaskAdmissionInput = from_js(value)?;
    let record = TaskAdmissionRecord::from_parts(
        input.operation_id,
        &input.name,
        &input.version,
        input.input,
        input.input_schema,
        input.output_schema,
        &input.requirements,
        &input.machine_digest,
        input.parent,
        Capabilities::new(input.grants),
        input.limits,
        input.run_limits,
        input.policy,
        input.extensions,
        input.execution,
    )
    .and_then(|record| record.with_resource_proofs(input.resource_proofs))
    .map_err(js_error)?;
    to_js_admitted(&record.canonical_value())
}

pub(super) fn durable_batch_request_from_input(
    input: WasmBatchAdmissionInput,
) -> Result<DurableBatchRequest, JsValue> {
    DurableBatchRequest::from_parts(
        input.group_id,
        input.batch_id,
        input.group_policy,
        &input.name,
        &input.version,
        input.inputs,
        input.input_schema,
        input.output_schema,
        &input.requirements,
        &input.machine_digest,
        input.parent,
        Capabilities::new(input.grants),
        input.limits,
        input.run_limits,
        input.extensions,
        input.policy,
        input.execution,
    )
    .and_then(|request| request.with_resource_proofs(input.resource_proofs))
    .map_err(js_error)
}

/// Builds and validates the complete immutable batch request before any
/// member admission. Inputs, task identity, limits, policy, and route are
/// projected by the same Rust constructor used by native hosts.
#[wasm_bindgen(js_name = admitBatch, unchecked_return_type = "WasmDurableBatchWire")]
pub fn admit_batch_wasm(
    #[wasm_bindgen(unchecked_param_type = "WasmBatchAdmissionInput")] value: JsValue,
) -> Result<JsValue, JsValue> {
    let input: WasmBatchAdmissionInput = from_js(value)?;
    let request = durable_batch_request_from_input(input)?;
    to_js_admitted(&request.canonical_value())
}

/// Builds the complete SDK-facing durable batch request in one Rust-owned projection.
#[wasm_bindgen(
    js_name = admitBatchRequest,
    unchecked_return_type = "WasmBatchAdmissionRequest"
)]
pub fn admit_batch_request_wasm(
    #[wasm_bindgen(unchecked_param_type = "WasmBatchAdmissionInput")] value: JsValue,
) -> Result<JsValue, JsValue> {
    let input: WasmBatchAdmissionInput = from_js(value)?;
    let request = durable_batch_request_from_input(input)?;
    let implementation_digest = blake3::Hash::from_bytes(request.machine.digest)
        .to_hex()
        .to_string();
    let canonical = request.canonical_value();
    let members = (0..request.inputs.len())
        .map(|index| {
            request
                .member_admission(index)
                .map(|member| member.canonical_value())
        })
        .collect::<Result<Vec<_>, _>>()
        .map_err(js_error)?;
    let input_digest = crate::executor::json::canonical_json_digest(&canonical, u64::MAX, &mut None)
        .map_err(js_error)?
        .to_vec();
    let projection = WasmBatchAdmissionRequest {
        contract: "harness.batch.v2",
        group_id: request.group_id,
        batch_id: request.batch_id,
        task_name: request.task.name.clone(),
        revision: request.task.version.clone(),
        implementation_digest,
        parent_task_id: request.parent,
        policy: WasmGroupPolicy {
            kind: request.group_policy,
        },
        members,
        canonical,
        input_digest,
    };
    to_js_admitted(&projection)
}
