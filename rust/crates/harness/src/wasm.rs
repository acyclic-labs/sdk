//! Thin JavaScript host for the exact native reducer.
#![allow(
    clippy::needless_pass_by_value,
    reason = "wasm-bindgen's exported ABI owns JavaScript values and byte buffers"
)]

use crate::wire_codec::{
    decode_command, decode_event_payload as decode_payload_wire, encode_apply_result,
    protocol_identity,
};
use crate::{
    conversation::{
        decode_attachment_manifest, encode_attachment_manifest, Attachment, ContentGrant,
        ConversationMessage, ConversationState, FileDescriptor, FileRef, Limits,
        ModelContextSelection, ReferencedAttachments, TaskOutcomeRecord, VolumeOperation,
        VolumeRef,
    },
    core::{
        AggregateKind, ApplyResult, Authority, AuthorityIssuer, Command, ExtensionAdmission,
        ExtensionConfiguration, ExtensionDependency, ExtensionForkPolicy, ExtensionRecord,
        ExtensionStateMigration, Reducer, SchemaRegistry, Scope, Snapshot,
    },
    executor::{ModelEventAdmission, ModelEventAdmissionState},
    fork::{ForkReport, ForkRequest, ForkSeed, ReferenceGrant, ResourceRevision},
    interaction::{ApprovalBinding, InteractionResolution, InteractionTicket, ResolutionReceipt},
    merge::ProjectMergeReceipt,
    model::{ModelContent, ModelEvent, ModelMessage},
    projection::{
        select_model_context_at_revision, validate_model_context_selection_at_revision,
        AttachmentListResolver, SelectedModelContext,
    },
    resources::{ProviderRef, ResourceRef},
    runtime::{
        batch_member_operation_id, task_admission_identities, task_definition_digest,
        validate_children_page, validate_children_request, validate_task_requirements,
        BatchGroupPolicy, DurableBatchRequest, TaskAdmissionRecord, TaskChild, TaskChildrenPage,
        TaskDependencyEnvironment, TaskRunLimits,
    },
    tool::{validate_value, ToolDefinition},
    turn::prepare_turn,
    AgentId, BatchId, Capabilities, ConversationId, EffectId, GroupId, OperationId, PolicyLayer,
    SessionId, TaskId, TurnId,
};
use prost::Message as _;
use serde::{Deserialize, Serialize};
use sha2::{Digest as _, Sha256};
use std::collections::BTreeSet;
use tsify_next::Tsify;
use wasm_bindgen::prelude::*;

/// Returns the Rust-owned policy used by ephemeral memory content hosts.
///
/// These exports are deliberately small scalar values so generated bindings
/// remain strongly typed in every host language and adapters do not need to
/// deserialize a second policy document.
#[wasm_bindgen(js_name = harnessDefaultResidentBytes)]
pub fn harness_default_resident_bytes() -> f64 {
    crate::memory_store::DEFAULT_RESIDENT_BYTES as f64
}

#[wasm_bindgen(js_name = harnessDefaultResidentFiles)]
pub fn harness_default_resident_files() -> f64 {
    crate::memory_store::DEFAULT_RESIDENT_FILES as f64
}

#[wasm_bindgen(js_name = harnessMaxInlineAttachments)]
pub fn harness_max_inline_attachments() -> u32 {
    crate::conversation::MAX_INLINE_ATTACHMENTS as u32
}

#[wasm_bindgen(js_name = harnessAttachmentManifestMediaType)]
pub fn harness_attachment_manifest_media_type() -> String {
    crate::conversation::ATTACHMENT_MANIFEST_MEDIA_TYPE.to_owned()
}

/// Rust-owned authenticated browser remote client re-exported by the Harness
/// WASM package. The wrapper keeps protobuf bytes opaque to JavaScript while
/// preserving the generated Rust service types and handshake rules.
#[wasm_bindgen]
pub struct BrowserHarnessRemoteClient {
    inner: acyclic_sdk_remote_web::BrowserHarnessClient,
}

#[wasm_bindgen]
impl BrowserHarnessRemoteClient {
    /// Connect with the Rust-owned browser transport and safe bounds.
    #[wasm_bindgen(js_name = connect)]
    pub async fn connect_js(endpoint: String, bearer_token: String) -> Result<Self, JsValue> {
        let inner =
            acyclic_sdk_remote_web::BrowserHarnessClient::connect_js(endpoint, bearer_token)
                .await?;
        Ok(Self { inner })
    }

    /// Connect with explicit bounds for advanced consumers.
    #[wasm_bindgen(js_name = connectWithLimits)]
    pub async fn connect_with_limits_js(
        endpoint: String,
        bearer_token: String,
        maximum_request_bytes: u64,
        maximum_response_bytes: u64,
    ) -> Result<Self, JsValue> {
        let inner = acyclic_sdk_remote_web::BrowserHarnessClient::connect_with_limits_js(
            endpoint,
            bearer_token,
            maximum_request_bytes,
            maximum_response_bytes,
        )
        .await?;
        Ok(Self { inner })
    }

    /// Return the negotiated Rust protocol identity.
    #[wasm_bindgen(js_name = capabilities)]
    pub fn capabilities_js(&self) -> Result<JsValue, JsValue> {
        self.inner.capabilities_js()
    }

    /// Submit one encoded command envelope.
    #[wasm_bindgen(js_name = submit)]
    pub async fn submit_js(&self, request: Vec<u8>) -> Result<Vec<u8>, JsValue> {
        self.inner.submit_js(request).await
    }

    /// Replay encoded deliveries from the requested cursor.
    #[wasm_bindgen(js_name = replay)]
    pub async fn replay_js(&self, request: Vec<u8>) -> Result<js_sys::Array, JsValue> {
        self.inner.replay_js(request).await
    }

    /// Observe one encoded operation status.
    #[wasm_bindgen(js_name = observe)]
    pub async fn observe_js(&self, request: Vec<u8>) -> Result<Vec<u8>, JsValue> {
        self.inner.observe_js(request).await
    }

    /// Cancel one encoded operation.
    #[wasm_bindgen(js_name = cancel)]
    pub async fn cancel_js(&self, request: Vec<u8>) -> Result<Vec<u8>, JsValue> {
        self.inner.cancel_js(request).await
    }
}

#[derive(Deserialize, Tsify)]
#[tsify(from_wasm_abi)]
#[serde(deny_unknown_fields)]
#[tsify(large_number_types_as_bigints)]
struct WasmLimitsInput {
    file_bytes: u64,
    path_bytes: u64,
    attachments: u64,
    render_bytes: u64,
    model_steps: u64,
    model_events_per_step: u64,
    tool_calls_per_step: u64,
    context_messages: u64,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct WasmPublicModelContextSelection {
    conversation_revision: u64,
    message_ids: Vec<uuid::Uuid>,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct WasmPublicSelectedModelContext {
    selection: WasmPublicModelContextSelection,
    messages: Vec<ModelMessage>,
}

#[derive(Deserialize, Tsify)]
#[tsify(from_wasm_abi, large_number_types_as_bigints)]
#[serde(deny_unknown_fields)]
struct WasmTaskRunLimitsInput {
    #[tsify(type = "bigint | null")]
    concurrency: Option<u64>,
    #[tsify(type = "bigint | null")]
    max_steps: Option<u64>,
    #[tsify(type = "bigint | null")]
    deadline_epoch_ms: Option<u64>,
}

#[derive(Deserialize, Serialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct WasmTaskChild {
    slot: String,
    task_id: TaskId,
}

#[derive(Deserialize, Serialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct WasmTaskChildrenPage {
    revision: u64,
    entries: Vec<WasmTaskChild>,
    next_after: Option<String>,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct WasmTaskChildrenPageInput {
    parent: TaskId,
    expected_revision: Option<u64>,
    after_slot: Option<String>,
    maximum: u32,
    page: WasmTaskChildrenPage,
}

#[derive(Deserialize, Tsify)]
#[tsify(from_wasm_abi)]
#[serde(deny_unknown_fields)]
struct WasmTaskIdentityInput {
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
#[tsify(from_wasm_abi)]
#[serde(deny_unknown_fields)]
struct WasmTaskDependencyDefinition {
    #[tsify(type = "string")]
    name: String,
    #[tsify(type = "string")]
    version: String,
    #[tsify(type = "readonly string[]")]
    requirements: Vec<String>,
}

#[derive(Deserialize, Tsify)]
#[tsify(from_wasm_abi)]
#[serde(deny_unknown_fields)]
struct WasmToolDependencyDefinition {
    #[tsify(type = "string")]
    name: String,
    #[tsify(type = "string")]
    version: String,
}

#[derive(Deserialize, Tsify)]
#[tsify(from_wasm_abi)]
#[serde(deny_unknown_fields)]
struct WasmExtensionDependencyDefinition {
    #[tsify(type = "string")]
    name: String,
    #[tsify(type = "number")]
    version: u32,
}

#[derive(Deserialize, Tsify)]
#[tsify(from_wasm_abi)]
#[serde(deny_unknown_fields)]
struct WasmTaskDependencyComponents {
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
#[tsify(from_wasm_abi)]
#[serde(deny_unknown_fields)]
struct WasmTaskDependencyInput {
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
#[tsify(from_wasm_abi)]
#[serde(deny_unknown_fields)]
struct WasmTaskAdmissionInput {
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
    #[tsify(type = "readonly string[]")]
    grants: Vec<String>,
    limits: WasmLimitsInput,
    run_limits: WasmTaskRunLimitsInput,
    #[tsify(type = "WasmMachineIdentityWire | null")]
    policy: Option<crate::registry::ComponentIdentity>,
    #[tsify(type = "WasmExtensionAdmissionWire | null")]
    extensions: Option<ExtensionAdmission>,
    #[tsify(type = "WasmExecutionPlacementWire | null")]
    execution: Option<crate::runtime::ExecutionPlacement>,
}

/// Tsify declarations for the provider-neutral model values.  These wrappers
/// deliberately mirror the serde model DTOs instead of maintaining a second
/// TypeScript-owned wire union.
#[derive(Clone, Debug, Deserialize, Serialize, Tsify)]
#[serde(rename_all = "snake_case", deny_unknown_fields)]
#[tsify(from_wasm_abi, into_wasm_abi)]
struct WasmModelWire {
    provider: String,
    name: String,
    revision: String,
    #[tsify(type = "WasmModelJsonValue")]
    options: serde_json::Value,
}

#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize, Tsify)]
#[serde(rename_all = "snake_case")]
#[tsify(from_wasm_abi, into_wasm_abi)]
enum WasmModelRole {
    System,
    User,
    Assistant,
    Tool,
}

#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize, Tsify)]
#[serde(rename_all = "snake_case")]
#[tsify(from_wasm_abi, into_wasm_abi)]
enum WasmFileProjectionPolicy {
    Reference,
    BoundedFull,
    Native,
}

#[derive(Clone, Debug, Deserialize, Serialize, Tsify)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
#[tsify(from_wasm_abi, into_wasm_abi)]
enum WasmModelContentPart {
    Text {
        text: String,
    },
    File {
        #[tsify(type = "WasmFileRefWire")]
        file: FileRef,
        policy: WasmFileProjectionPolicy,
    },
    ToolCall {
        #[serde(rename = "call_id", alias = "callId")]
        call_id: String,
        name: String,
        #[tsify(type = "WasmModelJsonValue")]
        arguments: serde_json::Value,
    },
    ToolResult {
        #[serde(rename = "call_id", alias = "callId")]
        call_id: String,
        name: String,
        #[tsify(type = "WasmModelJsonValue")]
        value: serde_json::Value,
    },
}

#[derive(Clone, Debug, Deserialize, Serialize, Tsify)]
#[serde(untagged)]
#[tsify(from_wasm_abi, into_wasm_abi)]
enum WasmModelContent {
    Text(String),
    Part(WasmModelContentPart),
    Parts(Vec<WasmModelContentPart>),
}

#[derive(Clone, Debug, Deserialize, Serialize, Tsify)]
#[serde(rename_all = "snake_case", deny_unknown_fields)]
#[tsify(from_wasm_abi, into_wasm_abi)]
struct WasmModelMessageWire {
    role: WasmModelRole,
    content: WasmModelContent,
}

/// Public model-message input used by the runtime validator.  The content
/// input intentionally reuses the generated camelCase facade type while the
/// Rust parser below still consumes the canonical `ModelMessage` DTO.
#[allow(
    dead_code,
    reason = "the struct exists to emit the generated TypeScript input type"
)]
#[derive(Deserialize, Tsify)]
#[tsify(from_wasm_abi)]
#[serde(rename_all = "snake_case", deny_unknown_fields)]
struct WasmModelMessageInput {
    #[tsify(type = "WasmModelRole")]
    role: WasmModelRole,
    #[tsify(type = "WasmModelContentInput")]
    content: WasmModelContent,
}

#[derive(Clone, Debug, Deserialize, Serialize, Tsify)]
#[serde(rename_all = "snake_case", deny_unknown_fields)]
#[tsify(from_wasm_abi, into_wasm_abi)]
struct WasmModelToolDefinitionWire {
    name: String,
    revision: String,
    description: String,
    #[tsify(type = "WasmModelJsonSchema")]
    input_schema: serde_json::Value,
    #[tsify(type = "WasmModelJsonSchema")]
    output_schema: serde_json::Value,
}

#[derive(Clone, Debug, Deserialize, Serialize, Tsify)]
#[serde(rename_all = "snake_case", deny_unknown_fields)]
#[tsify(from_wasm_abi, into_wasm_abi)]
struct WasmModelRequestWire {
    model: WasmModelWire,
    messages: Vec<WasmModelMessageWire>,
    tools: Vec<WasmModelToolDefinitionWire>,
    max_output_tokens: Option<u32>,
}

#[derive(Clone, Debug, Deserialize, Serialize, Tsify)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
#[tsify(from_wasm_abi, into_wasm_abi)]
enum WasmModelEvent {
    Content {
        delta: String,
    },
    Reasoning {
        delta: String,
    },
    ToolCall {
        #[serde(rename = "call_id", alias = "callId")]
        call_id: String,
        name: String,
        #[tsify(type = "WasmModelJsonValue")]
        arguments: serde_json::Value,
    },
    Completed {
        #[tsify(type = "WasmModelJsonValue")]
        metadata: serde_json::Value,
    },
}

#[derive(Clone, Debug, Deserialize, Serialize, Tsify)]
#[serde(rename_all = "snake_case", deny_unknown_fields)]
#[tsify(from_wasm_abi, into_wasm_abi)]
struct WasmModelAttemptWire {
    #[tsify(type = "string")]
    operation_id: OperationId,
    step: u32,
    #[tsify(type = "readonly number[]")]
    request_digest: [u8; 32],
    observed: Vec<WasmModelEvent>,
}

#[derive(Deserialize, Tsify)]
#[tsify(from_wasm_abi)]
#[serde(deny_unknown_fields)]
struct WasmBatchAdmissionInput {
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
    #[tsify(type = "readonly string[]")]
    grants: Vec<String>,
    limits: WasmLimitsInput,
    run_limits: WasmTaskRunLimitsInput,
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
struct WasmBatchAdmissionRequest {
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
struct WasmGroupPolicy {
    kind: BatchGroupPolicy,
}

fn hex_bytes(bytes: &[u8]) -> String {
    let mut output = String::with_capacity(bytes.len() * 2);
    for byte in bytes {
        output.push_str(&format!("{byte:02x}"));
    }
    output
}

fn wasm_limits(input: WasmLimitsInput) -> Result<Limits, crate::Error> {
    Ok(Limits {
        file_bytes: input.file_bytes,
        path_bytes: usize::try_from(input.path_bytes)
            .map_err(|_| crate::Error::Invalid("path_bytes is not representable".into()))?,
        attachments: usize::try_from(input.attachments)
            .map_err(|_| crate::Error::Invalid("attachments is not representable".into()))?,
        render_bytes: input.render_bytes,
        model_steps: usize::try_from(input.model_steps)
            .map_err(|_| crate::Error::Invalid("model_steps is not representable".into()))?,
        model_events_per_step: usize::try_from(input.model_events_per_step).map_err(|_| {
            crate::Error::Invalid("model_events_per_step is not representable".into())
        })?,
        tool_calls_per_step: usize::try_from(input.tool_calls_per_step).map_err(|_| {
            crate::Error::Invalid("tool_calls_per_step is not representable".into())
        })?,
        context_messages: usize::try_from(input.context_messages)
            .map_err(|_| crate::Error::Invalid("context_messages is not representable".into()))?,
    })
}

fn wasm_run_limits(input: WasmTaskRunLimitsInput) -> Result<TaskRunLimits, crate::Error> {
    Ok(TaskRunLimits {
        concurrency: input
            .concurrency
            .map(usize::try_from)
            .transpose()
            .map_err(|_| crate::Error::Invalid("concurrency is not representable".into()))?,
        max_steps: input
            .max_steps
            .map(usize::try_from)
            .transpose()
            .map_err(|_| crate::Error::Invalid("max_steps is not representable".into()))?,
        deadline_epoch_ms: input.deadline_epoch_ms,
    })
}

// These wire declarations are emitted from the same Rust WASM module as the
// admission constructors.  The input declarations above are Tsify-derived;
// these output declarations keep the canonical serde projection typed without
// reintroducing a second TypeScript-owned contract.
#[wasm_bindgen(typescript_custom_section)]
const ADMISSION_WIRE_TYPES: &'static str = r#"
export interface WasmMachineIdentityWire {
    readonly name: string;
    readonly version: string;
    readonly digest: readonly number[];
}
export type WasmToolJsonValue = null | string | number | boolean | readonly WasmToolJsonValue[] | Readonly<{ [key: string]: WasmToolJsonValue }>;
export type WasmToolJsonSchema = boolean | Readonly<{ [key: string]: WasmToolJsonValue }>;
export type WasmGroupPolicy =
    | Readonly<{ kind: "collect-all" }>
    | Readonly<{ kind: "cancel-on-failure" }>;
export interface WasmNativeLimitsWire {
    readonly file_bytes: bigint;
    readonly path_bytes: bigint;
    readonly attachments: bigint;
    readonly render_bytes: bigint;
    readonly model_steps: bigint;
    readonly model_events_per_step: bigint;
    readonly tool_calls_per_step: bigint;
    readonly context_messages: bigint;
}
export interface WasmTaskRunLimitsWire {
    readonly concurrency: bigint | null;
    readonly max_steps: bigint | null;
    readonly deadline_epoch_ms: bigint | null;
}
export interface WasmProviderRefWire {
    readonly namespace: string;
    readonly family: string;
    readonly version: string;
}
export interface WasmProjectVolumeOwnerWire {
    readonly kind: "project";
    readonly id: string;
}
export interface WasmAgentVolumeOwnerWire {
    readonly kind: "agent";
    readonly id: string;
}
export interface WasmSessionVolumeOwnerWire {
    readonly kind: "session";
    readonly id: string;
}
export type WasmVolumeOwnerWire =
    | WasmProjectVolumeOwnerWire
    | WasmAgentVolumeOwnerWire
    | WasmSessionVolumeOwnerWire;
export interface WasmVolumeRefWire {
    readonly provider: WasmProviderRefWire;
    readonly id: string;
    readonly class: "project" | "agent_private" | "session_shared";
    readonly owner: WasmVolumeOwnerWire;
}
export interface WasmFileDescriptorWire {
    readonly sha256: readonly number[];
    readonly byte_length: number;
    readonly media_type: string;
}
export interface WasmFileRefWire {
    readonly volume: WasmVolumeRefWire;
    readonly path: string;
    readonly version: string;
    readonly descriptor: WasmFileDescriptorWire;
    readonly display_name: string;
}
export interface WasmAuthorityWire {
    readonly kind: "agent";
    readonly id: string;
}
export interface WasmEventReferenceWire {
    readonly authority: WasmAuthorityWire;
    readonly revision: bigint;
}
export interface WasmExtensionDependencyWire {
    readonly name: string;
    readonly version: number;
}
export interface WasmExtensionConfigurationWire {
    readonly extension: WasmExtensionDependencyWire;
    readonly schema_digest: readonly number[];
    readonly content: WasmFileRefWire;
}
export interface WasmResourceRefWire {
    readonly kind: "workspace" | "generation" | "artifact" | "sandbox" | "checkpoint" | "stream" | "context" | "run";
    readonly provider: WasmProviderRefWire;
    readonly key: readonly number[];
    readonly version: string | null;
}
export interface WasmExecutionPlacementWire {
    readonly provider: WasmMachineIdentityWire;
    readonly build: WasmResourceRefWire & Readonly<{ kind: "artifact" }>;
    readonly environment: (WasmResourceRefWire & Readonly<{ kind: "sandbox" }>) | null;
    readonly readiness_revision: readonly number[];
}
export interface WasmExtensionAdmissionWire {
    readonly source: WasmEventReferenceWire;
    readonly selected: readonly WasmExtensionDependencyWire[];
    readonly configurations: readonly WasmExtensionConfigurationWire[];
}
export interface WasmTaskAdmissionWire {
    readonly contract: "harness.task-admission.v2";
    readonly operation_id: string;
    readonly task: WasmMachineIdentityWire;
    readonly machine: WasmMachineIdentityWire;
    readonly input: unknown;
    readonly input_schema: WasmToolJsonSchema;
    readonly output_schema: WasmToolJsonSchema;
    readonly parent: string | null;
    readonly grants: readonly string[];
    readonly limits: WasmNativeLimitsWire;
    readonly run_limits: WasmTaskRunLimitsWire;
    readonly policy: WasmMachineIdentityWire | null;
    readonly extensions: WasmExtensionAdmissionWire | null;
    readonly execution: WasmExecutionPlacementWire | null;
}
export interface WasmDurableBatchWire {
    readonly contract: "harness.batch.v2";
    readonly group_id: string;
    readonly batch_id: string;
    readonly group_policy: "collect-all" | "cancel-on-failure";
    readonly task: WasmMachineIdentityWire;
    readonly machine: WasmMachineIdentityWire;
    readonly inputs: readonly unknown[];
    readonly input_schema: WasmToolJsonSchema;
    readonly output_schema: WasmToolJsonSchema;
    readonly parent: string | null;
    readonly grants: readonly string[];
    readonly limits: WasmNativeLimitsWire;
    readonly run_limits: WasmTaskRunLimitsWire;
    readonly extensions: WasmExtensionAdmissionWire | null;
    readonly policy: WasmMachineIdentityWire | null;
    readonly execution: WasmExecutionPlacementWire | null;
}
export interface WasmBatchAdmissionRequest {
    readonly contract: "harness.batch.v2";
    readonly groupId: string;
    readonly batchId: string;
    readonly taskName: string;
    readonly revision: string;
    readonly implementationDigest: string;
    readonly parentTaskId: string | null;
    readonly policy: WasmGroupPolicy;
    readonly members: readonly WasmTaskAdmissionWire[];
    readonly canonical: WasmDurableBatchWire;
    readonly inputDigest: readonly number[];
}
export interface WasmTaskAdmissionIdentities {
    readonly task: WasmMachineIdentityWire;
    readonly machine: WasmMachineIdentityWire;
}
export interface WasmTaskChildrenPage {
    readonly revision: bigint;
    readonly entries: readonly Readonly<{ readonly slot: string; readonly taskId: string }>[];
    readonly nextAfter: string | null;
}
export interface WasmTaskChildrenPageInput {
    readonly parent: string;
    readonly expectedRevision: bigint | null;
    readonly afterSlot: string | null;
    readonly maximum: number;
    readonly page: WasmTaskChildrenPage;
}
"#;

#[wasm_bindgen(typescript_custom_section)]
const TURN_PREPARATION_TYPES: &'static str = r#"
export type WasmTurnDisposition = "dispatch" | "reconcile" | "indeterminate" | "completed";
export interface WasmTurnPreparation {
    readonly user_id: string;
    readonly append_user: boolean;
    readonly selection: Readonly<{
        readonly conversation_revision: bigint;
        readonly message_ids: readonly string[];
    }>;
    readonly selection_is_new: boolean;
    readonly disposition: WasmTurnDisposition;
}
"#;

// JSON aliases and validator limits are the only model declarations that are
// not emitted directly by Tsify. The DTOs above own the generated Rust wire
// types; these aliases describe the public camelCase input view.
#[wasm_bindgen(typescript_custom_section)]
const MODEL_TYPES: &'static str = r#"
export type WasmModelJsonValue =
    | null
    | string
    | number
    | boolean
    | bigint
    | readonly WasmModelJsonValue[]
    | Readonly<{ readonly [key: string]: WasmModelJsonValue }>;
export type WasmModelJsonSchema =
    | boolean
    | Readonly<{ readonly [key: string]: WasmModelJsonValue }>;
export interface WasmModelLimitsInput {
    readonly file_bytes: number | bigint;
    readonly path_bytes: number | bigint;
    readonly attachments: number | bigint;
    readonly render_bytes: number | bigint;
    readonly model_steps: number | bigint;
    readonly model_events_per_step: number | bigint;
    readonly tool_calls_per_step: number | bigint;
    readonly context_messages: number | bigint;
}
type WasmModelCamelContentPart<Part extends WasmModelContentPart> =
    Part extends { readonly kind: "tool_call"; readonly call_id: string }
        ? Omit<Part, "call_id" | "arguments"> & Readonly<{ callId: string; arguments: unknown }>
        : Part extends { readonly kind: "tool_result"; readonly call_id: string }
            ? Omit<Part, "call_id" | "value"> & Readonly<{ callId: string; value: unknown }>
            : Part;
export type WasmModelContentPartInput = WasmModelCamelContentPart<WasmModelContentPart>;
export type WasmModelContentInput = string | WasmModelContentPartInput | readonly WasmModelContentPartInput[];
type WasmModelCamelEvent<Event extends WasmModelEvent> =
    Event extends { readonly kind: "tool_call"; readonly call_id: string }
        ? Omit<Event, "call_id" | "arguments"> & Readonly<{ callId: string; arguments: unknown }>
        : Event extends { readonly kind: "completed" }
            ? Omit<Event, "metadata"> & Readonly<{ metadata: unknown }>
        : Event;
export type WasmModelEventInput = WasmModelCamelEvent<WasmModelEvent>;
"#;

/// Exact bytes captured by the owner-facing TypeScript adapter before Rust
/// performs the canonical projection.  The map key is the canonical JSON
/// spelling of a `FileRef`; bytes are never resolved by path or display name.
struct WasmProjectionResolver {
    files: std::collections::HashMap<String, Vec<u8>>,
}

impl WasmProjectionResolver {
    fn from_js(value: JsValue) -> Result<Self, JsValue> {
        let map = value
            .dyn_into::<js_sys::Map>()
            .map_err(|_| JsValue::from_str("projection files must be a Map"))?;
        let mut files = std::collections::HashMap::new();
        let mut failure = None;
        map.for_each(&mut |bytes, key| {
            if failure.is_some() {
                return;
            }
            let Some(key) = key.as_string() else {
                failure = Some(JsValue::from_str(
                    "projection file map keys must be canonical JSON strings",
                ));
                return;
            };
            if !js_sys::Uint8Array::is_type_of(&bytes) {
                failure = Some(JsValue::from_str(
                    "projection file map values must be Uint8Array",
                ));
                return;
            }
            files.insert(key, js_sys::Uint8Array::new(&bytes).to_vec());
        });
        if let Some(error) = failure {
            return Err(error);
        }
        Ok(Self { files })
    }

    fn key(file: &FileRef) -> Result<String, crate::Error> {
        String::from_utf8(crate::contract::canonical_json_bytes(file)?)
            .map_err(|error| crate::Error::Invalid(format!("file reference is not UTF-8: {error}")))
    }

    fn bytes(&self, file: &FileRef) -> Result<Vec<u8>, crate::Error> {
        let key = Self::key(file)?;
        self.files.get(&key).cloned().ok_or_else(|| {
            crate::Error::Unsupported("projection file bytes were not captured".into())
        })
    }
}

impl AttachmentListResolver for WasmProjectionResolver {
    fn resolve<'a>(
        &'a self,
        manifest: &'a FileRef,
        item_count: u32,
    ) -> crate::BoxFuture<'a, crate::Result<Vec<Attachment>>> {
        Box::pin(async move {
            let bytes = self.bytes(manifest)?;
            decode_attachment_manifest(manifest, &bytes, item_count)
        })
    }

    fn read<'a>(&'a self, file: &'a FileRef) -> crate::BoxFuture<'a, crate::Result<Vec<u8>>> {
        Box::pin(async move { self.bytes(file) })
    }
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
/// The input is a contract projection only; no executable task handlers cross
/// the WASM boundary and Rust owns graph traversal, revision matching, grants,
/// and extension requirement admission.
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
    )
    .map_err(js_error)
}

/// Builds and validates the complete owner-retained task admission envelope.
/// TypeScript supplies public values, while Rust owns identity derivation,
/// schema/value validation, limits, authority, and execution binding.
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
        wasm_limits(input.limits).map_err(js_error)?,
        wasm_run_limits(input.run_limits).map_err(js_error)?,
        input.policy,
        input.extensions,
        input.execution,
    )
    .map_err(js_error)?;
    to_js_admitted(&record.canonical_value())
}

fn durable_batch_request_from_input(
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
        wasm_limits(input.limits).map_err(js_error)?,
        wasm_run_limits(input.run_limits).map_err(js_error)?,
        input.extensions,
        input.policy,
        input.execution,
    )
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

/// Builds the complete SDK-facing durable batch request in one Rust-owned
/// projection. Member envelopes, policy, implementation digest, canonical
/// manifest, and request digest all derive from the same validated request.
#[wasm_bindgen(
    js_name = admitBatchRequest,
    unchecked_return_type = "WasmBatchAdmissionRequest"
)]
pub fn admit_batch_request_wasm(
    #[wasm_bindgen(unchecked_param_type = "WasmBatchAdmissionInput")] value: JsValue,
) -> Result<JsValue, JsValue> {
    let input: WasmBatchAdmissionInput = from_js(value)?;
    let implementation_digest = hex_bytes(&input.machine_digest);
    let request = durable_batch_request_from_input(input)?;
    let canonical = request.canonical_value();
    let members = (0..request.inputs.len())
        .map(|index| {
            request
                .member_admission(index)
                .map(|member| member.canonical_value())
        })
        .collect::<Result<Vec<_>, _>>()
        .map_err(js_error)?;
    let input_digest = crate::contract::canonical_json_digest(&canonical)
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

/// Admit one retryable browser command through the same Rust policy used by
/// native Harness hosts. The JavaScript facade keeps persistence and event
/// callbacks, while command shape, identity, and credential/body exclusions
/// remain owned by this boundary.
#[wasm_bindgen(js_name = validateOfflineCommand)]
pub fn validate_offline_command(value: JsValue) -> Result<JsValue, JsValue> {
    let value = js_json_value(&value)?;
    let object = value
        .as_object()
        .ok_or_else(|| JsValue::from_str("offline outbox command must be an object"))?;
    require_exact_keys(
        object,
        &["operationId", "authority", "kind", "payload", "offlineSafe"],
    )?;
    if object.get("offlineSafe") != Some(&serde_json::Value::Bool(true)) {
        return Err(JsValue::from_str(
            "command is not safe for the offline outbox",
        ));
    }
    let kind = object
        .get("kind")
        .and_then(serde_json::Value::as_str)
        .ok_or_else(|| JsValue::from_str("offline outbox command kind is invalid"))?;
    if kind == "interaction.resolve.approval" {
        return Err(JsValue::from_str(
            "command is not safe for the offline outbox",
        ));
    }
    let operation_id = object
        .get("operationId")
        .and_then(serde_json::Value::as_str)
        .ok_or_else(|| JsValue::from_str("offline outbox operation identity is invalid"))?;
    validate_identity("operation", operation_id)?;
    let authority = object
        .get("authority")
        .and_then(serde_json::Value::as_object)
        .ok_or_else(|| JsValue::from_str("offline outbox authority is invalid"))?;
    require_exact_keys(authority, &["kind", "id"])?;
    let authority_kind = authority
        .get("kind")
        .and_then(serde_json::Value::as_str)
        .ok_or_else(|| JsValue::from_str("offline outbox authority kind is invalid"))?;
    let authority_id = authority
        .get("id")
        .and_then(serde_json::Value::as_str)
        .ok_or_else(|| JsValue::from_str("offline outbox authority identity is invalid"))?;
    validate_safe_authority_id(authority_kind, authority_id)?;
    let payload = object
        .get("payload")
        .and_then(serde_json::Value::as_object)
        .ok_or_else(|| JsValue::from_str("offline outbox payload must be a ref-only record"))?;
    for key in payload.keys() {
        if !matches!(
            key.as_str(),
            "content" | "attachments" | "artifacts" | "references" | "metadata"
        ) {
            return Err(JsValue::from_str(
                "offline outbox payload contains an unsupported field",
            ));
        }
    }
    if let Some(content) = payload.get("content") {
        serde_json::from_value::<FileRef>(content.clone())
            .map_err(|_| JsValue::from_str("offline outbox content reference is invalid"))?;
    }
    if let Some(attachments) = payload.get("attachments") {
        serde_json::from_value::<ReferencedAttachments>(attachments.clone())
            .map_err(|_| JsValue::from_str("offline outbox attachments are invalid"))?;
    }
    for name in ["artifacts", "references"] {
        if let Some(references) = payload.get(name) {
            let references = references
                .as_array()
                .ok_or_else(|| JsValue::from_str("offline outbox references must be a list"))?;
            for reference in references {
                serde_json::from_value::<FileRef>(reference.clone())
                    .map_err(|_| JsValue::from_str("offline outbox file reference is invalid"))?;
            }
        }
    }
    if let Some(metadata) = payload.get("metadata") {
        let metadata = metadata
            .as_object()
            .ok_or_else(|| JsValue::from_str("offline outbox metadata must be a record"))?;
        if metadata
            .values()
            .any(|value| !value.is_null() && !value.is_boolean() && !value.is_number())
        {
            return Err(JsValue::from_str(
                "offline outbox metadata contains an unsafe value",
            ));
        }
    }
    reject_forbidden_keys(&value)?;
    to_js(&value)
}

/// Rust owns the retry schedule. Hosts only wait using the returned duration
/// and carry the opaque attempt token into the next call.
#[wasm_bindgen(js_name = harnessReplayBackoff)]
pub fn harness_replay_backoff(attempt: u32) -> Result<JsValue, JsValue> {
    let exponent = attempt.min(7);
    let delay_ms = 50_u32.saturating_mul(1_u32 << exponent).min(5_000);
    to_js(&serde_json::json!({
        "delayMs": delay_ms,
        "nextAttempt": attempt.saturating_add(1),
    }))
}

/// Validate one replay delivery and return the durable state transitions. The
/// host performs listener dispatch and storage I/O, while Rust owns generation,
/// contiguity, operation identity, and the per-event acknowledgement cursors.
#[wasm_bindgen(js_name = reconcileReplayDelivery)]
pub fn reconcile_replay_delivery(previous: JsValue, delivery: JsValue) -> Result<JsValue, JsValue> {
    let previous = js_json_value(&previous)?;
    let delivery = js_json_value(&delivery)?;
    let result = reconcile_replay_delivery_value(&previous, &delivery)?;
    to_js(&result)
}

/// Backward-compatible cursor-only projection for generated consumers that do
/// not need acknowledgement details.
#[wasm_bindgen(js_name = validateReplayDelivery)]
pub fn validate_replay_delivery(previous: JsValue, delivery: JsValue) -> Result<JsValue, JsValue> {
    let previous = js_json_value(&previous)?;
    let delivery = js_json_value(&delivery)?;
    let result = reconcile_replay_delivery_value(&previous, &delivery)?;
    to_js(
        result
            .get("cursor")
            .ok_or_else(|| JsValue::from_str("replay cursor is missing"))?,
    )
}

fn reconcile_replay_delivery_value(
    previous: &serde_json::Value,
    delivery: &serde_json::Value,
) -> Result<serde_json::Value, JsValue> {
    let delivery = delivery
        .as_object()
        .ok_or_else(|| JsValue::from_str("replay delivery must be an object"))?;
    let authority = delivery
        .get("authority")
        .and_then(serde_json::Value::as_object)
        .ok_or_else(|| JsValue::from_str("replay delivery authority is invalid"))?;
    require_exact_keys(authority, &["kind", "id"])?;
    let authority_kind = authority
        .get("kind")
        .and_then(serde_json::Value::as_str)
        .ok_or_else(|| JsValue::from_str("replay delivery authority kind is invalid"))?;
    let authority_id = authority
        .get("id")
        .and_then(serde_json::Value::as_str)
        .ok_or_else(|| JsValue::from_str("replay delivery authority identity is invalid"))?;
    validate_safe_authority_id(authority_kind, authority_id)?;
    let generation = delivery
        .get("generation")
        .and_then(serde_json::Value::as_str)
        .filter(|generation| !generation.is_empty())
        .ok_or_else(|| JsValue::from_str("replay generation is invalid"))?;
    let from_revision = json_u64(delivery.get("fromRevision"), "replay start revision")?;
    let through_revision = json_u64(delivery.get("throughRevision"), "replay end revision")?;
    if through_revision < from_revision {
        return Err(JsValue::from_str("replay delivery coverage is invalid"));
    }
    let expected_revision = match previous {
        serde_json::Value::Null => 0,
        serde_json::Value::Object(previous) => {
            let previous_generation = previous
                .get("generation")
                .and_then(serde_json::Value::as_str)
                .filter(|generation| !generation.is_empty())
                .ok_or_else(|| JsValue::from_str("replay cursor generation is invalid"))?;
            if previous_generation != generation {
                return Err(JsValue::from_str("replay generation changed"));
            }
            json_u64(previous.get("revision"), "replay cursor revision")?
        }
        _ => return Err(JsValue::from_str("replay cursor is invalid")),
    };
    if from_revision != expected_revision {
        return Err(JsValue::from_str("replay delivery is not contiguous"));
    }
    let events = delivery
        .get("events")
        .and_then(serde_json::Value::as_array)
        .ok_or_else(|| JsValue::from_str("replay delivery events are invalid"))?;
    let mut revision = expected_revision;
    let mut acknowledgements = Vec::with_capacity(events.len());
    let mut operation_ids = BTreeSet::new();
    for event in events {
        let event = event
            .as_object()
            .ok_or_else(|| JsValue::from_str("replay event is invalid"))?;
        let event_authority = event
            .get("authority")
            .ok_or_else(|| JsValue::from_str("replay event authority is missing"))?;
        if event_authority
            != delivery
                .get("authority")
                .unwrap_or(&serde_json::Value::Null)
        {
            return Err(JsValue::from_str("replay event authority mismatch"));
        }
        revision = revision
            .checked_add(1)
            .ok_or_else(|| JsValue::from_str("replay revision exceeds the supported range"))?;
        if json_u64(event.get("revision"), "replay event revision")? != revision {
            return Err(JsValue::from_str("replay event revision mismatch"));
        }
        let operation_id = event
            .get("operationId")
            .and_then(serde_json::Value::as_str)
            .ok_or_else(|| JsValue::from_str("replay event operation identity is invalid"))?;
        let normalized = validate_identity("operation", operation_id)?;
        if !operation_ids.insert(normalized.clone()) {
            return Err(JsValue::from_str(
                "replay event operation identity is duplicated",
            ));
        }
        acknowledgements.push(serde_json::json!({
            "operationId": normalized,
            "cursor": { "generation": generation, "revision": revision },
        }));
    }
    if revision != through_revision {
        return Err(JsValue::from_str("replay delivery coverage mismatch"));
    }
    Ok(serde_json::json!({
        "cursor": { "generation": generation, "revision": revision },
        "acknowledgements": acknowledgements,
    }))
}

fn json_u64(value: Option<&serde_json::Value>, field: &str) -> Result<u64, JsValue> {
    value
        .and_then(serde_json::Value::as_u64)
        .ok_or_else(|| JsValue::from_str(&format!("{field} must be a non-negative integer")))
}

fn validate_safe_authority_id(kind: &str, value: &str) -> Result<(), JsValue> {
    if kind.is_empty()
        || value.is_empty()
        || matches!(value, "." | "..")
        || value
            .chars()
            .any(|character| character == '/' || character == '\\' || character.is_control())
    {
        return Err(JsValue::from_str(
            "authority identity is not a safe path segment",
        ));
    }
    Ok(())
}

fn require_exact_keys(
    object: &serde_json::Map<String, serde_json::Value>,
    expected: &[&str],
) -> Result<(), JsValue> {
    if object.len() != expected.len() || expected.iter().any(|key| !object.contains_key(*key)) {
        return Err(JsValue::from_str(
            "offline outbox command contains an unsupported field",
        ));
    }
    Ok(())
}

fn reject_forbidden_keys(value: &serde_json::Value) -> Result<(), JsValue> {
    match value {
        serde_json::Value::Object(fields) => {
            for (key, child) in fields {
                let normalized = key.to_ascii_lowercase();
                if normalized.contains("token")
                    || normalized.contains("authorization")
                    || normalized.contains("credential")
                    || normalized.contains("secret")
                    || normalized.contains("password")
                    || normalized.contains("api_key")
                    || normalized.contains("api-key")
                    || matches!(
                        normalized.as_str(),
                        "scope" | "proof" | "body" | "text" | "bytes" | "base64" | "data"
                    )
                {
                    return Err(JsValue::from_str(
                        "offline outbox cannot persist inline bytes or credentials",
                    ));
                }
                reject_forbidden_keys(child)?;
            }
        }
        serde_json::Value::Array(items) => {
            for item in items {
                reject_forbidden_keys(item)?;
            }
        }
        _ => {}
    }
    Ok(())
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
    if limit == 0 || limit as usize > crate::conversation::MAX_CONVERSATION_PAGE_MESSAGES {
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
        "execution_placement" => {
            let value: crate::runtime::ExecutionPlacement = from_js(value)?;
            value.validate().map_err(js_error)?;
            to_js_admitted(&value)
        }
        "task_admission" => {
            let value: serde_json::Value = from_js(value)?;
            let request = crate::runtime::TaskAdmissionRecord::from_canonical_value(value)
                .map_err(js_error)?;
            to_js_admitted(&request.canonical_value())
        }
        "machine_identity" => {
            let value: crate::workflow::MachineIdentity = from_js(value)?;
            value.validate().map_err(js_error)?;
            to_js_admitted(&value)
        }
        "workflow_admission" => {
            let value: crate::workflow::WorkflowAdmission = from_js(value)?;
            value.validate().map_err(js_error)?;
            to_js_admitted(&value)
        }
        "durable_batch_request" => {
            let value: serde_json::Value = from_js(value)?;
            let request = DurableBatchRequest::from_canonical_value(value).map_err(js_error)?;
            to_js_admitted(&request.canonical_value())
        }
        "resource_ref" => {
            let value: ResourceRef = from_js(value)?;
            value.validate().map_err(js_error)?;
            to_js_admitted(&value)
        }
        "private_directory_page" => {
            let value: crate::conversation::PrivateDirectoryPage = from_js(value)?;
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

/// Runs the canonical Rust conversation projection over bytes captured by the
/// owner.  TypeScript supplies a map rather than a callback so authorization
/// and async reads finish before this deterministic core is entered.
#[wasm_bindgen(js_name = selectModelContext)]
pub async fn select_model_context_wasm(
    conversation: JsValue,
    selection: JsValue,
    files: JsValue,
    maximum_messages: u32,
    maximum_attachments: u32,
    maximum_render_bytes: f64,
    maximum_projected_attachments: u32,
) -> Result<JsValue, JsValue> {
    let conversation: ConversationState = from_js(conversation)?;
    let selection: ModelContextSelection = from_js(selection)?;
    let resolver = WasmProjectionResolver::from_js(files)?;
    if !maximum_render_bytes.is_finite()
        || maximum_render_bytes < 0.0
        || maximum_render_bytes.fract() != 0.0
        || maximum_render_bytes > 9_007_199_254_740_991.0
    {
        return Err(JsValue::from_str(
            "maximum render bytes must be a safe non-negative integer",
        ));
    }
    let maximum_render_bytes = maximum_render_bytes
        .to_string()
        .parse::<u64>()
        .map_err(|_| JsValue::from_str("maximum render bytes are out of range"))?;
    let selected = select_model_context_at_revision(
        &conversation,
        selection.clone(),
        &resolver,
        maximum_messages as usize,
        maximum_attachments as usize,
        maximum_render_bytes,
        maximum_projected_attachments as usize,
    )
    .await
    .map_err(js_error)?;
    to_js(&selected)
}

/// Plans one deterministic conversation turn before any model or content
/// callback runs.  The reducer state and payload checks are shared with the
/// native filesystem memory host; JavaScript retains ownership of asynchronous
/// reads and model dispatch after this plan is committed.
#[wasm_bindgen(
    js_name = prepareConversationTurn,
    unchecked_return_type = "WasmTurnPreparation"
)]
#[expect(
    clippy::too_many_arguments,
    reason = "keep the generated WASM signature aligned with the native turn contract"
)]
pub fn prepare_conversation_turn_wasm(
    conversation: JsValue,
    operation_id: String,
    content: JsValue,
    attachments: JsValue,
    limits: JsValue,
    existing_selection: JsValue,
    has_completed_output: bool,
    can_reconcile: bool,
) -> Result<JsValue, JsValue> {
    let conversation: ConversationState = from_js(conversation)?;
    let operation_id = OperationId::parse(&operation_id).map_err(js_error)?;
    let content: FileRef = from_js(content)?;
    let attachments: ReferencedAttachments = from_js(attachments)?;
    let limits: Limits = from_js(limits)?;
    let existing_selection = if existing_selection.is_null() || existing_selection.is_undefined() {
        None
    } else {
        Some(from_js::<ModelContextSelection>(existing_selection)?)
    };
    let preparation = prepare_turn(
        &conversation,
        operation_id,
        content,
        attachments,
        limits,
        existing_selection,
        has_completed_output,
        can_reconcile,
    )
    .map_err(js_error)?;
    to_js(&preparation)
}

/// Validates selection order and tool linkage before the host resolves any
/// owner-mediated file bytes.
#[wasm_bindgen(js_name = validateModelContextSelection)]
pub fn validate_model_context_selection_wasm(
    conversation: JsValue,
    selection: JsValue,
) -> Result<(), JsValue> {
    let conversation: ConversationState = from_js(conversation)?;
    let selection: ModelContextSelection = from_js(selection)?;
    validate_model_context_selection_at_revision(
        &conversation,
        &selection,
        selection.conversation_revision,
    )
    .map_err(js_error)
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

    /// Authenticates lazy directory and named-path access against the
    /// owner's signed, segment-bounded private-volume read grant.
    #[wasm_bindgen(js_name = verifyPrivateDirectoryRead)]
    pub fn verify_private_directory_read(
        &self,
        scope: JsValue,
        volume: JsValue,
        granted_prefix: String,
        path: String,
    ) -> Result<(), JsValue> {
        let scope: Scope = from_js(scope)?;
        let volume: VolumeRef = from_js(volume)?;
        let grant = ContentGrant::verify_directory_read(
            &self.issuer.verifier(),
            &scope,
            &volume,
            &granted_prefix,
        )
        .map_err(js_error)?;
        grant
            .require_directory_path(&volume, &path)
            .map_err(js_error)
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

    /// Applies a deterministic command. Fresh extension migrations require the
    /// native content-admission host and cannot be synthesized by this reducer.
    pub fn apply(&mut self, command: JsValue) -> Result<JsValue, JsValue> {
        let command: Command = from_js(command)?;
        let result: ApplyResult = self.reducer.apply(command).map_err(js_error)?;
        to_js(&result)
    }

    /// Applies one canonical Protobuf command, excluding fresh migrations
    /// that require executable content admission, and returns a response.
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
        .serialize_maps_as_objects(true)
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

/// Decodes one canonical event payload using the native event union.
#[wasm_bindgen(js_name = decodeEventPayload)]
pub fn decode_event_payload(
    event_type: String,
    canonical_payload_json: Vec<u8>,
) -> Result<JsValue, JsValue> {
    let payload = decode_payload_wire(&event_type, &canonical_payload_json).map_err(js_error)?;
    to_js(&payload)
}

/// Decodes and validates a canonical Protobuf apply response using Rust-owned
/// event, authority, scope, digest, and payload rules.
#[wasm_bindgen(js_name = decodeApplyResponse)]
pub fn decode_apply_response(bytes: Vec<u8>) -> Result<JsValue, JsValue> {
    let response = crate::wire::ApplyResponse::decode(bytes.as_slice())
        .map_err(|error| JsValue::from_str(&error.to_string()))?;
    let envelope = response
        .event
        .ok_or_else(|| JsValue::from_str("apply response event is missing"))?;
    let (_, event) =
        crate::wire_codec::decode_event(&envelope.encode_to_vec()).map_err(js_error)?;
    let result = match crate::wire::ApplyState::try_from(response.state)
        .map_err(|_| JsValue::from_str("apply response state is invalid"))?
    {
        crate::wire::ApplyState::Applied => ApplyResult::Applied { event },
        crate::wire::ApplyState::Replayed => ApplyResult::Replayed { event },
        crate::wire::ApplyState::Unspecified => {
            return Err(JsValue::from_str("apply response state is unspecified"));
        }
    };
    to_js(&result)
}

/// Decodes a generated aggregate kind using the native enum mapping.
#[wasm_bindgen(js_name = decodeAggregateKind)]
pub fn decode_aggregate_kind_wasm(value: i32) -> Result<JsValue, JsValue> {
    let kind = match crate::wire::AggregateKind::try_from(value)
        .map_err(|_| JsValue::from_str("aggregate kind is invalid"))?
    {
        crate::wire::AggregateKind::Agent => AggregateKind::Agent,
        crate::wire::AggregateKind::Conversation => AggregateKind::Conversation,
        crate::wire::AggregateKind::Session => AggregateKind::Session,
        crate::wire::AggregateKind::Turn => AggregateKind::Turn,
        crate::wire::AggregateKind::Task => AggregateKind::Task,
        crate::wire::AggregateKind::Unspecified => {
            return Err(JsValue::from_str("aggregate kind is unspecified"));
        }
    };
    to_js(&kind)
}

/// JavaScript-facing tool definition shape. The public TypeScript facade uses
/// camelCase names while the native definition remains `snake_case`.
#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct WasmToolDefinitionInput {
    name: String,
    revision: String,
    description: String,
    input_schema: serde_json::Value,
    output_schema: serde_json::Value,
}

impl From<WasmToolDefinitionInput> for ToolDefinition {
    fn from(value: WasmToolDefinitionInput) -> Self {
        Self {
            name: value.name,
            revision: value.revision,
            description: value.description,
            input_schema: value.input_schema,
            output_schema: value.output_schema,
        }
    }
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct WasmToolInvocationInput {
    call_id: String,
    name: String,
    arguments: serde_json::Value,
}

#[derive(Deserialize)]
struct WasmToolResultInput {
    value: serde_json::Value,
}

/// Validates a model-visible tool definition using the native contract.
#[wasm_bindgen(js_name = validateToolDefinition)]
pub fn validate_tool_definition(definition: JsValue) -> Result<(), JsValue> {
    let definition: ToolDefinition = from_js::<WasmToolDefinitionInput>(definition)?.into();
    definition.validate().map_err(js_error)
}

/// Validates one tool invocation against its registered definition.
#[wasm_bindgen(js_name = validateToolInvocation)]
pub fn validate_tool_invocation(definition: JsValue, invocation: JsValue) -> Result<(), JsValue> {
    let definition: ToolDefinition = from_js::<WasmToolDefinitionInput>(definition)?.into();
    definition.validate().map_err(js_error)?;
    let invocation: WasmToolInvocationInput = from_js(invocation)?;
    if invocation.call_id.is_empty() || invocation.name != definition.name {
        return Err(JsValue::from_str(
            "tool invocation identity does not match definition",
        ));
    }
    validate_value(
        &definition.input_schema,
        &invocation.arguments,
        "tool input",
    )
    .map_err(js_error)
}

/// Validates one successful tool result against its registered definition.
#[wasm_bindgen(js_name = validateToolResult)]
pub fn validate_tool_result(definition: JsValue, result: JsValue) -> Result<(), JsValue> {
    let definition: ToolDefinition = from_js::<WasmToolDefinitionInput>(definition)?.into();
    definition.validate().map_err(js_error)?;
    let result: WasmToolResultInput = from_js(result)?;
    validate_value(&definition.output_schema, &result.value, "tool output").map_err(js_error)
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
    content.validate_limits(limits).map_err(js_error)
}

/// Validates a complete provider-neutral model message list with the native
/// role, message-count, and content bounds.  Context builders and the stock
/// TypeScript loop therefore share the same closed role set and limits as
/// native durable execution.
#[wasm_bindgen(
    js_name = validateModelMessages,
)]
pub fn validate_model_messages(
    #[wasm_bindgen(unchecked_param_type = "readonly WasmModelMessageInput[]")] messages: JsValue,
    #[wasm_bindgen(unchecked_param_type = "WasmModelLimitsInput")] limits: JsValue,
) -> Result<(), JsValue> {
    let messages: Vec<ModelMessage> = from_js(messages)?;
    let limits: Limits = from_js(limits)?;
    limits.validate().map_err(js_error)?;
    if messages.is_empty() || messages.len() > limits.context_messages {
        return Err(JsValue::from_str("model context count is invalid"));
    }
    for message in messages {
        message.content.validate_limits(limits).map_err(js_error)?;
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
    content.validate_user_input().map_err(js_error)
}

/// Admits an already projected, provider-proven context with native model bounds.
#[wasm_bindgen(js_name = validateSelectedModelContext)]
pub fn validate_selected_model_context(selected: JsValue, limits: JsValue) -> Result<(), JsValue> {
    let selected: WasmPublicSelectedModelContext = from_js(selected)?;
    let limits: Limits = from_js(limits)?;
    SelectedModelContext {
        selection: ModelContextSelection {
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
#[tsify(from_wasm_abi, into_wasm_abi)]
struct WasmModelEventAdmissionState {
    #[tsify(type = "number")]
    count: usize,
    calls: Vec<String>,
    completed: bool,
    #[tsify(type = "number")]
    text_bytes: u64,
}

#[derive(Clone, Debug, Deserialize, Serialize, Tsify)]
#[tsify(into_wasm_abi)]
struct WasmModelEventAdmission {
    event: WasmModelEvent,
    state: WasmModelEventAdmissionState,
}

/// Admits one provider model event with the native stream rules and returns
/// the detached state needed for the next event. Text accounting is cumulative
/// across model steps while event and tool-call bounds reset at each step.
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
    limits.validate().map_err(js_error)?;
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
    let admission_state = ModelEventAdmissionState {
        count: state.count,
        calls: state.calls,
        completed: state.completed,
    };
    let mut admission =
        ModelEventAdmission::from_state(admission_state, limits).map_err(js_error)?;
    admission.observe(&event, limits).map_err(js_error)?;
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
        count: admission.state().count,
        calls: admission.state().calls,
        completed: admission.state().completed,
        text_bytes,
    };
    let output = WasmModelEventAdmission {
        event: match event {
            ModelEvent::Content { delta } => WasmModelEvent::Content { delta },
            ModelEvent::Reasoning { delta } => WasmModelEvent::Reasoning { delta },
            ModelEvent::ToolCall {
                call_id,
                name,
                arguments,
            } => WasmModelEvent::ToolCall {
                call_id,
                name,
                arguments,
            },
            ModelEvent::Completed { metadata } => WasmModelEvent::Completed { metadata },
        },
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

/// Validates a protobuf handshake; returns encoded `Error` bytes, or empty on success.
#[wasm_bindgen(js_name = validateWireHandshake)]
pub fn validate_wire_handshake(request: Vec<u8>, response: Vec<u8>) -> Vec<u8> {
    validate_wire(|| crate::wire_validation::validate_wire_handshake(&request, &response))
}

/// Validates one complete protobuf command before it crosses a wire adapter.
#[wasm_bindgen(js_name = validateWireCommand)]
pub fn validate_wire_command(command: Vec<u8>) -> Vec<u8> {
    validate_wire(|| crate::wire_codec::decode_command(&command).map(|_| ()))
}

/// Validates only the protocol identity of a command envelope.
#[wasm_bindgen(js_name = validateWireCommandProtocol)]
pub fn validate_wire_command_protocol(command: Vec<u8>) -> Vec<u8> {
    validate_wire(|| {
        let command = crate::wire::CommandEnvelope::decode(command.as_slice())
            .map_err(|error| crate::Error::Invalid(format!("invalid command envelope: {error}")))?;
        crate::wire_api::validate_command_protocol(&command)
    })
}

/// Validates a protobuf replay request against the compiled protocol identity.
#[wasm_bindgen(js_name = validateWireResume)]
pub fn validate_wire_resume(request: Vec<u8>) -> Vec<u8> {
    validate_wire(|| {
        let request = crate::wire::ResumeRequest::decode(request.as_slice())
            .map_err(|error| crate::Error::Invalid(format!("invalid resume request: {error}")))?;
        crate::wire_api::validate_resume_protocol(&request)
    })
}

/// Validates a protobuf observe request using the canonical Rust scope rules.
#[wasm_bindgen(js_name = validateWireObserve)]
pub fn validate_wire_observe(request: Vec<u8>) -> Vec<u8> {
    validate_wire(|| {
        let request = crate::wire::ObserveRequest::decode(request.as_slice())
            .map_err(|error| crate::Error::Invalid(format!("invalid observe request: {error}")))?;
        crate::wire_api::validate_observe_request(&request).map(|_| ())
    })
}

/// Validates a protobuf cancel request using the canonical Rust scope rules.
#[wasm_bindgen(js_name = validateWireCancel)]
pub fn validate_wire_cancel(request: Vec<u8>) -> Vec<u8> {
    validate_wire(|| {
        let request = crate::wire::CancelRequest::decode(request.as_slice())
            .map_err(|error| crate::Error::Invalid(format!("invalid cancel request: {error}")))?;
        crate::wire_api::validate_cancel_request(&request).map(|_| ())
    })
}

/// Validates a protobuf admission identity; returns encoded `Error` bytes, or empty on success.
#[wasm_bindgen(js_name = validateWireAdmission)]
pub fn validate_wire_admission(command: Vec<u8>, admission: Vec<u8>) -> Vec<u8> {
    validate_wire(|| crate::wire_validation::validate_wire_admission(&command, &admission))
}

/// Validates a protobuf operation status identity; returns encoded `Error` bytes, or empty on success.
#[wasm_bindgen(js_name = validateWireStatus)]
pub fn validate_wire_status(request: Vec<u8>, status: Vec<u8>) -> Vec<u8> {
    validate_wire(|| crate::wire_validation::validate_wire_status(&request, &status))
}

/// Validates a protobuf cancellation identity; returns encoded `Error` bytes, or empty on success.
#[wasm_bindgen(js_name = validateWireCancellation)]
pub fn validate_wire_cancellation(request: Vec<u8>, response: Vec<u8>) -> Vec<u8> {
    validate_wire(|| crate::wire_validation::validate_wire_cancellation(&request, &response))
}

fn validate_wire(validate: impl FnOnce() -> crate::Result<()>) -> Vec<u8> {
    validate().map_or_else(
        |error| crate::encode_error(&error).encode_to_vec(),
        |_| Vec::new(),
    )
}

fn exact_nonnegative_u64(value: f64, field: &str) -> Result<u64, JsValue> {
    if !value.is_finite() || value < 0.0 || value.fract() != 0.0 || value > 9_007_199_254_740_991.0
    {
        return Err(JsValue::from_str(&format!(
            "{field} must be a safe non-negative integer"
        )));
    }
    value
        .to_string()
        .parse::<u64>()
        .map_err(|_| JsValue::from_str(&format!("{field} is outside the supported range")))
}

#[derive(Serialize)]
struct WasmContentEntry {
    name: String,
    kind: &'static str,
}

#[derive(Serialize)]
struct WasmContentPage {
    generation: u64,
    entries: Vec<WasmContentEntry>,
    #[serde(rename = "hasMore")]
    has_more: bool,
}

/// Bounded Rust-owned content state for the WASM `MemoryConversation` adapter.
/// The native filesystem provider uses the same crate-level core while
/// retaining its signed provider-generation proof around delegated reads.
#[wasm_bindgen]
pub struct WasmContentStore {
    store: crate::memory_store::MemoryStore,
}

#[wasm_bindgen]
impl WasmContentStore {
    #[wasm_bindgen(constructor)]
    pub fn new(
        volume: JsValue,
        maximum_file_bytes: f64,
        maximum_path_bytes: f64,
        maximum_resident_bytes: f64,
        maximum_resident_files: f64,
    ) -> Result<Self, JsValue> {
        let volume: VolumeRef = from_js(volume)?;
        Self::from_limits(
            volume,
            maximum_file_bytes,
            maximum_path_bytes,
            maximum_resident_bytes,
            maximum_resident_files,
        )
    }

    /// Constructs a store with the canonical Rust policy for resident data.
    ///
    /// File and path limits remain explicit because they are selected by the
    /// conversation contract; residency defaults are platform policy and must
    /// not be independently re-authored by a JavaScript adapter.
    #[wasm_bindgen(js_name = newDefault)]
    pub fn new_default(
        volume: JsValue,
        maximum_file_bytes: f64,
        maximum_path_bytes: f64,
    ) -> Result<Self, JsValue> {
        let volume: VolumeRef = from_js(volume)?;
        Self::from_limits(
            volume,
            maximum_file_bytes,
            maximum_path_bytes,
            crate::memory_store::DEFAULT_RESIDENT_BYTES as f64,
            crate::memory_store::DEFAULT_RESIDENT_FILES as f64,
        )
    }

    fn from_limits(
        volume: VolumeRef,
        maximum_file_bytes: f64,
        maximum_path_bytes: f64,
        maximum_resident_bytes: f64,
        maximum_resident_files: f64,
    ) -> Result<Self, JsValue> {
        let store = crate::memory_store::MemoryStore::new(
            volume,
            exact_nonnegative_u64(maximum_file_bytes, "maximum_file_bytes")?,
            exact_nonnegative_u64(maximum_path_bytes, "maximum_path_bytes")?,
            exact_nonnegative_u64(maximum_resident_bytes, "maximum_resident_bytes")?,
            exact_nonnegative_u64(maximum_resident_files, "maximum_resident_files")?,
        )
        .map_err(js_error)?;
        Ok(Self { store })
    }

    /// Stores one immutable file and optionally advances its path head.
    pub fn stage(
        &mut self,
        path: String,
        bytes: Vec<u8>,
        media_type: String,
        display_name: String,
        update_path: bool,
    ) -> Result<JsValue, JsValue> {
        let reference = self
            .store
            .stage(&path, &bytes, &media_type, &display_name, update_path)
            .map_err(js_error)?;
        to_js_admitted(&reference)
    }

    /// Reads only an exact, resident immutable reference owned by this store.
    pub fn read(&self, file: JsValue) -> Result<Vec<u8>, JsValue> {
        let file: FileRef = from_js(file)?;
        self.store.read(&file).map_err(js_error)
    }

    /// Tests local residency without exposing mutable storage maps.
    pub fn has(&self, file: JsValue) -> Result<bool, JsValue> {
        let file: FileRef = from_js(file)?;
        self.store.has(&file).map_err(js_error)
    }

    /// Reports whether a new file at `path` would conflict with a file or
    /// directory already retained by this provider.
    #[wasm_bindgen(js_name = pathConflicts)]
    pub fn path_conflicts(&self, path: String) -> bool {
        self.store.path_conflicts(&path)
    }

    /// Returns a generation-pinned directory page from Rust-owned path state.
    pub fn list(
        &self,
        path: String,
        generation: JsValue,
        after: Option<String>,
        maximum: f64,
    ) -> Result<JsValue, JsValue> {
        let generation = self.requested_generation(generation)?;
        let maximum = usize::try_from(exact_nonnegative_u64(maximum, "maximum")?)
            .map_err(|_| JsValue::from_str("maximum is outside the supported range"))?;
        let page = self
            .store
            .list(&path, Some(generation), after.as_deref(), maximum)
            .map_err(js_error)?;
        let page = WasmContentPage {
            generation: page.generation,
            entries: page
                .entries
                .into_iter()
                .map(|entry| WasmContentEntry {
                    name: entry.name,
                    kind: match entry.kind {
                        crate::memory_store::MemoryStoreEntryKind::File => "file",
                        crate::memory_store::MemoryStoreEntryKind::Directory => "directory",
                    },
                })
                .collect(),
            has_more: page.has_more,
        };
        to_js_admitted(&page)
    }

    /// Resolves a path at or before an explicit generation.
    pub fn read_path(&self, path: String, generation: JsValue) -> Result<JsValue, JsValue> {
        let generation = self.requested_generation(generation)?;
        let file = self
            .store
            .read_path(&path, Some(generation))
            .map_err(js_error)?;
        to_js_admitted(&file)
    }

    /// Returns the current path generation as an exact JavaScript bigint.
    pub fn generation(&self) -> Result<JsValue, JsValue> {
        to_js(&self.store.generation())
    }

    fn requested_generation(&self, value: JsValue) -> Result<u64, JsValue> {
        let requested = if value.is_null() || value.is_undefined() {
            None
        } else {
            Some(from_js::<u64>(value)?)
        };
        let generation = requested.unwrap_or_else(|| self.store.generation());
        if generation > self.store.generation() {
            return Err(JsValue::from_str(
                "private directory generation is unavailable",
            ));
        }
        Ok(generation)
    }
}
