use super::*;


#[wasm_bindgen(typescript_custom_section)]
pub(super) const ADMISSION_WIRE_TYPES: &'static str = r#"
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
export interface WasmResourceCreationProofRefWire {
    readonly provider: WasmProviderRefWire;
    readonly preparation: string;
    readonly resource: WasmResourceRefWire;
    readonly evidence: WasmResourceRefWire;
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
    readonly resource_proofs: readonly WasmResourceCreationProofRefWire[];
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
    readonly resource_proofs: readonly WasmResourceCreationProofRefWire[];
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
pub(super) const TURN_PREPARATION_TYPES: &'static str = r#"
export type WasmTurnDisposition = "dispatch" | "reconcile" | "indeterminate" | "completed";
export interface WasmTurnPreparation {
    readonly user_id: string;
    readonly append_user: boolean;
    readonly selection: Readonly<{
        readonly conversation_revision: bigint;
        readonly message_ids: readonly string[];
        readonly checkpoint?: WasmFileRefWire;
    }>;
    readonly selection_is_new: boolean;
    readonly disposition: WasmTurnDisposition;
}
"#;

#[wasm_bindgen(typescript_custom_section)]
pub(super) const MODEL_TYPES: &'static str = r#"
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
export type WasmModelToolResultContentInput =
    | Readonly<{ kind: "json"; value: unknown }>
    | (Omit<Extract<ToolResultContent, { kind: "parts" }>, "parts">
        & Readonly<{ parts: readonly ModelDataPart[] }>);
type WasmModelCamelContentPart<Part extends ModelContentPart> =
    Part extends { readonly kind: "tool_call"; readonly call_id: string }
        ? Omit<Part, "call_id" | "arguments"> & Readonly<{ callId: string; arguments: unknown }>
        : Part extends { readonly kind: "tool_result"; readonly call_id: string }
            ? Omit<Part, "call_id" | "content"> & Readonly<{ callId: string; content: WasmModelToolResultContentInput }>
            : Part;
export type WasmModelContentPartInput = WasmModelCamelContentPart<ModelContentPart>;
export type WasmModelContentInput = string | WasmModelContentPartInput | readonly WasmModelContentPartInput[];
export type WasmModelMessageInput = Omit<ModelMessage, "content"> & { content: WasmModelContentInput };
type WasmModelCamelEvent<Event extends ModelEvent> =
    Event extends { readonly kind: "tool_call"; readonly call_id: string }
        ? Omit<Event, "call_id" | "arguments"> & Readonly<{ callId: string; arguments: unknown }>
        : Event extends { readonly kind: "completed" }
            ? Omit<Event, "metadata"> & Readonly<{ metadata: unknown }>
        : Event;
export type WasmModelEventInput = WasmModelCamelEvent<ModelEvent>;
"#;
