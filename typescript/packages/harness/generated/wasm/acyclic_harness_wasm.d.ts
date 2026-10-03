/* tslint:disable */
/* eslint-disable */

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
    readonly contract: "harness.task-admission.v2" | "harness.task-admission.v3";
    readonly operation_id: string;
    readonly task: WasmMachineIdentityWire;
    readonly machine: WasmMachineIdentityWire;
    readonly input: unknown;
    readonly input_schema: WasmToolJsonSchema;
    readonly output_schema: WasmToolJsonSchema;
    readonly parent: string | null;
    readonly dependencies?: readonly string[];
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


/**
 * Ordered authority-resolution level from the runtime root to one invocation.
 */
export type AuthorityLevel = "runtime" | "agent" | "conversation" | "session" | "turn" | "task" | "invocation";

/**
 * Public model-message input used by the runtime validator.  The content
 * input intentionally reuses the generated camelCase facade type while the
 * Rust parser below still consumes the canonical `ModelMessage` DTO.
 */
export interface WasmModelMessageInput {
    role: WasmModelRole;
    content: WasmModelContentInput;
}

/**
 * Stable wire identity used during compatibility handshakes.
 */
export interface ProtocolIdentity {
    /**
     * Semantic protocol version.
     */
    version: string;
    /**
     * Digest of the canonical descriptor set.
     */
    descriptor_digest: string;
}

/**
 * Tsify declarations for the provider-neutral model values.  These wrappers
 * deliberately mirror the serde model DTOs instead of maintaining a second
 * TypeScript-owned wire union.
 */
export interface WasmModelWire {
    provider: string;
    name: string;
    revision: string;
    options: WasmModelJsonValue;
}

export interface WasmBatchAdmissionInput {
    group_id: string;
    batch_id: string;
    group_policy: "collect-all" | "cancel-on-failure";
    name: string;
    version: string;
    inputs: readonly WasmToolJsonValue[];
    input_schema: WasmToolJsonSchema;
    output_schema: WasmToolJsonSchema;
    requirements: readonly string[];
    machine_digest: readonly number[];
    parent: string | null;
    grants: readonly string[];
    limits: WasmLimitsInput;
    run_limits: WasmTaskRunLimitsInput;
    extensions: WasmExtensionAdmissionWire | null;
    policy: WasmMachineIdentityWire | null;
    execution: WasmExecutionPlacementWire | null;
}

export interface WasmExtensionDependencyDefinition {
    name: string;
    version: number;
}

export interface WasmLimitsInput {
    file_bytes: bigint;
    path_bytes: bigint;
    attachments: bigint;
    render_bytes: bigint;
    model_steps: bigint;
    model_events_per_step: bigint;
    tool_calls_per_step: bigint;
    context_messages: bigint;
}

export interface WasmModelAttemptWire {
    operation_id: string;
    step: number;
    request_digest: readonly number[];
    observed: WasmModelEvent[];
}

export interface WasmModelEventAdmission {
    event: WasmModelEvent;
    state: WasmModelEventAdmissionState;
}

export interface WasmModelEventAdmissionState {
    count: number;
    calls: string[];
    completed: boolean;
    text_bytes: number;
}

export interface WasmModelMessageWire {
    role: WasmModelRole;
    content: WasmModelContent;
}

export interface WasmModelOptionPolicyWire {
    name: string;
    version: string;
    digest: readonly number[];
    schema: WasmModelJsonSchema;
}

export interface WasmModelRequestWire {
    model: WasmModelWire;
    messages: WasmModelMessageWire[];
    tools: WasmModelToolDefinitionWire[];
    max_output_tokens: number | null;
}

export interface WasmModelToolDefinitionWire {
    name: string;
    revision: string;
    description: string;
    input_schema: WasmModelJsonSchema;
    output_schema: WasmModelJsonSchema;
    model_output_schema: WasmModelJsonSchema;
}

export interface WasmTaskAdmissionInput {
    operation_id: string;
    name: string;
    version: string;
    input: WasmToolJsonValue;
    input_schema: WasmToolJsonSchema;
    output_schema: WasmToolJsonSchema;
    requirements: readonly string[];
    machine_digest: readonly number[];
    parent: string | null;
    dependencies?: readonly string[];
    grants: readonly string[];
    limits: WasmLimitsInput;
    run_limits: WasmTaskRunLimitsInput;
    policy: WasmMachineIdentityWire | null;
    extensions: WasmExtensionAdmissionWire | null;
    execution: WasmExecutionPlacementWire | null;
}

export interface WasmTaskDependencyComponents {
    model: boolean;
    context: boolean;
    interactions: boolean;
    policy: boolean;
    host: boolean;
    state: boolean;
    spawner: boolean;
    content: boolean;
    artifacts: boolean;
    content_write: boolean;
    artifacts_write: boolean;
}

export interface WasmTaskDependencyDefinition {
    name: string;
    version: string;
    requirements: readonly string[];
}

export interface WasmTaskDependencyInput {
    tasks: readonly WasmTaskDependencyDefinition[];
    tools: readonly WasmToolDependencyDefinition[];
    components: WasmTaskDependencyComponents;
    grants: readonly string[];
    extensions: readonly WasmExtensionDependencyDefinition[];
}

export interface WasmTaskIdentityInput {
    name: string;
    version: string;
    input_schema: WasmToolJsonSchema;
    output_schema: WasmToolJsonSchema;
    requirements: readonly string[];
    machine_digest: readonly number[];
}

export interface WasmTaskRunLimitsInput {
    concurrency: bigint | null;
    max_steps: bigint | null;
    deadline_epoch_ms: bigint | null;
}

export interface WasmToolDependencyDefinition {
    name: string;
    version: string;
}

export type WasmFileProjectionPolicy = "reference" | "bounded_full" | "native";

export type WasmModelContent = string | WasmModelContentPart | WasmModelContentPart[];

export type WasmModelContentPart = { kind: "text"; text: string } | { kind: "file"; file: WasmFileRefWire; policy: WasmFileProjectionPolicy } | { kind: "tool_call"; call_id: string; name: string; arguments: WasmModelJsonValue } | { kind: "tool_result"; call_id: string; name: string; value: WasmModelJsonValue };

export type WasmModelEvent = { kind: "content"; delta: string } | { kind: "reasoning"; delta: string } | { kind: "tool_call"; call_id: string; name: string; arguments: WasmModelJsonValue } | { kind: "completed"; metadata: WasmModelJsonValue };

export type WasmModelRole = "system" | "user" | "assistant" | "tool";


/**
 * Bounded Rust-owned content state for the WASM `MemoryConversation` adapter.
 * The native filesystem provider uses the same crate-level core while
 * retaining its signed provider-generation proof around delegated reads.
 */
export class WasmContentStore {
    free(): void;
    [Symbol.dispose](): void;
    /**
     * Returns the current path generation as an exact JavaScript bigint.
     */
    generation(): any;
    /**
     * Tests local residency without exposing mutable storage maps.
     */
    has(file: any): boolean;
    /**
     * Returns a generation-pinned directory page from Rust-owned path state.
     */
    list(path: string, generation: any, after: string | null | undefined, maximum: number): any;
    constructor(volume: any, maximum_file_bytes: number, maximum_path_bytes: number, maximum_resident_bytes: number, maximum_resident_files: number);
    /**
     * Reports whether a new file at `path` would conflict with a file or
     * directory already retained by this provider.
     */
    pathConflicts(path: string): boolean;
    /**
     * Reads only an exact, resident immutable reference owned by this store.
     */
    read(file: any): Uint8Array;
    /**
     * Resolves a path at or before an explicit generation.
     */
    read_path(path: string, generation: any): any;
    /**
     * Stores one immutable file and optionally advances its path head.
     */
    stage(path: string, bytes: Uint8Array, media_type: string, display_name: string, update_path: boolean): any;
}

/**
 * Opaque synchronous reducer hosted in WebAssembly.
 */
export class WasmReducer {
    free(): void;
    [Symbol.dispose](): void;
    /**
     * Applies a deterministic command. Fresh extension migrations require the
     * native content-admission host and cannot be synthesized by this reducer.
     */
    apply(command: any): any;
    /**
     * Applies one canonical Protobuf command, excluding fresh migrations
     * that require executable content admission, and returns a response.
     */
    applyWire(command: Uint8Array): Uint8Array;
    /**
     * Attenuates a scope without permitting capability expansion.
     */
    attenuate(parent: any, id: string, capabilities: any): any;
    /**
     * Returns the authoritative conversation projection, never a parallel JS reducer.
     */
    conversationJson(): string;
    /**
     * Returns the same bounded page with Rust-owned JS integer projection:
     * revisions and cursors stay `BigInt`, validated file lengths become Number.
     */
    conversationPage(after_sequence: bigint, limit: number): any;
    /**
     * Returns an immutable, bounded page of canonical conversation records.
     * `after_sequence` is an exclusive cursor in the message sequence, not
     * the potentially larger aggregate event revision.
     */
    conversationPageJson(after_sequence: bigint, limit: number): string;
    /**
     * Resolves a complete canonical attachment manifest under the Rust rules.
     */
    decodeAttachmentManifest(manifest: any, bytes: Uint8Array, item_count: number): any;
    /**
     * Delegates a segment-bounded private directory to a reader without a fork.
     */
    delegatePrivateDirectoryRead(owner_scope: any, reader: string, id: string, volume: any, prefix: string): any;
    /**
     * Delegates one pinned private file from its signed owner to a reader.
     */
    delegatePrivateFileRead(owner_scope: any, reader: string, id: string, file: any): any;
    /**
     * Computes the canonical capability for one private directory boundary.
     */
    directoryReadCapability(volume: any, prefix: string): string;
    /**
     * Derives the canonical SHA-256 descriptor for staged bytes.
     */
    fileDescriptorJson(bytes: Uint8Array, media_type: string): string;
    /**
     * Computes the exact-version read capability without embedding it in a ref.
     */
    fileReadCapability(file: any): string;
    /**
     * Issues a root scope from this host's explicit authority object.
     */
    issueScope(id: string, capabilities: any): any;
    /**
     * Issues a root scope signed for one acting agent.
     */
    issueScopeForAgent(agent: string, id: string, capabilities: any): any;
    /**
     * Resolves named policy layers and issues only their effective grant.
     */
    issueScopeWithPolicies(id: string, layers: any): any;
    /**
     * Resolves hierarchy policy and binds its result to one acting agent.
     */
    issueScopeWithPoliciesForAgent(agent: string, id: string, layers: any): any;
    /**
     * Creates an empty reducer with explicit host-managed authority.
     */
    constructor(authority: any, issuer_id: string, issuer_key: Uint8Array, schemas: any);
    /**
     * Returns the exact wire identity used by this compiled core.
     */
    protocolIdentity(): any;
    /**
     * Restores a snapshot under explicit host-managed authority.
     */
    static restore(snapshot: any, issuer_id: string, issuer_key: Uint8Array, schemas: any): WasmReducer;
    /**
     * Returns a versioned integrity-checked restoration snapshot.
     */
    snapshot(): any;
    /**
     * Validates one ref-only canonical message under explicit runtime limits.
     */
    validateConversationMessage(message: any, limits: any): void;
    /**
     * Validates an exact content reference using the native Rust contract.
     */
    validateFileRef(file: any): void;
    /**
     * Applies configured runtime bounds to one exact file reference.
     */
    validateFileUnderLimits(file: any, limits: any): void;
    /**
     * Authenticates an owner-issued read grant for this exact immutable file.
     * A caller-supplied capability string alone is never accepted as proof.
     */
    verifyContentRead(scope: any, file: any): void;
    /**
     * Authenticates a writer against the original private-volume owner.
     */
    verifyContentWrite(scope: any, volume: any): void;
    /**
     * Verifies exact bytes against the pinned SHA-256 and byte-length descriptor.
     */
    verifyFileBytes(file: any, bytes: Uint8Array): void;
    /**
     * Authenticates lazy directory and named-path access against the
     * owner's signed, segment-bounded private-volume read grant.
     */
    verifyPrivateDirectoryRead(scope: any, volume: any, granted_prefix: string, path: string): void;
    /**
     * Authenticates the signed scope before provider discovery or routing.
     */
    verifyScope(scope: any): void;
    /**
     * Computes the canonical Rust capability for one validated volume operation.
     */
    volumeCapability(volume: any, operation: string): string;
    /**
     * Stable physical namespace shared by Filesystem and Objects adapters.
     */
    volumeStorageName(volume: any): string;
}

/**
 * Builds and validates the complete immutable batch request before any
 * member admission. Inputs, task identity, limits, policy, and route are
 * projected by the same Rust constructor used by native hosts.
 */
export function admitBatch(value: WasmBatchAdmissionInput): WasmDurableBatchWire;

/**
 * Builds the complete SDK-facing durable batch request in one Rust-owned
 * projection. Member envelopes, policy, implementation digest, canonical
 * manifest, and request digest all derive from the same validated request.
 */
export function admitBatchRequest(value: WasmBatchAdmissionInput): WasmBatchAdmissionRequest;

/**
 * Admits one provider model event with the native stream rules and returns
 * the detached state needed for the next event. Text accounting is cumulative
 * across model steps while event and tool-call bounds reset at each step.
 */
export function admitModelEvent(event: WasmModelEventInput, limits: WasmModelLimitsInput, state: WasmModelEventAdmissionState | null): WasmModelEventAdmission;

/**
 * Builds and validates the complete owner-retained task admission envelope.
 * TypeScript supplies public values, while Rust owns identity derivation,
 * schema/value validation, limits, authority, and execution binding.
 */
export function admitTask(value: WasmTaskAdmissionInput): WasmTaskAdmissionWire;

/**
 * Derives the same immutable per-slot operation as Rust durable admission.
 */
export function batchMemberOperationId(group: string, batch: string, index: number): string;

/**
 * Decodes a generated aggregate kind using the native enum mapping.
 */
export function decodeAggregateKind(value: number): any;

/**
 * Decodes and validates a canonical Protobuf apply response using Rust-owned
 * event, authority, scope, digest, and payload rules.
 */
export function decodeApplyResponse(bytes: Uint8Array): any;

/**
 * Decodes a complete canonical attachment list without a reducer instance.
 */
export function decodeAttachmentManifest(manifest: any, bytes: Uint8Array, item_count: number): any;

/**
 * Parses canonical JSON without passing full-width integer literals through
 * JavaScript Number. Large serde integers are returned as `BigInt`.
 */
export function decodeCanonicalJson(bytes: Uint8Array): any;

/**
 * Decodes one canonical event payload using the native event union.
 */
export function decodeEventPayload(event_type: string, canonical_payload_json: Uint8Array): any;

/**
 * Parses external JSON with exact integers but without demanding canonical
 * key order or whitespace. Callers must still apply their schema and numeric
 * range policy before presenting model-authored values to an executor.
 */
export function decodeJson(bytes: Uint8Array): any;

/**
 * Derives a stable child operation/message identity from one admitted operation
 * and a local role label without duplicating UUID bit manipulation in hosts.
 */
export function deriveOperationUuid(operation: string, label: string): string;

/**
 * Hashes the same bounded canonical JSON bytes used by native admissions.
 */
export function digestCanonicalJson(value: any): Uint8Array;

/**
 * Encodes a validated attachment list in the exact typed manifest wire form.
 */
export function encodeAttachmentManifest(items: any): Uint8Array;

/**
 * Serializes a plain JavaScript data value through Rust's canonical JSON
 * representation. Unsafe integer Numbers are rejected before conversion;
 * callers must supply `BigInt` for exact full-width identities and counters.
 */
export function encodeCanonicalJson(value: any): Uint8Array;

/**
 * Stages a descriptor with Rust-owned SHA-256, media-type, and safe
 * byte-length rules, projecting its bounded length as a JS Number.
 */
export function fileDescriptor(bytes: Uint8Array, media_type: string): any;

/**
 * Converts a fully captured report into its canonical publishable child seed.
 */
export function forkSeedFromReport(report: any): any;

/**
 * Plans one deterministic conversation turn before any model or content
 * callback runs.  The reducer state and payload checks are shared with the
 * native filesystem memory host; JavaScript retains ownership of asynchronous
 * reads and model dispatch after this plan is committed.
 */
export function prepareConversationTurn(conversation: any, operation_id: string, content: any, attachments: any, limits: any, existing_selection: any, has_completed_output: boolean, can_reconcile: boolean): WasmTurnPreparation;

/**
 * Canonical model-request admission shared by native and TypeScript execution.
 * Content authority must first be checked by the caller's bound content resolver;
 * this export performs no retrieval and grants no filesystem permissions.
 */
export function prepareModelRequest(request: WasmModelRequestWire, limits: WasmModelLimitsInput, policy: WasmModelOptionPolicyWire | null): any;

/**
 * Runs the canonical Rust conversation projection over bytes captured by the
 * owner.  TypeScript supplies a map rather than a callback so authorization
 * and async reads finish before this deterministic core is entered.
 */
export function selectModelContext(conversation: any, selection: any, files: any, maximum_messages: number, maximum_attachments: number, maximum_render_bytes: number, maximum_projected_attachments: number): Promise<any>;

/**
 * Derives the exact task and machine identities retained by durable
 * admission. The digest envelope and resumable machine pin are shared with
 * native Rust registration.
 */
export function taskAdmissionIdentities(value: WasmTaskIdentityInput): WasmTaskAdmissionIdentities;

/**
 * Derives the same pinned task registration digest used by native admission.
 */
export function taskIdentityDigest(name: string, version: string, input_schema: any, output_schema: any, requirements: any, machine_digest: Uint8Array): Uint8Array;

/**
 * Uses one half of a canonical action digest as a stable local identity.
 * The digest is already SHA-256; the two halves separate approval operations
 * from their interaction tickets without a second hashing convention.
 */
export function uuidFromDigestHalf(digest: Uint8Array, second: boolean): string;

/**
 * Pure v2 contract admission shared by native and JavaScript hosts. The
 * returned object is detached and canonically shaped by Rust serde; context
 * supplies `Limits` for messages and the open ticket for resolutions.
 */
export function validateContract(kind: string, value: any, context: any): any;

/**
 * Returns the one Rust UUID spelling accepted for a conversation identity.
 */
export function validateConversationMessageId(value: string): string;

/**
 * Parses one public Harness identity with the canonical Rust contract and
 * returns its normalized UUID spelling for a branded TypeScript facade.
 */
export function validateIdentity(kind: string, value: string): string;

/**
 * Validates provider-neutral model content under the exact native limits.
 */
export function validateModelContent(content: WasmModelContentInput, limits: WasmModelLimitsInput): void;

/**
 * Validates selection order and tool linkage before the host resolves any
 * owner-mediated file bytes.
 */
export function validateModelContextSelection(conversation: any, selection: any): void;

/**
 * Validates durable model-input manifest evidence through the production
 * Rust admission path. The manifest JSON is supplied as exact bytes so a
 * caller cannot replace it with an equivalent but differently encoded value.
 */
export function validateModelInputManifest(request: any, limits: any, policy: any, manifest_json: string): void;

/**
 * Validates a complete provider-neutral model message list with the native
 * role, message-count, and content bounds.  Context builders and the stock
 * TypeScript loop therefore share the same closed role set and limits as
 * native durable execution.
 */
export function validateModelMessages(messages: readonly WasmModelMessageInput[], limits: WasmModelLimitsInput): void;

/**
 * Admits an already projected, provider-proven context with native model bounds.
 */
export function validateSelectedModelContext(selected: any, limits: any): void;

/**
 * Validates and reprojects one owner-retained direct-child page using the
 * same bounds and bytewise slot ordering as native Harness hosts.
 */
export function validateTaskChildrenPage(value: WasmTaskChildrenPageInput): WasmTaskChildrenPage;

/**
 * Validates the exact task dependency graph used by the TypeScript builder.
 * The input is a contract projection only; no executable task handlers cross
 * the WASM boundary and Rust owns graph traversal, revision matching, grants,
 * and extension requirement admission.
 */
export function validateTaskRequirements(value: WasmTaskDependencyInput): void;

/**
 * Validates a model-visible tool definition using the native contract.
 */
export function validateToolDefinition(definition: any): void;

/**
 * Validates one tool invocation against its registered definition.
 */
export function validateToolInvocation(definition: any, invocation: any): void;

/**
 * Validates a model-visible projected tool result against its pinned schema.
 */
export function validateToolProjection(definition: any, result: any): void;

/**
 * Validates one successful tool result against its registered definition.
 */
export function validateToolResult(definition: any, result: any): void;

/**
 * Applies the same JSON Schema admission used by Rust tool execution before
 * a TypeScript facade turns an untrusted model value into a typed argument.
 */
export function validateToolValue(schema: any, value: any): any;

/**
 * Validates one human-authored model input using the native content rules.
 */
export function validateUserInput(content: WasmModelContentInput): void;

/**
 * Validates a protobuf admission identity; returns encoded `Error` bytes, or empty on success.
 */
export function validateWireAdmission(command: Uint8Array, admission: Uint8Array): Uint8Array;

/**
 * Validates a protobuf cancel request using the canonical Rust scope rules.
 */
export function validateWireCancel(request: Uint8Array): Uint8Array;

/**
 * Validates a protobuf cancellation identity; returns encoded `Error` bytes, or empty on success.
 */
export function validateWireCancellation(request: Uint8Array, response: Uint8Array): Uint8Array;

/**
 * Validates one complete protobuf command before it crosses a wire adapter.
 */
export function validateWireCommand(command: Uint8Array): Uint8Array;

/**
 * Validates only the protocol identity of a command envelope.
 */
export function validateWireCommandProtocol(command: Uint8Array): Uint8Array;

/**
 * Validates a protobuf handshake; returns encoded `Error` bytes, or empty on success.
 */
export function validateWireHandshake(request: Uint8Array, response: Uint8Array): Uint8Array;

/**
 * Validates a protobuf observe request using the canonical Rust scope rules.
 */
export function validateWireObserve(request: Uint8Array): Uint8Array;

/**
 * Validates a protobuf replay request against the compiled protocol identity.
 */
export function validateWireResume(request: Uint8Array): Uint8Array;

/**
 * Validates a protobuf operation status identity; returns encoded `Error` bytes, or empty on success.
 */
export function validateWireStatus(request: Uint8Array, status: Uint8Array): Uint8Array;

/**
 * Checks immutable file identity without constructing a reducer or issuer.
 */
export function verifyFileBytes(file: any, bytes: Uint8Array): void;

export type InitInput = RequestInfo | URL | Response | BufferSource | WebAssembly.Module;

export interface InitOutput {
    readonly memory: WebAssembly.Memory;
    readonly __wbg_wasmcontentstore_free: (a: number, b: number) => void;
    readonly __wbg_wasmreducer_free: (a: number, b: number) => void;
    readonly admitBatch: (a: any) => [number, number, number];
    readonly admitBatchRequest: (a: any) => [number, number, number];
    readonly admitModelEvent: (a: any, b: any, c: any) => [number, number, number];
    readonly admitTask: (a: any) => [number, number, number];
    readonly batchMemberOperationId: (a: number, b: number, c: number, d: number, e: number) => [number, number, number, number];
    readonly decodeAggregateKind: (a: number) => [number, number, number];
    readonly decodeApplyResponse: (a: number, b: number) => [number, number, number];
    readonly decodeAttachmentManifest: (a: any, b: number, c: number, d: number) => [number, number, number];
    readonly decodeCanonicalJson: (a: number, b: number) => [number, number, number];
    readonly decodeEventPayload: (a: number, b: number, c: number, d: number) => [number, number, number];
    readonly decodeJson: (a: number, b: number) => [number, number, number];
    readonly deriveOperationUuid: (a: number, b: number, c: number, d: number) => [number, number, number, number];
    readonly digestCanonicalJson: (a: any) => [number, number, number, number];
    readonly encodeAttachmentManifest: (a: any) => [number, number, number, number];
    readonly encodeCanonicalJson: (a: any) => [number, number, number, number];
    readonly fileDescriptor: (a: number, b: number, c: number, d: number) => [number, number, number];
    readonly forkSeedFromReport: (a: any) => [number, number, number];
    readonly prepareConversationTurn: (a: any, b: number, c: number, d: any, e: any, f: any, g: any, h: number, i: number) => [number, number, number];
    readonly prepareModelRequest: (a: any, b: any, c: any) => [number, number, number];
    readonly selectModelContext: (a: any, b: any, c: any, d: number, e: number, f: number, g: number) => any;
    readonly taskAdmissionIdentities: (a: any) => [number, number, number];
    readonly taskIdentityDigest: (a: number, b: number, c: number, d: number, e: any, f: any, g: any, h: number, i: number) => [number, number, number, number];
    readonly uuidFromDigestHalf: (a: number, b: number, c: number) => [number, number, number, number];
    readonly validateContract: (a: number, b: number, c: any, d: any) => [number, number, number];
    readonly validateConversationMessageId: (a: number, b: number) => [number, number, number, number];
    readonly validateIdentity: (a: number, b: number, c: number, d: number) => [number, number, number, number];
    readonly validateModelContent: (a: any, b: any) => [number, number];
    readonly validateModelContextSelection: (a: any, b: any) => [number, number];
    readonly validateModelInputManifest: (a: any, b: any, c: any, d: number, e: number) => [number, number];
    readonly validateModelMessages: (a: any, b: any) => [number, number];
    readonly validateSelectedModelContext: (a: any, b: any) => [number, number];
    readonly validateTaskChildrenPage: (a: any) => [number, number, number];
    readonly validateTaskRequirements: (a: any) => [number, number];
    readonly validateToolDefinition: (a: any) => [number, number];
    readonly validateToolInvocation: (a: any, b: any) => [number, number];
    readonly validateToolProjection: (a: any, b: any) => [number, number];
    readonly validateToolResult: (a: any, b: any) => [number, number];
    readonly validateToolValue: (a: any, b: any) => [number, number, number];
    readonly validateUserInput: (a: any) => [number, number];
    readonly validateWireAdmission: (a: number, b: number, c: number, d: number) => [number, number];
    readonly validateWireCancel: (a: number, b: number) => [number, number];
    readonly validateWireCancellation: (a: number, b: number, c: number, d: number) => [number, number];
    readonly validateWireCommand: (a: number, b: number) => [number, number];
    readonly validateWireCommandProtocol: (a: number, b: number) => [number, number];
    readonly validateWireHandshake: (a: number, b: number, c: number, d: number) => [number, number];
    readonly validateWireObserve: (a: number, b: number) => [number, number];
    readonly validateWireResume: (a: number, b: number) => [number, number];
    readonly validateWireStatus: (a: number, b: number, c: number, d: number) => [number, number];
    readonly verifyFileBytes: (a: any, b: number, c: number) => [number, number];
    readonly wasmcontentstore_generation: (a: number) => [number, number, number];
    readonly wasmcontentstore_has: (a: number, b: any) => [number, number, number];
    readonly wasmcontentstore_list: (a: number, b: number, c: number, d: any, e: number, f: number, g: number) => [number, number, number];
    readonly wasmcontentstore_new: (a: any, b: number, c: number, d: number, e: number) => [number, number, number];
    readonly wasmcontentstore_pathConflicts: (a: number, b: number, c: number) => number;
    readonly wasmcontentstore_read: (a: number, b: any) => [number, number, number, number];
    readonly wasmcontentstore_read_path: (a: number, b: number, c: number, d: any) => [number, number, number];
    readonly wasmcontentstore_stage: (a: number, b: number, c: number, d: number, e: number, f: number, g: number, h: number, i: number, j: number) => [number, number, number];
    readonly wasmreducer_apply: (a: number, b: any) => [number, number, number];
    readonly wasmreducer_applyWire: (a: number, b: number, c: number) => [number, number, number, number];
    readonly wasmreducer_attenuate: (a: number, b: any, c: number, d: number, e: any) => [number, number, number];
    readonly wasmreducer_conversationJson: (a: number) => [number, number, number, number];
    readonly wasmreducer_conversationPage: (a: number, b: bigint, c: number) => [number, number, number];
    readonly wasmreducer_conversationPageJson: (a: number, b: bigint, c: number) => [number, number, number, number];
    readonly wasmreducer_decodeAttachmentManifest: (a: number, b: any, c: number, d: number, e: number) => [number, number, number];
    readonly wasmreducer_delegatePrivateDirectoryRead: (a: number, b: any, c: number, d: number, e: number, f: number, g: any, h: number, i: number) => [number, number, number];
    readonly wasmreducer_delegatePrivateFileRead: (a: number, b: any, c: number, d: number, e: number, f: number, g: any) => [number, number, number];
    readonly wasmreducer_directoryReadCapability: (a: number, b: any, c: number, d: number) => [number, number, number, number];
    readonly wasmreducer_fileDescriptorJson: (a: number, b: number, c: number, d: number, e: number) => [number, number, number, number];
    readonly wasmreducer_fileReadCapability: (a: number, b: any) => [number, number, number, number];
    readonly wasmreducer_issueScope: (a: number, b: number, c: number, d: any) => [number, number, number];
    readonly wasmreducer_issueScopeForAgent: (a: number, b: number, c: number, d: number, e: number, f: any) => [number, number, number];
    readonly wasmreducer_issueScopeWithPolicies: (a: number, b: number, c: number, d: any) => [number, number, number];
    readonly wasmreducer_issueScopeWithPoliciesForAgent: (a: number, b: number, c: number, d: number, e: number, f: any) => [number, number, number];
    readonly wasmreducer_new: (a: any, b: number, c: number, d: number, e: number, f: any) => [number, number, number];
    readonly wasmreducer_protocolIdentity: (a: number) => [number, number, number];
    readonly wasmreducer_restore: (a: any, b: number, c: number, d: number, e: number, f: any) => [number, number, number];
    readonly wasmreducer_snapshot: (a: number) => [number, number, number];
    readonly wasmreducer_validateConversationMessage: (a: number, b: any, c: any) => [number, number];
    readonly wasmreducer_validateFileRef: (a: number, b: any) => [number, number];
    readonly wasmreducer_validateFileUnderLimits: (a: number, b: any, c: any) => [number, number];
    readonly wasmreducer_verifyContentRead: (a: number, b: any, c: any) => [number, number];
    readonly wasmreducer_verifyContentWrite: (a: number, b: any, c: any) => [number, number];
    readonly wasmreducer_verifyFileBytes: (a: number, b: any, c: number, d: number) => [number, number];
    readonly wasmreducer_verifyPrivateDirectoryRead: (a: number, b: any, c: any, d: number, e: number, f: number, g: number) => [number, number];
    readonly wasmreducer_verifyScope: (a: number, b: any) => [number, number];
    readonly wasmreducer_volumeCapability: (a: number, b: any, c: number, d: number) => [number, number, number, number];
    readonly wasmreducer_volumeStorageName: (a: number, b: any) => [number, number, number, number];
    readonly __wbindgen_malloc: (a: number, b: number) => number;
    readonly __wbindgen_realloc: (a: number, b: number, c: number, d: number) => number;
    readonly __wbindgen_exn_store: (a: number) => void;
    readonly __externref_table_alloc: () => number;
    readonly __wbindgen_externrefs: WebAssembly.Table;
    readonly __wbindgen_destroy_closure: (a: number, b: number) => void;
    readonly __externref_table_dealloc: (a: number) => void;
    readonly __wbindgen_free: (a: number, b: number, c: number) => void;
    readonly __wbindgen_start: () => void;
}

export type SyncInitInput = BufferSource | WebAssembly.Module;

/**
 * Instantiates the given `module`, which can either be bytes or
 * a precompiled `WebAssembly.Module`.
 *
 * @param {{ module: SyncInitInput }} module - Passing `SyncInitInput` directly is deprecated.
 *
 * @returns {InitOutput}
 */
export function initSync(module: { module: SyncInitInput } | SyncInitInput): InitOutput;

/**
 * If `module_or_path` is {RequestInfo} or {URL}, makes a request and
 * for everything else, calls `WebAssembly.instantiate` directly.
 *
 * @param {{ module_or_path: InitInput | Promise<InitInput> }} module_or_path - Passing `InitInput` directly is deprecated.
 *
 * @returns {Promise<InitOutput>}
 */
export default function __wbg_init (module_or_path?: { module_or_path: InitInput | Promise<InitInput> } | InitInput | Promise<InitInput>): Promise<InitOutput>;
