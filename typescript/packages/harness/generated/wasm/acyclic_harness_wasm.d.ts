/* tslint:disable */
/* eslint-disable */

/** A host-owned bounded HTTP exchange; cancellation must synchronously abort I/O. */
export interface McpBrowserExchange {
    readonly response: Promise<McpBrowserResponseHead>;
    readonly read: () => Promise<Uint8Array | null>;
    readonly cancel: () => void;
}
/** Platform I/O only. Do not retry requests or follow redirects. */
export type McpBrowserHttpProvider = (operation: string, request: McpHttpRequest) => McpBrowserExchange;



/** Bound owner authenticates each read; declarations confer no authority.
 * Functions are captured for one call and never retained in a process catalog. */
export interface ContextDiscoveryReader {
    list(query: ContextDirectoryQuery): Promise<PrivateDirectoryPage>;
    prefix(query: ContextReadQuery): Promise<Uint8Array>;
    read(query: ContextReadQuery): Promise<ContextPathResult>;
}



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
 * A bounded page from one exact owner-private generation.
 */
export interface PrivateDirectoryPage {
    /**
     * Exact immutable generation from which this page was read.
     */
    generation: WasmResourceRefWire & { kind: 'generation' };
    /**
     * Ordered entries returned for the requested directory segment.
     */
    entries: PrivateDirectoryEntry[];
    /**
     * Whether another page may be requested with the returned cursor.
     */
    hasMore: boolean;
}

/**
 * A declared representation; summary creation remains an admitted model operation.
 */
export type ContextRepresentation = "full" | "summary" | "reference";

/**
 * A host-declared directory boundary. A declaration conveys no authority.
 */
export interface ContextRoot {
    /**
     * Exact volume identity, independently of consumer role.
     */
    volume: WasmVolumeRefWire;
    /**
     * Exact owner-issued subtree capability boundary; empty denotes the volume root.
     */
    directory: string;
}

/**
 * A pinned input; a file reference conveys no read authority.
 */
export type ContextSourceValue = { kind: "file"; file: WasmFileRefWire } | { kind: "attribute"; attribute: ContextAttribute };

/**
 * Actual context and output capacities of one immutable selected model.
 */
export interface ModelContextCapacity {
    /**
     * Maximum tokens in the provider's complete input plus generated output.
     */
    context_tokens: number;
    /**
     * Maximum generated tokens supported by the selected model.
     */
    output_tokens: number;
}

/**
 * Admission barrier for every version-pinned file in a conversation message.
 *
 * Implementations must verify exact byte residency and access before Stream
 * publishes references. The provider retains admitted versions independently.
 */
export type PrivateDirectoryEntryKind = "file" | "directory";

/**
 * An authoritative attribute state, shared by prompt rebuilds and updates.
 */
export interface ContextAttribute {
    /**
     * Registered, namespaced application type (roles are ordinary attributes).
     */
    type_name: string;
    /**
     * Exact type/renderer definition revision.
     */
    type_revision: string;
    /**
     * Schema of this attribute type.
     */
    schema: WasmModelJsonSchema;
    /**
     * Exact authoritative state revision.
     */
    state_revision: string;
    /**
     * State satisfying the registered schema.
     */
    value: WasmModelJsonValue;
}

/**
 * Available logical resources advertised to admission policy.
 */
export type ResourceSnapshot = Record<string, bigint>;

/**
 * Bounded immutable remote catalog. The existing `ToolRegistry` is the only
 * execution registry; this value is a validated registration input.
 */
export interface McpCatalog {
    /**
     * Consumer-selected namespace; names do not convey remote authority.
     */
    server: string;
    /**
     * Exact transport/configuration/catalog revision selected by the host.
     */
    revision: string;
    /**
     * Explicit schema selection pinned with this configuration revision.
     */
    schema_exposure: McpSchemaExposure;
    /**
     * Explicit local discovery selection; notifications cannot change it.
     */
    discovery: McpDiscoveryPolicy;
    /**
     * Complete bounded catalog, not a partially fetched `tools/list` page.
     */
    tools: McpToolDefinition[];
}

/**
 * Canonical MCP result; content stays ordered and may be multimodal.
 */
export interface McpToolResult {
    /**
     * Protocol content blocks. Validation occurs at the protocol boundary.
     */
    content: WasmToolJsonValue[];
    /**
     * Structured value checked against the remote output contract.
     */
    structuredContent?: WasmToolJsonValue;
    /**
     * A tool failure is a recorded result, rather than a transport failure.
     */
    isError?: boolean;
    /**
     * Opaque protocol metadata retained for the owning consumer.
     */
    _meta?: WasmToolJsonValue;
}

/**
 * Durable orchestration behavior represented as data.
 */
export type Orchestration = { kind: "leaf" } | { kind: "join" } | { kind: "race" } | { kind: "quorum"; required: number } | { kind: "reduce"; reducer: EntrypointRef };

/**
 * Exact portable HTTP input. Credentials belong to the bound provider.
 */
export interface McpHttpRequest {
    /**
     * Pinned HTTP(S) endpoint.
     */
    endpoint: string;
    /**
     * HTTP method selected by the protocol owner.
     */
    method: string;
    /**
     * Protocol headers; keys use lowercase ASCII.
     */
    headers: Map<string, string>;
    /**
     * One UTF-8 JSON-RPC message, or empty for GET/DELETE.
     */
    body: number[];
    /**
     * Finite whole-exchange provider deadline.
     */
    timeout_ms: number;
    /**
     * Finite total response-body allowance.
     */
    maximum_response_bytes: number;
}

/**
 * Exact portable exchange descriptor included in native process approval.
 */
export interface McpStdioRequest {
    /**
     * Initialization identity, distinct from the admitted operation.
     */
    initialization: string;
    /**
     * Exact admitted remote request identity.
     */
    operation: string;
    /**
     * Discovery or tool call, explicitly selected by the host.
     */
    method: McpStdioMethod;
    /**
     * Exact object parameters included in approval and durable identity.
     */
    params: WasmToolJsonValue;
    /**
     * Positive cumulative byte allowance per input/output direction. Native
     * capture also enforces its combined stdout/stderr allowance independently.
     */
    maximum_bytes: number;
}

/**
 * Explicit bounded selection from a pinned source.
 */
export type ContextExtent = { kind: "whole" } | { kind: "span"; start: number; end: number };

/**
 * Explicit finite discovery bounds, independent of the retained workspace count.
 */
export interface ContextDiscoveryLimits {
    /**
     * Combined directory entries inspected, including nonmatching names.
     */
    entries: number;
    /**
     * Combined directories visited, including instruction ancestor scopes.
     */
    directories: number;
    /**
     * Maximum frontmatter prefix bytes read per skill.
     */
    header_bytes: number;
    /**
     * Maximum complete instruction bytes per file.
     */
    instruction_bytes: number;
}

/**
 * Explicit lifetime owner for durable work.
 */
export type DurableOwner = { kind: "attached"; authority: Authority } | { kind: "detached"; authority: Authority };

/**
 * Explicit model schema exposure for an installed complete catalog.
 */
export type McpSchemaExposure = { kind: "eager" } | { kind: "selected"; names: string[] };

/**
 * Frontmatter only; the body is not fetched or injected by discovery.
 */
export interface SkillMetadata {
    /**
     * Stable frontmatter name.
     */
    name: string;
    /**
     * Frontmatter description.
     */
    description: string;
    /**
     * Additional declarative frontmatter fields; no execution authority is inferred.
     */
    fields: Record<string, WasmModelJsonValue>;
    /**
     * Exact lazy body location and provenance.
     */
    source: PinnedContextPath;
}

/**
 * Host-approved selection and representation. Model suggestions need host policy approval.
 */
export interface ContextSelection {
    /**
     * Pinned authoritative input.
     */
    source: ContextSourceValue;
    /**
     * Exact selected extent.
     */
    extent: ContextExtent;
    /**
     * Requested representation.
     */
    representation: ContextRepresentation;
}

/**
 * Host-selected local discovery policy; neither variant grants call authority.
 */
export type McpDiscoveryPolicy = "disabled" | "search";

/**
 * Immutable component implementation identity.
 */
export interface ComponentIdentity {
    /**
     * Stable local or namespaced logical name.
     */
    name: string;
    /**
     * Semantic implementation version.
     */
    version: string;
    /**
     * Implementation/schema digest.
     */
    digest: number[];
}

/**
 * Immutable discovery revision. Serialize this value through the existing admitted
 * caller journal for restart; it contains no credentials or mutable provider handles.
 */
export interface DiscoveredContext {
    /**
     * Root-to-leaf instructions in declared root/filename order.
     */
    instructions: WasmFileRefWire[];
    /**
     * Skills in declared root and lexical directory order; duplicate names reject discovery.
     */
    skills: SkillMetadata[];
}

/**
 * Immutable durable operation declaration.
 */
export interface OperationSpec {
    /**
     * Stable operation identity.
     */
    operation_id: string;
    /**
     * Optional structured parent.
     */
    parent: ParentLink | null;
    /**
     * Explicit durable owner.
     */
    owner: DurableOwner;
    /**
     * Restartable implementation identity.
     */
    entrypoint: EntrypointRef;
    /**
     * Dependencies that must succeed before admission.
     */
    dependencies: string[];
    /**
     * Logical capacity requirements.
     */
    resources: ResourceRequest;
    /**
     * Small provider-neutral placement labels, not process or file content.
     */
    placement: Record<string, string>;
    /**
     * Orchestration behavior.
     */
    orchestration: Orchestration;
    /**
     * Immutable, provider-owned initial state bytes.
     */
    state: WasmFileRefWire;
}

/**
 * Immutable path selected by discovery; resolving it never follows a newer head.
 */
export interface PinnedContextPath {
    /**
     * Declared volume and read boundary.
     */
    root: ContextRoot;
    /**
     * Exact retained filesystem generation.
     */
    generation: WasmResourceRefWire & { kind: 'generation' };
    /**
     * Volume-relative path.
     */
    path: string;
}

/**
 * Kind of independently ordered durable aggregate.
 */
export type AggregateKind = "agent" | "conversation" | "session" | "turn" | "task";

/**
 * Logical resource quantities; providers decide how they map to capacity.
 */
export type ResourceRequest = Record<string, bigint>;

/**
 * Mutable context assembled for one model step.
 */
export interface Context {
    /**
     * Ordered model-visible messages.
     */
    messages: WasmModelMessageWire[];
    /**
     * Stage-owned, namespaced version-pinned metadata files.
     */
    metadata: Record<string, WasmFileRefWire>;
}

/**
 * Negotiated server initialization, retained with the admitted session.
 */
export interface McpInitializeResult {
    /**
     * Explicit supported protocol revision, with no silent downgrade.
     */
    protocolVersion: string;
    /**
     * Server-advertised feature set; it does not grant host authority.
     */
    capabilities: WasmToolJsonValue;
    /**
     * Server implementation name and version.
     */
    serverInfo: WasmToolJsonValue;
    /**
     * Optional server instructions, treated as external content.
     */
    instructions?: string;
}

/**
 * One bounded discovery page. A host must fetch the complete catalog before
 * replacing its model-visible selection; a page is not a replacement catalog.
 */
export interface McpToolsPage {
    /**
     * Remote contracts in this page.
     */
    tools: McpToolDefinition[];
    /**
     * Opaque server cursor for the next explicitly admitted discovery request.
     */
    nextCursor?: string;
}

/**
 * One lazily discovered name, without eager byte or ref transfer.
 */
export interface PrivateDirectoryEntry {
    /**
     * One normalized child name relative to the listed directory.
     */
    name: string;
    /**
     * Whether the name resolves to a regular file or another directory.
     */
    kind: PrivateDirectoryEntryKind;
}

/**
 * One ref-only durable inbox item with a gapless per-task sequence.
 */
export interface InboxItem {
    /**
     * Owning task.
     */
    task_id: string;
    /**
     * Gapless one-based sequence.
     */
    sequence: bigint;
    /**
     * Sender-defined idempotency identity.
     */
    message_id: string;
    /**
     * Immutable payload bytes staged before Stream publication.
     */
    payload: WasmFileRefWire;
}

/**
 * Ordered authority-resolution level from the runtime root to one invocation.
 */
export type AuthorityLevel = "runtime" | "agent" | "conversation" | "session" | "turn" | "task" | "invocation";

/**
 * Ordinary declaration values used by both explicit reload and live bindings.
 */
export interface ContextDiscovery {
    /**
     * Ordered repository scopes.
     */
    instructions: InstructionScope[];
    /**
     * Ordered independent skill roots (builtins and editable skills can be separate).
     */
    skills: ContextRoot[];
    /**
     * Replaceable filenames and enablement policy.
     */
    policy: ContextDiscoveryPolicy;
    /**
     * Explicit work and byte limits.
     */
    limits: ContextDiscoveryLimits;
}

/**
 * Pinned resource allocation.
 */
export interface Reservation {
    /**
     * Provider-owned stable lease identity.
     */
    id: string;
    /**
     * Selected worker/placement identity.
     */
    placement: string;
    /**
     * Resources actually admitted; partial admission remains observable.
     */
    admitted: ResourceRequest;
}

/**
 * Placement of source messages relative to existing context.
 */
export type ContextPlacement = "prepend" | "append";

/**
 * Provider-owned additive upper bounds for an exact prepared request.
 * Counters must include structured content, native media and provider framing.
 * The SDK supplies no tokenizer or model-name capacity catalog.
 */
export interface ModelTokenCount {
    /**
     * Binds every count to the exact canonical model request.
     */
    request_digest: number[];
    /**
     * Upper bound for tools, options and request framing independent of messages.
     */
    fixed_tokens: number;
    /**
     * Ordered per-message upper bounds, including message-specific framing.
     * The provider must make these safe for ordered subsequences of this request.
     */
    message_tokens: number[];
}

/**
 * Public Rust projection for the immutable workflow admission envelope.
 *
 * The validator still consumes the canonical `WorkflowAdmission`; this DTO
 * only gives the generated TypeScript surface the exact serde shape and
 * bigint treatment used by the Rust value.
 */
export interface WasmWorkflowAdmissionWire {
    operation_id: string;
    request_digest: readonly number[];
    initial: Readonly<{ readonly machine: WasmMachineIdentityWire; readonly revision: bigint; readonly state: unknown }>;
}

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
 * Pull worker capacity and placement identity.
 */
export interface Worker {
    /**
     * Stable worker identity.
     */
    id: string;
    /**
     * Currently available logical capacity.
     */
    available: ResourceSnapshot;
    /**
     * Labels available for exact-match operation placement.
     */
    labels?: Record<string, string>;
}

/**
 * Refresh timing selected by the embedding admitted workflow.
 */
export type ContextReloadPolicy = "explicit" | "next_request";

/**
 * Remote JSON Schema contract returned by `tools/list`.
 */
export interface McpToolDefinition {
    /**
     * Server-local name, preserved independently of the Harness namespace.
     */
    name: string;
    /**
     * Optional human-facing title.
     */
    title?: string;
    /**
     * Description supplied by the server.
     */
    description?: string;
    /**
     * Runtime-validated arguments.
     */
    inputSchema: WasmToolJsonSchema;
    /**
     * Runtime-validated structured output when supplied by the server.
     */
    outputSchema?: WasmToolJsonSchema;
}

/**
 * Rendering mode for the same immutable source state.
 */
export type ContextRenderMode = "prompt" | "update";

/**
 * Replaceable discovery policy. Empty lists disable either discovery independently.
 */
export interface ContextDiscoveryPolicy {
    /**
     * Instruction filenames, evaluated in this order at each ancestor scope.
     */
    instruction_names: string[];
    /**
     * Skill entry filename. `None` disables skill discovery.
     */
    skill_name: string | undefined;
}

/**
 * Repository instruction scope, evaluated from the declared root to the active directory.
 */
export interface InstructionScope {
    /**
     * Declared repository boundary.
     */
    root: ContextRoot;
    /**
     * Directory relative to that boundary; empty selects only root instructions.
     */
    active_directory: string;
}

/**
 * Resume hint for one finite coordinator discovery sweep. It conveys no
 * execution authority and can be serialized by the caller across restarts.
 */
export interface TaskWakeCursor {
    /**
     * Last inspected coordinator revision.
     */
    after_revision: bigint;
    /**
     * Fixed inclusive revision at which this sweep ends.
     */
    through_revision: bigint;
}

/**
 * Stable identity of one independently ordered history.
 */
export interface Authority {
    /**
     * Aggregate kind.
     */
    kind: AggregateKind;
    /**
     * Provider-independent identity.
     */
    id: string;
}

/**
 * Stable parent link and child slot.
 */
export interface ParentLink {
    /**
     * Parent operation.
     */
    operation_id: string;
    /**
     * Stable logical slot, independent of observation order.
     */
    slot: string;
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
 * Terminal outcome of admitted work.
 */
export type Outcome<T> = { Succeeded: T } | { Failed: { message: string } } | "Cancelled" | { Indeterminate: { operation_id: string } };

/**
 * The only operations this one-shot client can admit.
 */
export type McpStdioMethod = "tools/list" | "tools/call";

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

/**
 * Versioned resumable entrypoint for durable work.
 */
export interface EntrypointRef {
    /**
     * Stable namespaced entrypoint name.
     */
    name: string;
    /**
     * Semantic implementation version.
     */
    version: string;
    /**
     * Digest of the state/input schema and implementation contract.
     */
    digest: number[];
    /**
     * JSON Schema for the durable result value.
     */
    result_schema: WasmModelJsonSchema;
}

/**
 * Work atomically claimed from the coordinator.
 */
export interface WorkLease {
    /**
     * Complete immutable operation declaration.
     */
    operation: OperationSpec;
    /**
     * Pinned execution allocation.
     */
    reservation: Reservation;
    /**
     * Latest durable resumable checkpoint, if any.
     */
    checkpoint: (WasmResourceRefWire & { kind: 'checkpoint' }) | null;
    /**
     * Coordinator-observed revision associated with this reservation. Recovery
     * may observe progress since the reservation's initial admission.
     */
    operation_revision: bigint;
}

export interface ContextDirectoryQuery {
    root: ContextRoot;
    path: string;
    expected_generation: (WasmResourceRefWire & { kind: 'generation' }) | null;
    after: string | undefined;
    maximum_entries: number;
}

export interface ContextPathResult {
    file: WasmFileRefWire;
    bytes: Uint8Array;
}

export interface ContextReadQuery {
    source: PinnedContextPath;
    maximum_bytes: bigint;
}

export interface McpBrowserResponseHead {
    status: number;
    headers: Record<string,string>;
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

export interface WasmBrowserSessionLimits {
    active_tasks: bigint;
    total_tasks: bigint;
    depth: number;
    model_steps: bigint;
}

export interface WasmBrowserStreamLimits {
    commands: bigint;
    journal_bytes: bigint;
    paths: number;
    path_bytes: number;
    records: number;
    payload_bytes: number;
    commits: number;
    idempotency_results: number;
}

export interface WasmBrowserTaskOptions {
    filesystem_database: string;
    stream_database: string;
    maximum_object_bytes: bigint;
    stream_limits: WasmBrowserStreamLimits;
    filesystem_provider: WasmProviderRefWire;
    volume: WasmVolumeRefWire;
    limits: WasmLimitsInput;
    run_limits: WasmTaskRunLimitsInput;
    session_limits: WasmBrowserSessionLimits;
    concurrency: number;
    maximum_payload_bytes: bigint;
}

export interface WasmBrowserTick {
    wake: WasmBrowserWake;
    work: WasmBrowserWork | null;
}

export interface WasmBrowserWake {
    cursor: TaskWakeCursor | null;
    through_revision: bigint;
    events_read: number;
    tasks_polled: number;
    woken: string[];
}

export interface WasmBrowserWorkError {
    code: number;
    message: string;
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

export interface WasmMachineDefinition {
    name: string;
    version: string;
    digest: number[];
    state_schema: WasmModelJsonSchema;
    input_schema: WasmModelJsonSchema;
    output_schema: WasmModelJsonSchema;
    requirements: string[];
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

export interface WasmModelRequestWire {
    model: WasmModelWire;
    messages: WasmModelMessageWire[];
    tools: WasmModelToolDefinitionWire[];
    max_output_tokens: number | undefined;
}

export interface WasmModelToolDefinitionWire {
    name: string;
    revision: string;
    description: string;
    input_schema: WasmModelJsonSchema;
    output_schema: WasmModelJsonSchema;
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

export type StreamErrorCode = "invalid_path" | "invalid_argument" | "limit_exceeded" | "not_found" | "already_exists" | "prefix_not_retained" | "out_of_range" | "idempotency_mismatch" | "capacity" | "access_denied" | "unavailable" | "hierarchy_changed" | "deadline_elapsed" | "unsupported";

export type WasmBrowserAdmission = { kind: "accepted"; task_id: string } | { kind: "rejected"; reason: string } | { kind: "indeterminate"; operation_id: string };

export type WasmBrowserRecoveredWork = { kind: "idle" } | { kind: "claimed"; lease: WorkLease } | { kind: "unresolved"; lease: WorkLease; error: WasmBrowserWorkError };

export type WasmBrowserWork = { kind: "unresolved"; lease: WorkLease; error: WasmBrowserWorkError } | { kind: "suspended"; task_id: string; revision: bigint } | { kind: "completed"; task_id: string } | { kind: "yielded"; lease: WorkLease } | { kind: "reconciling"; lease: WorkLease };

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
 * One Rust-backed live follow cursor.
 *
 * `next` releases the state lock before awaiting the stream, so `close` can
 * always signal a pending call and promptly release its cursor.
 */
export class WasmFollow {
    private constructor();
    free(): void;
    [Symbol.dispose](): void;
    /**
     * Cancels the cursor and wakes any pending `next` call.
     */
    close(): void;
    /**
     * Waits for one record. Returns `null` after close or stream termination.
     */
    next(): Promise<Uint8Array | null>;
}

/**
 * An observed initialization and immutable binding, with a readiness request
 * that the host must admit separately. Acceptance performs no network I/O.
 */
export class WasmMcpHttpInitialization {
    private constructor();
    free(): void;
    [Symbol.dispose](): void;
    /**
     * Returns the exact readiness notification for separate host admission.
     */
    initializedRequest(): McpHttpRequest;
    /**
     * Consumes this observation and returns its independently pinned binding.
     */
    intoTransport(): WasmMcpHttpTransport;
    /**
     * Returns the negotiated result as UTF-8 JSON, preserving remote integers.
     */
    resultJson(): Uint8Array;
}

/**
 * Low-level explicitly bound transport for a host's admitted MCP operations.
 * The existing Harness tool/task journal owns admission and result retention.
 */
export class WasmMcpHttpTransport {
    free(): void;
    [Symbol.dispose](): void;
    /**
     * Validates a host-retained response through the same native decoder.
     * The original binding remains unchanged. Check the byte allowance before
     * copying the host's body into WASM memory.
     */
    acceptInitialization(operation: string, head: McpBrowserResponseHead, body: Uint8Array): Promise<WasmMcpHttpInitialization>;
    /**
     * Calls an already admitted operation. Canonical JSON bytes preserve
     * full-width remote numbers and avoid a second JavaScript result engine.
     */
    callJson(operation: string, name: string, _arguments: WasmToolJsonValue): Promise<Uint8Array>;
    /**
     * Stages the Rust-owned initialization message without invoking a provider.
     */
    initializationRequest(operation: string, name: string, version: string): McpHttpRequest;
    /**
     * Discovers one separately admitted page. The host gathers and validates
     * the complete catalog before publishing a replacement.
     */
    listToolsJson(operation: string, cursor?: string | null): Promise<Uint8Array>;
    /**
     * Binds finite platform I/O and an optional host-provided session without effects.
     */
    constructor(provider: McpBrowserHttpProvider, endpoint: string, session: string | null | undefined, maximum_bytes: number, timeout_ms: number);
    /**
     * Queries the transport's explicit receipt capability without network I/O.
     * This HTTP provider has none; the owning journal retains known results.
     */
    reconcileJson(operation: string): Promise<Uint8Array | undefined>;
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
     * Composes the native immutable prefix after authenticating every exact
     * reference against this owner's signed scope. The host captures resident
     * bytes before entry; supplied bytes and capability strings grant nothing.
     */
    prepareInheritedModelRequest(scope: any, request: any, prefix: any, files: any, limits: any): Promise<Uint8Array>;
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
 * Browser ABI over canonical Rust memory or durable providers.
 *
 * Unary operations use `dispatch(operation, request_bytes)` and return the
 * corresponding protobuf response bytes. `read` and `children` return arrays
 * of encoded stream response messages because protobuf streams have no single
 * finite response envelope.
 */
export class WasmStream {
    free(): void;
    [Symbol.dispose](): void;
    /**
     * Lists one fixed-snapshot child page, returning encoded `ChildrenResponse` messages.
     */
    children(input: Uint8Array): Promise<Uint8Array[]>;
    /**
     * Executes one finite unary operation over canonical protobuf bytes.
     */
    dispatch(operation: string, input: Uint8Array): Promise<Uint8Array>;
    constructor();
    /**
     * Opens the canonical Rust provider on one durable `IndexedDB` journal.
     */
    static openBrowser(name: string, maximum_commands: number, maximum_journal_bytes: bigint): Promise<WasmStream>;
    /**
     * Opens a live follow cursor backed by the canonical provider.
     */
    open_follow(input: Uint8Array): Promise<WasmFollow>;
    /**
     * Reads one bounded page, returning encoded `ReadResponse` messages.
     */
    read(input: Uint8Array): Promise<Uint8Array[]>;
}

/**
 * The existing typed registries, sealed into each opened runtime. Host callbacks
 * are trusted pure machine implementations, with the same contract as Rust.
 */
export class WasmTaskRegistry {
    free(): void;
    [Symbol.dispose](): void;
    constructor();
    registerMachine(definition: WasmMachineDefinition, initialize: Function, transition: Function): void;
    /**
     * Registers the Rust stock-turn adapter over the ordinary model outbox.
     * Registration starts no worker and grants no model or task authority.
     */
    registerStockTurn(): void;
    /**
     * Callbacks share one immutable definition revision and are trusted host
     * implementations. They receive runtime-owned invocation identities.
     */
    registerTool(definition: WasmModelToolDefinitionWire, execute: Function, reconcile: Function, project: Function): void;
}

/**
 * Thin event-loop handle; scheduling, ownership, effects and recovery stay in
 * `FilesystemTaskRuntime`. No worker starts when this handle is opened.
 */
export class WasmTaskRuntime {
    private constructor();
    free(): void;
    [Symbol.dispose](): void;
    admit(operation: string, name: string, version: string, input: WasmModelJsonValue, parent?: string | null): Promise<WasmBrowserAdmission>;
    /**
     * Publishes durable cancellation without claiming terminal completion.
     */
    cancel(task: string): Promise<void>;
    /**
     * Composes an explicit provider with the stock model command executor.
     * Generate receives canonical request bytes, separate dispatch identity and
     * an `AbortSignal`, and returns an async iterator;
     * reconcile receives the exact retained attempt and never redispatches it.
     */
    configureModel(model: WasmModelWire, generate: (request: Uint8Array, dispatch: Pick<WasmModelAttemptWire, 'operation_id' | 'step' | 'request_digest'>, signal: AbortSignal) => AsyncIterator<WasmModelEvent>, reconcile: (attempt: WasmModelAttemptWire) => WasmModelEvent[] | null | Promise<WasmModelEvent[] | null>): void;
    /**
     * A page observation is not a retained model-consumption acknowledgment.
     */
    inbox(task: string, after: bigint, maximum: number): Promise<InboxItem[]>;
    /**
     * Creates only the explicitly authorized private journal volume.
     */
    initializeVolume(): Promise<void>;
    /**
     * Opens existing providers with caller-supplied limits and signed authority.
     * Volume creation and task admission remain separate explicit operations.
     */
    static open(options: WasmBrowserTaskOptions, owner: WasmReducer, registry: WasmTaskRegistry, signed_scope: any): Promise<WasmTaskRuntime>;
    /**
     * Observes only; absence does not dispatch or replay an uncertain effect.
     */
    outcome(task: string): Promise<Outcome<WasmModelJsonValue> | null>;
    reconcileAdmission(operation: string): Promise<[string, ComponentIdentity] | null>;
    /**
     * Reads the original reservation without claiming or releasing work.
     */
    recoverWork(task: string): Promise<WasmBrowserRecoveredWork>;
    /**
     * Resumes only the exact retained lease; another tick never substitutes it.
     */
    resume(lease: WorkLease, maximum_transitions: number): Promise<WasmBrowserWork>;
    /**
     * Runs only this original task through the ordinary fenced worker path.
     */
    runOperation(worker: Worker, task: string, maximum_transitions: number): Promise<WasmBrowserWork | null>;
    /**
     * Returns only after receiver-owned durable bytes are verified. A lost
     * acknowledgment permits exact redelivery; it grants no effect retry.
     */
    send(lease: WorkLease, recipient: string, message: string, payload: WasmFileRefWire): Promise<void>;
    /**
     * Stages an exact immutable command/input artifact through the ordinary store.
     */
    stage(operation: string, key: string, bytes: Uint8Array): Promise<WasmFileRefWire>;
    /**
     * Executes one caller-bounded tick using the production command resolver.
     * Missing tool/model routes stay unavailable; no fabricated default swarm.
     */
    workerTick(worker: Worker, cursor: TaskWakeCursor | null, maximum_events: number, maximum_transitions: number): Promise<WasmBrowserTick>;
}

/**
 * Type-only bridge for the complete Rust-owned Stream error-code contract.
 */
export function __streamErrorCodeContract(value: StreamErrorCode): StreamErrorCode;

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
 * Applies a host renderer's output using the same native placement and bounds.
 * Hosts supply authorized reads; this call performs no I/O or model admission.
 */
export function applyContextProjection(context: Context, messages: readonly WasmModelMessageInput[], mode: ContextRenderMode, placement: ContextPlacement, limits: WasmModelLimitsInput): Context;

/**
 * Derives the same immutable per-slot operation as Rust durable admission.
 */
export function batchMemberOperationId(group: string, batch: string, index: number): string;

/**
 * Caller admits refresh through its ordinary workflow; this function starts no watcher.
 */
export function captureDiscoveredContext(declaration: ContextDiscovery, reader: ContextDiscoveryReader): Promise<DiscoveredContext>;

/**
 * Select an immutable revision using Rust's explicit or next-request refresh policy.
 */
export function contextForRequest(declaration: ContextDiscovery, reader: ContextDiscoveryReader, current: DiscoveredContext, reload: ContextReloadPolicy): Promise<DiscoveredContext>;

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
 * Validate and project one hosted HTTP JSON success response into the public
 * JavaScript shape. Rust owns the scalar widths and tagged response schema:
 * decimal uint64 strings become `bigint`, base64 bytes become `Uint8Array`,
 * and token timestamps become `Date` values before the value crosses the
 * browser boundary.
 */
export function decodeHttpResponse(route: string, response_json: string): unknown;

/**
 * Parses external JSON with exact integers but without demanding canonical
 * key order or whitespace. Callers must still apply their schema and numeric
 * range policy before presenting model-authored values to an executor.
 */
export function decodeJson(bytes: Uint8Array): any;

/**
 * Decode one `ReadResponse` frame's fields to its encoded `RecordBatch`.
 *
 * The shared Rust decoder bounds the declared length before decompressing and
 * requires the decoded length to match it exactly.
 */
export function decodeReadResponse(codec: number, data: Uint8Array, decoded_length: bigint): Uint8Array;

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
 * Encode one protobuf request into the hosted Stream HTTP JSON shape.
 *
 * Protobuf remains the only request contract crossing from TypeScript into
 * Rust.  Rust owns the conversion of uint64 values and opaque bytes to the
 * decimal and base64 spellings required by the hosted API, keeping the HTTP
 * adapter from maintaining a second scalar conversion table.
 */
export function encodeHttpRequest(route: string, input: Uint8Array): string;

/**
 * Creates a direct-parent segment from the exact admitted request bytes.
 */
export function encodeModelPrefix(request: Uint8Array, parent: any, parent_request: Uint8Array | null | undefined, limits: any): Uint8Array;

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
 * Return whether a code can be emitted by this WASM adapter.
 *
 * Keeping this validator beside the Rust error mapping prevents the TypeScript adapter from
 * maintaining a second, potentially stale list of base Stream error codes.
 */
export function is_stream_error_code(value: string): boolean;

/**
 * Projects the host-selected schema exposure through the ordinary model wire.
 */
export function mcpModelDefinitions(catalog: McpCatalog, maximum_tools: number, maximum_bytes: number): WasmModelToolDefinitionWire[];

/**
 * Binds the exact native request without rounding its filesystem work counters
 * through JavaScript numbers. This performs no native dispatch or path lookup.
 */
export function nativeProcessApprovalDigest(task: string, command: string, request_json: string): Uint8Array;

/**
 * Normalize and encode canonical protobuf bytes for one commit request.
 *
 * The returned bytes use the same deterministic ordering as the in-memory
 * provider. Validation failures are thrown as stable error codes.
 */
export function normalizeCommitRequest(input: Uint8Array): Uint8Array;

/**
 * Parses a bounded frontmatter prefix without fetching or interpreting a skill body.
 */
export function parseSkillMetadata(prefix: Uint8Array, source: PinnedContextPath): SkillMetadata;

/**
 * Plans one deterministic conversation turn before any model or content
 * callback runs.  The reducer state and payload checks are shared with the
 * native filesystem memory host; JavaScript retains ownership of asynchronous
 * reads and model dispatch after this plan is committed.
 */
export function prepareConversationTurn(conversation: any, operation_id: string, content: any, attachments: any, limits: any, existing_selection: any, has_completed_output: boolean, can_reconcile: boolean): WasmTurnPreparation;

/**
 * Constructs the same canonical request bytes used by native providers.
 */
export function prepareModelRequest(request: any, limits: any): Uint8Array;

/**
 * Projects a discovered revision using the native messages, placement and bounds.
 * Discovery/body reads and their authority remain in ordinary provider bindings.
 */
export function projectDiscoveredContext(snapshot: DiscoveredContext, context: Context, placement: ContextPlacement, limits: WasmModelLimitsInput): Context;

/**
 * Decode one unary memory-provider response from canonical protobuf bytes
 * into the public JavaScript result shape. Rust owns the response oneofs,
 * scalar widths, copied byte buffers, and camelCase projection at this
 * boundary; TypeScript keeps only request adaptation and cursor lifecycle.
 */
export function projectMemoryResponse(operation: string, input: Uint8Array): unknown;

/**
 * Project a hosted HTTP error code onto the public Stream error vocabulary.
 *
 * The hosted API may report either the Rust-owned wire code or a public alias.
 * Unknown values and a commit-only alias on another route return no value.
 */
export function publicHttpErrorCode(raw: string, route: string): string | undefined;

/**
 * Ordinary bounded owner read of a discovered body; no model projection or execution is implied.
 */
export function readPinnedContextPath(source: PinnedContextPath, reader: ContextDiscoveryReader, maximum_bytes: number): Promise<ContextPathResult>;

/**
 * Searches a pinned catalog without network I/O or authority changes.
 */
export function searchMcpCatalog(catalog: McpCatalog, query: string, after: string | null | undefined, maximum_results: number, maximum_tools: number, maximum_bytes: number): WasmModelToolDefinitionWire[];

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
 * Validate canonical protobuf bytes for one append request.
 *
 * The empty string means that the request passed the same domain validators as
 * the in-memory provider. Otherwise this returns one stable error code.
 */
export function validateAppendRequest(input: Uint8Array): string;

/**
 * Validates one aggregate identity with the same path-segment policy used by
 * every Rust stream access. The returned spelling is unchanged so hosts can
 * retain their branded string facade without reimplementing the policy.
 */
export function validateAuthorityPathSegment(value: string): string;

/**
 * Validates one component name with the canonical Rust byte and character
 * policy. The field-specific error text remains a thin TypeScript concern.
 */
export function validateComponentLabel(value: string): string;

/**
 * Validates a host-approved pinned selection without granting read authority.
 */
export function validateContextSelection(selection: ContextSelection, limits: WasmModelLimitsInput): void;

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
 * Validate one hosted HTTP JSON success response using the same path, width,
 * identity, and tagged-union rules as the canonical Stream domain.
 *
 * The HTTP adapter keeps its intentionally simple JSON representation (u64
 * values are decimal strings and opaque bytes are base64). This entry point
 * validates that representation using the same Rust projection used by
 * `decodeHttpResponse`, without crossing a second scalar schema boundary.
 */
export function validateHttpResponse(route: string, response_json: string): void;

/**
 * Parses one public Harness identity with the canonical Rust contract and
 * returns its normalized UUID spelling for a branded TypeScript facade.
 */
export function validateIdentity(kind: string, value: string): string;

/**
 * Validates the complete bounded replacement before the host selects it.
 */
export function validateMcpCatalog(catalog: McpCatalog, maximum_tools: number, maximum_bytes: number): void;

/**
 * Validates an exact stdio descriptor without selecting a native provider.
 */
export function validateMcpStdioRequest(request: McpStdioRequest): void;

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
 * Validates a complete provider-neutral model message list with the native
 * role, message-count, and content bounds.  Context builders and the stock
 * TypeScript loop therefore share the same closed role set and limits as
 * native durable execution.
 */
export function validateModelMessages(messages: readonly WasmModelMessageInput[], limits: WasmModelLimitsInput): void;

/**
 * Validate one canonical Stream path using the same parser used by every
 * provider and wire decoder.
 *
 * The empty string means success; failures use the stable Stream error code
 * consumed by the TypeScript adapter.
 */
export function validatePath(path: string): string;

/**
 * Validate one canonical protobuf request at the browser boundary.
 *
 * `kind` is deliberately a small closed set so callers cannot accidentally
 * select a different validator after adding a new wire message. The empty
 * string means success; failures use the same stable codes as the append and
 * commit entry points.
 */
export function validateRequest(kind: string, input: Uint8Array): string;

/**
 * Admits an already projected, provider-proven context with native model bounds.
 */
export function validateSelectedModelContext(selected: any, limits: any): void;

/**
 * Validate one JavaScript representation of a canonical Stream sequence.
 *
 * JavaScript passes the decimal spelling of its `bigint`; Rust owns the
 * unsigned 64-bit range accepted by every Stream wire field.
 */
export function validateSequence(value: string): string;

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
 * Validates and projects one immutable workflow admission through Rust.
 * Hosts retain this detached value before any replay or dispatch begins.
 */
export function validateWorkflowAdmission(value: WasmWorkflowAdmissionWire): WasmWorkflowAdmissionWire;

/**
 * Checks immutable file identity without constructing a reducer or issuer.
 */
export function verifyFileBytes(file: any, bytes: Uint8Array): void;

export type InitInput = RequestInfo | URL | Response | BufferSource | WebAssembly.Module;

export interface InitOutput {
    readonly memory: WebAssembly.Memory;
    readonly __wbg_wasmcontentstore_free: (a: number, b: number) => void;
    readonly __wbg_wasmmcphttpinitialization_free: (a: number, b: number) => void;
    readonly __wbg_wasmmcphttptransport_free: (a: number, b: number) => void;
    readonly __wbg_wasmreducer_free: (a: number, b: number) => void;
    readonly __wbg_wasmtaskregistry_free: (a: number, b: number) => void;
    readonly __wbg_wasmtaskruntime_free: (a: number, b: number) => void;
    readonly admitBatch: (a: any) => [number, number, number];
    readonly admitBatchRequest: (a: any) => [number, number, number];
    readonly admitModelEvent: (a: any, b: any, c: any) => [number, number, number];
    readonly admitTask: (a: any) => [number, number, number];
    readonly applyContextProjection: (a: any, b: any, c: any, d: any, e: any) => [number, number, number];
    readonly batchMemberOperationId: (a: number, b: number, c: number, d: number, e: number) => [number, number, number, number];
    readonly captureDiscoveredContext: (a: any, b: any) => any;
    readonly contextForRequest: (a: any, b: any, c: any, d: any) => any;
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
    readonly encodeModelPrefix: (a: number, b: number, c: any, d: number, e: number, f: any) => [number, number, number, number];
    readonly fileDescriptor: (a: number, b: number, c: number, d: number) => [number, number, number];
    readonly forkSeedFromReport: (a: any) => [number, number, number];
    readonly mcpModelDefinitions: (a: any, b: any, c: any) => [number, number, number];
    readonly nativeProcessApprovalDigest: (a: number, b: number, c: number, d: number, e: number, f: number) => [number, number, number, number];
    readonly parseSkillMetadata: (a: number, b: number, c: any) => [number, number, number];
    readonly prepareConversationTurn: (a: any, b: number, c: number, d: any, e: any, f: any, g: any, h: number, i: number) => [number, number, number];
    readonly prepareModelRequest: (a: any, b: any) => [number, number, number, number];
    readonly projectDiscoveredContext: (a: any, b: any, c: any, d: any) => [number, number, number];
    readonly readPinnedContextPath: (a: any, b: any, c: number) => any;
    readonly searchMcpCatalog: (a: any, b: number, c: number, d: number, e: number, f: any, g: any, h: any) => [number, number, number];
    readonly selectModelContext: (a: any, b: any, c: any, d: number, e: number, f: number, g: number) => any;
    readonly taskAdmissionIdentities: (a: any) => [number, number, number];
    readonly taskIdentityDigest: (a: number, b: number, c: number, d: number, e: any, f: any, g: any, h: number, i: number) => [number, number, number, number];
    readonly uuidFromDigestHalf: (a: number, b: number, c: number) => [number, number, number, number];
    readonly validateAuthorityPathSegment: (a: any) => [number, number, number, number];
    readonly validateComponentLabel: (a: any) => [number, number, number, number];
    readonly validateContextSelection: (a: any, b: any) => [number, number];
    readonly validateContract: (a: number, b: number, c: any, d: any) => [number, number, number];
    readonly validateConversationMessageId: (a: number, b: number) => [number, number, number, number];
    readonly validateIdentity: (a: number, b: number, c: number, d: number) => [number, number, number, number];
    readonly validateMcpCatalog: (a: any, b: any, c: any) => [number, number];
    readonly validateMcpStdioRequest: (a: any) => [number, number];
    readonly validateModelContent: (a: any, b: any) => [number, number];
    readonly validateModelContextSelection: (a: any, b: any) => [number, number];
    readonly validateModelMessages: (a: any, b: any) => [number, number];
    readonly validateSelectedModelContext: (a: any, b: any) => [number, number];
    readonly validateTaskChildrenPage: (a: any) => [number, number, number];
    readonly validateTaskRequirements: (a: any) => [number, number];
    readonly validateToolDefinition: (a: any) => [number, number];
    readonly validateToolInvocation: (a: any, b: any) => [number, number];
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
    readonly validateWorkflowAdmission: (a: any) => [number, number, number];
    readonly verifyFileBytes: (a: any, b: number, c: number) => [number, number];
    readonly wasmcontentstore_generation: (a: number) => [number, number, number];
    readonly wasmcontentstore_has: (a: number, b: any) => [number, number, number];
    readonly wasmcontentstore_list: (a: number, b: number, c: number, d: any, e: number, f: number, g: number) => [number, number, number];
    readonly wasmcontentstore_new: (a: any, b: number, c: number, d: number, e: number) => [number, number, number];
    readonly wasmcontentstore_pathConflicts: (a: number, b: number, c: number) => number;
    readonly wasmcontentstore_read: (a: number, b: any) => [number, number, number, number];
    readonly wasmcontentstore_read_path: (a: number, b: number, c: number, d: any) => [number, number, number];
    readonly wasmcontentstore_stage: (a: number, b: number, c: number, d: number, e: number, f: number, g: number, h: number, i: number, j: number) => [number, number, number];
    readonly wasmmcphttpinitialization_initializedRequest: (a: number) => [number, number, number];
    readonly wasmmcphttpinitialization_intoTransport: (a: number) => number;
    readonly wasmmcphttpinitialization_resultJson: (a: number) => [number, number, number, number];
    readonly wasmmcphttptransport_acceptInitialization: (a: number, b: number, c: number, d: any, e: any) => any;
    readonly wasmmcphttptransport_callJson: (a: number, b: number, c: number, d: number, e: number, f: any) => any;
    readonly wasmmcphttptransport_initializationRequest: (a: number, b: number, c: number, d: number, e: number, f: number, g: number) => [number, number, number];
    readonly wasmmcphttptransport_listToolsJson: (a: number, b: number, c: number, d: number, e: number) => any;
    readonly wasmmcphttptransport_new: (a: any, b: number, c: number, d: number, e: number, f: any, g: any) => [number, number, number];
    readonly wasmmcphttptransport_reconcileJson: (a: number, b: number, c: number) => any;
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
    readonly wasmreducer_prepareInheritedModelRequest: (a: number, b: any, c: any, d: any, e: any, f: any) => any;
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
    readonly wasmtaskregistry_new: () => number;
    readonly wasmtaskregistry_registerMachine: (a: number, b: any, c: any, d: any) => [number, number];
    readonly wasmtaskregistry_registerStockTurn: (a: number) => [number, number];
    readonly wasmtaskregistry_registerTool: (a: number, b: any, c: any, d: any, e: any) => [number, number];
    readonly wasmtaskruntime_admit: (a: number, b: number, c: number, d: number, e: number, f: number, g: number, h: any, i: number, j: number) => any;
    readonly wasmtaskruntime_cancel: (a: number, b: number, c: number) => any;
    readonly wasmtaskruntime_configureModel: (a: number, b: any, c: any, d: any) => [number, number];
    readonly wasmtaskruntime_inbox: (a: number, b: number, c: number, d: bigint, e: number) => any;
    readonly wasmtaskruntime_initializeVolume: (a: number) => any;
    readonly wasmtaskruntime_open: (a: any, b: number, c: number, d: any) => any;
    readonly wasmtaskruntime_outcome: (a: number, b: number, c: number) => any;
    readonly wasmtaskruntime_reconcileAdmission: (a: number, b: number, c: number) => any;
    readonly wasmtaskruntime_recoverWork: (a: number, b: number, c: number) => any;
    readonly wasmtaskruntime_resume: (a: number, b: any, c: number) => any;
    readonly wasmtaskruntime_runOperation: (a: number, b: any, c: number, d: number, e: number) => any;
    readonly wasmtaskruntime_send: (a: number, b: any, c: number, d: number, e: number, f: number, g: any) => any;
    readonly wasmtaskruntime_stage: (a: number, b: number, c: number, d: number, e: number, f: number, g: number) => any;
    readonly wasmtaskruntime_workerTick: (a: number, b: any, c: any, d: number, e: number) => any;
    readonly __streamErrorCodeContract: (a: any) => any;
    readonly __wbg_wasmfollow_free: (a: number, b: number) => void;
    readonly __wbg_wasmstream_free: (a: number, b: number) => void;
    readonly decodeHttpResponse: (a: number, b: number, c: number, d: number) => [number, number, number];
    readonly decodeReadResponse: (a: number, b: number, c: number, d: bigint) => [number, number, number, number];
    readonly encodeHttpRequest: (a: number, b: number, c: number, d: number) => [number, number, number, number];
    readonly is_stream_error_code: (a: number, b: number) => number;
    readonly normalizeCommitRequest: (a: number, b: number) => [number, number, number, number];
    readonly projectMemoryResponse: (a: number, b: number, c: number, d: number) => [number, number, number];
    readonly publicHttpErrorCode: (a: number, b: number, c: number, d: number) => [number, number];
    readonly validateAppendRequest: (a: number, b: number) => [number, number];
    readonly validateHttpResponse: (a: number, b: number, c: number, d: number) => [number, number];
    readonly validatePath: (a: number, b: number) => [number, number];
    readonly validateRequest: (a: number, b: number, c: number, d: number) => [number, number];
    readonly validateSequence: (a: number, b: number) => [number, number];
    readonly wasmfollow_close: (a: number) => void;
    readonly wasmfollow_next: (a: number) => any;
    readonly wasmstream_children: (a: number, b: number, c: number) => any;
    readonly wasmstream_dispatch: (a: number, b: number, c: number, d: number, e: number) => any;
    readonly wasmstream_new: () => number;
    readonly wasmstream_openBrowser: (a: number, b: number, c: number, d: bigint) => any;
    readonly wasmstream_open_follow: (a: number, b: number, c: number) => any;
    readonly wasmstream_read: (a: number, b: number, c: number) => any;
    readonly __wbindgen_malloc: (a: number, b: number) => number;
    readonly __wbindgen_realloc: (a: number, b: number, c: number, d: number) => number;
    readonly __wbindgen_exn_store: (a: number) => void;
    readonly __externref_table_alloc: () => number;
    readonly __wbindgen_externrefs: WebAssembly.Table;
    readonly __wbindgen_destroy_closure: (a: number, b: number) => void;
    readonly __wbindgen_free: (a: number, b: number, c: number) => void;
    readonly __externref_table_dealloc: (a: number) => void;
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
