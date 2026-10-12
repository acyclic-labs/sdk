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

/**
 * Bounded immutable remote catalog.
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
     * Host-selected schema for complete model-facing result envelopes.
     */
    projection_schema: WasmToolJsonSchema;
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
 * Canonical executor observation suitable for a durable journal.
 */
export type ExecutionEvent = { kind: "started"; request_digest: number[] } | { kind: "context_prepared"; step: number; projection: WasmFileRefWire; accounting: WasmFileRefWire | null } | { kind: "context_compacted"; step: number; projection: WasmFileRefWire; compaction: WasmFileRefWire; accounting: WasmFileRefWire } | { kind: "model_started"; step: number; purpose: ModelPurpose; request_digest: number[]; request: WasmFileRefWire } | { kind: "model"; step: number; purpose: ModelPurpose; event: WasmFileRefWire } | { kind: "tool_started"; step: number; call_id: string; invocation: WasmFileRefWire } | { kind: "tool_completed"; step: number; call_id: string; result: WasmFileRefWire; projection: WasmFileRefWire } | { kind: "tool_failed"; step: number; call_id: string; reason: ToolFailureKind };

/**
 * Canonical executor observation suitable for a durable journal.
 */
export type ModelPurpose = "response" | "summary";

/**
 * Canonical successful write/edit result, independent of model projection.
 */
export interface FileResult {
    /**
     * Exact retained publication result with workspace and content identity.
     */
    file: WasmFileRefWire;
}

/**
 * Claim to verify against original task admission and linked implementation.
 */
export interface NativeConfigurationBinding {
    /**
     * Original immutable extension admission event.
     */
    source: WasmEventReferenceWire;
    /**
     * Exact registered option schema and immutable JSON file.
     */
    configuration: WasmExtensionConfigurationWire;
    /**
     * Original owner-linked executable implementation.
     */
    implementation_digest: readonly number[];
}

/**
 * Closed provider-neutral role set; adapters may reject unsupported combinations.
 */
export type ModelRole = "system" | "user" | "assistant" | "tool";

/**
 * Complete declarative inventory, without granting file or option authority.
 */
export interface WasmModelContentInventoryWire {
    files: readonly WasmFileRefWire[];
    nativeConfigurations: readonly NativeConfigurationBinding[];
}

/**
 * Complete immutable request to a model provider.
 */
export interface ModelRequest {
    /**
     * Selected model value.
     */
    model: Model;
    /**
     * Ordered reconstructable context.
     */
    messages: ModelMessage[];
    /**
     * Tools visible for this request.
     */
    tools: ToolDefinition[];
    /**
     * Maximum output tokens when constrained.
     */
    max_output_tokens: number | undefined;
}

/**
 * Complete literal-search accounting with bounded retained match positions.
 */
export interface SearchMatches {
    /**
     * Ordered, possibly overlapping exact UTF-8 match intervals.
     */
    matches: TextRange[];
    /**
     * All matches in the verified source, including omitted intervals.
     */
    total_matches: number;
    /**
     * Exact count of additional matches excluded from the retained result.
     */
    omitted_matches: number;
    /**
     * Individual byte comparisons actually performed.
     */
    work: number;
}

/**
 * Consumer-declared content that compaction must preserve verbatim.
 */
export interface CompactionRetention {
    /**
     * Roles retained in full. Defaults to instructions (`System`).
     */
    roles: ModelRole[];
    /**
     * Retains messages containing native media references in full.
     */
    native_media: boolean;
}

/**
 * Consumer-selected ceilings for an explicit partial read.
 */
export interface ReadOptions {
    /**
     * Maximum verified source bytes, checked before source I/O by adapters.
     */
    maximum_input_bytes: number;
    /**
     * Maximum selected UTF-8 bytes, independent of the source allowance.
     */
    maximum_text_bytes: number;
}

/**
 * Consumer-selected finite literal-search allowances.
 */
export interface SearchOptions {
    /**
     * Maximum verified source bytes, checked before I/O by adapters.
     */
    maximum_input_bytes: number;
    /**
     * Maximum nonempty literal needle bytes.
     */
    maximum_query_bytes: number;
    /**
     * Maximum individual byte comparisons across all candidates.
     */
    maximum_work: number;
    /**
     * Maximum retained match intervals; remaining matches are counted.
     */
    maximum_matches: number;
}

/**
 * Data in a tool result cannot contain another call or result.
 */
export type ModelDataPart = { kind: "text"; text: string } | { kind: "file"; file: WasmFileRefWire; policy: FileProjectionPolicy };

/**
 * Deterministic capability set.
 */
export type Capabilities = string[];

/**
 * Durable literal-search result, independent of its model projection.
 */
export interface SearchResult {
    /**
     * Original reference, including provider, volume, generation and content identity.
     */
    file: WasmFileRefWire;
    /**
     * Exact literal searched in that source.
     */
    query: string;
    /**
     * Complete match accounting with explicitly bounded retained positions.
     */
    matches: SearchMatches;
}

/**
 * Durable orchestration behavior represented as data.
 */
export type Orchestration = { kind: "leaf" } | { kind: "join" } | { kind: "race" } | { kind: "quorum"; required: number } | { kind: "reduce"; reducer: EntrypointRef };

/**
 * Durable read result retaining exact source identity and explicit omissions.
 */
export interface ReadResult {
    /**
     * Original reference, including provider, volume, generation and content identity.
     */
    file: WasmFileRefWire;
    /**
     * Exact selected bytes and omitted source intervals.
     */
    selection: TextSelection;
}

/**
 * Exact UTF-8 byte interval; offsets refer to the original immutable source.
 */
export interface TextRange {
    /**
     * Inclusive byte offset.
     */
    start: number;
    /**
     * Exclusive byte offset.
     */
    end: number;
}

/**
 * Exact child-owned Summary projection bound by the original fork seed.
 */
export interface SummaryForkCapture {
    /**
     * Original immutable checkpoint and bounded-work selection.
     */
    selection: SummaryForkSelection;
    /**
     * Canonical whole Context in the child-owned inherited namespace.
     */
    context: WasmFileRefWire;
    /**
     * Owner-controlled copies of private model payloads at their pinned generation.
     */
    payloads: WasmFileRefWire[];
    /**
     * Original noncopied references required by the captured Context.
     */
    references: WasmFileRefWire[];
}

/**
 * Exact immutable UTF-8 file read arguments.
 */
export interface ReadFileInput {
    /**
     * Owner-authorized immutable source.
     */
    file: WasmFileRefWire;
}

/**
 * Exact immutable source and byte interval supplied by the caller.
 */
export interface ReadInput {
    /**
     * Owner-authorized immutable source.
     */
    file: WasmFileRefWire;
    /**
     * Exact UTF-8 byte interval; no implicit rounding or truncation.
     */
    range: TextRange;
}

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
     * Positive cumulative byte allowance per input/output direction.
     */
    maximum_bytes: number;
}

/**
 * Exact replacement against one pinned file generation.
 */
export interface EditFileInput {
    /**
     * Owner-authorized immutable source.
     */
    file: WasmFileRefWire;
    /**
     * One exact nonempty unambiguous needle.
     */
    old_text: string;
    /**
     * Exact replacement, including an empty deletion.
     */
    new_text: string;
}

/**
 * Exact request pin retained in the existing execution journal, never a new ledger.
 */
export interface McpCallbackRequest {
    /**
     * Operation enclosing the server request.
     */
    parent: string;
    /**
     * Pinned callback admission policy.
     */
    policy: McpCallbackPolicy;
    /**
     * Complete JSON-RPC request, including opaque server ID and exact parameters.
     */
    message: WasmToolJsonValue;
}

/**
 * Executor-owned identity supplied separately from canonical model-visible bytes.
 */
export interface ModelDispatch {
    /**
     * Admitted execution operation identity.
     */
    operation_id: string;
    /**
     * Zero-based logical executor step.
     */
    step: number;
    /**
     * Digest of the exact canonical request supplied with this dispatch.
     */
    request_digest: readonly number[];
}

/**
 * Explicit bounded selection from a pinned source.
 */
export type ContextExtent = { kind: "whole" } | { kind: "span"; start: number; end: number };

/**
 * Explicit common image quality intent.
 */
export type ImageDetail = "auto" | "low" | "high";

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
 * Explicit model representation, independent of the retained canonical result.
 */
export type ProjectionMode = "full" | "reference";

/**
 * Explicit model schema exposure for an installed complete catalog.
 */
export type McpSchemaExposure = { kind: "eager" } | { kind: "selected"; names: string[] };

/**
 * Explicit native input contract; no implicit conversion or fallback.
 */
export interface NativeMediaPolicy {
    /**
     * Common modality and processing intent.
     */
    intent: NativeMediaIntent;
    /**
     * Positive finite byte ceiling, also bounded by task content limits.
     */
    maximum_bytes: number;
    /**
     * Positive finite work ceiling interpreted by the selected adapter.
     */
    maximum_work: number;
    /**
     * Optional immutable registered adapter options; never bearer authority.
     */
    configuration: NativeConfigurationBinding | null;
}

/**
 * Explicit single-file V4A update arguments.
 */
export interface PatchFileInput {
    /**
     * Owner-authorized immutable source generation.
     */
    file: WasmFileRefWire;
    /**
     * Nonempty exact V4A update diff fragment.
     */
    diff: string;
}

/**
 * File handling policy selected before provider dispatch.
 */
export type FileProjectionPolicy = "reference" | "bounded_full" | { native: NativeMediaPolicy };

/**
 * Finite work and output bounds for one history read.
 */
export interface HistoryReadLimits {
    /**
     * Maximum number of decoded events.
     */
    maximum_events: number;
    /**
     * Maximum total encoded record bytes, checked before decoding each record.
     */
    maximum_bytes: bigint;
}

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
 * Idempotent request to capture selected resources for a new child.
 */
export interface ForkRequest {
    /**
     * Stable identity used for preparation and publication reconciliation.
     */
    operation_id: string;
    /**
     * Publishing parent aggregate.
     */
    parent: Authority;
    /**
     * Exact parent revision before publication.
     */
    parent_revision: bigint;
    /**
     * Fresh child aggregate.
     */
    child: Authority;
    /**
     * Fresh child agent identity.
     */
    child_agent: string;
    /**
     * Additional agents allowed to attach to the child environment as readers.
     */
    attached_agents: readonly string[];
    /**
     * Exact child allocation and bounded inherited prefix chosen before preparation.
     */
    preparation: ForkPreparation;
    /**
     * Ordered required and optional resource selections.
     */
    selections: readonly ForkSelection[];
    /**
     * Present only when one provider attests a common capture boundary.
     */
    boundary: AttestedBoundary | undefined;
    /**
     * Operation-sealed workspace read; never grants source transcript access.
     */
    authorized_source?: ProjectSourceAuthorization;
}

/**
 * Immutable authorization scope captured at admission.
 */
export interface Scope {
    /**
     * Stable scope identity.
     */
    id: string;
    /**
     * Effective capabilities after policy intersection.
     */
    capabilities: Capabilities;
    /**
     * Host-attested agent on whose behalf this operation runs, if any.
     */
    agent: string | null;
    /**
     * Trusted issuer identity.
     */
    issuer: string;
    /**
     * Parent proof, present for attenuated scopes.
     */
    parent_proof: number[] | null;
    /**
     * Keyed proof over the complete grant.
     */
    proof: number[];
}

/**
 * Immutable checkpoint and bounded canonical work selected before fork admission.
 */
export interface SummaryForkSelection {
    /**
     * Exact checkpoint published by the original parent execution journal.
     */
    checkpoint: WasmFileRefWire;
    /**
     * Finite model projection and content bounds, narrowed by receiving scope.
     */
    limits: WasmNativeLimitsWire;
    /**
     * Shared event/encoded-byte allowance for checkpoint proof and canonical tail.
     */
    history_limits: HistoryReadLimits;
}

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
 * Immutable discovery revision.
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
     * Original signed owner grant retained with this immutable declaration.
     */
    owner_scope: Scope;
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
 * Immutable opt-in policy; bind this with the server/catalog revision.
 */
export interface McpCallbackPolicy {
    /**
     * Consumer namespace, not remote implementation name.
     */
    server: string;
    /**
     * Exact host-selected server revision.
     */
    revision: string;
    /**
     * Enable model requests under the task's ordinary model grant.
     */
    sampling: boolean;
    /**
     * Enable forms under the task's ordinary interaction grant.
     */
    elicitation: boolean;
    /**
     * Finite callback count per exchange, including replayed requests.
     */
    maximum_requests: number;
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
 * Immutable provider allocation and context bounds for one fork operation.
 */
export interface ForkPreparation {
    /**
     * Fresh project workspace derived from the selected project generation.
     */
    child_project_volume: WasmVolumeRefWire;
    /**
     * Fresh private workspace owned by the child agent.
     */
    child_private_volume: WasmVolumeRefWire;
    /**
     * Optional child-owned unmounted result namespace, distinct from scratch.
     */
    child_result_volume?: WasmVolumeRefWire | null;
    /**
     * Inclusive final parent conversation sequence; zero selects none.
     */
    inherited_through_sequence: bigint;
    /**
     * Deployment limit, no greater than the protocol ceiling.
     */
    maximum_inherited_messages: bigint;
    /**
     * Maximum bytes in the child-owned inherited-context file.
     */
    maximum_inherited_bytes: bigint;
    /**
     * Maximum retained references in the inherited prefix.
     */
    maximum_inherited_references: number;
    /**
     * Optional immutable Summary projection selected before fork admission.
     */
    summary: SummaryForkSelection | undefined;
}

/**
 * Immutable provider-owned model selection; there is no global catalog.
 */
export interface Model {
    /**
     * Provider selected by application code.
     */
    provider: string;
    /**
     * Provider-owned model name.
     */
    name: string;
    /**
     * Immutable model revision or digest.
     */
    revision: string;
    /**
     * Provider-specific options retained as typed JSON.
     */
    options: WasmModelJsonValue;
}

/**
 * JavaScript-facing tool definition shape.
 */
export interface WasmToolDefinitionInput {
    name: string;
    revision: string;
    description: string;
    inputSchema: WasmToolJsonSchema;
    outputSchema: WasmToolJsonSchema;
    projectionSchema: WasmToolJsonSchema;
}

/**
 * Kind of independently ordered durable aggregate.
 */
export type AggregateKind = "agent" | "conversation" | "session" | "turn" | "task";

/**
 * Literal search arguments; there is no implicit regex, directory or shell search.
 */
export interface SearchInput {
    /**
     * Owner-authorized immutable source.
     */
    file: WasmFileRefWire;
    /**
     * Nonempty case-sensitive UTF-8 literal, including overlapping matches.
     */
    query: string;
}

/**
 * Logical resource quantities; providers decide how they map to capacity.
 */
export type ResourceRequest = Record<string, bigint>;

/**
 * Model-visible tool definition with immutable schemas.
 */
export interface ToolDefinition {
    /**
     * Stable bounded tool name; local names and explicit namespaces are both valid.
     */
    name: string;
    /**
     * Immutable revision of definition, executor, and projection semantics.
     */
    revision: string;
    /**
     * Model-visible description.
     */
    description: string;
    /**
     * JSON Schema for invocation arguments.
     */
    input_schema: WasmModelJsonSchema;
    /**
     * JSON Schema for the canonical successful executor value.
     */
    output_schema: WasmModelJsonSchema;
    /**
     * JSON Schema for the model-facing projection, independently of the result.
     */
    projection_schema: WasmModelJsonSchema;
}

/**
 * Mutable context assembled for one model step.
 */
export interface Context {
    /**
     * Ordered model-visible messages.
     */
    messages: ModelMessage[];
    /**
     * Stage-owned, namespaced version-pinned metadata files.
     */
    metadata: Record<string, WasmFileRefWire>;
    /**
     * Position of the active turn input.
     */
    current_input_index?: number;
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
 * One bounded discovery page.
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
 * One exact retained resource revision selected for a child.
 */
export type ResourceRevision = { kind: "history"; reference: WasmResourceRefWire } | { kind: "project"; reference: { volume: WasmVolumeRefWire; generation: WasmResourceRefWire } } | { kind: "private_volume"; reference: { volume: WasmVolumeRefWire; generation: WasmResourceRefWire; paths: readonly string[] } } | { kind: "context"; reference: WasmResourceRefWire } | { kind: "process"; reference: WasmResourceRefWire } | { kind: "artifact"; reference: WasmResourceRefWire } | { kind: "shared_volume"; reference: WasmVolumeRefWire } | { kind: "extension"; reference: { name: string; version: number; implementation_digest: readonly number[]; reference: WasmResourceRefWire } };

/**
 * One explicit partial read, including the exact omitted source intervals.
 */
export interface TextSelection {
    /**
     * Selected source interval.
     */
    range: TextRange;
    /**
     * Exact selected UTF-8 with original line endings.
     */
    text: string;
    /**
     * Bytes excluded before the selection.
     */
    omitted_before: number;
    /**
     * Bytes excluded after the selection.
     */
    omitted_after: number;
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
 * One provider-neutral part requiring an explicit provider projection.
 */
export type ModelContentPart = { kind: "text"; text: string } | { kind: "file"; file: WasmFileRefWire; policy: FileProjectionPolicy } | { kind: "tool_call"; call_id: string; name: string; arguments: WasmModelJsonValue } | { kind: "tool_result"; call_id: string; name: string; content: ToolResultContent };

/**
 * One provider-neutral prompt item.
 */
export interface ModelMessage {
    /**
     * Semantic role (`system`, `user`, `assistant`, or `tool`).
     */
    role: ModelRole;
    /**
     * Typed, provider-neutral content; file bytes are never part of this value.
     */
    content: ModelContent;
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
 * Ordered event emitted by a model run.
 */
export type ModelEvent = { kind: "content"; delta: string } | { kind: "reasoning"; delta: string } | { kind: "tool_call"; call_id: string; name: string; arguments: WasmModelJsonValue } | { kind: "completed"; metadata: WasmModelJsonValue };

/**
 * Ordinary configurable threshold compaction; no model-family heuristic.
 */
export interface ThresholdCompaction {
    /**
     * Input headroom retained for each response.
     */
    response_reserve_tokens: number;
    /**
     * Minimum recent suffix token budget retained alongside mandatory content.
     */
    recent_tokens: number;
    /**
     * Consumer-selected roles and media that remain verbatim.
     */
    retention: CompactionRetention;
}

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
 * Original immutable project/scratch snapshot proved by a retained allocation.
 */
export interface TaskVolumeSnapshot {
    /**
     * Exact provider/owner volume identity authenticated by the creation receipt.
     */
    volume: WasmVolumeRefWire;
    /**
     * Original allocation generation; later source edits require a new selection.
     */
    generation: WasmResourceRefWire;
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
 * Portable fork arguments.
 */
export interface ForkToolArguments {
    /**
     * Chosen new child agent identity.
     */
    id: string;
    /**
     * Exact initial child prompt.
     */
    msg: string;
    /**
     * Explicit workspace source task; absence selects the current calling task.
     */
    source?: string | undefined;
}

/**
 * Provider evidence of a consistent boundary across selected resources.
 */
export interface AttestedBoundary {
    /**
     * Provider qualified to attest this boundary.
     */
    provider: WasmProviderRefWire;
    /**
     * Opaque bounded proof, interpreted only by that provider.
     */
    evidence: readonly number[];
}

/**
 * Provider-owned additive upper bounds for an exact prepared request.
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
     */
    message_tokens: number[];
}

/**
 * Public Rust projection for the immutable workflow admission envelope.
 */
export interface WasmWorkflowAdmissionWire {
    operation_id: string;
    request_digest: readonly number[];
    initial: Readonly<{ readonly machine: WasmMachineIdentityWire; readonly revision: bigint; readonly state: unknown }>;
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
    available: ResourceRequest;
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
 * Replaceable primitive policy.
 */
export type CompactionPolicy = { kind: "disabled" } | { kind: "threshold"; config: ThresholdCompaction };

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
 * Requested modality is independent of the stored file's MIME type.
 */
export type NativeMediaIntent = { kind: "image"; detail: ImageDetail } | { kind: "audio"; maximum_duration_ms: number } | { kind: "video"; maximum_duration_ms: number; maximum_frames: number } | { kind: "document"; maximum_pages: number };

/**
 * Required or optional exact resource selection.
 */
export interface ForkSelection {
    /**
     * Required selections must capture successfully before publication.
     */
    required: boolean;
    /**
     * Exact source revision.
     */
    revision: ResourceRevision;
}

/**
 * Resume hint for one finite coordinator discovery sweep.
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
 * Selection of authoritative logical history before a fork is admitted.
 */
export type ForkHistoryPolicy = "fork" | "fresh" | { summary: SummaryForkSelection };

/**
 * Single-operation UTF-8 publication arguments.
 */
export interface WriteFileInput {
    /**
     * Valid public destination path.
     */
    path: string;
    /**
     * Exact content bytes encoded as UTF-8.
     */
    text: string;
    /**
     * Stored MIME type.
     */
    media_type: string;
    /**
     * Human-readable file name.
     */
    display_name: string;
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
 * Stable, non-secret terminal tool failure classes.
 */
export type ToolFailureKind = "executor_rejected" | "invalid_output" | "projection_rejected" | "publication_rejected";

/**
 * Terminal outcome of admitted work.
 */
export type Outcome<T> = { Succeeded: T } | { Failed: { message: string } } | "Cancelled" | { Indeterminate: { operation_id: string } };

/**
 * The complete model-visible projection envelope.
 */
export type ToolResultContent = { kind: "json"; value: WasmModelJsonValue } | { kind: "parts"; parts: ModelDataPart[] };

/**
 * The only operations this one-shot client can admit.
 */
export type McpStdioMethod = "tools/list" | "tools/call" | "resources/list" | "resources/templates/list" | "resources/read" | "prompts/list" | "prompts/get";

/**
 * Transient model-visible content. Canonical conversation storage always retains refs.
 */
export type ModelContent = string | ModelContentPart | ModelContentPart[];

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
     * Coordinator-observed revision associated with this reservation.
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
    resource_proofs: readonly WasmResourceCreationProofRefWire[];
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

export interface WasmModelEventAdmission {
    event: ModelEvent;
    state: WasmModelEventAdmissionState;
}

export interface WasmModelEventAdmissionState {
    count: number;
    calls: string[];
    completed: boolean;
    text_bytes: number;
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
    resource_proofs: readonly WasmResourceCreationProofRefWire[];
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


/**
 * Owner-bound publication through the existing aggregate, retry index and CAS.
 */
export class WasmBrowserAggregate {
    private constructor();
    free(): void;
    [Symbol.dispose](): void;
    /**
     * Publishes the event and archived-operation location in one existing commit.
     */
    execute(command: any): Promise<any>;
    /**
     * Reads the command head without serializing retained history.
     */
    head(): any;
    /**
     * Binds an archival reader to this exact provider and owner verifier.
     */
    historyReader(): WasmBrowserHistoryReader;
    /**
     * Opens canonical history, optionally from an authenticated checkpoint.
     */
    static openBrowser(options: any, schemas: any, snapshot?: any | null): Promise<WasmBrowserAggregate>;
    /**
     * Reconciles only the exact admitted command; no replacement dispatch is authored.
     */
    reconcile(command: any): Promise<any>;
    /**
     * Advances the projection through at most one caller-bounded page.
     */
    refreshThrough(revision: bigint, maximum_events: number): Promise<boolean>;
    /**
     * Captures the existing authenticated reducer projection.
     */
    snapshot(): any;
}

/**
 * An explicit archival reader bound to one owning browser Stream and issuer.
 */
export class WasmBrowserHistoryReader {
    private constructor();
    free(): void;
    [Symbol.dispose](): void;
    /**
     * Reads the same atomic logical-message head without constructing a reducer.
     */
    latestConversationMessage(maximum_bytes: bigint): Promise<any>;
    /**
     * Opens the provider and reader without constructing a reducer projection.
     */
    static openBrowser(options: any): Promise<WasmBrowserHistoryReader>;
    /**
     * Resolves the existing atomically published operation location and event.
     */
    operationEvent(operation: any): Promise<any>;
    /**
     * Captures a committed traversal boundary; subsequent appends are excluded.
     */
    pin(after_revision: bigint): Promise<any>;
    /**
     * Runs the same bounded page and attestation checks as the native reader.
     */
    readPage(cursor: any, limits: any): Promise<any>;
}

/**
 * Browser facade. All transitions, bounds and reconciliation use Client<D>.
 */
export class WasmClientViews {
    free(): void;
    [Symbol.dispose](): void;
    /**
     * Explicit logical retention tick, never a hidden timer.
     */
    advance(now: bigint): any;
    /**
     * Begin one explicit hypothesis; values stay as JavaScript references.
     */
    begin(metadata: any, predicted: any, assumption: any): string;
    /**
     * Export one explicitly selected hypothesis, preserving reference values.
     */
    checkpoint(branch: string): any;
    /**
     * Remove one local prediction; never cancels a durable effect.
     */
    discard(branch: string): any;
    /**
     * Configure the trusted restore validator once, before importing branches.
     */
    enableRestore(validator: Function): void;
    /**
     * Pure construction; callbacks run only on explicit transitions.
     */
    constructor(identity: string, namespace: string, sequence: bigint, limits: any, validate: Function, observe: Function, corresponds: Function);
    /**
     * Narrow changed hypothesis IDs and work; no full-state export.
     */
    observe(key: string, evidence: any): any;
    /**
     * Release an unreferenced demanded record.
     */
    release(key: string): void;
    /**
     * Resident record/branch/edge counts and conservatively accounted bytes.
     */
    residency(): any;
    /**
     * Import a bounded checkpoint only through the trusted domain validator.
     */
    restore(checkpoint: any): void;
    /**
     * Data-only allocation watermark. Never recycles an imported identity.
     */
    sequence(): bigint;
    /**
     * Inspect outcome metadata without serializing hypothesis bodies.
     */
    status(branch: string): any;
    /**
     * Select only one demanded record and explicitly supplied overlays.
     */
    view(key: string, overlays: any): any;
}

/**
 * Bounded Rust-owned content state for the WASM `MemoryConversation` adapter.
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
    /**
     * Creates a bounded content store for the supplied volume.
     */
    constructor(volume: any, maximum_file_bytes: number, maximum_path_bytes: number, maximum_resident_bytes: number, maximum_resident_files: number);
    /**
     * Reports whether a new file at `path` would conflict with a file or directory already retained by this provider.
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
 * An observed initialization and immutable binding, with a readiness request that the host must admit separately.
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
 */
export class WasmMcpHttpTransport {
    free(): void;
    [Symbol.dispose](): void;
    /**
     * Validates a host-retained response through the same native decoder.
     */
    acceptInitialization(operation: string, head: McpBrowserResponseHead, body: Uint8Array): Promise<WasmMcpHttpInitialization>;
    /**
     * Calls an already admitted operation.
     */
    callJson(operation: string, name: string, _arguments: WasmToolJsonValue): Promise<Uint8Array>;
    /**
     * Stages the Rust-owned initialization message without invoking a provider.
     */
    initializationRequest(operation: string, name: string, version: string): McpHttpRequest;
    /**
     * Discovers one separately admitted page.
     */
    listToolsJson(operation: string, cursor?: string | null): Promise<Uint8Array>;
    /**
     * Binds finite platform I/O and an optional host-provided session without effects.
     */
    constructor(provider: McpBrowserHttpProvider, endpoint: string, session: string | null | undefined, maximum_bytes: number, timeout_ms: number);
    /**
     * Queries the transport's explicit receipt capability without network I/O.
     */
    reconcileJson(operation: string): Promise<Uint8Array | undefined>;
    /**
     * Executes one explicitly admitted resource or prompt operation through the same bounded Rust transport; method/parameters/results validate at runtime.
     */
    serviceJson(operation: string, method: string, params: WasmToolJsonValue): Promise<Uint8Array>;
}

/**
 * Opaque synchronous reducer hosted in WebAssembly.
 */
export class WasmReducer {
    free(): void;
    [Symbol.dispose](): void;
    /**
     * Applies a deterministic command.
     */
    apply(command: any): any;
    /**
     * Applies one canonical Protobuf command, excluding fresh migrations that require executable content admission, and returns a response.
     */
    applyWire(command: Uint8Array): Uint8Array;
    /**
     * Attenuates a scope without permitting capability expansion.
     */
    attenuate(parent: any, id: string, capabilities: any): any;
    /**
     * Reads one admitted selection through the reducer's retained identity index.
     */
    contextSelectionForOperation(operation: any): any;
    /**
     * Returns the authoritative conversation projection, never a parallel JS reducer.
     */
    conversationJson(): string;
    /**
     * Returns the same bounded page with Rust-owned JS integer projection: revisions and cursors stay `BigInt`, validated file lengths become Number.
     */
    conversationPage(after_sequence: bigint, limit: number): any;
    /**
     * Returns an immutable, bounded page of canonical conversation records. `after_sequence` is an exclusive cursor in the message sequence, not the potentially larger aggregate event revision.
     */
    conversationPageJson(after_sequence: bigint, limit: number): string;
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
     * Computes the exact-version read capability without embedding it in a ref.
     */
    fileReadCapability(file: any): string;
    /**
     * Returns only authority and revision; command admission needs no snapshot.
     */
    head(): any;
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
     * Selects logical fork history against this reducer before provider preparation.
     */
    prepareForkRequest(request: unknown, policy: ForkHistoryPolicy | null): ForkRequest;
    /**
     * Composes the native immutable prefix after authenticating every exact reference against this owner's signed scope.
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
     * Returns a versioned issuer-authenticated restoration snapshot.
     */
    snapshot(): any;
    /**
     * Applies configured runtime bounds to one exact file reference.
     */
    validateFileUnderLimits(file: any, limits: any): void;
    /**
     * Authenticates an owner-issued read grant for this exact immutable file.
     */
    verifyContentRead(scope: any, file: any): void;
    /**
     * Authenticates a writer against the original private-volume owner.
     */
    verifyContentWrite(scope: any, volume: any): void;
    /**
     * Authenticates lazy directory and named-path access against the owner's signed, segment-bounded private-volume read grant.
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
 * Type-only bridge for the complete Rust-owned Stream error-code contract.
 */
export function __streamErrorCodeContract(value: StreamErrorCode): StreamErrorCode;

/**
 * Builds and validates the complete immutable batch request before any member admission.
 */
export function admitBatch(value: WasmBatchAdmissionInput): WasmDurableBatchWire;

/**
 * Builds the complete SDK-facing durable batch request in one Rust-owned projection.
 */
export function admitBatchRequest(value: WasmBatchAdmissionInput): WasmBatchAdmissionRequest;

/**
 * Admits one provider model event with the native stream rules and returns the detached state needed for the next event.
 */
export function admitModelEvent(event: WasmModelEventInput, limits: WasmModelLimitsInput, state: WasmModelEventAdmissionState | null): WasmModelEventAdmission;

/**
 * Builds and validates the complete owner-retained task admission envelope.
 */
export function admitTask(value: WasmTaskAdmissionInput): WasmTaskAdmissionWire;

/**
 * Applies a host renderer's output using the same native placement and bounds.
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
 * Decodes a complete canonical attachment list without a reducer instance.
 */
export function decodeAttachmentManifest(manifest: any, bytes: Uint8Array, item_count: number): any;

/**
 * Parses canonical JSON without passing full-width integer literals through JavaScript Number.
 */
export function decodeCanonicalJson(bytes: Uint8Array): any;

/**
 * Decodes one canonical event payload using the native event union.
 */
export function decodeEventPayload(event_type: string, canonical_payload_json: Uint8Array): any;

/**
 * Admits one canonical ref-only execution journal observation through Rust.
 */
export function decodeExecutionEventJson(bytes: Uint8Array): ExecutionEvent;

/**
 * Validate and project one hosted HTTP JSON success response into the public JavaScript shape.
 */
export function decodeHttpResponse(route: string, response_json: string): unknown;

/**
 * Parses external JSON with exact integers but without demanding canonical key order or whitespace.
 */
export function decodeJson(bytes: Uint8Array): any;

export function decodeReadResponse(codec: number, data: Uint8Array, decoded_length: bigint): Uint8Array;

/**
 * Returns the ordinary replaceable policy used by the stock Rust executor.
 */
export function defaultCompactionPolicy(): CompactionPolicy;

/**
 * Derives a stable child operation/message identity from one admitted operation and a local role label without duplicating UUID bit manipulation in hosts.
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
 * Serializes a plain JavaScript data value through Rust's canonical JSON representation.
 */
export function encodeCanonicalJson(value: any): Uint8Array;

/**
 * Encode one protobuf request into the hosted Stream HTTP JSON shape.
 */
export function encodeHttpRequest(route: string, input: Uint8Array): string;

/**
 * Creates a direct-parent segment from the exact admitted request bytes.
 */
export function encodeModelPrefix(request: Uint8Array, parent: any, parent_request: Uint8Array | null | undefined, limits: any): Uint8Array;

/**
 * Stages a descriptor with Rust-owned SHA-256, media-type, and safe byte-length rules, projecting its bounded length as a JS Number.
 */
export function fileDescriptor(bytes: Uint8Array, media_type: string): any;

/**
 * Converts a fully captured report into its canonical publishable child seed.
 */
export function forkSeedFromReport(report: any): any;

/**
 * Return whether a code can be emitted by this WASM adapter.
 */
export function is_stream_error_code(value: string): boolean;

/**
 * Wraps a consumer's JSON value schema in the complete typed projection envelope.
 */
export function jsonToolProjectionSchema(value_schema: WasmToolJsonSchema): WasmToolJsonSchema;

/**
 * Returns the same stable callback identity used by recorded runtime admission.
 */
export function mcpCallbackOperation(request: McpCallbackRequest): string;

/**
 * Projects the host-selected schema exposure through the ordinary model wire.
 */
export function mcpModelDefinitions(catalog: McpCatalog, maximum_tools: number, maximum_bytes: number): ToolDefinition[];

/**
 * Returns all media/options refs and original-admission claims without IO.
 */
export function modelContentInventory(content: WasmModelContentInput, limits: WasmModelLimitsInput): WasmModelContentInventoryWire;

/**
 * Normalize and encode canonical protobuf bytes for one commit request.
 */
export function normalizeCommitRequest(input: Uint8Array): Uint8Array;

/**
 * Parses a bounded frontmatter prefix without fetching or interpreting a skill body.
 */
export function parseSkillMetadata(prefix: Uint8Array, source: PinnedContextPath): SkillMetadata;

/**
 * Plans one deterministic conversation turn before any model or content callback runs.
 */
export function prepareConversationTurn(conversation: any, operation_id: string, content: any, attachments: any, limits: any, existing_selection: any, has_completed_output: boolean, can_reconcile: boolean): WasmTurnPreparation;

/**
 * Constructs the same canonical request bytes used by native providers.
 */
export function prepareModelRequest(request: any, limits: any): Uint8Array;

/**
 * Projects a discovered revision using the native messages, placement and bounds.
 */
export function projectDiscoveredContext(snapshot: DiscoveredContext, context: Context, placement: ContextPlacement, limits: WasmModelLimitsInput): Context;

/**
 * Decode one unary memory-provider response from canonical protobuf bytes into the public JavaScript result shape.
 */
export function projectMemoryResponse(operation: string, input: Uint8Array): unknown;

/**
 * Project a hosted HTTP error code onto the public Stream error vocabulary.
 */
export function publicHttpErrorCode(raw: string, route: string): string | undefined;

/**
 * Ordinary bounded owner read of a discovered body; no model projection or execution is implied.
 */
export function readPinnedContextPath(source: PinnedContextPath, reader: ContextDiscoveryReader, maximum_bytes: number): Promise<ContextPathResult>;

/**
 * Searches a pinned catalog without network I/O or authority changes.
 */
export function searchMcpCatalog(catalog: McpCatalog, query: string, after: string | null | undefined, maximum_results: number, maximum_tools: number, maximum_bytes: number): ToolDefinition[];

/**
 * Runs the canonical Rust conversation projection over bytes captured by the owner.
 */
export function selectModelContext(conversation: any, selection: any, files: any, maximum_messages: number, maximum_attachments: number, maximum_render_bytes: number, maximum_projected_attachments: number): Promise<any>;

/**
 * Derives the exact task and machine identities retained by durable admission.
 */
export function taskAdmissionIdentities(value: WasmTaskIdentityInput): WasmTaskAdmissionIdentities;

/**
 * Derives the same pinned task registration digest used by native admission.
 */
export function taskIdentityDigest(name: string, version: string, input_schema: any, output_schema: any, requirements: any, machine_digest: Uint8Array): Uint8Array;

/**
 * Uses one half of a canonical action digest as a stable local identity.
 */
export function uuidFromDigestHalf(digest: Uint8Array, second: boolean): string;

/**
 * Validate canonical protobuf bytes for one append request.
 */
export function validateAppendRequest(input: Uint8Array): string;

/**
 * Validates one aggregate identity with the same path-segment policy used by every Rust stream access.
 */
export function validateAuthorityPathSegment(value: string): string;

/**
 * Validates one component name with the canonical Rust byte and character policy.
 */
export function validateComponentLabel(value: string): string;

/**
 * Validates a host-approved pinned selection without granting read authority.
 */
export function validateContextSelection(selection: ContextSelection, limits: WasmModelLimitsInput): void;

/**
 * Pure v2 contract admission shared by native and JavaScript hosts.
 */
export function validateContract(kind: string, value: any, context: any): any;

/**
 * Returns the one Rust UUID spelling accepted for a conversation identity.
 */
export function validateConversationMessageId(value: string): string;

/**
 * Validate one hosted HTTP JSON success response using the same path, width, identity, and tagged-union rules as the canonical Stream domain.
 */
export function validateHttpResponse(route: string, response_json: string): void;

/**
 * Parses one public Harness identity with the canonical Rust contract and returns its normalized UUID spelling for a branded TypeScript facade.
 */
export function validateIdentity(kind: string, value: string): string;

/**
 * Validates a pinned callback request without granting model/interaction authority.
 */
export function validateMcpCallbackRequest(request: McpCallbackRequest): void;

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
 * Validates selection order and tool linkage before the host resolves any owner-mediated file bytes.
 */
export function validateModelContextSelection(conversation: any, selection: any): void;

/**
 * Validates a complete provider-neutral model message list with the native role, message-count, and content bounds.
 */
export function validateModelMessages(messages: readonly WasmModelMessageInput[], limits: WasmModelLimitsInput): void;

/**
 * Validates provider-owned counts against the exact canonical prepared request.
 */
export function validateModelTokenCount(request: ModelRequest, count: ModelTokenCount, limits: any): bigint;

/**
 * Validate one canonical Stream path using the same parser used by every provider and wire decoder.
 */
export function validatePath(path: string): string;

/**
 * Validate one canonical protobuf request at the browser boundary.
 */
export function validateRequest(kind: string, input: Uint8Array): string;

/**
 * Admits an already projected, provider-proven context with native model bounds.
 */
export function validateSelectedModelContext(selected: any, limits: any): void;

/**
 * Validate one JavaScript representation of a canonical Stream sequence.
 */
export function validateSequence(value: string): string;

/**
 * Validates and reprojects one owner-retained direct-child page using the same bounds and bytewise slot ordering as native Harness hosts.
 */
export function validateTaskChildrenPage(value: WasmTaskChildrenPageInput): WasmTaskChildrenPage;

/**
 * Validates the exact task dependency graph used by the TypeScript builder.
 */
export function validateTaskRequirements(value: WasmTaskDependencyInput): void;

/**
 * Checks a threshold against actual selected capacity, returning its finite output ceiling.
 */
export function validateThresholdCompaction(config: ThresholdCompaction, capacity: ModelContextCapacity, output_tokens: number | undefined): number;

/**
 * Validates a model-visible tool definition using the native contract.
 */
export function validateToolDefinition(definition: any): void;

/**
 * Validates one tool invocation against its registered definition.
 */
export function validateToolInvocation(definition: any, invocation: any): void;

/**
 * Validates the explicit result envelope without granting file or option authority.
 */
export function validateToolProjection(definition: WasmToolDefinitionInput, projection: WasmModelToolResultContentInput, limits: WasmModelLimitsInput): void;

/**
 * Validates one successful tool result against its registered definition.
 */
export function validateToolResult(definition: any, result: any): void;

/**
 * Applies the same JSON Schema admission used by Rust tool execution before a TypeScript facade turns an untrusted model value into a typed argument.
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
 */
export function validateWorkflowAdmission(value: WasmWorkflowAdmissionWire): WasmWorkflowAdmissionWire;

/**
 * Checks immutable file identity without constructing a reducer or issuer.
 */
export function verifyFileBytes(file: any, bytes: Uint8Array): void;

export type InitInput = RequestInfo | URL | Response | BufferSource | WebAssembly.Module;

export interface InitOutput {
    readonly memory: WebAssembly.Memory;
    readonly __wbg_wasmbrowseraggregate_free: (a: number, b: number) => void;
    readonly __wbg_wasmbrowserhistoryreader_free: (a: number, b: number) => void;
    readonly __wbg_wasmclientviews_free: (a: number, b: number) => void;
    readonly __wbg_wasmcontentstore_free: (a: number, b: number) => void;
    readonly __wbg_wasmmcphttpinitialization_free: (a: number, b: number) => void;
    readonly __wbg_wasmmcphttptransport_free: (a: number, b: number) => void;
    readonly __wbg_wasmreducer_free: (a: number, b: number) => void;
    readonly admitBatch: (a: any) => [number, number, number];
    readonly admitBatchRequest: (a: any) => [number, number, number];
    readonly admitModelEvent: (a: any, b: any, c: any) => [number, number, number];
    readonly admitTask: (a: any) => [number, number, number];
    readonly applyContextProjection: (a: any, b: any, c: any, d: any, e: any) => [number, number, number];
    readonly batchMemberOperationId: (a: number, b: number, c: number, d: number, e: number) => [number, number, number, number];
    readonly captureDiscoveredContext: (a: any, b: any) => any;
    readonly contextForRequest: (a: any, b: any, c: any, d: any) => any;
    readonly decodeAggregateKind: (a: number) => [number, number, number];
    readonly decodeAttachmentManifest: (a: any, b: number, c: number, d: number) => [number, number, number];
    readonly decodeCanonicalJson: (a: number, b: number) => [number, number, number];
    readonly decodeEventPayload: (a: number, b: number, c: number, d: number) => [number, number, number];
    readonly decodeExecutionEventJson: (a: number, b: number) => [number, number, number];
    readonly decodeJson: (a: number, b: number) => [number, number, number];
    readonly defaultCompactionPolicy: () => [number, number, number];
    readonly deriveOperationUuid: (a: number, b: number, c: number, d: number) => [number, number, number, number];
    readonly digestCanonicalJson: (a: any) => [number, number, number, number];
    readonly encodeAttachmentManifest: (a: any) => [number, number, number, number];
    readonly encodeCanonicalJson: (a: any) => [number, number, number, number];
    readonly encodeModelPrefix: (a: number, b: number, c: any, d: number, e: number, f: any) => [number, number, number];
    readonly fileDescriptor: (a: number, b: number, c: number, d: number) => [number, number, number];
    readonly forkSeedFromReport: (a: any) => [number, number, number];
    readonly jsonToolProjectionSchema: (a: any) => [number, number, number];
    readonly mcpCallbackOperation: (a: any) => [number, number, number, number];
    readonly mcpModelDefinitions: (a: any, b: any, c: any) => [number, number, number];
    readonly modelContentInventory: (a: any, b: any) => [number, number, number];
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
    readonly validateMcpCallbackRequest: (a: any) => [number, number];
    readonly validateMcpCatalog: (a: any, b: any, c: any) => [number, number];
    readonly validateMcpStdioRequest: (a: any) => [number, number];
    readonly validateModelContent: (a: any, b: any) => [number, number];
    readonly validateModelContextSelection: (a: any, b: any) => [number, number];
    readonly validateModelMessages: (a: any, b: any) => [number, number];
    readonly validateModelTokenCount: (a: any, b: any, c: any) => [bigint, number, number];
    readonly validateSelectedModelContext: (a: any, b: any) => [number, number];
    readonly validateTaskChildrenPage: (a: any) => [number, number, number];
    readonly validateTaskRequirements: (a: any) => [number, number];
    readonly validateThresholdCompaction: (a: any, b: any, c: any) => [number, number, number];
    readonly validateToolDefinition: (a: any) => [number, number];
    readonly validateToolInvocation: (a: any, b: any) => [number, number];
    readonly validateToolProjection: (a: any, b: any, c: any) => [number, number];
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
    readonly wasmbrowseraggregate_execute: (a: number, b: any) => any;
    readonly wasmbrowseraggregate_head: (a: number) => [number, number, number];
    readonly wasmbrowseraggregate_historyReader: (a: number) => [number, number, number];
    readonly wasmbrowseraggregate_openBrowser: (a: any, b: any, c: number) => any;
    readonly wasmbrowseraggregate_reconcile: (a: number, b: any) => any;
    readonly wasmbrowseraggregate_refreshThrough: (a: number, b: bigint, c: number) => any;
    readonly wasmbrowseraggregate_snapshot: (a: number) => [number, number, number];
    readonly wasmbrowserhistoryreader_latestConversationMessage: (a: number, b: bigint) => any;
    readonly wasmbrowserhistoryreader_openBrowser: (a: any) => any;
    readonly wasmbrowserhistoryreader_operationEvent: (a: number, b: any) => any;
    readonly wasmbrowserhistoryreader_pin: (a: number, b: bigint) => any;
    readonly wasmbrowserhistoryreader_readPage: (a: number, b: any, c: any) => any;
    readonly wasmclientviews_advance: (a: number, b: any) => [number, number, number];
    readonly wasmclientviews_begin: (a: number, b: any, c: any, d: any) => [number, number, number, number];
    readonly wasmclientviews_checkpoint: (a: number, b: any) => [number, number, number];
    readonly wasmclientviews_discard: (a: number, b: any) => [number, number, number];
    readonly wasmclientviews_enableRestore: (a: number, b: any) => [number, number];
    readonly wasmclientviews_new: (a: any, b: any, c: any, d: any, e: any, f: any, g: any) => [number, number, number];
    readonly wasmclientviews_observe: (a: number, b: any, c: any) => [number, number, number];
    readonly wasmclientviews_release: (a: number, b: any) => [number, number];
    readonly wasmclientviews_residency: (a: number) => [number, number, number];
    readonly wasmclientviews_restore: (a: number, b: any) => [number, number];
    readonly wasmclientviews_sequence: (a: number) => bigint;
    readonly wasmclientviews_status: (a: number, b: any) => [number, number, number];
    readonly wasmclientviews_view: (a: number, b: any, c: any) => [number, number, number];
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
    readonly wasmmcphttptransport_serviceJson: (a: number, b: number, c: number, d: number, e: number, f: any) => any;
    readonly wasmreducer_apply: (a: number, b: any) => [number, number, number];
    readonly wasmreducer_applyWire: (a: number, b: number, c: number) => [number, number, number, number];
    readonly wasmreducer_attenuate: (a: number, b: any, c: number, d: number, e: any) => [number, number, number];
    readonly wasmreducer_contextSelectionForOperation: (a: number, b: any) => [number, number, number];
    readonly wasmreducer_conversationJson: (a: number) => [number, number, number, number];
    readonly wasmreducer_conversationPage: (a: number, b: bigint, c: number) => [number, number, number];
    readonly wasmreducer_conversationPageJson: (a: number, b: bigint, c: number) => [number, number, number, number];
    readonly wasmreducer_delegatePrivateDirectoryRead: (a: number, b: any, c: number, d: number, e: number, f: number, g: any, h: number, i: number) => [number, number, number];
    readonly wasmreducer_delegatePrivateFileRead: (a: number, b: any, c: number, d: number, e: number, f: number, g: any) => [number, number, number];
    readonly wasmreducer_directoryReadCapability: (a: number, b: any, c: number, d: number) => [number, number, number, number];
    readonly wasmreducer_fileReadCapability: (a: number, b: any) => [number, number, number, number];
    readonly wasmreducer_head: (a: number) => [number, number, number];
    readonly wasmreducer_issueScope: (a: number, b: number, c: number, d: any) => [number, number, number];
    readonly wasmreducer_issueScopeForAgent: (a: number, b: number, c: number, d: number, e: number, f: any) => [number, number, number];
    readonly wasmreducer_issueScopeWithPolicies: (a: number, b: number, c: number, d: any) => [number, number, number];
    readonly wasmreducer_issueScopeWithPoliciesForAgent: (a: number, b: number, c: number, d: number, e: number, f: any) => [number, number, number];
    readonly wasmreducer_new: (a: any, b: number, c: number, d: number, e: number, f: any) => [number, number, number];
    readonly wasmreducer_prepareForkRequest: (a: number, b: any, c: any) => [number, number, number];
    readonly wasmreducer_prepareInheritedModelRequest: (a: number, b: any, c: any, d: any, e: any, f: any) => any;
    readonly wasmreducer_protocolIdentity: (a: number) => [number, number, number];
    readonly wasmreducer_restore: (a: any, b: number, c: number, d: number, e: number, f: any) => [number, number, number];
    readonly wasmreducer_snapshot: (a: number) => [number, number, number];
    readonly wasmreducer_validateFileUnderLimits: (a: number, b: any, c: any) => [number, number];
    readonly wasmreducer_verifyContentRead: (a: number, b: any, c: any) => [number, number];
    readonly wasmreducer_verifyContentWrite: (a: number, b: any, c: any) => [number, number];
    readonly wasmreducer_verifyPrivateDirectoryRead: (a: number, b: any, c: any, d: number, e: number, f: number, g: number) => [number, number];
    readonly wasmreducer_verifyScope: (a: number, b: any) => [number, number];
    readonly wasmreducer_volumeCapability: (a: number, b: any, c: number, d: number) => [number, number, number, number];
    readonly wasmreducer_volumeStorageName: (a: number, b: any) => [number, number, number, number];
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
