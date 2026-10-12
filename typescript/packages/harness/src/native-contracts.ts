import type { ExecutionEvent } from "../generated/wasm/acyclic_harness_wasm.js";
export type { ExecutionEvent } from "../generated/wasm/acyclic_harness_wasm.js";
/** Explicitly initialized Rust contract validator with strongly typed v2 inputs. */
import * as wasm from "../generated/wasm/acyclic_harness_wasm.js";
import type {
  CompactionPolicy, ThresholdCompaction, ModelContextCapacity, ModelTokenCount, ModelRequest as ModelRequestWire,
  Context, ContextSelection, ContextRenderMode, ContextPlacement, WasmModelMessageInput, WasmModelContentInventoryWire,
  ContextDiscovery, ContextDiscoveryReader, ContextPathResult, ContextReloadPolicy,
  DiscoveredContext, PinnedContextPath, SkillMetadata,
  WasmBatchAdmissionInput, WasmDurableBatchWire, WasmReducer, WasmToolJsonValue,
  WasmTaskAdmissionIdentities, WasmTaskAdmissionInput, WasmTaskAdmissionWire,
  WasmMachineIdentityWire, WasmNativeLimitsWire, WasmTaskRunLimitsWire,
  WasmExecutionPlacementWire, WasmWorkflowAdmissionWire,
  WasmTaskIdentityInput, WasmTurnPreparation, ModelContent as ModelContentWire,
  WasmModelEventAdmission, WasmModelEventAdmissionState, ModelRole as ModelRoleWire,
  WasmTaskDependencyInput,
  McpCatalog, McpStdioRequest, McpCallbackRequest,
} from "../generated/wasm/acyclic_harness_wasm.js";
import type {
  Attachment, ConversationMessage, ConversationMessageId, ConversationPage, FileDescriptor, FileRef, Limits, MessageKind, ProviderRef, ReferencedAttachments, TaskOutcomeRecord, VolumeClass, VolumeRef,
} from "./conversation.js";
import type {
  ForkReport, ForkRequest, ForkSeed, ReferenceGrant, ResourceKind, ResourceRef, ResourceRevision,
} from "./fork.js";
import type { ExtensionAdmission, ExtensionConfiguration, ExtensionDependency, ExtensionRecord, ExtensionStateMigration } from "./extension.js";
import type { ApprovalBinding, InteractionId, InteractionResolution, InteractionTicket, ResolutionReceipt } from "./interaction.js";
import type { ProjectMergeReceipt } from "./project.js";
import type { BatchAdmissionRequest } from "./task-group.js";
import type { PrivateDirectoryPage } from "./content.js";
import type { TaskChildrenPage } from "./task.js";
import type { ModelContent, ModelEvent, ToolDefinition, ToolJsonSchema, ToolJsonValue, ToolInvocation, ToolResult, ToolResultContent } from "./model.js";
import type { IdentityKind, IdentityKindMap, OperationId, TaskId } from "./index.js";
import { assertHarnessWasmExports, ensureHarnessWasm } from "./wasm-runtime.js";
import { HARNESS_MAX_ATTACHMENT_COUNT } from "./limits-contract.js";

export type { WasmTaskAdmissionInput, WasmBatchAdmissionInput, WasmToolJsonValue, WasmTaskAdmissionWire, WasmDurableBatchWire, WasmTaskAdmissionIdentities, WasmExecutionPlacementWire, WasmTaskRunLimitsWire, WasmNativeLimitsWire, WasmMachineIdentityWire } from "../generated/wasm/acyclic_harness_wasm.js";
/** MCP contracts are emitted from the production Rust protocol owner. */
export type { McpCatalog, McpSchemaExposure, McpDiscoveryPolicy, McpToolDefinition, McpToolResult, McpToolsPage,
  McpStdioRequest, McpStdioMethod, McpCallbackPolicy, McpCallbackRequest,
  McpInitializeResult, McpHttpRequest, McpBrowserHttpProvider, McpBrowserExchange,
  McpBrowserResponseHead } from "../generated/wasm/acyclic_harness_wasm.js";
/** Pinned composition values generated from the production Rust types. */
export type { Context, ContextAttribute, ContextSourceValue, ContextSelection,
  CompactionPolicy, CompactionRetention, ThresholdCompaction,
  ModelContextCapacity as ModelContextCapacityWire, ModelTokenCount as ModelTokenCountWire,
  ContextRepresentation, ContextExtent, ContextRenderMode, ContextPlacement,
  ContextRoot, InstructionScope, ContextDiscoveryPolicy, ContextDiscoveryLimits,
  ContextDiscovery, ContextReloadPolicy, ContextDiscoveryReader, ContextDirectoryQuery,
  ContextReadQuery, ContextPathResult, DiscoveredContext, PinnedContextPath, SkillMetadata } from "../generated/wasm/acyclic_harness_wasm.js";


/** Rust-owned per-step model event admission state with cumulative text bytes. */
export type ModelEventAdmissionState = Readonly<Omit<WasmModelEventAdmissionState, "calls"> & Readonly<{
  calls: readonly string[];
}>>;

/** Detached model event paired with its Rust admission state. */
export type ModelEventAdmission = Omit<WasmModelEventAdmission, "event" | "state"> & Readonly<{
  event: ModelEvent;
  state: ModelEventAdmissionState;
}>;

/** Serde shape returned by the canonical Rust context projector. */
export type NativeFileRef = Omit<FileRef, "descriptor"> & Readonly<{
  descriptor: Omit<FileDescriptor, "byte_length"> & Readonly<{ byte_length: number | bigint }>;
}>;
export interface NativeSelectedModelContext {
  readonly selection: Readonly<{
    readonly conversation_revision: bigint;
    readonly message_ids: readonly ConversationMessageId[];
    readonly checkpoint?: NativeFileRef;
  }>;
  readonly messages: readonly Readonly<{
    role: ModelRoleWire;
    content: ModelContentWire;
  }>[];
}

/** Exact Rust projection for a pinned resumable-tool machine admission. */
export type WorkflowAdmissionWire = Readonly<WasmWorkflowAdmissionWire>;

interface ContractValues {
  readonly limits: Limits;
  readonly provider_ref: ProviderRef;
  readonly volume_ref: VolumeRef;
  readonly file_ref: FileRef;
  readonly file_descriptor: FileDescriptor;
  readonly resource_ref: ResourceRef;
  readonly private_directory_page: PrivateDirectoryPage;
  readonly extension_record: ExtensionRecord;
  readonly extension_state_migration: ExtensionStateMigration;
  readonly extension_dependency: ExtensionDependency;
  readonly extension_configuration: ExtensionConfiguration;
  readonly extension_admission: ExtensionAdmission;
  readonly attachments: ReferencedAttachments;
  readonly conversation_message: ConversationMessage;
  readonly task_outcome: TaskOutcomeRecord;
  readonly execution_placement: WasmExecutionPlacementWire;
  readonly task_admission: WasmTaskAdmissionWire;
  readonly workflow_admission: WorkflowAdmissionWire;
  readonly machine_identity: WasmMachineIdentityWire;
  readonly durable_batch_request: WasmDurableBatchWire;
  readonly resource_revision: ResourceRevision;
  readonly fork_request: ForkRequest;
  readonly fork_report: ForkReport;
  readonly fork_seed: ForkSeed;
  readonly reference_grant: ReferenceGrant;
  readonly interaction_ticket: InteractionTicket;
  readonly interaction_resolution: InteractionResolution;
  readonly resolution_receipt: ResolutionReceipt;
  readonly approval_binding: ApprovalBinding;
  readonly project_merge_receipt: ProjectMergeReceipt;
}

/** Rust performs admission validation; TypeScript preserves the exact public shape. */
export class NativeContracts {
  static #instance: NativeContracts | undefined;
  private constructor() {}

  /** Initialize the shared WASM instance before synchronous contract calls. */
  static async create(): Promise<NativeContracts> {
    await ensureHarnessWasm();
    if (NativeContracts.#instance === undefined) {
      assertHarnessWasmExports(wasm);
      NativeContracts.#instance = new NativeContracts();
    }
    return NativeContracts.#instance;
  }

  /** The replaceable default comes directly from the production Rust executor's policy. */
  defaultCompactionPolicy(): CompactionPolicy {
    return freezeNative(normalizeNativeValue(wasm.defaultCompactionPolicy())) as CompactionPolicy;
  }

  /** Rust validates selected capacity and returns the finite effective output ceiling. */
  validateThresholdCompaction(config: ThresholdCompaction, capacity: ModelContextCapacity,
    outputTokens?: number): number {
    return wasm.validateThresholdCompaction(config, capacity, outputTokens);
  }

  /** Rust prepares the canonical request and binds the provider's exact additive counts. */
  validateModelTokenCount(request: ModelRequestWire, count: ModelTokenCount, limits: Limits): bigint {
    return wasm.validateModelTokenCount(request, count, limits);
  }

  /** Admit task dependencies with the same graph and capability rules as Rust. */
  validateTaskRequirements(value: WasmTaskDependencyInput): void {
    wasm.validateTaskRequirements(value);
  }

  validate<Family extends string>(kind: "provider_ref", value: ProviderRef<Family>): ProviderRef<Family>;
  validate<Class extends VolumeClass, Family extends string>(kind: "volume_ref", value: VolumeRef<Class, Family>): VolumeRef<Class, Family>;
  validate<Class extends VolumeClass, Family extends string>(kind: "file_ref", value: FileRef<Class, Family>): FileRef<Class, Family>;
  validate<Kind extends ResourceKind>(kind: "resource_ref", value: ResourceRef<Kind>): ResourceRef<Kind>;
  validate<K extends Exclude<keyof ContractValues, "conversation_message" | "interaction_resolution"
    | "provider_ref" | "volume_ref" | "file_ref" | "resource_ref">>(kind: K, value: ContractValues[K]): ContractValues[K];
  validate<Kind extends MessageKind>(kind: "conversation_message", value: ConversationMessage<Kind>, limits: Limits): ConversationMessage<Kind>;
  validate(kind: "interaction_resolution", value: InteractionResolution,
    ticket: InteractionTicket): InteractionResolution;
  validate(kind: keyof ContractValues, value: ContractValues[keyof ContractValues],
    context?: Limits | InteractionTicket): ContractValues[keyof ContractValues] {
    // Retain native u64/usize counters as BigInt.
    const admitted = normalizeNativeValue(
      kind === "workflow_admission"
        ? wasm.validateWorkflowAdmission(value as WorkflowAdmissionWire)
        : wasm.validateContract(kind, value, context ?? null),
    );
    if (kind === "project_merge_receipt") {
      const receipt = admitted as ProjectMergeReceipt;
      const statement = normalizeNativeValue(receipt.provider_proof.statement, true) as ProjectMergeReceipt["provider_proof"]["statement"];
      return freezeNative({ ...receipt, provider_proof: { ...receipt.provider_proof,
        statement,
      } });
    }
    return freezeNative(admitted) as ContractValues[keyof ContractValues];
  }

  verifyFileBytes(file: FileRef, bytes: Uint8Array): void {
    wasm.verifyFileBytes(file, bytes);
  }

  decodeAttachmentManifest(manifest: FileRef, bytes: Uint8Array, itemCount: number): readonly Attachment[] {
    if (!Number.isSafeInteger(itemCount) || itemCount < 0 || itemCount > HARNESS_MAX_ATTACHMENT_COUNT) {
      throw new TypeError("attachment count is outside the protocol limit");
    }
    return freezeNative(normalizeNativeValue(wasm.decodeAttachmentManifest(manifest, bytes, itemCount))) as readonly Attachment[];
  }

  /** Rust's typed serde serialization is the manifest byte contract. */
  encodeAttachmentManifest(items: readonly Attachment[]): Uint8Array {
    return Uint8Array.from(wasm.encodeAttachmentManifest(items));
  }

  forkSeed(report: ForkReport): ForkSeed {
    return freezeNative(normalizeNativeValue(wasm.forkSeedFromReport(report))) as ForkSeed;
  }

  /** The Rust tool registry's JSON Schema admission, before a typed parser runs. */
  validateToolValue(schema: ToolJsonSchema, value: unknown): unknown {
    return normalizeNativeValue(wasm.validateToolValue(schema, value), true);
  }

  /** Validate host-approved selections without granting read authority. */
  validateContextSelection(selection: ContextSelection, limits: Limits): void {
    wasm.validateContextSelection(selection, limits);
  }

  /** Parse only the bounded frontmatter prefix through the native Rust parser. */
  parseSkillMetadata(prefix: Uint8Array, source: PinnedContextPath): SkillMetadata {
    return freezeNative(normalizeNativeValue(wasm.parseSkillMetadata(prefix, source))) as SkillMetadata;
  }

  /** Call-scoped owner reads feeding the same Rust discovery implementation. */
  async captureDiscoveredContext(declaration: ContextDiscovery, reader: ContextDiscoveryReader): Promise<DiscoveredContext> {
    return freezeNative(normalizeNativeValue(await wasm.captureDiscoveredContext(declaration, reader))) as DiscoveredContext;
  }

  /** Select an immutable revision at the caller's admitted request boundary. */
  async contextForRequest(declaration: ContextDiscovery, reader: ContextDiscoveryReader,
    current: DiscoveredContext, reload: ContextReloadPolicy): Promise<DiscoveredContext> {
    return freezeNative(normalizeNativeValue(await wasm.contextForRequest(declaration, reader, current, reload))) as DiscoveredContext;
  }

  /** Bounded pinned body read; the bytes belong to this result and remain a Uint8Array. */
  async readPinnedContextPath(source: PinnedContextPath, reader: ContextDiscoveryReader,
    maximumBytes: number): Promise<ContextPathResult> {
    if (!Number.isSafeInteger(maximumBytes) || maximumBytes <= 0 || maximumBytes > 0xffff_ffff) {
      throw new RangeError("pinned context read bound is invalid");
    }
    const result = await wasm.readPinnedContextPath(source, reader, maximumBytes);
    return Object.freeze({ file: freezeNative(normalizeNativeValue(result.file)) as ContextPathResult["file"],
      bytes: Uint8Array.from(result.bytes) });
  }

  /** Project an immutable discovery revision using the same Rust source and bounds. */
  projectDiscoveredContext(snapshot: DiscoveredContext, context: Context,
    placement: ContextPlacement, limits: Limits): Context {
    return freezeNative(normalizeNativeValue(wasm.projectDiscoveredContext(
      snapshot, context, placement, limits,
    ))) as Context;
  }

  /** Shared Rust projection placement and finite bounds; performs no effects. */
  applyContextProjection(context: Context, messages: readonly WasmModelMessageInput[],
    mode: ContextRenderMode, placement: ContextPlacement, limits: Limits): Context {
    return freezeNative(normalizeNativeValue(wasm.applyContextProjection(
      context, messages, mode, placement, limits,
    ))) as Context;
  }

  /** Rust owns the complete model-visible tool definition contract. */
  validateToolDefinition(definition: Pick<ToolDefinition, "name" | "revision" | "description" | "inputSchema" | "outputSchema" | "projection">): void {
    wasm.validateToolDefinition(nativeToolDefinition(definition));
  }

  /** Validate a whole catalog before an explicit registration/reload. */
  validateMcpCatalog(catalog: McpCatalog, maximumTools: number, maximumBytes: number): void {
    wasm.validateMcpCatalog(catalog, maximumTools, maximumBytes);
  }

  /** Validate an exact native stdio descriptor without selecting a process provider. */
  validateMcpStdioRequest(request: McpStdioRequest): void {
    wasm.validateMcpStdioRequest(request);
  }

  /** Validates the retained request without granting callback authority. */
  validateMcpCallbackRequest(request: McpCallbackRequest): void {
    wasm.validateMcpCallbackRequest(request);
  }

  /** Returns the original Rust-owned callback operation identity. */
  mcpCallbackOperation(request: McpCallbackRequest): string {
    return wasm.mcpCallbackOperation(request);
  }

  /** Project explicitly selected schemas without granting tool authority. */
  mcpModelDefinitions(catalog: McpCatalog, maximumTools: number, maximumBytes: number): readonly import("../generated/wasm/acyclic_harness_wasm.js").ToolDefinition[] {
    return wasm.mcpModelDefinitions(catalog, maximumTools, maximumBytes);
  }

  /** Pure bounded search; returned definitions convey no invocation authority. */
  searchMcpCatalog(catalog: McpCatalog, query: string, after: string | undefined,
    maximumResults: number, maximumTools: number, maximumBytes: number): readonly import("../generated/wasm/acyclic_harness_wasm.js").ToolDefinition[] {
    return wasm.searchMcpCatalog(catalog, query, after, maximumResults, maximumTools, maximumBytes);
  }

  /** Rust owns tool invocation identity and argument schema validation. */
  validateToolInvocation(
    definition: Pick<ToolDefinition, "name" | "revision" | "description" | "inputSchema" | "outputSchema" | "projection">,
    invocation: Pick<ToolInvocation, "callId" | "name" | "arguments">,
  ): void {
    wasm.validateToolInvocation(nativeToolDefinition(definition), invocation);
  }

  /** Rust owns tool result output schema validation. */
  validateToolResult(
    definition: Pick<ToolDefinition, "name" | "revision" | "description" | "inputSchema" | "outputSchema" | "projection">,
    result: ToolResult,
  ): void {
    wasm.validateToolResult(nativeToolDefinition(definition), result);
  }

  jsonToolProjectionSchema(schema: ToolJsonSchema): ToolJsonSchema {
    return normalizeNativeValue(wasm.jsonToolProjectionSchema(schema), true) as ToolJsonSchema;
  }

  /** Validate the complete projection using its own schema and caller-selected bounds. */
  validateToolProjection(
    definition: Pick<ToolDefinition, "name" | "revision" | "description" | "inputSchema" | "outputSchema" | "projection">,
    projection: ToolResultContent,
    limits: Limits,
  ): void {
    wasm.validateToolProjection(nativeToolDefinition(definition), projection, limits);
  }

  /** The Rust inventory includes tool media and immutable native option references. */
  modelContentInventory(content: ModelContent, limits: Limits): Readonly<Omit<WasmModelContentInventoryWire, "files"> & { files: readonly FileRef[] }> {
    return freezeNative(normalizeNativeValue(wasm.modelContentInventory(content, limits))) as
      Readonly<Omit<WasmModelContentInventoryWire, "files"> & { files: readonly FileRef[] }>;
  }

  /** Rust owns model stream event, tool-call, completion, and UTF-8 byte admission. */
  admitModelEvent(event: ModelEvent, limits: Limits, state?: ModelEventAdmissionState): ModelEventAdmission {
    // Preserve provider BigInts while projecting bounded admission counters to Numbers.
    const admitted = normalizeNativeValue(
      wasm.admitModelEvent(
        event,
        limits,
        state === undefined ? null : { ...state, calls: [...state.calls] },
      ),
      true,
      true,
    ) as WasmModelEventAdmission;
    const admittedState = normalizeNativeValue(admitted.state, true) as ModelEventAdmissionState;
    if (admitted.event === null || typeof admitted.event !== "object"
      || !Number.isSafeInteger(admittedState.count) || admittedState.count < 0
      || !Number.isSafeInteger(admittedState.text_bytes) || admittedState.text_bytes < 0
      || !Array.isArray(admittedState.calls) || typeof admittedState.completed !== "boolean") {
      throw new TypeError("native model event admission returned an invalid state");
    }
    let projectedEvent = admitted.event as ModelEvent;
    if (admitted.event.kind === "tool_call") {
      const { call_id: callId, ...rest } = admitted.event;
      projectedEvent = { ...rest, callId } as ModelEvent;
    }
    return freezeNative({
      event: projectedEvent,
      state: { ...admittedState, calls: [...admittedState.calls] },
    });
  }

  /** Runs the bounded canonical Rust projection over owner-captured bytes. */
  selectModelContext(
    conversation: Readonly<{ agent: string | null; messages: readonly ConversationMessage[] }>,
    selection: Readonly<{ conversation_revision: bigint; message_ids: readonly ConversationMessageId[] }>,
    files: ReadonlyMap<string, Uint8Array>,
    maximumMessages: number,
    maximumAttachments: number,
    maximumRenderBytes: number,
    maximumProjectedAttachments: number,
  ): Promise<NativeSelectedModelContext> {
    return wasm.selectModelContext(
      conversation, selection, files, maximumMessages, maximumAttachments, maximumRenderBytes,
      maximumProjectedAttachments,
    ).then(value => normalizeNativeValue(value) as NativeSelectedModelContext);
  }

  /** Runs canonical selection/linkage checks before owner-mediated reads. */
  validateModelContextSelection(
    conversation: Readonly<{ agent: string | null; messages: readonly ConversationMessage[] }>,
    selection: Readonly<{ conversation_revision: bigint; message_ids: readonly ConversationMessageId[] }>,
  ): void {
    wasm.validateModelContextSelection(conversation, selection);
  }

  /** Rust-owned deterministic admission and retry plan for one conversation turn. */
  prepareConversationTurn(
    conversation: Readonly<{ agent: string | null; messages: readonly ConversationMessage[] }>,
    operationId: OperationId,
    content: FileRef,
    attachments: ReferencedAttachments,
    limits: Limits,
    existingSelection: Readonly<{ conversation_revision: bigint; message_ids: readonly ConversationMessageId[] }> | null,
    hasCompletedOutput: boolean,
    canReconcile: boolean,
  ): WasmTurnPreparation {
    return normalizeNativeValue(wasm.prepareConversationTurn(
      conversation, operationId, content, attachments, limits, existingSelection,
      hasCompletedOutput, canReconcile,
    )) as WasmTurnPreparation;
  }

  validateConversationMessageId(value: string): ConversationMessageId {
    return wasm.validateConversationMessageId(value) as ConversationMessageId;
  }

  validateIdentity<Kind extends IdentityKind>(kind: Kind, value: string): IdentityKindMap[Kind] {
    return wasm.validateIdentity(kind, value) as IdentityKindMap[Kind];
  }

  deriveOperationId(operation: OperationId, label: string): OperationId {
    return wasm.deriveOperationUuid(operation, label) as OperationId;
  }

  batchMemberOperationId(group: string, batch: string, index: number): OperationId {
    // The native WASM entry point accepts u32; reject values it would truncate.
    if (!Number.isSafeInteger(index) || index < 0 || index > 0xffff_ffff) throw new RangeError("batch index is out of range");
    return wasm.batchMemberOperationId(group, batch, index) as OperationId;
  }

  taskIdentityDigest(name: string, version: string, input: ToolJsonSchema, output: ToolJsonSchema,
    requirements: readonly string[], machineDigest: Uint8Array): readonly number[] {
    const digest = Uint8Array.from(wasm.taskIdentityDigest(name, version, input, output,
      [...requirements], Uint8Array.from(machineDigest)));
    if (digest.length !== 32) throw new TypeError("native task identity digest has an invalid length");
    return Object.freeze(Array.from(digest));
  }

  taskAdmissionIdentities(value: WasmTaskIdentityInput): WasmTaskAdmissionIdentities {
    return freezeNative(normalizeNativeValue(wasm.taskAdmissionIdentities({
      ...value,
      requirements: [...value.requirements],
      machine_digest: [...value.machine_digest],
    }))) as WasmTaskAdmissionIdentities;
  }

  /** Rust-owned durable task admission and canonical envelope projection. */
  admitTask(value: WasmTaskAdmissionInput): WasmTaskAdmissionWire {
    return freezeNative(normalizeNativeValue(wasm.admitTask({
      ...value,
      requirements: [...value.requirements],
      machine_digest: [...value.machine_digest],
      grants: [...value.grants],
    }))) as WasmTaskAdmissionWire;
  }

  /** Rust-owned immutable batch request construction and validation. */
  admitBatch(value: WasmBatchAdmissionInput): WasmDurableBatchWire {
    return freezeNative(normalizeNativeValue(wasm.admitBatch({
      ...value,
      requirements: [...value.requirements],
      machine_digest: [...value.machine_digest],
      inputs: [...value.inputs],
      grants: [...value.grants],
    }))) as WasmDurableBatchWire;
  }

  /** Rust-owned complete batch envelope, members, and canonical digest. */
  admitBatchRequest(value: WasmBatchAdmissionInput): BatchAdmissionRequest {
    return freezeNative(normalizeNativeValue(wasm.admitBatchRequest({
      ...value,
      requirements: [...value.requirements],
      machine_digest: [...value.machine_digest],
      inputs: [...value.inputs],
      grants: [...value.grants],
    }))) as BatchAdmissionRequest;
  }

  idFromDigest(digest: Uint8Array, kind: "operation"): OperationId;
  idFromDigest(digest: Uint8Array, kind: "interaction"): InteractionId;
  idFromDigest(digest: Uint8Array, kind: "operation" | "interaction"): OperationId | InteractionId {
    const value = wasm.uuidFromDigestHalf(digest, kind === "interaction");
    return kind === "operation"
      ? this.validateIdentity("operation", value)
      : this.validateIdentity("interaction", value);
  }

  fileDescriptor(bytes: Uint8Array, mediaType: string): FileDescriptor {
    return freezeNative(normalizeNativeValue(wasm.fileDescriptor(Uint8Array.from(bytes), mediaType))) as FileDescriptor;
  }

  /** Rust JSON parser retains every full-width integer before any JS normalization. */
  decodeCanonicalJson(bytes: Uint8Array): unknown {
    return normalizeNativeValue(wasm.decodeCanonicalJson(bytes));
  }

  /** Untrusted provider JSON may not silently round integers into JS Number. */
  decodeModelJson(bytes: Uint8Array): ToolJsonValue {
    return normalizeNativeValue(wasm.decodeJson(bytes), true) as ToolJsonValue;
  }

  /** Admits the Rust-generated ref-only executor observation and freezes its exact wire fields. */
  decodeExecutionEventJson(bytes: Uint8Array): ExecutionEvent {
    return freezeNative(normalizeNativeValue(wasm.decodeExecutionEventJson(bytes))) as ExecutionEvent;
  }

  conversationPage(core: WasmReducer, afterSequence: bigint, limit: number): ConversationPage {
    return freezeNative(normalizeNativeValue(core.conversationPage(afterSequence, limit))) as ConversationPage;
  }

  /** Rust validates and reprojects owner-retained child pages and their request bounds. */
  validateTaskChildrenPage(
    parent: TaskId,
    expectedRevision: bigint | null,
    afterSlot: string | null,
    maximum: number,
    page: TaskChildrenPage,
  ): TaskChildrenPage {
    // Snapshot getters once before crossing WASM.
    const revision = page.revision;
    const entries = page.entries.map(entry => ({ slot: entry.slot, taskId: entry.taskId }));
    const nextAfter = page.nextAfter;
    if (typeof revision !== "bigint" || revision < 0n) {
      throw new TypeError("native task child page revision is invalid");
    }
    const snapshot: TaskChildrenPage = { revision, entries, nextAfter };
    return freezeNative(normalizeNativeValue(wasm.validateTaskChildrenPage({
      parent,
      expectedRevision,
      afterSlot,
      maximum,
      page: snapshot,
    }))) as TaskChildrenPage;
  }

  encodeCanonicalJson(value: unknown): Uint8Array {
    return Uint8Array.from(wasm.encodeCanonicalJson(value));
  }

  /** Compare complete admitted records without maintaining parallel field lists. */
  canonicalEqual<Value>(left: Value, right: Value): boolean {
    const encodedLeft = this.encodeCanonicalJson(left);
    const encodedRight = this.encodeCanonicalJson(right);
    return encodedLeft.byteLength === encodedRight.byteLength
      && encodedLeft.every((byte, index) => byte === encodedRight[index]);
  }

  /** Rust canonical JSON plus BLAKE3; only the digest leaves this boundary. */
  digestCanonicalJson(value: unknown): Uint8Array {
    const digest = Uint8Array.from(wasm.digestCanonicalJson(value));
    if (digest.byteLength !== 32) throw new TypeError("native canonical digest has an invalid length");
    return digest;
  }
}

/** Strip executable parser/handler members before crossing the serde WASM ABI. */
function nativeToolDefinition(
  definition: Pick<ToolDefinition, "name" | "revision" | "description" | "inputSchema" | "outputSchema" | "projection">,
): Readonly<{ name: string; revision: string; description: string; inputSchema: ToolJsonSchema; outputSchema: ToolJsonSchema; projectionSchema: ToolJsonSchema }> {
  return {
    name: definition.name,
    revision: definition.revision,
    description: definition.description,
    inputSchema: definition.inputSchema,
    outputSchema: definition.outputSchema,
    projectionSchema: definition.projection.schema,
  };
}

/** Rust owns typed integer projection; JS only unwraps bytes and maps. */
function normalizeNativeValue(value: unknown, safeJsonNumbers = false, preserveLargeBigInts = false): unknown {
  if (typeof value === "bigint") {
    if (!safeJsonNumbers) return value;
    const exact = Number(value);
    return preserveLargeBigInts && !Number.isSafeInteger(exact) ? value : boundedJsonNumber(value);
  }
  if (typeof value === "number" && safeJsonNumbers
    && (!Number.isFinite(value) || Object.is(value, -0)
      || (Number.isInteger(value) && !Number.isSafeInteger(value)))) {
    throw new TypeError("provider JSON contains an inexact number");
  }
  if (value instanceof Uint8Array) return [...value];
  if (Array.isArray(value)) return value.map(child => normalizeNativeValue(child, safeJsonNumbers, preserveLargeBigInts));
  if (value instanceof Map) {
    const entries: [string, unknown][] = [];
    for (const [key, child] of value) {
      if (typeof key !== "string") throw new TypeError("native JSON map has an invalid key");
      entries.push([key, normalizeNativeValue(child, safeJsonNumbers, preserveLargeBigInts)]);
    }
    return Object.fromEntries(entries);
  }
  if (value !== null && typeof value === "object") {
    return Object.fromEntries(Object.entries(value).map(([key, child]) => [key,
      normalizeNativeValue(child, safeJsonNumbers, preserveLargeBigInts)] as const));
  }
  return value;
}
function boundedJsonNumber(value: bigint): number {
  const exact = Number(value);
  if (!Number.isSafeInteger(exact)) throw new TypeError("native JSON integer exceeds JavaScript precision");
  return exact;
}

function freezeNative<Value>(value: Value): Value {
  if (value !== null && typeof value === "object") {
    for (const child of Object.values(value)) freezeNative(child);
    Object.freeze(value);
  }
  return value;
}
