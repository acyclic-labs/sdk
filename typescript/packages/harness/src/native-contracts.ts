/** Explicitly initialized Rust contract validator with strongly typed v2 inputs. */
import initWasm, * as wasm from "../generated/wasm/acyclic_harness_wasm.js";
import type {
  InitInput, WasmBatchAdmissionInput, WasmDurableBatchWire, WasmReducer, WasmToolJsonValue,
  WasmTaskAdmissionIdentities, WasmTaskAdmissionInput, WasmTaskAdmissionWire,
  WasmTaskIdentityInput,
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
import type { BatchAdmissionRequest, BatchId, GroupId, PrivateDirectoryPage, RuntimeTaskId } from "./runtime.js";
import type { ModelEvent, ToolDefinition, ToolJsonSchema, ToolJsonValue, ToolInvocation, ToolResult } from "./model.js";
import type { IdentityKind, IdentityKindMap, OperationId } from "./index.js";

/** Rust generated admission projection input and output shapes. */
export type TaskAdmissionProjectionInput = WasmTaskAdmissionInput;
export type BatchAdmissionProjectionInput = WasmBatchAdmissionInput;
/** JSON value accepted by the Rust admission ABI after schema validation. */
export type NativeJsonValue = WasmToolJsonValue;
export type TaskAdmissionWire = WasmTaskAdmissionWire;
export type DurableBatchWire = WasmDurableBatchWire;
export type TaskAdmissionIdentities = WasmTaskAdmissionIdentities;

/** Exact serde shape admitted by Rust `DurableBatchRequest`; hosts retain this value. */
export interface ExecutionPlacementWire {
  readonly provider: MachineIdentityWire;
  readonly build: ResourceRef<"artifact">;
  readonly environment: ResourceRef<"sandbox"> | null;
  readonly readiness_revision: readonly number[];
}

export interface TaskRunLimitsWire {
  /** Rust `usize`/`u64`; native admission preserves the full-width integer. */
  readonly concurrency: bigint | null;
  readonly max_steps: bigint | null;
  readonly deadline_epoch_ms: bigint | null;
}

/** Rust `Limits` as returned by an admitted task/batch envelope. */
export interface NativeLimitsWire {
  readonly file_bytes: bigint;
  readonly path_bytes: bigint;
  readonly attachments: bigint;
  readonly render_bytes: bigint;
  readonly model_steps: bigint;
  readonly model_events_per_step: bigint;
  readonly tool_calls_per_step: bigint;
  readonly context_messages: bigint;
}

/** Rust-owned per-step model event admission state with cumulative text bytes. */
export interface ModelEventAdmissionState {
  readonly count: number;
  readonly calls: readonly string[];
  readonly completed: boolean;
  readonly text_bytes: number;
}

/** Detached model event paired with its Rust admission state. */
export interface ModelEventAdmission {
  readonly event: ModelEvent;
  readonly state: ModelEventAdmissionState;
}

/** Serde shape returned by the canonical Rust context projector. */
export type NativeFileRef = Omit<FileRef, "descriptor"> & Readonly<{
  descriptor: Omit<FileDescriptor, "byte_length"> & Readonly<{ byte_length: number | bigint }>;
}>;
export type NativeModelContentPart =
  | Readonly<{ kind: "text"; text: string }>
  | Readonly<{ kind: "file"; file: NativeFileRef; policy: "reference" | "bounded_full" | "native" }>
  | Readonly<{ kind: "tool_call"; call_id: string; name: string; arguments: unknown }>
  | Readonly<{ kind: "tool_result"; call_id: string; name: string; value: unknown }>;
export type NativeModelContent = string | NativeModelContentPart | readonly NativeModelContentPart[];
export interface NativeSelectedModelContext {
  readonly selection: Readonly<{
    readonly conversation_revision: bigint;
    readonly message_ids: readonly ConversationMessageId[];
  }>;
  readonly messages: readonly Readonly<{
    role: "system" | "user" | "assistant" | "tool";
    content: NativeModelContent;
  }>[];
}

export interface MachineIdentityWire {
  readonly name: string;
  readonly version: string;
  readonly digest: readonly number[];
}

/** Exact Rust serde shape for a pinned resumable-tool machine admission. */
export interface WorkflowAdmissionWire {
  readonly operation_id: string;
  readonly request_digest: readonly number[];
  readonly initial: Readonly<{
    machine: MachineIdentityWire;
    revision: bigint;
    state: unknown;
  }>;
}

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
  readonly execution_placement: ExecutionPlacementWire;
  readonly task_admission: TaskAdmissionWire;
  readonly workflow_admission: WorkflowAdmissionWire;
  readonly machine_identity: MachineIdentityWire;
  readonly durable_batch_request: DurableBatchWire;
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

type SimpleContract = Exclude<keyof ContractValues, "conversation_message" | "interaction_resolution">;
type FixedSimpleContract = Exclude<SimpleContract, "provider_ref" | "volume_ref" | "file_ref" | "resource_ref">;
const REQUIRED_NATIVE_EXPORTS = [
  "validateContract", "verifyFileBytes", "decodeAttachmentManifest",
  "encodeAttachmentManifest", "forkSeedFromReport", "validateToolValue",
  "validateToolDefinition", "validateToolInvocation", "validateToolResult",
  "validateModelContent", "validateUserInput", "admitModelEvent", "selectModelContext",
  "validateModelContextSelection",
  "validateWireHandshake", "validateWireCommand", "validateWireCommandProtocol",
  "validateWireResume", "validateWireObserve", "validateWireCancel",
  "validateWireAdmission", "validateWireStatus", "validateWireCancellation",
  "validateConversationMessageId", "validateIdentity", "deriveOperationUuid", "batchMemberOperationId", "taskIdentityDigest",
  "taskAdmissionIdentities", "admitTask", "admitBatch", "admitBatchRequest",
  "fileDescriptor", "uuidFromDigestHalf", "decodeCanonicalJson", "decodeJson",
  "encodeCanonicalJson", "digestCanonicalJson",
] as const satisfies readonly (keyof typeof wasm)[];
type NativeExports = Pick<typeof wasm, typeof REQUIRED_NATIVE_EXPORTS[number]>;

/** Rust performs admission validation; TypeScript preserves the exact public shape. */
export class NativeContracts {
  static #default: Promise<NativeContracts> | undefined;
  private constructor(private readonly native: NativeExports) {}

  /** Initialize once before using synchronous contract methods. */
  static create(module?: InitInput): Promise<NativeContracts> {
    if (module !== undefined) return this.#initialize(module);
    return this.#default ??= this.#initialize().catch(error => {
      this.#default = undefined;
      throw error;
    });
  }

  static async #initialize(module?: InitInput): Promise<NativeContracts> {
    if (module === undefined) {
      const bundled = new URL("../generated/wasm/acyclic_harness_wasm_bg.wasm", import.meta.url);
      if (bundled.protocol === "file:") {
        const filesystem: string = "node:fs/promises";
        const { readFile } = await import(filesystem) as { readFile(url: URL): Promise<Uint8Array> };
        await initWasm({ module_or_path: await readFile(bundled) });
      } else await initWasm();
    }
    else await initWasm({ module_or_path: module });
    const native: NativeExports = wasm;
    if (REQUIRED_NATIVE_EXPORTS.some(name => typeof native[name] !== "function")) {
      throw new Error("Harness WASM contracts are unavailable");
    }
    return new NativeContracts(native);
  }

  validate<Family extends string>(kind: "provider_ref", value: ProviderRef<Family>): ProviderRef<Family>;
  validate<Class extends VolumeClass, Family extends string>(kind: "volume_ref", value: VolumeRef<Class, Family>): VolumeRef<Class, Family>;
  validate<Class extends VolumeClass, Family extends string>(kind: "file_ref", value: FileRef<Class, Family>): FileRef<Class, Family>;
  validate<Kind extends ResourceKind>(kind: "resource_ref", value: ResourceRef<Kind>): ResourceRef<Kind>;
  validate<K extends FixedSimpleContract>(kind: K, value: ContractValues[K]): ContractValues[K];
  validate<Kind extends MessageKind>(kind: "conversation_message", value: ConversationMessage<Kind>, limits: Limits): ConversationMessage<Kind>;
  validate(kind: "interaction_resolution", value: InteractionResolution,
    ticket: InteractionTicket): InteractionResolution;
  validate(kind: keyof ContractValues, value: ContractValues[keyof ContractValues],
    context?: Limits | InteractionTicket): ContractValues[keyof ContractValues] {
    // Contract envelopes retain Rust u64/usize values as BigInt. Converting
    // them to Number here would silently change the wire shape and can lose
    // precision on retry/reconciliation paths.
    const admitted = normalizeNativeValue(this.native.validateContract(kind, value, context ?? null));
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
    this.native.verifyFileBytes(file, bytes);
  }

  decodeAttachmentManifest(manifest: FileRef, bytes: Uint8Array, itemCount: number): readonly Attachment[] {
    if (!Number.isSafeInteger(itemCount) || itemCount < 0 || itemCount > 65_536) {
      throw new TypeError("attachment count is outside the protocol limit");
    }
    return freezeNative(normalizeNativeValue(this.native.decodeAttachmentManifest(manifest, bytes, itemCount))) as readonly Attachment[];
  }

  /** Rust's typed serde serialization is the manifest byte contract. */
  encodeAttachmentManifest(items: readonly Attachment[]): Uint8Array {
    return Uint8Array.from(this.native.encodeAttachmentManifest(items));
  }

  forkSeed(report: ForkReport): ForkSeed {
    return freezeNative(normalizeNativeValue(this.native.forkSeedFromReport(report))) as ForkSeed;
  }

  /** The Rust tool registry's JSON Schema admission, before a typed parser runs. */
  validateToolValue(schema: ToolJsonSchema, value: unknown): unknown {
    return normalizeNativeValue(this.native.validateToolValue(schema, value), true);
  }

  /** Rust owns the complete model-visible tool definition contract. */
  validateToolDefinition(definition: Pick<ToolDefinition, "name" | "revision" | "description" | "inputSchema" | "outputSchema">): void {
    this.native.validateToolDefinition(nativeToolDefinition(definition));
  }

  /** Rust owns tool invocation identity and argument schema validation. */
  validateToolInvocation(
    definition: Pick<ToolDefinition, "name" | "revision" | "description" | "inputSchema" | "outputSchema">,
    invocation: Pick<ToolInvocation, "callId" | "name" | "arguments">,
  ): void {
    this.native.validateToolInvocation(nativeToolDefinition(definition), invocation);
  }

  /** Rust owns tool result output schema validation. */
  validateToolResult(
    definition: Pick<ToolDefinition, "name" | "revision" | "description" | "inputSchema" | "outputSchema">,
    result: ToolResult,
  ): void {
    this.native.validateToolResult(nativeToolDefinition(definition), result);
  }

  /** Rust owns model stream event, tool-call, completion, and UTF-8 byte admission. */
  admitModelEvent(event: ModelEvent, limits: Limits, state?: ModelEventAdmissionState): ModelEventAdmission {
    // Model arguments and completion metadata are provider JSON and may carry
    // full-width integers. Preserve those BigInts while normalizing the
    // bounded admission counters below to the public Number state shape.
    const admitted = normalizeNativeValue(this.native.admitModelEvent(event, limits, state ?? null), true, true) as ModelEventAdmission;
    const admittedState = normalizeNativeValue(admitted.state, true) as ModelEventAdmissionState;
    if (admitted.event === null || typeof admitted.event !== "object"
      || !Number.isSafeInteger(admittedState.count) || admittedState.count < 0
      || !Number.isSafeInteger(admittedState.text_bytes) || admittedState.text_bytes < 0
      || !Array.isArray(admittedState.calls) || typeof admittedState.completed !== "boolean") {
      throw new TypeError("native model event admission returned an invalid state");
    }
    return freezeNative({ ...admitted, state: { ...admittedState, calls: [...admittedState.calls] } });
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
    return this.native.selectModelContext(
      conversation, selection, files, maximumMessages, maximumAttachments, maximumRenderBytes,
      maximumProjectedAttachments,
    ).then(value => normalizeNativeValue(value) as NativeSelectedModelContext);
  }

  /** Runs canonical selection/linkage checks before owner-mediated reads. */
  validateModelContextSelection(
    conversation: Readonly<{ agent: string | null; messages: readonly ConversationMessage[] }>,
    selection: Readonly<{ conversation_revision: bigint; message_ids: readonly ConversationMessageId[] }>,
  ): void {
    this.native.validateModelContextSelection(conversation, selection);
  }

  validateWireHandshake(request: Uint8Array, response: Uint8Array): Uint8Array {
    return Uint8Array.from(this.native.validateWireHandshake(request, response));
  }
  validateWireCommand(command: Uint8Array): Uint8Array {
    return Uint8Array.from(this.native.validateWireCommand(command));
  }
  validateWireCommandProtocol(command: Uint8Array): Uint8Array {
    return Uint8Array.from(this.native.validateWireCommandProtocol(command));
  }
  validateWireResume(request: Uint8Array): Uint8Array {
    return Uint8Array.from(this.native.validateWireResume(request));
  }
  validateWireObserve(request: Uint8Array): Uint8Array {
    return Uint8Array.from(this.native.validateWireObserve(request));
  }
  validateWireCancel(request: Uint8Array): Uint8Array {
    return Uint8Array.from(this.native.validateWireCancel(request));
  }
  validateWireAdmission(command: Uint8Array, admission: Uint8Array): Uint8Array {
    return Uint8Array.from(this.native.validateWireAdmission(command, admission));
  }
  validateWireStatus(request: Uint8Array, status: Uint8Array): Uint8Array {
    return Uint8Array.from(this.native.validateWireStatus(request, status));
  }
  validateWireCancellation(request: Uint8Array, response: Uint8Array): Uint8Array {
    return Uint8Array.from(this.native.validateWireCancellation(request, response));
  }

  validateConversationMessageId(value: string): ConversationMessageId {
    return this.native.validateConversationMessageId(value) as ConversationMessageId;
  }

  validateIdentity<Kind extends IdentityKind>(kind: Kind, value: string): IdentityKindMap[Kind] {
    return this.native.validateIdentity(kind, value) as IdentityKindMap[Kind];
  }

  deriveOperationId(operation: OperationId, label: string): OperationId {
    return this.native.deriveOperationUuid(operation, label) as OperationId;
  }

  batchMemberOperationId(group: string, batch: string, index: number): OperationId {
    if (!Number.isSafeInteger(index) || index < 0 || index > 65_535) throw new RangeError("batch index is out of range");
    return this.native.batchMemberOperationId(group, batch, index) as OperationId;
  }

  taskIdentityDigest(name: string, version: string, input: ToolJsonSchema, output: ToolJsonSchema,
    requirements: readonly string[], machineDigest: Uint8Array): readonly number[] {
    const digest = Uint8Array.from(this.native.taskIdentityDigest(name, version, input, output,
      [...requirements], Uint8Array.from(machineDigest)));
    if (digest.length !== 32) throw new TypeError("native task identity digest has an invalid length");
    return Object.freeze(Array.from(digest));
  }

  taskAdmissionIdentities(value: WasmTaskIdentityInput): TaskAdmissionIdentities {
    return freezeNative(normalizeTypedNativeValue(this.native.taskAdmissionIdentities({
      ...value,
      requirements: [...value.requirements],
      machine_digest: [...value.machine_digest],
    })));
  }

  /** Rust-owned durable task admission and canonical envelope projection. */
  admitTask(value: TaskAdmissionProjectionInput): TaskAdmissionWire {
    return freezeNative(normalizeTypedNativeValue(this.native.admitTask({
      ...value,
      requirements: [...value.requirements],
      machine_digest: [...value.machine_digest],
      grants: [...value.grants],
    })));
  }

  /** Rust-owned immutable batch request construction and validation. */
  admitBatch(value: BatchAdmissionProjectionInput): DurableBatchWire {
    return freezeNative(normalizeTypedNativeValue(this.native.admitBatch({
      ...value,
      requirements: [...value.requirements],
      machine_digest: [...value.machine_digest],
      inputs: [...value.inputs],
      grants: [...value.grants],
    })));
  }

  /** Rust-owned complete batch envelope, members, and canonical digest. */
  admitBatchRequest(value: BatchAdmissionProjectionInput): BatchAdmissionRequest {
    const projected = normalizeTypedNativeValue(this.native.admitBatchRequest({
      ...value,
      requirements: [...value.requirements],
      machine_digest: [...value.machine_digest],
      inputs: [...value.inputs],
      grants: [...value.grants],
    }));
    return freezeNative({
      ...projected,
      groupId: projected.groupId as GroupId,
      batchId: projected.batchId as BatchId,
      parentTaskId: projected.parentTaskId as RuntimeTaskId | null,
    });
  }

  idFromDigest(digest: Uint8Array, kind: "operation"): OperationId;
  idFromDigest(digest: Uint8Array, kind: "interaction"): InteractionId;
  idFromDigest(digest: Uint8Array, kind: "operation" | "interaction"): OperationId | InteractionId {
    const value = this.native.uuidFromDigestHalf(digest, kind === "interaction");
    return kind === "operation"
      ? this.validateIdentity("operation", value)
      : this.validateIdentity("interaction", value);
  }

  fileDescriptor(bytes: Uint8Array, mediaType: string): FileDescriptor {
    return freezeNative(normalizeNativeValue(this.native.fileDescriptor(Uint8Array.from(bytes), mediaType))) as FileDescriptor;
  }

  /** Rust JSON parser retains every full-width integer before any JS normalization. */
  decodeCanonicalJson(bytes: Uint8Array): unknown {
    return normalizeNativeValue(this.native.decodeCanonicalJson(bytes));
  }

  /** Untrusted provider JSON may not silently round integers into JS Number. */
  decodeModelJson(bytes: Uint8Array): ToolJsonValue {
    return normalizeNativeValue(this.native.decodeJson(bytes), true) as ToolJsonValue;
  }

  conversationPage(core: WasmReducer, afterSequence: bigint, limit: number): ConversationPage {
    return freezeNative(normalizeNativeValue(core.conversationPage(afterSequence, limit))) as ConversationPage;
  }

  encodeCanonicalJson(value: unknown): Uint8Array {
    return Uint8Array.from(this.native.encodeCanonicalJson(value));
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
    const digest = Uint8Array.from(this.native.digestCanonicalJson(value));
    if (digest.byteLength !== 32) throw new TypeError("native canonical digest has an invalid length");
    return digest;
  }
}

/** Strip executable parser/handler members before crossing the serde WASM ABI. */
function nativeToolDefinition(
  definition: Pick<ToolDefinition, "name" | "revision" | "description" | "inputSchema" | "outputSchema">,
): Readonly<{ name: string; revision: string; description: string; inputSchema: ToolJsonSchema; outputSchema: ToolJsonSchema }> {
  return {
    name: definition.name,
    revision: definition.revision,
    description: definition.description,
    inputSchema: definition.inputSchema,
    outputSchema: definition.outputSchema,
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
function normalizeTypedNativeValue<Value>(value: Value): Value {
  return normalizeNativeValue(value) as Value;
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
