/** Explicitly initialized Rust contract validator with strongly typed v2 inputs. */
import initWasm, * as wasm from "../generated/wasm/acyclic_harness_wasm.js";
import type { InitInput, WasmReducer } from "../generated/wasm/acyclic_harness_wasm.js";
import type {
  Attachment, ConversationMessage, ConversationMessageId, ConversationPage, FileDescriptor, FileRef, Limits, MessageKind, ProviderRef, ReferencedAttachments, TaskOutcomeRecord, VolumeClass, VolumeRef,
} from "./conversation.js";
import type {
  ForkReport, ForkRequest, ForkSeed, ReferenceGrant, ResourceKind, ResourceRef, ResourceRevision,
} from "./fork.js";
import type { ExtensionRecord } from "./extension.js";
import type { ApprovalBinding, InteractionId, InteractionResolution, InteractionTicket } from "./interaction.js";
import type { ProjectMergeReceipt } from "./project.js";
import type { ToolJsonSchema, ToolJsonValue } from "./model.js";
import type { IdentityKind, IdentityKindMap, OperationId } from "./index.js";

interface ContractValues {
  readonly limits: Limits;
  readonly provider_ref: ProviderRef;
  readonly volume_ref: VolumeRef;
  readonly file_ref: FileRef;
  readonly file_descriptor: FileDescriptor;
  readonly resource_ref: ResourceRef;
  readonly extension_record: ExtensionRecord;
  readonly attachments: ReferencedAttachments;
  readonly conversation_message: ConversationMessage;
  readonly task_outcome: TaskOutcomeRecord;
  readonly resource_revision: ResourceRevision;
  readonly fork_request: ForkRequest;
  readonly fork_report: ForkReport;
  readonly fork_seed: ForkSeed;
  readonly reference_grant: ReferenceGrant;
  readonly interaction_ticket: InteractionTicket;
  readonly interaction_resolution: InteractionResolution;
  readonly approval_binding: ApprovalBinding;
  readonly project_merge_receipt: ProjectMergeReceipt;
}

type SimpleContract = Exclude<keyof ContractValues, "conversation_message" | "interaction_resolution">;
type FixedSimpleContract = Exclude<SimpleContract, "provider_ref" | "volume_ref" | "file_ref" | "resource_ref">;
const REQUIRED_NATIVE_EXPORTS = [
  "validateContract", "verifyFileBytes", "decodeAttachmentManifest",
  "encodeAttachmentManifest", "forkSeedFromReport", "validateToolValue",
  "validateConversationMessageId", "validateIdentity", "deriveOperationUuid",
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

  validateConversationMessageId(value: string): ConversationMessageId {
    return this.native.validateConversationMessageId(value) as ConversationMessageId;
  }

  validateIdentity<Kind extends IdentityKind>(kind: Kind, value: string): IdentityKindMap[Kind] {
    return this.native.validateIdentity(kind, value) as IdentityKindMap[Kind];
  }

  deriveOperationId(operation: OperationId, label: string): OperationId {
    return this.native.deriveOperationUuid(operation, label) as OperationId;
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

/** Rust owns typed integer projection; JS only unwraps bytes and maps. */
function normalizeNativeValue(value: unknown, safeJsonNumbers = false): unknown {
  if (typeof value === "bigint") {
    return safeJsonNumbers ? boundedJsonNumber(value) : value;
  }
  if (typeof value === "number" && safeJsonNumbers
    && (!Number.isFinite(value) || Object.is(value, -0)
      || (Number.isInteger(value) && !Number.isSafeInteger(value)))) {
    throw new TypeError("provider JSON contains an inexact number");
  }
  if (value instanceof Uint8Array) return [...value];
  if (Array.isArray(value)) return value.map(child => normalizeNativeValue(child, safeJsonNumbers));
  if (value instanceof Map) {
    const entries: [string, unknown][] = [];
    for (const [key, child] of value) {
      if (typeof key !== "string") throw new TypeError("native JSON map has an invalid key");
      entries.push([key, normalizeNativeValue(child, safeJsonNumbers)]);
    }
    return Object.fromEntries(entries);
  }
  if (value !== null && typeof value === "object") {
    return Object.fromEntries(Object.entries(value).map(([key, child]) => [key,
      normalizeNativeValue(child, safeJsonNumbers)] as const));
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
