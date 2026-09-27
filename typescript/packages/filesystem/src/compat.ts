/** Public control-plane contracts shared by native, hosted, and plugin adapters. */

import type {
  FsWorkspace,
  WorkspaceRebaseOptions,
  WorkspaceRebaseResult,
} from "./contracts.js";
import {
  GIT_COMPAT_ACTION_VARIANTS,
  GIT_COMPAT_BYTE_FIELDS,
  GIT_COMPAT_DIRTY_STATE_WIRE,
  GIT_COMPAT_OPAQUE_PATHS,
  GIT_COMPAT_OUTPUT_VARIANTS,
  GIT_COMPAT_PENDING_FIELDS,
  GIT_COMPAT_PUBLIC_ALIASES,
  GIT_COMPAT_TIMESTAMP_FIELDS,
  GIT_COMPAT_TRANSITION_IDENTITY_BYTES,
  GIT_COMPAT_UUID_PATHS,
} from "../generated/git-compat-contract.js";
import type {
  GenerationIdentity,
  GitCompatCommand,
  GitCompatOutput,
  GitCompatRepository,
  GitFilesystemExecutor,
  GitFilesystemResult,
  GitPendingTransition,
  OperationIdentity,
  WorkspaceContextIdentity,
  WorkspaceIdentity,
  WorkspaceRootIdentity,
} from "../generated/git-compat-contract.js";
export type {
  GenerationIdentity,
  GitBisectResult,
  GitBranch,
  GitCaptureProof,
  GitCommit,
  GitCommitIdentity,
  GitCompatCommand,
  GitCompatOutput,
  GitCompatRepository,
  GitCompatStatus,
  GitDirtyState,
  GitFilesystemAction,
  GitFilesystemExecutor,
  GitFilesystemResult,
  GitGenerationRef,
  GitPendingTransition,
  GitResetMode,
  GitTreeRef,
  OperationIdentity,
  WorkspaceContextIdentity,
  WorkspaceIdentity,
  WorkspaceRootIdentity,
} from "../generated/git-compat-contract.js";

const gitCompatOutputVariants = [...GIT_COMPAT_OUTPUT_VARIANTS] as const;
const gitCompatActionVariants = [...GIT_COMPAT_ACTION_VARIANTS] as const;
const gitCompatDirtyStateByWire = new Map(
  Object.entries(GIT_COMPAT_DIRTY_STATE_WIRE).map(([typescript, wire]) => [wire, typescript]),
);

/** JSON values accepted by the versioned Rust compatibility boundary. */
export type CompatibilityJson =
  | null
  | boolean
  | number
  | string
  | readonly CompatibilityJson[]
  | { readonly [key: string]: CompatibilityJson };

export type CompatibilityWireKind =
  | "merge-plan"
  | "merge-candidate"
  | "multi-root-plan"
  | "multi-root-candidate"
  | "publication";

/** Forward-compatible envelope. Rust remains the schema authority. */
export interface CompatibilityWireEnvelope {
  readonly version: number;
  readonly kind: CompatibilityWireKind;
  readonly payload: CompatibilityJson;
  readonly [key: string]: CompatibilityJson;
}

/** Immutable merge/publication codec shared by N-API and WASM. */
export interface CompatibilityWire {
  encode(kind: CompatibilityWireKind, value: CompatibilityJson): CompatibilityWireEnvelope;
  decode(kind: CompatibilityWireKind, envelope: CompatibilityWireEnvelope): CompatibilityJson;
}

export interface RawCompatibilityWire {
  encodeMergePlanJson(valueJson: string): string;
  decodeMergePlanJson(valueJson: string): string;
  encodeMergeCandidateJson(valueJson: string): string;
  decodeMergeCandidateJson(valueJson: string): string;
  encodeMultiRootPlanJson(valueJson: string): string;
  decodeMultiRootPlanJson(valueJson: string): string;
  encodeMultiRootCandidateJson(valueJson: string): string;
  decodeMultiRootCandidateJson(valueJson: string): string;
  encodePublicationJson(valueJson: string): string;
  decodePublicationJson(valueJson: string): string;
}

/** Decodes an exact byte identity without JavaScript's truncating coercions. */
export function decodeFixedBytes(value: unknown, length: number, name: string): Uint8Array {
  if (
    !Array.isArray(value) ||
    value.length !== length ||
    value.some(byte => !Number.isInteger(byte) || byte < 0 || byte > 255)
  ) {
    throw new TypeError(`${name} must be a ${length}-byte identity`);
  }
  return Uint8Array.from(value as number[]);
}

function compatibilityJson(value: unknown, name: string): CompatibilityJson {
  if (
    value === null ||
    typeof value === "boolean" ||
    typeof value === "string" ||
    (typeof value === "number" && Number.isFinite(value))
  ) return value;
  if (Array.isArray(value)) {
    return value.map((entry, index) => compatibilityJson(entry, `${name}[${index}]`));
  }
  if (
    typeof value === "object" &&
    (Object.getPrototypeOf(value) === Object.prototype || Object.getPrototypeOf(value) === null)
  ) {
    return Object.fromEntries(
      Object.entries(value).map(([key, entry]) => [key, compatibilityJson(entry, `${name}.${key}`)]),
    );
  }
  throw new TypeError(`${name} is not finite JSON data`);
}

function isCompatibilityObject(
  value: CompatibilityJson,
): value is { readonly [key: string]: CompatibilityJson } {
  return value !== null && typeof value === "object" && !Array.isArray(value);
}

function compatibilityEnvelope(value: unknown, kind: CompatibilityWireKind): CompatibilityWireEnvelope {
  const decoded = compatibilityJson(value, "compatibility envelope");
  if (
    !isCompatibilityObject(decoded) ||
    decoded.kind !== kind ||
    typeof decoded.version !== "number" ||
    !Number.isSafeInteger(decoded.version) ||
    decoded.version <= 0 ||
    !("payload" in decoded)
  ) {
    throw new TypeError("Rust returned an invalid envelope for compatibility data");
  }
  return decoded as CompatibilityWireEnvelope;
}

/** Adapts generated bindings without reimplementing Rust validation semantics. */
export function adaptCompatibilityWire(raw: RawCompatibilityWire): CompatibilityWire {
  const functions = {
    "merge-plan": [raw.encodeMergePlanJson, raw.decodeMergePlanJson],
    "merge-candidate": [raw.encodeMergeCandidateJson, raw.decodeMergeCandidateJson],
    "multi-root-plan": [raw.encodeMultiRootPlanJson, raw.decodeMultiRootPlanJson],
    "multi-root-candidate": [
      raw.encodeMultiRootCandidateJson,
      raw.decodeMultiRootCandidateJson,
    ],
    publication: [raw.encodePublicationJson, raw.decodePublicationJson],
  } as const;
  return {
    encode(kind, value) {
      const encoded = functions[kind][0](JSON.stringify(value));
      return compatibilityEnvelope(JSON.parse(encoded) as unknown, kind);
    },
    decode(kind, envelope) {
      compatibilityEnvelope(envelope, kind);
      return compatibilityJson(
        JSON.parse(functions[kind][1](JSON.stringify(envelope))) as unknown,
        "decoded compatibility value",
      );
    },
  };
}

export interface WorkspaceLineageRecord {
  readonly version: number;
  readonly revision: bigint;
  readonly workspaceId: WorkspaceIdentity;
  readonly workspaceName: string;
  readonly parentWorkspaceId: WorkspaceIdentity | undefined;
  readonly parentWorkspaceName: string | undefined;
  readonly forkGeneration: GenerationIdentity;
  readonly initialGeneration: GenerationIdentity;
}

/** Recursive workspace lifecycle. Publication is authorized only to the direct parent. */
export interface WorkspaceGraph {
  registerRoot(workspace: FsWorkspace): Promise<WorkspaceLineageRecord>;
  fork(
    parent: FsWorkspace,
    destination: string,
    idempotencyKey?: OperationIdentity,
  ): Promise<FsWorkspace>;
  authorizeJoin(
    childWorkspaceId: WorkspaceIdentity,
    parentWorkspaceId: WorkspaceIdentity,
  ): Promise<WorkspaceLineageRecord>;
  ancestors(workspaceId: WorkspaceIdentity, maximum: number): Promise<readonly WorkspaceLineageRecord[]>;
}

export type WorkspaceContextState = "active" | "frozen" | "discarded";

/** One stable physical-root binding inside a recursively forkable context. */
export interface WorkspaceContextRoot {
  readonly rootId: WorkspaceRootIdentity;
  readonly sourcePath: string;
  readonly workspaceId: WorkspaceIdentity;
  readonly workspaceName: string;
  readonly parentWorkspaceId: WorkspaceIdentity | undefined;
  readonly mountPath: string | undefined;
}

/** Agent-neutral set of root workspaces with exactly one publication parent. */
export interface WorkspaceContext {
  readonly version: number;
  readonly revision: bigint;
  readonly contextId: WorkspaceContextIdentity;
  readonly parentContextId: WorkspaceContextIdentity | undefined;
  readonly roots: readonly WorkspaceContextRoot[];
  readonly state: WorkspaceContextState;
}

/** Durable recursive context registry. It never enumerates filesystem contents. */
export interface WorkspaceContextRegistry {
  registerRoot(
    contextId: WorkspaceContextIdentity,
    roots: readonly WorkspaceContextRoot[],
  ): Promise<WorkspaceContext>;
  registerChild(
    contextId: WorkspaceContextIdentity,
    parentContextId: WorkspaceContextIdentity,
    roots: readonly WorkspaceContextRoot[],
  ): Promise<WorkspaceContext>;
  adoptRoot(contextId: WorkspaceContextIdentity, root: WorkspaceContextRoot): Promise<WorkspaceContext>;
  removeRoot(contextId: WorkspaceContextIdentity, rootId: WorkspaceRootIdentity): Promise<WorkspaceContext>;
  resolve(contextId: WorkspaceContextIdentity): Promise<WorkspaceContext>;
  setActive(contextId: WorkspaceContextIdentity, active: boolean): Promise<WorkspaceContext>;
  setWorkspace(
    contextId: WorkspaceContextIdentity,
    rootId: WorkspaceRootIdentity,
    workspaceId: WorkspaceIdentity,
    workspaceName: string,
    parentWorkspaceId?: WorkspaceIdentity,
  ): Promise<WorkspaceContext>;
  discardSubtree(
    parentContextId: WorkspaceContextIdentity,
    childContextId: WorkspaceContextIdentity,
    maximum: number,
  ): Promise<readonly WorkspaceContextIdentity[]>;
}

export interface OperationWindowLease {
  readonly workspaceId: WorkspaceIdentity;
  readonly leaseId: OperationIdentity;
  readonly pinnedParent: GenerationIdentity;
  readonly expiresAtMillis: bigint;
}

export type OperationWindowPhase =
  | { readonly kind: "idle" }
  | {
      readonly kind: "active";
      readonly pinnedParent: GenerationIdentity;
      readonly pendingParent: GenerationIdentity | undefined;
      readonly activeLeaseCount: number;
    }
  | {
      readonly kind: "reconciling";
      readonly ticket: OperationIdentity;
      readonly pinnedParent: GenerationIdentity;
      readonly pendingParent: GenerationIdentity | undefined;
    };

export type OperationWindowClose =
  | { readonly kind: "still-active"; readonly remaining: number }
  | { readonly kind: "already-closed" }
  | {
      readonly kind: "reconcile";
      readonly ticket: OperationIdentity;
      readonly pinnedParent: GenerationIdentity;
      readonly pendingParent: GenerationIdentity | undefined;
    };

export type WorkspaceOperationWindowClose =
  | { readonly kind: "still-active"; readonly remaining: number }
  | { readonly kind: "already-closed" }
  | { readonly kind: "reconciled"; readonly rebase: WorkspaceRebaseResult };

/** Durable overlapping tool leases; the final close owns one deferred rebase. */
export interface OperationWindowCoordinator {
  begin(
    workspaceId: WorkspaceIdentity,
    parent: GenerationIdentity,
    owner: string,
    nowMillis: bigint,
    expiresAtMillis: bigint,
  ): Promise<OperationWindowLease>;
  observeParent(workspaceId: WorkspaceIdentity, parent: GenerationIdentity): Promise<boolean>;
  finish(lease: OperationWindowLease, nowMillis: bigint): Promise<OperationWindowClose>;
  inspect(workspaceId: WorkspaceIdentity): Promise<OperationWindowPhase>;
  finishWorkspace(
    workspace: FsWorkspace,
    lease: OperationWindowLease,
    nowMillis: bigint,
    options: WorkspaceRebaseOptions,
  ): Promise<WorkspaceOperationWindowClose>;
  recoverWorkspace(
    workspace: FsWorkspace,
    nowMillis: bigint,
    options: WorkspaceRebaseOptions,
  ): Promise<WorkspaceRebaseResult | undefined>;
}

export type FilesystemConflictKind =
  | "text"
  | "binary"
  | "metadata"
  | "binding"
  | "rename"
  | "directory"
  | "symbolic-link"
  | "hard-link"
  | "special-payload";

export interface ConflictValue {
  readonly digest: Uint8Array | undefined;
  readonly bytes: Uint8Array | undefined;
  readonly metadata: Uint8Array | undefined;
}

export interface FilesystemConflict {
  readonly key: Uint8Array;
  readonly path: string | undefined;
  readonly kind: FilesystemConflictKind;
  readonly base: ConflictValue;
  readonly ours: ConflictValue;
  readonly theirs: ConflictValue;
}

export type MergeResolution =
  | { readonly kind: "base" }
  | { readonly kind: "ours" }
  | { readonly kind: "theirs" }
  | { readonly kind: "delete" }
  | { readonly kind: "replace-content"; readonly bytes: Uint8Array }
  | { readonly kind: "unresolved" };

/** Drivers see immutable inputs and return declarations; they never receive write authority. */
export interface MergeDriver {
  readonly name: string;
  readonly version: string;
  readonly deterministic: boolean;
  resolve(conflict: FilesystemConflict): Promise<MergeResolution> | MergeResolution;
}

/** Serializes the natural command shape for Rust's shared public projection. */
export function stringifyGitCompatCommand(command: GitCompatCommand): string {
  return JSON.stringify(command, (_key, value: unknown) => {
    if (typeof value === "bigint") return value.toString();
    if (value instanceof Uint8Array) return Array.from(value);
    return value;
  });
}

/** Reject timestamps that native JSON could persist but JavaScript could not read exactly. */
export function gitCompatSafeTimestamp(value: bigint): number {
  const number = Number(value);
  if (!Number.isSafeInteger(number)) {
    throw new RangeError("Git compatibility commit time must fit a safe JSON integer");
  }
  return number;
}

/** Decode and validate the native façade's stable JSON envelope. */
export function parseGitCompatOutputJson(json: string): GitCompatOutput {
  return projectGitCompatOutputJson(json);
}

/**
 * Rehydrates the Rust JSON projection for package callers. Rust has already
 * deserialized and validated this payload; this adapter only restores JS
 * ownership types (typed arrays, bigint, and omitted optional values) using
 * the generated field contract.
 */
export function projectGitCompatOutputJson(json: string): GitCompatOutput {
  const value = parseGitJson(json);
  if (value === "NoOp") return value;
  const output = object(value, "Git command output");
  const variants = Object.keys(output);
  if (variants.length !== 1 || !gitCompatOutputVariants.includes(variants[0] as typeof gitCompatOutputVariants[number])) {
    throw new TypeError("Git command output has an unknown variant");
  }
  if (variants[0] === "Status") {
    const status = object(output.Status, "Git Status output");
    for (const field of ["branch", "workspace", "dirty", "all_changes_staged"]) {
      if (!(field in status)) throw new TypeError(`Git Status output lacks ${field}`);
    }
  }
  if (variants[0] === "Bisect") {
    const bisect = object(output.Bisect, "Git Bisect output");
    if ("remaining" in bisect) u32(bisect.remaining, "bisect remaining");
  }
  if (variants[0] === "Text" && typeof output.Text !== "string") {
    throw new TypeError("Git Text output must be a string");
  }
  if (variants[0] === "Paths" && (!Array.isArray(output.Paths)
    || output.Paths.some(path => typeof path !== "string"))) {
    throw new TypeError("Git Paths output must be an array of strings");
  }
  if (variants[0] === "Action") {
    tagged(output.Action, gitCompatActionVariants, "Git action");
  } else if (variants[0] === "Prepared") {
    const envelope = object(output[variants[0]], `Git ${variants[0]} output`);
    tagged(envelope.action, gitCompatActionVariants, "Git action");
  }
  return hydrateGitCompatValue(output, "") as GitCompatOutput;
}

/** Rehydrates a pending Rust transition without reproducing its action union. */
export function projectGitPendingTransitionJson(json: string): GitPendingTransition {
  const value = object(parseGitJson(json), "pending transition");
  for (const field of GIT_COMPAT_PENDING_FIELDS) {
    if (!(field in value)) throw new TypeError(`Git pending transition lacks its ${field}`);
  }
  const projected = hydrateGitCompatValue(value, "pending") as Record<string, unknown>;
  if (!(projected.id instanceof Uint8Array) || projected.action === undefined) {
    throw new TypeError("Git pending transition projection is malformed");
  }
  tagged(value.action, gitCompatActionVariants, "Git pending action");
  return projected as unknown as GitPendingTransition;
}

/** Serializes a public executor result using the Rust wire field contract. */
export function stringifyGitFilesystemResultPublic(result: GitFilesystemResult): string {
  tagged(result, ["Captured", "Forked", "Applied", "Data"], "filesystem result");
  return JSON.stringify(serializeGitCompatValue(result, ""));
}

function hydrateGitCompatValue(value: unknown, parentPath: string): unknown {
  if (isOpaqueGitPath(parentPath)) return value;
  if (value === null) {
    return undefined;
  }
  if (parentPath === "Status.dirty") {
    if (typeof value !== "string") throw new TypeError("Git status dirty state must be a string");
    const natural = gitCompatDirtyStateByWire.get(value);
    if (natural === undefined) throw new TypeError(`Unknown Git dirty state: ${value}`);
    return natural;
  }
  if (typeof value !== "object") {
    if (GIT_COMPAT_TIMESTAMP_FIELDS.has(parentPath.split(".").at(-1) ?? "")) {
      return integer(value, `Git ${parentPath}`);
    }
    if (parentPath.split(".").at(-1) === "remaining") return u32(value, `Git ${parentPath}`);
    return value;
  }
  if (Array.isArray(value)) {
    const field = parentPath.split(".").at(-1) ?? "";
    const width = GIT_COMPAT_BYTE_FIELDS[field];
    if (width !== undefined) return Uint8Array.from(bytes(value, width ?? undefined, `Git ${parentPath}`));
    return value.map((item, index) => hydrateGitCompatValue(item, `${parentPath}[${index}]`));
  }
  const objectValue = value as Record<string, unknown>;
  const result: Record<string, unknown> = {};
  for (const [wireKey, nested] of Object.entries(objectValue)) {
    const path = parentPath ? `${parentPath}.${wireKey}` : wireKey;
    const contractPath = contractPathSuffix(path);
    if (GIT_COMPAT_UUID_PATHS.has(contractPath)) {
      result[publicAlias(path, wireKey)] = uuidIdentity(nested, `Git ${path}`);
      continue;
    }
    const projected = hydrateGitCompatValue(nested, path);
    if (projected !== undefined) result[publicAlias(path, wireKey)] = projected;
  }
  return result;
}

function isOpaqueGitPath(path: string): boolean {
  return [...GIT_COMPAT_OPAQUE_PATHS].some(candidate =>
    path === candidate || path.endsWith(`.${candidate}`) || path.endsWith(`${candidate}.value`));
}

function contractPathSuffix(path: string): string {
  if (path.endsWith(".proof.operation_id")) return "capture_proof.operation_id";
  for (const candidate of [...GIT_COMPAT_UUID_PATHS, ...GIT_COMPAT_OPAQUE_PATHS]) {
    if (path === candidate || path.endsWith(`.${candidate}`)) return candidate;
  }
  return path;
}

function publicAlias(path: string, wireKey: string): string {
  for (const [wirePath, publicKey] of Object.entries(GIT_COMPAT_PUBLIC_ALIASES)) {
    if (path === wirePath || path.endsWith(`.${wirePath}`)) return publicKey;
  }
  return wireKey;
}

function serializeGitCompatValue(value: unknown, parentPath: string): unknown {
  if (value === undefined) return null;
  if (typeof value === "bigint") {
    if (isOpaqueGitValuePath(parentPath)) {
      throw new TypeError(`Git ${parentPath} opaque data must be JSON-safe`);
    }
    if (value <= BigInt(Number.MAX_SAFE_INTEGER) && value >= BigInt(Number.MIN_SAFE_INTEGER)) {
      return Number(value);
    }
    const rawJson = (JSON as typeof JSON & { rawJSON?: (text: string) => unknown }).rawJSON;
    if (rawJson === undefined) throw new RangeError(`Git ${parentPath} needs lossless JSON serialization`);
    return rawJson(value.toString());
  }
  if (value instanceof Uint8Array) {
    const contractPath = contractPathSuffix(parentPath);
    if (GIT_COMPAT_UUID_PATHS.has(contractPath)) return uuidString(value, `Git ${parentPath}`);
    return Array.from(value);
  }
  if (Array.isArray(value)) return value.map((item, index) => serializeGitCompatValue(item, `${parentPath}[${index}]`));
  if (typeof value === "object" && value !== null) {
    return Object.fromEntries(Object.entries(value).map(([key, nested]) => [
      key,
      serializeGitCompatValue(nested, parentPath ? `${parentPath}.${key}` : key),
    ]));
  }
  return value;
}

function isOpaqueGitValuePath(path: string): boolean {
  return path === "Data.value"
    || path.startsWith("Data.value.")
    || path.endsWith(".Filesystem.Data.value")
    || path.includes(".Filesystem.Data.value.");
}

/** Decode one durable pending transition returned by the native store. */
export function parseGitPendingTransitionJson(json: string): GitPendingTransition {
  return projectGitPendingTransitionJson(json);
}

/** Encode an executor result without relying on `Uint8Array`'s object-shaped JSON form. */
export function stringifyGitFilesystemResult(result: GitFilesystemResult): string {
  return stringifyGitFilesystemResultPublic(result);
}

/** Execute and complete one durable action with Rust-equivalent return semantics. */
export async function finishGitCompatOutput(
  repository: Pick<GitCompatRepository, "completeTransitionResult">,
  output: GitCompatOutput,
  executor: GitFilesystemExecutor,
): Promise<GitCompatOutput> {
  if (typeof output === "object" && "Prepared" in output) {
    const { transition, action } = output.Prepared;
    const result = await executor.execute(transition, action);
    const completed = await repository.completeTransitionResult(transition, result);
    return completed === "NoOp" ? { Filesystem: result } : completed;
  }
  if (typeof output === "object" && "Action" in output) {
    throw new Error("Git compatibility returned an action without a durable transition");
  }
  return output;
}

function tagged<const K extends string>(
  value: unknown,
  variants: readonly K[],
  label: string,
): readonly [K, unknown] {
  const record = object(value, label);
  const keys = Object.keys(record);
  if (keys.length !== 1 || !variants.includes(keys[0] as K)) {
    throw new TypeError(`Git ${label} has an unknown or ambiguous variant`);
  }
  const kind = keys[0] as K;
  return [kind, record[kind]];
}

function object(value: unknown, label: string): Record<string, unknown> {
  if (value === null || typeof value !== "object" || Array.isArray(value)) {
    throw new TypeError(`Git ${label} must be an object`);
  }
  return value as Record<string, unknown>;
}

function bytes(value: unknown, length: number | undefined, label: string): number[] {
  const values = value instanceof Uint8Array ? Array.from(value) : value;
  if (!Array.isArray(values) || (length !== undefined && values.length !== length)
    || values.some((byte) => !Number.isInteger(byte) || byte < 0 || byte > 255)) {
    throw new TypeError(`Git ${label} must be ${length ?? "a"} byte array`);
  }
  return values as number[];
}

// Lazy source epochs are Rust u64 values. Read the original JSON token before
// JavaScript rounds it; older engines fail closed on an unsafe numeric value.
function parseGitJson(json: string): unknown {
  return JSON.parse(json, (key: string, value: unknown, context?: { source?: string }) => {
    if (key !== "epoch" || typeof value !== "number") return value;
    const source = context?.source;
    if (source === undefined) {
      if (!Number.isSafeInteger(value)) throw new RangeError("Git source epoch needs lossless JSON parsing");
      return BigInt(value);
    }
    if (!/^(0|[1-9][0-9]*)$/.test(source)) throw new TypeError("Git source epoch must be a u64");
    return u64(BigInt(source), "source epoch");
  }) as unknown;
}

function u64(value: unknown, label: string): bigint {
  const epoch = typeof value === "bigint"
    ? value
    : typeof value === "number" && Number.isSafeInteger(value)
      ? BigInt(value)
      : undefined;
  if (epoch === undefined || epoch < 0n || epoch > 18_446_744_073_709_551_615n) {
    throw new TypeError(`Git ${label} must be a u64`);
  }
  return epoch;
}

// Rust's transparent OperationId wraps uuid::Uuid: JSON uses its canonical
// string form, while the native N-API boundary accepts the same 16 raw bytes.
function uuidIdentity(value: unknown, label: string): OperationIdentity {
  if (typeof value !== "string" || !/^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/i.test(value)) {
    throw new TypeError(`Git ${label} must be a UUID string`);
  }
  const hex = value.replaceAll("-", "");
  return Uint8Array.from({
    length: GIT_COMPAT_TRANSITION_IDENTITY_BYTES,
  }, (_, index) => Number.parseInt(hex.slice(index * 2, index * 2 + 2), 16));
}

function uuidString(value: unknown, label: string): string {
  const hex = Array.from(
    bytes(value, GIT_COMPAT_TRANSITION_IDENTITY_BYTES, label),
    (byte) => byte.toString(16).padStart(2, "0"),
  ).join("");
  return `${hex.slice(0, 8)}-${hex.slice(8, 12)}-${hex.slice(12, 16)}-${hex.slice(16, 20)}-${hex.slice(20)}`;
}

function integer(value: unknown, label: string): number {
  if (typeof value !== "number" || !Number.isSafeInteger(value)) {
    throw new TypeError(`Git ${label} must be a safe integer`);
  }
  return value;
}

function u32(value: unknown, label: string): number {
  const result = integer(value, label);
  if (result < 0 || result > 0xffff_ffff) {
    throw new TypeError(`Git ${label} must be a u32`);
  }
  return result;
}
