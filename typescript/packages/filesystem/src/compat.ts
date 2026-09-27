/** Public control-plane contracts shared by native, hosted, and plugin adapters. */

import type {
  FsWorkspace,
  WorkspaceRebaseOptions,
  WorkspaceRebaseResult,
} from "./contracts.js";

export type WorkspaceIdentity = Uint8Array;
export type GenerationIdentity = Uint8Array;
export type OperationIdentity = Uint8Array;
export type WorkspaceContextIdentity = Uint8Array;
export type WorkspaceRootIdentity = Uint8Array;
/** Canonical lowercase BLAKE3 compatibility commit ID. */
export type GitCommitIdentity = string;

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

export type GitResetMode = "soft" | "mixed" | "hard";

export type GitCompatCommand =
  | { readonly kind: "status" }
  | { readonly kind: "diff"; readonly cached?: boolean }
  | { readonly kind: "log"; readonly maximum: number }
  | { readonly kind: "show"; readonly object?: string }
  | { readonly kind: "add"; readonly paths: readonly string[] }
  | {
      readonly kind: "commit";
      readonly message: string;
      readonly author: string;
      readonly authoredAtSeconds: bigint;
    }
  | { readonly kind: "branch"; readonly create?: string }
  | { readonly kind: "switch"; readonly branch: string; readonly create?: boolean }
  | { readonly kind: "restore"; readonly source?: string; readonly paths: readonly string[] }
  | { readonly kind: "reset"; readonly target: string; readonly mode: GitResetMode }
  | { readonly kind: "merge" | "rebase"; readonly branch: string }
  | { readonly kind: "stash-push" | "stash-pop" }
  | { readonly kind: "cherry-pick" | "revert"; readonly object: string }
  | { readonly kind: "tag"; readonly name?: string; readonly target?: string; readonly delete?: boolean }
  | { readonly kind: "blame"; readonly path: string }
  | { readonly kind: "grep"; readonly pattern: string; readonly path?: string }
  | { readonly kind: "clean"; readonly dryRun: boolean }
  | { readonly kind: "archive"; readonly object?: string }
  | { readonly kind: "apply"; readonly patch: Uint8Array }
  | { readonly kind: "bisect"; readonly arguments: readonly string[] }
  | { readonly kind: "rev-parse"; readonly argument: string }
  | { readonly kind: "symbolic-ref"; readonly short?: boolean }
  | { readonly kind: "merge-base"; readonly left: string; readonly right: string }
  | { readonly kind: "ls-files" }
  | { readonly kind: "check-ignore"; readonly paths: readonly string[] };

export interface GitCompatStatus {
  readonly branch: string;
  readonly head: GitCommitIdentity | undefined;
  readonly workspace: GitTreeRef;
  readonly dirty: "clean" | "dirty" | "unknown";
  readonly allChangesStaged: true;
}

export interface GitBisectResult {
  readonly active: boolean;
  readonly good: GitCommitIdentity | undefined;
  readonly bad: GitCommitIdentity | undefined;
  readonly current: GitCommitIdentity | undefined;
  readonly remaining: number;
  readonly first_bad: GitCommitIdentity | undefined;
}

export type GitCompatOutput =
  | "NoOp"
  | { readonly Status: GitCompatStatus }
  | { readonly Commits: readonly Readonly<Record<string, unknown>>[] }
  | { readonly Branches: Readonly<Record<string, unknown>> }
  | { readonly Tags: Readonly<Record<string, unknown>> }
  | { readonly Committed: Readonly<Record<string, unknown>> }
  | { readonly Bisect: GitBisectResult }
  | { readonly Action: GitFilesystemAction }
  | {
      readonly Prepared: {
        readonly transition: OperationIdentity;
        readonly action: GitFilesystemAction;
      };
    }
  | { readonly Filesystem: GitFilesystemResult }
  | { readonly Text: string }
  | { readonly Paths: readonly string[] };

export interface GitGenerationRef {
  readonly workspace_id: WorkspaceIdentity;
  readonly generation: GenerationIdentity;
}

/** Exact or source-backed tree, matching Rust's tagged GitTreeRef serde shape. */
export type GitTreeRef =
  | ({ readonly kind: "exact" } & GitGenerationRef)
  | {
      readonly kind: "lazy";
      readonly id: Uint8Array;
      readonly workspace_id: WorkspaceIdentity;
      readonly authored_generation: GenerationIdentity;
      readonly source: { readonly identity: Uint8Array; readonly epoch: bigint };
      readonly overlay: Uint8Array;
      readonly shadows: Uint8Array;
    };

/** SDK-verified-at-issuance fork origin; obtain it from core capture. */
export interface GitCaptureProof {
  readonly fork_parent: GitTreeRef;
  readonly initial_generation: GenerationIdentity;
  readonly operation_id: OperationIdentity;
}

/** Complete typed work request emitted by the compatibility state machine. */
export type GitFilesystemAction =
  | { readonly CaptureCommit: {
      readonly workspace_tree: GitTreeRef;
      readonly head_tree: GitTreeRef | undefined;
      readonly head_workspace_tree: GitTreeRef | undefined;
      readonly tracked_paths: readonly string[];
      readonly message: string;
      readonly author: string;
      readonly authored_at_seconds: number;
      readonly expected_head: GitCommitIdentity | undefined;
    } }
  | { readonly ForkBranch: {
      readonly branch: string;
      readonly source_tree: GitTreeRef;
      readonly head: GitCommitIdentity | undefined;
      readonly switch: boolean;
    } }
  | { readonly SwitchWorkspace: { readonly workspace_id: WorkspaceIdentity } }
  | { readonly Diff: {
      readonly from: GitTreeRef | undefined;
      readonly to: GitTreeRef;
      readonly tracked_paths: readonly string[];
    } }
  | { readonly RestoreGeneration: {
      readonly tree: GitTreeRef;
      readonly paths: readonly string[] | undefined;
    } }
  | { readonly RestorePaths: {
      readonly tree: GitTreeRef;
      readonly paths: readonly string[];
    } }
  | { readonly Join: {
      readonly target_tree: GitTreeRef;
      readonly source_workspace: WorkspaceIdentity;
      readonly rebase: boolean;
      readonly tracked_paths: readonly string[];
    } }
  | { readonly ApplyCommit: {
      readonly commit: GitCommitIdentity;
      readonly reverse: boolean;
      readonly base: GitTreeRef | undefined;
      readonly source: GitTreeRef | undefined;
      readonly paths: readonly string[];
      readonly tracked_paths: readonly string[];
    } }
  | { readonly Blame: {
      readonly path: string;
      readonly commits: readonly Readonly<Record<string, unknown>>[];
    } }
  | { readonly Grep: {
      readonly pattern: string;
      readonly path: string | undefined;
      readonly tree: GitTreeRef;
    } }
  | { readonly Clean: {
      readonly dry_run: boolean;
      readonly tree: GitTreeRef;
      readonly tracked_paths: readonly string[];
    } }
  | { readonly Archive: { readonly tree: GitTreeRef } }
  | { readonly ApplyPatch: { readonly patch: readonly number[] } }
  | { readonly CheckIgnore: {
      readonly paths: readonly string[];
      readonly tree: GitTreeRef;
    } };

export type GitFilesystemResult =
  | {
      readonly Captured: {
        readonly tree: GitTreeRef;
        readonly tracked_paths: readonly string[];
        readonly proof: GitCaptureProof | undefined;
      };
    }
  | { readonly Forked: { readonly workspace_id: WorkspaceIdentity } }
  | { readonly Applied: {
      readonly tree: GitTreeRef | undefined;
      readonly tracked_paths: readonly string[] | undefined;
    } }
  | {
      readonly Data: {
        readonly kind: string;
        readonly value: unknown;
      };
    };

export interface GitPendingTransition {
  readonly id: OperationIdentity;
  readonly action: GitFilesystemAction;
  /** Opaque private state mutation retained only for durable native recovery. */
  readonly mutation: unknown;
}

/** Caller-owned filesystem execution. The callback receives a durable retry identity. */
export interface GitFilesystemExecutor {
  execute(
    operationId: OperationIdentity,
    action: GitFilesystemAction,
  ): Promise<GitFilesystemResult>;
}

/** Backend-neutral Git-shaped façade. All modifications are automatically staged. */
export interface GitCompatRepository {
  execute(command: GitCompatCommand, workspaceGeneration: GenerationIdentity): Promise<GitCompatOutput>;
  /** Executes argv after an `acyclic git` caller strips that two-token prefix. */
  executeArgv(
    argv: readonly string[],
    workspaceGeneration: GenerationIdentity,
    defaultAuthor: string,
    nowSeconds: bigint,
  ): Promise<GitCompatOutput>;
  pendingTransition(): Promise<GitPendingTransition | undefined>;
  completeTransition(
    transition: OperationIdentity,
    resultingGeneration?: GenerationIdentity,
  ): Promise<GitCompatOutput>;
  completeTransitionResult(
    transition: OperationIdentity,
    result: GitFilesystemResult,
  ): Promise<GitCompatOutput>;
  run(
    command: GitCompatCommand,
    workspaceGeneration: GenerationIdentity,
    executor: GitFilesystemExecutor,
  ): Promise<GitCompatOutput>;
  /** Composed execution for argv following `acyclic git`; never bare system Git. */
  runArgv(
    argv: readonly string[],
    workspaceGeneration: GenerationIdentity,
    defaultAuthor: string,
    nowSeconds: bigint,
    executor: GitFilesystemExecutor,
  ): Promise<GitCompatOutput>;
  resume(executor: GitFilesystemExecutor): Promise<GitCompatOutput | undefined>;
  abortTransition(transition: OperationIdentity): Promise<void>;
  registerBranchWorkspace(
    branch: string,
    workspaceId: WorkspaceIdentity,
    head: GitCommitIdentity | undefined,
    switchToBranch: boolean,
  ): Promise<GitCompatOutput>;
  recordCommit(
    expectedHead: GitCommitIdentity | undefined,
    generation: GenerationIdentity,
    trackedPaths: readonly string[],
    message: string,
    author: string,
    authoredAtSeconds: bigint,
  ): Promise<GitCompatOutput>;
}

/** Encode every advertised typed command into the native serde contract. */
export function encodeGitCompatCommand(
  command: GitCompatCommand,
): Readonly<Record<string, unknown>> | string {
  switch (command.kind) {
    case "status": return "Status";
    case "diff": return { Diff: { cached: command.cached ?? false } };
    case "log": return { Log: { maximum: command.maximum } };
    case "show": return { Show: { object: command.object ?? null } };
    case "add": return { Add: { paths: command.paths } };
    case "commit": {
      const authoredAtSeconds = gitCompatSafeTimestamp(command.authoredAtSeconds);
      return { Commit: {
        message: command.message,
        author: command.author,
        authored_at_seconds: authoredAtSeconds,
      } };
    }
    case "branch": return { Branch: { create: command.create ?? null } };
    case "switch": return { Switch: { branch: command.branch, create: command.create ?? false } };
    case "restore": return { Restore: { source: command.source ?? null, paths: command.paths } };
    case "reset": return {
      Reset: {
        target: command.target,
        mode: command.mode[0]!.toUpperCase() + command.mode.slice(1),
      },
    };
    case "merge": return { Merge: { branch: command.branch } };
    case "rebase": return { Rebase: { branch: command.branch } };
    case "stash-push": return "StashPush";
    case "stash-pop": return "StashPop";
    case "cherry-pick": return { CherryPick: { object: command.object } };
    case "revert": return { Revert: { object: command.object } };
    case "tag": return {
      Tag: {
        name: command.name ?? null,
        target: command.target ?? null,
        delete: command.delete ?? false,
      },
    };
    case "blame": return { Blame: { path: command.path } };
    case "grep": return { Grep: { pattern: command.pattern, path: command.path ?? null } };
    case "clean": return { Clean: { dry_run: command.dryRun } };
    case "archive": return { Archive: { object: command.object ?? null } };
    case "apply": return { Apply: { patch: Array.from(command.patch) } };
    case "bisect": return { Bisect: { arguments: command.arguments } };
    case "rev-parse": return { RevParse: { argument: command.argument } };
    case "symbolic-ref": return { SymbolicRef: { short: command.short ?? false } };
    case "merge-base": return { MergeBase: { left: command.left, right: command.right } };
    case "ls-files": return "LsFiles";
    case "check-ignore": return { CheckIgnore: { paths: command.paths } };
  }
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
  return normalizeGitCompatOutput(parseGitJson(json));
}

/** Decode one durable pending transition returned by the native store. */
export function parseGitPendingTransitionJson(json: string): GitPendingTransition {
  const pending = object(parseGitJson(json), "pending transition");
  if (!("mutation" in pending)) throw new TypeError("Git pending transition lacks its mutation");
  return {
    id: uuidIdentity(pending.id, "pending transition ID"),
    action: normalizeAction(pending.action),
    mutation: pending.mutation,
  };
}

/** Encode an executor result without relying on `Uint8Array`'s object-shaped JSON form. */
export function stringifyGitFilesystemResult(result: GitFilesystemResult): string {
  const [kind, value] = tagged(result, ["Captured", "Forked", "Applied", "Data"], "filesystem result");
  switch (kind) {
    case "Captured": {
      const captured = object(value, "Captured result");
      return JSON.stringify({ Captured: {
        tree: treeRefJson(captured.tree, "captured tree"),
        tracked_paths: strings(captured.tracked_paths, "captured tracked paths"),
        proof: captured.proof == null ? null : captureProofJson(captured.proof),
      } });
    }
    case "Forked": {
      const forked = object(value, "Forked result");
      return JSON.stringify({ Forked: {
        workspace_id: bytes(forked.workspace_id, 16, "forked workspace"),
      } });
    }
    case "Applied": {
      const applied = object(value, "Applied result");
      return JSON.stringify({ Applied: {
        tree: applied.tree == null ? null : treeRefJson(applied.tree, "applied tree"),
        tracked_paths: applied.tracked_paths == null ? null : strings(applied.tracked_paths, "applied tracked paths"),
      } });
    }
    case "Data": {
      const data = object(value, "Data result");
      if (typeof data.kind !== "string") throw new TypeError("Git Data result kind must be a string");
      return JSON.stringify(
        { Data: { kind: data.kind, value: data.value } },
        (_key, nested) => nested instanceof Uint8Array ? Array.from(nested) : nested,
      );
    }
  }
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

function normalizeGitCompatOutput(value: unknown): GitCompatOutput {
  if (value === "NoOp") return value;
  const [kind, body] = tagged(
    value,
    ["Status", "Commits", "Branches", "Tags", "Committed", "Bisect", "Action", "Prepared", "Filesystem", "Text", "Paths"],
    "command output",
  );
  switch (kind) {
    case "Status": {
      const status = object(body, "Status output");
      if (typeof status.branch !== "string"
        || !["clean", "dirty", "unknown"].includes(status.dirty as string)
        || status.all_changes_staged !== true) {
        throw new TypeError("Git Status output is malformed");
      }
      return { Status: {
        branch: status.branch,
        head: optionalCommit(status.head, "status head"),
        workspace: treeRef(status.workspace, "status workspace"),
        dirty: status.dirty as GitCompatStatus["dirty"],
        allChangesStaged: true,
      } };
    }
    case "Commits": {
      if (!Array.isArray(body)) throw new TypeError("Git Commits output must be an array");
      return { Commits: body.map((commit) => normalizeCommit(commit)) };
    }
    case "Branches": {
      const branches = object(body, "Branches output");
      if (typeof branches.current !== "string" || !Array.isArray(branches.branches)) {
        throw new TypeError("Git Branches output is malformed");
      }
      return { Branches: {
        current: branches.current,
        branches: branches.branches.map((branch) => normalizeBranch(branch)),
      } };
    }
    case "Tags": {
      const tags = object(body, "Tags output");
      return { Tags: Object.fromEntries(
        Object.entries(tags).map(([name, commit]) => [name, commitId(commit, `tag ${name}`)]),
      ) };
    }
    case "Committed": return { Committed: normalizeCommit(body) };
    case "Bisect": return { Bisect: normalizeBisect(body) };
    case "Text": {
      if (typeof body !== "string") throw new TypeError("Git Text output must be a string");
      return { Text: body };
    }
    case "Paths": return { Paths: strings(body, "Git Paths output") };
    case "Action": return { Action: normalizeAction(body) };
    case "Prepared": {
      const prepared = object(body, "Prepared output");
      return { Prepared: {
        transition: uuidIdentity(prepared.transition, "prepared transition"),
        action: normalizeAction(prepared.action),
      } };
    }
    case "Filesystem": return { Filesystem: normalizeResult(body) };
  }
}

function normalizeAction(value: unknown): GitFilesystemAction {
  const [kind, body] = tagged(
    value,
    ["CaptureCommit", "ForkBranch", "SwitchWorkspace", "Diff", "RestoreGeneration", "RestorePaths", "Join", "ApplyCommit", "Blame", "Grep", "Clean", "Archive", "ApplyPatch", "CheckIgnore"],
    "filesystem action",
  );
  const data = object(body, `${kind} action`);
  switch (kind) {
    case "CaptureCommit": return { CaptureCommit: {
      workspace_tree: treeRef(data.workspace_tree, "capture workspace tree"),
      head_tree: optionalTreeRef(data.head_tree, "capture head tree"),
      head_workspace_tree: optionalTreeRef(data.head_workspace_tree, "capture head workspace tree"),
      tracked_paths: strings(data.tracked_paths, "capture tracked paths"),
      message: text(data.message, "capture message"),
      author: text(data.author, "capture author"),
      authored_at_seconds: integer(data.authored_at_seconds, "capture timestamp"),
      expected_head: optionalCommit(data.expected_head, "capture expected head"),
    } };
    case "ForkBranch": return { ForkBranch: {
      branch: text(data.branch, "fork branch"),
      source_tree: treeRef(data.source_tree, "fork source tree"),
      head: optionalCommit(data.head, "fork head"),
      switch: boolean(data.switch, "fork switch"),
    } };
    case "SwitchWorkspace": return { SwitchWorkspace: {
      workspace_id: identity(data.workspace_id, 16, "switch workspace"),
    } };
    case "Diff": return { Diff: {
      from: optionalTreeRef(data.from, "diff source"),
      to: treeRef(data.to, "diff destination"),
      tracked_paths: strings(data.tracked_paths, "diff tracked paths"),
    } };
    case "RestoreGeneration": return { RestoreGeneration: {
      tree: treeRef(data.tree, "restore tree"),
      paths: data.paths == null ? undefined : strings(data.paths, "restore paths"),
    } };
    case "RestorePaths": return { RestorePaths: {
      tree: treeRef(data.tree, "path restore tree"),
      paths: strings(data.paths, "path restore paths"),
    } };
    case "Join": return { Join: {
      target_tree: treeRef(data.target_tree, "join target tree"),
      source_workspace: identity(data.source_workspace, 16, "join source workspace"),
      rebase: boolean(data.rebase, "join rebase"),
      tracked_paths: strings(data.tracked_paths, "join tracked paths"),
    } };
    case "ApplyCommit": return { ApplyCommit: {
      commit: commitId(data.commit, "applied commit"),
      reverse: boolean(data.reverse, "apply reverse"),
      base: optionalTreeRef(data.base, "apply base"),
      source: optionalTreeRef(data.source, "apply source"),
      paths: strings(data.paths, "apply paths"),
      tracked_paths: strings(data.tracked_paths, "apply tracked paths"),
    } };
    case "Blame": {
      if (!Array.isArray(data.commits)) throw new TypeError("Git blame commits must be an array");
      return { Blame: {
        path: text(data.path, "blame path"),
        commits: data.commits.map((commit) => normalizeCommit(commit)),
      } };
    }
    case "Grep": return { Grep: {
      pattern: text(data.pattern, "grep pattern"),
      path: optionalText(data.path, "grep path"),
      tree: treeRef(data.tree, "grep tree"),
    } };
    case "Clean": return { Clean: {
      dry_run: boolean(data.dry_run, "clean dry-run"),
      tree: treeRef(data.tree, "clean tree"),
      tracked_paths: strings(data.tracked_paths, "clean tracked paths"),
    } };
    case "Archive": return { Archive: {
      tree: treeRef(data.tree, "archive tree"),
    } };
    case "ApplyPatch": return { ApplyPatch: {
      patch: bytes(data.patch, undefined, "patch"),
    } };
    case "CheckIgnore": return { CheckIgnore: {
      paths: strings(data.paths, "ignore paths"),
      tree: treeRef(data.tree, "ignore tree"),
    } };
  }
}

function normalizeResult(value: unknown): GitFilesystemResult {
  const [kind, body] = tagged(value, ["Captured", "Forked", "Applied", "Data"], "filesystem result");
  const data = object(body, `${kind} result`);
  switch (kind) {
    case "Captured": return { Captured: {
      tree: treeRef(data.tree, "captured tree"),
      tracked_paths: strings(data.tracked_paths, "captured tracked paths"),
      proof: data.proof == null ? undefined : captureProof(data.proof),
    } };
    case "Forked": return { Forked: {
      workspace_id: identity(data.workspace_id, 16, "forked workspace"),
    } };
    case "Applied": return { Applied: {
      tree: optionalTreeRef(data.tree, "applied tree"),
      tracked_paths: data.tracked_paths == null ? undefined : strings(data.tracked_paths, "applied tracked paths"),
    } };
    case "Data": return { Data: {
      kind: text(data.kind, "data result kind"),
      value: data.value,
    } };
  }
}

function normalizeCommit(value: unknown): Readonly<Record<string, unknown>> {
  const commit = object(value, "commit");
  return {
    ...commit,
    id: commitId(commit.id, "commit id"),
    tree: treeRef(commit.tree, "commit tree"),
    workspace_tree: treeRef(commit.workspace_tree, "commit workspace tree"),
    capture_proof: commit.capture_proof == null ? undefined : captureProof(commit.capture_proof),
    tracked_paths: strings(commit.tracked_paths, "commit tracked paths"),
    parents: commits(commit.parents, "commit parents"),
    author: text(commit.author, "commit author"),
    authored_at_seconds: integer(commit.authored_at_seconds, "commit timestamp"),
    message: text(commit.message, "commit message"),
  };
}

function normalizeBranch(value: unknown): Readonly<Record<string, unknown>> {
  const branch = object(value, "branch");
  return {
    ...branch,
    name: text(branch.name, "branch name"),
    workspace_id: identity(branch.workspace_id, 16, "branch workspace"),
    head: optionalCommit(branch.head, "branch head"),
    tracked_paths: strings(branch.tracked_paths, "branch tracked paths"),
  };
}

function normalizeBisect(value: unknown): GitBisectResult {
  const result = object(value, "bisect result");
  return {
    active: boolean(result.active, "bisect active"),
    good: optionalCommit(result.good, "bisect good"),
    bad: optionalCommit(result.bad, "bisect bad"),
    current: optionalCommit(result.current, "bisect current"),
    remaining: integer(result.remaining, "bisect remaining"),
    first_bad: optionalCommit(result.first_bad, "bisect first bad"),
  };
}

function treeRef(value: unknown, label: string): GitTreeRef {
  const reference = object(value, label);
  if (reference.kind === "exact") return {
    kind: "exact",
    workspace_id: identity(reference.workspace_id, 16, `${label} workspace`),
    generation: identity(reference.generation, 32, `${label} generation`),
  };
  if (reference.kind === "lazy") {
    const source = object(reference.source, `${label} source`);
    const epoch = u64(source.epoch, `${label} source epoch`);
    return {
      kind: "lazy",
      id: identity(reference.id, 32, `${label} snapshot`),
      workspace_id: identity(reference.workspace_id, 16, `${label} workspace`),
      authored_generation: identity(reference.authored_generation, 32, `${label} authored generation`),
      source: { identity: identity(source.identity, 16, `${label} source identity`), epoch },
      overlay: identity(reference.overlay, 32, `${label} overlay`),
      shadows: identity(reference.shadows, 32, `${label} shadows`),
    };
  }
  throw new TypeError(`Git ${label} has an unknown tree kind`);
}

function optionalTreeRef(value: unknown, label: string): GitTreeRef | undefined {
  return value == null ? undefined : treeRef(value, label);
}

function captureProof(value: unknown): GitCaptureProof {
  const proof = object(value, "capture proof");
  return {
    fork_parent: treeRef(proof.fork_parent, "capture fork parent"),
    initial_generation: identity(proof.initial_generation, 32, "capture initial generation"),
    operation_id: uuidIdentity(proof.operation_id, "capture operation"),
  };
}

function treeRefJson(value: unknown, label: string): Readonly<Record<string, unknown>> {
  const tree = treeRef(value, label);
  if (tree.kind === "exact") return {
    kind: "exact",
    workspace_id: Array.from(tree.workspace_id),
    generation: Array.from(tree.generation),
  };
  return {
    kind: "lazy",
    id: Array.from(tree.id),
    workspace_id: Array.from(tree.workspace_id),
    authored_generation: Array.from(tree.authored_generation),
    source: { identity: Array.from(tree.source.identity), epoch: u64Json(tree.source.epoch, `${label} source epoch`) },
    overlay: Array.from(tree.overlay),
    shadows: Array.from(tree.shadows),
  };
}

function captureProofJson(value: unknown): Readonly<Record<string, unknown>> {
  const proof = object(value, "capture proof");
  return {
    fork_parent: treeRefJson(proof.fork_parent, "capture fork parent"),
    initial_generation: bytes(proof.initial_generation, 32, "capture initial generation"),
    operation_id: uuidString(proof.operation_id, "capture operation"),
  };
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

function identity(value: unknown, length: number, label: string): Uint8Array {
  return Uint8Array.from(bytes(value, length, label));
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

function u64Json(value: unknown, label: string): unknown {
  const epoch = u64(value, label);
  if (epoch <= BigInt(Number.MAX_SAFE_INTEGER)) return Number(epoch);
  const rawJson = (JSON as typeof JSON & { rawJSON?: (text: string) => unknown }).rawJSON;
  if (rawJson === undefined) throw new RangeError("Git source epoch needs lossless JSON serialization");
  return rawJson(epoch.toString());
}

// Rust's transparent OperationId wraps uuid::Uuid: JSON uses its canonical
// string form, while the native N-API boundary accepts the same 16 raw bytes.
function uuidIdentity(value: unknown, label: string): OperationIdentity {
  if (typeof value !== "string" || !/^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/i.test(value)) {
    throw new TypeError(`Git ${label} must be a UUID string`);
  }
  const hex = value.replaceAll("-", "");
  return Uint8Array.from({ length: 16 }, (_, index) => Number.parseInt(hex.slice(index * 2, index * 2 + 2), 16));
}

function uuidString(value: unknown, label: string): string {
  const hex = Array.from(bytes(value, 16, label), (byte) => byte.toString(16).padStart(2, "0")).join("");
  return `${hex.slice(0, 8)}-${hex.slice(8, 12)}-${hex.slice(12, 16)}-${hex.slice(16, 20)}-${hex.slice(20)}`;
}

function text(value: unknown, label: string): string {
  if (typeof value !== "string") throw new TypeError(`Git ${label} must be a string`);
  return value;
}

function optionalText(value: unknown, label: string): string | undefined {
  return value == null ? undefined : text(value, label);
}

function boolean(value: unknown, label: string): boolean {
  if (typeof value !== "boolean") throw new TypeError(`Git ${label} must be a boolean`);
  return value;
}

function integer(value: unknown, label: string): number {
  if (typeof value !== "number" || !Number.isSafeInteger(value)) {
    throw new TypeError(`Git ${label} must be a safe integer`);
  }
  return value;
}

function strings(value: unknown, label: string): readonly string[] {
  if (!Array.isArray(value) || value.some((item) => typeof item !== "string")) {
    throw new TypeError(`Git ${label} must be a string array`);
  }
  return value as string[];
}

function commitId(value: unknown, label: string): GitCommitIdentity {
  if (typeof value !== "string" || !/^[0-9a-f]{64}$/.test(value)) {
    throw new TypeError(`Git ${label} must be a lowercase 64-character hexadecimal commit ID`);
  }
  return value;
}

function optionalCommit(value: unknown, label: string): GitCommitIdentity | undefined {
  return value == null ? undefined : commitId(value, label);
}

function commits(value: unknown, label: string): readonly GitCommitIdentity[] {
  if (!Array.isArray(value)) throw new TypeError(`Git ${label} must be an array`);
  return value.map((commit) => commitId(commit, label));
}
