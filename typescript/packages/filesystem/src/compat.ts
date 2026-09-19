/** Public control-plane contracts shared by native, hosted, and plugin adapters. */

import type {
  FsWorkspace,
  WorkspaceRebaseOptions,
  WorkspaceRebaseResult,
} from "./contracts.js";

export type WorkspaceIdentity = Uint8Array;
export type GenerationIdentity = Uint8Array;
export type OperationIdentity = Uint8Array;
/** Canonical lowercase BLAKE3 compatibility commit ID. */
export type GitCommitIdentity = string;

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
  | { readonly kind: "bisect"; readonly arguments: readonly string[] };

export interface GitCompatStatus {
  readonly branch: string;
  readonly head: GitCommitIdentity | undefined;
  readonly workspace: GenerationIdentity;
  readonly dirty: boolean;
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
  | { readonly Filesystem: GitFilesystemResult };

export interface GitGenerationRef {
  readonly workspace_id: WorkspaceIdentity;
  readonly generation: GenerationIdentity;
}

/** Complete typed work request emitted by the compatibility state machine. */
export type GitFilesystemAction =
  | { readonly CaptureCommit: {
      readonly workspace_generation: GenerationIdentity;
      readonly head_generation: GenerationIdentity | undefined;
      readonly tracked_paths: readonly string[];
      readonly message: string;
      readonly author: string;
      readonly authored_at_seconds: number;
      readonly expected_head: GitCommitIdentity | undefined;
    } }
  | { readonly ForkBranch: {
      readonly branch: string;
      readonly source_workspace: WorkspaceIdentity;
      readonly source_generation: GenerationIdentity;
      readonly head: GitCommitIdentity | undefined;
      readonly switch: boolean;
    } }
  | { readonly SwitchWorkspace: { readonly workspace_id: WorkspaceIdentity } }
  | { readonly Diff: {
      readonly from: GitGenerationRef | undefined;
      readonly to: GitGenerationRef;
    } }
  | { readonly RestoreGeneration: {
      readonly workspace_id: WorkspaceIdentity;
      readonly generation: GenerationIdentity;
      readonly paths: readonly string[] | undefined;
    } }
  | { readonly RestorePaths: {
      readonly workspace_id: WorkspaceIdentity;
      readonly generation: GenerationIdentity;
      readonly paths: readonly string[];
    } }
  | { readonly Join: { readonly source_workspace: WorkspaceIdentity; readonly rebase: boolean } }
  | { readonly ApplyCommit: {
      readonly commit: GitCommitIdentity;
      readonly reverse: boolean;
      readonly base: GitGenerationRef | undefined;
      readonly source: GitGenerationRef | undefined;
      readonly paths: readonly string[];
    } }
  | { readonly Blame: {
      readonly path: string;
      readonly commits: readonly Readonly<Record<string, unknown>>[];
    } }
  | { readonly Grep: {
      readonly pattern: string;
      readonly path: string | undefined;
      readonly generation: GitGenerationRef;
    } }
  | { readonly Clean: {
      readonly dry_run: boolean;
      readonly generation: GitGenerationRef;
      readonly tracked_paths: readonly string[];
    } }
  | { readonly Archive: {
      readonly workspace_id: WorkspaceIdentity;
      readonly generation: GenerationIdentity;
    } }
  | { readonly ApplyPatch: { readonly patch: readonly number[] } };

export type GitFilesystemResult =
  | {
      readonly Captured: {
        readonly generation: GenerationIdentity;
        readonly workspace_id: WorkspaceIdentity;
        readonly tracked_paths: readonly string[];
      };
    }
  | { readonly Forked: { readonly workspace_id: WorkspaceIdentity } }
  | { readonly Applied: { readonly generation: GenerationIdentity | undefined } }
  | {
      readonly Data: {
        readonly kind: string;
        readonly value: unknown;
      };
    };

export interface GitPendingTransition {
  readonly id: OperationIdentity;
  readonly action: GitFilesystemAction;
  readonly mutation: Readonly<Record<string, unknown>>;
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
