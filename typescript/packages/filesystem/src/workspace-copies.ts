import type {
  CommitResult, FileExtentPlan, LiveMutationResult, LiveTransactionResult, RebaseResult,
  TransactionConflict, TransactionRebaseResult, TransactionResult, WorkCounters, WorkspaceDirectoryPage,
  WorkspaceExtentPlan, WorkspaceName, WorkspaceStat,
} from "./contracts.js";

function copyBytes(value: Uint8Array | undefined): Uint8Array | undefined {
  return value === undefined ? undefined : Uint8Array.from(value);
}

export function bigintRecord(value: Readonly<Record<string, string | number>>): Readonly<Record<string, bigint>> {
  return Object.fromEntries(Object.entries(value).map(([key, item]) => [key, BigInt(item)]));
}

function copyName(value: WorkspaceName): WorkspaceName {
  return { encoding: value.encoding, bytes: Uint8Array.from(value.bytes) };
}

export function copyWorkspaceStat(value: WorkspaceStat): WorkspaceStat {
  const metadata = value.metadata;
  return {
    ...value,
    fileId: Uint8Array.from(value.fileId),
    linkCount: BigInt(value.linkCount),
    logicalBytes: value.logicalBytes === undefined ? undefined : BigInt(value.logicalBytes),
    metadata: {
      ...metadata,
      posixFlags: metadata.posixFlags === undefined ? undefined : BigInt(metadata.posixFlags),
      createdNs: metadata.createdNs === undefined ? undefined : BigInt(metadata.createdNs),
      modifiedNs: metadata.modifiedNs === undefined ? undefined : BigInt(metadata.modifiedNs),
      accessedNs: metadata.accessedNs === undefined ? undefined : BigInt(metadata.accessedNs),
      changedNs: metadata.changedNs === undefined ? undefined : BigInt(metadata.changedNs),
    },
  };
}

export function copyWorkspaceDirectoryPage(value: WorkspaceDirectoryPage): WorkspaceDirectoryPage {
  return {
    hasMore: value.hasMore,
    entries: value.entries.map((entry) => ({
      name: copyName(entry.name), fileId: Uint8Array.from(entry.fileId), kind: entry.kind,
    })),
  };
}

export function copyWorkspaceExtentPlan(value: WorkspaceExtentPlan): WorkspaceExtentPlan {
  return {
    spans: value.spans.map((span) => ({
      ...span,
      offset: BigInt(span.offset),
      length: BigInt(span.length),
      sourceEnd: BigInt(span.sourceEnd),
    })),
  };
}

function extentInteger(value: unknown, origin: string, label: string): bigint {
  if ((typeof value === "bigint" && value >= 0n) ||
      (typeof value === "string" && /^(0|[1-9][0-9]*)$/.test(value))) {
    return BigInt(value);
  }
  throw new TypeError(`${origin} returned a malformed ${label}`);
}

export function copyFileExtentPlan(value: {
  readonly kind: "inline" | "sparse";
  readonly spans?: readonly {
    readonly kind: "hole" | "allocated-zero" | "content";
    readonly offset: bigint | string;
    readonly length: bigint | string;
    readonly sourceEnd: bigint | string;
    readonly objectId?: Uint8Array | undefined;
    readonly objectOffset?: bigint | string | undefined;
  }[] | undefined;
  readonly retainedAllocationBytes?: bigint | string | undefined;
}, work: WorkCounters, origin: string, allowMissingSpans = false): FileExtentPlan {
  if (value.kind === "inline") return { kind: "inline", work };
  if (value.kind !== "sparse" ||
      (!Array.isArray(value.spans) && !(allowMissingSpans && value.spans === undefined))) {
    throw new TypeError(`${origin} returned a malformed file extent plan`);
  }
  const spans = value.spans ?? [];
  return {
    kind: "sparse",
    spans: spans.map(span => {
      const common = {
        offset: extentInteger(span.offset, origin, "file extent span"),
        length: extentInteger(span.length, origin, "file extent span"),
        sourceEnd: extentInteger(span.sourceEnd, origin, "file extent span"),
      };
      if (span.kind === "content") {
        if (!(span.objectId instanceof Uint8Array) || span.objectId.byteLength !== 33 ||
            span.objectOffset === undefined) {
          throw new TypeError(`${origin} returned a malformed content extent`);
        }
        return { kind: "content" as const, ...common,
          objectId: Uint8Array.from(span.objectId),
          objectOffset: extentInteger(span.objectOffset, origin, "content extent") };
      }
      if (span.kind !== "hole" && span.kind !== "allocated-zero") {
        throw new TypeError(`${origin} returned a malformed file extent span`);
      }
      return { kind: span.kind, ...common };
    }),
    retainedAllocationBytes: extentInteger(value.retainedAllocationBytes ?? 0n, origin, "file extent plan"),
    work,
  };
}

interface RawCommitFields<Status extends string> {
  readonly status: Status;
  readonly generationId: Uint8Array | undefined;
  readonly epoch: bigint | string | undefined;
  readonly sequence: bigint | string | undefined;
  readonly committedFingerprint: Uint8Array | undefined;
}

const commitStatuses: ReadonlySet<CommitResult["status"]> = new Set([
  "committed", "already-committed", "conflict", "fenced", "idempotency-conflict",
]);
const liveStatuses: ReadonlySet<LiveMutationResult["status"]> = new Set([
  "committed", "already-committed", "conflicted", "retry-limit", "fenced", "idempotency-conflict",
]);

function copyFixedBytes(value: unknown, length: number, label: string): Uint8Array | undefined {
  if (value === undefined) return undefined;
  if (!(value instanceof Uint8Array) || value.byteLength !== length) {
    throw new TypeError(`${label} must be ${length} bytes`);
  }
  return Uint8Array.from(value);
}

export function copyRebaseResult(value: Pick<RebaseResult, "status" | "generationId" | "conflictCount" | "truncated">,
  work: WorkCounters): RebaseResult {
  if (value.status !== "safe" && value.status !== "conflicted") {
    throw new TypeError("checkout rebase has an invalid status");
  }
  if (!Number.isSafeInteger(value.conflictCount) || value.conflictCount < 0 || typeof value.truncated !== "boolean") {
    throw new TypeError("checkout rebase has malformed conflict counts");
  }
  return { status: value.status, generationId: copyFixedBytes(value.generationId, 32, "generation identity"),
    conflictCount: value.conflictCount, truncated: value.truncated, work };
}

function copyCreatedFileIds(ids: readonly (Uint8Array | undefined)[]): readonly (Uint8Array | undefined)[] {
  return ids.map(id => copyFixedBytes(id, 16, "created file identity"));
}

export function copyTransactionResult(value: { readonly createdFileIds: readonly (Uint8Array | undefined)[] },
  work: WorkCounters): TransactionResult {
  return { createdFileIds: copyCreatedFileIds(value.createdFileIds), work };
}

function copyCommitFields<Status extends string>(value: RawCommitFields<Status>, work: WorkCounters,
  statuses: ReadonlySet<Status>) {
  if (!statuses.has(value.status)) throw new TypeError("checkout result has an invalid status");
  return {
    status: value.status,
    generationId: copyFixedBytes(value.generationId, 32, "generation identity"),
    epoch: value.epoch === undefined ? undefined : BigInt(value.epoch),
    sequence: value.sequence === undefined ? undefined : BigInt(value.sequence),
    committedFingerprint: copyFixedBytes(value.committedFingerprint, 32, "committed fingerprint"),
    work,
  };
}

export function copyCheckoutCommit(value: RawCommitFields<CommitResult["status"]>, work: WorkCounters): CommitResult {
  return copyCommitFields(value, work, commitStatuses);
}

type RawLiveMutation = RawCommitFields<LiveMutationResult["status"]> & Pick<LiveMutationResult, "conflictCount" | "truncated">;

export function copyLiveMutation(value: RawLiveMutation, work: WorkCounters): LiveMutationResult {
  return { ...copyCommitFields(value, work, liveStatuses), conflictCount: value.conflictCount, truncated: value.truncated };
}

export function copyLiveTransaction(value: RawLiveMutation & { readonly createdFileIds: readonly (Uint8Array | undefined)[] },
  work: WorkCounters): LiveTransactionResult {
  return { ...copyLiveMutation(value, work), createdFileIds: copyCreatedFileIds(value.createdFileIds) };
}

function copyTransactionConflict(value: TransactionConflict): TransactionConflict {
  return {
    ...value,
    fileId: copyBytes(value.fileId),
    directoryId: copyBytes(value.directoryId),
    offset: value.offset === undefined ? undefined : BigInt(value.offset),
    length: value.length === undefined ? undefined : BigInt(value.length),
    name: value.name === undefined ? undefined : copyName(value.name),
    expected: copyBytes(value.expected),
    actual: copyBytes(value.actual),
  };
}

export function copyTransactionRebase(value: TransactionRebaseResult): TransactionRebaseResult {
  if (value.status !== "rebased" && value.status !== "conflicted") {
    throw new TypeError("transaction rebase has an invalid status");
  }
  return {
    status: value.status,
    generationId: copyBytes(value.generationId),
    conflicts: value.conflicts.map(copyTransactionConflict),
    truncated: value.truncated,
  };
}
