import type { JoinResult, JoinStatus, MergeConflict, MergePreparationResult, WorkCounters, WorkspaceCommit, WorkspaceDeleteStatus, WorkspaceRebaseResult, WorkspaceRebaseStatus } from "./contracts.js";
import type { WasmRawMergeConflict } from "./binding-types.js";
import { copyFixedBytes, copyName } from "./binding-values.js";

export function decodeMergeConflict(raw: unknown): MergeConflict {
  if (typeof raw !== "object" || raw === null) throw new TypeError("binding returned a malformed conflict");
  const conflict = raw as Partial<WasmRawMergeConflict>;
  const { fileId, directoryId, name } = conflict;
  if (conflict.kind === "file" && fileId instanceof Uint8Array && fileId.byteLength === 16 &&
      directoryId === undefined && name === undefined) {
    return { kind: "file", fileId: Uint8Array.from(fileId) };
  }
  if (conflict.kind === "binding" && fileId === undefined &&
      directoryId instanceof Uint8Array && directoryId.byteLength === 16 &&
      typeof name === "object" && name !== null &&
      (name.encoding === "utf8" || name.encoding === "posix-bytes" || name.encoding === "windows-utf16le") &&
      name.bytes instanceof Uint8Array) {
    return { kind: "binding", directoryId: Uint8Array.from(directoryId),
      name: copyName(name) };
  }
  throw new TypeError("binding returned a malformed conflict");
}

export function parseMergePreparation(
  value: {
    readonly status: string;
    readonly generationId: Uint8Array | undefined;
    readonly conflicts: readonly unknown[];
    readonly truncated: boolean;
    readonly work: WorkCounters;
  },
): MergePreparationResult {
  const generationId = copyFixedBytes(value.generationId, 32, "generation identity");
  if (value.status === "prepared" && generationId !== undefined &&
      value.conflicts.length === 0 && !value.truncated) {
    return { status: "prepared", generationId,
      conflicts: [], truncated: false, work: value.work };
  }
  if (value.status === "conflicted" && generationId === undefined) {
    return { status: "conflicted", generationId: undefined,
      conflicts: value.conflicts.map(decodeMergeConflict), truncated: value.truncated, work: value.work };
  }
  throw new TypeError("binding returned a malformed merge preparation");
}

interface RawResult {
  readonly status: string;
  readonly generationId?: Uint8Array | undefined;
  readonly conflicts: readonly unknown[];
  readonly truncated: boolean;
}

const joinStatuses: ReadonlySet<JoinStatus> = new Set<JoinStatus>([
  "applied", "already-applied", "no-changes", "stale-target", "conflicted", "fenced", "idempotency-conflict",
]);
const rebaseStatuses: ReadonlySet<WorkspaceRebaseStatus> = new Set<WorkspaceRebaseStatus>([
  "rebased", "already-rebased", "current", "stale", "conflicted", "fenced", "idempotency-conflict",
]);
const deleteStatuses: ReadonlySet<WorkspaceDeleteStatus> = new Set<WorkspaceDeleteStatus>([
  "deleted", "already-deleted", "conflict", "idempotency-conflict",
]);
export const commitStatuses: ReadonlySet<WorkspaceCommit["status"]> = new Set<WorkspaceCommit["status"]>([
  "committed", "already-committed", "conflict", "fenced", "idempotency-conflict",
]);

function isStatus<Status extends string>(statuses: ReadonlySet<Status>, value: string): value is Status {
  return statuses.has(value as Status);
}

function parseResult<Status extends string>(
  value: RawResult, statuses: ReadonlySet<Status>,
): { readonly status: Status; readonly generationId: Uint8Array | undefined; readonly conflicts: readonly MergeConflict[]; readonly truncated: boolean } {
  if (!isStatus(statuses, value.status)) throw new TypeError("binding result has an invalid status");
  return {
    status: value.status,
    generationId: copyFixedBytes(value.generationId, 32, "generation identity"),
    conflicts: value.conflicts.map(decodeMergeConflict),
    truncated: value.truncated,
  };
}

export function parseJoinResult(value: RawResult): JoinResult {
  return parseResult(value, joinStatuses);
}

export function parseWorkspaceRebaseResult(value: RawResult): WorkspaceRebaseResult {
  return parseResult(value, rebaseStatuses);
}

export function parseWorkspaceDelete(status: string): WorkspaceDeleteStatus {
  if (!isStatus(deleteStatuses, status)) throw new TypeError("workspace deletion has an invalid status");
  return status;
}

export function parseWorkspaceCommit(value: unknown): WorkspaceCommit {
  if (typeof value !== "object" || value === null) throw new TypeError("workspace commit must be an object");
  const candidate = value as { readonly status?: unknown; readonly generationId?: unknown };
  if (typeof candidate.status !== "string" || !isStatus(commitStatuses, candidate.status)) {
    throw new TypeError("workspace commit has an invalid status");
  }
  return { status: candidate.status, generationId: copyFixedBytes(candidate.generationId, 32, "generation identity") };
}
