import type {
  JoinOptions, JoinResult, JoinStatus, MergeConflict, MergePreparationResult, WorkCounters,
  WorkspaceCommit, WorkspaceDeleteStatus,
  WorkspaceRebaseOptions, WorkspaceRebaseResult, WorkspaceRebaseStatus, WasmRawMergeConflict,
} from "./contracts.js";

export function decodeMergeConflict(raw: unknown, origin: string): MergeConflict {
  if (typeof raw !== "object" || raw === null) throw new Error(`${origin} returned a malformed conflict`);
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
      name: { encoding: name.encoding, bytes: Uint8Array.from(name.bytes) } };
  }
  throw new Error(`${origin} returned a malformed conflict`);
}

export function copyMergeConflict(conflict: MergeConflict): MergeConflict {
  return decodeMergeConflict(conflict, "merge preparation");
}

function copyGenerationId(value: unknown, label: string): Uint8Array | undefined {
  if (value === undefined) return undefined;
  if (!(value instanceof Uint8Array) || value.byteLength !== 32) {
    throw new TypeError(`${label} has an invalid generation identity`);
  }
  return Uint8Array.from(value);
}

export function parseMergePreparation<Conflict>(
  value: {
    readonly status: string;
    readonly generationId: Uint8Array | undefined;
    readonly conflicts: readonly Conflict[];
    readonly truncated: boolean;
    readonly work: WorkCounters;
  },
  decodeConflict: (value: Conflict) => MergeConflict,
): MergePreparationResult {
  const generationId = copyGenerationId(value.generationId, "merge preparation");
  if (value.status === "prepared" && generationId !== undefined &&
      value.conflicts.length === 0 && !value.truncated) {
    return { status: "prepared", generationId,
      conflicts: [], truncated: false, work: value.work };
  }
  if (value.status === "conflicted" && generationId === undefined) {
    return { status: "conflicted", generationId: undefined,
      conflicts: value.conflicts.map(decodeConflict), truncated: value.truncated, work: value.work };
  }
  throw new TypeError("binding returned a malformed merge preparation");
}

interface RawResult<Conflict> {
  readonly status: string;
  readonly generationId: Uint8Array | undefined;
  readonly conflicts: readonly Conflict[];
  readonly truncated: boolean;
}

function positive(value: number, label: string): void {
  if (!Number.isSafeInteger(value) || value <= 0) {
    throw new RangeError(`${label} must be a positive safe integer`);
  }
}

function validateOptions(options: JoinOptions | WorkspaceRebaseOptions, operation: string): void {
  positive(options.maximumGenerations, `maximum ${operation} generations`);
  positive(options.maximumChanges, `maximum ${operation} changes`);
  positive(options.maximumConflicts, `maximum ${operation} conflicts`);
}

export function validateJoinOptions(options: JoinOptions): void { validateOptions(options, "join"); }
export function validateWorkspaceRebaseOptions(options: WorkspaceRebaseOptions): void { validateOptions(options, "rebase"); }

const joinStatuses: ReadonlySet<JoinStatus> = new Set<JoinStatus>([
  "applied", "already-applied", "no-changes", "stale-target", "conflicted", "fenced", "idempotency-conflict",
]);
const rebaseStatuses: ReadonlySet<WorkspaceRebaseStatus> = new Set<WorkspaceRebaseStatus>([
  "rebased", "already-rebased", "current", "stale", "conflicted", "fenced", "idempotency-conflict",
]);
const deleteStatuses: ReadonlySet<WorkspaceDeleteStatus> = new Set<WorkspaceDeleteStatus>([
  "deleted", "already-deleted", "conflict", "idempotency-conflict",
]);
const commitStatuses: ReadonlySet<WorkspaceCommit["status"]> = new Set<WorkspaceCommit["status"]>([
  "committed", "already-committed", "conflict", "fenced", "idempotency-conflict",
]);

function isStatus<Status extends string>(statuses: ReadonlySet<Status>, value: string): value is Status {
  return statuses.has(value as Status);
}

function parseResult<Status extends string, Conflict>(
  value: RawResult<Conflict>, statuses: ReadonlySet<Status>, label: string,
  decodeConflict: (value: Conflict) => MergeConflict,
): { readonly status: Status; readonly generationId: Uint8Array | undefined; readonly conflicts: readonly MergeConflict[]; readonly truncated: boolean } {
  if (!isStatus(statuses, value.status)) throw new TypeError(`${label} result has an invalid status`);
  return {
    status: value.status,
    generationId: copyGenerationId(value.generationId, `${label} result`),
    conflicts: value.conflicts.map(decodeConflict),
    truncated: value.truncated,
  };
}

export function parseJoinResult<Conflict>(value: RawResult<Conflict>, decodeConflict: (value: Conflict) => MergeConflict): JoinResult {
  return parseResult(value, joinStatuses, "join", decodeConflict);
}

export function parseWorkspaceRebaseResult<Conflict>(value: RawResult<Conflict>, decodeConflict: (value: Conflict) => MergeConflict): WorkspaceRebaseResult {
  return parseResult(value, rebaseStatuses, "workspace rebase", decodeConflict);
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
  return { status: candidate.status, generationId: copyGenerationId(candidate.generationId, "workspace commit") };
}
