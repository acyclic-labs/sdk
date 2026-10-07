import type {
  JoinResult, MergeConflict, MergePreparationResult, WorkCounters,
  WorkspaceCommit, WorkspaceDeleteStatus, WorkspaceRebaseResult,
  WasmRawMergeConflict,
} from "./contracts.js";

type StatusDomain = "join" | "workspaceRebase" | "workspaceDelete" | "transactionCommit" |
  "checkoutCommit" | "liveMutation" | "transactionRebase" | "rebaseDecision";
type RuntimeStatusMetadata = Readonly<Record<StatusDomain, ReadonlySet<string>>>;

let runtimeStatusMetadata: RuntimeStatusMetadata | undefined;

function statusValues(value: unknown, label: string): ReadonlySet<string> {
  if (typeof value !== "object" || value === null) throw new TypeError(`status metadata ${label} is malformed`);
  const items = Reflect.get(value, label);
  if (!Array.isArray(items) || items.length === 0 || items.some(item => typeof item !== "string")) {
    throw new TypeError(`status metadata ${label} is malformed`);
  }
  return new Set(items);
}

/** Installs the Rust-generated discriminant metadata used by all adapters. */
export function installStatusMetadata(value: unknown): void {
  runtimeStatusMetadata = {
    join: statusValues(value, "joinOutcome"),
    workspaceRebase: statusValues(value, "workspaceRebase"),
    workspaceDelete: statusValues(value, "workspaceDelete"),
    transactionCommit: statusValues(value, "transactionCommit"),
    checkoutCommit: statusValues(value, "checkoutCommit"),
    liveMutation: statusValues(value, "liveMutation"),
    transactionRebase: statusValues(value, "transactionRebase"),
    rebaseDecision: statusValues(value, "rebaseDecision"),
  };
}

export function isKnownStatus<T extends string>(value: string, domain: StatusDomain, label: string): value is T {
  if (runtimeStatusMetadata?.[domain].has(value) !== true) {
    throw new TypeError(`${label} returned an invalid status`);
  }
  return true;
}

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

function parseResult<Status extends string, Conflict>(
  value: RawResult<Conflict>, label: string,
  decodeConflict: (value: Conflict) => MergeConflict,
): { readonly status: Status; readonly generationId: Uint8Array | undefined; readonly conflicts: readonly MergeConflict[]; readonly truncated: boolean } {
  const domain = label === "join" ? "join" : "workspaceRebase";
  if (!isKnownStatus<Status>(value.status, domain, label)) throw new TypeError(`${label} returned an invalid status`);
  return {
    status: value.status,
    generationId: copyGenerationId(value.generationId, `${label} result`),
    conflicts: value.conflicts.map(decodeConflict),
    truncated: value.truncated,
  };
}

export function parseJoinResult<Conflict>(value: RawResult<Conflict>, decodeConflict: (value: Conflict) => MergeConflict): JoinResult {
  return parseResult(value, "join", decodeConflict);
}

export function parseWorkspaceRebaseResult<Conflict>(value: RawResult<Conflict>, decodeConflict: (value: Conflict) => MergeConflict): WorkspaceRebaseResult {
  return parseResult(value, "workspace rebase", decodeConflict);
}

export function parseWorkspaceDelete(status: string): WorkspaceDeleteStatus {
  if (!isKnownStatus<WorkspaceDeleteStatus>(status, "workspaceDelete", "workspace delete")) {
    throw new TypeError("workspace delete returned an invalid status");
  }
  return status;
}

export function parseWorkspaceCommit(value: unknown): WorkspaceCommit {
  if (typeof value !== "object" || value === null) throw new TypeError("workspace commit must be an object");
  const candidate = value as { readonly status?: unknown; readonly generationId?: unknown };
  if (typeof candidate.status !== "string") {
    throw new TypeError("workspace commit has an invalid status");
  }
  if (!isKnownStatus<WorkspaceCommit["status"]>(candidate.status, "transactionCommit", "workspace commit")) {
    throw new TypeError("workspace commit returned an invalid status");
  }
  return { status: candidate.status, generationId: copyGenerationId(candidate.generationId, "workspace commit") };
}
