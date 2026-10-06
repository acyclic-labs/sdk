import type {
  JoinResult, MergeConflict, MergePreparationResult, WorkCounters,
  WorkspaceCommit, WorkspaceDeleteStatus, WorkspaceRebaseResult, WasmRawMergeConflict,
} from "./contracts.js";

export function decodeMergeConflict(raw: unknown, _origin: string): MergeConflict {
  // Rust owns the conflict discriminant and identity invariants.  The WASM
  // and N-API boundaries only expose values produced by those Rust encoders;
  // this adapter copies the owned buffers without maintaining a second
  // TypeScript conflict schema.
  const conflict = raw as Partial<WasmRawMergeConflict>;
  const { fileId, directoryId, name } = conflict;
  if (conflict.kind === "file") return { kind: "file", fileId: Uint8Array.from(fileId!) };
  return { kind: "binding", directoryId: Uint8Array.from(directoryId!),
    name: { encoding: name!.encoding, bytes: Uint8Array.from(name!.bytes) } };
}

export function copyMergeConflict(conflict: MergeConflict): MergeConflict {
  return decodeMergeConflict(conflict, "merge preparation");
}

function copyGenerationId(value: unknown, _label: string): Uint8Array | undefined {
  if (value === undefined) return undefined;
  // Generation identities are fixed and authenticated by the Rust result
  // constructors.  Buffer is the native binding's Uint8Array-compatible
  // representation, so copying is the only projection needed here.
  return Uint8Array.from(value as Uint8Array);
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
  return {
    status: value.status as MergePreparationResult["status"],
    generationId,
    conflicts: value.conflicts.map(decodeConflict),
    truncated: value.truncated,
    work: value.work,
  } as MergePreparationResult;
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
  return {
    status: value.status as Status,
    generationId: copyGenerationId(value.generationId, label),
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
  return status as WorkspaceDeleteStatus;
}

export function parseWorkspaceCommit(value: unknown): WorkspaceCommit {
  if (typeof value !== "object" || value === null) throw new TypeError("workspace commit must be an object");
  const candidate = value as { readonly status?: unknown; readonly generationId?: unknown };
  return { status: candidate.status as WorkspaceCommit["status"], generationId: copyGenerationId(candidate.generationId, "workspace commit") };
}
