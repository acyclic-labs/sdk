import type {
  FsGeneration, FsJoinPlan, FsWorkspace, JoinResult, MergeConflictSelection,
  NativeRawJoinPlan, ResolvableFsJoinPlan, WasmRawGeneration, WasmRawJoinPlan,
  WasmRawJoinResult, WasmRawWorkspace, WorkspaceRebaseResult,
} from "./contracts.js";
import { copyWorkspaceExtentPlan, copyWorkspaceStat } from "./workspace-copies.js";
import { parseWorkspaceCommit, parseWorkspaceDelete, validateWorkspaceRebaseOptions } from "./workspace-results.js";

type RawOperations = Pick<WasmRawWorkspace,
  "head" | "sync" | "checkpoint" | "pin" | "delete" |
  "read" | "readRange" | "stat" | "readSymbolicLink" |
  "planExtents" | "write" | "remove" | "liveRebase"
>;
type WorkspaceOperations = Pick<FsWorkspace,
  "head" | "sync" | "checkpoint" | "pin" | "delete" |
  "read" | "readRange" | "stat" | "readSymbolicLink" |
  "planExtents" | "write" | "remove" | "liveRebase"
>;

export function workspaceOperations(
  raw: RawOperations,
  adaptGeneration: (raw: WasmRawGeneration) => FsGeneration,
  parseRebase: (raw: WasmRawJoinResult) => WorkspaceRebaseResult,
): WorkspaceOperations {
  return {
    async head() { return Uint8Array.from(await raw.head()); },
    async sync() { return adaptGeneration(await raw.sync()); },
    async checkpoint(label) {
      requireName(label);
      return adaptGeneration(await raw.checkpoint(label));
    },
    async pin(identity) {
      requireName(identity);
      return adaptGeneration(await raw.pin(identity));
    },
    async delete(idempotencyKey) {
      if (idempotencyKey !== undefined) requireIdentity(idempotencyKey);
      return parseWorkspaceDelete(await raw.delete(idempotencyKey));
    },
    async read(path, maximumBytes) {
      if (maximumBytes <= 0n) throw new RangeError("maximum read bytes must be positive");
      return Uint8Array.from(await raw.read(path, maximumBytes));
    },
    async readRange(path, offset, length) { return Uint8Array.from(await raw.readRange(path, offset, length)); },
    async stat(path) { return copyWorkspaceStat(await raw.stat(path)); },
    async readSymbolicLink(path) { return Uint8Array.from(await raw.readSymbolicLink(path)); },
    async planExtents(path, offset, length, maximumSpans) {
      return copyWorkspaceExtentPlan(await raw.planExtents(path, offset, length, maximumSpans));
    },
    async write(path, bytes) { return parseWorkspaceCommit(await raw.write(path, bytes)); },
    async remove(path) { return parseWorkspaceCommit(await raw.remove(path)); },
    async liveRebase(options, idempotencyKey) {
      validateWorkspaceRebaseOptions(options);
      if (idempotencyKey !== undefined) requireIdentity(idempotencyKey);
      return parseRebase(await raw.liveRebase(
        idempotencyKey, options.maximumGenerations, options.maximumChanges, options.maximumConflicts,
      ));
    },
  };
}

function requireName(value: string): void {
  if (value.length === 0) throw new RangeError("workspace name must be non-empty");
}

function requireIdentity(value: Uint8Array): void {
  if (value.byteLength !== 16) throw new RangeError("idempotency key must be exactly 16 bytes");
}

export function adaptJoinPlanBase(
  raw: WasmRawJoinPlan,
  parseResult: (raw: WasmRawJoinResult) => JoinResult,
): FsJoinPlan {
  return {
    get targetHead() { return Uint8Array.from(raw.targetHead); },
    get commonAncestor() { return Uint8Array.from(raw.commonAncestor); },
    async apply(ifTarget, idempotencyKey) {
      requireGenerationIdentity(ifTarget);
      if (idempotencyKey !== undefined) requireIdentity(idempotencyKey);
      return parseResult(await raw.apply(ifTarget, idempotencyKey));
    },
    async close() {},
  };
}

export function adaptResolvableJoinPlan(
  raw: NativeRawJoinPlan,
  parseResult: (raw: WasmRawJoinResult) => JoinResult,
): ResolvableFsJoinPlan {
  return Object.assign(adaptJoinPlanBase(raw, parseResult), {
    async applySides(
      ifTarget: Uint8Array,
      selections: readonly MergeConflictSelection[],
      idempotencyKey?: Uint8Array,
    ): Promise<JoinResult> {
      requireGenerationIdentity(ifTarget);
      if (idempotencyKey !== undefined) requireIdentity(idempotencyKey);
      return parseResult(await raw.applySides(ifTarget, idempotencyKey, selections.map((selection) =>
        selection.kind === "file"
          ? { kind: "file", fileId: Uint8Array.from(selection.fileId), side: selection.side }
          : {
              kind: "binding",
              directoryId: Uint8Array.from(selection.directoryId),
              name: { encoding: selection.name.encoding, bytes: Uint8Array.from(selection.name.bytes) },
              side: selection.side,
            }
      )));
    },
  });
}

function requireGenerationIdentity(value: Uint8Array): void {
  if (value.byteLength !== 32) {
    throw new RangeError("target generation generation identity must be exactly 32 bytes");
  }
}
