import type {
  FsGeneration, FsJoinPlan, FsWorkspace, JoinResult, MergeConflictSelection,
  NativeRawJoinPlan, ResolvableFsJoinPlan, WasmRawGeneration, WasmRawJoinPlan,
  WasmRawJoinResult, WasmRawWorkspace, WorkspaceRebaseResult,
} from "./contracts.js";
import { copyWorkspaceExtentPlan, copyWorkspaceStat } from "./workspace-copies.js";
import { parseWorkspaceCommit, parseWorkspaceDelete } from "./workspace-results.js";

function nativeBoundary<T>(value: unknown): T {
  return value as T;
}

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
  validatePositiveBound: (value: number, label: string) => void,
): WorkspaceOperations {
  return {
    async head() { return Uint8Array.from(await raw.head()); },
    async sync() { return adaptGeneration(await raw.sync()); },
    async checkpoint(label) {
      return adaptGeneration(await raw.checkpoint(label));
    },
    async pin(identity) {
      return adaptGeneration(await raw.pin(identity));
    },
    async delete(idempotencyKey) {
      return parseWorkspaceDelete(await raw.delete(idempotencyKey));
    },
    async read(path, maximumBytes) {
      return Uint8Array.from(await raw.read(path, maximumBytes));
    },
    async readRange(path, offset, length) { return Uint8Array.from(await raw.readRange(path, offset, length)); },
    async stat(path) { return copyWorkspaceStat(await raw.stat(path)); },
    async readSymbolicLink(path) { return Uint8Array.from(await raw.readSymbolicLink(path)); },
    async planExtents(path, offset, length, maximumSpans) {
      validatePositiveBound(maximumSpans, "maximum extent spans");
      return copyWorkspaceExtentPlan(await raw.planExtents(path, offset, length, maximumSpans));
    },
    async write(path, bytes) { return parseWorkspaceCommit(await raw.write(path, bytes)); },
    async remove(path) { return parseWorkspaceCommit(await raw.remove(path)); },
    async liveRebase(options, idempotencyKey) {
      validatePositiveBound(options.maximumGenerations, "maximum rebase generations");
      validatePositiveBound(options.maximumChanges, "maximum rebase changes");
      validatePositiveBound(options.maximumConflicts, "maximum rebase conflicts");
      return parseRebase(await raw.liveRebase(
        idempotencyKey, options.maximumGenerations, options.maximumChanges, options.maximumConflicts,
      ));
    },
  };
}

export function adaptJoinPlanBase(
  raw: WasmRawJoinPlan,
  parseResult: (raw: WasmRawJoinResult) => JoinResult,
): FsJoinPlan {
  return {
    get targetHead() { return Uint8Array.from(raw.targetHead); },
    get commonAncestor() { return Uint8Array.from(raw.commonAncestor); },
    async apply(ifTarget, idempotencyKey) {
      return parseResult(nativeBoundary<Parameters<typeof parseResult>[0]>(await raw.apply(ifTarget, idempotencyKey)));
    },
    async close() {},
  };
}

export function adaptResolvableJoinPlan(
  raw: NativeRawJoinPlan,
  parseResult: (raw: WasmRawJoinResult) => JoinResult,
): ResolvableFsJoinPlan {
  return Object.assign(adaptJoinPlanBase(nativeBoundary<WasmRawJoinPlan>(raw), parseResult), {
    async applySides(
      ifTarget: Uint8Array,
      selections: readonly MergeConflictSelection[],
      idempotencyKey?: Uint8Array,
    ): Promise<JoinResult> {
      return parseResult(nativeBoundary<Parameters<typeof parseResult>[0]>(await raw.applySides(ifTarget, idempotencyKey, selections.map((selection) =>
        selection.kind === "file"
          ? { kind: "file", fileId: Uint8Array.from(selection.fileId), side: selection.side }
          : {
              kind: "binding",
              directoryId: Uint8Array.from(selection.directoryId),
              name: { encoding: selection.name.encoding, bytes: Uint8Array.from(selection.name.bytes) },
              side: selection.side,
            }
      ))));
    },
  });
}
