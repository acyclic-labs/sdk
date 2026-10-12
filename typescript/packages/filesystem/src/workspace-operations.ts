import type { FsJoinPlan, JoinResult, MergeConflictSelection, ResolvableFsJoinPlan } from "./contracts.js";
import type { NativeRawJoinPlan, WasmRawJoinPlan } from "./binding-types.js";
import { parseJoinResult } from "./workspace-results.js";
import { copyName } from "./binding-values.js";

export function adaptJoinPlan(
  raw: WasmRawJoinPlan & Partial<Pick<NativeRawJoinPlan, "applySides">>,
): FsJoinPlan & Partial<Pick<ResolvableFsJoinPlan, "applySides">> {
  return {
    get targetHead() { return Uint8Array.from(raw.targetHead); },
    get commonAncestor() { return Uint8Array.from(raw.commonAncestor); },
    async apply(ifTarget, idempotencyKey) {
      return parseJoinResult(await raw.apply(ifTarget, idempotencyKey));
    },
    async close() {},
    ...(raw.applySides === undefined ? {} : {
      async applySides(
        ifTarget: Uint8Array,
        selections: readonly MergeConflictSelection[],
        idempotencyKey?: Uint8Array,
      ): Promise<JoinResult> {
        return parseJoinResult(await raw.applySides!(ifTarget, idempotencyKey, selections.map((selection) =>
          selection.kind === "file"
            ? { kind: "file", fileId: Uint8Array.from(selection.fileId), side: selection.side }
            : {
                kind: "binding",
                directoryId: Uint8Array.from(selection.directoryId),
                name: copyName(selection.name),
                side: selection.side,
              }
        )));
      },
    }),
  };
}
