import type { FsChangeSet, FsGeneration, GenerationDiff, RawGenerationChangeSet } from "./contracts.js";

/** Shared immutable diff facade with a binding-specific result decoder. */
export function createChangeSetAdapter<RawDiff>(
  adaptGeneration: (raw: RawGenerationChangeSet<RawDiff>["from"]) => FsGeneration,
  decodeDiff: (raw: RawDiff) => GenerationDiff,
) {
  const handles = new WeakMap<FsChangeSet, RawGenerationChangeSet<RawDiff>>();
  function adaptChangeSet(raw: RawGenerationChangeSet<RawDiff>): FsChangeSet {
    const changeSet: FsChangeSet = {
      get from() { return adaptGeneration(raw.from); },
      get to() { return adaptGeneration(raw.to); },
      changes() { return decodeDiff(raw.changes()); },
      async compose(next, maximumChanges) {
        if (!Number.isSafeInteger(maximumChanges) || maximumChanges <= 0) {
          throw new RangeError("maximum changes must be a positive safe integer");
        }
        return adaptChangeSet(await raw.compose(rawChangeSet(next), maximumChanges));
      },
    };
    handles.set(changeSet, raw);
    return changeSet;
  }
  function rawChangeSet(changeSet: FsChangeSet): RawGenerationChangeSet<RawDiff> {
    const raw = handles.get(changeSet);
    if (raw === undefined) throw new TypeError("change set belongs to another filesystem runtime");
    return raw;
  }
  return { adaptChangeSet, rawChangeSet };
}
