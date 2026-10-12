import type { FsGeneration, WasmRawGeneration } from "./contracts.js";
import { copyBytes, ownBytes } from "./binding-values.js";
import { copyWorkspaceStat, copyWorkspaceDirectoryPage, copyWorkspaceExtentPlan } from "./workspace-copies.js";

/** One generation facade per binding, preserving the binding's identity domain. */
export function createGenerationAdapter() {
  const handles = new WeakMap<FsGeneration, WasmRawGeneration>();
  function adaptGeneration(raw: WasmRawGeneration): FsGeneration {
    const generation: FsGeneration = {
      get id() { return copyBytes(raw.id); },
      get workspaceId() { return copyBytes(raw.workspaceId); },
      async read(path, maximumBytes) {
        if (maximumBytes <= 0n) throw new RangeError("maximum read bytes must be positive");
        return ownBytes(await raw.read(path, maximumBytes));
      },
      async readRange(path, offset, length) { return ownBytes(await raw.readRange(path, offset, length)); },
      async stat(path) { return copyWorkspaceStat(await raw.stat(path)); },
      async listDirectory(path, after, maximumEntries) {
        return copyWorkspaceDirectoryPage(await raw.listDirectory(path, after, maximumEntries));
      },
      async readSymbolicLink(path) { return ownBytes(await raw.readSymbolicLink(path)); },
      async planExtents(path, offset, length, maximumSpans) {
        return copyWorkspaceExtentPlan(await raw.planExtents(path, offset, length, maximumSpans));
      },
      async pin(identity) {
        if (identity.length === 0) throw new RangeError("workspace name must be non-empty");
        return adaptGeneration(await raw.pin(identity));
      },
    };
    handles.set(generation, raw);
    return generation;
  }
  function rawGeneration(generation: FsGeneration): WasmRawGeneration {
    const raw = handles.get(generation);
    if (raw === undefined) throw new TypeError("generation belongs to another filesystem runtime");
    return raw;
  }
  return { adaptGeneration, rawGeneration };
}
