import type {
  FsGeneration, WasmRawGeneration, WorkspaceDirectoryPage, WorkspaceExtentPlan, WorkspaceStat,
} from "./contracts.js";

/** One generation facade per binding, preserving the binding's identity domain. */
export function createGenerationAdapter(
  decodeStat: (value: WorkspaceStat) => WorkspaceStat,
  decodePage: (value: WorkspaceDirectoryPage) => WorkspaceDirectoryPage,
  decodePlan: (value: WorkspaceExtentPlan) => WorkspaceExtentPlan,
) {
  const handles = new WeakMap<FsGeneration, WasmRawGeneration>();
  function adaptGeneration(raw: WasmRawGeneration): FsGeneration {
    const generation: FsGeneration = {
      get id() { return Uint8Array.from(raw.id); },
      get workspaceId() { return Uint8Array.from(raw.workspaceId); },
      async read(path, maximumBytes) {
        if (maximumBytes <= 0n) throw new RangeError("maximum read bytes must be positive");
        return Uint8Array.from(await raw.read(path, maximumBytes));
      },
      async readRange(path, offset, length) { return Uint8Array.from(await raw.readRange(path, offset, length)); },
      async stat(path) { return decodeStat(await raw.stat(path)); },
      async listDirectory(path, after, maximumEntries) {
        return decodePage(await raw.listDirectory(path, after, maximumEntries));
      },
      async readSymbolicLink(path) { return Uint8Array.from(await raw.readSymbolicLink(path)); },
      async planExtents(path, offset, length, maximumSpans) {
        return decodePlan(await raw.planExtents(path, offset, length, maximumSpans));
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
