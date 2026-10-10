import type {
  WasmTaskRuntime, WasmTaskAdmissionWire, WasmTaskChildrenPage,
  WasmSelectedDirectory, WasmSelectedFileRead, WasmFileRefWire, PrivateDirectoryPage,
  TaskVolumeSnapshot,
} from "../generated/wasm/acyclic_harness_wasm.js";

/** A selected retained source; the caller identity remains the sole read principal. */
export interface BrowserSourceInspection {
  readonly admission: WasmTaskAdmissionWire;
  project(): Promise<TaskVolumeSnapshot>;
  privateVolume(): Promise<TaskVolumeSnapshot>;
  agent(): Promise<string>;
  children(expectedRevision: bigint | null, afterSlot: string | null, maximum: number): Promise<WasmTaskChildrenPage>;
  list(selection: Omit<WasmSelectedDirectory, "caller" | "source">, after: string | null, maximum: number): Promise<PrivateDirectoryPage>;
  readPath(selection: Omit<WasmSelectedDirectory, "caller" | "source">): Promise<WasmSelectedFileRead>;
  diff(before: WasmFileRefWire, after: WasmFileRefWire): Promise<boolean>;
}

/** No cache, registry, mounted swarm, or TypeScript-owned authority decisions. */
export async function inspectTaskSource(runtime: WasmTaskRuntime, caller: string, source: string): Promise<BrowserSourceInspection> {
  const admission = await runtime.inspectSource(caller, source);
  return Object.freeze({
    admission,
    project: () => runtime.sourceProject(caller, source),
    privateVolume: () => runtime.sourcePrivateVolume(caller, source),
    agent: () => runtime.sourceAgent(caller, source),
    children: (revision: bigint | null, after: string | null, maximum: number) =>
      runtime.children(source, revision ?? undefined, after ?? undefined, maximum),
    list: (selection: Omit<WasmSelectedDirectory, "caller" | "source">, after: string | null, maximum: number) =>
      runtime.listSelectedDirectory({ ...selection, caller, source }, after ?? undefined, maximum),
    readPath: (selection: Omit<WasmSelectedDirectory, "caller" | "source">) =>
      runtime.readSelectedPath({ ...selection, caller, source }),
    diff: (before: WasmFileRefWire, after: WasmFileRefWire) => runtime.diffSelectedFiles(caller, source, before, after),
  });
}
