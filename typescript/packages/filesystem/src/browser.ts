import type { BrowserFsOptions, FsVolumeEngine, WasmBindings } from "./contracts.js";
import type {
  CompatibilityWire,
  OperationWindowCoordinator,
  WorkspaceContextRegistry,
} from "./compat.js";
import { adaptCompatibilityWire } from "./compat.js";
import {
  adaptWasmFs,
  adaptWasmWorkspaceContextRegistry,
  adaptWasmOperationWindows,
} from "./wasm-adapter.js";
import { installStatusMetadata } from "./workspace-results.js";

export type * from "./public-types.js";
export { DEFAULT_OBJECT_CACHE_OPTIONS, DEFAULT_VOLUME_LIMITS, portableVolumeOptions } from "./contracts.js";
export { CrossVolumeError, MountedView } from "./mounted.js";
export { performanceObserver } from "./observe.js";
export type { MountedCheckout, MountedSnapshot } from "./mounted.js";

let bindingsPromise: Promise<WasmBindings> | undefined;

async function bindings(): Promise<WasmBindings> {
  const generatedModule: string = "../generated/wasm/acyclic_fs_wasm.js";
  bindingsPromise ??= import(generatedModule).then(async (module): Promise<WasmBindings> => {
    const typed = module as WasmBindings;
    await typed.default();
    installStatusMetadata(typed.statusMetadata());
    return typed;
  }).catch((error: unknown) => {
    bindingsPromise = undefined;
    throw error;
  });
  return bindingsPromise;
}

export async function openBrowserFs(options: BrowserFsOptions): Promise<FsVolumeEngine> {
  if (options.databaseName.length === 0 || options.maximumObjectBytes <= 0) {
    throw new RangeError("browser filesystem options must be bounded and non-empty");
  }
  return adaptWasmFs(await (await bindings()).openBrowserFs(options), options.observer);
}

/** Opens the canonical Rust merge/publication wire codec. */
export async function openBrowserCompatibilityWire(): Promise<CompatibilityWire> {
  return adaptCompatibilityWire(await bindings());
}

/** Creates a process-local recursive workspace-context registry. */
export async function openBrowserWorkspaceContextRegistry(): Promise<WorkspaceContextRegistry> {
  const binding = await bindings();
  return adaptWasmWorkspaceContextRegistry(new binding.BrowserWorkspaceContextRegistry());
}

/** Opens durable leases on the filesystem's exact browser publication authority. */
export async function openBrowserOperationWindowCoordinator(filesystem: FsVolumeEngine): Promise<OperationWindowCoordinator> {
  return adaptWasmOperationWindows(filesystem);
}
