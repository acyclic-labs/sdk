import { resolveObserver } from "./observe.js";
import type { BrowserFsOptions, FsVolumeEngine, WasmBindings } from "./contracts.js";
import type {
  OperationWindowCoordinator,
} from "./compat.js";
import {
  adaptFs,
  adaptOperationWindows,
} from "./filesystem-adapter.js";

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
  return adaptFs(await (await bindings()).openBrowserFs(options), resolveObserver(options.observer));
}

/** Opens durable leases on the filesystem's exact browser publication authority. */
export async function openBrowserOperationWindowCoordinator(filesystem: FsVolumeEngine): Promise<OperationWindowCoordinator> {
  return adaptOperationWindows(filesystem);
}
