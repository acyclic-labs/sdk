/** Ordinary Rust task runtime backed by real browser providers. */
export {
  WasmReducer as BrowserTaskAuthority,
  WasmTaskRegistry as BrowserTaskRegistry,
  WasmTaskRuntime as BrowserTaskRuntime,
  type WasmBrowserTaskOptions as BrowserTaskOptions,
  type WasmMachineDefinition as BrowserMachineDefinition,
  type Worker as BrowserWorker,
  type TaskWakeCursor as BrowserWakeCursor,
  type WorkLease as BrowserWorkLease,
  type WasmBrowserAdmission as BrowserAdmission,
  type WasmBrowserTick as BrowserTick,
  type WasmBrowserWork as BrowserWork,
  type InboxItem as BrowserInboxItem,
} from "../generated/wasm/acyclic_harness_wasm.js";
export { ensureHarnessWasm as initializeBrowserRuntime } from "./wasm-runtime.js";
