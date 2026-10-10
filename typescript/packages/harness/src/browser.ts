/** Ordinary Rust task runtime backed by real browser providers. */
import type { Outcome, WasmModelJsonValue } from "../generated/wasm/acyclic_harness_wasm.js";
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
  type WasmBrowserRecoveredWork as BrowserRecoveredWork,
  type InboxItem as BrowserInboxItem,
  type WasmBrowserTaskObservation as BrowserTaskObservation,
  type WasmBrowserLeaseFence as BrowserLeaseFence,
} from "../generated/wasm/acyclic_harness_wasm.js";
export type BrowserTaskOutcome = Outcome<WasmModelJsonValue> | null;
export { ensureHarnessWasm as initializeBrowserRuntime } from "./wasm-runtime.js";
