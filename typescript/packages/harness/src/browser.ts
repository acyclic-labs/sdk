/** Ordinary Rust task runtime backed by real browser providers. */
import type { Outcome, WasmModelJsonValue } from "../generated/wasm/acyclic_harness_wasm.js";
export {
  WasmReducer as BrowserTaskAuthority,
  WasmTaskRegistry as BrowserTaskRegistry,
  WasmTaskRuntime as BrowserTaskRuntime,
  type WasmBrowserTaskOptions as BrowserTaskOptions,
  type WorkLease as BrowserWorkLease,
  type WasmBrowserRecoveredWork as BrowserRecoveredWork,
} from "../generated/wasm/acyclic_harness_wasm.js";
export type BrowserTaskOutcome = Outcome<WasmModelJsonValue> | null;
export { ensureHarnessWasm as initializeBrowserRuntime } from "./wasm-runtime.js";
