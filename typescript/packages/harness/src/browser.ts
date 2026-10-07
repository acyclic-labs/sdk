/** Ordinary Rust task runtime backed by real browser providers. */
export {
  WasmReducer as BrowserTaskAuthority,
  WasmTaskRegistry as BrowserTaskRegistry,
  WasmTaskRuntime as BrowserTaskRuntime,
  type WasmBrowserTaskOptions as BrowserTaskOptions,
  type WasmMachineDefinition as BrowserMachineDefinition,
} from "../generated/wasm/acyclic_harness_wasm.js";
export { ensureHarnessWasm as initializeBrowserRuntime } from "./wasm-runtime.js";
