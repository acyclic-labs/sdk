/** Rust agent host backed by real browser providers. */
export {
  WasmAgentAuthority as BrowserAgentAuthority,
  WasmAgentRegistry as BrowserAgentRegistry,
  WasmAgents as BrowserAgents,
  WasmAgentProviders as BrowserAgentProviders,
  AgentGuest,
  type WasmBrowserAgentOptions as BrowserAgentOptions,
  type WasmDelivered as BrowserDelivered,
  type WasmAgentPage as BrowserAgentPage,
  type WasmAgentRecord as BrowserAgentRecord,
} from "../generated/wasm/acyclic_harness_wasm.js";
export { ensureBrowserWasm as initializeBrowserRuntime } from "./wasm-runtime.js";
