/** The public frontend shares one Rust client across local and hosted backends. */
export * from "./client.js";
export * from "./cache.js";
export * from "./client-demand.js";
export { RequestScheduler, RequestCapacityError, type RequestLimits } from "./client-requests.js";
export { SnapshotStore, type ClientSnapshot } from "./client-snapshot.js";
export * from "./pagination.js";
export * from "./hypotheses.js";
export * from "./providers.js";
export { createBrowserMcpHttpProvider } from "./mcp-http.js";
export {
  WasmMcpHttpTransport, WasmMcpHttpInitialization,
  validateMcpCatalog, mcpModelDefinitions, searchMcpCatalog,
  validateMcpStdioRequest, validateMcpCallbackRequest, mcpCallbackOperation,
  jsonToolProjectionSchema,
  type McpCatalog, type McpCallbackRequest,
} from "../generated/wasm/acyclic_harness_wasm.js";
export { ensureMcpWasm as initializeMcp } from "./wasm-runtime.js";
