export {
  WasmProviderRegistry as ModelRegistry,
  WasmModelProvider,
  type ProviderSpec,
} from "../generated/wasm/acyclic_harness_wasm.js";
export { ensureHarnessWasm as initializeModelProviders } from "./wasm-runtime.js";
