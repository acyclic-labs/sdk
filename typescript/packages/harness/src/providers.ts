export {
  WasmProviderRegistry as ModelRegistry,
  WasmModelProvider,
  WasmModelAlloy,
  type AlloySpec,
  type AlloySelection,
  type ModelFeatures,
  type ProviderSpec,
} from "../generated/wasm/acyclic_harness_wasm.js";
export { ensureHarnessWasm as initializeModelProviders } from "./wasm-runtime.js";
