/** Discovery and projection are implemented once in the Rust harness. */
export {
  captureDiscoveredContext, contextForRequest, readPinnedContextPath,
  projectDiscoveredContext, applyContextProjection, parseSkillMetadata, fileDescriptor,
  type ContextDiscovery, type ContextDiscoveryReader, type DiscoveredContext,
  type PinnedContextPath, type SkillMetadata,
} from "../generated/wasm/acyclic_harness_wasm.js";
export { ensureContextWasm as initializeContextDiscovery } from "./wasm-runtime.js";
