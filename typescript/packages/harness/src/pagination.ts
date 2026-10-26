/** Whole provider pages and fetch custody are owned by the shared Rust core. */
export {
  WasmPageWindow as PageWindow,
  WasmPageRequest as PageRequest,
  type WasmPageData as Page,
} from "../generated/wasm/acyclic_harness_wasm.js";

export type PageDirection = "before" | "after";
