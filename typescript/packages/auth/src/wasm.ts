// Share one initialization promise across holders and resource identities.
import { wasmSource } from "@acyclic-labs/core/wasm";
import initialize from "../generated/wasm/acyclic_auth_wasm.js";

const wasm = new URL("../generated/wasm/acyclic_auth_wasm_bg.wasm", import.meta.url);
await initialize({ module_or_path: await wasmSource(wasm) });

export * from "../generated/wasm/acyclic_auth_wasm.js";
