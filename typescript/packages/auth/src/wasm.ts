// Share one initialization promise across holders and resource identities.
import initialize from "../generated/wasm/acyclic_auth_wasm.js";

const wasm = new URL("../generated/wasm/acyclic_auth_wasm_bg.wasm", import.meta.url);
if (wasm.protocol === "file:") {
  const { readFile } = await import("node:fs/promises");
  await initialize({ module_or_path: Uint8Array.from(await readFile(wasm)) });
} else {
  await initialize({ module_or_path: wasm });
}

export * from "../generated/wasm/acyclic_auth_wasm.js";
