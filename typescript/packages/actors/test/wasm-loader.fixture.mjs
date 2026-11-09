import assert from "node:assert/strict";
import { readFileSync } from "node:fs";
import { registerHooks } from "node:module";
import * as bridge from "../generated/wasm/acyclic_actors_wasm.js";
import { GuestActivation } from "../dist/guest-host.js";
import { GUEST_ABI_VERSION } from "../dist/guest.js";

const mode = process.argv[2];
const compiled = new WebAssembly.Module(readFileSync(new URL("../generated/wasm/acyclic_actors_wasm_bg.wasm", import.meta.url)));
let instance;
if (mode === "preinitialized") instance = bridge.initSync({ module: compiled });
let fsImports = 0;
registerHooks({
  resolve(specifier, context, next) {
    if (specifier === "node:fs/promises") {
      fsImports++;
      if (mode !== "file") throw new Error("node:fs/promises unavailable in guest host");
    }
    return next(specifier, context);
  },
});

const { admitDeploymentBindings, loadWasmModule } = await import("../dist/client.js");
if (mode === "provided") await loadWasmModule(compiled);
const declaration = {
  name: "originalState", capability: "", resource: "", targetAccount: new Uint8Array(), stateTransaction: true,
};
let restores = 0;
const codec = { name: "loader-test", version: "1" };
const definition = {
  abiVersion: GUEST_ABI_VERSION,
  codec: { identity: codec },
  async restore(checkpoint) {
    const bindings = await admitDeploymentBindings([declaration]);
    assert.equal(bindings[0].name, "originalState");
    restores++;
    return checkpoint.bytes[0];
  },
  fetch(state) {
    let value;
    state.with(current => { value = current; });
    return Response.json(value);
  },
};
const context = {
  env: {}, clock: () => 0n, deliveryId: null, admittedIdentity: null,
  actor: { id: null },
};
const activation = new GuestActivation(definition, { codec, bytes: new Uint8Array([7]) }, context);
const response = await activation.fetch(new Request("https://guest.test/"), context);
assert.equal(await response.json(), 7);
assert.equal(restores, 1);
// Admission still executes the Rust validator in the reused instance.
await assert.rejects(admitDeploymentBindings([{ ...declaration, resource: "identity:/forged" }]));
const loaded = await loadWasmModule();
assert.strictEqual(loaded, await loadWasmModule(compiled));
if (mode === "preinitialized") assert.strictEqual(await bridge.default(), instance);
assert.equal(fsImports, mode === "file" ? 1 : 0);
console.log(`restored:${mode}`);
