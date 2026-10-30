import assert from "node:assert/strict";
import { createHash } from "node:crypto";
import { readFile } from "node:fs/promises";
import test from "node:test";
import initializeManifest, { validate, verifyArtifact } from "../generated/wasm/acyclic_manifest.js";

// The selected asset is the actual module produced for this package.
const module = await readFile(new URL("../generated/wasm/acyclic_manifest_bg.wasm", import.meta.url));
await initializeManifest({ module_or_path: module });
const fixture = JSON.parse(await readFile(new URL("../../../../rust/crates/manifest/fuzz/corpus/manifest_json/valid.json", import.meta.url), "utf8"));

test("Node calls the Rust cross-reference, secret and hostname rules", () => {
  validate(JSON.stringify(fixture));
  for (const value of ["sk-ant-api03-key", "Bearer token", "https://user:key@example.com/"]) {
    const manifest = structuredClone(fixture);
    manifest.environments[0].secrets[0].reference = value;
    assert.throws(() => validate(JSON.stringify(manifest)));
  }
  const manifest = structuredClone(fixture);
  manifest.environments[0].crons[0].inbox.name = "streams:/other/inbox";
  assert.throws(() => validate(JSON.stringify(manifest)));
  manifest.environments[0].crons = [];
  manifest.environments[0].entries[0].fetch = { name: "HTTP", egress: [{ host: "169.254.169.0xfe", port: 443 }] };
  assert.throws(() => validate(JSON.stringify(manifest)));
});
test("selected entry admission snapshots intrinsic array extents", () => {
  const artifact = new TextEncoder().encode("export const actor = {};");
  const asset = new Uint8Array(module);
  const manifest = structuredClone(fixture);
  const entry = manifest.environments[0].entries[0];
  const sha256 = bytes => createHash("sha256").update(bytes).digest("hex");
  entry.artifactSha256 = sha256(artifact);
  entry.assets = [{ path: "selected.wasm", sha256: sha256(asset), size: asset.length,
    mediaType: "application/wasm", role: "module" }];
  const encoded = JSON.stringify(manifest);
  const admit = (code, bytes, paths = ["selected.wasm"]) =>
    verifyArtifact(encoded, "production", "agent", code, paths, bytes);
  admit(artifact, [asset]);

  // Own properties must never control a foreign byte copy or reenter admission.
  let callbacks = 0;
  for (const view of [artifact, asset]) {
    for (const name of ["length", "byteLength", "byteOffset", "buffer", "set", Symbol.iterator]) {
      Object.defineProperty(view, name, { get() { callbacks++; throw new Error("caller getter invoked"); } });
    }
  }
  admit(artifact, [asset]);
  assert.equal(callbacks, 0);

  const substituted = new Uint8Array(module);
  substituted[substituted.length - 1] ^= 1;
  assert.throws(() => admit(artifact, [substituted]), /asset-digest/);
  assert.throws(() => admit(artifact, [], []), /missing-asset/);
  assert.throws(() => admit(artifact, [asset], []), /undeclared-asset/);
  assert.throws(() => admit(Object.create(Uint8Array.prototype), [asset]), TypeError);

  const detached = new Uint8Array(module);
  structuredClone(detached.buffer, { transfer: [detached.buffer] });
  assert.throws(() => admit(artifact, [detached]), TypeError);

  const backing = new ArrayBuffer(module.length, { maxByteLength: module.length * 2 });
  const resizable = new Uint8Array(backing);
  resizable.set(module);
  admit(artifact, [resizable]);
  backing.resize(module.length + 1);
  assert.throws(() => admit(artifact, [resizable]), /asset-size/);
  backing.resize(module.length);
  admit(artifact, [resizable]);
  assert.equal(callbacks, 0);
});
