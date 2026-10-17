import assert from "node:assert/strict";
import { readFile } from "node:fs/promises";
import test from "node:test";
import initializeManifest, { validate } from "../generated/wasm/acyclic_manifest.js";

await initializeManifest({ module_or_path: await readFile(new URL("../generated/wasm/acyclic_manifest_bg.wasm", import.meta.url)) });
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