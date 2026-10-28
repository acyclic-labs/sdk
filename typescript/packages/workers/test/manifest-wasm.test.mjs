import assert from "node:assert/strict";
import { readFile } from "node:fs/promises";
import test from "node:test";
import initializeManifest, { validate, parseSecretReference, formatSecretReference, selectSecretReference } from "../generated/wasm/acyclic_manifest.js";

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
test("standalone secret projection uses the same bounded Rust grammar", () => {
  for (const reference of ["secrets:/a", "secrets:/Scope/deep/NAME", `secrets:/${"a".repeat(63)}/${"b".repeat(63)}/${"c".repeat(63)}/${"d".repeat(56)}`]) {
    assert.equal(formatSecretReference(parseSecretReference(reference)), reference);
    assert.deepEqual(JSON.parse(selectSecretReference("MODEL_KEY", reference)), { name: "MODEL_KEY", reference });
  }
  for (const reference of ["secrets:/", "secrets://a", "secrets:/a/", "secrets:/a//b", "secrets:/a%2Fb", "secrets:/a:b", "secrets:/a?q", "secrets:/a#f", "secrets:/é", "secrets:/.",
    `secrets:/${"x".repeat(64)}`, `secrets:/${"a".repeat(63)}/${"b".repeat(63)}/${"c".repeat(63)}/${"d".repeat(57)}`]) assert.throws(() => parseSecretReference(reference));
  assert.throws(() => formatSecretReference(["a/b"]));
  assert.throws(() => selectSecretReference("NOT-AN-ENV-NAME", "secrets:/a"));
  const refusal = error => error instanceof Error && error.name === "SecretReferenceError"
    && error.code === "invalid_argument" && typeof error.path === "string";
  assert.throws(() => parseSecretReference("x".repeat(1_048_576)), refusal);
  assert.throws(() => formatSecretReference(new Array(0xffff_ffff)), refusal);
  assert.throws(() => formatSecretReference(["a", "b".repeat(1_048_576)]), refusal);
  assert.throws(() => selectSecretReference("N".repeat(1_048_576), "secrets:/a"), refusal);
  const coercion = { toString() { throw new Error("input must not be coerced"); } };
  assert.throws(() => parseSecretReference(coercion), refusal);
  assert.throws(() => formatSecretReference([coercion]), refusal);
});
