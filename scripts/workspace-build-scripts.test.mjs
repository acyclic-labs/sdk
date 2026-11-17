import assert from "node:assert/strict";
import { globSync, readFileSync } from "node:fs";
import { dirname, join, resolve } from "node:path";
import { fileURLToPath } from "node:url";
import test from "node:test";

const root = resolve(dirname(fileURLToPath(import.meta.url)), "..");
const manifests = ["package.json", ...globSync(["typescript/packages/*/package.json", "typescript/packages/@acyclic/*/package.json"], { cwd: root })]
  .map(path => ({ path, manifest: JSON.parse(readFileSync(join(root, path), "utf8")) }));

test("every package script calls a script that package defines", () => {
  for (const { path, manifest } of manifests) {
    const scripts = manifest.scripts ?? {};
    for (const [name, command] of Object.entries(scripts)) {
      for (const [, called] of command.matchAll(/\bbun run ([A-Za-z][\w:-]*)/gu)) {
        assert.ok(Object.hasOwn(scripts, called), `${path}:${name} calls missing script ${called}`);
      }
    }
  }
});

test("the wasm-bindgen helper installs the version Cargo pins", () => {
  const pin = readFileSync(join(root, "Cargo.toml"), "utf8").match(/^wasm-bindgen = "=([^"]+)"/m)?.[1];
  assert.ok(pin, "Cargo.toml pins wasm-bindgen");
  const helper = readFileSync(join(root, "scripts/ensure-wasm-bindgen.sh"), "utf8");
  const versions = [...new Set([...helper.matchAll(/wasm-bindgen (\d+\.\d+\.\d+)/gu)].map(match => match[1]))];
  assert.deepEqual(versions, [pin], "the helper installs only the pinned version");
  assert.match(helper, new RegExp(`/download/${pin.replaceAll(".", "\\.")}/`, "u"), "the helper downloads the pinned release");
});
