import assert from "node:assert/strict";
import { mkdtempSync, mkdirSync, readFileSync, rmSync, symlinkSync, writeFileSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import test from "node:test";
import { sha256 } from "../../../shared/authority.mjs";
import { verifyRuntime, verifyTools, verifyTree } from "../src/inventory.mjs";

function fixture(t) {
  const root = mkdtempSync(join(tmpdir(), "sdk-erlang-inventory-"));
  t.after(() => rmSync(root, { recursive: true, force: true }));
  mkdirSync(join(root, "lib")); writeFileSync(join(root, "lib/module.beam"), "admitted module");
  return { root, index: { files_sha256: { "lib/module.beam": sha256(readFileSync(join(root, "lib/module.beam"))) }, links: {} } };
}

test("reject a modified admitted BEAM and an additional executable", t => {
  const { root, index } = fixture(t);
  verifyTree(root, index);
  writeFileSync(join(root, "lib/module.beam"), "different module");
  assert.throws(() => verifyTree(root, index), /file differs/);
  writeFileSync(join(root, "lib/module.beam"), "admitted module");
  writeFileSync(join(root, "lib/injected.beam"), "injected code");
  assert.throws(() => verifyTree(root, index), /inventory differs/);
});

test("reject traversal, malformed hashes, and file/link collisions in an inventory", t => {
  const { root, index } = fixture(t);
  assert.throws(() => verifyTree(root, { files_sha256: { "../outside": "a".repeat(64) } }), /invalid inventory/);
  assert.throws(() => verifyTree(root, { files_sha256: { "lib/module.beam": "bad" } }), /invalid inventory/);
  assert.throws(() => verifyTree(root, { ...index, links: { "lib/module.beam": "other" } }), /invalid inventory/);
  assert.throws(() => verifyTree(root, { ...index, links: { "lib/link": "../../outside" } }), /invalid inventory link/);
});

test("bind admitted link text and reject a link that escapes the runtime", { skip: process.platform === "win32" }, t => {
  const { root, index } = fixture(t);
  symlinkSync("module.beam", join(root, "lib/link")); index.links["lib/link"] = "module.beam";
  verifyTree(root, index);
  rmSync(join(root, "lib/link")); symlinkSync("./module.beam", join(root, "lib/link"));
  assert.throws(() => verifyTree(root, index), /inventory differs/);
  rmSync(join(root, "lib/link")); symlinkSync(tmpdir(), join(root, "lib/link"));
  assert.throws(() => verifyTree(root, index), /escapes root/);
});

test("OTP-only admission binds bytes and metadata without requiring Elixir", t => {
  const { root } = fixture(t);
  mkdirSync(join(root, "OTP-29.1.1")); writeFileSync(join(root, "OTP-29.1.1/erl"), "otp");
  const inventory = { schema: "acyclic.sdk.erlang-runtime-inventory.v1", otp_version: "29.1.1", otp_archive_sha256: "a".repeat(64),
    files_sha256: { "OTP-29.1.1/erl": sha256("otp") }, links: {} };
  const bytes = Buffer.from(JSON.stringify(inventory));
  const pin = { ...inventory, inventory_sha256: sha256(bytes), roots: ["OTP-29.1.1"], files: 1, links: 0 };
  verifyRuntime(root, bytes, pin);
  assert.throws(() => verifyRuntime(root, bytes, { ...pin, otp_version: "28.0" }), /metadata differs/);
  assert.throws(() => verifyRuntime(root, Buffer.from("{}"), pin), /differs from pin/);
  writeFileSync(join(root, "OTP-29.1.1/injected.beam"), "injected");
  assert.throws(() => verifyRuntime(root, bytes, pin), /inventory differs/);
});

test("generator tool admission binds versions and the complete executable set", t => {
  const { root, index } = fixture(t);
  const inventory = { ...index, schema: "acyclic.sdk.erlang-generation-tools.v1", gpb_version: "4.21.7", grpcbox_plugin_version: "0.10.0", rebar_version: "3.25.1" };
  const bytes = Buffer.from(JSON.stringify(inventory));
  const pin = { ...inventory, generator_inventory_sha256: sha256(bytes), generator_files: 1 };
  verifyTools(root, bytes, pin);
  assert.throws(() => verifyTools(root, bytes, { ...pin, gpb_version: "wrong" }), /metadata differs/);
  writeFileSync(join(root, "lib/new.beam"), "unadmitted code");
  assert.throws(() => verifyTools(root, bytes, pin), /inventory differs/);
});
