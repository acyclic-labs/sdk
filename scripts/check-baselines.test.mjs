import assert from "node:assert/strict";
import { cpSync, mkdtempSync, readFileSync, rmSync, writeFileSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import test from "node:test";
import { checkDescriptors, generate } from "./check-baselines.mjs";

test("all complete regenerated descriptor closures preserve their pinned identities", () => {
  checkDescriptors();
});

test("descriptor checking rejects breaking changes to a secondary root and an imported schema", () => {
  const root = mkdtempSync(join(tmpdir(), "sdk-proto-mutations-"));
  try {
    for (const family of ["auth", "actors", "filesystem", "inference", "machines", "objects", "stream"]) {
      cpSync(`rust/crates/${family}/proto`, join(root, `rust/crates/${family}/proto`), { recursive: true });
    }
    for (const [relative, before, after, family] of [
      ["stream/proto/stream/v1/session.proto", "string destination_node = 3;", "string destination_node = 99;", "stream"],
      ["filesystem/proto/protocol/v1/protocol.proto", "string descriptor_digest = 2;", "string descriptor_digest = 99;", "filesystem"],
    ]) {
      const path = join(root, "rust/crates", relative);
      const original = readFileSync(path, "utf8");
      assert.ok(original.includes(before));
      writeFileSync(path, original.replace(before, after));
      assert.throws(() => checkDescriptors(root), new RegExp(`${family} generated descriptor differs`));
      writeFileSync(path, original);
    }
  } finally {
    rmSync(root, { recursive: true, force: true });
  }
});

test("both native admission baselines equal the actual Rust generator", () => {
  for (const family of ["actors", "stream"]) {
    const actual = JSON.parse(generate(["native-family", process.cwd(), family]));
    const baseline = JSON.parse(readFileSync(`compatibility/native-families/${family}.json`, "utf8"));
    assert.deepEqual(actual, baseline);
  }
});
