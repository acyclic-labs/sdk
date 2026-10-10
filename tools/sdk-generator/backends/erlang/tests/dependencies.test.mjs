import assert from "node:assert/strict";
import { mkdirSync, mkdtempSync, rmSync, writeFileSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import test from "node:test";
import { sha256 } from "../../../shared/authority.mjs";
import { verifyDependencies } from "../src/qualify.mjs";

function fixture(t) {
  const root = mkdtempSync(join(tmpdir(), "erlang-dependencies-"));
  t.after(() => rmSync(root, { recursive: true, force: true }));
  mkdirSync(join(root, "grpcbox-0.18.0/ebin"), { recursive: true });
  writeFileSync(join(root, "grpcbox-0.18.0/ebin/grpcbox.beam"), "admitted compiled dependency");
  const index = { schema: "acyclic.sdk.erlang-runtime-dependencies.v1", upstream_lock_sha256: "a".repeat(64),
    packages: { grpcbox: { version: "0.18.0", application: "grpcbox", directory: "grpcbox-0.18.0", archive_sha256: "b".repeat(64) } },
    files_sha256: { "grpcbox-0.18.0/ebin/grpcbox.beam": sha256("admitted compiled dependency") } };
  const bytes = Buffer.from(JSON.stringify(index));
  const pin = { packages: index.packages, upstream_lock_sha256: index.upstream_lock_sha256, dependency_inventory_sha256: sha256(bytes), dependency_files: 1 };
  return { root, bytes, pin };
}

test("bind runtime dependency source lock, versions and complete compiled tree", t => {
  const f = fixture(t); verifyDependencies(f.root, f.bytes, f.pin);
  for (const pin of [{ ...f.pin, upstream_lock_sha256: "c".repeat(64) }, { ...f.pin, dependency_files: 2 }, { ...f.pin, packages: {} }]) {
    assert.throws(() => verifyDependencies(f.root, f.bytes, pin), /metadata differs/);
  }
  assert.throws(() => verifyDependencies(f.root, Buffer.from("{}"), f.pin), /differs from pin/);
});

test("reject altered dependency BEAMs and an additional application", t => {
  const f = fixture(t);
  writeFileSync(join(f.root, "grpcbox-0.18.0/ebin/grpcbox.beam"), "altered");
  assert.throws(() => verifyDependencies(f.root, f.bytes, f.pin), /file differs/);
  writeFileSync(join(f.root, "grpcbox-0.18.0/ebin/grpcbox.beam"), "admitted compiled dependency");
  mkdirSync(join(f.root, "injected-1.0/ebin"), { recursive: true });
  writeFileSync(join(f.root, "injected-1.0/ebin/injected.beam"), "injected");
  assert.throws(() => verifyDependencies(f.root, f.bytes, f.pin), /inventory differs/);
});
