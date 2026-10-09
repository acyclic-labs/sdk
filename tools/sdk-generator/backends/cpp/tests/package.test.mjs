import assert from "node:assert/strict";
import { truncateSync } from "node:fs";
import test from "node:test";
import { readPackage } from "../src/package.mjs";
import { sha256 } from "../../../shared/authority.mjs";
import { fixture } from "./fixtures/package.mjs";

test("archive admission verifies all 17 payload files against the producer and Rust export", t => {
  const f = fixture(t), admitted = readPackage(f.args);
  assert.equal(admitted.payload.size, 17);
  for (const [name, bytes] of admitted.payload) assert.deepEqual(bytes, f.payload.get(name));
});

test("authority, source, template and tool admission drift reject", t => {
  for (const mutate of [
    f => { f.receipt.source_revision = "b".repeat(40); },
    f => { f.receipt.input_sha256 = {}; },
    f => { f.receipt.generator_sha256 = "0".repeat(64); },
    f => { f.receipt.source_sha256["../toolchains/build-runtime.cmake"] = "0".repeat(64); },
    f => { f.receipt.source_sha256.extra = "0".repeat(64); },
    f => { f.receipt.toolchain_sha256 = "0".repeat(64); },
    f => { f.receipt.tool_sha256.cpp_plugin = "0".repeat(64); },
    f => { f.receipt.cpp_plugin_coordinate = "substituted"; },
    f => { f.receipt.template_sha256["CMakeLists.txt"] = "0".repeat(64); },
  ]) { const f = fixture(t); mutate(f); f.saveReceipt(); assert.throws(() => readPackage(f.args)); }
});

test("unsafe, duplicate, extra and incomplete receipt inventories reject", t => {
  for (const mutate of [
    f => f.receipt.outputs.push("../outside.ex"),
    f => f.receipt.outputs.push("LICENSE"),
    f => f.receipt.outputs.push("license"),
    f => { f.receipt.output_sha256.extra = "0".repeat(64); },
    f => { f.receipt.outputs = f.receipt.outputs.filter(name => !name.endsWith("actors.pb.h")); },
    f => { f.receipt.outputs = f.receipt.outputs.filter(name => !name.endsWith("workers.grpc.pb.cc")); },
  ]) { const f = fixture(t); mutate(f); f.saveReceipt(); assert.throws(() => readPackage(f.args)); }
});

test("links, traversal, duplicate and unlisted archive entries reject", t => {
  for (const mutate of [
    entries => { entries[0].type = "2"; },
    entries => { entries[0].name = "../outside"; },
    entries => { entries.push(entries[0]); },
    entries => { entries.push({ name: "extra.ex", bytes: Buffer.from("extra") }); },
  ]) { const f = fixture(t), entries = f.entries(); mutate(entries); f.saveArchive(entries); assert.throws(() => readPackage(f.args)); }
});

test("missing, changed and metadata-substituted archive payloads reject", t => {
  const missing = fixture(t); missing.saveArchive(missing.entries().slice(1));
  assert.throws(() => readPackage(missing.args), /inventory differs/);
  const altered = fixture(t), entries = altered.entries(); entries[0].bytes = Buffer.from("changed"); altered.saveArchive(entries);
  assert.throws(() => readPackage(altered.args), /payload digest differs/);
  for (const name of ["CMakeLists.txt", "authority/rust-authority.json"]) {
    const f = fixture(t); f.payload.set(name, Buffer.from("substituted")); f.receipt.output_sha256[name] = sha256(f.payload.get(name)); f.saveReceipt(); f.saveArchive();
    assert.throws(() => readPackage(f.args), /metadata differs|Rust input differs/);
  }
});

test("external digest, archive size and receipt size bounds are enforced", t => {
  const f = fixture(t); f.args.sha256 = "0".repeat(64); assert.throws(() => readPackage(f.args), /archive digest differs/);
  truncateSync(f.args.package, 16 * 1024 * 1024 + 1); assert.throws(() => readPackage(f.args), /size bound/);
  const large = fixture(t); truncateSync(large.args.receipt, 1024 * 1024 + 1); assert.throws(() => readPackage(large.args), /receipt exceeds size bound/);
});
