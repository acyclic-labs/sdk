import assert from "node:assert/strict";
import { truncateSync } from "node:fs";
import test from "node:test";
import { readPackage, targets } from "../src/package.mjs";
import { sha256 } from "../../../shared/authority.mjs";
import { fixture } from "./fixtures/package.mjs";

test("archive admission binds all payload bytes to the maintained producer and Rust authority", t => {
  const f = fixture(t), admitted = readPackage(f.args);
  assert.equal(admitted.payload.size, 10);
  for (const [name, bytes] of admitted.payload) assert.deepEqual(bytes, f.payload.get(name));
});

test("authority, producer and admitted tool drift reject before installation", t => {
  for (const mutate of [
    f => { f.receipt.source_revision = "b".repeat(40); },
    f => { delete f.receipt.input_sha256[targets[0]]; },
    f => { f.receipt.generator_sha256 = "0".repeat(64); },
    f => { f.receipt.authority_reader_sha256 = "0".repeat(64); },
    f => { f.receipt.toolchain_sha256 = "0".repeat(64); },
    f => { f.receipt.tool_sha256.grpc_plugin = "0".repeat(64); },
    f => { f.receipt.swift_runtime_sha256 = {}; },
  ]) { const f = fixture(t); mutate(f); f.saveReceipt(); assert.throws(() => readPackage(f.args)); }
});

test("unsafe and incomplete receipt inventories reject", t => {
  for (const mutate of [
    f => f.receipt.outputs.push("../outside.swift"),
    f => f.receipt.outputs.push("LICENSE"),
    f => f.receipt.outputs.push("license"),
    f => { f.receipt.output_sha256.extra = "0".repeat(64); },
    f => { f.receipt.outputs = f.receipt.outputs.filter(name => !name.endsWith("actors.grpc.swift")); delete f.receipt.output_sha256["Sources/AcyclicTransport/actors/v1/actors.grpc.swift"]; },
  ]) { const f = fixture(t); mutate(f); f.saveReceipt(); assert.throws(() => readPackage(f.args)); }
});

test("links, traversal, duplicates and unlisted archive members reject", t => {
  for (const mutate of [
    entries => { entries[0].type = "2"; },
    entries => { entries[0].name = "../outside"; },
    entries => { entries.push(entries[0]); },
    entries => { entries.push({ name: "extra.swift", bytes: Buffer.from("extra") }); },
  ]) { const f = fixture(t), entries = f.entries(); mutate(entries); f.saveArchive(entries); assert.throws(() => readPackage(f.args)); }
});

test("missing, altered and metadata-substituted payloads reject", t => {
  const missing = fixture(t); missing.saveArchive(missing.entries().slice(1));
  assert.throws(() => readPackage(missing.args), /inventory differs/);
  const altered = fixture(t), entries = altered.entries(); entries[0].bytes = Buffer.from("altered"); altered.saveArchive(entries);
  assert.throws(() => readPackage(altered.args), /payload digest differs/);
  const metadata = fixture(t); metadata.payload.set("Package.swift", Buffer.from("substituted manifest"));
  metadata.receipt.output_sha256["Package.swift"] = sha256(metadata.payload.get("Package.swift")); metadata.saveReceipt(); metadata.saveArchive();
  assert.throws(() => readPackage(metadata.args), /metadata differs/);
});

test("external archive digest and compressed size bounds are enforced", t => {
  const f = fixture(t); f.args.sha256 = "0".repeat(64);
  assert.throws(() => readPackage(f.args), /archive digest differs/);
  truncateSync(f.args.package, 16 * 1024 * 1024 + 1);
  assert.throws(() => readPackage(f.args), /size bound/);
});
