import assert from "node:assert/strict";
import { truncateSync } from "node:fs";
import test from "node:test";
import { readPackage } from "../src/package.mjs";
import { sha256 } from "../../../shared/authority.mjs";
import { fixture } from "./fixtures/package.mjs";

test("admit complete Erlang archive including original Rust descriptors and application metadata", t => {
  const f = fixture(t), admitted = readPackage(f.args);
  assert.equal(admitted.payload.size, 21);
  for (const [name, bytes] of admitted.payload) assert.deepEqual(bytes, f.payload.get(name));
});

test("reject authority, producer, template, runtime and executable bundle drift", t => {
  for (const mutate of [
    f => { f.receipt.source_revision = "b".repeat(40); },
    f => { f.receipt.input_sha256 = {}; },
    f => { f.receipt.source_sha256["inventory.mjs"] = "0".repeat(64); },
    f => { f.receipt.source_sha256.extra = "0".repeat(64); },
    f => { f.receipt.toolchain_sha256 = "0".repeat(64); },
    f => { f.receipt.generator_inventory_sha256 = "0".repeat(64); },
    f => { f.receipt.runtime_inventory_sha256 = "0".repeat(64); },
    f => { f.receipt.grpcbox_plugin_version = "wrong"; },
    f => { f.receipt.template_sha256["rebar.config"] = "0".repeat(64); },
  ]) { const f = fixture(t); mutate(f); f.saveReceipt(); assert.throws(() => readPackage(f.args)); }
});

test("reject unsafe, duplicate, case-colliding and incomplete receipt inventories", t => {
  for (const mutate of [
    f => f.receipt.outputs.push("../outside.erl"),
    f => f.receipt.outputs.push("LICENSE"),
    f => f.receipt.outputs.push("license"),
    f => { f.receipt.output_sha256.extra = "0".repeat(64); },
    f => { f.receipt.outputs = f.receipt.outputs.filter(name => !name.endsWith("actors_pb.erl")); },
    f => { f.receipt.outputs = f.receipt.outputs.filter(name => !name.endsWith("actors.proto.bin")); },
  ]) { const f = fixture(t); mutate(f); f.saveReceipt(); assert.throws(() => readPackage(f.args)); }
});

test("reject archive links, traversal, duplicate members and additional code", t => {
  for (const mutate of [
    entries => { entries[0].type = "2"; },
    entries => { entries[0].name = "../outside"; },
    entries => { entries.push(entries[0]); },
    entries => { entries.push({ name: "src/injected.erl", bytes: Buffer.from("extra") }); },
  ]) { const f = fixture(t), entries = f.entries(); mutate(entries); f.saveArchive(entries); assert.throws(() => readPackage(f.args)); }
});

test("reject missing and changed payloads and self-consistently substituted maintained inputs", t => {
  const missing = fixture(t); missing.saveArchive(missing.entries().slice(1)); assert.throws(() => readPackage(missing.args), /inventory differs/);
  const changed = fixture(t), entries = changed.entries(); entries[0].bytes = Buffer.from("changed"); changed.saveArchive(entries); assert.throws(() => readPackage(changed.args), /payload digest differs/);
  for (const name of ["rebar.config", "src/acyclic_sdk_transport.app.src", "authority/rust-authority.json", "authority/actors/v1/actors.proto.bin"]) {
    const f = fixture(t); f.payload.set(name, Buffer.from("substituted")); f.receipt.output_sha256[name] = sha256(f.payload.get(name)); f.saveReceipt(); f.saveArchive();
    assert.throws(() => readPackage(f.args), /metadata differs|manifest differs|Rust input differs/);
  }
});

test("enforce caller digest, compressed archive and receipt size bounds", t => {
  const f = fixture(t); f.args.sha256 = "0".repeat(64); assert.throws(() => readPackage(f.args), /archive digest differs/);
  truncateSync(f.args.package, 16 * 1024 * 1024 + 1); assert.throws(() => readPackage(f.args), /size bound/);
  const large = fixture(t); truncateSync(large.args.receipt, 1024 * 1024 + 1); assert.throws(() => readPackage(large.args), /receipt exceeds size bound/);
});
