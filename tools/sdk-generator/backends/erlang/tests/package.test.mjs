import assert from "node:assert/strict";
import { truncateSync } from "node:fs";
import test from "node:test";
import { readPackage } from "../src/package.mjs";
import { fixture } from "./fixtures/package.mjs";
import { registerArchiveTests } from "../../../shared/tests/package-admission.mjs";

registerArchiveTests("Erlang", readPackage, fixture, {
  extraMember: "src/injected.erl", metadata: ["rebar.config", "src/acyclic_sdk_transport.app.src", "authority/rust-authority.json", "authority/actors/v1/actors.proto.bin"],
  metadataError: /metadata differs|manifest differs|Rust input differs/,
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

test("receipt size bound is enforced", t => {
  const f = fixture(t);
  truncateSync(f.args.receipt, 1024 * 1024 + 1);
  assert.throws(() => readPackage(f.args), /receipt exceeds size bound/);
});
