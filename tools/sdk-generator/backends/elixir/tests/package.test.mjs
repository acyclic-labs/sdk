import assert from "node:assert/strict";
import { truncateSync } from "node:fs";
import test from "node:test";
import { readPackage } from "../src/package.mjs";
import { fixture } from "./fixtures/package.mjs";
import { registerArchiveTests } from "../../../shared/tests/package-admission.mjs";

registerArchiveTests("Elixir", readPackage, fixture, {
  extraMember: "extra.ex", metadata: ["mix.exs", "authority/rust-authority.json", "authority/actors/v1/actors.proto"],
  metadataError: /metadata differs|Rust input differs/,
});
test("authority, source, template and runtime admission drift reject", t => {
  for (const mutate of [
    f => { f.receipt.source_revision = "b".repeat(40); },
    f => { f.receipt.input_sha256 = {}; },
    f => { f.receipt.generator_sha256 = "0".repeat(64); },
    f => { f.receipt.source_sha256["runtime.mjs"] = "0".repeat(64); },
    f => { f.receipt.source_sha256.extra = "0".repeat(64); },
    f => { f.receipt.toolchain_sha256 = "0".repeat(64); },
    f => { f.receipt.tool_sha256.elixir_plugin = "0".repeat(64); },
    f => { f.receipt.runtime_inventory_sha256 = "0".repeat(64); },
    f => { f.receipt.template_sha256["mix.exs"] = "0".repeat(64); },
  ]) { const f = fixture(t); mutate(f); f.saveReceipt(); assert.throws(() => readPackage(f.args)); }
});

test("unsafe, duplicate, extra and incomplete receipt inventories reject", t => {
  for (const mutate of [
    f => f.receipt.outputs.push("../outside.ex"),
    f => f.receipt.outputs.push("LICENSE"),
    f => f.receipt.outputs.push("license"),
    f => { f.receipt.output_sha256.extra = "0".repeat(64); },
    f => { f.receipt.outputs = f.receipt.outputs.filter(name => !name.endsWith("actors.pb.ex")); },
    f => { f.receipt.outputs = f.receipt.outputs.filter(name => !name.endsWith("actors.proto.bin")); },
  ]) { const f = fixture(t); mutate(f); f.saveReceipt(); assert.throws(() => readPackage(f.args)); }
});

test("receipt size bound is enforced", t => {
  const f = fixture(t);
  truncateSync(f.args.receipt, 1024 * 1024 + 1);
  assert.throws(() => readPackage(f.args), /receipt exceeds size bound/);
});
