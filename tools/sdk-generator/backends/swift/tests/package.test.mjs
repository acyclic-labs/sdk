import assert from "node:assert/strict";
import test from "node:test";
import { readPackage, targets } from "../src/package.mjs";
import { fixture } from "./fixtures/package.mjs";
import { registerArchiveTests } from "../../../shared/tests/package-admission.mjs";

registerArchiveTests("Swift", readPackage, fixture, {
  extraMember: "extra.swift", metadata: ["Package.swift"],
  metadataError: /metadata differs/,
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
