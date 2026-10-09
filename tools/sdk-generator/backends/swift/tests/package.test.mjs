import assert from "node:assert/strict";
import { mkdirSync, mkdtempSync, readFileSync, rmSync, truncateSync, writeFileSync } from "node:fs";
import { tmpdir } from "node:os";
import { dirname, join } from "node:path";
import { fileURLToPath } from "node:url";
import { gzipSync } from "node:zlib";
import test from "node:test";
import { pins, readPackage, targets } from "../src/package.mjs";
import { sha256 } from "../../../shared/authority.mjs";

const directory = dirname(fileURLToPath(import.meta.url));
function tar(entries) {
  const blocks = [];
  for (const { name, bytes, type = "0" } of entries) {
    const header = Buffer.alloc(512);
    header.write(name, 0, 100); header.write("0000644\0", 100); header.write("0000000\0", 108); header.write("0000000\0", 116);
    header.write(bytes.length.toString(8).padStart(11, "0") + "\0", 124); header.write("00000000000\0", 136);
    header.fill(32, 148, 156); header.write(type, 156); header.write("ustar\0", 257); header.write("00", 263);
    const checksum = header.reduce((sum, byte) => sum + byte, 0);
    header.write(checksum.toString(8).padStart(6, "0") + "\0 ", 148);
    blocks.push(header, bytes, Buffer.alloc((512 - bytes.length % 512) % 512));
  }
  return Buffer.concat([...blocks, Buffer.alloc(1024)]);
}
function fixture(t) {
  const root = mkdtempSync(join(tmpdir(), "swift-package-test-"));
  t.after(() => rmSync(root, { recursive: true }));
  const authority = join(root, "authority"); mkdirSync(authority);
  const inputHashes = {}, families = [];
  for (const source of targets) {
    const descriptor = source + ".bin";
    for (const name of [source, descriptor]) {
      mkdirSync(dirname(join(authority, name)), { recursive: true }); writeFileSync(join(authority, name), name);
      inputHashes[name] = sha256(name);
    }
    families.push({ source, source_sha256: inputHashes[source], descriptor, descriptor_sha256: inputHashes[descriptor] });
  }
  const manifest = Buffer.from(JSON.stringify({ schema: "acyclic.sdk.rust-authority.v1", authority: "rust", source_revision: "a".repeat(40), families }));
  writeFileSync(join(authority, "rust-authority.json"), manifest);
  const payload = new Map([["LICENSE", Buffer.from("license")], ["NOTICE", Buffer.from("notice")],
    ["Package.swift", readFileSync(join(directory, "../templates/package/Package.swift"))], ["authority/rust-authority.json", manifest]]);
  for (const source of targets) for (const suffix of [".pb.swift", ".grpc.swift"]) payload.set("Sources/AcyclicTransport/" + source.slice(0, -6) + suffix, Buffer.from("staging fixture"));
  const receipt = {
    schema: "acyclic.sdk.swift-producer-receipt.v1", authority: "rust", target: "swift", source_revision: "a".repeat(40),
    authority_manifest_sha256: sha256(manifest), input_sha256: inputHashes,
    generator_sha256: sha256(readFileSync(join(directory, "../src/generate.mjs"))),
    authority_reader_sha256: sha256(readFileSync(join(directory, "../../../shared/authority.mjs"))), toolchain_sha256: sha256(JSON.stringify(pins)),
    protoc_version: pins.protoc_version, swift_plugin_version: pins.swift_plugin_version, grpc_plugin_version: pins.grpc_plugin_version,
    swift_version: pins.swift_runtime["linux-x64"].version, swift_runtime_sha256: pins.swift_runtime["linux-x64"].files,
    tool_sha256: { protoc: pins.protoc["linux-x64"].sha256, swift_plugin: pins.swift_plugin["linux-x64"].sha256, grpc_plugin: pins.grpc_plugin["linux-x64"].sha256 },
    outputs: [...payload.keys()].sort(), output_sha256: Object.fromEntries([...payload].map(([name, bytes]) => [name, sha256(bytes)])),
  };
  const args = { authority, receipt: join(root, "receipt.json"), package: join(root, "package.tar.gz") };
  const saveReceipt = () => writeFileSync(args.receipt, JSON.stringify(receipt));
  const entries = () => [...payload].map(([name, bytes]) => ({ name, bytes }));
  const saveArchive = (members = entries()) => { const bytes = gzipSync(tar(members)); writeFileSync(args.package, bytes); args.sha256 = sha256(bytes); };
  saveReceipt(); saveArchive();
  return { args, receipt, payload, saveReceipt, saveArchive, entries };
}

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
