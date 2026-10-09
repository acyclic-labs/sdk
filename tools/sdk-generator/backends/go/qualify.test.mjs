import assert from "node:assert/strict";
import { mkdtempSync, readFileSync, rmSync, writeFileSync, existsSync, mkdirSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import test from "node:test";
import { gzipSync } from "node:zlib";
import { sha256 } from "../../../../scripts/archive-utils.mjs";
import { qualify, validatePayload, verifyArchive } from "./qualify.mjs";

const entry = (path, body = "module fixture\n", type = "0") => ({ path, body: Buffer.from(body), type });
const receiptFor = entries => ({ outputs: entries.map(item => item.path),
  output_sha256: Object.fromEntries(entries.map(item => [item.path, sha256(item.body)])) });

// Minimal regular-file TAR fixture for the existing shared archive reader.
function archiveBytes(entries) {
  const blocks = [];
  for (const item of entries) {
    const header = Buffer.alloc(512);
    header.write(item.path, 0, 100);
    header.write("0000644\0", 100);
    header.write("0000000\0", 108);
    header.write("0000000\0", 116);
    header.write(item.body.length.toString(8).padStart(11, "0") + "\0", 124);
    header.write("00000000000\0", 136);
    header.fill(32, 148, 156);
    header.write(item.type, 156);
    header.write("ustar\0", 257);
    const checksum = header.reduce((sum, byte) => sum + byte, 0);
    header.write(checksum.toString(8).padStart(6, "0") + "\0 ", 148);
    blocks.push(header, item.body, Buffer.alloc((512 - item.body.length % 512) % 512));
  }
  return gzipSync(Buffer.concat([...blocks, Buffer.alloc(1024)]));
}

test("archive admission rejects traversal, links, aliases and digest drift", () => {
  for (const [item, reason] of [
    [entry("../outside"), /unsafe/], [entry("nested//go.mod"), /unsafe/],
    [entry("go.mod", "../outside", "2"), /unsafe/], [entry("NUL.txt"), /Windows/],
  ]) assert.throws(() => validatePayload([item], receiptFor([item]), "win32"), reason);
  const aliases = [entry("go.mod"), entry("GO.MOD")];
  assert.throws(() => validatePayload(aliases, receiptFor(aliases), "win32"), /collide/);
  const files = [entry("go.mod")];
  const receipt = receiptFor(files);
  receipt.output_sha256["go.mod"] = "0".repeat(64);
  assert.throws(() => validatePayload(files, receipt), /payload digest mismatch/);
});

test("archive identity is checked before payload qualification", t => {
  const root = mkdtempSync(join(tmpdir(), "go-archive-admission-"));
  t.after(() => rmSync(root, { recursive: true }));
  const path = join(root, "package.tar.gz");
  const files = [entry("go.mod")];
  writeFileSync(path, archiveBytes(files));
  assert.throws(() => verifyArchive(path, "0".repeat(64), receiptFor(files)), /archive digest mismatch/);
});

function fixture(t) {
  const root = mkdtempSync(join(tmpdir(), "go-installed-qualification-"));
  t.after(() => rmSync(root, { recursive: true }));
  const authority = join(root, "authority");
  mkdirSync(authority);
  writeFileSync(join(authority, "shared.bin"), "shared descriptor fixture");
  const sources = ["actors/v1/actors.proto", "workers/v1/workers.proto", "stream/v2/stream.proto"];
  const families = sources.map((source, index) => {
    const name = `source-${index}.proto`;
    // Source paths mirror the canonical names; descriptors deliberately do not.
    const path = join(authority, source);
    mkdirSync(join(authority, source.slice(0, source.lastIndexOf("/"))), { recursive: true });
    writeFileSync(path, name);
    return { source, source_sha256: sha256(Buffer.from(name)), descriptor: "shared.bin",
      descriptor_sha256: sha256(Buffer.from("shared descriptor fixture")) };
  });
  mkdirSync(join(authority, "validation", "v1"), { recursive: true });
  writeFileSync(join(authority, "validation", "v1", "options.proto"), "attested import only");
  families.push({ source: "validation/v1/options.proto", source_sha256: sha256(Buffer.from("attested import only")) });
  const manifest = Buffer.from(JSON.stringify({ schema: "acyclic.sdk.rust-authority.v1", authority: "rust",
    source_revision: "test-only", families }));
  writeFileSync(join(authority, "rust-authority.json"), manifest);
  const files = [entry("go.mod"), entry("go.sum", "")];
  const receipt = { ...receiptFor(files), schema: "acyclic.sdk.go-producer-receipt.v1", target: "go",
    authority: "rust", source_revision: "test-only", authority_manifest_sha256: sha256(manifest), go_version: "go1.27.2" };
  const receiptPath = join(root, "receipt.json");
  writeFileSync(receiptPath, JSON.stringify(receipt));
  const archive = join(root, "package.tar.gz");
  writeFileSync(archive, archiveBytes(files));
  return { root, args: { package: archive, sha256: sha256(readFileSync(archive)), receipt: receiptPath,
    authority, go: process.execPath, output: join(root, "qualification") } };
}

function commands(calls, failure = "") {
  return (go, args, options) => {
    calls.push({ go, args, options });
    if (args[0] === "version") return { status: 0, stdout: "go version go1.27.2 test/test\n", stderr: "" };
    if (args.includes("-tags=negative")) return { status: 1, stdout: failure === "negative" ? "missing dependency"
      : "cannot use string as []byte\ncannot use uint64 as *uint64\ncannot use string as []byte\n", stderr: "" };
    if (failure === "positive" && args.includes("test")) return { status: 1, stdout: "positive control failed", stderr: "" };
    return { status: 0, stdout: "", stderr: "" };
  };
}

test("successful workflow uses verified shared descriptor paths and isolated workspace", t => {
  const { args } = fixture(t);
  const calls = [];
  const result = qualify(args, commands(calls));
  assert.equal(calls.length, 4);
  assert.deepEqual(calls.map(call => call.args.includes("test")), [false, false, true, true]);
  for (const call of calls) {
    assert.equal(call.options.env.GOWORK, "off");
    assert.equal(call.options.env.GOFLAGS, "");
  }
  const descriptors = JSON.parse(calls.at(-1).options.env.SDK_DESCRIPTOR_FILES);
  assert.equal(new Set(Object.values(descriptors)).size, 1);
  for (const path of Object.values(descriptors)) assert.equal(readFileSync(path, "utf8"), "shared descriptor fixture");
  assert.equal(readFileSync(join(args.output, "authority/validation/v1/options.proto"), "utf8"), "attested import only");
  assert.equal(result.negative_type_controls_rejected, 3);
  assert.equal(result.rust_backed_rpc_qualified, false);
  assert.equal(result.embedded_runtime_qualified, false);
  assert.deepEqual(JSON.parse(readFileSync(join(args.output, "qualification.json"), "utf8")), result);
});

test("descriptor-less imports are digest-checked and tested families still require descriptors", t => {
  const { args } = fixture(t);
  const manifestPath = join(args.authority, "rust-authority.json");
  const manifest = JSON.parse(readFileSync(manifestPath));
  delete manifest.families[0].descriptor;
  delete manifest.families[0].descriptor_sha256;
  const bytes = Buffer.from(JSON.stringify(manifest));
  writeFileSync(manifestPath, bytes);
  const receipt = JSON.parse(readFileSync(args.receipt));
  receipt.authority_manifest_sha256 = sha256(bytes);
  writeFileSync(args.receipt, JSON.stringify(receipt));
  const calls = [];
  assert.throws(() => qualify(args, commands(calls)), /tested family lacks an attested descriptor/);
  assert.equal(calls.length, 0);
  assert.equal(existsSync(args.output), false);
  const other = fixture(t);
  writeFileSync(join(other.args.authority, "validation/v1/options.proto"), "drift");
  assert.throws(() => qualify(other.args, commands(calls)), /authority input digest mismatch/);
  assert.equal(calls.length, 0);
  assert.equal(existsSync(other.args.output), false);
});

for (const failure of ["positive", "negative"]) test(`${failure} failure cannot produce qualification`, t => {
  const { args } = fixture(t);
  assert.throws(() => qualify(args, commands([], failure)), failure === "positive" ? /positive failed/ : /unrelated reason/);
  assert.equal(existsSync(join(args.output, "qualification.json")), false);
});
