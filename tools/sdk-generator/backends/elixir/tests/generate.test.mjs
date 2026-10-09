import assert from "node:assert/strict";
import { existsSync, mkdirSync, mkdtempSync, readFileSync, readdirSync, rmSync, writeFileSync } from "node:fs";
import { tmpdir } from "node:os";
import { delimiter, dirname, join } from "node:path";
import test from "node:test";
import { sha256 } from "../../../shared/authority.mjs";
import { generate } from "../src/generate.mjs";

function fixture(t) {
  const root = mkdtempSync(join(tmpdir(), "elixir-generator-test-"));
  t.after(() => rmSync(root, { recursive: true }));
  const source = join(root, "source"), authority = join(root, "authority"), runtime = join(root, "runtime");
  for (const directory of [source, authority, runtime]) mkdirSync(directory);
  for (const name of ["LICENSE", "NOTICE"]) writeFileSync(join(source, name), name);
  for (const name of ["a.proto", "b.proto"]) writeFileSync(join(authority, name), name);
  writeFileSync(join(authority, "shared.bin"), "Rust descriptor bytes");
  const manifest = { schema: "acyclic.sdk.rust-authority.v1", authority: "rust", source_revision: "a".repeat(40),
    families: ["b.proto", "a.proto"].map(source => ({ source, source_sha256: sha256(source), descriptor: "shared.bin", descriptor_sha256: sha256("Rust descriptor bytes") })) };
  writeFileSync(join(authority, "rust-authority.json"), JSON.stringify(manifest));
  const runtimeFiles = { "OTP/bin/erl": "Erlang fixture", "Elixir/bin/elixir": "Elixir fixture" };
  for (const [name, bytes] of Object.entries(runtimeFiles)) { const target = join(runtime, name); mkdirSync(dirname(target), { recursive: true }); writeFileSync(target, bytes); }
  const runtimeIndex = { otp_version: "29.1.1", elixir_version: "1.20.4", otp_archive_sha256: "1".repeat(64), elixir_archive_sha256: "2".repeat(64),
    files_sha256: Object.fromEntries(Object.entries(runtimeFiles).map(([name, bytes]) => [name, sha256(bytes)])), links: {} };
  const runtimeInventory = join(root, "runtime.json"); writeFileSync(runtimeInventory, JSON.stringify(runtimeIndex));
  const protoc = join(root, "protoc"), plugin = join(root, "plugin"); writeFileSync(protoc, "compiler fixture"); writeFileSync(plugin, "plugin fixture");
  const host = `${process.platform}-${process.arch}`;
  const toolchain = { protoc_version: "libprotoc 28.3", protobuf_version: "0.17.1", protoc: { [host]: { sha256: sha256("compiler fixture") } },
    elixir_plugin: { [host]: { sha256: sha256("plugin fixture") } }, runtime: { [host]: { inventory_sha256: sha256(readFileSync(runtimeInventory)),
      otp_version: "29.1.1", otp_release: "29", elixir_version: "1.20.4", roots: ["OTP", "Elixir"], files: 2, links: 0,
      otp_archive_sha256: runtimeIndex.otp_archive_sha256, elixir_archive_sha256: runtimeIndex.elixir_archive_sha256 } } };
  const args = { "source-root": source, authority, protoc, "elixir-plugin": plugin, "runtime-root": runtime, "runtime-inventory": runtimeInventory, output: join(root, "package") };
  const calls = [];
  const command = (exe, argv, options) => {
    calls.push({ exe, argv, options });
    if (argv[0] === "--version") return { status: 0, stdout: exe === protoc ? "libprotoc 28.3\n" : "0.17.1\n", stderr: "" };
    if (exe === join(runtime, "OTP/bin/erl")) return { status: 0, stdout: "29\n", stderr: "" };
    const flag = argv.find(value => value.startsWith("--elixir_out="));
    const output = flag.slice("--elixir_out=plugins=grpc,gen_descriptors=true,gen_proto_source=true:".length);
    for (const name of ["a.pb.ex", "b.pb.ex"]) { mkdirSync(join(output, "acyclic"), { recursive: true }); writeFileSync(join(output, "acyclic", name), name); }
    return { status: 0, stdout: "", stderr: "" };
  };
  return { root, source, authority, runtime, runtimeInventory, runtimeIndex, manifest, protoc, plugin, args, toolchain, calls, command };
}

test("descriptor-only generation stages the exact Mix package and isolated runtime", t => {
  const inherited = ["ESCRIPT_EMULATOR", "ERL_LIBS", "ERL_AFLAGS", "ROOTDIR", "BINDIR", "EMU"];
  const previous = new Map(inherited.map(name => [name, process.env[name]]));
  for (const name of inherited) process.env[name] = "/unadmitted/runtime";
  t.after(() => { for (const [name, value] of previous) { if (value === undefined) delete process.env[name]; else process.env[name] = value; } });
  const f = fixture(t), receipt = generate(f.args, f);
  assert.equal(receipt.target, "elixir"); assert.equal(receipt.source_revision, f.manifest.source_revision);
  assert.equal(receipt.outputs.length, 10);
  for (const [name, digest] of Object.entries(receipt.output_sha256)) assert.equal(sha256(readFileSync(join(f.args.output, name))), digest);
  assert.equal(readFileSync(join(f.args.output, "authority/a.proto"), "utf8"), "a.proto");
  const generation = f.calls.find(call => call.argv.some(value => value.startsWith("--elixir_out=")));
  const descriptorPaths = generation.argv[0].slice("--descriptor_set_in=".length).split(delimiter);
  assert.equal(descriptorPaths.length, 1);
  assert.ok(generation.argv.includes(`--plugin=protoc-gen-elixir=${f.plugin}`));
  assert.deepEqual(generation.argv.slice(-2), ["a.proto", "b.proto"]);
  assert.ok(!generation.argv.some(value => value.startsWith("-I")));
  assert.equal(generation.options.env.ERL_FLAGS, "+S 1:1"); assert.equal(generation.options.env.ELIXIR_ERL_OPTIONS, "+fnu");
  assert.ok(generation.options.env.HOME.startsWith(generation.options.cwd));
  for (const name of inherited) assert.equal(generation.options.env[name], undefined);
  assert.equal(existsSync(generation.options.cwd), false);
});

test("authority and executable drift reject before any native command or output", t => {
  for (const target of ["authority", "compiler", "plugin"]) {
    const f = fixture(t);
    writeFileSync(target === "authority" ? join(f.authority, "a.proto") : target === "compiler" ? f.protoc : f.plugin, "drift");
    assert.throws(() => generate(f.args, f), /differs|digest mismatch/); assert.equal(f.calls.length, 0); assert.equal(existsSync(f.args.output), false);
  }
});

test("changed, missing and additional runtime files reject before generation", t => {
  for (const mutation of ["changed", "missing", "extra", "index"]) {
    const f = fixture(t), file = join(f.runtime, "OTP/bin/erl");
    if (mutation === "changed") writeFileSync(file, "drift");
    if (mutation === "missing") rmSync(file);
    if (mutation === "extra") writeFileSync(join(f.runtime, "OTP/extra.beam"), "extra");
    if (mutation === "index") writeFileSync(f.runtimeInventory, "{}");
    assert.throws(() => generate(f.args, f), /runtime.*differs/); assert.equal(f.calls.length, 0); assert.equal(existsSync(f.args.output), false);
  }
});

test("wrong native versions and tool failures preserve absent package output", t => {
  for (const failure of ["compiler", "plugin", "otp", "status"]) {
    const f = fixture(t);
    const command = (exe, argv, options) => {
      if (failure === "status") return { status: 1, stdout: "", stderr: "native fault" };
      if ((failure === "compiler" && exe === f.protoc) || (failure === "plugin" && exe === f.plugin) || (failure === "otp" && argv[0] === "-noshell")) return { status: 0, stdout: "wrong", stderr: "" };
      return f.command(exe, argv, options);
    };
    assert.throws(() => generate(f.args, { ...f, command }), /version differs|release differs|command failed/);
    assert.equal(existsSync(f.args.output), false);
  }
});

test("incomplete and unexpected generated bindings cannot produce a package receipt", t => {
  for (const mutation of ["missing", "extra"]) {
    const f = fixture(t);
    const command = (exe, argv, options) => {
      const result = f.command(exe, argv, options), flag = argv.find(value => value.startsWith("--elixir_out="));
      if (flag) {
        const output = flag.slice("--elixir_out=plugins=grpc,gen_descriptors=true,gen_proto_source=true:".length);
        if (mutation === "missing") rmSync(join(output, "acyclic/b.pb.ex"));
        else writeFileSync(join(output, "extra.ex"), "unexpected code");
      }
      return result;
    };
    assert.throws(() => generate(f.args, { ...f, command }), /inventory/);
    assert.equal(existsSync(join(f.args.output, "generation-receipt.json")), false);
  }
});

test("tool mutation during native generation prevents a success receipt", t => {
  const f = fixture(t);
  const command = (exe, argv, options) => { const result = f.command(exe, argv, options); if (argv.some(value => value.startsWith("--elixir_out="))) writeFileSync(f.plugin, "changed during generation"); return result; };
  assert.throws(() => generate(f.args, { ...f, command }), /inputs changed/);
  assert.equal(existsSync(join(f.args.output, "generation-receipt.json")), false);
});

test("existing and overlapping output paths remain intact", t => {
  const f = fixture(t); mkdirSync(f.args.output); writeFileSync(join(f.args.output, "keep"), "owned");
  assert.throws(() => generate(f.args, f), /absent/); assert.deepEqual(readdirSync(f.args.output), ["keep"]);
  assert.throws(() => generate({ ...f.args, output: join(f.runtime, "new") }, f), /overlaps/);
  assert.equal(f.calls.length, 0);
});
