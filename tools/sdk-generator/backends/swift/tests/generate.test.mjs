import { generatorFixture } from "../../../shared/tests/generation.mjs";
import assert from "node:assert/strict";
import { createHash } from "node:crypto";
import { existsSync, mkdirSync, readFileSync, readdirSync, writeFileSync } from "node:fs";
import { delimiter, join } from "node:path";
import test from "node:test";
import { generate } from "../src/generate.mjs";

const hash = bytes => createHash("sha256").update(bytes).digest("hex");
function fixture(t) {
  const { root, source, authority, manifest, save, protoc, plugin, toolchain } = generatorFixture(t, "swift", "swift_plugin");
  const grpcPlugin = join(root, "grpc-plugin"); writeFileSync(grpcPlugin, "grpc fixture");
  const swiftHome = join(root, "swift-home"); mkdirSync(join(swiftHome, "usr/bin"), { recursive: true });
  writeFileSync(join(swiftHome, "usr/bin/swift"), "swift fixture");
  toolchain.grpc_plugin_version = "fixture";
  toolchain.grpc_plugin = { [`${process.platform}-${process.arch}`]: { sha256: hash("grpc fixture"), url: "fixture" } };
  toolchain.swift_runtime = { [`${process.platform}-${process.arch}`]: { version: "Swift fixture", files: { "usr/bin/swift": hash("swift fixture") } } };
  const args = { "source-root": source, authority, protoc, "swift-plugin": plugin, "grpc-plugin": grpcPlugin, "swift-home": swiftHome, output: join(root, "new-package") };
  const calls = [];
  let snapshot;
  const command = (executable, argv, options) => {
    calls.push(argv);
    if (executable === join(swiftHome, "usr/bin/swift")) return { status: 0, stdout: "Swift fixture" };
    assert.equal(executable, protoc);
    if (argv[0] === "--version") return { status: 0, stdout: "libprotoc 28.3\n" };
    snapshot = options.cwd;
    const descriptors = argv[0].slice("--descriptor_set_in=".length).split(delimiter);
    assert.equal(descriptors.length, 1);
    assert.equal(readFileSync(descriptors[0], "utf8"), "attested descriptors");
    assert.equal(readdirSync(snapshot).filter(name => name.endsWith(".proto")).length, 0);
    const output = argv.find(arg => arg.startsWith("--swift_out=")).slice("--swift_out=".length);
    for (const suffix of [".pb.swift", ".grpc.swift"]) writeFileSync(join(output, argv.at(-1).slice(0,-6)+suffix), `generated ${argv.at(-1)} ${suffix}`);
    return { status: 0, stdout: "", stderr: "" };
  };
  return { root, args, manifest, save, toolchain, command, calls, snapshot: () => snapshot };
}

test("each family uses only verified descriptor snapshots and has inventoried output", t => {
  const f = fixture(t);
  const receipt = generate(f.args, { command: f.command, toolchain: f.toolchain });
  assert.deepEqual(f.calls.slice(2).map(argv => argv.at(-1)), ["a.proto", "b.proto", "import.proto"]);
  assert.equal(existsSync(f.snapshot()), false);
  assert.equal(receipt.source_revision, f.manifest.source_revision);
  assert.equal(receipt.authority_manifest_sha256, hash(readFileSync(join(f.args.authority, "rust-authority.json"))));
  for (const name of receipt.outputs) assert.equal(receipt.output_sha256[name], hash(readFileSync(join(f.args.output, name))));
  assert.deepEqual(receipt.outputs.filter(name => name.startsWith("Sources/")), [
    "Sources/AcyclicTransport/a.grpc.swift", "Sources/AcyclicTransport/a.pb.swift",
    "Sources/AcyclicTransport/b.grpc.swift", "Sources/AcyclicTransport/b.pb.swift",
    "Sources/AcyclicTransport/import.grpc.swift", "Sources/AcyclicTransport/import.pb.swift",
  ]);
  assert.ok(receipt.outputs.includes("Package.swift"));
  assert.ok(receipt.outputs.includes("LICENSE"));
  assert.deepEqual(JSON.parse(readFileSync(join(f.args.output, "generation-receipt.json"))), receipt);
});

test("unsafe, duplicate, missing and drifting authority inputs fail before tool execution", t => {
  for (const mutate of [
    f => { f.manifest.families[0].source = "../outside.proto"; },
    f => { f.manifest.families[0] = f.manifest.families[1]; },
    f => { f.manifest.families[0].descriptor = "missing.bin"; },
    f => { f.manifest.families[0].descriptor_sha256 = "0".repeat(64); },
    f => { f.manifest.families.at(-1).source_sha256 = "0".repeat(64); },
    f => { for (const family of f.manifest.families) delete family.descriptor; },
    f => { f.manifest.source_revision = "mutable-branch"; },
  ]) {
    const f = fixture(t);
    mutate(f);
    f.save();
    assert.throws(() => generate(f.args, { command: f.command, toolchain: f.toolchain }));
    assert.equal(f.calls.length, 0);
    assert.equal(existsSync(f.args.output), false);
  }
});

test("existing and overlapping output paths are preserved", t => {
  const f = fixture(t);
  mkdirSync(f.args.output);
  writeFileSync(join(f.args.output, "keep"), "caller bytes");
  assert.throws(() => generate(f.args, { command: f.command, toolchain: f.toolchain }), /absent/);
  assert.equal(readFileSync(join(f.args.output, "keep"), "utf8"), "caller bytes");
  f.args.output = join(f.args.authority, "new-package");
  assert.throws(() => generate(f.args, { command: f.command, toolchain: f.toolchain }), /overlaps/);
  assert.equal(f.calls.length, 0);
});

test("plugin drift and compiler version mismatch cannot create output", t => {
  const f = fixture(t);
  writeFileSync(f.args["swift-plugin"], "different plugin");
  assert.throws(() => generate(f.args, { command: f.command, toolchain: f.toolchain }), /admitted host pin/);
  assert.equal(existsSync(f.args.output), false);
  writeFileSync(f.args["swift-plugin"], "plugin fixture");
  writeFileSync(f.args.protoc, "compiler drift with same version");
  assert.throws(() => generate(f.args, { command: f.command, toolchain: f.toolchain }), /compiler executable differs/);
  assert.equal(f.calls.length, 0);
  assert.equal(existsSync(f.args.output), false);
  writeFileSync(f.args.protoc, "compiler fixture");
  assert.throws(() => generate(f.args, { command: () => ({ status: 0, stdout: "Swift fixture" }), toolchain: f.toolchain }), /protoc version/);
  assert.equal(existsSync(f.args.output), false);
});

test("failed generation retains partial output but cannot write a receipt", t => {
  const f = fixture(t);
  const command = (exe, argv, options) => argv[0] === "--version" ? f.command(exe, argv, options)
    : { status: 1, stderr: "compiler failure" };
  assert.throws(() => generate(f.args, { command, toolchain: f.toolchain }), /generation failed/);
  assert.equal(existsSync(f.args.output), true);
  assert.equal(existsSync(join(f.args.output, "generation-receipt.json")), false);
});

test("gRPC plugin and Swift runtime drift fail before any command", t => {
  for (const target of ["grpc-plugin", "swift-home"]) {
    const f = fixture(t);
    const path = target === "swift-home" ? join(f.args[target], "usr/bin/swift") : f.args[target];
    writeFileSync(path, "changed binary");
    assert.throws(() => generate(f.args, { command: f.command, toolchain: f.toolchain }),
      target === "swift-home" ? /digest mismatch/ : /grpc-plugin executable differs/);
    assert.equal(f.calls.length, 0);
    assert.equal(existsSync(f.args.output), false);
  }
});

test("Swift runtime version mismatch fails before protoc execution", t => {
  const f = fixture(t);
  assert.throws(() => generate(f.args, { command: () => ({ status: 0, stdout: "wrong Swift" }), toolchain: f.toolchain }), /Swift version differs/);
  assert.equal(existsSync(f.args.output), false);
});

test("mid-generation compiler, plugin and runtime mutations prevent success receipts", t => {
  for (const name of ["protoc", "swift-plugin", "grpc-plugin", "swift-home"]) {
    const f = fixture(t);
    const command = (exe, argv, options) => {
      const result = f.command(exe,argv,options);
      if (argv.some(value => value.startsWith("--swift_out="))) writeFileSync(name === "swift-home" ? join(f.args[name],"usr/bin/swift") : f.args[name],"changed during generation");
      return result;
    };
    assert.throws(() => generate(f.args,{...f,command}), /tool inputs changed|digest mismatch/);
    assert.equal(existsSync(join(f.args.output,"generation-receipt.json")),false);
  }
});

test("missing family bindings and unexpected output cannot produce success", t => {
  for (const collision of [false, true]) {
    const f = fixture(t);
    const command = (exe, argv, options) => {
      if (argv[0] === "--version") return f.command(exe, argv, options);
      if (collision) {
        const output = argv.find(arg => arg.startsWith("--swift_out=")).slice("--swift_out=".length);
        for (const suffix of [".pb.swift", ".grpc.swift"]) writeFileSync(join(output, argv.at(-1).slice(0,-6)+suffix), "fixture");
        writeFileSync(join(output, "unexpected.txt"), "unexpected output");
      }
      return { status: 0, stdout: "", stderr: "" };
    };
    assert.throws(() => generate(f.args, { command, toolchain: f.toolchain }), collision ? /unexpected generated Swift output/ : /lacks complete Swift bindings/);
    assert.equal(existsSync(join(f.args.output, "generation-receipt.json")), false);
  }
});
