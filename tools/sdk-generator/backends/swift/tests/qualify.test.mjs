import assert from "node:assert/strict";
import { createHash } from "node:crypto";
import { existsSync, mkdirSync, readFileSync, writeFileSync } from "node:fs";
import { basename, dirname, join } from "node:path";
import test from "node:test";
import { qualify } from "../src/qualify.mjs";
import { sha256 } from "../../../shared/authority.mjs";
import { fixture } from "./fixtures/package.mjs";

function installedFixture(t) {
  const f = fixture(t), root = dirname(f.args.package), runtime = join(root, "runtime"), dependencies = join(root, "dependencies");
  mkdirSync(join(runtime, "usr/bin"), { recursive: true }); mkdirSync(dependencies);
  for (const name of ["swift", "swiftc"]) writeFileSync(join(runtime, "usr/bin", name), name);
  const inventory = { archive_sha256: "b".repeat(64), swift_version: "Swift fixture", files: { "usr/bin/swift": sha256("swift"), "usr/bin/swiftc": sha256("swiftc") }, links: {} };
  const inventoryBytes = Buffer.from(JSON.stringify(inventory)), inventoryPath = join(root, "inventory.json"); writeFileSync(inventoryPath, inventoryBytes);
  const git = join(root, "git"); writeFileSync(git, "git fixture");
  const dependencyPins = ["grpc-swift-2", "grpc-swift-protobuf", "swift-protobuf", "swift-collections"].map((name, index) => ({ name, version: "1.0.0", revision: String(index+1).repeat(40) }));
  const packageBytes = new Map(dependencyPins.map(pin => [pin.name, Buffer.from("package " + pin.name)]));
  for (const pin of dependencyPins) { mkdirSync(join(dependencies, pin.name)); writeFileSync(join(dependencies, pin.name, "Package.swift"), packageBytes.get(pin.name)); }
  const toolchain = { sdk_inventory_sha256: sha256(inventoryBytes), sdk_archive_sha256: inventory.archive_sha256, sdk_files: 2, sdk_links: 0,
    git: { "linux-x64": { sha256: sha256("git fixture"), version: "git fixture" } }, dependencies: dependencyPins };
  const args = { ...f.args, "swift-home": runtime, "sdk-inventory": inventoryPath, dependencies, git, output: join(root, "qualification") };
  const state = {}, calls = [];
  function write(path, bytes) { mkdirSync(dirname(path), { recursive: true }); writeFileSync(path, bytes); }
  const command = (exe, argv, options) => {
    calls.push({ exe, argv });
    assert.equal(options.env.GIT_CONFIG_VALUE_1, "never"); assert.equal(options.env.GIT_CONFIG_VALUE_2, "never");
    assert.equal(options.env.GIT_NO_REPLACE_OBJECTS, "1");
    if (argv[0] === "--version") return { status: 0, stdout: exe === git ? "git fixture\n" : "Swift fixture\n" };
    if (exe === git && argv[0] === "-C") {
      const pin = dependencyPins.find(pin => pin.name === basename(argv[1])); assert.ok(pin);
      if (argv[2] === "rev-parse") return { status: 0, stdout: pin.revision + "\n" };
      assert.equal(argv[2], "ls-tree");
      const bytes = packageBytes.get(pin.name), blob = createHash("sha1").update(`blob ${bytes.length}\0`).update(bytes).digest("hex");
      return { status: 0, stdout: `100644 blob ${blob}\tPackage.swift\0` };
    }
    if (exe === git && argv[0] === "clone") { const destination = argv.at(-1); write(join(destination,"Package.swift"), packageBytes.get(basename(destination))); return { status: 0, stdout: "" }; }
    if (argv[0] === "package") { write(join(args.output,"consumer/.swiftpm/configuration/mirrors.json"),"fixture mirrors"); return { status: 0, stdout: "" }; }
    if (argv[0] === "build") {
      write(join(args.output, "consumer/Package.resolved"), JSON.stringify({ pins: dependencyPins.map(pin => ({ identity: pin.name, state: { revision: pin.revision, version: pin.version } })) }));
      for (const pin of dependencyPins) write(join(args.output, "build/checkouts", pin.name, "Package.swift"), packageBytes.get(pin.name));
      write(join(args.output, "build/out/Products/Release-linux-x86_64/Consumer"), "compiled fixture");
      if (state.dependencyDrift) write(join(args.output,"build/checkouts/grpc-swift-2/Package.swift"),"changed source");
      if (state.extraCompiledSource) write(join(args.output,"build/checkouts/grpc-swift-2/Extra.swift"),"unlisted source");
      return { status: 0, stdout: "Build complete!\n" };
    }
    if (basename(exe) === "Consumer") {
      if (state.payloadDrift) write(join(args.output,"sdk/LICENSE"),"changed payload");
      return { status: 0, stdout: state.missingMarker ? "PASS only\n" :
        "PASS Swift bytes, unsigned bounds, optional zero, populated oneofs, gRPC codecs and bounded RPC metadata\nPASS Swift 25 generated client/server calls, Rust descriptor message/field/path/stream checks, populated native payloads\n" };
    }
    assert.equal(basename(exe), "swiftc");
    const name = basename(argv.at(-1));
    if (name === "main.swift") return { status: 0, stdout: "" };
    if (state.acceptedNegative) return { status: 0, stdout: "" };
    if (state.unrelatedNegative) return { status: 1, stderr: "no such module AcyclicTransport" };
    return { status: 1, stderr: `${name}:3: error: cannot assign value of type 'String' to type '${name.includes("Integer") ? "UInt64" : "Data"}'\n` };
  };
  return { ...f, args, toolchain, command, state, calls, runtime };
}

test("offline qualifier staging reaches native markers, exact dependencies and three specific negative diagnostics", t => {
  const f = installedFixture(t), result = qualify(f.args, f);
  assert.equal(result.native_in_process_rpc_methods, 25);
  assert.equal(Object.keys(result.dependencies).length, 4);
  assert.equal(result.negative_type_controls.length, 3);
  assert.equal(result.network_transport_qualified, false); assert.equal(result.rust_backed_rpc_qualified, false);
  assert.ok(existsSync(join(f.args.output, "qualification.json")));
  assert.equal(result.archive_sha256, f.args.sha256);
  assert.ok(f.calls.some(call => call.argv.includes("--no-hardlinks")));
});

test("SDK, Git and dependency source drift cannot create qualification output", t => {
  for (const mutate of [
    f => writeFileSync(join(f.runtime,"usr/bin/swiftc"), "drifted compiler"),
    f => writeFileSync(f.args.git, "drifted git"),
    f => writeFileSync(join(f.args.dependencies,"grpc-swift-2/Package.swift"),"drifted source"),
  ]) { const f = installedFixture(t); mutate(f); assert.throws(() => qualify(f.args,f)); assert.equal(existsSync(f.args.output),false); }
});

test("partial native completion, accepted negatives and unrelated diagnostic failures cannot qualify", t => {
  for (const key of ["missingMarker","acceptedNegative","unrelatedNegative"]) {
    const f = installedFixture(t); f.state[key] = true;
    assert.throws(() => qualify(f.args,f)); assert.equal(existsSync(join(f.args.output,"qualification.json")),false);
  }
});

test("compiled dependency and installed payload drift cannot qualify", t => {
  for (const key of ["dependencyDrift","extraCompiledSource","payloadDrift"]) {
    const f = installedFixture(t); f.state[key] = true;
    assert.throws(() => qualify(f.args,f)); assert.equal(existsSync(join(f.args.output,"qualification.json")),false);
  }
});

test("untracked preparation metadata in clone inputs is excluded from compiled mirrors", t => {
  const f = installedFixture(t);
  writeFileSync(join(f.args.dependencies,"grpc-swift-2/untracked.swift"),"not a Git input");
  const result = qualify(f.args,f);
  assert.equal(Object.keys(result.dependencies["grpc-swift-2"].files_sha256).length,1);
  assert.equal(existsSync(join(f.args.output,"feed/grpc-swift-2/untracked.swift")),false);
});

test("mutated or extra staged consumer inputs cannot write qualification success", t => {
  for (const name of ["consumer/Package.swift", "consumer/Sources/Consumer/main.swift", "negative/InvalidActorBytes.swift",
    "negative/InvalidWorkerBytes.swift", "negative/InvalidOptionalInteger.swift", "consumer/.swiftpm/configuration/mirrors.json",
    "consumer/Package.resolved", "consumer/Sources/Consumer/Extra.swift", "consumer/Package@swift-6.4.swift", "negative/Extra.swift"]) {
    const f = installedFixture(t);
    const command = (exe, argv, options) => {
      const result = f.command(exe, argv, options);
      if (basename(exe) === "Consumer") { const file = join(f.args.output,name); mkdirSync(dirname(file),{recursive:true}); writeFileSync(file,"changed staged source"); }
      return result;
    };
    assert.throws(() => qualify(f.args,{...f,command}), /staged consumer|dependency lock changed/);
    assert.equal(existsSync(join(f.args.output,"qualification.json")),false);
  }
});

test("existing and overlapping output paths remain intact", t => {
  const f = installedFixture(t); mkdirSync(f.args.output); writeFileSync(join(f.args.output,"keep"),"caller bytes");
  assert.throws(() => qualify(f.args,f), /absent/); assert.equal(readFileSync(join(f.args.output,"keep"),"utf8"),"caller bytes");
  f.args.output = join(f.args.dependencies,"new-output"); assert.throws(() => qualify(f.args,f),/overlaps/);
  assert.equal(f.calls.length,0);
});
