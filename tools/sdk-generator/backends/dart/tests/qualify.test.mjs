import assert from "node:assert/strict";
import { existsSync, mkdirSync, mkdtempSync, readFileSync, rmSync, writeFileSync } from "node:fs";
import { tmpdir } from "node:os";
import { dirname, join } from "node:path";
import { pathToFileURL } from "node:url";
import { gzipSync, gunzipSync } from "node:zlib";
import test from "node:test";
import { tarEntries } from "../../../../../scripts/archive-utils.mjs";
import { sha256 } from "../../../shared/authority.mjs";
import { qualify } from "../src/qualify.mjs";

// Small inert archive fixtures; production packaging always uses pinned bsdtar.
function archive(payload) {
  const blocks = [];
  for (const [name, body] of Object.entries(payload)) {
    const bytes = Buffer.from(body), header = Buffer.alloc(512);
    header.write(name); header.write("0000644\0", 100); header.write("0000000\0", 108); header.write("0000000\0", 116);
    header.write(bytes.length.toString(8).padStart(11, "0") + "\0", 124); header.write("00000000000\0", 136);
    header.fill(32, 148, 156); header[156] = 48;
    const checksum = header.reduce((a, b) => a + b, 0); header.write(checksum.toString(8).padStart(6, "0") + "\0 ", 148);
    blocks.push(header, bytes, Buffer.alloc((512 - bytes.length % 512) % 512));
  }
  return gzipSync(Buffer.concat([...blocks, Buffer.alloc(1024)]));
}

function fixture(t, failure = "") {
  const root = mkdtempSync(join(tmpdir(), "dart-qualification-test-")); t.after(() => rmSync(root, { recursive: true }));
  const put = (path, bytes) => { mkdirSync(dirname(path), { recursive: true }); writeFileSync(path, bytes); };
  const authority = join(root, "authority"), packageRoot = join(root, "package"), runtime = join(root, "runtime"), cache = join(root, "prepared-cache"), archiver = join(root, "tar");
  const targets = ["actors/v1/actors.proto", "workers/v1/workers.proto", "stream/v2/stream.proto"];
  const manifest = { schema: "acyclic.sdk.rust-authority.v1", authority: "rust", source_revision: "a".repeat(40), families: targets.map(source => {
    put(join(authority, source), source); return { source, source_sha256: sha256(source), descriptor: "shared.bin", descriptor_sha256: sha256("descriptors") };
  }) };
  put(join(authority, "shared.bin"), "descriptors");
  const manifestBytes = Buffer.from(JSON.stringify(manifest)); put(join(authority, "rust-authority.json"), manifestBytes);
  const payload = { "pubspec.yaml": readFileSync("tools/sdk-generator/backends/dart/templates/package/pubspec.yaml"), LICENSE: "LICENSE", NOTICE: "NOTICE", "authority/rust-authority.json": manifestBytes };
  targets.forEach((source, i) => {
    const family = source.slice(0, -6);
    payload[`lib/${family}.pb.dart`] = `class Thing${i} extends $pb.GeneratedMessage {}`;
    payload[`lib/${family}.pbjson.dart`] = `/// Descriptor for \`Thing${i}\`. Decode as a \`google.protobuf.DescriptorProto\`.\nfinal $typed_data.Uint8List thing${i} = data;`;
    payload[`lib/${family}.pbenum.dart`] = "enums"; payload[`lib/${family}.pbgrpc.dart`] = "grpc";
  });
  for (const [name, bytes] of Object.entries(payload)) put(join(packageRoot, name), bytes);
  const receipt = { schema: "acyclic.sdk.dart-producer-receipt.v1", authority: "rust", target: "dart", source_revision: manifest.source_revision,
    authority_manifest_sha256: sha256(manifestBytes), outputs: Object.keys(payload), output_sha256: Object.fromEntries(Object.entries(payload).map(([name, bytes]) => [name, sha256(bytes)])) };
  put(join(packageRoot, "generation-receipt.json"), JSON.stringify(receipt));
  put(join(runtime, "bin/dart.exe"), "Dart fixture"); put(archiver, "tar fixture");
  const dependency = archive({ "pubspec.yaml": "name: dependency\nversion: 1.0.0\n", "lib/dependency.dart": "dependency" });
  put(join(cache, "dependency-1.0.0.tar.gz"), dependency); put(join(cache, "untrusted-sdk.tar.gz"), "must not reuse");
  const toolchain = { dart_sdk_version: "fixture", dart_sdk_archive: { url: "fixture", sha256: sha256("SDK archive fixture") },
    runtime_files: { "bin/dart.exe": sha256("Dart fixture") }, archiver: { [`${process.platform}-${process.arch}`]: { sha256: sha256("tar fixture"), version: "bsdtar fixture" } },
    dependencies: [{ name: "dependency", version: "1.0.0", sha256: sha256(dependency) }] };
  const args = { package: packageRoot, authority, "dart-home": runtime, archiver, cache, output: join(root, "qualified") }, calls = [];
  const command = (exe, argv, options) => {
    calls.push({ exe, argv, options });
    if (argv[0] === "--version") return { status: 0, stdout: exe === archiver ? (failure === "tar-version" ? "wrong" : "bsdtar fixture\n") : failure === "dart-version" ? "wrong" : 'Dart SDK version: fixture (stable) on "windows_x64"\n' };
    if (argv.includes("-czf")) {
      const start = argv.indexOf("-C"), source = argv[start + 1];
      const packed = Object.fromEntries(argv.slice(start + 2).map(name => [name, readFileSync(join(source, name))]));
      if (failure === "build-payload") packed.LICENSE = "drift";
      if (failure === "build-inventory") packed.extra = "extra";
      const bytes = archive(packed); if (failure === "gzip-time") bytes.writeUInt32LE(123, 4);
      put(argv[argv.indexOf("-czf") + 1], bytes);
    }
    if (argv[0] === "-xzf") {
      const dest = argv[3]; for (const entry of tarEntries(gunzipSync(readFileSync(argv[1])))) put(join(dest, entry.path), entry.body);
      if (dest === join(args.output, "sdk")) {
        if (failure === "install-payload") put(join(dest, "LICENSE"), "drift");
        if (failure === "install-inventory") put(join(dest, "unexpected.dart"), "extra");
      } else if (failure === "dependency-install") put(join(dest, "lib/dependency.dart"), "drift");
    }
    if (argv[0] === "pub") {
      const config = { packages: [
        { name: "acyclic_sdk_transport", rootUri: pathToFileURL(join(args.output, "sdk")).href, packageUri: "lib/" },
        { name: "sdk_dart_consumer", rootUri: pathToFileURL(options.cwd).href, packageUri: "lib/" },
        { name: "dependency", rootUri: pathToFileURL(join(options.env.PUB_CACHE, "hosted/pub.dev/dependency-1.0.0")).href, packageUri: "lib/" },
      ] };
      if (failure === "config") config.packages[0].rootUri = pathToFileURL(args.package).href;
      if (failure === "config-extra") config.packages.push(config.packages[0]);
      put(join(options.cwd, ".dart_tool/package_config.json"), JSON.stringify(config));
      if (failure === "lock") put(join(options.cwd, "pubspec.lock"), "drift");
    }
    if (argv[0] === "compile") { put(argv.at(-1), "compiled consumer fixture"); if (failure === "compile") return { status: 1, stderr: "compile error" }; }
    if (argv[0].startsWith("--packages=")) {
      if (failure === "late-sdk") put(join(args.output, "sdk/LICENSE"), "drift");
      if (failure === "late-dependency") put(join(options.env.PUB_CACHE, "hosted/pub.dev/dependency-1.0.0/lib/dependency.dart"), "drift");
      if (failure === "late-runtime") put(join(runtime, "bin/dart.exe"), "drift");
      if (failure === "late-runtime-inventory") put(join(runtime, "unadmitted.dll"), "extra");
      return { status: failure === "positive" ? 1 : 0, stdout: failure === "silent" ? "" : "PASS: installed Dart descriptors, bytes, unsigned bits, optional presence, oneof and gRPC shapes\n" };
    }
    if (argv[0] === "analyze") {
      const name = argv.at(-1), type = name.includes("Optional") ? "Int64?" : "List<int>?";
      const text = `ERROR|COMPILE_TIME_ERROR|ARGUMENT_TYPE_NOT_ASSIGNABLE|${join(options.cwd, name)}|2|45|3|The argument type 'fixture' can't be assigned to the parameter type '${type}'.\n`;
      return { status: failure === "accepted" ? 0 : 3, stdout: failure === "unrelated" ? "unrelated error" : failure === "extra-diagnostic" ? text + "other error\n" : text };
    }
    return { status: 0, stdout: "" };
  };
  return { root, args, payload, receipt, manifest, toolchain, command, calls, put };
}

test("builds, installs and checks a fresh offline SDK with three independent static rejections", t => {
  const f = fixture(t), result = qualify(f.args, f);
  assert.equal(result.independent_negative_static_type_controls_rejected, 3);
  assert.equal(result.complete_file_descriptor_comparison, false); assert.equal(result.rust_backed_rpc_qualified, false);
  assert.equal(result.archive_sha256, sha256(readFileSync(join(f.args.output, "package/acyclic_sdk_transport-0.2.0-alpha.1.tar.gz"))));
  assert.equal(f.calls.filter(c => c.argv[0] === "analyze").length, 3);
  for (const { argv, options } of f.calls) {
    assert.equal(options.env.PUB_CACHE, join(f.args.output, "cache")); assert.equal(options.env.HOME, join(f.args.output, "home"));
    if (argv[0] === "pub") { assert.ok(argv.includes("--offline")); assert.ok(argv.includes("--enforce-lockfile")); }
    if (argv.includes("-czf")) assert.ok(argv.includes("gzip:!timestamp"));
    if (argv[0] === "-xzf") assert.ok(argv[1].startsWith(f.args.output));
  }
  assert.deepEqual(JSON.parse(readFileSync(join(f.args.output, "qualification.json"))), result);
});

test("drifting package, authority, runtime, archiver, dependencies and metadata fail before commands", t => {
  for (const drift of ["package", "authority", "runtime", "archiver", "dependency", "template"]) {
    const f = fixture(t); const name = drift === "package" ? join(f.args.package, "LICENSE") : drift === "authority" ? join(f.args.authority, "shared.bin")
      : drift === "runtime" ? join(f.args["dart-home"], "bin/dart.exe") : drift === "archiver" ? f.args.archiver : drift === "dependency" ? join(f.args.cache, "dependency-1.0.0.tar.gz") : join(f.args.package, "pubspec.yaml");
    f.put(name, "drift"); if (drift === "template") { f.receipt.output_sha256["pubspec.yaml"] = sha256("drift"); f.put(join(f.args.package, "generation-receipt.json"), JSON.stringify(f.receipt)); }
    assert.throws(() => qualify(f.args, f)); assert.equal(f.calls.length, 0); assert.equal(existsSync(f.args.output), false);
  }
});

test("existing and overlapping destinations are preserved", t => {
  const f = fixture(t); mkdirSync(f.args.output); f.put(join(f.args.output, "keep"), "caller bytes");
  assert.throws(() => qualify(f.args, f), /absent/); assert.equal(readFileSync(join(f.args.output, "keep"), "utf8"), "caller bytes");
  f.args.output = join(f.args.cache, "new"); assert.throws(() => qualify(f.args, f), /overlaps/); assert.equal(f.calls.length, 0);
});

test("unsafe or duplicate package outputs and missing bindings fail before commands", t => {
  for (const name of ["../outside", "LICENSE", "lib/actors/v1/actors.pb.dart"]) {
    const f = fixture(t); if (name === "lib/actors/v1/actors.pb.dart") f.receipt.outputs = f.receipt.outputs.filter(n => n !== name); else f.receipt.outputs.push(name);
    f.put(join(f.args.package, "generation-receipt.json"), JSON.stringify(f.receipt)); assert.throws(() => qualify(f.args, f)); assert.equal(f.calls.length, 0);
  }
});

test("unsafe dependency archive members fail even when the archive digest is admitted", t => {
  const f = fixture(t), bytes = archive({ "../escape": "bad", "pubspec.yaml": "name: dependency" });
  f.put(join(f.args.cache, "dependency-1.0.0.tar.gz"), bytes); f.toolchain.dependencies[0].sha256 = sha256(bytes);
  assert.throws(() => qualify(f.args, f), /unsafe/); assert.equal(f.calls.length, 0); assert.equal(existsSync(f.args.output), false);
});

test("runtime inventory additions fail before commands", t => {
  const f = fixture(t); f.put(join(f.args["dart-home"], "unadmitted.dll"), "extra");
  assert.throws(() => qualify(f.args, f), /runtime inventory/); assert.equal(f.calls.length, 0);
});

for (const failure of ["dart-version", "tar-version", "build-payload", "build-inventory", "gzip-time", "install-payload", "install-inventory", "dependency-install", "config", "config-extra", "lock", "compile", "positive", "silent", "accepted", "unrelated", "extra-diagnostic", "late-sdk", "late-dependency", "late-runtime", "late-runtime-inventory"]) {
  test(`${failure} cannot write a success receipt`, t => {
    const f = fixture(t, failure); assert.throws(() => qualify(f.args, f)); assert.equal(existsSync(join(f.args.output, "qualification.json")), false);
  });
}
