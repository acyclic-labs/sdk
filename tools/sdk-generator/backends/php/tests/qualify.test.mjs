import assert from "node:assert/strict";
import { existsSync, mkdirSync, mkdtempSync, readFileSync, rmSync, writeFileSync } from "node:fs";
import { tmpdir } from "node:os";
import { dirname, join } from "node:path";
import { fileURLToPath } from "node:url";
import { gzipSync } from "node:zlib";
import test from "node:test";
import { qualify } from "../src/qualify.mjs";
import { sha256 } from "../../../shared/authority.mjs";

function archive(payload) {
  const blocks = [];
  for (const [name, body] of Object.entries(payload)) {
    const bytes = Buffer.from(body), header = Buffer.alloc(512);
    header.write(name); header.write("0000644\0", 100); header.write("0000000\0", 108); header.write("0000000\0", 116);
    header.write(bytes.length.toString(8).padStart(11, "0") + "\0", 124); header.write("00000000000\0", 136);
    header.fill(32, 148, 156); header[156] = 48;
    const checksum = header.reduce((a, b) => a + b, 0); header.write(checksum.toString(8).padStart(6, "0") + "\0 ", 148);
    blocks.push(header, bytes, Buffer.alloc((512 - bytes.length % 512) * (bytes.length % 512 ? 1 : 0)));
  }
  return gzipSync(Buffer.concat([...blocks, Buffer.alloc(1024)]));
}
function fixture(t, failure = "") {
  const root = mkdtempSync(join(tmpdir(), "php-qualification-test-")); t.after(() => rmSync(root, { recursive: true }));
  const put = (file, bytes) => { mkdirSync(dirname(file), { recursive: true }); writeFileSync(file, bytes); };
  const authority = join(root, "authority"), packageRoot = join(root, "package"), runtime = join(root, "runtime"), cache = join(root, "prepared-cache");
  const targets = ["actors/v1/actors.proto", "workers/v1/workers.proto", "stream/v1/stream.proto"];
  const manifest = { schema: "acyclic.sdk.rust-authority.v1", authority: "rust", source_revision: "a".repeat(40), families: targets.map(source => {
    put(join(authority, source), source); return { source, source_sha256: sha256(source), descriptor: "shared.bin", descriptor_sha256: sha256("descriptors") };
  }) };
  put(join(authority, "shared.bin"), "descriptors");
  const manifestBytes = Buffer.from(JSON.stringify(manifest)); put(join(authority, "rust-authority.json"), manifestBytes);
  const payload = { "composer.json": readFileSync("tools/sdk-generator/backends/php/templates/package/composer.json"), LICENSE: "license", NOTICE: "notice", "authority/rust-authority.json": manifestBytes };
  for (const [family, version] of [["Actors", "V1"], ["Workers", "V1"], ["Stream", "V1"]]) {
    payload[`src/GPBMetadata/${family}/${version}/${family}.php`] = "metadata fixture";
    payload[`src/Acyclic/${family}/${version}/${family}ServiceClient.php`] = "client fixture";
  }
  for (const [name, bytes] of Object.entries(payload)) put(join(packageRoot, name), bytes);
  const receipt = { schema: "acyclic.sdk.php-producer-receipt.v1", authority: "rust", target: "php", source_revision: manifest.source_revision,
    authority_manifest_sha256: sha256(manifestBytes), outputs: Object.keys(payload), output_sha256: Object.fromEntries(Object.entries(payload).map(([name, bytes]) => [name, sha256(bytes)])) };
  put(join(packageRoot, "generation-receipt.json"), JSON.stringify(receipt));
  const composer = join(root, "composer.phar"), extension = join(root, "grpc.dll"), archiver = join(root, "tar");
  put(join(runtime, "php.exe"), "PHP fixture"); put(composer, "Composer fixture"); put(extension, "gRPC fixture"); put(archiver, "tar fixture");
  const dependencyMetadata = { name: "fixture/dependency", version: "1.0.0", require: { php: ">=8.2" }, autoload: {} };
  const contents = { "composer.json": JSON.stringify(dependencyMetadata), "src/dependency.php": "dependency fixture" };
  const rawArchive = Buffer.from("published ZIP fixture"); put(join(cache, "fixture-dependency-1.0.0.zip"), rawArchive);
  const toolchain = { php_version: "fixture", runtime_archive: { url: "fixture", sha256: sha256("runtime archive") }, runtime_files: { "php.exe": sha256("PHP fixture") },
    composer: { version: "fixture", sha256: sha256("Composer fixture") }, grpc_extension: { version: "fixture", binary_sha256: sha256("gRPC fixture") },
    archiver: { [`${process.platform}-${process.arch}`]: { version: "bsdtar fixture", sha256: sha256("tar fixture") } },
    dependencies: [{ name: dependencyMetadata.name, version: "1.0.0", source: { reference: "b".repeat(40) }, archive_sha256: sha256(rawArchive), composer: dependencyMetadata }] };
  const args = { package: packageRoot, authority, "php-home": runtime, composer, "grpc-extension": extension, archiver, cache, output: join(root, "qualified") }, calls = [];
  const sdkName = JSON.parse(payload["composer.json"]).name;
  const vendor = join(args.output, "project/vendor"), installed = join(vendor, ...sdkName.split("/")), dependency = join(vendor, "fixture/dependency");
  const command = (exe, argv, options) => {
    calls.push({ exe, argv, options });
    if (exe === archiver && argv[0] === "--version") return { status: 0, stdout: failure === "tar-version" ? "wrong" : "bsdtar fixture\n" };
    if (argv.includes("-r")) return { status: 0, stdout: failure === "php-version" ? "[]" : JSON.stringify(["fixture", 8, "fixture"]) };
    if (argv.includes("--version")) return { status: 0, stdout: failure === "composer-version" ? "wrong" : "Composer version fixture date\n" };
    if (argv.some(arg => arg.endsWith("archive_inventory.php"))) {
      if (failure === "zip-inspection") return { status: 1, stderr: "ZIP CRC differs" };
      const entries = Object.entries(contents).map(([name, body]) => ({ path: `root/${name}`, directory: false, data: Buffer.from(body).toString("base64") }));
      if (failure === "zip-path") entries[1].path = "root/../escape";
      if (failure === "zip-case-collision") entries.push({ ...entries[0], path: "ROOT/COMPOSER.JSON" });
      if (failure === "zip-root") entries[1].path = "other/source.php";
      if (failure === "zip-data") entries[1].data = "not-base64!";
      if (failure === "zip-metadata") entries[0].data = Buffer.from("{}").toString("base64");
      return { status: 0, stdout: JSON.stringify(entries) };
    }
    if (argv.includes("-czf")) {
      const index = argv.indexOf("-C"), source = argv[index + 1];
      const packed = Object.fromEntries(argv.slice(index + 2).map(name => [name, readFileSync(join(source, name))]));
      if (failure === "build-payload") packed.LICENSE = "drift";
      if (failure === "build-inventory") packed.extra = "extra";
      const bytes = archive(packed); if (failure === "gzip-time") bytes.writeUInt32LE(123, 4);
      put(argv[argv.indexOf("-czf") + 1], bytes);
    }
    if (argv.includes("update")) {
      const project = JSON.parse(readFileSync(join(options.cwd, "composer.json")));
      const packages = project.repositories.filter(repo => repo.type === "package").map(repo => ({ name: repo.package.name, version: repo.package.version, dist: repo.package.dist }));
      if (failure === "lock-inventory") packages.push(packages[0]);
      if (failure === "lock-version") packages[0].version = "9.0.0";
      if (failure === "lock-url") packages[0].dist.url = "file:///outside/package.tar.gz";
      if (failure === "lock-reference") packages[1].dist.reference = "c".repeat(40);
      put(join(options.cwd, "composer.lock"), JSON.stringify({ packages, "packages-dev": failure === "lock-dev" ? [{}] : [] }));
    }
    if (argv.includes("install")) {
      for (const [name, bytes] of Object.entries(payload)) put(join(installed, name), bytes);
      for (const [name, bytes] of Object.entries(contents)) put(join(dependency, name), bytes);
      const locked = JSON.parse(readFileSync(join(options.cwd, "composer.lock")));
      if (failure === "installed-reference") locked.packages[1].dist.reference = "c".repeat(40);
      put(join(vendor, "composer/installed.json"), JSON.stringify(locked)); put(join(vendor, "autoload.php"), "autoload fixture");
      if (failure === "install-payload") put(join(installed, "LICENSE"), "drift");
      if (failure === "install-inventory") put(join(installed, "extra"), "extra");
      if (failure === "dependency-payload") put(join(dependency, "src/dependency.php"), "drift");
      if (failure === "dependency-inventory") put(join(dependency, "extra"), "extra");
      if (failure === "install-lock") put(join(options.cwd, "composer.lock"), "{}");
      if (failure === "install-project") put(join(options.cwd, "composer.json"), "{}");
    }
    if (argv.includes("installed_consumer.php")) {
      if (failure === "late-sdk") put(join(installed, "LICENSE"), "drift");
      if (failure === "late-dependency") put(join(dependency, "src/dependency.php"), "drift");
      if (failure === "late-runtime") put(join(runtime, "php.exe"), "drift");
      if (failure === "late-runtime-inventory") put(join(runtime, "unadmitted.dll"), "extra");
      if (failure === "late-composer") put(composer, "drift");
      if (failure === "late-extension") put(extension, "drift");
      if (failure === "late-archiver") put(archiver, "drift");
      if (failure === "late-lock") put(join(options.cwd, "composer.lock"), "{}");
      if (failure === "late-project") put(join(options.cwd, "composer.json"), "{}");
      if (failure === "late-control") put(join(options.cwd, "installed_consumer.php"), "drift");
      if (failure === "late-autoloader") put(join(vendor, "autoload.php"), "drift");
      if (failure === "late-installed-metadata") put(join(vendor, "composer/installed.json"), "{}");
      if (failure === "late-vendor-package") put(join(vendor, "unexpected/package/source.php"), "extra");
      return { status: failure === "positive" ? 1 : 0, stdout: failure === "silent" ? "" : "PASS: installed PHP descriptors, bytes, unsigned bits, optional zero, oneof and client RPC shapes\n" };
    }
    if (argv.includes("native_client.php")) return { status: failure === "native" ? 1 : 0, stdout: failure === "native-silent" ? "" : "PASS: native PHP gRPC client creation and shutdown\n" };
    const invalid = argv.find(arg => /^Invalid\w+\.php$/.test(arg));
    if (invalid) {
      const expected = invalid.includes("Optional") ? "Exception: Expect integer." : "InvalidArgumentException: Expect string.";
      const text = `Fatal error: Uncaught ${expected} in ${join(dependency, "GPBUtil.php")}:74\n${join(options.cwd, invalid)}(3)\n`;
      return { status: failure === "accepted" ? 0 : 255, stderr: failure === "unrelated" ? "other fatal error" : failure === "extra-diagnostic" ? "Warning: unrelated\n" + text : failure === "wrong-negative-line" ? text.replace("(3)", "(8)") : text };
    }
    return { status: 0, stdout: "" };
  };
  return { root, args, payload, receipt, toolchain, command, calls, put };
}

test("canonical packaging, locked offline installation and three intended runtime rejections write a receipt", t => {
  const f = fixture(t), result = qualify(f.args, f);
  assert.equal(result.independent_negative_runtime_type_controls_rejected, 3);
  assert.equal(result.native_client_creation_shutdown_passed, true); assert.equal(result.rust_backed_rpc_qualified, false);
  assert.equal(result.complete_file_descriptor_comparison, true);
  assert.equal(f.calls.filter(call => call.argv.some(arg => /^Invalid\w+\.php$/.test(arg))).length, 3);
  for (const { argv, options } of f.calls) {
    assert.equal(options.env.COMPOSER_DISABLE_NETWORK, "1"); assert.equal(options.env.COMPOSER_CACHE_DIR, join(f.args.output, "cache"));
    assert.equal(options.env.HOME, join(f.args.output, "home"));
    if (argv.includes("-czf")) assert.ok(argv.includes("gzip:!timestamp"));
    if (argv.includes("install")) { assert.ok(argv.includes("--no-plugins")); assert.ok(argv.includes("--no-scripts")); }
  }
  assert.deepEqual(JSON.parse(readFileSync(join(f.args.output, "qualification.json"))), result);
});
test("drift in package, authority, runtime, tools, dependency or template fails before commands", t => {
  for (const drift of ["package", "authority", "runtime", "composer", "extension", "archiver", "dependency", "template"]) {
    const f = fixture(t); const file = drift === "package" ? join(f.args.package, "LICENSE") : drift === "authority" ? join(f.args.authority, "shared.bin")
      : drift === "runtime" ? join(f.args["php-home"], "php.exe") : drift === "composer" ? f.args.composer : drift === "extension" ? f.args["grpc-extension"]
        : drift === "archiver" ? f.args.archiver : drift === "dependency" ? join(f.args.cache, "fixture-dependency-1.0.0.zip") : join(f.args.package, "composer.json");
    f.put(file, "drift"); if (drift === "template") { f.receipt.output_sha256["composer.json"] = sha256("drift"); f.put(join(f.args.package, "generation-receipt.json"), JSON.stringify(f.receipt)); }
    assert.throws(() => qualify(f.args, f));
    // ZIP digests are checked after version admission, before any output exists.
    if (drift !== "dependency") assert.equal(f.calls.length, 0);
    assert.equal(existsSync(f.args.output), false);
  }
});
test("existing and overlapping outputs are preserved", t => {
  const f = fixture(t); mkdirSync(f.args.output); f.put(join(f.args.output, "keep"), "caller data");
  assert.throws(() => qualify(f.args, f), /absent/); assert.equal(readFileSync(join(f.args.output, "keep"), "utf8"), "caller data");
  f.args.output = join(f.args.cache, "new"); assert.throws(() => qualify(f.args, f), /overlaps/); assert.equal(f.calls.length, 0);
});
test("missing, unsafe and case-colliding package output is rejected", t => {
  for (const mutation of ["missing", "unsafe", "case"]) {
    const f = fixture(t);
    if (mutation === "missing") f.receipt.outputs = f.receipt.outputs.filter(name => !name.endsWith("ActorsServiceClient.php"));
    else f.receipt.outputs.push(mutation === "case" ? "license" : "../outside");
    f.put(join(f.args.package, "generation-receipt.json"), JSON.stringify(f.receipt)); assert.throws(() => qualify(f.args, f)); assert.equal(f.calls.length, 0);
  }
});
test("runtime inventory additions and unsafe or duplicate dependency coordinates fail", t => {
  for (const kind of ["runtime", "coordinate", "duplicate"]) {
    const f = fixture(t);
    if (kind === "runtime") f.put(join(f.args["php-home"], "unadmitted.dll"), "extra");
    if (kind === "coordinate") f.toolchain.dependencies[0].name = "../escape";
    if (kind === "duplicate") f.toolchain.dependencies.push(f.toolchain.dependencies[0]);
    assert.throws(() => qualify(f.args, f)); assert.equal(existsSync(f.args.output), false);
  }
});
test("ambient Composer and PHP configuration cannot enter child commands", t => {
  const keys = ["COMPOSER_AUTH", "COMPOSER_REPOSITORIES", "PHPRC", "PHP_INI_SCAN_DIR", "TAR_OPTIONS"];
  const prior = Object.fromEntries(keys.map(key => [key, process.env[key]]));
  t.after(() => { for (const key of keys) if (prior[key] === undefined) delete process.env[key]; else process.env[key] = prior[key]; });
  for (const key of keys) process.env[key] = "untrusted fixture";
  const f = fixture(t); qualify(f.args, f);
  for (const { options } of f.calls) for (const key of keys) assert.equal(options.env[key], undefined);
});
for (const failure of ["php-version", "composer-version", "tar-version", "zip-inspection", "zip-path", "zip-case-collision", "zip-root", "zip-data", "zip-metadata",
  "build-payload", "build-inventory", "gzip-time", "lock-inventory", "lock-version", "lock-url", "lock-reference", "lock-dev", "installed-reference", "install-payload", "install-inventory",
  "dependency-payload", "dependency-inventory", "install-lock", "install-project", "positive", "silent", "native", "native-silent", "accepted", "unrelated", "extra-diagnostic", "wrong-negative-line",
  "late-sdk", "late-dependency", "late-runtime", "late-runtime-inventory", "late-composer", "late-extension", "late-archiver", "late-lock", "late-project", "late-control", "late-autoloader", "late-installed-metadata", "late-vendor-package"]) {
  test(`${failure} cannot produce a success receipt`, t => {
    const f = fixture(t, failure); assert.throws(() => qualify(f.args, f)); assert.equal(existsSync(join(f.args.output, "qualification.json")), false);
  });
}
