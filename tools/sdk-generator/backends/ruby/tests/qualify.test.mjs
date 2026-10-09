import assert from "node:assert/strict";
import { existsSync, mkdirSync, mkdtempSync, readFileSync, rmSync, writeFileSync } from "node:fs";
import { tmpdir } from "node:os";
import { delimiter, dirname, join } from "node:path";
import test from "node:test";
import { sha256 } from "../../../shared/authority.mjs";
import { qualify } from "../src/qualify.mjs";

function fixture(t, failure = "") {
  const root = mkdtempSync(join(tmpdir(), "ruby-qualification-test-")); t.after(() => rmSync(root, { recursive: true }));
  const put = (path, bytes) => { mkdirSync(dirname(path), { recursive: true }); writeFileSync(path, bytes); };
  const authority = join(root, "authority");
  const families = ["actors/v1/actors.proto", "workers/v1/workers.proto", "stream/v2/stream.proto"].map(source => {
    put(join(authority, source), source);
    return { source, source_sha256: sha256(source), descriptor: "shared.bin", descriptor_sha256: sha256("descriptors") };
  });
  put(join(authority, "shared.bin"), "descriptors"); put(join(authority, "import.proto"), "import");
  families.push({ source: "import.proto", source_sha256: sha256("import") });
  const manifest = Buffer.from(JSON.stringify({ schema: "acyclic.sdk.rust-authority.v1", authority: "rust", source_revision: "a".repeat(40), families }));
  put(join(authority, "rust-authority.json"), manifest);
  const packageRoot = join(root, "package");
  const spec = readFileSync(new URL("../templates/package/acyclic-sdk-transport.gemspec", import.meta.url), "utf8").replace("@RUST_SOURCE_REVISION@", "a".repeat(40));
  const payload = { "acyclic-sdk-transport.gemspec": spec, LICENSE: "license", NOTICE: "notice", "authority/rust-authority.json": manifest, "lib/bindings.rb": "generated binding" };
  for (const [name, bytes] of Object.entries(payload)) put(join(packageRoot, name), bytes);
  const receipt = { schema: "acyclic.sdk.ruby-producer-receipt.v1", authority: "rust", target: "ruby", source_revision: "a".repeat(40),
    authority_manifest_sha256: sha256(manifest), outputs: Object.keys(payload), output_sha256: Object.fromEntries(Object.entries(payload).map(([name, bytes]) => [name, sha256(bytes)])) };
  put(join(packageRoot, "generation-receipt.json"), JSON.stringify(receipt));
  const rubyHome = join(root, "ruby"), cache = join(root, "prepared-cache");
  put(join(rubyHome, "bin/ruby.exe"), "ruby fixture"); put(join(rubyHome, "bin/gem"), "gem fixture");
  put(join(cache, "dependency-1.0.gem"), "dependency fixture"); put(join(cache, "untrusted-sdk.gem"), "old SDK must not be reused");
  const toolchain = { ruby_version: "fixture", ruby_platform: "fixture-platform", ruby_api_version: "fixture-api", source_date_epoch: "1767225600",
    runtime_source: { url: "fixture", sha256: sha256("runtime archive fixture") },
    runtime_files: { "bin/ruby.exe": sha256("ruby fixture"), "bin/gem": sha256("gem fixture") },
    dependencies: [{ name: "dependency", version: "1.0", platform: "ruby", sha256: sha256("dependency fixture") }] };
  const args = { package: packageRoot, authority, "ruby-home": rubyHome, cache, output: join(root, "qualified") };
  const calls = [];
  const command = (exe, argv, options) => {
    calls.push({ exe, argv, options });
    if (argv[0] === "--version") return { status: 0, stdout: failure === "version" ? "wrong version" : "ruby fixture (build) [fixture-platform]\n" };
    if (argv.includes("build")) put(join(options.cwd, "acyclic-sdk-transport-0.2.0.alpha.1.gem"), "SDK archive fixture");
    if (argv.includes("install") && argv.at(-1).endsWith("acyclic-sdk-transport-0.2.0.alpha.1.gem")) {
      const installed = join(options.env.GEM_HOME, "gems/acyclic-sdk-transport-0.2.0.alpha.1");
      for (const [name, bytes] of Object.entries(payload)) if (!name.endsWith(".gemspec")) put(join(installed, name), failure === "payload_install" && name === "lib/bindings.rb" ? "drift" : bytes);
      if (failure === "inventory") put(join(installed, "unexpected.rb"), "unexpected");
      put(join(options.env.GEM_HOME, "cache/acyclic-sdk-transport-0.2.0.alpha.1.gem"), failure === "archive_install" ? "wrong archive" : "SDK archive fixture");
    }
    if (argv[0].endsWith("installed_consumer.rb")) return { status: failure === "positive" ? 1 : 0,
      stdout: failure === "silent" ? "" : "PASS: installed Ruby descriptors, bytes, unsigned bounds, optional presence, oneof and gRPC shapes\n" };
    if (/Invalid/.test(argv[0])) {
      const field = argv[0].includes("ActorBytes") ? "code_sha256" : argv[0].includes("WorkerBytes") ? "javascript_module" : "if_tail";
      return { status: failure === "unrelated" ? 1 : 0, stdout: failure === "accepted" ? "accepted" : `PASS: intended ${field} TypeError: intended fixture\n` };
    }
    return { status: 0, stdout: "" };
  };
  return { args, toolchain, command, calls, put, payload, receipt };
}

test("qualifier builds and installs exact gem in a fresh cache with independent runtime type controls", t => {
  const f = fixture(t); const result = qualify(f.args, f);
  assert.equal(result.independent_negative_runtime_type_controls_rejected, 3);
  assert.equal(result.compile_time_type_controls_supported, false);
  assert.equal(result.rust_backed_rpc_qualified, false);
  assert.equal(f.calls.filter(call => /Invalid/.test(call.argv[0])).length, 3);
  for (const { argv, options } of f.calls) {
    assert.equal(options.env.GEM_HOME, join(f.args.output, "cache"));
    assert.deepEqual(options.env.GEM_PATH.split(delimiter), [join(f.args.output, "cache"), join(f.args["ruby-home"], "lib/ruby/gems/fixture-api")]);
    if (argv.includes("install")) {
      assert.ok(argv.includes("--local")); assert.ok(argv.includes("--ignore-dependencies"));
      assert.ok(argv.at(-1).startsWith(f.args.output));
    }
  }
  assert.equal(readFileSync(join(f.args.output, "feed/dependency-1.0.gem"), "utf8"), "dependency fixture");
  assert.deepEqual(JSON.parse(readFileSync(join(f.args.output, "qualification.json"))), result);
});

test("package, authority, runtime, dependency and template drift fail before tool execution", t => {
  for (const drift of ["package", "authority", "runtime", "dependency", "template"]) {
    const f = fixture(t);
    const path = drift === "package" ? join(f.args.package, "lib/bindings.rb") : drift === "authority" ? join(f.args.authority, "import.proto")
      : drift === "runtime" ? join(f.args["ruby-home"], "bin/ruby.exe") : drift === "dependency" ? join(f.args.cache, "dependency-1.0.gem") : join(f.args.package, "acyclic-sdk-transport.gemspec");
    f.put(path, "drift");
    if (drift === "template") { f.receipt.output_sha256["acyclic-sdk-transport.gemspec"] = sha256("drift"); f.put(join(f.args.package, "generation-receipt.json"), JSON.stringify(f.receipt)); }
    assert.throws(() => qualify(f.args, f)); assert.equal(f.calls.length, 0); assert.equal(existsSync(f.args.output), false);
  }
});

test("existing and overlapping output destinations are preserved", t => {
  const f = fixture(t); f.put(join(f.args.output, "keep"), "retained");
  assert.throws(() => qualify(f.args, f), /must be absent/); assert.equal(readFileSync(join(f.args.output, "keep"), "utf8"), "retained");
  f.args.output = join(f.args.cache, "new-output"); assert.throws(() => qualify(f.args, f), /overlaps/); assert.equal(f.calls.length, 0);
});

for (const failure of ["positive", "silent", "accepted", "unrelated", "version", "archive_install", "payload_install", "inventory"]) {
  test(`${failure} failure cannot produce installed Ruby qualification`, t => {
    const f = fixture(t, failure); assert.throws(() => qualify(f.args, f));
    assert.equal(existsSync(join(f.args.output, "qualification.json")), false);
  });
}
