import assert from "node:assert/strict";
import { createHash } from "node:crypto";
import { existsSync, mkdirSync, mkdtempSync, readFileSync, rmSync, writeFileSync } from "node:fs";
import { tmpdir } from "node:os";
import { dirname, join } from "node:path";
import test from "node:test";
import { sha256 } from "../../../shared/authority.mjs";
import { qualify } from "../src/qualify.mjs";

function fixture(t, failure = "") {
  const root = mkdtempSync(join(tmpdir(), "dotnet-installed-test-"));
  t.after(() => rmSync(root, { recursive: true }));
  const put = (path, bytes) => { mkdirSync(dirname(path), { recursive: true }); writeFileSync(path, bytes); };
  const authority = join(root, "authority");
  const families = ["actors/v1/actors.proto", "workers/v1/workers.proto", "stream/v2/stream.proto"].map(source => {
    put(join(authority, source), source);
    return { source, source_sha256: sha256(source), descriptor: "shared.bin", descriptor_sha256: sha256("descriptors") };
  });
  put(join(authority, "shared.bin"), "descriptors");
  put(join(authority, "import.proto"), "import");
  families.push({ source: "import.proto", source_sha256: sha256("import") });
  const manifest = Buffer.from(JSON.stringify({ schema: "acyclic.sdk.rust-authority.v1", authority: "rust", source_revision: "a".repeat(40), families }));
  put(join(authority, "rust-authority.json"), manifest);
  const packageRoot = join(root, "package");
  const payload = { "rust-authority.json": manifest, "generated/Bindings.cs": "fixture generated source" };
  for (const name of ["Acyclic.Sdk.Transport.csproj", "packages.lock.json", "global.json", "README.md"]) {
    payload[name] = readFileSync(new URL(`../templates/package/${name}`, import.meta.url));
  }
  for (const [name, bytes] of Object.entries(payload)) put(join(packageRoot, name), bytes);
  const receipt = { schema: "acyclic.sdk.dotnet-producer-receipt.v1", target: "dotnet", authority: "rust", source_revision: "a".repeat(40),
    authority_manifest_sha256: sha256(manifest), outputs: Object.keys(payload),
    output_sha256: Object.fromEntries(Object.entries(payload).map(([name, bytes]) => [name, sha256(bytes)])) };
  put(join(packageRoot, "generation-receipt.json"), JSON.stringify(receipt));
  const dotnet = join(root, "sdk/dotnet.exe"), nuget = join(root, "nuget.exe");
  put(dotnet, "dotnet fixture"); put(nuget, "nuget fixture");
  const toolchain = { dotnet_sdk: "8.0.425", nuget_version: "7.9.0", nuget_exe_sha256: sha256("nuget fixture"),
    deterministic_timestamp: "2026-01-01T00:00:00Z", dependency_archives: {} };
  const cache = join(root, "prepared-cache");
  const dependencies = JSON.parse(payload["packages.lock.json"]).dependencies["net8.0"];
  for (const [id, dependency] of Object.entries(dependencies)) {
    const name = `${id.toLowerCase()}.${dependency.resolved}.nupkg`;
    put(join(cache, id.toLowerCase(), dependency.resolved, name), name);
    toolchain.dependency_archives[name] = { sha512: createHash("sha512").update(name).digest("base64") };
  }
  put(join(cache, "acyclic.sdk.transport/0.2.0-alpha.1/acyclic.sdk.transport.0.2.0-alpha.1.nupkg"), "stale SDK must not be copied");
  const args = { package: packageRoot, authority, dotnet, nuget, cache, output: join(root, "qualification") };
  const calls = [];
  const command = (exe, argv, options) => {
    calls.push({ exe, argv, options });
    if (argv[0] === "--list-sdks") return { status: 0, stdout: "8.0.425 [fixture]\n" };
    if (exe === nuget) {
      put(join(argv[argv.indexOf("-OutputDirectory") + 1], "Acyclic.Sdk.Transport.0.2.0-alpha.1.nupkg"), "new built archive");
    } else if (argv[0] === "pack") {
      put(join(dirname(argv[1]), "bin/Release/net8.0/Acyclic.Sdk.Transport.dll"), "new assembly");
    } else if (argv[0] === "restore" && argv[1].endsWith("Consumer.csproj")) {
      const installed = join(options.env.NUGET_PACKAGES, "acyclic.sdk.transport/0.2.0-alpha.1");
      put(join(installed, "acyclic.sdk.transport.0.2.0-alpha.1.nupkg"), "new built archive");
      put(join(installed, "lib/net8.0/Acyclic.Sdk.Transport.dll"), failure === "installation" ? "stale assembly" : "new assembly");
    } else if (argv[0] === "build" && options.cwd.includes("Invalid")) {
      const integer = options.cwd.endsWith("InvalidOptionalInteger");
      if (failure === "accepted") return { status: 0, stdout: "" };
      return { status: 1, stdout: failure === "unrelated" ? "error CS0246: missing namespace\n"
        : `error CS0029: Cannot implicitly convert type 'string' to '${integer ? "ulong" : "Google.Protobuf.ByteString"}'\n` };
    } else if (argv[0].endsWith("Consumer.dll") && failure === "positive") return { status: 1, stdout: "consumer failed" };
    return { status: 0, stdout: "PASS\n", stderr: "" };
  };
  return { args, command, toolchain, calls, put };
}

test("offline qualifier installs its deterministic package in a fresh cache and checks independent negatives", t => {
  const f = fixture(t); const result = qualify(f.args, f);
  assert.equal(result.negative_type_controls_rejected, 3);
  assert.equal(result.rust_backed_rpc_qualified, false);
  const pack = f.calls.find(call => call.exe === f.args.nuget);
  assert.ok(pack.argv.includes("-Deterministic"));
  assert.equal(pack.argv[pack.argv.indexOf("-DeterministicTimestamp") + 1], "2026-01-01T00:00:00Z");
  const restore = f.calls.filter(call => call.argv[0] === "restore");
  assert.equal(restore.length, 5);
  assert.ok(restore[0].argv.includes("--locked-mode"));
  for (const call of restore) {
    assert.ok(call.argv.includes("--disable-parallel"));
    assert.equal(call.options.env.NUGET_PACKAGES, join(f.args.output, "cache"));
    assert.match(readFileSync(call.argv[call.argv.indexOf("--configfile") + 1], "utf8"), /<clear\/>/);
  }
  assert.equal(f.calls.filter(call => call.argv[0] === "build" && call.options.cwd.includes("Invalid")).length, 3);
  const positive = f.calls.find(call => call.argv[0].endsWith("Consumer.dll"));
  assert.equal(positive.argv.at(-1), sha256("new assembly"));
  for (const path of positive.argv.slice(1, 4)) assert.equal(readFileSync(path, "utf8"), "descriptors");
  assert.deepEqual(JSON.parse(readFileSync(join(f.args.output, "qualification.json"))), result);
});

test("payload, authority, tool and dependency drift are rejected before any build", t => {
  for (const drift of ["payload", "authority", "tool", "dependency"]) {
    const f = fixture(t);
    const path = drift === "payload" ? join(f.args.package, "generated/Bindings.cs") : drift === "authority" ? join(f.args.authority, "import.proto")
      : drift === "tool" ? f.args.nuget : join(f.args.cache, "google.protobuf/3.31.1/google.protobuf.3.31.1.nupkg");
    f.put(path, "drift"); assert.throws(() => qualify(f.args, f));
    assert.equal(f.calls.length, 0); assert.equal(existsSync(f.args.output), false);
  }
});

for (const failure of ["positive", "accepted", "unrelated", "installation"]) {
  test(`${failure} failure cannot produce qualification`, t => {
    const f = fixture(t, failure); assert.throws(() => qualify(f.args, f));
    assert.equal(existsSync(join(f.args.output, "qualification.json")), false);
  });
}
