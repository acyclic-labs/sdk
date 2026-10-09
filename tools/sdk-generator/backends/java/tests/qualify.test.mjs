import assert from "node:assert/strict";
import { existsSync, mkdirSync, mkdtempSync, readFileSync, rmSync, writeFileSync } from "node:fs";
import { tmpdir } from "node:os";
import { dirname, join } from "node:path";
import test from "node:test";
import { sha256 } from "../../../shared/authority.mjs";
import { qualify } from "../src/qualify.mjs";

const artifact = "sdk-java-transport-0.2.0-alpha.1";
function fixture(t, failure = "") {
  const root = mkdtempSync(join(tmpdir(), "java-installed-test-"));
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
  const payload = { "pom.xml": "pinned fixture pom", "src/main/resources/META-INF/rust-authority.json": manifest,
    "src/main/java/Bindings.java": "fixture generated source" };
  for (const [name, bytes] of Object.entries(payload)) put(join(packageRoot, name), bytes);
  const receipt = { schema: "acyclic.sdk.java-producer-receipt.v1", target: "java", authority: "rust", source_revision: "a".repeat(40),
    authority_manifest_sha256: sha256(manifest), outputs: Object.keys(payload),
    output_sha256: Object.fromEntries(Object.entries(payload).map(([name, bytes]) => [name, sha256(bytes)])) };
  put(join(packageRoot, "generation-receipt.json"), JSON.stringify(receipt));
  const extension = process.platform === "win32" ? ".exe" : "";
  const javaHome = join(root, "jdk");
  for (const name of [`bin/java${extension}`, `bin/javac${extension}`, "lib/modules", "release"]) put(join(javaHome, name), name);
  const mavenHome = join(root, "maven");
  for (const name of ["boot/plexus-classworlds-fixture.jar", "bin/m2.conf", "lib/core.jar"]) put(join(mavenHome, name), name);
  const cache = join(root, "cache");
  const dependency = join(cache, "dep/runtime.jar");
  put(dependency, "runtime fixture");
  const args = { package: packageRoot, authority, "java-home": javaHome, "maven-home": mavenHome, cache, output: join(root, "qualification") };
  const calls = [];
  const command = (exe, argv, options) => {
    calls.push({ exe, argv, options });
    if (argv.includes("-version")) return { status: 0, stdout: "", stderr: exe.includes("javac") ? "javac 17.0.14\n" : 'openjdk version "17.0.14"\n' };
    if (argv.includes("--version")) return { status: 0, stdout: "Apache Maven 3.9.9\n", stderr: "" };
    if (argv.includes("install")) {
      const project = dirname(argv[argv.indexOf("-f") + 1]);
      put(join(project, "target", `${artifact}.jar`), "installed jar fixture");
      put(join(cache, "dev/acyclic/sdk-java-transport/0.2.0-alpha.1", `${artifact}.jar`), "installed jar fixture");
      put(join(cache, "dev/acyclic/sdk-java-transport/0.2.0-alpha.1", `${artifact}.pom`), payload["pom.xml"]);
      put(join(project, "runtime-classpath.txt"), dependency);
      return { status: 0, stdout: "BUILD SUCCESS\n", stderr: "" };
    }
    if (argv.at(-1).endsWith("InstalledConsumer.java")) return { status: 0, stdout: "", stderr: "" };
    if (argv.includes("InstalledConsumer")) return { status: failure === "positive" ? 1 : 0, stdout: "PASS: installed Java descriptors\n", stderr: "" };
    const name = argv.at(-1);
    if (failure === "accepted" && name.endsWith("InvalidWorkerBytes.java")) return { status: 0, stdout: "", stderr: "" };
    return { status: 1, stdout: "", stderr: failure === "unrelated" ? "error: package missing\n"
      : `error: incompatible types: String cannot be converted to ${name.endsWith("InvalidOptionalInteger.java") ? "long" : "ByteString"}\n` };
  };
  return { args, calls, command, receipt, put };
}

test("offline runner installs exact package and checks each invalid assignment separately", t => {
  const f = fixture(t);
  const result = qualify(f.args, f.command);
  const build = f.calls.find(call => call.argv.includes("install"));
  assert.ok(build.argv.includes("-o"));
  assert.ok(build.argv.includes("-s") && build.argv.includes("-gs"));
  for (const call of f.calls) assert.equal(call.options.env.JAVA_TOOL_OPTIONS, undefined);
  const negative = f.calls.filter(call => /Invalid.*\.java$/.test(call.argv.at(-1)));
  assert.equal(negative.length, 3);
  assert.equal(new Set(negative.map(call => call.argv.at(-1))).size, 3);
  const positive = f.calls.find(call => call.argv.includes("InstalledConsumer"));
  assert.ok(positive.argv.some(arg => arg.startsWith("-Dsdk.qualified.jar=")));
  assert.equal(new Set(positive.argv.slice(-3)).size, 1);
  for (const path of positive.argv.slice(-3)) assert.equal(readFileSync(path, "utf8"), "descriptors");
  assert.equal(result.negative_type_controls_rejected, 3);
  assert.equal(result.rust_backed_rpc_qualified, false);
  assert.deepEqual(JSON.parse(readFileSync(join(f.args.output, "qualification.json"))), result);
});

test("payload and authority drift cannot start a build or create output", t => {
  for (const drift of ["payload", "authority"]) {
    const f = fixture(t);
    f.put(drift === "payload" ? join(f.args.package, "pom.xml") : join(f.args.authority, "import.proto"), "drift");
    assert.throws(() => qualify(f.args, f.command), /digest mismatch/);
    assert.equal(f.calls.length, 0);
    assert.equal(existsSync(f.args.output), false);
  }
});

for (const failure of ["positive", "accepted", "unrelated"]) test(`${failure} consumer failure cannot produce qualification`, t => {
  const f = fixture(t, failure);
  assert.throws(() => qualify(f.args, f.command), failure === "unrelated" ? /unrelated reason/ : /failed/);
  assert.equal(existsSync(join(f.args.output, "qualification.json")), false);
});
