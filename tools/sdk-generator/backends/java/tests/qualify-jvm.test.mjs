import assert from "node:assert/strict";
import { existsSync, mkdirSync, mkdtempSync, readFileSync, rmSync, writeFileSync } from "node:fs";
import { tmpdir } from "node:os";
import { dirname, join } from "node:path";
import test from "node:test";
import { sha256 } from "../../../shared/authority.mjs";
import { qualifyJvm } from "../src/qualify-jvm.mjs";

function fixture(t, language = "kotlin", failure = "") {
  const root = mkdtempSync(join(tmpdir(), "jvm-consumer-test-")); t.after(() => rmSync(root, { recursive: true }));
  const put = (path, bytes) => { mkdirSync(dirname(path), { recursive: true }); writeFileSync(path, bytes); };
  const authority = join(root, "authority");
  const families = ["actors/v1/actors.proto", "workers/v1/workers.proto", "stream/v2/stream.proto"].map(source => {
    put(join(authority, source), source);
    return { source, source_sha256: sha256(source), descriptor: "shared.bin", descriptor_sha256: sha256("descriptors") };
  });
  families.push({ source: "import.proto", source_sha256: sha256("import") });
  put(join(authority, "import.proto"), "import"); put(join(authority, "shared.bin"), "descriptors");
  const manifest = Buffer.from(JSON.stringify({ schema: "acyclic.sdk.rust-authority.v1", authority: "rust", source_revision: "a".repeat(40), families }));
  put(join(authority, "rust-authority.json"), manifest);
  const installation = join(root, "java-qualified"); const cache = join(root, "cache");
  const dependency = join(cache, "sdk-runtime.jar"); put(dependency, "SDK runtime fixture");
  put(join(installation, "installed/sdk-java-transport-0.2.0-alpha.1.jar"), "SDK JAR fixture");
  put(join(installation, "installed/sdk-java-transport-0.2.0-alpha.1.pom"), "SDK POM fixture");
  const proof = { schema: "acyclic.sdk.java.installed-qualification.v1", scope: "installed-java-transport-bindings",
    source_revision: "a".repeat(40), authority_manifest_sha256: sha256(manifest), positive_controls_passed: true, negative_type_controls_rejected: 3,
    installed_jar_sha256: sha256("SDK JAR fixture"), installed_pom_sha256: sha256("SDK POM fixture"), dependency_sha256: { [dependency]: sha256("SDK runtime fixture") } };
  put(join(installation, "qualification.json"), JSON.stringify(proof));
  put(join(cache, "compiler.jar"), "compiler fixture");
  const toolchains = { [language]: { version: "fixture", main_class: "fixture.Compiler", artifacts: [{ path: "compiler.jar", sha256: sha256("compiler fixture") }], runtime: ["compiler.jar"] } };
  const javaHome = join(root, "jdk"), extension = process.platform === "win32" ? ".exe" : "";
  for (const name of [`bin/java${extension}`, `bin/javac${extension}`, "lib/modules", "release"]) put(join(javaHome, name), name);
  const args = { language, installation, authority, cache, "java-home": javaHome, output: join(root, "qualified") };
  const calls = [];
  const command = (exe, argv, options) => {
    calls.push({ exe, argv, options });
    if (argv.includes("fixture.Compiler") && argv.includes("-version")) return { status: 0, stdout: failure === "version" ? "wrong version"
      : language === "kotlin" ? "info: kotlinc-jvm fixture\n" : "Scala compiler version fixture\n" };
    if (argv.includes("-version")) return { status: 0, stderr: exe.includes("javac") ? "javac 17.0.14\n" : 'openjdk version "17.0.14"\n' };
    if (argv.some(name => name.includes("Invalid") && /\.(kt|scala)$/.test(name))) {
      if (failure === "accepted") return { status: 0, stdout: "accepted" };
      if (failure === "unrelated") return { status: 1, stdout: "error: unresolved namespace\n" };
      const type = argv.at(-1).includes("OptionalInteger") ? "Long" : "ByteString";
      return { status: 1, stdout: language === "kotlin" ? `error: argument type mismatch: actual type is 'String', but '${type}' was expected.\n`
        : `Type Mismatch Error\nFound: String\nRequired: ${type}\n1 error found\n` };
    }
    if (argv.includes("InstalledKotlinConsumer") || argv.includes("InstalledScalaConsumer")) {
      const java = "PASS: installed Java descriptors, bytes, unsigned bounds, optional presence, oneof and gRPC shapes";
      const consumer = `PASS: installed ${language === "kotlin" ? "Kotlin" : "Scala"} Java-binding interoperability, bytes, integer bits, presence and oneof`;
      return { status: failure === "positive" ? 1 : 0,
        stdout: failure === "silent" ? "" : failure === "java_marker" ? consumer : failure === "language_marker" ? java : `${java}\n${consumer}\n` };
    }
    return { status: 0, stdout: "" };
  };
  return { args, toolchains, command, calls, put, dependency };
}

for (const language of ["kotlin", "scala"]) {
  test(`${language} runner snapshots pinned compiler, SDK and dependencies and rejects three intended types`, t => {
    const f = fixture(t, language); const result = qualifyJvm(f.args, f);
    assert.equal(result.independent_negative_type_controls_rejected, 3);
    assert.equal(result.rust_backed_rpc_qualified, false);
    const compiles = f.calls.filter(call => call.argv.includes("fixture.Compiler") && call.argv.includes("-d"));
    assert.equal(compiles.length, 4);
    for (const call of compiles) {
      const compiler = call.argv[call.argv.indexOf("-cp") + 1];
      assert.ok(compiler.startsWith(join(f.args.output, "compiler")));
      assert.equal(readFileSync(compiler, "utf8"), "compiler fixture");
      assert.equal(call.options.env.JAVA_HOME, f.args["java-home"]);
    }
    const positive = f.calls.find(call => call.argv.includes(language === "kotlin" ? "InstalledKotlinConsumer" : "InstalledScalaConsumer"));
    assert.ok(positive.argv.includes(`-Dsdk.qualified.jar=${join(f.args.output, "installed/sdk-java-transport-0.2.0-alpha.1.jar")}`));
    for (const path of positive.argv.slice(-3)) assert.equal(readFileSync(path, "utf8"), "descriptors");
    assert.deepEqual(JSON.parse(readFileSync(join(f.args.output, "qualification.json"))), result);
  });
}

test("SDK, authority, compiler and dependency drift are rejected before execution", t => {
  for (const drift of ["SDK", "authority", "compiler", "dependency"]) {
    const f = fixture(t); const path = drift === "SDK" ? join(f.args.installation, "installed/sdk-java-transport-0.2.0-alpha.1.jar")
      : drift === "authority" ? join(f.args.authority, "import.proto") : drift === "compiler" ? join(f.args.cache, "compiler.jar") : f.dependency;
    f.put(path, "drift"); assert.throws(() => qualifyJvm(f.args, f));
    assert.equal(f.calls.length, 0); assert.equal(existsSync(f.args.output), false);
  }
});

test("existing outputs and overlapping input paths are preserved", t => {
  const f = fixture(t); f.put(join(f.args.output, "keep"), "retained");
  assert.throws(() => qualifyJvm(f.args, f), /must be absent/);
  assert.equal(readFileSync(join(f.args.output, "keep"), "utf8"), "retained");
  f.args.output = join(f.args.cache, "new-output"); assert.throws(() => qualifyJvm(f.args, f), /overlaps/); assert.equal(f.calls.length, 0);
});

for (const failure of ["positive", "accepted", "unrelated", "version", "silent", "java_marker", "language_marker"]) {
  test(`${failure} failure cannot produce JVM qualification`, t => {
    const f = fixture(t, "kotlin", failure); assert.throws(() => qualifyJvm(f.args, f));
    assert.equal(existsSync(join(f.args.output, "qualification.json")), false);
  });
}
