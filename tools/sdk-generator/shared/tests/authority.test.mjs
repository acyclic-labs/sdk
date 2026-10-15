import assert from "node:assert/strict";
import { spawnSync } from "node:child_process";
import { existsSync, mkdirSync, mkdtempSync, readFileSync, readdirSync, rmSync, symlinkSync, writeFileSync } from "node:fs";
import { tmpdir } from "node:os";
import { basename, join } from "node:path";
import { pathToFileURL } from "node:url";
import test from "node:test";
import { copyGeneratedFiles, equalEntries, generationOutput, inventory, qualificationOutput, sourceInventory, stageDescriptorSets, verifyInstalledPayload, verifyPackageOutputs } from "../authority.mjs";

test("CLI executes only as the main module and preserves required string arguments", t => {
  const root = mkdtempSync(join(tmpdir(), "sdk generator cli "));
  t.after(() => rmSync(root, { recursive: true, force: true }));
  const file = join(root, "generator.mjs");
  const authority = new URL("../authority.mjs", import.meta.url).href;
  writeFileSync(file, `import { runCli } from ${JSON.stringify(authority)};\n` +
    'runCli(import.meta.url, values => values, ["input", "output"]);\n');
  const run = argv => spawnSync(process.execPath, argv, { encoding: "utf8", timeout: 60_000 });
  const direct = run([file, "--input", "source with spaces", "--output", "destination"]);
  assert.equal(direct.status, 0, direct.stderr);
  assert.deepEqual(JSON.parse(direct.stdout), { input: "source with spaces", output: "destination" });
  const imported = run(["--input-type=module", "-e", `await import(${JSON.stringify(pathToFileURL(file).href)});`]);
  assert.equal(imported.status, 0, imported.stderr);
  assert.equal(imported.stdout, "");
  const missing = run([file, "--input", "source"]);
  assert.equal(missing.status, 1);
  assert.match(missing.stderr, /--input and --output are required/);
  const qualifier = join(root, "qualifier.mjs");
  writeFileSync(qualifier, `import { runCli } from ${JSON.stringify(authority)};\n` +
    'runCli(import.meta.url, values => { console.error("progress"); return { path: values.output }; }, ["input", "output"], "qualification inputs required");\n');
  const qualified = run([qualifier, "--input", "source", "--output", "destination"]);
  assert.equal(qualified.status, 0, qualified.stderr);
  assert.deepEqual(JSON.parse(qualified.stdout), { path: "destination" });
  assert.equal(qualified.stderr.trim(), "progress");
  const incomplete = run([qualifier, "--input", "source"]);
  assert.equal(incomplete.status, 1);
  assert.match(incomplete.stderr, /qualification inputs required/);
  assert.doesNotMatch(incomplete.stderr, /progress|--input and --output are required/);
  const unknown = run([file, "--unrecognized", "value"]);
  assert.equal(unknown.status, 1);
  assert.match(unknown.stderr, /Unknown option/);
});

for (const { name, verify, overlap } of [
  { name: "generation", verify: generationOutput, overlap: /overlaps protected input/ },
  { name: "qualification", verify: qualificationOutput, overlap: /qualification output overlaps an input/ },
]) {
  test(`${name} output ownership refuses existing paths and overlapping inputs without creating files`, t => {
    const root = mkdtempSync(join(tmpdir(), "sdk-output-ownership-"));
    t.after(() => rmSync(root, { recursive: true, force: true }));
    const source = join(root, "source"), output = join(root, "output");
    mkdirSync(source);
    assert.equal(verify(output, [source]), output);
    assert.equal(existsSync(output), false);
    assert.throws(() => verify(source, []), /must be absent/);
    assert.throws(() => verify(join(source, "nested"), [source]), overlap);
    assert.equal(existsSync(join(source, "nested")), false);
    assert.throws(() => verify(output, [join(output, "protected")]), overlap);
    assert.equal(existsSync(output), false);
    writeFileSync(output, "owned by caller");
    assert.throws(() => verify(output, []), /must be absent/);
    assert.equal(readFileSync(output, "utf8"), "owned by caller");
  });
}

test("descriptor staging deduplicates shared sets and orders their exact admitted bytes", t => {
  const root = mkdtempSync(join(tmpdir(), "sdk-descriptor-staging-"));
  t.after(() => rmSync(root, { recursive: true, force: true }));
  const inputs = new Map([["z.bin", Buffer.from("last")], ["a.bin", Buffer.from("first")]]);
  const manifest = { families: [
    { descriptor: "z.bin", descriptor_sha256: "z" },
    { descriptor: "a.bin", descriptor_sha256: "a" },
    { descriptor: "z.bin", descriptor_sha256: "z" },
    { source: "import.proto" },
  ] };
  const paths = stageDescriptorSets(manifest, inputs, root);
  assert.deepEqual(paths.map(path => basename(path)), ["a.bin", "z.bin"]);
  assert.deepEqual(paths.map(path => readFileSync(path, "utf8")), ["first", "last"]);
  assert.deepEqual(readdirSync(root).sort(), ["a.bin", "z.bin"]);
  assert.throws(() => stageDescriptorSets(manifest, inputs, root), { code: "EEXIST" });
  assert.equal(readFileSync(join(root, "a.bin"), "utf8"), "first");
});

test("generated output copying preserves nested bytes and rejects cross-family collisions", t => {
  const root = mkdtempSync(join(tmpdir(), "sdk-output-copy-"));
  t.after(() => rmSync(root, { recursive: true, force: true }));
  const source = join(root, "source"), output = join(root, "output");
  mkdirSync(join(source, "nested"), { recursive: true });
  mkdirSync(output);
  const bytes = Buffer.from([0, 255, 13, 10]);
  writeFileSync(join(source, "nested", "binding.rb"), bytes);
  const emitted = new Set();
  copyGeneratedFiles(source, output, inventory(source), emitted);
  assert.deepEqual(readFileSync(join(output, "nested", "binding.rb")), bytes);
  assert.deepEqual([...emitted], ["nested/binding.rb"]);
  assert.throws(() => copyGeneratedFiles(source, output, inventory(source), emitted), /outputs collide/);
  assert.throws(() => copyGeneratedFiles(source, output, inventory(source), new Set()), { code: "EEXIST" });
  assert.deepEqual(readFileSync(join(output, "nested", "binding.rb")), bytes);
});

test("shared file inventory keeps sorted relative paths and rejects links", t => {
  const root = mkdtempSync(join(tmpdir(), "sdk-file-inventory-"));
  t.after(() => rmSync(root, { recursive: true, force: true }));
  mkdirSync(join(root, "nested"));
  writeFileSync(join(root, "z"), "last");
  writeFileSync(join(root, "nested", "a"), "first");
  assert.deepEqual(inventory(root), ["nested/a", "z"]);
  assert.deepEqual(inventory(root, "nested"), ["nested/a"]);
  symlinkSync(join(root, "nested"), join(root, "link"), process.platform === "win32" ? "junction" : "dir");
  assert.throws(() => inventory(root), /contains a link/);
});

test("source inventory rejects symlinked roots as well as links inside the tree", t => {
  const root = mkdtempSync(join(tmpdir(), "sdk-source-inventory-"));
  t.after(() => rmSync(root, { recursive: true, force: true }));
  const source = join(root, "source"), alias = join(root, "alias");
  mkdirSync(join(source, "nested"), { recursive: true });
  writeFileSync(join(source, "nested", "unit.cc"), "source");
  assert.deepEqual(sourceInventory(source), ["nested/unit.cc"]);
  assert.deepEqual(sourceInventory(source, "nested"), ["nested/unit.cc"]);
  assert.throws(() => sourceInventory(source, "nested/unit.cc"), /directory kind differs/);
  const kind = process.platform === "win32" ? "junction" : "dir";
  symlinkSync(source, alias, kind);
  assert.throws(() => sourceInventory(alias), /directory kind differs/);
  symlinkSync(join(source, "nested"), join(source, "linked"), kind);
  assert.throws(() => sourceInventory(source), /contains a link/);
});

test("installed payload verification rejects missing, extra and changed files", t => {
  const root = mkdtempSync(join(tmpdir(), "sdk-installed-payload-"));
  t.after(() => rmSync(root, { recursive: true, force: true }));
  mkdirSync(join(root, "nested"));
  const payload = new Map([["nested/b", Buffer.from("second")], ["a", Buffer.from("first")]]);
  writeFileSync(join(root, "a"), "first");
  assert.throws(() => verifyInstalledPayload(root, payload), /installed payload inventory differs/);
  writeFileSync(join(root, "nested/b"), "second");
  assert.doesNotThrow(() => verifyInstalledPayload(root, payload));
  writeFileSync(join(root, "extra"), "unexpected");
  assert.throws(() => verifyInstalledPayload(root, payload), /installed payload inventory differs/);
  rmSync(join(root, "extra"));
  writeFileSync(join(root, "nested/b"), "changed");
  assert.throws(() => verifyInstalledPayload(root, payload), /authority input digest mismatch/);
});

test("receipt entry equality ignores key insertion order but retains every key and value", () => {
  assert.equal(equalEntries({ b: "two", a: "one" }, { a: "one", b: "two" }), true);
  assert.equal(equalEntries({ a: "one" }, { a: "changed" }), false);
  assert.equal(equalEntries({ a: "one" }, { a: "one", b: "two" }), false);
  assert.equal(equalEntries({ a: "one" }, { b: "one" }), false);
});

test("package output admission retains exact names, folded uniqueness and digest inventory", () => {
  const digest = "a".repeat(64), required = new Set(["a", "nested/b"]);
  const digests = { "nested/b": digest, a: digest };
  const verify = (outputs, hashes = digests, allowed = required) =>
    verifyPackageOutputs(outputs, hashes, allowed, "backend output diagnostic");
  assert.doesNotThrow(() => verify(["a", "nested/b"]));
  assert.doesNotThrow(() => verify(["nested/b", "a"]));
  for (const outputs of [["a", "a"], ["a", "other"], [null, "nested/b"]]) {
    assert.throws(() => verify(outputs), /^Error: backend output diagnostic$/);
  }
  assert.throws(() => verify(["../escape"], { "../escape": digest }, new Set(["../escape"])), /backend output diagnostic/);
  assert.throws(() => verify(["a", "A"], { a: digest, A: digest }, new Set(["a", "A"])), /backend output diagnostic/);
  assert.throws(() => verify(["a"]), /package output inventory incomplete/);
  for (const hashes of [
    { a: digest }, { ...digests, extra: digest }, { ...digests, a: "g".repeat(64) },
    { ...digests, a: "a".repeat(63) }, { ...digests, a: undefined },
  ]) assert.throws(() => verify(["a", "nested/b"], hashes), /package digest inventory differs/);
});
