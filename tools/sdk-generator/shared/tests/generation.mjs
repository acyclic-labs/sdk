import assert from "node:assert/strict";
import { createHash } from "node:crypto";
import { existsSync, mkdirSync, mkdtempSync, readFileSync, readdirSync, rmSync, writeFileSync } from "node:fs";
import { tmpdir } from "node:os";
import { delimiter, join } from "node:path";
import test from "node:test";

const hash = bytes => createHash("sha256").update(bytes).digest("hex");
export function generatorFixture(t, language, pluginKey) {
  const root = mkdtempSync(join(tmpdir(), `${language.toLowerCase()}-generator-test-`));
  t.after(() => rmSync(root, { recursive: true }));
  const source = join(root, "source");
  const authority = join(root, "authority");
  mkdirSync(source);
  mkdirSync(authority);
  for (const name of ["LICENSE", "NOTICE"]) writeFileSync(join(source, name), name);
  for (const name of ["a.proto", "b.proto", "import.proto"]) writeFileSync(join(authority, name), name);
  writeFileSync(join(authority, "shared.bin"), "attested descriptors");
  const manifest = { schema: "acyclic.sdk.rust-authority.v1", authority: "rust", source_revision: "a".repeat(40),
    families: ["b.proto", "a.proto"].map(source => ({ source, source_sha256: hash(source),
      descriptor: "shared.bin", descriptor_sha256: hash("attested descriptors") })) };
  manifest.families.push({ source: "import.proto", source_sha256: hash("import.proto") });
  const save = () => writeFileSync(join(authority, "rust-authority.json"), JSON.stringify(manifest));
  save();
  const protoc = join(root, "protoc");
  const plugin = join(root, "plugin");
  writeFileSync(protoc, "compiler fixture");
  writeFileSync(plugin, "plugin fixture");
  const toolchain = { protoc_version: "libprotoc 28.3",
    protoc: { [`${process.platform}-${process.arch}`]: { sha256: hash("compiler fixture") } }, [`${pluginKey}_version`]: "fixture",
    [pluginKey]: { [`${process.platform}-${process.arch}`]: { sha256: hash("plugin fixture"), url: "fixture" } } };
  return { root, source, authority, manifest, save, protoc, plugin, toolchain };
}

export function registerGenerationTests(language, generate, { target, pluginArgument, pluginKey, outputFlag, extension, metadata, pluginMismatch }) {
  function fixture(t) {
    const { root, source, authority, manifest, save, protoc, plugin, toolchain } = generatorFixture(t, language, pluginKey);
    const args = { "source-root": source, authority, protoc, [pluginArgument]: plugin, output: join(root, "new-package") };
    const calls = [];
    let snapshot;
    const command = (executable, argv, options) => {
      calls.push(argv);
      assert.equal(executable, protoc);
      if (argv[0] === "--version") return { status: 0, stdout: "libprotoc 28.3\n" };
      snapshot = options.cwd;
      const descriptors = argv[0].slice("--descriptor_set_in=".length).split(delimiter);
      assert.equal(descriptors.length, 1);
      assert.equal(readFileSync(descriptors[0], "utf8"), "attested descriptors");
      assert.equal(readdirSync(snapshot).filter(name => name.endsWith(".proto")).length, 0);
      const output = argv.find(arg => arg.startsWith(outputFlag)).slice(outputFlag.length);
      writeFileSync(join(output, `${argv.at(-1)}${extension}`), `generated ${argv.at(-1)}`);
      return { status: 0, stdout: "", stderr: "" };
    };
    return { root, args, manifest, save, toolchain, command, calls, snapshot: () => snapshot };
  }

  test(`${language}: each family uses only verified descriptor snapshots and has inventoried output`, t => {
    const f = fixture(t);
    const receipt = generate(f.args, { command: f.command, toolchain: f.toolchain });
    assert.equal(receipt.schema, `acyclic.sdk.${target}-producer-receipt.v1`);
    assert.equal(receipt.target, target);
    assert.equal(receipt[`${pluginKey}_version`], f.toolchain[`${pluginKey}_version`]);
    assert.equal(receipt[`${pluginKey}_coordinate`], "fixture");
    assert.deepEqual(receipt.tool_sha256, { protoc: hash("compiler fixture"), [pluginKey]: hash("plugin fixture") });
    assert.equal(receipt.generator_sha256, hash(readFileSync(new URL(`../../backends/${target}/src/generate.mjs`, import.meta.url))));
    assert.equal(receipt.authority_reader_sha256, hash(readFileSync(new URL("../authority.mjs", import.meta.url))));
    assert.deepEqual(f.calls.slice(1).map(argv => argv.at(-1)), ["a.proto", "b.proto", "import.proto"]);
    assert.equal(existsSync(f.snapshot()), false);
    assert.equal(receipt.source_revision, f.manifest.source_revision);
    assert.equal(receipt.authority_manifest_sha256, hash(readFileSync(join(f.args.authority, "rust-authority.json"))));
    for (const name of receipt.outputs) assert.equal(receipt.output_sha256[name], hash(readFileSync(join(f.args.output, name))));
    for (const name of metadata) assert.ok(receipt.outputs.includes(name));
    assert.deepEqual(JSON.parse(readFileSync(join(f.args.output, "generation-receipt.json"))), receipt);
  });

  test(`${language}: unsafe, duplicate, missing and drifting authority inputs fail before tool execution`, t => {
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

  test(`${language}: existing and overlapping output paths are preserved`, t => {
    const f = fixture(t);
    mkdirSync(f.args.output);
    writeFileSync(join(f.args.output, "keep"), "caller bytes");
    assert.throws(() => generate(f.args, { command: f.command, toolchain: f.toolchain }), /absent/);
    assert.equal(readFileSync(join(f.args.output, "keep"), "utf8"), "caller bytes");
    f.args.output = join(f.args.authority, "new-package");
    assert.throws(() => generate(f.args, { command: f.command, toolchain: f.toolchain }), /overlaps/);
    assert.equal(f.calls.length, 0);
  });

  test(`${language}: plugin drift and compiler version mismatch cannot create output`, t => {
    const f = fixture(t);
    writeFileSync(f.args[pluginArgument], "different plugin");
    assert.throws(() => generate(f.args, { command: f.command, toolchain: f.toolchain }), pluginMismatch);
    assert.equal(existsSync(f.args.output), false);
    writeFileSync(f.args[pluginArgument], "plugin fixture");
    writeFileSync(f.args.protoc, "compiler drift with same version");
    assert.throws(() => generate(f.args, { command: f.command, toolchain: f.toolchain }), /compiler executable differs/);
    assert.equal(f.calls.length, 0);
    assert.equal(existsSync(f.args.output), false);
    writeFileSync(f.args.protoc, "compiler fixture");
    assert.throws(() => generate(f.args, { command: () => ({ status: 0, stdout: "libprotoc wrong" }), toolchain: f.toolchain }), /protoc version/);
    assert.equal(existsSync(f.args.output), false);
  });

  test(`${language}: failed generation retains partial output but cannot write a receipt`, t => {
    const f = fixture(t);
    const command = (exe, argv, options) => argv[0] === "--version" ? f.command(exe, argv, options)
      : { status: 1, stderr: "compiler failure" };
    assert.throws(() => generate(f.args, { command, toolchain: f.toolchain }), /generation failed/);
    assert.equal(existsSync(f.args.output), true);
    assert.equal(existsSync(join(f.args.output, "generation-receipt.json")), false);
  });

  test(`${language}: missing family bindings and output collisions cannot produce success`, t => {
    for (const collision of [false, true]) {
      const f = fixture(t);
      const command = (exe, argv, options) => {
        if (argv[0] === "--version") return f.command(exe, argv, options);
        if (collision) {
          const output = argv.find(arg => arg.startsWith(outputFlag)).slice(outputFlag.length);
          writeFileSync(join(output, `Same${extension}`), "same class name");
        }
        return { status: 0, stdout: "", stderr: "" };
      };
      assert.throws(() => generate(f.args, { command, toolchain: f.toolchain }), collision ? /collide/ : new RegExp(`no ${language} bindings`));
      assert.equal(existsSync(join(f.args.output, "generation-receipt.json")), false);
    }
  });
}
