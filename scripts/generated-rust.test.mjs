import assert from "node:assert/strict";
import { readFileSync } from "node:fs";
import { posix } from "node:path";
import { runInNewContext } from "node:vm";
import { spawnSync } from "node:child_process";
import test from "node:test";
import YAML from "./vendor/yaml.cjs";
import fc from "fast-check";
import { generationTypeScript, normalizeGeneratedRust, normalizeGeneratedTypeScript, packagedRustDirectories, rustBindingFiles } from "./generated-bindings.mjs";

test("metadata consumers import without installing generator dependencies", () => {
  const module = new URL("./generated-bindings.mjs", import.meta.url).href;
  const script = `import { registerHooks } from "node:module";
    registerHooks({ resolve(name, context, next) {
      if (!name.startsWith("node:") && !name.startsWith("file:")) throw new Error("unexpected dependency: " + name);
      return next(name, context);
    }});
    const { compatibilityArtifacts } = await import(${JSON.stringify(module)});
    if (!compatibilityArtifacts.objects) throw new Error("metadata missing");`;
  const result = spawnSync(process.execPath, ["--input-type=module", "-e", script], { encoding: "utf8" });
  assert.equal(result.status, 0, result.stderr);
});

test("bounded TypeScript reflow preserves syntax, literals and repeated normalization", () => {
  const ts = generationTypeScript();
  const printer = ts.createPrinter({ removeComments: true });
  const parse = source => {
    const file = ts.createSourceFile("generated.ts", source, ts.ScriptTarget.Latest, true);
    assert.deepEqual(file.parseDiagnostics, []);
    return printer.printFile(file);
  };
  fc.assert(fc.property(fc.string(), fc.integer({ min: 100, max: 140 }), (value, count) => {
    const source = `// @generated\nexport const text = ${JSON.stringify(value)};\nexport const regexp = /a  b\\/\\*/;\nexport const template = \`a  b ${"${1 + 2}"}\`;\n`
      + Array.from({ length: count }, (_, index) => `/**\n * Message ${index}.\n */\nexport interface Message${index} {\n /**\n  * Payload.\n  */\n value: string;\n}\n`).join("");
    const normalized = normalizeGeneratedTypeScript(source);
    assert.equal(parse(normalized), parse(source));
    assert.equal(normalizeGeneratedTypeScript(normalized), normalized);
    assert.ok(normalized.split("\n").length < 800);
  }), { numRuns: 32 });
});

test("PRs and main pushes check source generation before any rewriting", () => {
  const workflow = YAML.parse(readFileSync(".github/workflows/static.yml", "utf8"));
  assert.ok(Object.hasOwn(workflow.on, "pull_request"));
  assert.ok(workflow.on.push.branches.includes("main"));
  const job = workflow.jobs.generated;
  assert.equal(job.if, undefined);
  assert.equal(job.needs, undefined);
  assert.ok(job.steps.every(step => step.if === undefined));
  const commands = job.steps.map(step => step.run ?? "").join("\n");
  assert.match(commands, /bun scripts\/check-generated\.mjs --source-only/);
  assert.doesNotMatch(commands, /(?:sync-generated|scripts\/generate\.mjs|bun run generate)/);
});

test("the actual Rust generation guard rejects every changed, missing or extra fragment", () => {
  const source = readFileSync("scripts/check-generated.mjs", "utf8");
  const start = source.indexOf("  for (const [relative, packaged] of packagedRustDirectories) {");
  const end = source.indexOf("  for (const [source, destination] of generatedDescriptors)", start);
  assert.ok(start >= 0 && end > start);
  const guard = source.slice(start, end);
  const contents = new Map();
  const inventories = new Map();
  for (const [relative, packaged] of packagedRustDirectories) {
    const files = rustBindingFiles(packaged);
    inventories.set(`fresh/${relative}`, files);
    inventories.set(packaged, files);
    for (const file of files) {
      const text = readFileSync(`${packaged}/${file}`, "utf8");
      contents.set(`fresh/${relative}/${file}`, text);
      contents.set(`${packaged}/${file}`, text);
    }
  }
  const check = () => runInNewContext(guard, {
    root: ".", freshRust: "fresh", join: posix.join, packagedRustDirectories, normalizeGeneratedRust,
    readFileSync: path => contents.get(path), rustBindingFiles: directory => inventories.get(directory),
  });
  assert.doesNotThrow(check);
  for (const [, packaged] of packagedRustDirectories) {
    const files = inventories.get(packaged);
    assert.ok(files.length > 1);
    for (const file of files) {
      const path = `${packaged}/${file}`;
      const text = contents.get(path);
      contents.set(path, `${text} `);
      assert.throws(check, { message: `packaged Rust drift: ${path}` });
      contents.set(path, text);
      inventories.set(packaged, files.filter(name => name !== file));
      assert.throws(check, { message: `packaged Rust file set drift: ${packaged}` });
      inventories.set(packaged, files);
    }
    inventories.set(packaged, [...files, "stale.rs"].sort());
    assert.throws(check, { message: `packaged Rust file set drift: ${packaged}` });
    inventories.set(packaged, files);
  }
  assert.doesNotThrow(check);
});
