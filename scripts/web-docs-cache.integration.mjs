import assert from "node:assert/strict";
import { execFileSync, spawnSync } from "node:child_process";
import { chmodSync, mkdirSync, mkdtempSync, readFileSync, readdirSync, rmSync, statSync, writeFileSync } from "node:fs";
import { tmpdir } from "node:os";
import { join, resolve } from "node:path";
import test from "node:test";
import { webDocsCommands } from "./web-docs-build.mjs";
import { releaseBuild as artifactBuild } from "./release-build.mjs";

test("real project artifacts restore completely and invalidate the full declared closure", async t => {
  const module = process.env.SDK_ARTIFACT_BUILD_MODULE;
  if (!module) throw new Error("SDK_ARTIFACT_BUILD_MODULE must name the shared artifact builder");
  const scratch = mkdtempSync(join(tmpdir(), "webdocs-artifacts-"));
  t.after(() => rmSync(scratch, { recursive: true, force: true }));
  const cwd = join(scratch, "source");
  mkdirSync(cwd);
  const git = args => execFileSync("git", args, { cwd, encoding: "utf8" }).trim();
  git(["init", "-q"]);
  writeFileSync(join(cwd, ".gitignore"), "dist/\ngenerated/\n*.tsbuildinfo\nexternal.d.ts\n");
  writeFileSync(join(cwd, "tsconfig.json"), JSON.stringify({ compilerOptions: {
    composite: true, strict: true, outDir: "dist", tsBuildInfoFile: "tsconfig.tsbuildinfo",
    types: [], target: "ES2022", module: "NodeNext",
  }, files: ["index.ts"] }));
  writeFileSync(join(cwd, "index.ts"), "export const value: number = 1;\n");
  writeFileSync(join(cwd, "main.c"), "int main(void) { return 0; }\n");
  // Generated TS and binary WASM stand alongside real compiler output in one
  // inventory; the compiler and output recovery are never mocked.
  const compiler = resolve("node_modules/typescript/bin/tsc");
  const compile = webDocsCommands("build").at(-1);
  assert.ok(compile);
  const flags = compile.slice(2);
  writeFileSync(join(cwd, "build.mjs"), `import fs from 'node:fs';
import {spawnSync} from 'node:child_process';
fs.mkdirSync('generated', {recursive:true});
fs.writeFileSync('generated/contract.ts', 'export const contract = 1;');
fs.writeFileSync('generated/module.wasm', Buffer.from([0,97,115,109,1,0,0,0]));
process.exitCode = spawnSync(process.execPath, ${JSON.stringify([compiler, ...flags])}, {stdio:'inherit'}).status;
`);
  git(["add", "."]);
  git(["-c", "user.name=Artifact fixture", "-c", "user.email=fixture@example.invalid",
    "-c", "commit.gpgsign=false", "-c", "core.hooksPath=/dev/null", "commit", "-qm", "fixture"]);
  const context = join(scratch, "context.json");
  const declaration = { schema: "artifact-build-context.v1", builder: "sha256:" + "a".repeat(64),
    inputPaths: ["external.d.ts"], environment: ["PATH", "HOME", "WEB_DOCS_CONTROL_FLAG"] };
  const saveContext = () => writeFileSync(context, JSON.stringify(declaration));
  saveContext();
  const external = join(cwd, "external.d.ts");
  writeFileSync(external, "export type External = number;\n");
  const outputs = ["dist", "generated", "tsconfig.tsbuildinfo"];
  /** @type {NodeJS.ProcessEnv} */
  const env = { ...process.env, ACTION_CACHE_ROOT: join(scratch, "store") };
  const options = { id: "sdk/webdocs/real-compiler", command: process.execPath,
    args: ["build.mjs"], cwd, env, outputs, context };
  const observations = [];
  const run = async label => {
    const started = performance.now(), cpu = process.cpuUsage();
    const rc = await artifactBuild(options);
    const used = process.cpuUsage(cpu);
    observations.push({ label, rc, seconds: (performance.now() - started) / 1000,
      controllerCpuSeconds: (used.user + used.system) / 1e6 });
    return rc;
  };
  const entries = () => readdirSync(join(scratch, "store", "entries"), { recursive: true, encoding: "utf8" })
    .filter(file => file.endsWith(".json")).map(file => JSON.parse(readFileSync(join(scratch, "store", "entries", file), "utf8")));
  assert.equal(await run("cold"), 0);
  const emitted = join(cwd, "dist/index.js"), modified = statSync(emitted).mtimeMs;
  assert.equal(await run("unchanged"), 0);
  assert.equal(statSync(emitted).mtimeMs, modified, "matching artifacts keep their content clock");
  rmSync(join(cwd, "dist"), { recursive: true });
  rmSync(join(cwd, "generated"), { recursive: true });
  rmSync(join(cwd, "tsconfig.tsbuildinfo"));
  assert.equal(await run("deleted-complete-output"), 0);
  assert.match(readFileSync(emitted, "utf8"), /value/);
  assert.ok(readFileSync(join(cwd, "tsconfig.tsbuildinfo")).length);
  assert.ok(WebAssembly.validate(readFileSync(join(cwd, "generated/module.wasm"))));
  assert.equal(entries().length, 1, "restoration reused the same artifact closure");
  writeFileSync(join(cwd, "dist/stale.js"), "stale");
  assert.equal(await run("stale-output-pruned"), 0);
  assert.equal(readdirSync(join(cwd, "dist")).includes("stale.js"), false);
  writeFileSync(external, "export type External = string;\n");
  assert.equal(await run("external-input-changed"), 0);
  env.WEB_DOCS_CONTROL_FLAG = "changed";
  assert.equal(await run("environment-changed"), 0);
  declaration.builder = "sha256:" + "b".repeat(64); saveContext();
  assert.equal(await run("builder-changed"), 0);
  assert.equal(entries().length, 4);
  writeFileSync(join(cwd, "index.ts"), "export const value: number = 'bad';\n");
  git(["add", "index.ts"]);
  git(["-c", "user.name=Artifact fixture", "-c", "user.email=fixture@example.invalid",
    "-c", "commit.gpgsign=false", "-c", "core.hooksPath=/dev/null", "commit", "-qm", "invalid type"]);
  assert.notEqual(await run("changed-source-compiler-failure"), 0);
  assert.equal(entries().length, 4, "a failed compilation never records PASS");
  writeFileSync(join(cwd, "index.ts"), "export const value = 1;\n");
  await assert.rejects(() => run("dirty-source"), /admitted clean Git tree/);
  // Missing/corrupt content cannot produce a successful restore.
  git(["checkout", "HEAD~1", "--", "index.ts"]);
  git(["reset", "--hard", "HEAD~1"]);
  const last = entries().find(entry => entry.inputs.builder === declaration.builder);
  const digest = last.outputs["dist/index.js"].sha256;
  const blob = join(scratch, "store/blobs", digest.slice(0, 2), digest);
  chmodSync(blob, 0o644);
  writeFileSync(blob, "corrupt");
  await assert.rejects(() => run("corrupt-artifact"), /ACTION_CACHE_CORRUPT/);
  // Preserve the existing single-output release consumer on the same mechanism.
  const output = join(cwd, "generated/executable");
  const single = { ...options, id: "sdk/webdocs/single-artifact", command: "cc",
    args: ["main.c", "-o", output], outputs: undefined, output };
  assert.equal(await artifactBuild(single), 0);
  assert.equal(spawnSync(output).status, 0);
  rmSync(output);
  assert.equal(await artifactBuild(single), 0);
  assert.equal(spawnSync(output).status, 0);
  console.log("WEB_DOCS_ARTIFACT_CONTROLS " + JSON.stringify({ status: "PASS", observations }));
});
