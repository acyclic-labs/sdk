import assert from "node:assert/strict";
import { spawnSync } from "node:child_process";
import { mkdirSync, mkdtempSync, readFileSync, rmSync, writeFileSync } from "node:fs";
import { tmpdir } from "node:os";
import { join, resolve } from "node:path";
import test from "node:test";
import { writeChanged } from "./generated-bindings.mjs";

test("project builds reject changed dependency types and restore missing emitted output", t => {
  const root = mkdtempSync(join(tmpdir(), "sdk-incremental-"));
  t.after(() => rmSync(root, { recursive: true, force: true }));
  for (const name of ["api", "consumer"]) {
    mkdirSync(join(root, name));
    writeFileSync(join(root, name, "tsconfig.json"), JSON.stringify({
      compilerOptions: { composite: true, outDir: "dist", rootDir: ".", types: [], strict: true,
        target: "ES2022", module: "NodeNext", moduleResolution: "NodeNext" },
      files: ["index.ts"], ...(name === "consumer" ? { references: [{ path: "../api" }] } : {}),
    }));
  }
  writeFileSync(join(root, "tsconfig.json"), JSON.stringify({ files: [], references: [{ path: "consumer" }] }));
  const api = join(root, "api/index.ts");
  const initial = "export const value: number = 1;\n";
  writeChanged(api, initial);
  writeFileSync(join(root, "consumer/index.ts"), "import { value } from '../api';\nexport const result: number = value;\n");
  const scripts = JSON.parse(readFileSync("package.json", "utf8")).scripts;
  const compiler = resolve("node_modules/typescript/bin/tsc");
  // Exercise the maintained build flags and the actual compiler.
  const args = scripts.build.split(" && ").at(-1).split(" ").slice(1);
  const measurements = [];
  const build = (label = "build") => {
    const started = performance.now();
    const compilerArgs = [compiler, ...args, "--pretty", "false"];
    const timed = process.platform === "linux";
    const timing = join(root, "cpu.txt");
    const result = spawnSync(timed ? "/usr/bin/time" : process.execPath,
      timed ? ["-f", "%U %S", "-o", timing, process.execPath, ...compilerArgs] : compilerArgs,
      { cwd: root, encoding: "utf8" });
    const cpu = timed ? readFileSync(timing, "utf8").trim().split("\n").at(-1).split(" ").map(Number) : [];
    measurements.push({ label, seconds: (performance.now() - started) / 1000,
      cpuSeconds: cpu.length ? cpu[0] + cpu[1] : null, exitCode: result.status });
    return result;
  };
  let result = build();
  assert.equal(result.status, 0, result.stdout + result.stderr);
  const output = join(root, "consumer/dist/index.js");
  writeChanged(api, "export const value: string = 'changed';\n");
  result = build();
  assert.notEqual(result.status, 0, "an upstream declaration change must invalidate its consumer");
  assert.match(result.stdout + result.stderr, /not assignable to type 'number'/);
  writeChanged(api, initial);
  result = build("repaired-type");
  assert.equal(result.status, 0, result.stdout + result.stderr);
  rmSync(output);
  result = build("missing-output");
  console.log("WEB_DOCS_INCREMENTAL_OBSERVATIONS " + JSON.stringify({ measurements }));
  assert.equal(result.status, 0, result.stdout + result.stderr);
  assert.match(readFileSync(output, "utf8"), /result/);
  console.log("WEB_DOCS_COMPILER_RESULT " + JSON.stringify({ status: "PASS", measurements }));
});
