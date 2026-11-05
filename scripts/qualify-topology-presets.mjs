#!/usr/bin/env node
// Authored SDK conversations exercise the shipping role loader and agent tools.
// Select prebuilt executables from Cargo JSON with graphcoder-e2e/artifacts.mjs.
import assert from "node:assert/strict";
import { spawnSync } from "node:child_process";
import { chmodSync, copyFileSync, cpSync, mkdirSync, readFileSync, writeFileSync } from "node:fs";
import { isAbsolute, join } from "node:path";
import { fileURLToPath } from "node:url";

const [binary, mock, output] = process.argv.slice(2);
const recovery = "node scripts/qualify-topology-presets.mjs GRAPHCODER MOCK_MODEL FRESH_ABSOLUTE_OUTPUT";
assert.ok(binary && mock && output && [binary, mock, output].every(isAbsolute),
  `step=select topology executables and evidence; recovery=${recovery}`);
const root = fileURLToPath(new URL("../", import.meta.url));
mkdirSync(output);
const input = join(output, "input");
mkdirSync(input);
cpSync(join(root, "scripts/graphcoder-e2e/cases"), join(input, "cases"), { recursive: true });
copyFileSync(join(root, "scripts/graphcoder-e2e/coverage.json"), join(input, "coverage.json"));
const id = "topology-blackboard", path = join(input, "cases", `${id}.json`);
chmodSync(path, 0o600);
const original = JSON.parse(readFileSync(path));
assert.equal(original.model.steps[0].tools[0].arguments.role, "blackboard",
  `step=locate original blackboard admission; recovery=${recovery}`);
const stale = original.model.steps.flatMap(step => step.tools ?? []).filter(tool => tool.id === "stale-board");
assert.equal(stale.length, 1, `step=locate original stale edit; recovery=${recovery}`);
assert.ok(stale[0].arguments.version, `step=require original version condition; recovery=${recovery}`);
// A killed qualification can leave only its own input tree modified.
for (const phase of ["generic-worker-control", "private-volume-control", "unconditional-edit-control", "selected-role"]) {
  const negative = phase !== "selected-role", inputCase = structuredClone(original);
  if (phase === "generic-worker-control") inputCase.model.steps[0].tools[0].arguments.role = "worker";
  for (const step of inputCase.model.steps) for (const tool of step.tools ?? []) {
    if (phase === "private-volume-control" && tool.arguments.path === ".volumes/shared/board.md")
      tool.arguments.path = ".volumes/home/board.md";
    if (phase === "unconditional-edit-control" && tool.id === "stale-board") delete tool.arguments.version;
  }
  writeFileSync(path, JSON.stringify(inputCase));
  const destination = join(output, phase);
  const result = spawnSync(process.execPath,
    [join(root, "scripts/graphcoder-e2e/run.mjs"), binary, mock, id],
    { cwd: root, encoding: "utf8", maxBuffer: Infinity,
      env: { ...process.env, E2E_OUTPUT: destination, E2E_CASES_ROOT: input } });
  writeFileSync(`${destination}.stdout`, result.stdout ?? "");
  writeFileSync(`${destination}.stderr`, result.stderr ?? "");
  assert.ok(!result.error && result.signal === null,
    `step=run ${phase}; cause=${result.error ?? result.signal}; recovery=${recovery}`);
  const report = JSON.parse(readFileSync(join(destination, "matrix.json")));
  for (const value of report.results) if (value.error) console.error(`${phase}: ${value.error}`);
  assert.deepEqual(report.results.map(value => value.case), [id],
    `step=retain every topology result; recovery=${recovery}`);
  assert.equal(result.status, negative ? 1 : 0,
    `step=qualify ${phase}; evidence=${destination}; recovery=${recovery}`);
  for (const value of report.results) {
    assert.equal(value.status, negative ? "fail" : "pass",
      `step=qualify ${value.case}; evidence=${destination}; recovery=${recovery}`);
    if (negative) assert.ok(value.evidence.requests.some(request => request.status === 409),
      `step=observe ${phase} at original provider request; recovery=${recovery}`);
  }
  console.log(JSON.stringify({ phase, case: id, seconds: report.seconds,
    caseSeconds: report.results[0].seconds, status: "pass" }));
}
