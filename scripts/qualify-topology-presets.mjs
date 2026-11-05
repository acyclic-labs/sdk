#!/usr/bin/env node
// Authored SDK conversations exercise the shipping role loader and agent tools.
// Select prebuilt executables from Cargo JSON with graphcoder-e2e/artifacts.mjs.
import assert from "node:assert/strict";
import { spawnSync } from "node:child_process";
import { mkdirSync, readFileSync, writeFileSync } from "node:fs";
import { isAbsolute, join } from "node:path";
import { fileURLToPath } from "node:url";

const [binary, mock, output] = process.argv.slice(2);
const recovery = "node scripts/qualify-topology-presets.mjs GRAPHCODER MOCK_MODEL FRESH_ABSOLUTE_OUTPUT";
assert.ok(binary && mock && output && [binary, mock, output].every(isAbsolute),
  `step=select topology executables and evidence; recovery=${recovery}`);
const root = fileURLToPath(new URL("../", import.meta.url));
const roles = ["blackboard"];
const originals = roles.map(role => {
  const path = join(root, "scripts/graphcoder-e2e/cases", `topology-${role}.json`);
  return { role, path, bytes: readFileSync(path) };
});
mkdirSync(output);
const groups = process.platform === "win32" ? roles.map(role => [role]) : [roles];
try {
  // Restore the former generic-worker selection. The unchanged conversation
  // requires the selected role directive before it can dispatch any member.
  for (const { role, path, bytes } of originals) {
    const test = JSON.parse(bytes);
    assert.equal(test.model.steps[0].tools[0].arguments.role, role,
      `step=locate original ${role} admission; recovery=${recovery}`);
    test.model.steps[0].tools[0].arguments.role = "worker";
    writeFileSync(path, JSON.stringify(test, null, 2) + "\n");
  }
  for (const phase of ["generic-worker-control", "selected-role"]) {
    if (phase === "selected-role")
      for (const { path, bytes } of originals) writeFileSync(path, bytes);
    for (const group of groups) {
      const destination = join(output, `${phase}-${group.join("-")}`);
      const ids = group.map(role => `topology-${role}`);
      const result = spawnSync(process.execPath,
        [join(root, "scripts/graphcoder-e2e/run.mjs"), binary, mock, ...ids],
        { cwd: root, encoding: "utf8", maxBuffer: Infinity,
          env: { ...process.env, E2E_OUTPUT: destination } });
      writeFileSync(`${destination}.stdout`, result.stdout ?? "");
      writeFileSync(`${destination}.stderr`, result.stderr ?? "");
      assert.ok(!result.error && result.signal === null,
        `step=run ${phase}; cause=${result.error ?? result.signal}; recovery=${recovery}`);
      const report = JSON.parse(readFileSync(join(destination, "matrix.json")));
      assert.deepEqual(report.results.map(value => value.case).sort(), [...ids].sort(),
        `step=retain every topology result; recovery=${recovery}`);
      const negative = phase === "generic-worker-control";
      assert.equal(result.status, negative ? 1 : 0,
        `step=qualify ${phase}; evidence=${destination}; recovery=${recovery}`);
      for (const value of report.results) {
        assert.equal(value.status, negative ? "fail" : "pass",
          `step=qualify ${value.case}; evidence=${destination}; recovery=${recovery}`);
        if (negative) assert.ok(value.evidence.requests.some(request => request.status === 409),
          `step=observe missing role directive at original provider request; recovery=${recovery}`);
      }
      console.log(JSON.stringify({ phase, cases: ids, seconds: report.seconds, status: "pass" }));
    }
  }
} finally {
  for (const { path, bytes } of originals) writeFileSync(path, bytes);
}
