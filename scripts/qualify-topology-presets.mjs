#!/usr/bin/env node
// Authored SDK conversations exercise the shipping role loader and agent tools.
// Select prebuilt executables from Cargo JSON with graphcoder-e2e/artifacts.mjs.
import assert from "node:assert/strict";
import { spawnSync } from "node:child_process";
import { chmodSync, copyFileSync, cpSync, mkdirSync, readFileSync, writeFileSync } from "node:fs";
import { isAbsolute, join } from "node:path";
import { fileURLToPath } from "node:url";

const [binary, mock, output, selection, ...extra] = process.argv.slice(2);
const nestedOnly = selection === "--nested";
const recovery = "node scripts/qualify-topology-presets.mjs GRAPHCODER MOCK_MODEL FRESH_ABSOLUTE_OUTPUT [--nested]";
assert.ok(binary && mock && output && [binary, mock, output].every(isAbsolute),
  `step=select topology executables and evidence; recovery=${recovery}`);
assert.ok((selection === undefined || nestedOnly) && extra.length === 0,
  `step=select topology qualification; recovery=${recovery}`);
const root = fileURLToPath(new URL("../", import.meta.url));
mkdirSync(output);
const input = join(output, "input");
mkdirSync(input);
cpSync(join(root, "scripts/graphcoder-e2e/cases"), join(input, "cases"), { recursive: true });
copyFileSync(join(root, "scripts/graphcoder-e2e/coverage.json"), join(input, "coverage.json"));
// A killed qualification can leave only its own input tree modified.
for (const role of nestedOnly ? ["nested"] : ["blackboard", "hierarchy", "nested"]) {
  const nested = role === "nested";
  const id = nested ? "nested-children" : `topology-${role}`;
  const path = join(input, "cases", `${id}.json`);
  chmodSync(path, 0o600);
  const original = JSON.parse(readFileSync(path));
  if (!nested) assert.equal(original.model.steps[0].tools[0].arguments.role, role,
    `step=locate original ${role} admission; recovery=${recovery}`);
  mkdirSync(join(output, id));
  const controls = nested ? ["obsolete-wait-control"] : role === "blackboard"
    ? ["private-volume-control", "unconditional-edit-control"]
    : ["leaf-result-control", "review-result-control"];
  const positive = nested ? "selected-contract" : "selected-role";
  for (const phase of [...(nested ? [] : ["generic-worker-control"]), ...controls, positive]) {
    const negative = phase !== positive, inputCase = structuredClone(original);
    let changed = 0;
    if (phase === "generic-worker-control") {
      inputCase.model.steps[0].tools[0].arguments.role = "worker";
      changed++;
    }
    for (const step of inputCase.model.steps) {
      if ((phase === "leaf-result-control" && step.when.user === "HIERARCHY_LEAF") ||
          (phase === "review-result-control" && step.when.user === "HIERARCHY_REVIEW LEAF_ORIGINAL_EVIDENCE")) {
        assert.equal(step.text.length, 1, `step=locate original contribution; recovery=${recovery}`);
        step.text[0] = "SUBSTITUTED_CONTRIBUTION";
        changed++;
      }
      for (const tool of step.tools ?? []) {
        if (phase === "obsolete-wait-control" && tool.name === "wait") {
          assert.equal(tool.arguments.mode, undefined,
            `step=locate canonical recursive wait; recovery=${recovery}`);
          tool.arguments.mode = "all";
          changed++;
        }
        if (phase === "private-volume-control" && tool.arguments.path === ".volumes/shared/board.md") {
          tool.arguments.path = ".volumes/home/board.md";
          changed++;
        }
        if (phase === "unconditional-edit-control" && tool.id === "stale-board") {
          assert.ok(tool.arguments.version, `step=require original version condition; recovery=${recovery}`);
          delete tool.arguments.version;
          changed++;
        }
      }
    }
    assert.equal(changed > 0, negative, `step=plant ${phase}; recovery=${recovery}`);
    if (phase === "obsolete-wait-control") assert.equal(changed, 2,
      `step=restore both original recursive waits; recovery=${recovery}`);
    writeFileSync(path, JSON.stringify(inputCase));
    const destination = join(output, id, phase);
    const result = spawnSync(process.execPath,
      [join(root, "scripts/graphcoder-e2e/run.mjs"), binary, mock, id],
      { cwd: root, encoding: "utf8", maxBuffer: Infinity,
        env: { ...process.env, E2E_OUTPUT: destination, E2E_CASES_ROOT: input } });
    writeFileSync(`${destination}.stdout`, result.stdout ?? "");
    writeFileSync(`${destination}.stderr`, result.stderr ?? "");
    assert.ok(!result.error && result.signal === null,
      `step=run ${phase}; cause=${result.error ?? result.signal}; recovery=${recovery}`);
    const report = JSON.parse(readFileSync(join(destination, "matrix.json")));
    for (const value of report.results) if (value.error) console.error(`${id}/${phase}: ${value.error}`);
    assert.deepEqual(report.results.map(value => value.case), [id],
      `step=retain every topology result; recovery=${recovery}`);
    assert.equal(result.status, negative ? 1 : 0,
      `step=qualify ${phase}; evidence=${destination}; recovery=${recovery}`);
    for (const value of report.results) {
      assert.equal(value.status, negative ? "fail" : "pass",
        `step=qualify ${value.case}; evidence=${destination}; recovery=${recovery}`);
      if (negative) {
        const rejected = nested
          ? value.error.includes("generation count") && value.error.includes("3 !== 7")
          : value.evidence.requests.some(request => request.status === 409);
        assert.ok(rejected, `step=observe ${phase} at original provider request; recovery=${recovery}`);
      }
    }
    console.log(JSON.stringify({ phase, case: id, seconds: report.seconds,
      caseSeconds: report.results[0].seconds, status: "pass" }));
  }
}
