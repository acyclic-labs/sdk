#!/usr/bin/env node
// Real GraphCoder + recorded HTTP model, including controls that select a different policy.
import assert from "node:assert/strict";
import { spawnSync } from "node:child_process";
import { copyFileSync, cpSync, mkdirSync, readFileSync, writeFileSync } from "node:fs";
import { isAbsolute, join } from "node:path";
import { fileURLToPath } from "node:url";

const [binary, mock, output] = process.argv.slice(2);
assert.ok(binary && mock && output && [binary, mock, output].every(isAbsolute),
  "usage: node scripts/qualify-topology-modes.mjs GRAPHCODER MOCK FRESH_OUTPUT");
const root = fileURLToPath(new URL("../", import.meta.url));
mkdirSync(output);
const input = join(output, "input");
mkdirSync(input);
cpSync(join(root, "scripts/graphcoder-e2e/cases"), join(input, "cases"), { recursive: true });
copyFileSync(join(root, "scripts/graphcoder-e2e/coverage.json"), join(input, "coverage.json"));

const tool = (id, name, args) => ({ id, name, arguments: args });
const step = (user, result, tools, text) => ({ when: { user, ...(result ? { tool_result: result } : {}) },
  ...(tools ? { tools } : { text: [text] }) });
// One root emits 257 siblings; its first child descends through depth 65.
// The resulting requests exercise native admission and persistent checkpoints.
const width = 257, depth = 65;
const steps = [step("SWARM_ROOT", null, Array.from({ length: width }, (_, n) =>
  tool(`spawn-${n}`, "spawn", { name: `worker-${n}`, task: `SWARM_UNIT_${n}_TASK`, role: "worker" }))),
  step("SWARM_ROOT", "agent", [tool("wait-workers", "wait", { agents: Array.from({ length: width }, (_, n) => `worker-${n}`) })]),
  step("SWARM_ROOT", "SWARM_CHAIN_DONE", null, "SWARM_ALL_DONE")];
for (let n = 1; n < width; n++) steps.push(step(`SWARM_UNIT_${n}_TASK`, null, null, `SIBLING_${n}_DONE`));
for (let n = 1; n <= depth; n++) {
  const user = n === 1 ? "SWARM_UNIT_0_TASK" : `SWARM_DEPTH_${n}_TASK`;
  if (n === depth) steps.push(step(user, null, null, "SWARM_CHAIN_DONE"));
  else steps.push(step(user, null, [tool(`deeper-${n}`, "spawn", { name: "deeper", task: `SWARM_DEPTH_${n + 1}_TASK`, role: "worker" })]),
    step(user, "agent", [tool(`join-${n}`, "wait", { agents: ["deeper"] })]),
    step(user, "SWARM_CHAIN_DONE", null, "SWARM_CHAIN_DONE"));
}
const swarm = { id: "topology-swarm", workspace: { kind: "plain", files: {} }, model: { steps },
  // This synthetic provider must admit the complete forked conversation; this
  // qualification exercises topology, while context-window admission has its own suite.
  capacity: { context_tokens: 1 << 20, output_tokens: 1 << 16 },
  inputs: [{ mode: "headless", text: "SWARM_ROOT", answer: "SWARM_ALL_DONE", extra_args: ["--topology", "swarm", "--transcripts", "{{workspace}}/../native-traces", "--usage-jsonl", "{{workspace}}/../usage.jsonl"] }],
  assert: { requests: steps.length, model_tools: [{ step: 0, present: ["spawn"] }], tool_contains: ["SWARM_CHAIN_DONE", "SIBLING_256_DONE"], trace: { directory: "../native-traces", usage: "../usage.jsonl", agents: width + depth, topology: { version: 1, mode: "swarm" } } },
  physical: true, adapter: "responses" };
writeFileSync(join(input, "cases/topology-swarm.json"), JSON.stringify(swarm));
const coverage = JSON.parse(readFileSync(join(input, "coverage.json")));
coverage.rows.A3.cases.push("topology-swarm");
writeFileSync(join(input, "coverage.json"), JSON.stringify(coverage));

for (const [id, negativeMode] of [["topology-single", "swarm"], ["topology-team", "swarm"], ["topology-swarm", "team:256"]]) {
  const file = join(input, "cases", `${id}.json`);
  const positive = JSON.parse(readFileSync(file));
  for (const negative of [false, true]) {
    const selected = structuredClone(positive);
    if (negative) selected.inputs[0].extra_args[1] = negativeMode;
    writeFileSync(file, JSON.stringify(selected));
    const destination = join(output, `${id}-${negative ? "negative" : "positive"}`);
    const result = spawnSync(process.execPath, [join(root, "scripts/graphcoder-e2e/run.mjs"), binary, mock, id],
      { cwd: root, encoding: "utf8", maxBuffer: Infinity,
        env: { ...process.env, E2E_OUTPUT: destination, E2E_CASES_ROOT: input } });
    writeFileSync(`${destination}.stdout`, result.stdout ?? "");
    writeFileSync(`${destination}.stderr`, result.stderr ?? "");
    assert.ifError(result.error);
    const report = JSON.parse(readFileSync(join(destination, "matrix.json")));
    assert.equal(result.status, negative ? 1 : 0, JSON.stringify(report.results));
    assert.ok(report.results.length > 0);
    for (const row of report.results) assert.equal(row.status, negative ? "fail" : "pass", JSON.stringify(row));
    console.log(JSON.stringify({ id, negative, status: "pass", seconds: report.seconds }));
  }
}
