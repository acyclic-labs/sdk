import { readFileSync, readdirSync } from "node:fs";
import { workflowExecution } from "./static-policy.mjs";

// Explicit runner capabilities; workflow row counts and matrix key names are
// not policy. Qualification workers match the selected runner's native CPUs.
const runnerCpus = new Map([["kaizen-linux", 8], ["ubuntu-24.04-arm", 4], ["macos-15", 3], ["windows-2025", 4]]);
const allowed = new Set([...runnerCpus.keys(), "ubuntu-24.04", "ubuntu-latest"]);
const lanes = JSON.parse(readFileSync(".github/qualification-lanes.json", "utf8"));
for (const lane of lanes) {
  if (runnerCpus.get(lane.runner) !== lane.workers) {
    throw new Error(`qualification lane ${lane.lane} must run one worker per vCPU on a known runner, not ${lane.workers} on ${lane.runner}`);
  }
}
// The plan producer emits this exact registry. Other dynamic producers require
// their own explicit binding and cannot evade runner policy with an expression.
const matrixIncludes = new Map([["${{ fromJSON(needs.plan.outputs.matrix) }}", lanes]]);
const directory = ".github/workflows";
for (const name of readdirSync(directory).filter(name => /\.ya?ml$/.test(name)).sort()) {
  const path = `${directory}/${name}`;
  for (const runner of workflowExecution(path, readFileSync(path, "utf8"), matrixIncludes)) {
    if (!allowed.has(runner)) throw new Error(`${path} uses unapproved runner ${runner}`);
  }
}
