// Scope the shipping R17 cases under the same native process/mount owner as
// the complete acceptance matrix. Compilation is the caller's prerequisite.
import assert from "node:assert/strict";
import { mkdir, readFile } from "node:fs/promises";
import { join, resolve } from "node:path";
import { stageArtifacts } from "./graphcoder-e2e/artifacts.mjs";
import { Processes } from "./graphcoder-e2e/process.mjs";
import { executionSucceeded } from "./graphcoder-e2e/assertions.mjs";

const [cargoJson, destination, ...selected] = process.argv.slice(2);
assert.ok(cargoJson && destination, "usage: node scripts/qualify-shell-lifetime.mjs CARGO_JSON FRESH_DIRECTORY");
const records = await readFile(cargoJson, "utf8"), root = resolve(destination);
await mkdir(root);
const { artifacts } = await stageArtifacts(["graphcoder", "harness-mock-model"].map(name =>
  ({ name, target: name, test: false, records })), join(root, "bin"));
const processes = new Processes();
try {
  const cases = ["bash-capture", "shell-background-job", "shell-no-deadline"].flatMap(id =>
    process.platform === "linux" ? [id, id + "-physical"] : [id]);
  for (const id of selected.length ? selected : cases) {
    assert.ok(cases.includes(id), `unknown shell lifetime case ${id}`);
    const output = join(root, id);
    const result = await processes.run(process.execPath, ["scripts/graphcoder-e2e/run.mjs",
      artifacts.graphcoder.path, artifacts["harness-mock-model"].path, id],
    { env: { ...process.env, E2E_OUTPUT: output } });
    if (result.code !== 0) process.stderr.write(result.stdout + result.stderr);
    executionSucceeded(result);
    const report = JSON.parse(await readFile(join(output, "matrix.json"), "utf8"));
    assert.deepEqual(report.results.map(item => [item.case, item.status]), [[id, "pass"]]);
    console.log(JSON.stringify({ case: id, status: "PASS", seconds: report.seconds,
      artifacts, evidence: report.results[0].evidence }));
  }
} finally { await processes.close(); }
