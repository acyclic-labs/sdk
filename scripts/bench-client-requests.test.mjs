import assert from "node:assert/strict";
import { readFileSync } from "node:fs";
import test from "node:test";
import { benchClientRequests, cargoArgs, measurements } from "./bench-client-requests.mjs";

const output = [
  "actors admitted request conversion consume=false iterations=256 bytes=65536 elapsed_us=2000",
  "actors admitted request conversion consume=true iterations=256 bytes=65536 elapsed_us=500",
  "objects frame conversion reuse=false iterations=256 bytes=65536 elapsed_us=2200",
  "objects frame conversion reuse=true iterations=256 bytes=65536 elapsed_us=600",
  "stream append JSON direct=false iterations=256 bytes=65536 elapsed_us=3000",
  "stream append JSON direct=true iterations=256 bytes=65536 elapsed_us=1500",
].join("\n");

test("opt-in runner selects the three ignored debug probes without running ordinary suites", () => {
  assert.deepEqual(cargoArgs, [
    "test", "--locked", "-p", "acyclic-actors", "-p", "acyclic-objects", "-p", "acyclic-stream",
    "--lib", "--features", "acyclic-objects/http,acyclic-stream/http",
    "request_path_measure", "--", "--ignored", "--nocapture", "--test-threads=1",
  ]);
  const pkg = JSON.parse(readFileSync(new URL("../package.json", import.meta.url), "utf8"));
  assert.equal(pkg.scripts["bench:clients"], "node scripts/bench-client-requests.mjs");
  assert.deepEqual(measurements(output), [
    { name: "actors admitted request conversion", before: { iterations: 256, bytes: 65536, elapsed_us: 2000 }, after: { iterations: 256, bytes: 65536, elapsed_us: 500 } },
    { name: "objects frame conversion", before: { iterations: 256, bytes: 65536, elapsed_us: 2200 }, after: { iterations: 256, bytes: 65536, elapsed_us: 600 } },
    { name: "stream append JSON", before: { iterations: 256, bytes: 65536, elapsed_us: 3000 }, after: { iterations: 256, bytes: 65536, elapsed_us: 1500 } },
  ]);
});

test("missing, duplicate and mismatched probes cannot produce a successful report", () => {
  assert.throws(() => measurements(output.replace(/\nstream append JSON direct=true[^\n]+/, "")), /expected one/);
  assert.throws(() => measurements(`${output}\n${output}`), /expected one/);
  assert.throws(() => measurements(output.replace("direct=true iterations=256", "direct=true iterations=1")), /mismatched/);
});

test("Cargo failure is propagated even when output contains measurements", () => {
  const run = /** @type {typeof import('node:child_process').spawnSync} */ (/** @type {unknown} */ (() => ({ status: 1, stdout: "", stderr: "", signal: null })));
  assert.throws(() => benchClientRequests(run), /measurements failed: 1/);
});
