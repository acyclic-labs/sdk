import assert from "node:assert/strict";
import { createHash } from "node:crypto";
import { mkdir, mkdtemp, readFile, rm, writeFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import test from "node:test";
import { stageArtifacts } from "../graphcoder-e2e/artifacts.mjs";
import { plan } from "./matrix.mjs";
import { admitExperiment, experiments, runPlan } from "./runner.mjs";

const pin = "2e7505868c0e0dd88044288b89e5ea61284ce91c";
const matrix = { version: 1, repositories: {}, backends: [{ id: "raw", kind: "raw-host", round_trip_ms: 0 }],
  required_metrics: ["operations", "seconds", "ops_per_second", "p50_ms", "p99_ms", "client_round_trips"],
  cases: [{ id: "fixture-protocol", shape: "protocol-proof", states: ["cold", "warm", "hinted"] }] };
const requests = plan(matrix, pin);
const sha = bytes => createHash("sha256").update(bytes).digest("hex");

// These are protocol fixtures, not filesystem performance measurements.
function response(experiment) {
  return { version: 1, experiment_id: experiment.experiment_id, fixture_id: experiment.fixture_id,
    measurements: experiment.requests.map(request => ({ version: 1, request_id: request.id,
      status: "measured", correctness: "passed", os: "fixture-os", host_filesystem: "fixture-fs",
      samples_ms: [1], metrics: { operations: 1, seconds: 0.001, ops_per_second: 1000,
        p50_ms: 1, p99_ms: 1, client_round_trips: 0 } })) };
}

test("one experiment owns ordered cold, warm and hinted phases", () => {
  assert.equal(experiments(requests).length, 1);
  assert.throws(() => experiments([requests[1], requests[0], requests[2]]), /starts cold/);
  assert.throws(() => experiments([requests[0], requests[2], requests[1]]), /learning order/);
  assert.throws(() => experiments([...requests, requests[0]]), /duplicate experiment request/);
  assert.throws(() => experiments([requests[0], { ...requests[1], workload: { ...requests[1].workload, bytes: 1 } }]), /mixed experiment inputs/);
});

test("a worker cannot omit, reorder, skip or reuse measurements from another fixture", () => {
  const input = { experiment_id: requests[0].experiment_id, fixture_id: "fresh", requests };
  const valid = response(input);
  const identity = { fixture_id: "fresh", executable_sha256: "a".repeat(64), source_manifest_sha256: "b".repeat(64) };
  assert.equal(admitExperiment(requests, valid, identity).length, 3);
  for (const mutation of [
    value => { value.measurements.pop(); },
    value => { value.measurements.reverse(); },
    value => { value.fixture_id = "stale"; },
    value => { value.experiment_id = "another"; },
    value => { value.measurements[0].status = "skipped"; },
    value => { delete value.measurements[0].metrics.client_round_trips; }
  ]) {
    const invalid = structuredClone(valid);
    mutation(invalid);
    assert.throws(() => admitExperiment(requests, invalid, identity));
  }
});

async function setup() {
  const root = await mkdtemp(join(tmpdir(), "fs-perf-runner-"));
  const source = join(root, "source"), input = join(root, "cargo-input"), bundle = join(root, "bundle");
  await mkdir(source);
  await writeFile(join(source, "production.rs"), "shared filesystem source\n");
  await writeFile(input, "protocol test artifact: not a product binary\n");
  const record = { reason: "compiler-artifact", target: { name: "fs-perf" }, profile: { test: false }, executable: input };
  const files = { "production.rs": sha(await readFile(join(source, "production.rs"))) };
  const manifest = join(root, "source.json");
  await writeFile(manifest, JSON.stringify({ version: 1, source_pin: pin, files }));
  await stageArtifacts([{ name: "fs-perf", target: "fs-perf", test: false, records: JSON.stringify(record) }], bundle,
    { version: 1, source_pin: pin, source_manifest_sha256: sha(await readFile(manifest)), uploaded_snapshot_sha256: sha("fixture uploaded snapshot") });
  return { root, configuration: { records: join(root, "records"), backends: { raw: {} },
    driver: { directory: bundle, name: "fs-perf", manifest_sha256: sha(await readFile(join(bundle, "artifacts.json"))) },
    source: { directory: source, manifest, sha256: sha(await readFile(manifest)), expected_files: files } } };
}

class FixtureHost {
  calls = [];
  closed = false;
  change = () => {};
  async runOwned(program, args, options) {
    this.calls.push({ program, args, options });
    const experiment = JSON.parse(await readFile(args[1], "utf8"));
    const output = response(experiment);
    await this.change(output, experiment);
    await writeFile(args[3], JSON.stringify(output), { flag: "wx" });
    return { code: 0, signal: null, stdout: "fixture protocol\n", stderr: "" };
  }
  async close() { this.closed = true; }
}

test("a run admits source and executable before launch and exclusively owns each fixture", async () => {
  const { root, configuration } = await setup(), host = new FixtureHost();
  try {
    assert.ok(!Object.hasOwn(process.env, "FS_PERF_PROBE_API_KEY"));
    process.env.FS_PERF_PROBE_API_KEY = "fixture-value";
    const receipts = await runPlan(requests, configuration, host);
    assert.equal(receipts.length, 3);
    assert.equal(host.calls.length, 1);
    assert.equal(host.closed, true);
    const env = host.calls[0].options.env;
    assert.equal(env.FS_PERF_PROBE_API_KEY, undefined);
    assert.ok(env.CODEX_HOME.startsWith(configuration.records));
    assert.ok(env.TMPDIR.startsWith(configuration.records));
    const summary = JSON.parse(await readFile(join(configuration.records, "report.json"), "utf8"));
    assert.equal(summary.complete, true);
    await assert.rejects(runPlan(requests, configuration, host), /EEXIST/);
    assert.equal(host.calls.length, 1, "fixture reuse launched another worker");
  } finally { delete process.env.FS_PERF_PROBE_API_KEY; await rm(root, { recursive: true, force: true }); }
});

test("a changed or incomplete source closure cannot launch a worker", async () => {
  const { root, configuration } = await setup(), host = new FixtureHost();
  try {
    await writeFile(join(configuration.source.directory, "production.rs"), "changed source\n");
    await assert.rejects(runPlan(requests, configuration, host), /manifest bytes changed/);
    assert.equal(host.calls.length, 0);
    await rm(join(configuration.source.directory, "production.rs"));
    await assert.rejects(runPlan(requests, configuration, host), /missing manifest member/);
    assert.equal(host.calls.length, 0);
  } finally { await rm(root, { recursive: true, force: true }); }
});

test("individually valid binary and source manifests cannot mix producer graphs", async () => {
  const { root, configuration } = await setup(), host = new FixtureHost();
  try {
    const manifest = JSON.parse(await readFile(configuration.source.manifest, "utf8"));
    await writeFile(configuration.source.manifest, JSON.stringify({ ...manifest, another_producer: true }));
    configuration.source.sha256 = sha(await readFile(configuration.source.manifest));
    await assert.rejects(runPlan(requests, configuration, host), /different producer graphs/);
    assert.equal(host.calls.length, 0);
  } finally { await rm(root, { recursive: true, force: true }); }
});

test("a failed phase retains raw evidence and an incomplete report, then reaps its host", async () => {
  const { root, configuration } = await setup(), host = new FixtureHost();
  host.change = output => { output.measurements.pop(); };
  try {
    await assert.rejects(runPlan(requests, configuration, host), /every requested phase/);
    assert.equal(host.closed, true);
    const summary = JSON.parse(await readFile(join(configuration.records, "report.json"), "utf8"));
    assert.equal(summary.complete, false);
    assert.ok(summary.rows.every(row => row.status === "missing"));
    const fixture = host.calls[0].args[1].slice(0, -"experiment.json".length);
    assert.equal((await readFile(join(fixture, "stdout.log"), "utf8")), "fixture protocol\n");
  } finally { await rm(root, { recursive: true, force: true }); }
});

test("source mutation during execution cannot leave a qualified report", async () => {
  const { root, configuration } = await setup(), host = new FixtureHost();
  host.change = async () => { await writeFile(join(configuration.source.directory, "production.rs"), "mutated during execution\n"); };
  try {
    await assert.rejects(runPlan(requests, configuration, host), /manifest bytes changed/);
    const summary = JSON.parse(await readFile(join(configuration.records, "report.json"), "utf8"));
    assert.equal(summary.complete, false);
    assert.match(summary.failure, /manifest bytes changed/);
    assert.equal(host.closed, true);
  } finally { await rm(root, { recursive: true, force: true }); }
});
