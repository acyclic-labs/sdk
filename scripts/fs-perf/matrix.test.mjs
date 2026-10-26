import assert from "node:assert/strict";
import test from "node:test";
import { readFile } from "node:fs/promises";
import { plan, admit, markdown, report } from "./matrix.mjs";

const matrix = JSON.parse(await readFile(new URL("./matrix.json", import.meta.url), "utf8"));
const pin = "2e7505868c0e0dd88044288b89e5ea61284ce91c";
const requests = plan(matrix, pin);

function receipt(request) {
  return { version: 1, request_id: request.id, experiment_id: request.experiment_id,
    source_pin: request.source_pin, status: "measured", correctness: "passed",
    executable_sha256: "a".repeat(64), source_manifest_sha256: "b".repeat(64),
    os: "linux", host_filesystem: "ext4", fixture_id: request.experiment_id,
    samples_ms: [1, 2, 3, 4], metrics: {
      ...Object.fromEntries(request.required_metrics.map(name => [name, 0])),
      operations: 4, seconds: 1, ops_per_second: 4, p50_ms: 2, p99_ms: 4,
      peak_memory_bytes: 4096, peak_threads: 1
    } };
}

test("the full catalog includes cold/warm/hinted and every concurrency population on every backend", () => {
  for (const backend of matrix.backends) {
    const selected = requests.filter(request => request.backend.id === backend.id);
    assert.deepEqual(selected.filter(request => request.workload.id === "concurrency.disjoint")
      .map(request => request.workload.writers), [1, 4, 16, 64, 256]);
    assert.deepEqual(selected.filter(request => request.workload.id === "speculation.never-seen-typescript")
      .map(request => request.state), ["cold", "warm", "hinted"]);
    assert.equal(selected[0].workload.priority, 0);
    for (const row of selected.filter(request => request.workload.metrics_group === "cache")) {
      assert.ok(matrix.cache_metrics.every(metric => row.required_metrics.includes(metric)));
    }
  }
  // Negative control: deleting a backend cannot pass the catalog coverage check.
  const incomplete = plan({ ...matrix, backends: matrix.backends.filter(value => value.id !== "hosted-80ms") }, pin);
  assert.notEqual(incomplete.length, requests.length);
});

test("SDK identity is an admitted build input rather than a self-referential matrix commit", () => {
  assert.throws(() => plan(matrix), /admitted SDK revision/);
  const changed = plan(matrix, "f".repeat(40));
  assert.notEqual(changed[0].id, requests[0].id);
  assert.equal(changed[0].source_pin, "f".repeat(40));
  assert.equal(changed[0].workload.id, requests[0].workload.id);
});

test("missing, skipped, duplicated, or zero-operation measurements never qualify", () => {
  const request = requests[0], valid = receipt(request);
  admit(request, valid);
  for (const mutation of [
    value => { delete value.metrics.client_round_trips; },
    value => { value.status = "skipped"; },
    value => { value.metrics.operations = 0; },
    value => { value.source_pin = "f".repeat(40); },
    value => { value.samples_ms.pop(); },
    value => { value.metrics.p50_ms = 3; },
    value => { value.metrics.peak_memory_bytes = 0; },
    value => { value.metrics.allocations = 0.5; }
  ]) {
    const invalid = structuredClone(valid);
    mutation(invalid);
    assert.throws(() => admit(request, invalid));
  }
  const empty = report([request], []);
  assert.equal(empty.complete, false);
  assert.equal(empty.rows[0].metrics, null);
  assert.throws(() => report([request], [valid, valid]));
  assert.equal(report([], []).complete, false);
  assert.match(markdown(empty), /Measured 0\/1; complete: false/);
  assert.match(markdown(empty), /missing/);
});

test("raw-host ceilings bind the same workload, state, OS, and actual filesystem", () => {
  const embedded = requests.find(value => value.backend.id === "local");
  const raw = requests.find(value => value.backend.id === "raw" && value.workload.id === embedded.workload.id);
  const a = receipt(embedded), b = receipt(raw);
  b.metrics.seconds = 0.5;
  b.metrics.ops_per_second = 8;
  const matched = report([embedded, raw], [a, b]);
  assert.equal(matched.complete, true);
  assert.equal(matched.rows[0].raw_host_ratio, 0.5);
  // Negative control: a measurement from another filesystem is not a ceiling.
  b.host_filesystem = "ntfs";
  assert.equal(report([embedded, raw], [a, b]).complete, false);
});


test("actor exchanges are retained for local calls and inefficient baselines", () => {
  const request = requests.find(value => value.workload.id === "concurrency.disjoint" && value.backend.id === "local");
  for (const exchangesPerCommit of [1, 3]) {
    const measurement = receipt(request);
    measurement.metrics.client_round_trips = measurement.metrics.operations * exchangesPerCommit;
    assert.equal(admit(request, measurement).metrics.client_round_trips, 4 * exchangesPerCommit);
  }
});
