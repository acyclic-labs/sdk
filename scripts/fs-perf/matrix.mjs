import assert from "node:assert/strict";
import { createHash } from "node:crypto";
import { readFile, writeFile } from "node:fs/promises";
import { pathToFileURL } from "node:url";

const axes = ["writers", "readers", "agents", "changed_files", "watchers"];
const sha = /^[a-f0-9]{40}$/;
const digest = /^[a-f0-9]{64}$/;
const hash = value => createHash("sha256").update(JSON.stringify(value)).digest("hex");

export function plan(matrix, sourcePin) {
  assert.equal(matrix.version, 1, "unknown workload matrix version");
  assert.ok(typeof sourcePin === "string" && sha.test(sourcePin), "an admitted SDK revision is required");
  assert.ok(matrix.backends.length && matrix.cases.length, "empty matrix");
  assert.equal(new Set(matrix.backends.map(value => value.id)).size, matrix.backends.length, "duplicate backend");
  assert.equal(new Set(matrix.cases.map(value => value.id)).size, matrix.cases.length, "duplicate workload");
  assert.equal(matrix.backends.filter(value => value.kind === "raw-host").length, 1, "one raw-host ceiling is required");
  for (const repository of Object.values(matrix.repositories)) {
    assert.match(repository.revision, sha, "repository requires an immutable revision");
    assert.ok(repository.url.startsWith("https://github.com/") && repository.url.endsWith(".git"));
  }
  const requests = [];
  for (const workload of [...matrix.cases].sort((a, b) => (a.priority ?? 1) - (b.priority ?? 1))) {
    if (workload.repository) assert.ok(matrix.repositories[workload.repository], "unbound repository");
    if (workload.argv) assert.ok(workload.argv.length && workload.argv.every(value => typeof value === "string" && value.length));
    let variants = [{}];
    for (const axis of axes.filter(axis => Array.isArray(workload[axis]))) {
      assert.ok(workload[axis].length && workload[axis].every(value => Number.isSafeInteger(value) && value > 0));
      assert.equal(new Set(workload[axis]).size, workload[axis].length, "duplicate axis member");
      variants = variants.flatMap(prior => workload[axis].map(value => ({ ...prior, [axis]: value })));
    }
    const states = workload.states ?? ["cold"];
    assert.ok(states.length && states.every(value => ["cold", "warm", "hinted"].includes(value)));
    assert.equal(new Set(states).size, states.length, "duplicate state");
    for (const backend of matrix.backends) {
      assert.ok(Number.isSafeInteger(backend.round_trip_ms) && backend.round_trip_ms >= 0);
      for (const variant of variants) {
        // One experiment owns cold/warm/hinted in order. It cannot inherit a
        // learned trace or mutable fixture from a different experiment.
        const experiment = { source_pin: sourcePin, backend, workload: { ...workload, ...variant } };
        for (const state of states) {
          const identity = { ...experiment, state };
          const group = workload.metrics_group;
          const extra = group === "cache" ? matrix.cache_metrics : group === "storage" ? matrix.storage_metrics : [];
          requests.push({ version: 1, id: hash(identity), experiment_id: hash(experiment), ...identity,
            required_metrics: [...new Set([...matrix.required_metrics, ...(workload.metrics ?? []), ...extra])] });
        }
      }
    }
  }
  assert.equal(new Set(requests.map(value => value.id)).size, requests.length, "duplicate experiment identity");
  return requests;
}

export function admit(request, receipt) {
  assert.equal(receipt.version, 1, "unknown measurement receipt version");
  assert.equal(receipt.request_id, request.id, "measurement belongs to another experiment");
  assert.equal(receipt.source_pin, request.source_pin, "measurement uses another SDK revision");
  assert.equal(receipt.status, "measured", "skipped or failed work is not a measurement");
  assert.equal(receipt.correctness, "passed", "benchmark requires a verified final result");
  assert.match(receipt.executable_sha256, digest, "executable identity is required");
  assert.match(receipt.source_manifest_sha256, digest, "source admission receipt is required");
  assert.ok(typeof receipt.os === "string" && receipt.os.length
    && typeof receipt.host_filesystem === "string" && receipt.host_filesystem.length, "OS and actual filesystem are required");
  assert.ok(receipt.fixture_id && receipt.experiment_id === request.experiment_id, "fixture custody is required");
  for (const name of request.required_metrics) {
    const value = receipt.metrics[name];
    assert.ok(typeof value === "number" && Number.isFinite(value) && value >= 0, `unmeasured metric: ${name}`);
  }
  const metrics = receipt.metrics;
  for (const name of ["client_round_trips", "wire_bytes", "allocations", "peak_fds", "peak_threads",
    "stream_appends", "rebase_calls", "rebase_items", "rebase_content_reads"]) {
    if (request.required_metrics.includes(name)) assert.ok(Number.isSafeInteger(metrics[name]), `counter must be an integer: ${name}`);
  }
  if (request.required_metrics.includes("peak_memory_bytes")) assert.ok(metrics.peak_memory_bytes > 0, "peak memory must be measured");
  if (request.required_metrics.includes("peak_threads")) assert.ok(metrics.peak_threads > 0, "peak threads must be measured");
  assert.ok(Number.isSafeInteger(metrics.operations) && metrics.operations > 0, "zero-operation measurements are invalid");
  assert.ok(metrics.seconds > 0 && metrics.p99_ms >= metrics.p50_ms, "invalid duration or percentile order");
  const throughput = metrics.operations / metrics.seconds;
  assert.ok(Math.abs(metrics.ops_per_second - throughput) <= throughput * 1e-9, "throughput must use the recorded interval");
  assert.ok(Number.isSafeInteger(metrics.client_round_trips), "round trips must be measured counts");
  // Count proposal/reply exchanges at the client boundary, including an
  // in-process actor. Backend wire traffic is measured separately. Preserve
  // inefficient baselines too: admission must not impose the target value.
  assert.ok(Array.isArray(receipt.samples_ms) && receipt.samples_ms.length === metrics.operations, "retain every operation's latency sample");
  assert.ok(receipt.samples_ms.every(value => Number.isFinite(value) && value >= 0));
  const sorted = [...receipt.samples_ms].sort((a, b) => a - b);
  for (const [metric, fraction] of [["p50_ms", 0.5], ["p99_ms", 0.99]]) {
    assert.equal(metrics[metric], sorted[Math.ceil(sorted.length * fraction) - 1], "percentile disagrees with raw samples");
  }
  return receipt;
}

function ceilingKey(request, receipt) {
  return JSON.stringify([request.workload, request.state, receipt.os, receipt.host_filesystem]);
}

export function report(requests, receipts) {
  const byId = new Map();
  const planned = new Map(requests.map(value => [value.id, value]));
  for (const receipt of receipts) {
    assert.ok(planned.has(receipt.request_id), "receipt was not in the plan");
    assert.ok(!byId.has(receipt.request_id), "duplicate measurement receipt");
    byId.set(receipt.request_id, admit(planned.get(receipt.request_id), receipt));
  }
  const ceilings = new Map();
  for (const request of requests.filter(value => value.backend.kind === "raw-host")) {
    const receipt = byId.get(request.id);
    if (receipt) ceilings.set(ceilingKey(request, receipt), receipt.metrics.ops_per_second);
  }
  const rows = requests.map(request => {
    const parameters = Object.fromEntries(axes.filter(axis => typeof request.workload[axis] === "number")
      .map(axis => [axis, request.workload[axis]]));
    const receipt = byId.get(request.id);
    if (!receipt) return { request_id: request.id, case: request.workload.id, backend: request.backend.id,
      parameters, state: request.state, status: "missing", metrics: null, raw_host_ratio: null };
    const ceiling = ceilings.get(ceilingKey(request, receipt));
    return { request_id: request.id, case: request.workload.id, backend: request.backend.id,
      parameters, state: request.state, status: "measured", metrics: receipt.metrics,
      raw_host_ratio: ceiling > 0 ? receipt.metrics.ops_per_second / ceiling : null };
  });
  return { version: 1, planned_requests: requests.length, measured_requests: byId.size,
    complete: rows.length > 0 && rows.every(row => row.status === "measured" && row.raw_host_ratio !== null), rows };
}

export function markdown(summary) {
  const text = value => String(value).replaceAll("|", "\\|").replaceAll("\n", " ");
  const number = value => value === undefined || value === null ? "—" : Number(value).toPrecision(5);
  const lines = [`Measured ${summary.measured_requests}/${summary.planned_requests}; complete: ${summary.complete}.`, "",
    "| Workload | Parameters | Backend | State | ops/s | p50 ms | p99 ms | client RTT/op | appends/op | rebase items/op | raw-host ratio | Status |",
    "|---|---|---|---|---:|---:|---:|---:|---:|---:|---:|---|"];
  for (const row of summary.rows) {
    const m = row.metrics, per = name => m && m[name] !== undefined ? m[name] / m.operations : null;
    lines.push("| " + [text(row.case), text(Object.entries(row.parameters).map(([key, value]) => `${key}=${value}`).join(", ")),
      text(row.backend), text(row.state), number(m?.ops_per_second), number(m?.p50_ms), number(m?.p99_ms),
      number(per("client_round_trips")), number(per("stream_appends")), number(per("rebase_items")),
      number(row.raw_host_ratio), text(row.status)].join(" | ") + " |");
  }
  return lines.join("\n") + "\n";
}

async function main() {
  const [operation, matrixFile, sourcePin, input, output, ...extra] = process.argv.slice(2);
  assert.ok(!extra.length && (operation === "plan" || operation === "report"),
    "usage: matrix.mjs plan MATRIX SDK_PIN OUTPUT | report MATRIX SDK_PIN RECEIPTS OUTPUT");
  const requests = plan(JSON.parse(await readFile(matrixFile, "utf8")), sourcePin);
  if (operation === "plan") {
    assert.ok(input && !output);
    await writeFile(input, JSON.stringify(requests, null, 2) + "\n");
  } else {
    assert.ok(input && output);
    const summary = report(requests, JSON.parse(await readFile(input, "utf8")));
    await writeFile(output, JSON.stringify(summary, null, 2) + "\n");
    if (!summary.complete) process.exitCode = 1;
  }
}

if (process.argv[1] && import.meta.url === pathToFileURL(process.argv[1]).href) await main();
