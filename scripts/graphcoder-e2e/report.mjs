import { writeFile } from "node:fs/promises";
import { AssertionError } from "node:assert";
import { inspect } from "node:util";

const operandFormat = { depth: null, maxArrayLength: null, maxStringLength: null,
  customInspect: false, getters: false };

// Assertion stacks abbreviate operands. Retain their complete diagnostic
// representation, including cycles, BigInts and undefined, without invoking
// custom inspectors or getters. Work follows only the selected failure's bytes.
export function failureReport(error) {
  const report = { error: error?.stack ?? String(error) };
  if (error instanceof AssertionError) {
    report.assertion = { operator: error.operator,
      actual: inspect(error.actual, operandFormat), expected: inspect(error.expected, operandFormat) };
  }
  if (error instanceof Error && error.cause !== undefined) report.cause = inspect(error.cause, operandFormat);
  return report;
}

// A report-finalization failure cannot replace an earlier operation failure.
export function failedCase(result, failure, phase, recovery) {
  const detail = { ...failure, step: `case-${phase}`, recovery };
  return result.status === "fail"
    ? { ...result, [`${phase}_failure`]: detail }
    : { ...result, ...detail, status: "fail", phase };
}

// Observe deferred capture failure immediately, while later inputs may run.
export function retainCapture(pending, captures) {
  return pending.then(capture => { captures.push(capture); }, error => failureReport(error));
}

export async function finishCase(result, pending, recovery) {
  let failures;
  for (const failure of await Promise.all(pending)) {
    if (failure) (failures ??= []).push(failure);
  }
  return failures ? { ...failedCase(result, failures[0], "capture", recovery), capture_failures: failures } : result;
}

export async function verifyReport(results, readRequests, verify, recovery) {
  let records;
  try { records = await readRequests(); }
  catch (error) {
    const failure = failureReport(error);
    return { results: results.map(result => failedCase(result, failure, "evidence", recovery)) };
  }
  const requests = new Map();
  for (const record of records) {
    const key = record.case;
    let rows = requests.get(key);
    if (!rows) requests.set(key, rows = []);
    rows.push(record);
  }
  return { results: results.map(result => verify(result, requests.get(result.case) ?? [])), requests };
}

// Index each original attempt once, keeping failures in its denominator.
// Only successful intervals participate in the nearest-rank distribution.
export function summarizeCases(selected, results) {
  const cells = new Map(selected.map(name => [name, { case: name, attempts: 0, times: [] }]));
  for (const result of results) {
    const cell = cells.get(result.case);
    cell.attempts++;
    if (result.status === "pass") cell.times.push(result.seconds);
  }
  return Array.from(cells.values(), ({ case: name, attempts, times }) => {
    times.sort((a, b) => a - b);
    const percentile = fraction => times.length ? times[Math.ceil(fraction * times.length) - 1] : null;
    return { case: name, attempts, n: times.length, failures: attempts - times.length,
      p50_seconds: percentile(0.5), p99_seconds: percentile(0.99) };
  });
}

// The job's retained stdout is the publication boundary. A fixture filesystem
// failure must not make the only copy of its evidence depend on export/GC.
export async function publishReport(path, report, io = {
  publish: text => new Promise((resolve, reject) => process.stdout.write(text,
    error => error ? reject(error) : resolve())),
  writeFile,
}, signal) {
  const cancel = () => {
    if (!signal?.aborted) return false;
    report.pass = false;
    report.cancellation = signal.reason?.message ?? String(signal.reason);
    return true;
  };
  const publish = async () => {
    const text = JSON.stringify(report, null, 2) + "\n";
    await io.publish(text);
    await io.writeFile(path, text);
  };
  const cancelled = cancel();
  await publish();
  // Both original writes have joined. A signal aborts only once, so one
  // terminal failed snapshot closes cancellation during either write.
  if (!cancelled && cancel()) await publish();
}
