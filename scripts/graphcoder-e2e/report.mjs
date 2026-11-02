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

// The job's retained stdout is the publication boundary. A fixture filesystem
// failure must not make the only copy of its evidence depend on export/GC.
export async function publishReport(path, report, io = {
  publish: text => new Promise((resolve, reject) => process.stdout.write(text,
    error => error ? reject(error) : resolve())),
  writeFile,
}) {
  const text = JSON.stringify(report, null, 2) + "\n";
  await io.publish(text);
  await io.writeFile(path, text);
}
