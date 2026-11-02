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
  return report;
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
