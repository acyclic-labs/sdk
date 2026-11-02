import assert from "node:assert/strict";
import { readFileSync } from "node:fs";
import { readFile } from "node:fs/promises";

// The returned native cause and physical exit are independent observations.
// A successful exit cannot erase a selected startup or execution failure.
export function executionSucceeded(result) {
  assert.equal(result.termination?.startup_rejection ?? result.termination?.execution_failure,
    undefined, "selected native execution reported failure");
  if (result.code !== 0) assert.equal(result.code, 0, `executable exit ${result.code}: ${result.stderr}`);
  assert.equal(result.signal, null, "executable was interrupted");
}

// A selected native execution also owns the original report-ready exchange.
// Output, exit and a diagnostic ACK cannot substitute for that capability.
export function requireReportCapability(result, recovery) {
  assert.equal(result.report_capability?.ready, true,
    `step=admit selected native report capability; recovery=${recovery}`);
}

// JSONL replies carry either a committed answer or an explicit message error.
// The same contract applies to live, scripted and multi-turn acceptance.
export function messageReply(value, id) {
  assert.equal(typeof value?.id, "string", "missing message identity");
  assert.equal(value.id, id, "reply names a different prompt");
  if (value.error === undefined) {
    assert.ok(Number.isSafeInteger(value.inbox) && value.inbox >= 0, "missing committed inbox identity");
    assert.equal(typeof value.text, "string", "missing committed answer text");
  } else assert.equal(typeof value.error, "string", "missing message error text");
  return value;
}

export function jsonlReply(stdout, id) {
  const lines = stdout.trim().split(/\r?\n/).filter(Boolean).map(JSON.parse);
  assert.equal(lines.length, 1, "JSONL needs exactly one authoritative answer");
  return messageReply(lines[0], id);
}

export async function fileBytes(path, expected) {
  assert.deepEqual(await readFile(path), Buffer.from(expected, "utf8"), path);
}

// Both public FileReadSource variants retain identity inside their envelope.
export function fileRead(result, path, expected) {
  assert.equal(result.text, expected, "read returned different file bytes");
  const variants = Object.keys(result.file ?? {});
  assert.equal(variants.length, 1, "read needs exactly one original file source");
  assert.ok(variants[0] === "authored" || variants[0] === "content", "unknown file read source");
  const file = result.file[variants[0]];
  assert.equal(file?.path, path, "read names a different file");
  assert.ok(typeof file?.volume?.id === "string" && file.volume.id.length > 0, "read lost its volume identity");
  assert.ok(variants[0] === "authored" ? file.generation !== undefined : typeof file.version === "string",
    "read lost its selected revision");
  return file;
}

// A request notification is the mock's commit boundary: append and flush precede
// it, and the HTTP reply follows it. Killing the server can leave an unpublished
// tail, but cannot truncate an acknowledged record. Case assertions still require
// every expected generation and step, so interrupted conversations fail normally.
//
// The events are read before the records: every event in that history then
// has its record in the file read after it.
export async function recordedRequests(spool, path) {
  const acknowledged = await spool.history(event => event.event === "request");
  const text = readFileSync(path, "utf8");
  const lines = text.split("\n");
  assert.ok(lines.length > acknowledged.length, "acknowledged request lacks a complete JSONL record");
  return acknowledged.map((event, index) => {
    const record = JSON.parse(lines[index]);
    assert.equal(record.sequence, event.sequence, "request notification sequence");
    assert.equal(record.case, event.case, "request notification namespace");
    assert.equal(record.step, event.step, "request notification step");
    return record;
  });
}

export function subset(actual, expected) {
  if (Array.isArray(expected)) return Array.isArray(actual) && expected.length === actual.length && expected.every((value, index) => subset(actual[index], value));
  if (expected && typeof expected === "object") return actual !== null && typeof actual === "object" && Object.entries(expected).every(([key, value]) => Object.hasOwn(actual, key) && subset(actual[key], value));
  return actual === expected;
}

// Both provider records and PostToolUse carry the original ToolResult value.
// A completed call may still be a refusal; only explicit case expectations
// admit one, regardless of the enclosing process's successful exit.
export function toolOutcome(value, expectedErrors, recovery, caseId) {
  const error = value && typeof value === "object" ? value.error
    ?? (value.success === false || (typeof value.exit_code === "number" && value.exit_code !== 0)
      ? `tool reported success=${value.success}, exit_code=${value.exit_code}` : undefined)
    : typeof value === "string" && value.startsWith("tool executor rejected the admitted call (") ? value : undefined;
  if (error !== undefined && error !== null) assert.ok(
    expectedErrors?.some(text => JSON.stringify(error).includes(text)),
    `unexpected tool error ${JSON.stringify(error)}; recovery=${recovery}${caseId === undefined ? "" : ` ${caseId}`}`);
}

export function toolResults(records) {
  const results = [];
  for (const { body } of records) {
    for (const item of body.input ?? body.messages ?? body.contents ?? []) {
      for (const part of item.parts ?? []) if (part.functionResponse) results.push(JSON.stringify(part.functionResponse.response));
      if (item.type === "function_call_output") results.push(item.output);
      else if (item.role === "tool") results.push(item.content);
      else if (Array.isArray(item.content)) for (const part of item.content) if (part.type === "tool_result") results.push(typeof part.content === "string" ? part.content : JSON.stringify(part.content));
    }
  }
  return [...new Set(results)];
}

export function requests(test, records) {
  const generations = records.filter(record => ["responses", "messages", "chat/completions"].includes(record.path)
    || /^models\/[^/]+:streamGenerateContent(?:\?alt=sse)?$/.test(record.path));
  assert.equal(generations.length, test.assert.requests, "generation count");
  const expected = test.assert.steps ?? test.model.steps.map((_, index) => index);
  assert.deepEqual(records.filter(record => record.step !== null).map(record => record.step).sort((a,b) => a-b), [...expected].sort((a,b) => a-b), "each expected script step consumed exactly once");
  assert.ok(records.every(record => record.status !== 409), "unmatched model request");
  for (const expected of test.assert.http ?? []) assert.ok(records.some(record => subset(record, expected)), `missing HTTP request ${JSON.stringify(expected)}`);
  const wire = JSON.stringify(records), last = JSON.stringify(generations.at(-1)?.body);
  for (const text of test.assert.model_contains ?? []) assert.ok(wire.includes(text), `model wire missing ${text}`);
  for (const text of test.assert.model_absent ?? []) assert.ok(!wire.includes(text), `model wire contains forbidden ${text}`);
  for (const text of test.assert.last_model_contains ?? []) assert.ok(last.includes(text), `final model input missing ${text}`);
  for (const text of test.assert.last_model_absent ?? []) assert.ok(!last.includes(text), `final model input contains forbidden ${text}`);
  for (const [index, request] of generations.entries()) {
    const input = JSON.stringify(request.body);
    for (const text of test.assert.each_model_contains ?? []) assert.ok(input.includes(text), `model input ${index} missing ${text}`);
    for (const text of test.assert.each_model_absent ?? []) assert.ok(!input.includes(text), `model input ${index} contains forbidden ${text}`);
  }
  // Auxiliary search/fetch requests have independent, non-conversation schemas.
  const outputs = toolResults(generations).map(value => { try { return JSON.parse(value); } catch { return value; } });
  for (const expected of test.assert.tool_results ?? []) assert.ok(outputs.some(value => subset(value, expected)), `step=assert exact tool result; actual tool result missing exact value ${JSON.stringify(expected)}; recovery=node scripts/graphcoder-e2e/run.mjs GRAPHCODER MOCK_MODEL ${test.id}`);
  for (const expected of test.assert.file_versions ?? []) {
    assert.ok(outputs.some(value => {
      if (typeof value?.version !== "string") return false;
      let file;
      try { file = JSON.parse(value.version); } catch { return false; }
      return value.path === file.path && subset(file, expected);
    }), `step=assert exact file version; original file identity missing ${JSON.stringify(expected)}; recovery=node scripts/graphcoder-e2e/run.mjs GRAPHCODER MOCK_MODEL ${test.id}`);
  }
  for (const text of test.assert.tool_contains ?? []) assert.ok(outputs.some(value => JSON.stringify(value).includes(text)), `actual tool result missing ${text}`);
  for (const value of outputs) toolOutcome(value, test.assert.tool_errors,
    "node scripts/graphcoder-e2e/run.mjs GRAPHCODER MOCK_MODEL", test.id);
}
