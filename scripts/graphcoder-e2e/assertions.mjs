import assert from "node:assert/strict";
import { readFileSync } from "node:fs";
import { open, readFile } from "node:fs/promises";

// The returned native cause and physical exit are independent observations.
// A successful exit cannot erase a selected startup or execution failure.
export function executionSucceeded(result) {
  assert.equal(result.termination?.startup_rejection ?? result.termination?.execution_failure,
    undefined, "selected native execution reported failure");
  assert.equal(result.termination?.control_failure, undefined,
    "step=admit complete native report stream; recovery=node --test scripts/graphcoder-e2e/process-report-tail.test.mjs");
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
  const reader = await CommittedRequests.open(path);
  try {
    const records = [];
    for (const event of acknowledged) records.push(await reader.next(event));
    return records;
  } finally { await reader.close(); }
}

// Notifications follow a flushed record, so each consumes one original line.
// Read forward in pages: no file rescans, readiness probes or retained history.
export class CommittedRequests {
  #file;
  #position = 0;
  #tail = Buffer.alloc(0);
  #pending = Promise.resolve();
  #closed = false;
  static async open(path) { return new CommittedRequests(await open(path, "r")); }
  constructor(file) { this.#file = file; }
  next(event) {
    if (this.#closed) return Promise.reject(new Error("request record reader is closed"));
    const pending = this.#pending.then(() => this.#read(event));
    this.#pending = pending;
    return pending;
  }
  async #read(event) {
    const parts = [];
    for (;;) {
      const newline = this.#tail.indexOf(10);
      if (newline >= 0) {
        parts.push(this.#tail.subarray(0, newline));
        const bytes = parts.length === 1 ? parts[0] : Buffer.concat(parts);
        this.#tail = this.#tail.subarray(newline + 1);
        const record = JSON.parse(bytes.toString("utf8"));
        assert.equal(record.sequence, event.sequence, "request notification sequence");
        assert.equal(record.case, event.case, "request notification namespace");
        assert.equal(record.step, event.step, "request notification step");
        return record;
      }
      if (this.#tail.length) parts.push(this.#tail);
      const page = Buffer.alloc(64 * 1024);
      const { bytesRead } = await this.#file.read(page, 0, page.length, this.#position);
      assert.ok(bytesRead > 0, "acknowledged request lacks a complete JSONL record");
      this.#position += bytesRead;
      this.#tail = page.subarray(0, bytesRead);
    }
  }
  async close() {
    this.#closed = true;
    await this.#pending.catch(() => {});
    await this.#file.close();
  }
}

export function subset(actual, expected) {
  if (Array.isArray(expected)) return Array.isArray(actual) && expected.length === actual.length && expected.every((value, index) => subset(actual[index], value));
  if (expected && typeof expected === "object") return actual !== null && typeof actual === "object" && Object.entries(expected).every(([key, value]) => Object.hasOwn(actual, key) && subset(actual[key], value));
  return actual === expected;
}

// Both provider records and PostToolUse carry the original ToolResult value.
// A completed call may still be a refusal; only explicit case expectations
// admit one, regardless of the enclosing process's successful exit.
const TOOL_FAILURE = /^tool (execution failed|output violated its pinned schema|result projection was rejected|result artifact could not be published|is not available|was removed|arguments violated the input schema|was interrupted before its outcome was recorded)(?:$|[:;( ])/;

function toolFailure(error, expectedErrors, recovery, caseId) {
  if (error === undefined || error === null) return;
  const encoded = JSON.stringify(error);
  assert.ok(expectedErrors?.some(text => typeof text === "string" && text.length !== 0 && encoded.includes(text)),
    `step=admit tool outcome: unexpected tool error ${encoded}; recovery=${recovery}${caseId === undefined ? "" : ` ${caseId}`}`);
}

export function toolOutcome(value, expectedErrors, recovery, caseId) {
  if (value && typeof value === "object") {
    toolFailure(value.error, expectedErrors, recovery, caseId);
    if (value.version === 2 && Array.isArray(value.context))
      toolFailure(value.output?.error, expectedErrors, recovery, caseId);
    if (Array.isArray(value.settled)) for (const child of value.settled)
      toolFailure(child.failed ?? child.phase?.done?.outcome?.failed, expectedErrors, recovery, caseId);
    if (value.success === false || (typeof value.exit_code === "number" && value.exit_code !== 0))
      toolFailure(`tool reported success=${value.success}, exit_code=${value.exit_code}`, expectedErrors, recovery, caseId);
  } else if (typeof value === "string") {
    const exit = /^Exit code: ([^\r\n]+)/.exec(value);
    if (TOOL_FAILURE.test(value) || value.startsWith("tool executor rejected the admitted call (") || (exit && exit[1] !== "0"))
      toolFailure(value, expectedErrors, recovery, caseId);
  }
}

export const isGeneration = record => ["responses", "messages", "chat/completions"].includes(record.path)
  || /^models\/[^/]+:streamGenerateContent(?:\?alt=sse)?$/.test(record.path);

export function toolResults(records) {
  const results = [];
  for (const { body } of records) {
    for (const item of body.input ?? body.messages ?? body.contents ?? []) {
      for (const part of item.parts ?? []) if (part.functionResponse) {
        const response = part.functionResponse.response;
        results.push(typeof response?.output === "string" ? response.output : JSON.stringify(response));
      }
      if (item.type === "function_call_output") results.push(item.output);
      else if (item.role === "tool") results.push(item.content);
      else if (Array.isArray(item.content)) for (const part of item.content) if (part.type === "tool_result") results.push(typeof part.content === "string" ? part.content : JSON.stringify(part.content));
    }
  }
  return [...new Set(results)];
}

// Evidence names a provider body, never an observer's path or diagnostic.
// Each selected body is encoded once for all presence/absence queries.
export function modelInputs(expected, generations) {
  const contains = expected.model_contains;
  const all = contains?.length || expected.model_absent?.length
    || expected.each_model_contains?.length || expected.each_model_absent?.length;
  if (!all && !expected.last_model_contains?.length && !expected.last_model_absent?.length) return;
  const selected = all || generations.length === 0 ? generations : [generations.at(-1)];
  const inputs = selected.map(request => JSON.stringify(request.body));
  const last = inputs.at(-1) ?? "";
  for (const text of contains ?? []) assert.ok(inputs.some(input => input.includes(text)), `model wire missing ${text}`);
  for (const text of expected.model_absent ?? []) assert.ok(inputs.every(input => !input.includes(text)), `model wire contains forbidden ${text}`);
  for (const text of expected.last_model_contains ?? []) assert.ok(last.includes(text), `final model input missing ${text}`);
  for (const text of expected.last_model_absent ?? []) assert.ok(!last.includes(text), `final model input contains forbidden ${text}`);
  for (const [index, input] of inputs.entries()) {
    for (const text of expected.each_model_contains ?? []) assert.ok(input.includes(text), `model input ${index} missing ${text}`);
    for (const text of expected.each_model_absent ?? []) assert.ok(!input.includes(text), `model input ${index} contains forbidden ${text}`);
  }
}

export function requests(test, records) {
  const generations = records.filter(isGeneration);
  assert.equal(generations.length, test.assert.requests, "generation count");
  for (const expected of test.assert.model_tools ?? []) {
    const request = generations.find(request => request.step === expected.step);
    assert.ok(request, `missing model request for scripted step ${expected.step}`);
    const names = new Set((request.body.tools ?? []).flatMap(tool =>
      tool.functionDeclarations?.map(item => item.name) ?? [tool.function?.name ?? tool.name]));
    for (const name of expected.present ?? []) assert.ok(names.has(name), `step ${expected.step} does not offer ${name}`);
    for (const name of expected.absent ?? []) assert.ok(!names.has(name), `step ${expected.step} offers forbidden ${name}`);
  }
  const expected = test.assert.steps ?? test.model.steps.map((_, index) => index);
  assert.deepEqual(records.filter(record => record.step !== null).map(record => record.step).sort((a,b) => a-b), [...expected].sort((a,b) => a-b), "each expected script step consumed exactly once");
  assert.ok(records.every(record => record.status !== 409), "unmatched model request");
  for (const expected of test.assert.http ?? []) assert.ok(records.some(record => subset(record, expected)), `missing HTTP request ${JSON.stringify(expected)}`);
  modelInputs(test.assert, generations);
  for (const [before, after] of test.assert.stable_system_between ?? []) {
    const system = step => {
      const request = generations.find(request => request.step === step);
      assert.ok(request, `missing system-input comparison step ${step}`);
      const body = request.body;
      const selected = {
        instructions: body.instructions,
        system: body.system,
        systemInstruction: body.systemInstruction,
        messages: (body.input ?? body.messages ?? body.contents ?? []).filter(
          message => message.role === "system" || message.role === "developer"),
      };
      assert.ok(selected.instructions || selected.system || selected.systemInstruction || selected.messages.length,
        `step ${step} has no provider system input to compare`);
      return selected;
    };
    assert.deepEqual(system(after), system(before), `provider system input changed from step ${before} to ${after}`);
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
