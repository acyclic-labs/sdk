import assert from "node:assert/strict";

// A request notification is the mock's commit boundary: append and flush precede
// it, and the HTTP reply follows it. Killing the server can leave an unpublished
// tail, but cannot truncate an acknowledged record. Case assertions still require
// every expected generation and step, so interrupted conversations fail normally.
export function recordedRequests(text, events) {
  const acknowledged = events.filter(event => event.event === "request");
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

export function toolResults(records) {
  const results = [];
  for (const { body } of records) {
    for (const item of body.input ?? body.messages ?? []) {
      if (item.type === "function_call_output") results.push(item.output);
      else if (item.role === "tool") results.push(item.content);
      else if (Array.isArray(item.content)) for (const part of item.content) if (part.type === "tool_result") results.push(typeof part.content === "string" ? part.content : JSON.stringify(part.content));
    }
  }
  return [...new Set(results)];
}

export function requests(test, records) {
  const generations = records.filter(record => ["responses", "messages", "chat/completions"].includes(record.path));
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
  const outputs = toolResults(records).map(value => { try { return JSON.parse(value); } catch { return value; } });
  for (const text of test.assert.tool_contains ?? []) assert.ok(outputs.some(value => JSON.stringify(value).includes(text)), `actual tool result missing ${text}`);
  for (const value of outputs) if (value && typeof value === "object" && value.error) assert.ok((test.assert.tool_errors ?? []).some(text => JSON.stringify(value.error).includes(text)), `unexpected tool error ${JSON.stringify(value.error)}`);
}
