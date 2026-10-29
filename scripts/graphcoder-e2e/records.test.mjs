import assert from "node:assert/strict";
import test from "node:test";
import { mkdtempSync, writeFileSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { recordedRequests, requests } from "./assertions.mjs";

const directory = mkdtempSync(join(tmpdir(), "e2e-records-"));
let files = 0;
// The records file appears only while the history is read, as when the mock
// appends a record and then publishes its event: reading the file before the
// history fails every test here.
const recorded = (text, events) => {
  const path = join(directory, `${files++}.jsonl`);
  return recordedRequests({ history: async match => { writeFileSync(path, text); return events.filter(match); } }, path);
};

test("only flushed acknowledged records survive interruption, with all case assertions retained", async () => {
  const record = { sequence: 1, case: "a", step: 0, path: "responses", status: 200, body: {} };
  const event = { event: "request", sequence: 1, case: "a", step: 0 };
  for (const tail of ["", "{", JSON.stringify({ ...record, sequence: 2 })]) {
    const parsed = await recorded(JSON.stringify(record) + "\n" + tail, [event]);
    assert.deepEqual(parsed, [record]);
    requests({ assert: { requests: 1 }, model: { steps: [{}] } }, parsed);
    assert.throws(() => requests({ assert: { requests: 2 }, model: { steps: [{}, {}] } }, parsed), /generation count/);
  }
});

test("acknowledged corruption and missing records always fail", async () => {
  const event = { event: "request", sequence: 1, case: "a", step: 0 };
  await assert.rejects(recorded("{", [event]), /complete JSONL/);
  await assert.rejects(recorded("{\n", [event]), SyntaxError);
  await assert.rejects(recorded('{"sequence":2}\n', [event]), /sequence/);
});
