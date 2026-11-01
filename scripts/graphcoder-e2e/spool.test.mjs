import assert from "node:assert/strict";
import test from "node:test";
import { getEventListeners } from "node:events";
import { mkdtempSync, truncateSync } from "node:fs";
import { join } from "node:path";
import { tmpdir } from "node:os";
import { PassThrough } from "node:stream";
import { Processes, Spool } from "./process.mjs";

test("keyed transcript lookups read exactly one original event, including after EOF", async () => {
  const source = new PassThrough();
  let close;
  const closed = new Promise(resolve => { close = resolve; });
  const path = join(mkdtempSync(join(tmpdir(), "keyed-spool-")), "events.jsonl");
  const events = new Spool(source, closed, { transcript: path, key: event => [event.id, `alias:${event.id}`], unique: true });
  const first = { id: "first", text: "λ\n終" }, second = { id: "second", text: "z".repeat(128 * 1024) };
  const seen = events.next(event => event.id === "second");
  source.end(JSON.stringify(first) + "\r\n" + JSON.stringify(second));
  await seen;
  // EOF may win the next microtask, but cannot erase a prior indexed event.
  const early = events.keyed("first");
  close({ code: 0, signal: null });
  assert.deepEqual(await early, first);
  assert.deepEqual(await events.keyed("alias:second"), second);
  await events.history();
  truncateSync(path, 1);
  await assert.rejects(events.keyed("first"), error => error.kind === "spool" && /indexed event/.test(error.message));
});

test("abandoned next and keyed waits release their subscriptions without ending the source", async () => {
  const source = new PassThrough(), controller = new AbortController();
  const events = new Spool(source, new Promise(() => {}), { key: event => [event.id] });
  const reason = new Error("case no longer owns these waits");
  let comparisons = 0;
  const next = events.next(() => { comparisons++; return false; }, { signal: controller.signal }).catch(error => error);
  const keyed = events.keyed("abandoned", { signal: controller.signal }).catch(error => error);
  assert.equal(getEventListeners(controller.signal, "abort").length, 2);
  controller.abort(reason);
  assert.equal(getEventListeners(controller.signal, "abort").length, 0);
  const live = events.keyed("live");
  source.write('{"id":"live"}\n');
  assert.deepEqual(await live, { id: "live" });
  assert.equal(comparisons, 0, "cancelled predicates must never see subsequent events");
  for (const error of [await next, await keyed]) {
    assert.equal(error.kind, "cancelled");
    assert.equal(error.detail, reason);
  }
  await assert.rejects(events.keyed("live", { signal: controller.signal }), error => error.kind === "cancelled");
  source.end();
});

test("a file-backed key index releases parsed payloads as the number of unique keys grows", async () => {
  const processes = new Processes();
  const code = `
    import assert from 'node:assert/strict';
    import { mkdtempSync } from 'node:fs';
    import { tmpdir } from 'node:os';
    import { join } from 'node:path';
    import { PassThrough } from 'node:stream';
    import { Spool } from ${JSON.stringify(new URL("./process.mjs", import.meta.url).href)};
    const source = new PassThrough(), references = [];
    const events = new Spool(source, new Promise(() => {}), {
      transcript: join(mkdtempSync(join(tmpdir(), 'indexed-payloads-')), 'events.jsonl'),
      key: event => { references.push(new WeakRef(event)); return [event.id]; }, unique: true,
    });
    const total = 1024;
    const completed = events.next(event => event.id === total - 1);
    for (let id = 0; id < total; id++) {
      const line = JSON.stringify({id, payload: String(id).padEnd(16 * 1024, 'x')}) + '\\n';
      if (!source.write(line)) await new Promise(resolve => source.once('drain', resolve));
    }
    await completed;
    await events.history(() => false);
    // One event-loop turn ends WeakRef's mandatory current-job retention.
    await new Promise(resolve => setImmediate(resolve));
    global.gc();
    const retained = references.filter(reference => reference.deref() !== undefined).length;
    assert.ok(retained <= 1, 'index retained ' + retained + ' full event payloads');
    const selected = await events.keyed(511);
    assert.equal(selected.id, 511);
    assert.equal(selected.payload.length, 16 * 1024);
    source.end();
    console.log(JSON.stringify({keys: total, retained_parsed_payloads: retained}));
  `;
  try {
    const result = await processes.run(process.execPath, ["--expose-gc", "--input-type=module", "-e", code]);
    assert.equal(result.code, 0, result.stderr);
    assert.equal(JSON.parse(result.stdout).keys, 1024);
  } finally { await processes.close(); }
});
