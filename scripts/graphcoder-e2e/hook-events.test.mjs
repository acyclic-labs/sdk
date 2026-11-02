import assert from "node:assert/strict";
import test from "node:test";
import fs from "node:fs";
import { appendFile, open, rm, writeFile } from "node:fs/promises";
import { syncBuiltinESMExports } from "node:module";
import { join } from "node:path";
import { scratchDirectory } from "./scratch.mjs";
import { waitForHook } from "./hook-events.mjs";

test("hook subscription covers retained records, later appends and process exit", async () => {
  const root = scratchDirectory("hook-observation-"), path = join(root, "hooks.jsonl");
  const open = new Promise(() => {});
  try {
    await writeFile(path, '{"sequence":1}\n');
    assert.deepEqual(await waitForHook(path, row => row.sequence === 1, open), { sequence: 1 });
    const exited = { code: 1, signal: null };
    assert.deepEqual(await waitForHook(path, row => row.sequence === 1, Promise.resolve(exited)), { sequence: 1 });
    Object.defineProperty(exited, "stdout", { enumerable: true, get() { assert.fail("hook exit reread the process transcript"); } });
    let observed, retainedVisits = 0;
    const retained = new Promise(resolve => { observed = resolve; });
    const next = waitForHook(path, row => {
      if (row.sequence === 1) { retainedVisits++; observed(); }
      return row.sequence === 2;
    }, open);
    await retained;
    await appendFile(path, '{"sequence":');
    await appendFile(path, '2}\n');
    assert.deepEqual(await next, { sequence: 2 });
    assert.equal(retainedVisits, 1, "an append reparsed retained history");
    await assert.rejects(waitForHook(path, row => row.sequence === 3,
      Promise.resolve(exited)), error => error.code === "HOOK_SOURCE_EXITED" && error.detail === exited);
  } finally {
    await rm(root, { recursive: true, force: true });
  }
});

test("hook notifications share bounded reads before original exit", async t => {
  const root = scratchDirectory("hook-burst-"), path = join(root, "hooks.jsonl");
  let releaseRead, readStarted, publishExit, notified;
  const held = new Promise(resolve => { releaseRead = resolve; });
  const started = new Promise(resolve => { readStarted = resolve; });
  const closed = new Promise(resolve => { publishExit = resolve; });
  let reads = 0, notifications = 0, observation;
  const visited = [];
  try {
    await writeFile(path, "");
    const probe = await open(path, "r");
    const prototype = Object.getPrototypeOf(probe), read = prototype.read;
    await probe.close();
    // Hold delivery of an actual EOF, then count reads of that same file. No
    // synthetic watch events or replacement file contents drive this control.
    t.mock.method(prototype, "read", async function (...args) {
      const first = ++reads === 1;
      const result = await read.apply(this, args);
      if (first) { assert.equal(result.bytesRead, 0); readStarted(); await held; }
      return result;
    });
    const watch = fs.watch;
    t.mock.method(fs, "watch", (...args) => {
      const watcher = watch(...args);
      watcher.on("change", () => { notifications++; notified?.resolve(); notified = undefined; });
      watcher.on("error", error => { notified?.reject(error); notified = undefined; });
      return watcher;
    });
    syncBuiltinESMExports();
    observation = waitForHook(path, row => { visited.push(row.sequence); return false; }, closed);
    const missing = assert.rejects(observation, { code: "HOOK_SOURCE_EXITED" });
    await Promise.race([started, observation]);
    for (let sequence = 1; sequence <= 8; sequence++) {
      const changed = new Promise((resolve, reject) => { notified = { resolve, reject }; });
      await appendFile(path, `${JSON.stringify({ sequence })}\n`);
      await changed;
    }
    publishExit({ code: 0, signal: null });
    releaseRead();
    await missing;
    assert.deepEqual(visited, [1, 2, 3, 4, 5, 6, 7, 8], "the original cursor lost or repeated appended records");
    assert.ok(reads <= 3, `notification burst queued redundant reads: ${reads}`);
    t.diagnostic(`original notifications=${notifications}, file reads=${reads}, record visits=${visited.length}`);
  } finally {
    publishExit({ code: 1, signal: null });
    releaseRead();
    await observation?.catch(() => {});
    t.mock.restoreAll();
    syncBuiltinESMExports();
    await rm(root, { recursive: true, force: true });
  }
});
