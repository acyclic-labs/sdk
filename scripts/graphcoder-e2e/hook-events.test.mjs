import assert from "node:assert/strict";
import test from "node:test";
import { appendFile, rm, writeFile } from "node:fs/promises";
import { join } from "node:path";
import { scratchDirectory } from "./scratch.mjs";
import { waitForHook } from "./hook-events.mjs";

test("hook subscription covers retained records, later appends and process exit", async () => {
  const root = scratchDirectory("hook-observation-"), path = join(root, "hooks.jsonl");
  const open = new Promise(() => {});
  try {
    await writeFile(path, '{"sequence":1}\n');
    assert.deepEqual(await waitForHook(path, row => row.sequence === 1, open), { sequence: 1 });
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
      Promise.resolve({ code: 1, signal: null })), /source exited before its event/);
  } finally {
    await rm(root, { recursive: true, force: true });
  }
});
