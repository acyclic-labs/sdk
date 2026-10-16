import assert from "node:assert/strict";
import test from "node:test";
import { schedule, setAlarm } from "../dist/index.js";

test("guest scheduling preserves source bytes, retry identity and caller snapshot", async () => {
  const timer = { task_id: "01010101-0101-0101-0101-010101010101", operation_id: "02020202-0202-0202-0202-020202020202", deadline_unix_ms: 18446744073709551615n };
  const appended = [];
  const append = async (path, records, key) => {
    appended.push({ path, records, key });
    return 42;
  };
  const pending = schedule(append, timer);
  timer.task_id = "changed-after-call";
  assert.equal(await pending, 42);
  assert.equal(appended[0].path, "harness/v2/timers/01010101-0101-0101-0101-010101010101");
  assert.equal(new TextDecoder().decode(appended[0].records[0]), '{"deadline_unix_ms":18446744073709551615,"operation_id":"02020202-0202-0202-0202-020202020202","task_id":"01010101-0101-0101-0101-010101010101"}');
  assert.ok(appended[0].records[0] instanceof Uint8Array);
  assert.ok(appended[0].key instanceof Uint8Array);
  assert.equal(appended[0].key.byteLength, 32);
  const original = { ...timer, task_id: "01010101-0101-0101-0101-010101010101" };
  assert.equal(setAlarm, schedule);
  await setAlarm(append, original);
  assert.deepEqual(appended[1], appended[0]);
  for (const invalid of [
    { ...original, deadline_unix_ms: 0n },
    { ...original, deadline_unix_ms: 18446744073709551616n },
    { ...original, task_id: "01010101010101010101010101010101" },
  ]) await assert.rejects(schedule(append, invalid));
  assert.equal(appended.length, 2);
});
