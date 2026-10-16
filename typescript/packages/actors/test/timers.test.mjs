import assert from "node:assert/strict";
import test from "node:test";
import { schedule, setAlarm } from "../dist/index.js";

test("guest scheduling preserves source bytes, retry identity and caller snapshot", async () => {
  const timer = { task_id: "01010101-0101-0101-0101-010101010101", operation_id: "02020202-0202-0202-0202-020202020202", deadline_unix_ms: 18446744073709551615n };
  const retained = [];
  const retain = async (timer, encoded) => {
    retained.push({ timer, encoded });
    return 42;
  };
  const pending = schedule(retain, timer);
  timer.task_id = "changed-after-call";
  assert.equal(await pending, 42);
  assert.equal(retained[0].timer.task_id, "01010101-0101-0101-0101-010101010101");
  assert.equal(new TextDecoder().decode(retained[0].encoded), '{"deadline_unix_ms":18446744073709551615,"operation_id":"02020202-0202-0202-0202-020202020202","task_id":"01010101-0101-0101-0101-010101010101"}');
  assert.ok(retained[0].encoded instanceof Uint8Array);
  const original = { ...timer, task_id: "01010101-0101-0101-0101-010101010101" };
  assert.equal(setAlarm, schedule);
  await setAlarm(retain, original);
  assert.deepEqual(retained[1], retained[0]);
  for (const invalid of [
    { ...original, deadline_unix_ms: 0n },
    { ...original, deadline_unix_ms: 18446744073709551616n },
    { ...original, task_id: "01010101010101010101010101010101" },
  ]) await assert.rejects(schedule(retain, invalid));
  assert.equal(retained.length, 2);
});
