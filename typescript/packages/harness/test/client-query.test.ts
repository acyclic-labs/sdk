import { beforeAll, expect, test } from "bun:test";
import { AgentClient, WasmStream, initializeClientQueries } from "../src/client.js";
import { bindLocalSession } from "./session-authority.js";
import { appendRecord, conversation, message, receiveUntil, record, root, statusPath } from "./actor-records.fixture.js";

beforeAll(initializeClientQueries);

async function fixture(inbox = 1n) {
  const stream = new WasmStream();
  await bindLocalSession(stream);
  await appendRecord(stream, `${root}/conversation`, conversation("first", inbox));
  await appendRecord(stream, statusPath, '{"phase":"idle","cursor":0}');
  const client = new AgentClient(stream, root, 0n, 0n, 0);
  const read = () => receiveUntil(client, () => client.recordCount() === 1 && client.currentStatus() !== null);
  return { stream, client, read, free() { client.close(); client.free(); stream.free(); } };
}

test("actor deliveries and live getters read the same already-folded record", async () => {
  const f = await fixture();
  try {
    expect(f.client.recordCount()).toBe(0);
    expect(f.client.record(0)).toBeNull();
    expect(f.client.currentStatus()).toBeNull();
    let delivered = false;
    while (!delivered) {
      const event = await f.client.next();
      if (event.kind !== "conversation") continue;
      delivered = true;
      expect(event).toEqual(record(f.client, root, 0));
      expect(message(event).id).toBe("first");
      expect(f.client.agentWindow(root).nextSequence).toBe(event.sequence + 1n);
    }
    const approval = "approval-01010101010101010101010101010101";
    await appendRecord(f.stream, statusPath, JSON.stringify({ phase: { waiting_approval: { id: approval } }, cursor: 1 }));
    await receiveUntil(f.client, () => {
      const phase = f.client.currentStatus()?.phase;
      return typeof phase === "object" && "waiting_approval" in phase;
    });
    expect(f.client.agentStatus(root)).toEqual({ phase: { waiting_approval: { id: approval } }, cursor: 1n });
  } finally { f.free(); }
});

test("a failed UI render does not discard settled records or reorder later arrivals", async () => {
  const f = await fixture();
  try {
    await f.read();
    const first = record(f.client, root, 0);
    expect(() => { throw new Error("output rejected"); }).toThrow("output rejected");
    expect(record(f.client, root, 0)).toEqual(first);
    await appendRecord(f.stream, `${root}/conversation`, conversation("later", 2n));
    await receiveUntil(f.client, () => f.client.recordCount() === 2);
    expect([0, 1].map(index => message(record(f.client, root, index)).id)).toEqual(["first", "later"]);
    const replay = new AgentClient(f.stream, root, 0n, 0n, 0);
    try {
      await receiveUntil(replay, () => replay.recordCount() === 2);
      expect([replay.record(0), replay.record(1)]).toEqual([f.client.record(0), f.client.record(1)]);
    } finally { replay.close(); replay.free(); }
  } finally { f.free(); }
});

test("reopening at the explicit cursor excludes consumed records", async () => {
  const f = await fixture();
  try {
    await f.read();
    const cursor = f.client.agentWindow(root).nextSequence;
    const resumed = new AgentClient(f.stream, root, cursor, 1n, 0);
    try {
      expect(resumed.recordCount()).toBe(0);
      await appendRecord(f.stream, `${root}/conversation`, conversation("next", 2n));
      await receiveUntil(resumed, () => resumed.recordCount() === 1);
      expect(record(resumed, root, 0).sequence).toBe(cursor);
      expect(message(record(resumed, root, 0)).id).toBe("next");
      expect(resumed.agentWindow(root).nextSequence).toBe(cursor + 1n);
    } finally { resumed.close(); resumed.free(); }
  } finally { f.free(); }
});

for (const inbox of [1n, 9007199254740993n, 18446744073709551615n]) {
  test(`actor input correlation retains the full u64 through WASM: ${inbox}`, async () => {
    const f = await fixture(inbox);
    try {
      await f.read();
      expect(message(record(f.client, root, 0)).inbox).toBe(inbox);
      expect(record(f.client, root, 0).entry.output.descriptor.byte_length).toBe(0n);
    } finally { f.free(); }
  });
}

test("identical record payloads retain their separate stream origins on reopen", async () => {
  const f = await fixture();
  try {
    await appendRecord(f.stream, `${root}/conversation`, conversation("first", 1n));
    await receiveUntil(f.client, () => f.client.recordCount() === 2);
    const entries = [record(f.client, root, 0), record(f.client, root, 1)];
    expect(entries[0]!.entry).toEqual(entries[1]!.entry);
    expect(entries.map(entry => entry.sequence)).toEqual([0n, 1n]);
    const replay = new AgentClient(f.stream, root, 0n, 0n, 0);
    try {
      await receiveUntil(replay, () => replay.recordCount() === 2);
      expect([replay.record(0), replay.record(1)]).toEqual(entries);
    } finally { replay.close(); replay.free(); }
  } finally { f.free(); }
});

test("closing releases the view and subsequent reads refuse with a typed error", async () => {
  const f = await fixture();
  try {
    await f.read();
    f.client.close();
    expect(() => f.client.record(0)).toThrow();
    await expect(f.client.next()).rejects.toMatchObject({ name: "client_closed" });
  } finally { f.free(); }
});

test("invalid agent identities and missing session authority are typed refusals", () => {
  const stream = new WasmStream();
  try {
    try { new AgentClient(stream, "outside/agent", 0n, 0n, 0); throw new Error("path admitted"); }
    catch (error) { expect(error).toMatchObject({ name: "invalid_agent_path" }); }
    try { new AgentClient(stream, root, 0n, 0n, 0); throw new Error("unbound client admitted"); }
    catch (error) { expect(error).toMatchObject({ name: "client_no_session" }); }
  } finally { stream.free(); }
});
