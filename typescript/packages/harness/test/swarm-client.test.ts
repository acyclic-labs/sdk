import { expect, test } from "bun:test";
import { AgentClient, WasmStream, WireSession, initializeClientQueries } from "../src/client.js";
import { exerciseAgents, root } from "./swarm-client.scenario.js";
import { bindLocalSession } from "./session-authority.js";
import { appendRecord, conversation, receiveUntil, statusPath } from "./actor-records.fixture.js";

test("native and browser agent views use the same bounded Rust session", async () => {
  await initializeClientQueries();
  const stream = new WasmStream();
  await bindLocalSession(stream);
  try { expect(await exerciseAgents(stream)).toEqual({ selected: 2, liveRecords: 2, recovered: false }); }
  finally { stream.free(); }
});

test("agent view admission shares duplicate interest and refuses malformed input atomically", async () => {
  await initializeClientQueries();
  const stream = new WasmStream();
  await bindLocalSession(stream);
  await appendRecord(stream, `${root}/conversation`, conversation("retained", 0n));
  await appendRecord(stream, statusPath, '{"phase":"idle","cursor":1}');
  const client = new AgentClient(stream, root, 0n, 0n, 0);
  try {
    await receiveUntil(client, () => client.agentWindow(root).recordCount === 1n && client.agentStatus(root) !== null);
    const before = client.agentWindow(root);
    const repeated = Array<string>(257).fill(root);
    client.setView(repeated, new BigUint64Array(repeated.length), new BigUint64Array(repeated.length));
    expect(client.selectedAgents()).toEqual([root]);
    expect(client.agentWindow(root)).toEqual(before);
    for (const agents of [[], ["outside/agent"]]) {
      expect(() => client.setView(agents, new BigUint64Array(agents.length), new BigUint64Array(agents.length))).toThrow();
      expect(client.selectedAgents()).toEqual([root]);
    }
    expect(() => client.setView([root], new BigUint64Array(0), new BigUint64Array(1))).toThrow();
    expect(client.selectedAgents()).toEqual([root]);
  } finally { client.close(); client.free(); stream.free(); }
});

test("explicit view reservations count distinct stream paths", async () => {
  await initializeClientQueries();
  const stream = new WasmStream();
  await bindLocalSession(stream);
  const path = `${root}/conversation`;
  await appendRecord(stream, path, conversation("original", 0n));
  for (const invalid of [-1, 0, 1.5, 2 ** 32 + 1, NaN, Infinity, "1", true]) {
    expect(() => new WireSession(stream, invalid as number, 1)).toThrow();
    expect(() => new WireSession(stream, 1, invalid as number)).toThrow();
  }
  const unreserved = new WireSession(stream, undefined, undefined);
  try {
    unreserved.setView(Array.from({ length: 513 }, (_, index) => `view/${index}`), [], []);
  } finally { unreserved.free(); }
  const session = new WireSession(stream, 1, 1);
  try {
    session.setView([path, path], [], []);
    await session.openStream(path, 0n);
    expect(session.followedAgents()).toBe(1);
    expect(() => session.setView([path, `${root}/another`], [], [])).toThrow();
    expect(session.followedAgents()).toBe(1);
  } finally { session.free(); stream.free(); }
});

test("failed selected streams report once while healthy agents keep delivering", async () => {
  await initializeClientQueries();
  const stream = new WasmStream();
  await bindLocalSession(stream);
  await appendRecord(stream, `${root}/conversation`, conversation("before", 0n));
  await appendRecord(stream, statusPath, '{"phase":"idle","cursor":1}');
  const absent = `harness/agents/${"03".repeat(16)}`;
  const client = new AgentClient(stream, absent, 0n, 0n, 0);
  try {
    client.setView([absent, root], new BigUint64Array(2), new BigUint64Array(2));
    let refusals = 0;
    while (refusals < 2 || client.agentWindow(root).recordCount !== 1n || client.agentStatus(root) === null) {
      try { await client.next(); }
      catch (error) {
        expect(error).toBeInstanceOf(Error);
        expect((error as Error).name).not.toBe("not_open");
        refusals++;
      }
    }
    expect(refusals).toBe(2);
    await appendRecord(stream, `${root}/conversation`, conversation("after", 1n));
    await receiveUntil(client, () => client.agentWindow(root).recordCount === 2n);
    expect(client.agentWindow(root).nextSequence).toBe(2n);
  } finally { client.close(); client.free(); stream.free(); }
});
