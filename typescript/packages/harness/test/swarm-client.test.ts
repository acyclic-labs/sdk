import { expect, test } from "bun:test";
import { AgentClient, WasmStream, initializeClientQueries } from "../src/client.js";
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

test("agent view admission refuses oversized and malformed input atomically", async () => {
  await initializeClientQueries();
  const stream = new WasmStream();
  await bindLocalSession(stream);
  const client = new AgentClient(stream, root, 0n, 0n, 0);
  try {
    for (const agents of [[], Array(9).fill(root), ["outside/agent"]]) {
      expect(() => client.setView(agents, new BigUint64Array(agents.length), new BigUint64Array(agents.length))).toThrow();
      expect(client.selectedAgents()).toEqual([root]);
    }
    expect(() => client.setView([root], new BigUint64Array(0), new BigUint64Array(1))).toThrow();
    expect(client.selectedAgents()).toEqual([root]);
  } finally { client.close(); client.free(); stream.free(); }
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
