import { AgentClient, WasmStream, initializeClientQueries } from "../src/client.js";
import { appendRecord, conversation } from "./actor-records.fixture.js";

const rootId = "01".repeat(16), childId = "02".repeat(16);
export const root = `harness/agents/${rootId}`;
export const child = `${root}/children/${childId}`;
const inbox = 9007199254740993n;
function check(value: unknown, message: string): asserts value { if (!value) throw new Error(message); }

// This fixture feeds the public actor record contract to the real shared Rust
// client. It has no TaskVolume, private transport, membership map or file fold.
export async function exerciseAgents(stream: WasmStream, recover?: () => Promise<void>) {
  await initializeClientQueries();
  const append = (path: string, value: string) => appendRecord(stream, path, value);
  const entry = (id: string) => conversation(id, inbox);
  for (const [agent, id] of [[root, rootId], [child, childId]]) {
    await append(`${agent}/conversation`, entry(id));
    await append(`harness/status/${id}`, `{"phase":"running","cursor":${inbox}}`);
  }
  const client = new AgentClient(stream, root, 0n, 0n, 0);
  const record = (agent: string, index: number) => {
    const event = client.agentRecord(agent, index);
    check(event?.kind === "conversation", "selected conversation record is missing");
    return event;
  };
  const message = (agent: string, index: number) => {
    const input = record(agent, index).entry.input;
    check("message" in input, "conversation input is not a message");
    return input.message;
  };
  const selected = (agents: string[], from = 0n) => client.setView(agents,
    new BigUint64Array(agents.map(() => from)), new BigUint64Array(agents.map(() => 0n)));
  const until = async (condition: () => boolean) => { while (!condition()) await client.next(); };
  try {
    selected([root, child]);
    await until(() => client.agentWindow(root).recordCount === 1n
      && client.agentWindow(child).recordCount === 1n
      && client.agentStatus(root) !== null && client.agentStatus(child) !== null);
    check(record(child, 0).agent === child, "record lost its selected agent identity");
    check(message(root, 0).inbox === inbox, "input receipt lost u64 precision");
    check(client.agentStatus(child)?.cursor === inbox, "status cursor lost u64 precision");
    const before = client.agentWindow(root), revision = client.viewRevision();
    selected([child, root]);
    check(client.selectedAgents().join(",") === `${child},${root}`, "focus did not move");
    check(client.agentWindow(root).nextSequence === before.nextSequence, "promotion reset its cursor");
    check(client.agentWindow(root).recordCount === before.recordCount, "promotion discarded its records");
    check(client.viewRevision() > revision, "focus change did not invalidate the frame");
    if (recover) await recover();
    await append(`${root}/conversation`, entry("live"));
    await until(() => client.agentWindow(root).recordCount === 2n);
    check(message(root, 1).id === "live", "live record lost or duplicated");
    selected([child]);
    check(client.selectedAgents().length === 1, "demotion retained an agent");
    let refused = false;
    try { client.agentWindow(root); } catch (error) { refused = error instanceof Error && error.name === "not_requested"; }
    check(refused, "unrequested authority was not a typed refusal");
    selected([child, root], 2n);
    check(client.agentWindow(root).recordCount === 0n, "new selection retained an evicted window");
    check(client.agentWindow(root).nextSequence === 2n, "new selection ignored its explicit cursor");
    await append(`${root}/conversation`, entry("after-selection"));
    await until(() => client.agentWindow(root).recordCount === 1n);
    check(record(root, 0).sequence === 2n, "reselection created a hole or duplicate");
    return { selected: 2, liveRecords: 2, recovered: !!recover };
  } finally { client.close(); client.free(); }
}
