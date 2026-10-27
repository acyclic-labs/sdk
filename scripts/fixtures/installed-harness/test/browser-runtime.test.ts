import "fake-indexeddb/auto";
import { expect, test } from "bun:test";
import {
  BrowserAgentAuthority, BrowserAgentRegistry, BrowserAgents,
  initializeBrowserRuntime, type BrowserAgentOptions, type BrowserAgentRecord,
} from "@acyclic-labs/harness/browser";

// Package-resolution/consumer qualification. Real Chromium tests separately
// qualify platform IndexedDB and reopen.
test("installed browser entry hosts an agent turn", async () => {
  await initializeBrowserRuntime();
  const id = crypto.randomUUID();
  const owner = new BrowserAgentAuthority({ kind: "task", id }, "installed-browser", new Uint8Array(32).fill(23), []);
  const registry = new BrowserAgentRegistry();
  let agents: BrowserAgents | undefined;
  try {
    let generations = 0;
    registry.configureModel({ provider: "installed", name: "stock", revision: "1", options: {} },
      async function* (_bytes, _dispatch, _signal, files) {
        generations++;
        yield { kind: "content", delta: `stock:${new TextDecoder().decode(files.at(-1))}` };
        yield { kind: "completed", metadata: {} };
      }, () => null,
      selection => {
        expect(selection.revision).toBe("1");
        return { context_tokens: 1024, output_tokens: 16 };
      }, (bytes, request_digest) => {
        const request = JSON.parse(new TextDecoder().decode(bytes));
        return { request_digest, fixed_tokens: 1, message_tokens: request.messages.map(() => 2) };
      }, { kind: "disabled" });
    const options: BrowserAgentOptions = {
      filesystem_database: `installed-fs-${id}`, stream_database: `installed-stream-${id}`,
      maximum_object_bytes: 1048576n, filesystem_provider: { namespace: "installed-browser", family: "filesystem", version: "2" },
      stream_limits: { commands: 1000n, journal_bytes: 1048576n, paths: 100, path_bytes: 65536,
        records: 1000, payload_bytes: 1048576, commits: 1000, idempotency_results: 1000 },
      limits: { file_bytes: 65536n, path_bytes: 1024n, attachments: 4n, render_bytes: 65536n,
        model_steps: 4n, model_events_per_step: 16n, tool_calls_per_step: 4n, context_messages: 16n },
      principal: "installed-user",
      approval_policy: { approvers: [{ kind: "rules", principal: "installed-user", rules: [], fallback: "allow" }] },
    };
    agents = await BrowserAgents.open(options, owner, registry, undefined);
    const agent = await agents.create("installed-agent", "hello");
    expect(await agents.create("installed-agent", "hello")).toBe(agent);
    let entries: BrowserAgentRecord[] = [];
    while (entries.length === 0) {
      entries = await agents.read(agent, "conversation", 0n, 4);
      if (entries.length === 0) await new Promise(resolve => setTimeout(resolve, 10));
    }
    const output = (entries[0].value as { output: unknown }).output;
    const text = JSON.parse(new TextDecoder().decode(await agents.readFile(agent, output as never))).text;
    expect({ text, generations }).toEqual({ text: "stock:hello", generations: 1 });
    expect((await agents.list(null, null, null, 8)).agents).toEqual([agent]);
    expect(await agents.archive(agent, "done")).toEqual({ kind: "committed", inbox: 1n });
    expect(await agents.send(agent, "late", "ignored")).toEqual({ kind: "closed" });
  } finally {
    agents?.free();
    registry.free();
    owner.free();
  }
});
