import "fake-indexeddb/auto";
import { expect, test } from "bun:test";
import { fromBinary } from "@bufbuild/protobuf";
import {
  BrowserAgentAuthority, BrowserAgentRegistry, BrowserAgents,
  decodeAgentRecord, decodeReadResponse, initializeBrowserRuntime, type BrowserAgentOptions,
} from "@acyclic-labs/harness/browser";
import { ReadResponseSchema, RecordBatchSchema } from "./fixtures/stream/stream/v1/stream_pb.js";
import { fixtureAuthority, initializeFixtureAuthority } from "./fixtures/authority/fixture-authority.mjs";

// Package-resolution/consumer qualification. Real Chromium tests separately
// qualify platform IndexedDB and reopen.
test("installed browser entry hosts an agent turn", async () => {
  await initializeBrowserRuntime();
  await initializeFixtureAuthority();
  const id = crypto.randomUUID();
  const authority = fixtureAuthority(id);
  const owner = new BrowserAgentAuthority(authority.capability);
  const registry = new BrowserAgentRegistry();
  let agents: BrowserAgents | undefined;
  try {
    let generations = 0;
    registry.configureModel({ provider: "installed", name: "stock", revision: "1" },
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
        return { request_digest, input_tokens: BigInt(1 + 2 * request.messages.length),
          fixed_tokens: 1, message_tokens: request.messages.map(() => 2) };
      }, { kind: "disabled" });
    const options: BrowserAgentOptions = {
      filesystem_database: `installed-fs-${id}`, stream_database: `installed-stream-${id}`,
      maximum_object_bytes: 1048576n, filesystem_provider: { namespace: "installed-browser", family: "filesystem", version: "2" },
      stream_limits: { commands: 1000n, journal_bytes: 1048576n, paths: 100, path_bytes: 65536,
        records: 1000, payload_bytes: 1048576, commits: 1000, idempotency_results: 1000 },
      limits: { file_bytes: 65536n, path_bytes: 1024n, attachments: 4n, render_bytes: 65536n,
        model_steps: 4n, model_events_per_step: 16n, tool_calls_per_step: 4n, context_messages: 16n },
      principal: "installed-user",
      preset: { tools: [], web_tools: false, max_children: null,
        approval_policy: { approvers: [{ kind: "rules", principal: "installed-user", rules: [], fallback: "allow" }] } },
    };
    let deliveryFailed!: (error: Error) => void;
    const failed = new Promise<never>((_, reject) => { deliveryFailed = reject; });
    void failed.catch(() => {});
    agents = await BrowserAgents.open(options, owner, registry,
      (actor, error) => deliveryFailed(new Error(`agent ${actor} failed: ${error}`)));
    const completed = agents.completion();
    void completed.catch(() => {});
    const agent = await agents.create("installed-agent", "hello");
    expect(await agents.create("installed-agent", "hello")).toBe(agent);
    const cursor = await agents.follow(agent, "conversation", 0n);
    let output: unknown;
    try {
      for (;;) {
        const bytes = await Promise.race([cursor.next(), failed, completed]);
        if (bytes === null) throw new Error("conversation closed before its required record");
        const frame = fromBinary(ReadResponseSchema, bytes);
        const batch = fromBinary(RecordBatchSchema, decodeReadResponse(frame.codec, frame.data, frame.decodedLength));
        if (batch.records.length === 0) continue;
        expect(batch.records[0].sequence).toBe(0n);
        const record = decodeAgentRecord(batch.records[0].value) as { output: unknown };
        output = record.output;
        const unsupported = JSON.parse(new TextDecoder().decode(batch.records[0].value));
        unsupported.version++;
        expect(() => decodeAgentRecord(new TextEncoder().encode(JSON.stringify(unsupported))))
          .toThrow(/version/i);
        break;
      }
    } finally { cursor.close(); cursor.free(); }
    const text = JSON.parse(new TextDecoder().decode(await agents.readFile(agent, output as never))).text;
    expect({ text, generations }).toEqual({ text: "stock:hello", generations: 1 });
    expect((await agents.list(null, null, null, 8)).agents).toEqual([agent]);
    expect(await agents.archive(agent, "done")).toEqual({ kind: "committed", inbox: 1n });
    expect(await agents.send(agent, "late", "ignored")).toEqual({ kind: "closed" });
    agents.free();
    agents = undefined;
    await expect(completed).rejects.toBe("browser agent runtime aborted");

    // The real journal refuses the original dispatcher's first commit. Its
    // provider remains readable while active and late host observers fail.
    agents = await BrowserAgents.open({ ...options,
      filesystem_database: `${options.filesystem_database}-failure`,
      stream_database: `${options.stream_database}-failure`,
      stream_limits: { ...options.stream_limits, journal_bytes: 1n },
    }, owner, registry, undefined);
    const cause = await agents.completion().then(
      () => { throw new Error("failed host completed successfully"); },
      error => String(error),
    );
    expect(cause).toMatch(/capacity/i);
    await expect(agents.completion()).rejects.toBe(cause);
    expect((await agents.list(null, null, null, 8)).agents).toEqual([]);
  } finally {
    agents?.free();
    authority.close();
    registry.free();
    owner.free();
    authority.free();
  }
});
