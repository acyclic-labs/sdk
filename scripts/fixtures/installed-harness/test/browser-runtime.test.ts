import "fake-indexeddb/auto";
import { expect, test } from "bun:test";
import {
  BrowserTaskAuthority, BrowserTaskRegistry, BrowserTaskRuntime,
  initializeBrowserRuntime, type BrowserTaskOptions, type BrowserWorkLease, type BrowserRecoveredWork,
} from "@acyclic-labs/harness/browser";

// Package-resolution/consumer qualification. Real Chromium tests separately
// qualify platform IndexedDB, worker termination and competing connections.
test("installed browser entry executes the ordinary registered runtime", async () => {
  await initializeBrowserRuntime();
  const id = crypto.randomUUID();
  const owner = new BrowserTaskAuthority({ kind: "task", id }, "installed-browser", new Uint8Array(32).fill(23), []);
  const registry = new BrowserTaskRegistry();
  let runtime: BrowserTaskRuntime | undefined;
  try {
    const agent = "15151515-1515-1515-1515-151515151515";
    const provider = { namespace: "installed-browser", family: "filesystem", version: "2" };
    const volume = { provider, id: "private", class: "agent_private" as const, owner: { kind: "agent" as const, id: agent } };
    const options: BrowserTaskOptions = {
      filesystem_database: `installed-fs-${id}`, stream_database: `installed-stream-${id}`,
      maximum_object_bytes: 1048576n, maximum_payload_bytes: 65536n, filesystem_provider: provider, volume,
      stream_limits: { commands: 1000n, journal_bytes: 1048576n, paths: 100, path_bytes: 65536,
        records: 1000, payload_bytes: 1048576, commits: 1000, idempotency_results: 1000 },
      limits: { file_bytes: 65536n, path_bytes: 1024n, attachments: 4n, render_bytes: 65536n,
        model_steps: 4n, model_events_per_step: 16n, tool_calls_per_step: 4n, context_messages: 16n },
      run_limits: { concurrency: 1n, max_steps: 4n, deadline_epoch_ms: null },
      session_limits: { active_tasks: 1n, total_tasks: 3n, depth: 1, model_steps: 4n }, concurrency: 1,
    };
    const scope = owner.issueScopeForAgent(agent, "owner", [
      "operation:declare", "operation:observe", "operation:cancel", "operation:wake", "task:spawn:installed.complete@1",
      "task:spawn:acyclic.stock_turn@1", "model:generate",
      owner.volumeCapability(volume, "read"), owner.volumeCapability(volume, "write"),
    ]);
    expect(scope.agent).toBe(agent);
    expect(scope.parent_proof).toBeNull();
    const outputSchema: BrowserWorkLease["operation"]["entrypoint"]["result_schema"] = {
      type: "integer", default: { sha256: "literal", byte_length: 7n, media_type: "literal" },
    };
    registry.registerMachine({ name: "installed.complete", version: "1", digest: new Array(32).fill(41),
      state_schema: { type: "integer" }, input_schema: { type: "integer" }, output_schema: outputSchema, requirements: [] },
    (input: unknown) => input, (state: unknown) => ({ state, commands: [], status: { kind: "completed", value: state } }));
    registry.registerStockTurn();
    expect(() => registry.registerStockTurn()).toThrow();
    runtime = await BrowserTaskRuntime.open(options, owner, registry, scope);
    await runtime.initializeVolume();
    const unrelated = "00000000-0000-0000-0000-000000000001";
    const untouched = await runtime.admit(unrelated, "installed.complete", "1", 3);
    expect(untouched.kind).toBe("accepted");
    const admission = await runtime.admit("ffffffff-ffff-ffff-ffff-ffffffffffff", "installed.complete", "1", 7);
    expect(admission.kind).toBe("accepted");
    if (admission.kind !== "accepted") throw new Error(`admission ${admission.kind}`);
    const worker = { id: "installed-worker", available: {}, labels: {} };
    expect((await runtime.runOperation(worker, admission.task_id, 2))?.kind).toBe("completed");
    expect(await runtime.outcome(unrelated)).toBeNull();
    const recovery: BrowserRecoveredWork = await runtime.recoverWork(unrelated);
    expect(recovery).toEqual({ kind: "idle" });
    expect(await runtime.runOperation(worker, admission.task_id, 2)).toBeNull();
    const expected: Awaited<ReturnType<BrowserTaskRuntime["outcome"]>> = { Succeeded: 7n };
    expect(await runtime.outcome(admission.task_id)).toEqual(expected);
    const tick = await runtime.workerTick(worker, null, 8, 2);
    expect(tick.work?.kind).toBe("completed");
    expect(await runtime.outcome(unrelated)).toEqual({ Succeeded: 3n });

    let generations = 0;
    runtime.configureModel({ provider: "installed", name: "stock", revision: "1", options: {} },
      async function* () {
        generations++;
        yield { kind: "content", delta: "stock result" };
        yield { kind: "completed", metadata: {} };
      }, () => null,
      selection => {
        expect(selection.revision).toBe("1");
        return { context_tokens: 128, output_tokens: 16 };
      }, (bytes, request_digest) => {
        const request = JSON.parse(new TextDecoder().decode(bytes));
        return { request_digest, fixed_tokens: 1, message_tokens: request.messages.map(() => 2) };
      }, { kind: "disabled" });
    const command = crypto.randomUUID();
    const payload = await runtime.stage(command, "stock-model", new TextEncoder().encode(JSON.stringify({ input: "hello", max_steps: 1 })));
    const operation = crypto.randomUUID();
    const input = { operation_id: command, kind: "acyclic.model.v1", payload };
    const stock = await runtime.admit(operation, "acyclic.stock_turn", "1", input);
    expect(stock.kind).toBe("accepted");
    if (stock.kind !== "accepted") throw new Error(`stock admission ${stock.kind}`);
    expect(await runtime.admit(operation, "acyclic.stock_turn", "1", input)).toEqual(stock);
    await expect(runtime.admit(operation, "acyclic.stock_turn", "1", { ...input, operation_id: crypto.randomUUID() })).rejects.toThrow();
    expect((await runtime.runOperation(worker, stock.task_id, 4))?.kind).toBe("completed");
    expect(await runtime.outcome(stock.task_id)).toEqual({ Succeeded: { text: "stock result", metadata: {}, attachments: [], steps: 1n } });
    expect(generations).toBe(1);
    expect(await runtime.runOperation(worker, stock.task_id, 4)).toBeNull();
    expect(generations).toBe(1);
  } finally {
    runtime?.free();
    registry.free();
    owner.free();
  }
});
