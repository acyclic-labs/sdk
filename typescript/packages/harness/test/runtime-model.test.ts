import { test, expect } from "bun:test";
import { type TaskId, type HarnessRuntimeSpawner, type HarnessRuntimeState, policyIdentity, Task, type TaskMessage, defineTool, type EffectId, Harness, type ModelMessage, descriptorFor, type UserContentPart, ExecutionScope, type ConversationMessageId, TaskDefinition, jsonToolProjection } from "../src/index.js";
import { mailboxFile, durableDigest, parseNumber, jsonProjection, contracts, testModel, imagePolicy, parseNull } from "./support/runtime-fixture.js";
import { syntheticAccounting } from "./support/model-accounting.mjs";

test("executes model tool calls with durable task ownership and typed receipts", async () => {
  let modelStep = 0; let sender: TaskId | undefined; let toolOperationId: string | undefined;
  const payload = await mailboxFile(3);
  const host: HarnessRuntimeSpawner & HarnessRuntimeState = {
    policyIdentity: () => null,
    async attach(id) { return { task: new Task(id, async () => undefined), operationId: "operation:run", taskName: "acyclic.default-agent", revision: "1", implementationDigest: durableDigest }; },
    async reconcileEffect() { return { state: "succeeded", result: payload } as const; },
    async send(message: TaskMessage) { sender = message.sender; return { accepted: true, messageId: message.id }; },
    async *inbox() { yield* [] as TaskMessage[]; },
  }
  const tool = await defineTool<number, number>({ name: "double", revision: "1", description: "double", inputSchema: {}, outputSchema: {}, parseInput: parseNumber, projection: jsonProjection, parseOutput: parseNumber }, async (context, input) => {
    toolOperationId = context.operationId;
    const effect = await context.reconcileEffect("effect:tool" as EffectId);
    await context.send(context.taskId!, payload);
    return effect.state === "succeeded" ? input * 4 : 0;
  });
  const runtime = await Harness.builder(contracts).state(host).spawner(host).tool(tool).grant("tool:call:double").model(testModel, {
    ...syntheticAccounting(contracts), async *generate() { if (modelStep++ === 0) { yield { kind: "tool_call" as const, callId: "call", name: "double", arguments: 3 }; yield { kind: "completed" as const, metadata: {} }; } else { yield { kind: "content" as const, delta: "done" }; yield { kind: "completed" as const, metadata: { tokens: 1 } }; } },
    async reconcile() { return undefined; },
  }).build();
  const output = await runtime.run("go");
  expect(output.text).toBe("done");
  expect(output.receipts).toEqual([{ kind: "model-completed", metadata: {} }, { kind: "tool", step: 0, callId: "call", name: "double", arguments: 3, value: 12, projection: { kind: "json", value: 12 } }, { kind: "model-completed", metadata: { tokens: 1 } }]);
  expect(sender).toBe(output.taskId);
  expect(toolOperationId).toMatch(/^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/i);
  expect(await (await runtime.attach(output.taskId)).result()).toMatchObject({ kind: "succeeded", value: { text: "done" } });
});

test("typed run input carries attachment refs into the model context", async () => {
  let observed: readonly ModelMessage[] = [];
  const file = {
    volume: { provider: { namespace: "test", family: "filesystem", version: "2" }, id: "project", class: "project" as const, owner: { kind: "project" as const, id: "project" } },
    path: "images/chart.png", version: "generation", descriptor: await descriptorFor(new Uint8Array([1, 2]), "image/png"), display_name: "chart.png",
  };
  const runtime = await Harness.builder(contracts).model(testModel, {
    ...syntheticAccounting(contracts), async *generate(request) { observed = request.messages; yield { kind: "completed" as const, metadata: {} }; },
    async reconcile() { return undefined; },
  }).build();
  await runtime.run({ prompt: "describe", content: [{ kind: "file", file, policy: imagePolicy }] });
  expect(observed[0]?.content).toEqual([{ kind: "text", text: "describe" }, { kind: "file", file, policy: imagePolicy }]);
});

test("content-only turns dispatch exactly the content admitted by Rust validation", async () => {
  let observed: readonly ModelMessage[] = [];
  const file = {
    volume: { provider: { namespace: "test", family: "filesystem", version: "2" }, id: "project", class: "project" as const, owner: { kind: "project" as const, id: "project" } },
    path: "images/chart.png", version: "generation", descriptor: await descriptorFor(new Uint8Array([1, 2]), "image/png"), display_name: "chart.png",
  };
  const runtime = await Harness.builder(contracts).model(testModel, {
    ...syntheticAccounting(contracts), async *generate(request) { observed = request.messages; yield { kind: "completed" as const, metadata: {} }; },
    async reconcile() { return undefined; },
  }).build();
  await runtime.run({ prompt: "", content: [{ kind: "file", file, policy: imagePolicy }] });
  expect(observed[0]?.content).toEqual([{ kind: "file", file, policy: imagePolicy }]);
});

test("direct input is snapshotted before validation and model dispatch", async () => {
  let observed: readonly ModelMessage[] = [];
  let contentReads = 0;
  const file = {
    volume: { provider: { namespace: "test", family: "filesystem", version: "2" }, id: "project", class: "project" as const, owner: { kind: "project" as const, id: "project" } },
    path: "images/chart.png", version: "generation", descriptor: await descriptorFor(new Uint8Array([1, 2]), "image/png"), display_name: "chart.png",
  };
  const validContent: readonly UserContentPart[] = [{ kind: "file", file, policy: imagePolicy }];
  const input = {
    prompt: "",
    get content(): readonly UserContentPart[] {
      contentReads += 1;
      return contentReads === 1 ? validContent : [{ kind: "text", text: "" }];
    },
  };
  const runtime = await Harness.builder(contracts).model(testModel, {
    ...syntheticAccounting(contracts), async *generate(request) { observed = request.messages; yield { kind: "completed" as const, metadata: {} }; },
    async reconcile() { return undefined; },
  }).build();
  await runtime.run(input);
  expect(contentReads).toBe(1);
  expect(observed[0]?.content).toEqual(validContent);
});

test("model provider callbacks are captured once per binding and retain their receiver", async () => {
  type Provider = import("../src/model.js").ModelProvider;
  type StatefulProvider = Provider & { dispatches: number };
  const accounting = syntheticAccounting(contracts);
  const reads = { contextCapacity: 0, countTokens: 0, generate: 0, reconcile: 0 };
  const called: string[] = [];
  let admitted: Parameters<Provider["generate"]>[0] | undefined;
  const provider: StatefulProvider = {
    dispatches: 0,
    contextCapacity(this: StatefulProvider, model) {
      expect(this).toBe(provider);
      called.push("capacity");
      return accounting.contextCapacity(model);
    },
    countTokens(this: StatefulProvider, request) {
      expect(this).toBe(provider);
      called.push("count");
      return accounting.countTokens(request);
    },
    async *generate(this: StatefulProvider, request) {
      expect(this).toBe(provider);
      this.dispatches++;
      admitted = request;
      yield { kind: "completed" as const, metadata: {} };
    },
    async reconcile(this: StatefulProvider) {
      expect(this).toBe(provider);
      called.push("reconcile");
      return undefined;
    },
  };
  const callbacks = { contextCapacity: provider.contextCapacity, countTokens: provider.countTokens,
    generate: provider.generate, reconcile: provider.reconcile };
  const names = Object.keys(callbacks) as (keyof typeof callbacks)[];
  for (const name of names) Object.defineProperty(provider, name, {
    configurable: true, get() { reads[name]++; return callbacks[name]; },
  });
  const runtime = await Harness.builder(contracts).model(testModel, provider).build();
  const scope = new ExecutionScope().model({ ...testModel, revision: "scoped" }, provider);
  const scoped = await runtime.scoped(scope);
  expect(reads).toEqual({ contextCapacity: 2, countTokens: 2, generate: 2, reconcile: 2 });
  for (const name of names) Object.defineProperty(provider, name, {
    configurable: true, value() { throw new Error("replacement must require a new binding"); },
  });
  await runtime.run("root");
  await scoped.run("child");
  expect(provider.dispatches).toBe(2);
  const binding = scope.modelBinding;
  if (!binding || !admitted) throw new Error("expected an admitted scoped request");
  expect(Object.isFrozen(binding.provider)).toBe(true);
  expect(binding.provider.contextCapacity(binding.identity)).toEqual(accounting.contextCapacity(binding.identity));
  const count = binding.provider.countTokens(admitted);
  expect(count).toEqual(accounting.countTokens(admitted));
  await binding.provider.reconcile({ operationId: "binding-probe", step: 0,
    requestDigest: count.requestDigest, observed: [] });
  expect(called).toEqual(["capacity", "count", "reconcile"]);
  expect(reads).toEqual({ contextCapacity: 2, countTokens: 2, generate: 2, reconcile: 2 });
});

test("model provider callbacks must all be present before binding", () => {
  for (const name of ["contextCapacity", "countTokens", "generate", "reconcile"]) {
    let dispatches = 0;
    const provider = { ...syntheticAccounting(contracts), async *generate() {
      dispatches++; yield { kind: "completed" as const, metadata: {} };
    }, async reconcile() { return undefined; } };
    Object.defineProperty(provider, name, { value: undefined });
    expect(() => Harness.builder(contracts).model(testModel, provider)).toThrow();
    expect(() => new ExecutionScope().model(testModel, provider)).toThrow();
    expect(dispatches).toBe(0);
  }
});

test("model identity and options are pinned at binding, including scoped overrides", async () => {
  const rootIdentity = { provider: "root", name: "model", revision: "3", options: { mode: "original" } };
  const seen: unknown[] = [];
  const budgets: (number | undefined)[] = [];
  const provider = { ...syntheticAccounting(contracts), async *generate(request: { model: unknown; maxOutputTokens?: number }) { seen.push(request.model); budgets.push(request.maxOutputTokens); yield { kind: "completed" as const, metadata: {} }; },
    async reconcile() { return undefined; } };
  const runtime = await Harness.builder(contracts).model(rootIdentity, provider).modelOutputTokens(8_192).build();
  rootIdentity.options.mode = "mutated";
  await runtime.run("root");
  expect(seen[0]).toEqual({ provider: "root", name: "model", revision: "3", options: { mode: "original" } });
  const childIdentity = { provider: "child", name: "model", revision: "4", options: { mode: "scoped" } };
  const scoped = await runtime.scoped(new ExecutionScope().model(childIdentity, provider));
  childIdentity.options.mode = "mutated";
  await scoped.run("child");
  expect(seen[1]).toEqual({ provider: "child", name: "model", revision: "4", options: { mode: "scoped" } });
  expect(budgets).toEqual([8_192, 8_192]);
  await (await Harness.builder(contracts).model(testModel, provider).build()).run("without a budget");
  expect(budgets).toEqual([8_192, 8_192, undefined]);
  expect(() => Harness.builder(contracts).modelOutputTokens(0)).toThrow();
  expect(() => Harness.builder(contracts).modelOutputTokens(2 ** 32)).toThrow();
  expect(() => Harness.builder(contracts).model({ provider: "", name: "model", revision: "1", options: {} }, provider)).toThrow();
});

test("selected context preserves canonical roles without a synthetic user duplicate", async () => {
  let observed: readonly ModelMessage[] = [];
  const runtime = await Harness.builder(contracts).model(testModel, {
    ...syntheticAccounting(contracts), async *generate(request) { observed = request.messages; yield { kind: "completed" as const, metadata: {} }; },
    async reconcile() { return undefined; },
  }).build();
  await runtime.runSelectedContext({
    selection: { conversationRevision: 2n, messageIds: [
      ("01010101-0101-0101-0101-010101010101" as ConversationMessageId), ("02020202-0202-0202-0202-020202020202" as ConversationMessageId),
    ] },
    messages: [
      { role: "assistant", content: "earlier answer" },
      { role: "user", content: [{ kind: "text", text: "follow-up" }] },
    ],
  });
  expect(observed).toHaveLength(2);
  expect(observed[0]?.role).toBe("assistant");
  expect(observed[1]?.content).toEqual([{ kind: "text", text: "follow-up" }]);
  await runtime.runSelectedContext({
    selection: { conversationRevision: 257n, messageIds: Array.from({ length: 257 }, (_, index) =>
      (`${(index + 1).toString(16).padStart(8, "0")}-0000-0000-0000-000000000000` as ConversationMessageId)) },
    messages: Array.from({ length: 257 }, (_, index) => ({ role: "user" as const,
      content: index === 0 ? "x".repeat(128 * 1024 + 1) : "retained history" })),
  });
  expect(observed).toHaveLength(257);
  expect(observed[0]?.content).toHaveLength(128 * 1024 + 1);
  await expect(runtime.run({ prompt: "conflicting prompt", selectedContext: {
    selection: { conversationRevision: 1n, messageIds: [("02020202-0202-0202-0202-020202020202" as ConversationMessageId)] },
    messages: [{ role: "user", content: "recorded prompt" }],
  } } as never)).rejects.toThrow();
});

test("builder limits bound selected history and direct input before model dispatch", async () => {
  let dispatched = 0;
  const runtime = await Harness.builder(contracts).limits({ context_messages: 1, render_bytes: 8 }).model(testModel, {
    ...syntheticAccounting(contracts), async *generate() { dispatched++; yield { kind: "completed" as const, metadata: null }; },
    async reconcile() { return undefined; },
  }).build();
  await expect(runtime.run("nine bytes")).rejects.toThrow();
  await expect(runtime.runSelectedContext({
    selection: { conversationRevision: 2n, messageIds: [
      ("01010101-0101-0101-0101-010101010101" as ConversationMessageId), ("02020202-0202-0202-0202-020202020202" as ConversationMessageId),
    ] },
    messages: [{ role: "assistant", content: "old" }, { role: "user", content: "new" }],
  })).rejects.toThrow();
  expect(dispatched).toBe(0);
});

test("stream event limits stop an unbounded provider before further dispatch", async () => {
  let produced = 0;
  const runtime = await Harness.builder(contracts).limits({ model_events_per_step: 2 }).model(testModel, {
    ...syntheticAccounting(contracts), async *generate() {
      while (true) { produced++; yield { kind: "content" as const, delta: "." }; }
    },
    async reconcile() { return undefined; },
  }).build();
  await expect(runtime.run("bounded")).rejects.toThrow();
  expect(produced).toBe(3);
});

test("model loop consumes the detached event returned by Rust admission", async () => {
  let reads = 0;
  const event = {
    kind: "content" as const,
    get delta(): string {
      reads += 1;
      return reads === 1 ? "ok" : "x".repeat(3);
    },
  };
  const runtime = await Harness.builder(contracts).limits({ file_bytes: 2 }).model(testModel, {
    ...syntheticAccounting(contracts), async *generate() {
      yield event;
      yield { kind: "completed" as const, metadata: {} };
    },
    async reconcile() { return undefined; },
  }).build();
  await expect(runtime.run("x")).resolves.toMatchObject({ text: "ok" });
  expect(reads).toBe(1);
});

test("custom context builders cannot bypass model-content bounds", async () => {
  let dispatched = 0;
  const runtime = await Harness.builder(contracts).limits({ render_bytes: 8 }).context({
    async build() { return [{ role: "user" as const, content: "too much context" }]; },
  }).model(testModel, {
    ...syntheticAccounting(contracts), async *generate() { dispatched++; yield { kind: "completed" as const, metadata: {} }; },
    async reconcile() { return undefined; },
  }).build();
  await expect(runtime.run("short")).rejects.toThrow();
  expect(dispatched).toBe(0);
});

test("custom context builders use the native closed model-message contract", async () => {
  let dispatched = 0;
  const runtime = await Harness.builder(contracts).context({
    async build() {
      return [{ role: "developer" as never, content: "safe" }];
    },
  }).model(testModel, {
    ...syntheticAccounting(contracts), async *generate() { dispatched++; yield { kind: "completed" as const, metadata: {} }; },
    async reconcile() { return undefined; },
  }).build();
  await expect(runtime.run("safe")).rejects.toThrow();
  expect(dispatched).toBe(0);
});

test("model context tool values are admitted by Rust canonical JSON before dispatch", async () => {
  let dispatched = 0;
  const runtime = await Harness.builder(contracts).context({
    async build() { return [{ role: "assistant" as const, content: {
      kind: "tool_call" as const, callId: "call", name: "tool", arguments: { unsafe: Number.POSITIVE_INFINITY },
    } }, { role: "user" as const, content: "safe" }]; },
  }).model(testModel, {
    ...syntheticAccounting(contracts), async *generate() { dispatched++; yield { kind: "completed" as const, metadata: {} }; },
    async reconcile() { return undefined; },
  }).build();
  await expect(runtime.run("safe")).rejects.toThrow();
  expect(dispatched).toBe(0);
});

test("local task names and revisions use one bounded registry contract", async () => {
  const task = await TaskDefinition.live("leaf", "1", async () => 1);
  expect(task.name).toBe("leaf");
  expect(Object.isFrozen(task)).toBe(true);
  expect(Object.isFrozen(task.implementation)).toBe(true);
  await expect(TaskDefinition.live("bad/name", "1", async () => 1)).rejects.toThrow();
  await expect(TaskDefinition.live("leaf", "bad version", async () => 1)).rejects.toThrow();
});

test("model projections receive parsed invocation and retain independent canonical results", async () => {
const stringProjection = await jsonToolProjection<string>({ type: "string" });
class Projection {
  readonly schema = stringProjection.schema;
  readonly prefix = "selected";
  project(invocation: { arguments: { count: number } }, result: { value: number }) {
    return { kind: "json" as const, value: `${this.prefix}:${invocation.arguments.count}:${result.value}` };
  }
}
const projector = new Projection();
const definition = await defineTool<{ count: number }, number>({
  name: "projected", revision: "1", description: "projected",
  inputSchema: { type: "number" }, outputSchema: { type: "number" },
  parseInput: value => ({ count: parseNumber(value) }), parseOutput: parseNumber,
  projection: projector,
}, (_context, input) => input.count * 2);
let steps = 0;
const builder = Harness.builder(contracts).tool(definition).grant("tool:call:projected");
// The admitted method is captured once, including a prototype method's receiver.
projector.project = () => { throw new Error("replacement must not execute"); };
const runtime = await builder.model(testModel, {
  ...syntheticAccounting(contracts),
  async *generate(request) {
    if (steps++ === 0) yield { kind: "tool_call" as const, callId: "projected", name: "projected", arguments: 3 };
    else expect(request.messages.at(-1)?.content).toEqual({ kind: "tool_result", callId: "projected",
      name: "projected", content: { kind: "json", value: "selected:3:6" } });
    yield { kind: "completed" as const, metadata: {} };
  }, async reconcile() { return undefined; },
}).build();
expect(runtime.tool(definition).definition.projectionSchema).toEqual(stringProjection.schema);
const result = await runtime.run("go");
expect(result.receipts.find(receipt => receipt.kind === "tool"))
  .toMatchObject({ value: 6, projection: { kind: "json", value: "selected:3:6" } });
expect(steps).toBe(2);
});

test("invalid model projection fails before the next provider request", async () => {
const stringProjection = await jsonToolProjection<string>({ type: "string" });
const definition = await defineTool<null, number>({ name: "invalid-projection", revision: "1",
  description: "invalid projection", inputSchema: { type: "null" }, outputSchema: { type: "number" },
  parseInput: parseNull, parseOutput: parseNumber,
  projection: { schema: stringProjection.schema, project: () => ({ kind: "json", value: 1 }) },
}, () => 1);
let calls = 0;
const runtime = await Harness.builder(contracts).tool(definition).grant("tool:call:invalid-projection")
  .model(testModel, { ...syntheticAccounting(contracts), async *generate() {
    calls++;
    yield { kind: "tool_call" as const, callId: "invalid", name: "invalid-projection", arguments: null };
    yield { kind: "completed" as const, metadata: {} };
  }, async reconcile() { return undefined; } }).build();
await expect(runtime.run("go")).rejects.toThrow();
expect(calls).toBe(1);
});
