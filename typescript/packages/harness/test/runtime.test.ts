import { describe, expect, test } from "bun:test";
import {
  Batch,
  AgentHarness,
  ExecutionScope,
  GroupPolicies,
  Harness,
  HarnessBuilder,
  NativeContracts,
  TaskDefinition,
  Task,
  TaskContext,
  composeContentBindings,
  DEFAULT_LIMITS,
  defineRuntimeSchema,
  defineTool,
  descriptorFor,
  policyIdentity,
  type BatchId,
  type AgentId,
  type EffectId,
  type GroupId,
  type EffectiveScope,
  type HarnessRuntimeHost,
  type MessageId,
  type Outcome,
  type OperationId,
  type RuntimeTaskId,
  type TaskMessage,
  type ModelMessage,
  type ModelToolDefinition,
  type FileRef,
  type ConversationMessageId,
} from "../src/index.js";

const contracts = await NativeContracts.create();
const fixtureMessageId = (value: string): ConversationMessageId => value as ConversationMessageId;

const durableDigest = "ab".repeat(32);
const approvalPolicyIdentity = policyIdentity("approval-policy", "1", new Uint8Array(32).fill(1));
const denyPolicyIdentity = policyIdentity("deny-policy", "1", new Uint8Array(32).fill(2));
const scopedPolicyIdentity = policyIdentity("scoped-policy", "1", new Uint8Array(32).fill(3));
const numberSchema = defineRuntimeSchema("number", { type: "number" }, (value: unknown): number => {
  if (typeof value !== "number") throw new TypeError("invalid result");
  return value;
});
const parseNumber = numberSchema.parse;
const parseString = (value: unknown): string => {
  if (typeof value !== "string") throw new TypeError("expected string");
  return value;
};
const parseNull = (value: unknown): null => {
  if (value !== null) throw new TypeError("expected null");
  return null;
};

test("native tool JSON preserves special property names as data", async () => {
  const value = Object.fromEntries([["__proto__", { safe: true }], ["nested", { value: 1 }], ["byte_length", -2]]);
  const admitted = (await NativeContracts.create()).validateToolValue({ type: "object" }, value);
  expect(Object.getOwnPropertyDescriptor(admitted, "__proto__")?.value).toEqual({ safe: true });
  expect((admitted as { nested: { value: number } }).nested.value).toBe(1);
  expect((admitted as { byte_length: number }).byte_length).toBe(-2);
  expect(Object.getPrototypeOf(admitted)).toBe(Object.prototype);
});

test("native tool JSON rejects values that the WASM serializer would coerce", () => {
  expect(() => contracts.validateToolValue({ type: "object" }, { unsafe: Number.POSITIVE_INFINITY })).toThrow();
  expect(() => contracts.validateToolValue({ type: "object" }, { unsafe: -0 })).toThrow();
  expect(() => contracts.validateToolValue({ type: "object", maximum: Number.POSITIVE_INFINITY }, {})).toThrow();
});

test("native tool admission cannot reread a mutable getter after JSON preflight", () => {
  let reads = 0;
  const value = Object.defineProperty({}, "unsafe", {
    enumerable: true,
    get: () => ++reads === 1 ? 1 : Number.POSITIVE_INFINITY,
  });
  let admitted: unknown;
  try { admitted = contracts.validateToolValue({ type: "object" }, value); }
  catch { /* Rejecting an accessor is also safe. */ }
  if (admitted !== undefined) expect(admitted).toEqual({ unsafe: 1 });
  expect(reads).toBeLessThanOrEqual(1);
});

test("task and tool schemas enter the registry only as strict Rust JSON", () => {
  const unsafe = { type: "number", maximum: Number.POSITIVE_INFINITY };
  const schema = defineRuntimeSchema("unsafe", unsafe, parseNumber);
  const task = TaskDefinition.live("unsafe-schema-task", "1", (_context, value: number) => value,
    { input: schema });
  expect(() => Harness.builder(contracts).task(task)).toThrow();
  const tool = defineTool({ name: "unsafe-schema-tool", revision: "1", description: "unsafe schema",
    inputSchema: unsafe, outputSchema: { type: "number" }, parseInput: parseNumber,
    parseOutput: parseNumber }, (_context, value) => value);
  expect(() => Harness.builder(contracts).tool(tool)).toThrow();
});

test("content routing distinguishes owner volumes on one provider and keeps one writer", async () => {
  const file = await mailboxFile(2);
  const other = { ...file, volume: { ...file.volume,
    id: "private", class: "agent_private", owner: { kind: "agent", id: "01010101-0101-0101-0101-010101010101" as AgentId } } } as FileRef;
  const validated: number[] = [];
  const binding = (marker: number) => ({
    validate: (_reference: FileRef) => { validated.push(marker); },
    verify: (reference: FileRef, bytes: Uint8Array) => {
      if (reference.volume.provider.family === "filesystem" && bytes[0] !== 1) throw new Error("wrong owner");
    },
    read: async () => Uint8Array.of(marker, marker),
    fileReadCapability: () => "read:file",
    volumeReadCapability: () => "read:volume",
    directoryReadCapability: () => "read:directory",
  });
  const first = binding(1);
  const second = binding(2);
  const content = composeContentBindings(contracts, [
    { volume: file.volume, content: first },
    { volume: other.volume, content: second },
  ]);
  first.read = async () => Uint8Array.of(9);
  second.read = async () => Uint8Array.of(9);
  expect([...await content.read(file)]).toEqual([1, 1]);
  expect([...await content.read(other)]).toEqual([2, 2]);
  content.validate(file, DEFAULT_LIMITS);
  content.validate(other, DEFAULT_LIMITS);
  expect(validated).toEqual([1, 2]);
  expect(() => content.verify(file, Uint8Array.of(2))).toThrow("wrong owner");
  expect(() => composeContentBindings(contracts, [
    { volume: file.volume, content: first },
    { volume: file.volume, content: second },
  ])).toThrow("registered twice");
  expect(() => composeContentBindings(contracts, [
    { volume: other.volume, content: first },
    { volume: { provider: other.volume.provider, id: other.volume.id, class: "agent_private",
      owner: { kind: "agent", id: "01010101010101010101010101010101" as AgentId } }, content: second },
  ])).toThrow("registered twice");
});

test("builder snapshots owner content and writer callbacks at admission", async () => {
  const file = await mailboxFile(1);
  const writer = {
    volume: file.volume,
    writeCapability: () => "write:original",
    stage: async () => file,
  };
  const binding = {
    validate: () => {}, verify: () => {},
    read: async () => Uint8Array.of(1),
    fileReadCapability: () => "read:original",
    volumeReadCapability: () => "volume:original",
    directoryReadCapability: () => "directory:original",
    writer,
  };
  const runtime = Harness.builder(contracts).content(binding).build();
  binding.read = async () => Uint8Array.of(9);
  binding.fileReadCapability = () => "read:replacement";
  writer.stage = async () => { throw new Error("writer swapped"); };
  writer.writeCapability = () => "write:replacement";
  expect([...await runtime.content!.read(file)]).toEqual([1]);
  expect(runtime.content!.fileReadCapability(file)).toBe("read:original");
  expect(runtime.content!.writer!.writeCapability()).toBe("write:original");
  expect(await runtime.content!.writer!.stage("operation", file.path, Uint8Array.of(1), "application/octet-stream", file.display_name)).toBe(file);
  expect(Object.isFrozen(runtime.content)).toBe(true);
  expect(Object.isFrozen(runtime.content!.writer)).toBe(true);
});

async function mailboxFile(byteLength: number): Promise<FileRef> {
  return {
    volume: { provider: { namespace: "test", family: "filesystem", version: "2" }, id: "project", class: "project", owner: { kind: "project", id: "project" } },
    path: "mailbox/payload.bin",
    version: "generation",
    descriptor: await descriptorFor(new Uint8Array(byteLength), "application/octet-stream"),
    display_name: "payload.bin",
  };
}

describe("typed agent runtime", () => {
  test("pins task dependencies and rejects missing versions and cycles at build time", () => {
    const leaf = TaskDefinition.live("leaf", "2", () => 1);
    const parent = TaskDefinition.live("parent", "1", () => 2, { requirements: ["task:leaf@2"] });
    expect(() => Harness.builder(contracts).task(parent).task(leaf).build()).not.toThrow();
    const stale = TaskDefinition.live("stale", "1", () => 0, { requirements: ["task:leaf@1"] });
    expect(() => Harness.builder(contracts).task(stale).task(leaf).build()).toThrow("unsatisfied task requirement");
    const left = TaskDefinition.live("left", "1", () => 1, { requirements: ["task:right@1"] });
    const right = TaskDefinition.live("right", "1", () => 1, { requirements: ["task:left@1"] });
    expect(() => Harness.builder(contracts).task(left).task(right).build()).toThrow("task dependency cycle");
  });

  test("retains simultaneous pinned task revisions without an implicit latest", async () => {
    const first = TaskDefinition.live<void, number>("versioned", "1", () => 1);
    const second = TaskDefinition.live<void, number>("versioned", "2", () => 2);
    const needsFirst = TaskDefinition.live<void, number>("needs-first", "1", () => 1,
      { requirements: ["task:versioned@1"] });
    const runtime = Harness.builder(contracts).task(first).task(second).task(needsFirst).build();
    expect(Object.is(runtime.task("versioned@1"), first)).toBe(true);
    expect(Object.is(runtime.task("versioned@2"), second)).toBe(true);
    expect(runtime.task(first)).toBe(first);
    expect(() => runtime.task("versioned")).toThrow("ambiguous");
    expect(await runtime.spawn(first, undefined).result()).toEqual({ kind: "succeeded", value: 1 });
    expect(await runtime.spawn(second, undefined).result()).toEqual({ kind: "succeeded", value: 2 });
    expect(() => Harness.builder(contracts).task(first).task(first)).toThrow("conflicting registration");
    expect(() => TaskDefinition.live("bad@name", "1", () => 0)).toThrow("cannot contain @");
  });

  test("pins tool revisions independently and selects the model-visible revision explicitly", async () => {
    const first = defineTool<null, number>({ name: "versioned-tool", revision: "1", description: "first", inputSchema: {}, outputSchema: {}, parseInput: parseNull, parseOutput: parseNumber }, () => 1);
    const second = defineTool<null, number>({ name: "versioned-tool", revision: "2", description: "second", inputSchema: {}, outputSchema: {}, parseInput: parseNull, parseOutput: parseNumber }, () => 2);
    const needsFirst = TaskDefinition.live<void, number>("needs-first-tool", "1", () => 1,
      { requirements: ["tool:versioned-tool@1"] });
    const builder = Harness.builder(contracts).tool(first).tool(second).task(needsFirst).grant("tool:call:versioned-tool");
    const ambiguous = builder.build();
    expect(ambiguous.tool("versioned-tool@1").definition).toMatchObject({ name: first.name, revision: first.revision });
    expect(ambiguous.tool("versioned-tool@2").definition).toMatchObject({ name: second.name, revision: second.revision });
    expect(() => ambiguous.tool("versioned-tool")).toThrow("ambiguous");
    expect(await ambiguous.call(ambiguous.tool("versioned-tool@1"), null)).toBe(1);
    expect(await ambiguous.call(ambiguous.tool("versioned-tool@2"), null)).toBe(2);
    let visible: readonly ModelToolDefinition[] = [];
    const selected = builder.selectModelTool("versioned-tool", "2").model({
      async *generate(request) { visible = request.tools; yield { kind: "completed" as const, metadata: {} }; },
      async reconcile() { return undefined; },
    }).build();
    expect(selected.tool("versioned-tool").definition).toMatchObject({ name: second.name, revision: second.revision });
    const typedValue: number = await selected.call(selected.tool(second), null);
    expect(typedValue).toBe(2);
    expect(() => selected.tool({ ...second })).toThrow("not registered");
    await selected.run("select");
    expect(visible).toEqual([{ name: "versioned-tool", revision: "2", description: "second", inputSchema: {}, outputSchema: {} }]);
    expect(() => Harness.builder(contracts).tool(first).tool(first)).toThrow("conflicting registration");
    expect(() => builder.selectModelTool("versioned-tool", "3")).toThrow("not registered");
  });

  test("preserves task input and output types through nested execution", async () => {
    const double = TaskDefinition.live<number, number>("double", "1", async (_context, value) => value * 2);
    const sum = TaskDefinition.live<readonly number[], number>("sum", "1", async (context, values) => {
      const group = context.group<number>(GroupPolicies.collectAll);
      return (await group.map(context.task(double), values)).reduce((left, right) => left + right, 0);
    });
    const runtime = Harness.builder(contracts).task(double).task(sum).grant("task:spawn:double@1").build();

    const outcome: Outcome<number> = await runtime.spawn(sum, [1, 2, 3]).result();
    expect(outcome).toEqual({ kind: "succeeded", value: 12 });
  });

  test("a task context and its groups cannot invoke an ungranted descendant", async () => {
    const allowed = TaskDefinition.live<number, number>("allowed-child", "1", (_context, value) => value + 1);
    const denied = TaskDefinition.live<number, number>("denied-child", "1", (_context, value) => value + 100);
    const nested = TaskDefinition.live<number, boolean>("narrowed-parent", "1", async (child, value) => {
      const allowedOutcome = await child.spawn(child.task(allowed), value).result();
      if (allowedOutcome.kind !== "succeeded" || allowedOutcome.value !== value + 1) return false;
      try { child.spawn(child.task(denied), value); return false; }
      catch { return true; }
    });
    const tool = defineTool({ name: "child-tool", revision: "1", description: "child tool",
      inputSchema: { type: "null" }, outputSchema: { type: "number" },
      parseInput: parseNull, parseOutput: parseNumber }, () => 3);
    const owner = Harness.builder(contracts).task(allowed).task(denied).task(nested).tool(tool)
      .grant("task:spawn:allowed-child@1", "task:spawn:denied-child@1", "tool:call:child-tool").build();
    const narrowed = owner.scoped(ExecutionScope.create().onlyGrants("task:spawn:allowed-child@1"));
    const context = new TaskContext(narrowed, new AbortController().signal);
    const allowedRef = context.task(allowed);
    const deniedRef = new TaskContext(owner, new AbortController().signal).task(denied);
    expect(allowedRef).toMatchObject({ name: "allowed-child", revision: "1" });
    expect("implementation" in allowedRef).toBe(false);
    expect(Object.isFrozen(allowedRef)).toBe(true);
    expect(() => context.task(denied)).toThrow("task scope lacks task:spawn:denied-child@1");
    expect(() => context.tool(tool)).toThrow("task scope lacks tool:call:child-tool");
    expect(await narrowed.spawn(nested, 1).result()).toEqual({ kind: "succeeded", value: true });
    expect(await context.spawn(allowedRef, 1).result()).toEqual({ kind: "succeeded", value: 2 });
    expect(() => context.spawn(deniedRef, 1)).toThrow("task scope lacks task:spawn:denied-child@1");
    const group = context.group<number>(GroupPolicies.collectAll);
    const blocked = await group.spawnMany(deniedRef, new Batch("batch:blocked" as BatchId, [1]));
    expect(blocked[0]?.admission).toMatchObject({ kind: "rejected", reason: { code: "unsupported" } });
    await expect(group.reconcileBatch(deniedRef, new Batch("batch:blocked" as BatchId, [1])))
      .rejects.toThrow("task scope lacks task:spawn:denied-child@1");
    const admitted = await group.spawnMany(allowedRef, new Batch("batch:allowed" as BatchId, [2]));
    expect(admitted[0]?.admission.kind).toBe("accepted");
    const durable = new TaskContext(narrowed, new AbortController().signal,
      "task:parent" as RuntimeTaskId, true);
    expect(() => durable.admit(deniedRef, 1, "operation:blocked"))
      .toThrow("task scope lacks task:spawn:denied-child@1");
    expect(owner.task(denied)).toBe(denied);
    expect(owner.tool(tool).definition.name).toBe("child-tool");
    expect(await owner.spawn(denied, 1).result()).toEqual({ kind: "succeeded", value: 101 });
  });

  test("never executes a resumable definition in the local in-memory runner", async () => {
    expect(() => defineRuntimeSchema("empty", {}, value => value)).toThrow("non-vacuous");
    expect(() => defineRuntimeSchema("true", true as unknown as Parameters<typeof defineRuntimeSchema>[1], value => value))
      .toThrow("non-vacuous");
    let transitions = 0;
    const definition = TaskDefinition.resumable<number, number, number>("durable", "1", {
      state: numberSchema,
      initial: input => input,
      async transition(_context, state) { transitions++; return { kind: "finish", output: state * 2 }; },
    }, { implementationDigest: durableDigest, input: numberSchema, output: numberSchema });
    const local = Harness.builder(contracts).task(definition).build();
    expect(() => local.spawn(definition, 3)).toThrow("async durable admission");
    const rejected = await local.group<number>(GroupPolicies.collectAll).spawnMany(definition, new Batch("batch:resumable" as BatchId, [3]));
    expect(rejected[0]?.admission).toMatchObject({ kind: "rejected", reason: { code: "unsupported" } });
    expect(transitions).toBe(0);

    let admitted: { readonly input: number; readonly revision: string } | undefined;
    const host: HarnessRuntimeHost = {
      policyIdentity: () => null,
      async admitResumable(_operationId, task, input) {
        if (typeof input !== "number") throw new TypeError("test host expected a number");
        admitted = { input, revision: task.revision };
        return { kind: "accepted" as const, task: new Task("task:durable" as RuntimeTaskId, async () => 6) };
      },
      async attach(id) { return { task: new Task(id, async () => 6), operationId: "admission:durable", taskName: "durable", revision: "1", implementationDigest: durableDigest }; },
      async reconcileEffect() { return { state: "indeterminate" } as const; },
      async send(message: TaskMessage) { return { accepted: true, messageId: message.id }; },
      async *inbox() { yield* [] as TaskMessage[]; },
    };
    const hosted = Harness.builder(contracts).host(host).task(definition).build();
    const result = await hosted.admit(definition, 3, "admission:durable");
    expect(result.kind).toBe("accepted");
    if (result.kind !== "accepted") throw new Error("expected admission");
    expect(await result.task.result()).toEqual({ kind: "succeeded", value: 6 });
    expect(admitted).toEqual({ input: 3, revision: "1" });
    expect(transitions).toBe(0);
  });

  test("restored resumable state is schema-admitted before a typed transition", async () => {
    let transitions = 0;
    const definition = TaskDefinition.resumable<number, number, number>("checkpoint", "1", {
      state: numberSchema,
      initial: input => input,
      async transition(_context, state) { transitions++; return { kind: "finish", output: state * 2 }; },
    }, { implementationDigest: durableDigest, input: numberSchema, output: numberSchema });
    if (definition.implementation.kind !== "resumable") throw new TypeError("expected resumable definition");
    const context = new TaskContext(Harness.builder(contracts).build(), new AbortController().signal);
    await expect(definition.implementation.component.transition(context, "corrupt checkpoint")).rejects.toThrow();
    expect(transitions).toBe(0);
    expect(await definition.implementation.component.transition(context, 4)).toEqual({ kind: "finish", output: 8 });
    expect(transitions).toBe(1);
  });

  test("host-backed handles preserve authoritative events and cancellation", async () => {
    const taskId = "task:remote" as RuntimeTaskId;
    const observed: number[] = [];
    let cancelled = false;
    const task = Task.fromHost(taskId, {
      operationId: "operation:remote",
      async result() { return { kind: "succeeded", value: 7 } as const; },
      async *events(fromSequence) {
        observed.push(fromSequence);
        yield { id: "event:remote", taskId, sequence: 42, event: { kind: "started" } as const };
      },
      async cancel() { cancelled = true; return { requested: true, taskId }; },
    });
    expect(await task.result()).toEqual({ kind: "succeeded", value: 7 });
    const events = [];
    for await (const event of task.events(41)) events.push(event);
    expect(events.map(event => event.sequence)).toEqual([42]);
    expect(observed).toEqual([41]);
    expect(await task.cancel()).toEqual({ requested: true, taskId });
    expect(cancelled).toBeTrue();
    const disconnected = Task.fromHost(taskId, {
      operationId: "operation:remote",
      async result() { throw new Error("transport offline"); },
      async *events() { yield* []; },
      async cancel() { return { requested: false, taskId }; },
    });
    expect(await disconnected.result()).toEqual({ kind: "indeterminate", operationId: "operation:remote" });
    const disconnectedEvents = [];
    for await (const event of disconnected.events()) disconnectedEvents.push(event.event);
    expect(disconnectedEvents).toEqual([{ kind: "settled", outcome: { kind: "indeterminate", operationId: "operation:remote" } }]);
  });

  test("reconciles lost durable admission acknowledgement by stable operation ID", async () => {
    const definition = TaskDefinition.resumable<number, number, number>("retryable", "1", {
      state: numberSchema,
      initial: input => input,
      async transition(_context, state) { return { kind: "finish", output: state }; },
    }, { implementationDigest: durableDigest, input: numberSchema, output: numberSchema });
    const observed = new Map<string, Task<number>>();
    let loseAcknowledgement = true;
    let reject = false;
    const host: HarnessRuntimeHost = {
      policyIdentity: () => null,
      async admitResumable(operationId: string) {
        if (reject) return { kind: "rejected" as const, reason: { code: "unsupported" as const, message: "policy" } };
        let task = observed.get(operationId);
        if (!task) { task = new Task("task:once" as RuntimeTaskId, async () => "wrong type" as unknown as number); observed.set(operationId, task); }
        if (loseAcknowledgement) { loseAcknowledgement = false; throw new Error("ack lost"); }
        return { kind: "accepted" as const, task };
      },
      async attach(id) { return { task: observed.values().next().value as Task<unknown>, operationId: "operation:once", taskName: "retryable", revision: "1", implementationDigest: durableDigest }; },
      async reconcileEffect() { return { state: "indeterminate" } as const; },
      async send(message: TaskMessage) { return { accepted: true, messageId: message.id }; },
      async *inbox() { yield* [] as TaskMessage[]; },
    };
    const runtime = Harness.builder(contracts).host(host).task(definition).build();
    const first = await runtime.admit(definition, 1, "operation:once");
    expect(first).toEqual({ kind: "indeterminate", operationId: "operation:once" });
    const second = await runtime.admit(definition, 1, "operation:once");
    expect(second.kind).toBe("accepted");
    if (second.kind !== "accepted") throw new Error("expected accepted retry");
    expect(observed.size).toBe(1);
    expect(await second.task.result()).toMatchObject({ kind: "failed", error: { message: expect.stringContaining("tool value failed validation") } });
    const settled = [];
    for await (const event of second.task.events()) if (event.event.kind === "settled") settled.push(event.event.outcome);
    expect(settled).toMatchObject([{ kind: "failed", error: { message: expect.stringContaining("tool value failed validation") } }]);
    const reconnected = Harness.builder(contracts).host(host).task(definition).build();
    expect(await (await reconnected.attach(definition, "task:once" as RuntimeTaskId)).result())
      .toMatchObject({ kind: "failed", error: { message: expect.stringContaining("tool value failed validation") } });
    reject = true;
    expect(await runtime.admit(definition, 2, "operation:rejected")).toMatchObject({ kind: "rejected", reason: { code: "unsupported" } });
  });

  test("preserves unresolved batch admission and stable scoped entry identities", async () => {
    const definition = TaskDefinition.resumable<number, number, number>("batch-work", "2", {
      state: numberSchema,
      initial: input => input,
      async transition(_context, state) { return { kind: "finish", output: state }; },
    }, { implementationDigest: durableDigest, input: numberSchema, output: numberSchema });
    const ids: string[] = [];
    const host: HarnessRuntimeHost = {
      policyIdentity: () => null,
      async admitResumable(operationId) { ids.push(operationId); return { kind: "indeterminate", operationId }; },
      async reconcileBatch(groupId, batchId) { return { taskName: "batch-work", revision: "2", implementationDigest: durableDigest, entries: [0, 1].map(index => ({ key: { batchId, index }, admission: { kind: "indeterminate" as const, operationId: `${groupId}:batch-work@2#${durableDigest}:${batchId}:${index}` } })) }; },
      async attach(id) { return { task: new Task(id, async () => undefined), operationId: "operation:batch", taskName: "batch-work", revision: "2", implementationDigest: durableDigest }; },
      async reconcileEffect() { return { state: "indeterminate" } as const; },
      async send(message: TaskMessage) { return { accepted: true, messageId: message.id }; },
      async *inbox() { yield* [] as TaskMessage[]; },
    };
    const group = Harness.builder(contracts).host(host).task(definition).build().group<number>(GroupPolicies.collectAll, "group:stable" as GroupId);
    const batch = new Batch("batch:stable" as BatchId, [1, 2]);
    expect((await group.spawnMany(definition, batch)).map(entry => entry.admission.kind)).toEqual(["indeterminate", "indeterminate"]);
    expect((await group.spawnMany(definition, batch)).map(entry => entry.admission.kind)).toEqual(["indeterminate", "indeterminate"]);
    expect(ids).toEqual([0, 1, 0, 1].map(index => `group:stable:batch-work@2#${durableDigest}:batch:stable:${index}`));
    const completed = [];
    for await (const entry of group.asCompleted()) completed.push(entry);
    expect(completed).toHaveLength(2);
    expect(completed.map(entry => entry.admission.kind)).toEqual(["indeterminate", "indeterminate"]);
    expect((await group.join()).complete).toBeFalse();
    const reconstructed = Harness.builder(contracts).host(host).task(definition).build().group<number>(GroupPolicies.collectAll, "group:stable" as GroupId);
    expect((await reconstructed.reconcileBatch(definition, batch)).map(entry => entry.admission.kind)).toEqual(["indeterminate", "indeterminate"]);
    expect((await reconstructed.join()).entries).toHaveLength(2);
  });

  test("records valid batch admissions even when another input is invalid", async () => {
    const definition = TaskDefinition.resumable<number, number, number>("validate-input", "1", {
      state: numberSchema,
      initial: input => input,
      async transition(_context, state) { return { kind: "finish", output: state }; },
    }, { implementationDigest: durableDigest, input: defineRuntimeSchema("positive", { type: "number", exclusiveMinimum: 0 }, value => {
      if (typeof value !== "number" || value <= 0) throw new TypeError("positive number required");
      return value;
    }), output: numberSchema });
    const admitted: string[] = [];
    const host: HarnessRuntimeHost = {
      policyIdentity: () => null,
      async admitResumable(operationId: string) { admitted.push(operationId); return { kind: "indeterminate", operationId }; },
      async attach(id) { return { task: new Task(id, async () => undefined), operationId: "operation:partial", taskName: "validate-input", revision: "1", implementationDigest: durableDigest }; },
      async reconcileEffect() { return { state: "indeterminate" } as const; },
      async send(message: TaskMessage) { return { accepted: true, messageId: message.id }; },
      async *inbox() { yield* [] as TaskMessage[]; },
    };
    const group = Harness.builder(contracts).host(host).task(definition).build().group<number>(GroupPolicies.collectAll, "group:partial" as GroupId);
    const entries = await group.spawnMany(definition, new Batch("batch:partial" as BatchId, [1, -1, 2]));
    expect(entries.map(entry => entry.admission.kind)).toEqual(["indeterminate", "rejected", "indeterminate"]);
    expect(admitted).toHaveLength(2);
    expect((await group.join()).entries).toHaveLength(3);
  });

  test("routes typed tools through policy and interaction boundaries", async () => {
    const lengths = defineTool<{ readonly text: string }, number>({
      name: "length",
      revision: "1",
      description: "Count characters",
      inputSchema: { type: "object" },
      outputSchema: { type: "number" },
      parseInput(value) {
        if (value === null || typeof value !== "object" || Array.isArray(value)
          || !("text" in value) || typeof value.text !== "string") throw new TypeError("expected text input");
        return { text: value.text };
      },
      parseOutput: parseNumber,
    }, (_context, input) => input.text.length);
    const runtime = Harness.builder(contracts)
      .interactions({ route: async interaction => {
        expect(interaction.kind).toBe("approval");
        if (interaction.kind !== "approval") return { kind: "denied" };
        expect(interaction.actionDigest).toHaveLength(32);
        expect(interaction.operationId).toBeTruthy();
        return { kind: "approved" };
      } })
      .policy({ identity: () => approvalPolicyIdentity,
        evaluate: async () => ({ kind: "require-approval", prompt: "approve" }) })
      .tool(lengths)
      .grant("tool:call:length")
      .build();

    const handle = runtime.tool("length");
    expect("executor" in handle).toBe(false);
    expect("handler" in handle.definition).toBe(false);
    expect("parseInput" in handle.definition).toBe(false);
    expect("parseOutput" in handle.definition).toBe(false);
    expect(await runtime.call(handle, { text: "typed" })).toBe(5);
  });

  test("approvals bind one exact tool action and only approved grants dispatch", async () => {
    const tool = defineTool<number, number>({ name: "approved-action", revision: "1", description: "pinned action",
      inputSchema: { type: "number" }, outputSchema: { type: "number" },
      parseInput: parseNumber, parseOutput: parseNumber }, (_context, value) => value);
    const seen: number[][] = [];
    const route = async (interaction: import("../src/index.js").Interaction) => {
      if (interaction.kind !== "approval") throw new TypeError("expected an exact approval request");
      seen.push([...interaction.actionDigest]);
      return { kind: "approved" as const };
    };
    const builder = Harness.builder(contracts).tool(tool).grant("tool:call:approved-action")
      .policy({ identity: () => approvalPolicyIdentity,
        evaluate: async () => ({ kind: "require-approval", prompt: "approve action" }) });
    const approved = builder.interactions({ route }).build();
    expect(await approved.call(approved.tool(tool), 1)).toBe(1);
    expect(await approved.call(approved.tool(tool), 2)).toBe(2);
    expect(seen).toHaveLength(2);
    expect(seen[0]).not.toEqual(seen[1]);
    const signal = new AbortController().signal;
    expect(await approved.call(approved.tool(tool), 4, signal, undefined, "model-tool:turn:0", "provider-call")).toBe(4);
    expect(await approved.call(approved.tool(tool), 4, signal, undefined, "model-tool:turn:0", "provider-call")).toBe(4);
    expect(seen[2]).toEqual(seen[3]);
    const unapproved = builder.interactions({ route: async () => ({ kind: "accepted", text: "not an approval" }) }).build();
    await expect(unapproved.call(unapproved.tool(tool), 3)).rejects.toMatchObject({ kind: "invalid_response" });
    for (const kind of ["declined", "cancelled", "expired", "denied"] as const) {
      const rejected = builder.interactions({ route: async () => ({ kind }) }).build();
      await expect(rejected.call(rejected.tool(tool), 3)).rejects.toMatchObject({ kind });
    }
    const unresolved = Harness.builder(contracts).tool(tool).grant("tool:call:approved-action")
      .policy({ identity: () => approvalPolicyIdentity,
        evaluate: async () => ({ kind: "require-approval", prompt: "approve action" }) }).build();
    await expect(unresolved.call(unresolved.tool(tool), 3)).rejects.toMatchObject({ kind: "indeterminate" });
    const mismatched = builder.interactions({ route: async () => ({ kind: "indeterminate", operationId: "wrong" }) }).build();
    await expect(mismatched.call(mismatched.tool(tool), 3)).rejects.toThrow("another operation");
  });

  test("scoped policies require separate exact approvals for each pinned layer", async () => {
    let executed = 0;
    const prompts: string[] = [];
    const digests: number[][] = [];
    let declineSecond = true;
    const tool = defineTool<number, number>({ name: "two-approvals", revision: "1", description: "two approvals",
      inputSchema: { type: "number" }, outputSchema: { type: "number" },
      parseInput: parseNumber, parseOutput: parseNumber }, (_context, value) => { executed++; return value; });
    const root = Harness.builder(contracts).tool(tool).grant("tool:call:two-approvals")
      .policy({ identity: () => approvalPolicyIdentity,
        evaluate: async () => ({ kind: "require-approval", prompt: "parent approval" }) })
      .interactions({ route: async interaction => {
        if (interaction.kind !== "approval") throw new TypeError("unexpected interaction");
        prompts.push(interaction.prompt);
        digests.push([...interaction.actionDigest]);
        return declineSecond && interaction.prompt === "child approval"
          ? { kind: "declined" } : { kind: "approved" };
      } }).build();
    const scoped = root.scoped(ExecutionScope.create().policy({ identity: () => scopedPolicyIdentity,
      evaluate: async () => ({ kind: "require-approval", prompt: "child approval" }) }));
    expect(scoped.tool(tool)).toBe(root.tool(tool));
    await expect(scoped.call(scoped.tool(tool), 1)).rejects.toMatchObject({ kind: "declined" });
    expect(prompts).toEqual(["parent approval", "child approval"]);
    expect(digests[0]).not.toEqual(digests[1]);
    expect(executed).toBe(0);
    declineSecond = false;
    expect(await scoped.call(scoped.tool(tool), 2)).toBe(2);
    expect(executed).toBe(1);
  });

  test("durable tool calls preserve explicit unresolved and terminal outcomes", async () => {
    const operation = "01010101-0101-0101-0101-010101010101" as OperationId;
    const task = "task:durable-tool" as RuntimeTaskId;
    let localExecutions = 0;
    const tool = defineTool<number, number>({ name: "durable-count", revision: "1", description: "count",
      inputSchema: { type: "number" }, outputSchema: { type: "number" }, parseInput: parseNumber,
      parseOutput: parseNumber }, () => { localExecutions++; return 99; });
    const observed: unknown[] = [];
    const outcomes: Outcome<unknown, OperationId>[] = [
      { kind: "indeterminate", operationId: operation },
      { kind: "failed", error: { message: "executor rejected" } },
      { kind: "succeeded", value: 7 },
    ];
    const host: HarnessRuntimeHost = {
      policyIdentity: () => null,
      async attach() { throw new Error("not used"); },
      async reconcileEffect() { return { state: "indeterminate" }; },
      async send(message) { return { accepted: true, messageId: message.id }; },
      async *inbox() { yield* []; },
      async executeTool(taskId, operationId, definition, input) {
        observed.push({ taskId, operationId, name: definition.definition.name, input });
        return outcomes.shift() ?? { kind: "cancelled", receipt: { requested: true, taskId } };
      },
    };
    const runtime = Harness.builder(contracts).host(host).tool(tool).grant("tool:call:durable-count").build();
    const context = new TaskContext(runtime, new AbortController().signal, task, true);
    expect(() => context.call(runtime.tool(tool), 2)).toThrow("callDurable");
    expect(await context.callDurable(operation, runtime.tool(tool), 2)).toEqual({ kind: "indeterminate", operationId: operation });
    expect(await context.callDurable(operation, runtime.tool(tool), 2)).toEqual({ kind: "failed", error: { message: "executor rejected" } });
    expect(await context.callDurable(operation, runtime.tool(tool), 2)).toEqual({ kind: "succeeded", value: 7 });
    expect(await context.callDurable(operation, runtime.tool(tool), 2)).toEqual({ kind: "cancelled", receipt: { requested: true, taskId: task } });
    expect(localExecutions).toBe(0);
    expect(observed).toHaveLength(4);
  });

  test("durable approval is routed only through the owner host and preserves its outcome", async () => {
    let dispatched = 0;
    const tool = defineTool<number, number>({ name: "guarded-durable", revision: "1", description: "guarded",
      inputSchema: { type: "number" }, outputSchema: { type: "number" }, parseInput: parseNumber,
      parseOutput: parseNumber }, () => 1);
    const host: HarnessRuntimeHost = {
      policyIdentity: () => approvalPolicyIdentity,
      async attach() { throw new Error("not used"); },
      async reconcileEffect() { return { state: "indeterminate" }; },
      async send(message) { return { accepted: true, messageId: message.id }; },
      async *inbox() { yield* []; },
      async executeTool(_taskId, operationId) {
        dispatched++;
        return { kind: "indeterminate", operationId };
      },
    };
    const runtime = Harness.builder(contracts).host(host).tool(tool)
      .policy({ identity: () => approvalPolicyIdentity,
        evaluate: async () => ({ kind: "require-approval", prompt: "approve exact tool call" }) })
      .interactions({ route: async () => { throw new Error("local approval must not be used"); } })
      .grant("tool:call:guarded-durable").build();
    const context = new TaskContext(runtime, new AbortController().signal, "task:approval" as RuntimeTaskId, true);
    const operation = "02020202-0202-0202-0202-020202020202" as OperationId;
    const outcome = await context.callDurable(operation,
      runtime.tool(tool), 1);
    expect(outcome).toEqual({ kind: "indeterminate", operationId: operation });
    expect(dispatched).toBe(1);
  });

  test("durable hosts reject unpinned or composition-mismatched policies", () => {
    expect(() => policyIdentity("policy", "1", new Uint8Array(32))).toThrow("nonzero");
    expect(() => policyIdentity("policy", "1", new Uint8Array(31).fill(1))).toThrow("32-byte");
    const host: HarnessRuntimeHost = {
      policyIdentity: () => approvalPolicyIdentity,
      async attach() { throw new Error("not used"); },
      async reconcileEffect() { return { state: "indeterminate" }; },
      async send(message) { return { accepted: true, messageId: message.id }; },
      async *inbox() { yield* []; },
    };
    expect(() => Harness.builder(contracts).host(host).build()).toThrow("policy identity differs");
    expect(() => Harness.builder(contracts).host(host).policy({ identity: () => denyPolicyIdentity,
      evaluate: async () => ({ kind: "deny", reason: "denied" }) }).build()).toThrow("policy identity differs");
    const runtime = Harness.builder(contracts).host(host).policy({ identity: () => approvalPolicyIdentity,
      evaluate: async () => ({ kind: "allow" }) }).build();
    expect(() => runtime.scoped(ExecutionScope.create().policy({ identity: () => scopedPolicyIdentity,
      evaluate: async () => ({ kind: "deny", reason: "child" }) }))).toThrow("policy identity differs");
  });

  test("policy implementation drift during evaluation cannot dispatch a tool", async () => {
    let current = approvalPolicyIdentity;
    let executed = false;
    const tool = defineTool<number, number>({ name: "pinned-evaluation", revision: "1", description: "pinned",
      inputSchema: { type: "number" }, outputSchema: { type: "number" },
      parseInput: parseNumber, parseOutput: parseNumber }, (_context, value) => {
      executed = true;
      return value;
    });
    const runtime = Harness.builder(contracts).tool(tool).grant("tool:call:pinned-evaluation")
      .policy({ identity: () => current, evaluate: async () => {
        current = scopedPolicyIdentity;
        return { kind: "allow" };
      } }).build();
    await expect(runtime.call(runtime.tool(tool), 1)).rejects.toThrow("implementation changed");
    expect(executed).toBe(false);
  });

  test("typed interactions validate answer JSON before granting a result type", async () => {
    const observed: unknown[] = [];
    const runtime = Harness.builder(contracts).interactions({ route: async interaction => {
      observed.push(interaction);
      return { kind: "answered", value: 4 };
    } }).build();
    const context = new TaskContext(runtime, new AbortController().signal);
    const answer = await context.interactTyped({ id: "question:typed", kind: "question", prompt: "Count?" }, numberSchema);
    expect(answer).toEqual({ kind: "answered", value: 4 });
    expect(observed[0]).toMatchObject({ responseSchema: { type: "number" } });
    const invalid = Harness.builder(contracts).interactions({ route: async () => ({ kind: "answered", value: "four" }) }).build();
    await expect(new TaskContext(invalid, new AbortController().signal)
      .interactTyped({ id: "question:invalid", kind: "question", prompt: "Count?" }, numberSchema))
      .rejects.toThrow();
  });

  test("nested scopes preserve inherited grants, limits, and policy", async () => {
    const observed: EffectiveScope[] = [];
    const tool = defineTool<null, string>({ name: "guarded", revision: "1", description: "guarded", inputSchema: {}, outputSchema: {}, parseInput: parseNull, parseOutput: parseString }, () => "called");
    const deadline = new Date(Date.now() + 60_000);
    const parentPolicy = {
      identity: () => denyPolicyIdentity,
      async evaluate(_invocation: unknown, scope: EffectiveScope) {
        observed.push(scope);
        return { kind: "deny", reason: "parent denied" } as const;
      },
    };
    const runtime = Harness.builder(contracts).policy(parentPolicy).tool(tool).grant("read", "write", "tool:call:guarded").build().scoped(
      ExecutionScope.create().withLimits({ concurrency: 2, deadline, maxSteps: 12 }),
    );
    const child = runtime.scoped(ExecutionScope.create().policy({ identity: () => scopedPolicyIdentity,
      evaluate: async () => ({ kind: "allow" }) }));

    await expect(child.call(child.tool("guarded"), null)).rejects.toThrow("parent denied");
    expect(observed).toEqual([{ grants: ["read", "write", "tool:call:guarded"], limits: { concurrency: 2, deadline, maxSteps: 12 } }]);
    const withoutToolGrant = runtime.scoped(ExecutionScope.create().onlyGrants("read"));
    await expect(withoutToolGrant.call(withoutToolGrant.tool("guarded"), null))
      .rejects.toThrow("task scope lacks tool:call:guarded");
    expect(() => runtime.scoped(ExecutionScope.create().grant("admin"))).toThrow("child scope cannot widen grants");
    expect(runtime.scoped(ExecutionScope.create().onlyGrants()).scope.grants).toEqual([]);
    expect(() => runtime.scoped(ExecutionScope.create().withLimits({ concurrency: 3 }))).toThrow("child scope cannot widen concurrency");
    expect(() => runtime.scoped(ExecutionScope.create().withLimits({ maxSteps: 13 }))).toThrow("child scope cannot widen maxSteps");
    expect(() => new HarnessBuilder(contracts).limits({ model_steps: 0 })).toThrow("harness limits are invalid");
    expect(() => runtime.scoped(ExecutionScope.create().withLimits({ deadline: new Date(deadline.getTime() + 1) }))).toThrow("child scope cannot extend deadline");
    const grantless = Harness.builder(contracts).tool(tool).build().scoped(ExecutionScope.create());
    await expect(grantless.call(grantless.tool("guarded"), null)).rejects.toThrow("task scope lacks tool:call:guarded");
    expect(() => grantless.scoped(ExecutionScope.create().grant("admin"))).toThrow("child scope cannot widen grants");
    const directlyRestricted = Harness.builder(contracts).grant("read").build();
    expect(() => directlyRestricted.scoped(ExecutionScope.create().grant("admin"))).toThrow("child scope cannot widen grants");
    expect(() => Reflect.construct(AgentHarness, [new Map(), new Map(), {}, ExecutionScope.create().grant("read"), new Map(), new Map()]))
      .toThrow("must be created through HarnessBuilder");
  });

  test("keeps rejected batch admissions explicit after close", async () => {
    const task = TaskDefinition.live<number, number>("identity", "1", async (_context, value) => value);
    const group = Harness.builder(contracts).task(task).build().group<number>(GroupPolicies.collectAll);
    group.close();
    const entries = await group.spawnMany(task, new Batch("batch:test" as BatchId, [1, 2]));

    expect(entries.map(entry => entry.admission.kind)).toEqual(["rejected", "rejected"]);
  });

  test("streams thousands of completed tasks once without rebuilding pending races", async () => {
    const task = TaskDefinition.live<number, number>("bulk", "1", async (_context, value) => value);
    const group = Harness.builder(contracts).task(task).build().group<number>(GroupPolicies.collectAll);
    const inputs = Array.from({ length: 2_048 }, (_value, index) => index);
    await group.spawnMany(task, new Batch("batch:bulk" as BatchId, inputs));
    const completed: number[] = [];
    for await (const entry of group.asCompleted()) {
      if (entry.outcome?.kind === "succeeded") completed.push(entry.outcome.value);
    }
    expect(completed.length).toBe(inputs.length);
    expect(completed.sort((left, right) => left - right)).toEqual(inputs);
  });

  test("routes recovery, effect reconciliation, and typed messaging through a durable host", async () => {
    const messages: TaskMessage[] = [];
    const recoveredId = "task:recovered" as RuntimeTaskId;
    const payload = await mailboxFile(10);
    const host: HarnessRuntimeHost = {
      policyIdentity: () => null,
      async attach(id) { return { task: new Task(id, async () => 99), operationId: "operation:recovered", taskName: "hosted", revision: "1", implementationDigest: durableDigest }; },
      async reconcileEffect() { return { state: "succeeded", result: payload } as const; },
      async send(message: TaskMessage) { messages.push(message); return { accepted: true, messageId: message.id }; },
      async *inbox() { for (const message of messages) yield message; },
    };
    const definition = TaskDefinition.live<void, number>("hosted", "1", async context => {
      const effect = await context.reconcileEffect("effect:one" as EffectId);
      if (effect.state !== "succeeded") return -1;
      await context.send(recoveredId, payload, "message:one" as MessageId);
      for await (const message of context.inbox()) return message.value.descriptor.byte_length;
      return -1;
    });
    const runtime = Harness.builder(contracts).host(host).task(definition).build();
    expect(await runtime.spawn(definition, undefined).result()).toEqual({ kind: "succeeded", value: 10 });
    expect(messages[0]?.value).toEqual(payload);
    expect(() => new TaskContext(runtime, new AbortController().signal).send(recoveredId, { body: "inline secret" } as unknown as FileRef)).toThrow();
    expect(await (await runtime.attach([...runtime.running.keys()][0]!)).result()).toEqual({ kind: "succeeded", value: 10 });
  });

  test("cancels hanging siblings as soon as a cancel-on-failure group fails", async () => {
    let aborted = false;
    const task = TaskDefinition.live<"fail" | "hang", string>("work", "1", async (context, value) => {
      if (value === "fail") throw new Error("failed");
      context.signal.addEventListener("abort", () => { aborted = true; }, { once: true });
      await context.sleepUntil(new Date(Date.now() + 60_000));
      return "late";
    });
    const runtime = Harness.builder(contracts).task(task).build();
    await expect(runtime.group<string>(GroupPolicies.cancelOnFailure).map<"fail" | "hang">(task, ["hang", "fail"])).rejects.toThrow();
    expect(aborted).toBeTrue();
  });

  test("observes cancellation when a wait starts after its signal was aborted", async () => {
    const runtime = Harness.builder(contracts).build();
    const controller = new AbortController();
    controller.abort(new Error("already cancelled"));
    const task = new Task("task:pre-aborted" as RuntimeTaskId, signal =>
      new TaskContext(runtime, signal).sleepUntil(new Date(Date.now() + 60_000)), controller);

    expect(await task.result()).toMatchObject({ kind: "cancelled" });
  });

  test("race returns the first terminal outcome; firstSuccess ignores early failures", async () => {
    const task = TaskDefinition.live<{ readonly delay: number; readonly value?: string }, string>("race", "1", async (context, input) => {
      await context.sleepUntil(new Date(Date.now() + input.delay));
      if (input.value === undefined) throw new Error("early failure");
      return input.value;
    });
    const runtime = Harness.builder(contracts).task(task).build();
    const group = runtime.group<string>(GroupPolicies.collectAll);
    await group.spawnMany(task, new Batch("batch:race" as BatchId, [{ delay: 0 }, { delay: 10, value: "winner" }, { delay: 50, value: "late" }]));
    const first = await group.race();
    expect(first.outcome?.kind).toBe("failed");
    const succeeding = runtime.group<string>(GroupPolicies.collectAll);
    await succeeding.spawnMany(task, new Batch("batch:first-success" as BatchId, [{ delay: 0 }, { delay: 10, value: "winner" }, { delay: 50, value: "late" }]));
    expect((await succeeding.firstSuccess()).value).toBe("winner");
  });

  test("executes model tool calls with durable task ownership and typed receipts", async () => {
    let modelStep = 0; let sender: RuntimeTaskId | undefined;
    const payload = await mailboxFile(3);
    const host: HarnessRuntimeHost = {
      policyIdentity: () => null,
      async attach(id) { return { task: new Task(id, async () => undefined), operationId: "operation:run", taskName: "acyclic.default-agent", revision: "1", implementationDigest: durableDigest }; },
      async reconcileEffect() { return { state: "succeeded", result: payload } as const; },
      async send(message: TaskMessage) { sender = message.sender; return { accepted: true, messageId: message.id }; },
      async *inbox() { yield* [] as TaskMessage[]; },
    };
    const tool = defineTool<number, number>({ name: "double", revision: "1", description: "double", inputSchema: {}, outputSchema: {}, parseInput: parseNumber, parseOutput: parseNumber }, async (context, input) => {
      const effect = await context.reconcileEffect("effect:tool" as EffectId);
      await context.send(context.taskId!, payload);
      return effect.state === "succeeded" ? input * 4 : 0;
    });
    const runtime = Harness.builder(contracts).host(host).tool(tool).grant("tool:call:double").model({
      async *generate() { if (modelStep++ === 0) { yield { kind: "tool_call" as const, callId: "call", name: "double", arguments: 3 }; yield { kind: "completed" as const, metadata: {} }; } else { yield { kind: "content" as const, delta: "done" }; yield { kind: "completed" as const, metadata: { tokens: 1 } }; } },
      async reconcile() { return undefined; },
    }).build();
    const output = await runtime.run("go");
    expect(output.text).toBe("done");
    expect(output.receipts).toEqual([{ kind: "model-completed", metadata: {} }, { kind: "tool", step: 0, callId: "call", name: "double", arguments: 3, value: 12, projection: 12 }, { kind: "model-completed", metadata: { tokens: 1 } }]);
    expect(sender).toBe(output.taskId);
    expect(await (await runtime.attach(output.taskId)).result()).toMatchObject({ kind: "succeeded", value: { text: "done" } });
  });

  test("typed run input carries attachment refs into the model context", async () => {
    let observed: readonly ModelMessage[] = [];
    const file = {
      volume: { provider: { namespace: "test", family: "filesystem", version: "2" }, id: "project", class: "project" as const, owner: { kind: "project" as const, id: "project" } },
      path: "images/chart.png", version: "generation", descriptor: await descriptorFor(new Uint8Array([1, 2]), "image/png"), display_name: "chart.png",
    };
    const runtime = Harness.builder(contracts).model({
      async *generate(request) { observed = request.messages; yield { kind: "completed" as const, metadata: {} }; },
      async reconcile() { return undefined; },
    }).build();
    await runtime.run({ prompt: "describe", content: [{ kind: "file", file, policy: "native" }] });
    expect(observed[0]?.content).toEqual([{ kind: "text", text: "describe" }, { kind: "file", file, policy: "native" }]);
  });

  test("selected context preserves canonical roles without a synthetic user duplicate", async () => {
    let observed: readonly ModelMessage[] = [];
    const runtime = Harness.builder(contracts).model({
      async *generate(request) { observed = request.messages; yield { kind: "completed" as const, metadata: {} }; },
      async reconcile() { return undefined; },
    }).build();
    await runtime.runSelectedContext({
      selection: { conversationRevision: 2n, messageIds: [
        fixtureMessageId("01010101-0101-0101-0101-010101010101"), fixtureMessageId("02020202-0202-0202-0202-020202020202"),
      ] },
      messages: [
        { role: "assistant", content: "earlier answer" },
        { role: "user", content: [{ kind: "text", text: "follow-up" }] },
      ],
    });
    expect(observed).toHaveLength(2);
    expect(observed[0]?.role).toBe("assistant");
    expect(observed[1]?.content).toEqual([{ kind: "text", text: "follow-up" }]);
    await expect(runtime.run({ prompt: "conflicting prompt", selectedContext: {
      selection: { conversationRevision: 1n, messageIds: [fixtureMessageId("02020202-0202-0202-0202-020202020202")] },
      messages: [{ role: "user", content: "recorded prompt" }],
    } } as never)).rejects.toThrow("cannot be combined");
  });

  test("builder limits bound selected history and direct input before model dispatch", async () => {
    let dispatched = 0;
    const runtime = Harness.builder(contracts).limits({ context_messages: 1, render_bytes: 8 }).model({
      async *generate() { dispatched++; yield { kind: "completed" as const, metadata: null }; },
      async reconcile() { return undefined; },
    }).build();
    await expect(runtime.run("nine bytes")).rejects.toThrow("render limit");
    await expect(runtime.runSelectedContext({
      selection: { conversationRevision: 2n, messageIds: [
        fixtureMessageId("01010101-0101-0101-0101-010101010101"), fixtureMessageId("02020202-0202-0202-0202-020202020202"),
      ] },
      messages: [{ role: "assistant", content: "old" }, { role: "user", content: "new" }],
    })).rejects.toThrow("selected context");
    expect(dispatched).toBe(0);
  });

  test("stream event limits stop an unbounded provider before further dispatch", async () => {
    let produced = 0;
    const runtime = Harness.builder(contracts).limits({ model_events_per_step: 2, tool_calls_per_step: 1 }).model({
      async *generate() {
        while (true) { produced++; yield { kind: "content" as const, delta: "." }; }
      },
      async reconcile() { return undefined; },
    }).build();
    await expect(runtime.run("bounded")).rejects.toThrow("model event limit exceeded");
    expect(produced).toBe(3);
  });

  test("custom context builders cannot bypass model-content bounds", async () => {
    let dispatched = 0;
    const runtime = Harness.builder(contracts).limits({ render_bytes: 8 }).context({
      async build() { return [{ role: "user" as const, content: "too much context" }]; },
    }).model({
      async *generate() { dispatched++; yield { kind: "completed" as const, metadata: {} }; },
      async reconcile() { return undefined; },
    }).build();
    await expect(runtime.run("short")).rejects.toThrow("render limit");
    expect(dispatched).toBe(0);
  });

  test("model context tool values are admitted by Rust canonical JSON before dispatch", async () => {
    let dispatched = 0;
    const runtime = Harness.builder(contracts).context({
      async build() { return [{ role: "assistant" as const, content: {
        kind: "tool_call" as const, callId: "call", name: "tool", arguments: { unsafe: Number.POSITIVE_INFINITY },
      } }, { role: "user" as const, content: "safe" }]; },
    }).model({
      async *generate() { dispatched++; yield { kind: "completed" as const, metadata: {} }; },
      async reconcile() { return undefined; },
    }).build();
    await expect(runtime.run("safe")).rejects.toThrow();
    expect(dispatched).toBe(0);
  });

  test("local task names and revisions use one bounded registry contract", () => {
    const task = TaskDefinition.live("leaf", "1", async () => 1);
    expect(task.name).toBe("leaf");
    expect(Object.isFrozen(task)).toBe(true);
    expect(Object.isFrozen(task.implementation)).toBe(true);
    expect(() => TaskDefinition.live("bad/name", "1", async () => 1)).toThrow("task name");
    expect(() => TaskDefinition.live("leaf", "bad version", async () => 1)).toThrow("task revision");
  });

  test("tool registration snapshots its public definition", () => {
    const mutable = { name: "echo", revision: "1", description: "first", inputSchema: {}, outputSchema: {}, parseInput: parseNull, parseOutput: parseString,
      handler: async () => "ok" };
    const runtime = Harness.builder(contracts).tool(mutable).build();
    mutable.name = "different";
    mutable.description = "changed";
    expect(runtime.tool("echo").definition).toMatchObject({ name: "echo", description: "first" });
    expect(Object.isFrozen(runtime.tool("echo").definition)).toBe(true);
  });

  test("reused provider call IDs have distinct internal tool operations", async () => {
    const operations: string[] = [];
    const tool = defineTool<number, number>({ name: "again", revision: "1", description: "again",
      inputSchema: {}, outputSchema: {}, parseInput: parseNumber, parseOutput: parseNumber }, async (context, input) => {
      expect(context.callId).toBe("provider-call");
      operations.push(context.operationId!);
      return input;
    });
    let step = 0;
    const runtime = Harness.builder(contracts).tool(tool).grant("tool:call:again").model({
      async *generate() {
        if (step++ < 2) {
          yield { kind: "tool_call" as const, callId: "provider-call", name: "again", arguments: step };
        } else yield { kind: "content" as const, delta: "done" };
        yield { kind: "completed" as const, metadata: {} };
      },
      async reconcile() { return undefined; },
    }).build();
    expect((await runtime.run("again")).text).toBe("done");
    expect(operations).toHaveLength(2);
    expect(new Set(operations).size).toBe(2);
  });
});
