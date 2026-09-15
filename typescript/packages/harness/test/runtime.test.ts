import { describe, expect, test } from "bun:test";
import {
  Batch,
  ExecutionScope,
  GroupPolicies,
  Harness,
  TaskDefinition,
  Task,
  defineTool,
  type BatchId,
  type EffectId,
  type EffectiveScope,
  type HarnessRuntimeHost,
  type MessageId,
  type Outcome,
  type RuntimeTaskId,
  type TaskMessage,
} from "../src/index.js";

describe("typed agent runtime", () => {
  test("preserves task input and output types through nested execution", async () => {
    const double = TaskDefinition.live<number, number>("double", "1", async (_context, value) => value * 2);
    const sum = TaskDefinition.live<readonly number[], number>("sum", "1", async (context, values) => {
      const group = context.group<number>(GroupPolicies.collectAll);
      return (await group.map(double, values)).reduce((left, right) => left + right, 0);
    });
    const runtime = Harness.builder().task(double).task(sum).build();

    const outcome: Outcome<number> = await runtime.spawn(sum, [1, 2, 3]).result();
    expect(outcome).toEqual({ kind: "succeeded", value: 12 });
  });

  test("routes typed tools through policy and interaction boundaries", async () => {
    const lengths = defineTool<{ readonly text: string }, number>({
      name: "length",
      revision: "1",
      description: "Count characters",
      inputSchema: { type: "object" },
      outputSchema: { type: "number" },
    }, (_context, input) => input.text.length);
    const runtime = Harness.builder()
      .interactions({ route: async () => ({ kind: "accepted", text: "approved" }) })
      .policy({ evaluate: async () => ({ kind: "require-approval", prompt: "approve" }) })
      .tool(lengths)
      .build();

    expect(await runtime.call(runtime.tool("length"), { text: "typed" })).toBe(5);
  });

  test("nested scopes preserve inherited grants, limits, and policy", async () => {
    const observed: EffectiveScope[] = [];
    const tool = defineTool<void, string>({ name: "guarded", revision: "1", description: "guarded", inputSchema: {}, outputSchema: {} }, () => "called");
    const deadline = new Date(Date.now() + 60_000);
    const parentPolicy = {
      async evaluate(_invocation: unknown, scope: EffectiveScope) {
        observed.push(scope);
        return { kind: "deny", reason: "parent denied" } as const;
      },
    };
    const runtime = Harness.builder().policy(parentPolicy).tool(tool).build().scoped(
      ExecutionScope.create().grant("read", "write").withLimits({ concurrency: 2, deadline }),
    );
    const child = runtime.scoped(ExecutionScope.create().policy({ evaluate: async () => ({ kind: "allow" }) }));

    await expect(child.call(child.tool("guarded"), undefined)).rejects.toThrow("parent denied");
    expect(observed).toEqual([{ grants: ["read", "write"], limits: { concurrency: 2, deadline } }]);
    expect(() => runtime.scoped(ExecutionScope.create().grant("admin"))).toThrow("child scope cannot widen grants");
    expect(() => runtime.scoped(ExecutionScope.create().withLimits({ concurrency: 3 }))).toThrow("child scope cannot widen concurrency");
    expect(() => runtime.scoped(ExecutionScope.create().withLimits({ deadline: new Date(deadline.getTime() + 1) }))).toThrow("child scope cannot extend deadline");
  });

  test("keeps rejected batch admissions explicit after close", async () => {
    const task = TaskDefinition.live<number, number>("identity", "1", async (_context, value) => value);
    const group = Harness.builder().task(task).build().group<number>(GroupPolicies.collectAll);
    group.close();
    const entries = await group.spawnMany(task, new Batch("batch:test" as BatchId, [1, 2]));

    expect(entries.map(entry => entry.admission.kind)).toEqual(["rejected", "rejected"]);
  });

  test("routes recovery, effect reconciliation, and typed messaging through a durable host", async () => {
    const messages: TaskMessage<number>[] = [];
    const recoveredId = "task:recovered" as RuntimeTaskId;
    const host: HarnessRuntimeHost = {
      async attach<Output>(id) { return new Task(id, async () => 99 as Output); },
      async reconcileEffect<Value>(_taskId, effectId) { return { kind: "completed", value: effectId.length as Value }; },
      async send<Value>(message: TaskMessage<Value>) { messages.push(message as TaskMessage<number>); return { accepted: true, messageId: message.id }; },
      async *inbox<Value>() { for (const message of messages) yield message as TaskMessage<Value>; },
    };
    const definition = TaskDefinition.live<void, number>("hosted", "1", async context => {
      const effect = await context.reconcileEffect<number>("effect:one" as EffectId);
      await context.send(recoveredId, effect.kind === "completed" ? effect.value : 0, "message:one" as MessageId);
      for await (const message of context.inbox<number>()) return message.value;
      return -1;
    });
    const runtime = Harness.builder().host(host).task(definition).build();
    expect(await runtime.spawn(definition, undefined).result()).toEqual({ kind: "succeeded", value: 10 });
    expect(await (await runtime.attach<number>(recoveredId)).result()).toEqual({ kind: "succeeded", value: 99 });
  });

  test("cancels hanging siblings as soon as a cancel-on-failure group fails", async () => {
    let aborted = false;
    const task = TaskDefinition.live<"fail" | "hang", string>("work", "1", async (context, value) => {
      if (value === "fail") throw new Error("failed");
      context.signal.addEventListener("abort", () => { aborted = true; }, { once: true });
      await context.sleepUntil(new Date(Date.now() + 60_000));
      return "late";
    });
    const runtime = Harness.builder().task(task).build();
    await expect(runtime.group<string>(GroupPolicies.cancelOnFailure).map(task, ["hang", "fail"])).rejects.toThrow();
    expect(aborted).toBeTrue();
  });

  test("race ignores an early failure and returns the first observed success", async () => {
    const task = TaskDefinition.live<{ readonly delay: number; readonly value?: string }, string>("race", "1", async (context, input) => {
      await context.sleepUntil(new Date(Date.now() + input.delay));
      if (input.value === undefined) throw new Error("early failure");
      return input.value;
    });
    const runtime = Harness.builder().task(task).build();
    const group = runtime.group<string>(GroupPolicies.collectAll);
    await group.spawnMany(task, new Batch("batch:race" as BatchId, [{ delay: 0 }, { delay: 10, value: "winner" }, { delay: 50, value: "late" }]));
    const winner = await group.race();
    expect(winner.outcome).toEqual({ kind: "succeeded", value: "winner" });
  });

  test("executes model tool calls with durable task ownership and typed receipts", async () => {
    let modelStep = 0; let sender: RuntimeTaskId | undefined;
    const host: HarnessRuntimeHost = {
      async attach<Output>(id) { return new Task(id, async () => undefined as Output); },
      async reconcileEffect<Value>() { return { kind: "completed", value: 4 as Value }; },
      async send<Value>(message: TaskMessage<Value>) { sender = message.sender; return { accepted: true, messageId: message.id }; },
      async *inbox<Value>() { yield* [] as TaskMessage<Value>[]; },
    };
    const tool = defineTool<number, number>({ name: "double", revision: "1", description: "double", inputSchema: {}, outputSchema: {} }, async (context, input) => {
      const effect = await context.reconcileEffect<number>("effect:tool" as EffectId);
      await context.send(context.taskId!, input);
      return effect.kind === "completed" ? input * effect.value : 0;
    });
    const runtime = Harness.builder().host(host).tool(tool).model({
      async *generate() { if (modelStep++ === 0) yield { kind: "tool_call" as const, callId: "call", name: "double", arguments: 3 }; else { yield { kind: "content" as const, delta: "done" }; yield { kind: "completed" as const, metadata: { tokens: 1 } }; } },
      async reconcile() { return undefined; },
    }).build();
    const output = await runtime.run("go");
    expect(output.text).toBe("done");
    expect(output.receipts).toEqual([{ kind: "tool", callId: "call", name: "double", value: 12 }, { kind: "model-completed", metadata: { tokens: 1 } }]);
    expect(sender).toBe(output.taskId);
    expect(await (await runtime.attach(output.taskId)).result()).toMatchObject({ kind: "succeeded", value: { text: "done" } });
  });
});
