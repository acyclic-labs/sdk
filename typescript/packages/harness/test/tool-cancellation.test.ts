import { expect, test } from "bun:test";
import { ExecutionScope, Harness, type HarnessRuntimeState, type OperationId, type TaskId } from "../src/index.js";
import { approvalPolicyIdentity, contracts, jsonProjection, parseNumber, scopedPolicyIdentity } from "./support/runtime-fixture.js";

function gate() {
  const entered = Promise.withResolvers<void>();
  const released = Promise.withResolvers<void>();
  return {
    entered: entered.promise,
    release: () => released.resolve(),
    wait: () => { entered.resolve(); return released.promise; },
  };
}

function counterTool(binding: "handler" | "executor", execute: (value: number) => Promise<number>) {
  const definition = {
    name: "cancelled-tool", revision: "1", description: "Count local dispatches",
    inputSchema: { type: "number" }, outputSchema: { type: "number" },
    parseInput: parseNumber, parseOutput: parseNumber, projection: jsonProjection,
  };
  const builder = Harness.builder(contracts).grant("tool:call:cancelled-tool");
  return binding === "handler"
    ? builder.tool<number, number>({ ...definition, handler: (_context, value) => execute(value) })
    : builder.tool<number, number>(definition, {
      execute: async invocation => ({ value: await execute(invocation.arguments) }),
      reconcile: async () => undefined,
    });
}

for (const binding of ["handler", "executor"] as const) {
  for (const boundary of ["policy", "approval"] as const) {
    for (const cancel of [false, true]) {
      test(`${binding} ${cancel ? "does not dispatch after cancellation" : "dispatches once"} during ${boundary}`, async () => {
        const pending = gate();
        const controller = new AbortController();
        const reason = new Error("caller cancelled while awaiting a decision");
        let executions = 0;
        let approvals = 0;
        const runtime = await counterTool(binding, async value => { executions++; return value; })
          .policy({ identity: () => approvalPolicyIdentity, evaluate: async () => {
            if (boundary === "policy") await pending.wait();
            return { kind: "require-approval", prompt: "Allow this tool?" };
          } })
          .interactions({ route: async () => {
            approvals++;
            if (boundary === "approval") await pending.wait();
            return { kind: "approved" };
          } }).build();
        const result = runtime.call(runtime.tool("cancelled-tool"), 7, controller.signal);
        await pending.entered;
        expect(executions).toBe(0);
        if (cancel) controller.abort(reason);
        pending.release();
        if (cancel) await expect(result).rejects.toBe(reason);
        else expect(await result).toBe(7);
        expect(executions).toBe(cancel ? 0 : 1);
        expect(approvals).toBe(cancel && boundary === "policy" ? 0 : 1);
      });
    }
  }

  test(`${binding} rejects an already cancelled call before policy evaluation`, async () => {
    let evaluations = 0;
    let executions = 0;
    const runtime = await counterTool(binding, async value => { executions++; return value; })
      .policy({ identity: () => approvalPolicyIdentity, evaluate: async () => {
        evaluations++;
        return { kind: "allow" };
      } }).build();
    const controller = new AbortController();
    controller.abort();
    await expect(runtime.call(runtime.tool("cancelled-tool"), 1, controller.signal))
      .rejects.toBe(controller.signal.reason);
    expect(evaluations).toBe(0);
    expect(executions).toBe(0);
  });

  for (const answerKind of ["cancelled", "denied"] as const) {
    for (const cancel of [false, true]) {
      test(`${binding} ${cancel ? "preserves caller cancellation" : "preserves approval outcome"} when the router returns ${answerKind}`, async () => {
        const pending = gate();
        const controller = new AbortController();
        const reason = new Error("caller cancelled while the approval router was pending");
        let executions = 0;
        const runtime = await counterTool(binding, async value => { executions++; return value; })
          .policy({ identity: () => approvalPolicyIdentity,
            evaluate: async () => ({ kind: "require-approval", prompt: "Allow this tool?" }) })
          .interactions({ route: async () => {
            await pending.wait();
            return { kind: answerKind };
          } }).build();
        const result = runtime.call(runtime.tool("cancelled-tool"), 7, controller.signal);
        await pending.entered;
        if (cancel) controller.abort(reason);
        pending.release();
        if (cancel) await expect(result).rejects.toBe(reason);
        else await expect(result).rejects.toMatchObject({ kind: answerKind });
        expect(executions).toBe(0);
      });
    }
  }

  test(`${binding} preserves a completed effect when cancellation follows dispatch`, async () => {
    const pending = gate();
    const controller = new AbortController();
    let executions = 0;
    const runtime = await counterTool(binding, async value => {
      executions++;
      await pending.wait();
      return value;
    }).build();
    const result = runtime.call(runtime.tool("cancelled-tool"), 9, controller.signal);
    await pending.entered;
    controller.abort();
    pending.release();
    expect(await result).toBe(9);
    expect(executions).toBe(1);
  });
}

test("cancellation during one approval prevents requesting a later scoped approval", async () => {
  const pending = gate();
  const controller = new AbortController();
  const reason = new Error("cancelled before the next approval");
  let executions = 0;
  const prompts: string[] = [];
  const root = await counterTool("handler", async value => { executions++; return value; })
    .policy({ identity: () => approvalPolicyIdentity,
      evaluate: async () => ({ kind: "require-approval", prompt: "parent approval" }) })
    .interactions({ route: async interaction => {
      if (interaction.kind !== "approval") throw new TypeError("expected an approval");
      prompts.push(interaction.prompt);
      if (prompts.length === 1) await pending.wait();
      return { kind: "approved" };
    } }).build();
  const runtime = await root.scoped(new ExecutionScope().policy({ identity: () => scopedPolicyIdentity,
    evaluate: async () => ({ kind: "require-approval", prompt: "child approval" }) }));
  const result = runtime.call(runtime.tool("cancelled-tool"), 1, controller.signal);
  await pending.entered;
  controller.abort(reason);
  pending.release();
  await expect(result).rejects.toBe(reason);
  expect(prompts).toEqual(["parent approval"]);
  expect(executions).toBe(0);
});

for (const boundary of ["policy", "owner"] as const) {
  test(`durable tool cancellation during ${boundary} ${boundary === "policy" ? "prevents dispatch" : "preserves the owner outcome"}`, async () => {
    const pending = gate();
    const controller = new AbortController();
    const reason = new Error("caller cancelled a durable call");
    const operation = "12345678-1234-4234-8234-123456789abc" as OperationId;
    const task = "22345678-1234-4234-8234-123456789abc" as TaskId;
    let executions = 0;
    const state: HarnessRuntimeState = {
      policyIdentity: () => approvalPolicyIdentity,
      async attach() { throw new Error("not used"); },
      async reconcileEffect() { return { state: "indeterminate" }; },
      async send(message) { return { accepted: true, messageId: message.id }; },
      async *inbox() { yield* []; },
      async executeTool(observedTask, observedOperation) {
        expect(observedTask).toBe(task);
        expect(observedOperation).toBe(operation);
        executions++;
        if (boundary === "owner") await pending.wait();
        return { kind: "indeterminate", operationId: operation };
      },
    };
    const runtime = await counterTool("handler", async () => { throw new Error("local dispatch is not allowed"); })
      .state(state)
      .policy({ identity: () => approvalPolicyIdentity, evaluate: async () => {
        if (boundary === "policy") await pending.wait();
        return { kind: "allow" };
      } }).build();
    const result = runtime.callDurable(operation, runtime.tool("cancelled-tool"), 2, task, controller.signal);
    await pending.entered;
    expect(executions).toBe(boundary === "policy" ? 0 : 1);
    controller.abort(reason);
    pending.release();
    if (boundary === "policy") await expect(result).rejects.toBe(reason);
    else expect(await result).toEqual({ kind: "indeterminate", operationId: operation });
    expect(executions).toBe(boundary === "policy" ? 0 : 1);
  });
}
