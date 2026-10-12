import { test, expect } from "bun:test";
import { defineRuntimeSchema, TaskDefinition, Harness, type BatchId, type WasmTaskAdmissionWire, type HarnessRuntimeSpawner, type HarnessRuntimeState, policyIdentity, Task, type TaskId, type TaskMessage, TaskContext, type OperationId, AdmissionUncertainError, type HarnessExecutionProvider, type GroupId, type HostBatchReplay, type PolicyIdentity, type Outcome, type EffectId, type FileRef } from "../src/index.js";
import { numberSchema, durableDigest, contracts, numberTool, approvalPolicyIdentity, mailboxFile } from "./support/runtime-fixture.js";

test("never executes a resumable definition in the local in-memory runner", async () => {
  await expect(defineRuntimeSchema("empty", {}, value => value)).rejects.toThrow();
  await expect(defineRuntimeSchema("true", true as unknown as Parameters<typeof defineRuntimeSchema>[1], value => value))
    .rejects.toThrow();
  let transitions = 0;
  const definition = await TaskDefinition.resumable<number, number, number>("durable", "1", {
    state: numberSchema,
    initial: input => input,
    async transition(_context, state) { transitions++; return { kind: "finish", output: state * 2 }; },
  }, { implementationDigest: durableDigest, input: numberSchema, output: numberSchema });
  const local = await Harness.builder(contracts).task(definition).build();
  expect(() => local.spawn(definition, 3)).toThrow();
  const rejected = await local.group<number>({ kind: "collect-all" }).spawnMany(definition, { id: "batch:resumable" as BatchId, inputs: [3] });
  expect(rejected[0]?.admission).toMatchObject({ kind: "rejected", reason: { code: "unsupported" } });
  expect(transitions).toBe(0);

  let admitted: { readonly input: number; readonly revision: string } | undefined;
  let retained: WasmTaskAdmissionWire | undefined;
  const admissionId = "dddddddd-dddd-4ddd-8ddd-dddddddddddd";
  const host: HarnessRuntimeSpawner & HarnessRuntimeState = {
    policyIdentity: () => null,
    async admitResumable(_operationId, task, input, _runtime, _parent, record) {
      retained = record;
      if (typeof input !== "number") throw new TypeError("test host expected a number");
      admitted = { input, revision: task.revision };
      return { kind: "accepted" as const, task: new Task("task:durable" as TaskId, async () => 6) };
    },
    async attach(id) { return { task: new Task(id, async () => 6), operationId: admissionId, taskName: "durable", revision: "1", implementationDigest: durableDigest, admission: retained! }; },
    async reconcileEffect() { return { state: "indeterminate" } as const; },
    async send(message: TaskMessage) { return { accepted: true, messageId: message.id }; },
    async *inbox() { yield* [] as TaskMessage[]; },
  }
  const hosted = await Harness.builder(contracts).state(host).spawner(host).task(definition).build();
  const result = await hosted.admit(definition, 3, admissionId);
  expect(result.kind).toBe("accepted");
  if (result.kind !== "accepted") throw new Error("expected admission");
  expect(await result.task.result()).toEqual({ kind: "succeeded", value: 6 });
  expect(admitted).toEqual({ input: 3, revision: "1" });
  expect(transitions).toBe(0);
});

test("restored resumable state is schema-admitted before a typed transition", async () => {
  let transitions = 0;
  const definition = await TaskDefinition.resumable<number, number, number>("checkpoint", "1", {
    state: numberSchema,
    initial: input => input,
    async transition(_context, state) { transitions++; return { kind: "finish", output: state * 2 }; },
  }, { implementationDigest: durableDigest, input: numberSchema, output: numberSchema });
  if (definition.implementation.kind !== "resumable") throw new TypeError("expected resumable definition");
  const context = new TaskContext(await Harness.builder(contracts).build(), new AbortController().signal);
  await expect(definition.implementation.component.transition(context, "corrupt checkpoint")).rejects.toThrow();
  expect(transitions).toBe(0);
  expect(await definition.implementation.component.transition(context, 4)).toEqual({ kind: "finish", output: 8 });
  expect(transitions).toBe(1);
});

test("resumable transitions validate newly saved state and explicit waits", async () => {
  const context = new TaskContext(await Harness.builder(contracts).build(), new AbortController().signal);
  const invalid = await TaskDefinition.resumable<number, number, number>("invalid-checkpoint", "1", {
    state: numberSchema,
    initial: () => "invalid" as unknown as number,
    async transition() { return { kind: "continue", state: "invalid" as unknown as number }; },
  }, { implementationDigest: durableDigest, input: numberSchema, output: numberSchema });
  if (invalid.implementation.kind !== "resumable") throw new TypeError("expected resumable definition");
  await expect(Promise.resolve(invalid.implementation.component.initial(1))).rejects.toThrow();
  await expect(invalid.implementation.component.transition(context, 1)).rejects.toThrow();

  const wait = await TaskDefinition.resumable<number, number, number>("timer-checkpoint", "1", {
    state: numberSchema,
    initial: input => input,
    async transition(_context, state) {
      return { kind: "wait", state: state + 1,
        operationId: "11111111-1111-1111-1111-111111111111" as OperationId,
        deadline: new Date("2030-01-01T00:00:00.000Z") };
    },
  }, { implementationDigest: durableDigest, input: numberSchema, output: numberSchema });
  if (wait.implementation.kind !== "resumable") throw new TypeError("expected resumable definition");
  expect(await wait.implementation.component.transition(context, 1)).toEqual({
    kind: "wait", state: 2, operationId: "11111111-1111-1111-1111-111111111111" as OperationId,
    deadline: new Date("2030-01-01T00:00:00.000Z"),
  });
});

test("host-backed handles preserve authoritative events and cancellation", async () => {
  const taskId = "task:remote" as TaskId;
  const observed: number[] = [];
  let cancelled = false;
  const terminal = { id: "event:terminal", taskId, sequence: 42,
    event: { kind: "settled" as const, outcome: { kind: "succeeded" as const, value: 7 } } };
  const task = new Task(taskId, {
    operationId: "operation:remote",
    async result() { return { kind: "succeeded", value: 7 } as const; },
    async *events(fromSequence) {
      observed.push(fromSequence);
      if (fromSequence <= 41) yield { id: "event:remote", taskId, sequence: 41, event: { kind: "started" } as const };
    },
    async terminalEvent() { return terminal; },
    async cancel() { cancelled = true; return { requested: true, taskId }; },
  });
  expect(await task.result()).toEqual({ kind: "succeeded", value: 7 });
  const events = [];
  for await (const event of task.events(41)) events.push(event);
  expect(events.map(event => event.sequence)).toEqual([41, 42]);
  expect(observed).toEqual([41]);
  const afterTerminal = [];
  for await (const event of task.events(44)) afterTerminal.push(event);
  expect(afterTerminal).toEqual([]);
  expect(observed).toEqual([41, 44]);
  for (const outcome of [
    { kind: "failed" as const, error: { message: "failed" } },
    { kind: "cancelled" as const, receipt: { requested: true, taskId } },
  ]) {
    const settled = { ...terminal, sequence: 1, event: { kind: "settled" as const, outcome } };
    const replayed = new Task(taskId, {
      operationId: "operation:remote",
      async result() { return outcome; },
      async *events(fromSequence) {
        if (fromSequence <= 0) yield { id: "event:started", taskId, sequence: 0, event: { kind: "started" } as const };
        if (fromSequence <= settled.sequence) yield settled;
      },
      async terminalEvent() { return settled; },
      async cancel() { return { requested: true, taskId }; },
    });
    const replay = [];
    for await (const event of replayed.events()) replay.push(event);
    expect(replay).toEqual([{ id: "event:started", taskId, sequence: 0, event: { kind: "started" } }, { ...settled, sequence: 1 }]);
    const past = [];
    for await (const event of replayed.events(settled.sequence + 1)) past.push(event);
    expect(past).toEqual([]);
  }
  expect(await task.cancel()).toEqual({ requested: true, taskId });
  expect(cancelled).toBeTrue();
  const disconnected = new Task(taskId, {
    operationId: "operation:remote",
    async result() { throw new Error("transport offline"); },
    async *events() { yield* []; },
    async terminalEvent() { return { id: "event:indeterminate", taskId, sequence: 0,
      event: { kind: "settled" as const, outcome: { kind: "indeterminate" as const, operationId: "operation:remote" } } }; },
    async cancel() { return { requested: false, taskId }; },
  });
  expect(await disconnected.result()).toEqual({ kind: "indeterminate", operationId: "operation:remote" });
  const disconnectedEvents = [];
  for await (const event of disconnected.events()) disconnectedEvents.push(event.event);
  expect(disconnectedEvents).toEqual([{ kind: "settled", outcome: { kind: "indeterminate", operationId: "operation:remote" } }]);
  const outOfOrder = new Task(taskId, {
    operationId: "operation:remote",
    async result() { return { kind: "succeeded", value: 7 } as const; },
    async *events() { yield { id: "event:started", taskId, sequence: 0, event: { kind: "started" } as const }; },
    async terminalEvent() { return { ...terminal, sequence: 0 }; },
    async cancel() { return { requested: false, taskId }; },
  });
  await expect(async () => {
    for await (const _event of outOfOrder.events()) { /* consume */ }
  }).toThrow();
  const missingStreamEvent = new Task(taskId, {
    operationId: "operation:remote",
    async result() { return { kind: "succeeded", value: 7 } as const; },
    async *events() {
      yield { id: "event:later", taskId, sequence: 1, event: { kind: "started" } as const };
    },
    async terminalEvent() { return { ...terminal, sequence: 1 }; },
    async cancel() { return { requested: false, taskId }; },
  });
  await expect(async () => {
    for await (const _event of missingStreamEvent.events()) { /* consume */ }
  }).toThrow();
  const missingBeforeTerminal = new Task(taskId, {
    operationId: "operation:remote",
    async result() { return { kind: "succeeded", value: 7 } as const; },
    async *events() {
      yield { id: "event:started", taskId, sequence: 0, event: { kind: "started" } as const };
    },
    async terminalEvent() { return { ...terminal, sequence: 2 }; },
    async cancel() { return { requested: false, taskId }; },
  });
  await expect(async () => {
    for await (const _event of missingBeforeTerminal.events()) { /* consume */ }
  }).toThrow();
  const emptyReplay = new Task(taskId, {
    operationId: "operation:remote",
    async result() { return { kind: "succeeded", value: 7 } as const; },
    async *events() { yield* []; },
    async terminalEvent() { return { ...terminal, sequence: 2 }; },
    async cancel() { return { requested: false, taskId }; },
  });
  await expect(async () => {
    for await (const _event of emptyReplay.events()) { /* consume */ }
  }).toThrow();
  const resumedEmptyReplay = new Task(taskId, {
    operationId: "operation:remote",
    async result() { return { kind: "succeeded", value: 7 } as const; },
    async *events() { yield* []; },
    async terminalEvent() { return { ...terminal, sequence: 7 }; },
    async cancel() { return { requested: false, taskId }; },
  });
  await expect(async () => {
    for await (const _event of resumedEmptyReplay.events(5)) { /* consume */ }
  }).toThrow();
});

test("reconciles lost durable admission acknowledgement by stable operation ID", async () => {
  const definition = await TaskDefinition.resumable<number, number, number>("retryable", "1", {
    state: numberSchema,
    initial: input => input,
    async transition(_context, state) { return { kind: "finish", output: state }; },
  }, { implementationDigest: durableDigest, input: numberSchema, output: numberSchema });
  const observed = new Map<string, Task<number>>();
  const onceId = "eeeeeeee-eeee-4eee-8eee-eeeeeeeeeeee";
  const missingId = "ffffffff-ffff-4fff-8fff-ffffffffffff";
  const rejectedId = "12345678-1234-4234-8234-123456789abc";
  const invalidId = "12345678-1234-4234-8234-123456789abd";
  let retained: WasmTaskAdmissionWire | undefined;
  let loseAcknowledgement = true;
  let reject = false;
  const host: HarnessRuntimeSpawner & HarnessRuntimeState = {
    policyIdentity: () => null,
    async admitResumable(operationId: string, _definition, _input, _runtime, _parent, record) {
      retained ??= record;
      if (reject) return { kind: "rejected" as const, reason: { code: "unsupported" as const, message: "policy" } };
      let task = observed.get(operationId);
      if (!task) { task = new Task("task:once" as TaskId, async () => "wrong type" as unknown as number); observed.set(operationId, task); }
      if (loseAcknowledgement) { loseAcknowledgement = false; throw new AdmissionUncertainError(operationId, new Error("ack lost")); }
      return { kind: "accepted" as const, task };
    },
    async reconcileAdmission(operationId) {
      const task = observed.get(operationId);
      return task ? { task, operationId, taskName: "retryable", revision: "1",
        implementationDigest: durableDigest, admission: retained! } : null;
    },
    async attach(id) { return { task: observed.values().next().value as Task<unknown>, operationId: onceId, taskName: "retryable", revision: "1", implementationDigest: durableDigest, admission: retained! }; },
    async reconcileEffect() { return { state: "indeterminate" } as const; },
    async send(message: TaskMessage) { return { accepted: true, messageId: message.id }; },
    async *inbox() { yield* [] as TaskMessage[]; },
  }
  const runtime = await Harness.builder(contracts).state(host).spawner(host).task(definition).build();
  const first = await runtime.admit(definition, 1, onceId);
  expect(first).toEqual({ kind: "indeterminate", operationId: onceId });
  expect(await runtime.reconcileAdmission(missingId)).toBeNull();
  expect(await (await runtime.reconcileAdmission(onceId))?.result())
    .toMatchObject({ kind: "failed" });
  const second = await runtime.admit(definition, 1, onceId);
  expect(second.kind).toBe("accepted");
  if (second.kind !== "accepted") throw new Error("expected accepted retry");
  expect(observed.size).toBe(1);
  expect(await second.task.result()).toMatchObject({ kind: "failed" });
  const settled = [];
  for await (const event of second.task.events()) if (event.event.kind === "settled") settled.push(event.event.outcome);
  expect(settled).toMatchObject([{ kind: "failed" }]);
  const reconnected = await Harness.builder(contracts).state(host).spawner(host).task(definition).build();
  expect(await (await reconnected.attach(definition, "task:once" as TaskId)).result())
    .toMatchObject({ kind: "failed" });
  reject = true;
  expect(await runtime.admit(definition, 2, rejectedId)).toMatchObject({ kind: "rejected", reason: { code: "unsupported" } });
  reject = false;
  host.admitResumable = async () => { throw new TypeError("invalid admission request"); };
  await expect(runtime.admit(definition, 2, invalidId)).rejects.toThrow();
});

test("separate state and spawner bind without a combined host", async () => {
  const definition = await TaskDefinition.resumable<number, number, number>("split", "1", {
    state: numberSchema,
    initial: input => input,
    async transition(_context, value) { return { kind: "finish", output: value }; },
  }, { implementationDigest: durableDigest, input: numberSchema, output: numberSchema });
  const id = "task:split" as TaskId;
  const admissionId = "12345678-1234-4234-8234-123456789abe";
  let retained: WasmTaskAdmissionWire | undefined;
  let corruptAdmission = false;
  const operation = "03030303-0303-0303-0303-030303030303" as OperationId;
  const tool = await numberTool("split-tool", "split", () => { throw new Error("local tool must not run"); });
  const ownerTask = new Task(id, async () => 7);
  const untrustedTask = new Task(id, async () => 99);
  const state: HarnessRuntimeState = {
    policyIdentity: () => null,
    async attach(taskId) {
      if (taskId !== id) throw new Error("unknown task");
      const admission = corruptAdmission && retained ? { ...retained, input: 4 } : retained;
      return { task: ownerTask, operationId: admissionId, taskName: "split",
        revision: "1", implementationDigest: durableDigest,
        ...(admission === undefined ? {} : { admission }) };
    },
    async reconcileEffect() { return { state: "indeterminate" }; },
    async executeTool(taskId, effectId, _tool, input) {
      expect(taskId).toBe(id);
      expect(effectId).toBe(operation);
      return { kind: "succeeded", value: input };
    },
    async send(message) { return { accepted: true, messageId: message.id }; },
    async *inbox() { yield* []; },
  };
  const spawner: HarnessRuntimeSpawner = {
    policyIdentity: () => null,
    async admitResumable(_operationId, _definition, _input, _harness, _parentTaskId, admission) {
      retained = admission;
      return { kind: "accepted", task: untrustedTask };
    },
    async reconcileAdmission() {
      return { task: untrustedTask, operationId: admissionId,
        taskName: "split", revision: "1", implementationDigest: durableDigest,
        ...(retained === undefined ? {} : { admission: retained! }) };
    },
  };
  const runtime = await Harness.builder(contracts).state(state).spawner(spawner).task(definition)
    .tool(tool).grant("tool:call:split-tool").build();
  const admitted = await runtime.admit(definition, 3, admissionId);
  expect(admitted.kind).toBe("accepted");
  if (admitted.kind !== "accepted") throw new Error("split task was not admitted");
  expect(await admitted.task.result()).toEqual({ kind: "succeeded", value: 7 });
  expect(await (await runtime.reconcileAdmission(admissionId))?.result())
    .toEqual({ kind: "succeeded", value: 7 });
  expect(await new TaskContext(runtime, new AbortController().signal, id, true)
    .callDurable(operation, runtime.tool(tool), 3)).toEqual({ kind: "succeeded", value: 3 });
  corruptAdmission = true;
  await expect(runtime.admit(definition, 3, admissionId)).rejects.toThrow();
});

test("qualified execution is pinned and retries use the retained placement", async () => {
  const definition = await TaskDefinition.resumable<number, number, number>("placed", "1", {
    state: numberSchema, initial: input => input,
    async transition(_context, value) { return { kind: "finish", output: value }; },
  }, { implementationDigest: durableDigest, input: numberSchema, output: numberSchema });
  const localClosure = await TaskDefinition.live<number, number>("local-closure", "1", (_context, value) => value);
  const routeIdentity = await policyIdentity("test.execution", "1", Uint8Array.from({ length: 32 }, () => 7));
  const placement = {
    provider: routeIdentity,
    build: { kind: "artifact" as const,
      provider: { namespace: "test", family: "objects", version: "1" }, key: [1], version: null },
    environment: { kind: "sandbox" as const,
      provider: { namespace: "test", family: "machines", version: "1" }, key: [2], version: null },
    readiness_revision: Array.from({ length: 32 }, () => 3),
  };
  const operationId = "12345678-1234-4234-8234-123456789abc";
  const taskId = "task:placed" as TaskId;
  const ownerTask = new Task(taskId, async () => 7);
  let retained: WasmTaskAdmissionWire | undefined;
  let qualifications = 0;
  let corruptOwner = false;
  const attachment = () => retained === undefined
    ? { task: ownerTask, operationId, taskName: "placed", revision: "1", implementationDigest: durableDigest }
    : { task: ownerTask, operationId, taskName: "placed", revision: "1",
      implementationDigest: durableDigest, admission: retained! };
  const state: HarnessRuntimeState = {
    policyIdentity: () => null, executionIdentity: () => routeIdentity,
    async attach() { return corruptOwner && retained
      ? { ...attachment(), admission: { ...retained, execution: { ...placement, readiness_revision: Array.from({ length: 32 }, () => 4) } } }
      : attachment(); },
    async reconcileEffect() { return { state: "indeterminate" }; },
    async send(message) { return { accepted: true, messageId: message.id }; },
    async *inbox() { yield* []; },
  };
  const spawner: HarnessRuntimeSpawner = {
    policyIdentity: () => null, executionIdentity: () => routeIdentity,
    async admitResumable(_id, _definition, _input, _harness, _parent, admission) {
      retained = admission;
      return { kind: "accepted", task: ownerTask };
    },
    async reconcileAdmission() { return retained ? attachment() : null; },
  };
  const execution: HarnessExecutionProvider = {
    identity: () => routeIdentity, spawner: () => spawner, state: () => state,
    async qualifyTask() { qualifications++; return placement; },
    async qualifyBatch() { throw new Error("batch route is not registered"); },
  };
  const runtime = await Harness.builder(contracts).execution(execution).task(definition).task(localClosure).build();
  const unbound = await Harness.builder(contracts).task(definition).build();
  expect(() => unbound.admissionRecord(operationId, definition, 3, undefined, placement))
    .toThrow();
  const otherIdentity = await policyIdentity("other.execution", "1", Uint8Array.from({ length: 32 }, () => 8));
  expect(() => runtime.admissionRecord(operationId, definition, 3, undefined,
    { ...placement, provider: otherIdentity }))
    .toThrow();
  expect(() => runtime.spawn(localClosure, 3)).toThrow();
  expect((await runtime.admit(definition, 3, operationId)).kind).toBe("accepted");
  // The full task-admission envelope owns the canonical representation of
  // nested resource keys and byte vectors.
  expect(retained?.execution).toEqual(contracts.validate("task_admission", retained!).execution);
  expect(retained).toEqual(contracts.validate("task_admission", retained!));
  expect(retained?.operation_id).toBe(operationId);
  expect(retained?.run_limits).toEqual({ concurrency: null, max_steps: null, deadline_epoch_ms: null });
  expect(qualifications).toBe(1);
  expect((await runtime.admit(definition, 3, operationId)).kind).toBe("accepted");
  expect(qualifications).toBe(1);
  corruptOwner = true;
  await expect(runtime.admit(definition, 3, operationId)).rejects.toThrow();
});

test("preserves unresolved batch admission and stable scoped entry identities", async () => {
  const definition = await TaskDefinition.resumable<number, number, number>("batch-work", "2", {
    state: numberSchema,
    initial: input => input,
    async transition(_context, state) { return { kind: "finish", output: state }; },
  }, { implementationDigest: durableDigest, input: numberSchema, output: numberSchema });
  const ids: string[] = [];
  let admittedManifests = 0;
  let retainedBatch: import("../src/index.js").BatchAdmissionRequest | undefined;
  const host: HarnessRuntimeSpawner & HarnessRuntimeState = {
    policyIdentity: () => null,
    async admitBatch(request) {
      admittedManifests++;
      retainedBatch = request;
      expect(request.canonical.contract).toBe("harness.batch.v2");
      expect(request.canonical.group_policy).toBe("collect-all");
      expect(request.canonical.parent).toBeNull();
      expect(request.canonical.inputs).toEqual([1n, 2n]);
      expect(request.inputDigest).toEqual(Array.from(contracts.digestCanonicalJson(request.canonical)));
      expect(Object.isFrozen(request.inputDigest)).toBeTrue();
      ids.push(...request.members.map(member => member.operation_id));
      return { taskName: request.taskName, revision: request.revision,
        implementationDigest: request.implementationDigest, inputDigest: request.inputDigest,
        entries: request.members.map((member, index) => ({ key: { batchId: request.batchId, index },
          admission: { kind: "indeterminate" as const, operationId: member.operation_id } })) };
    },
    async reconcileBatch(request) { return { taskName: "batch-work", revision: "2", implementationDigest: durableDigest,
      inputDigest: request.inputDigest, entries: request.members.map((member, index) => ({ key: { batchId: request.batchId, index },
        admission: { kind: "indeterminate" as const, operationId: member.operation_id } })) }; },
    async loadBatch(batchId) { return retainedBatch?.batchId === batchId ? retainedBatch.canonical : null; },
    async cancelBatch(batchId) {
      if (retainedBatch?.batchId !== batchId) return null;
      return { groupId: retainedBatch.groupId, batchId,
        entries: retainedBatch.members.map((member, index) => ({
          key: { batchId, index },
          status: { kind: "indeterminate" as const, operationId: member.operation_id },
        })) };
    },
    async attach(id) { return { task: new Task(id, async () => undefined), operationId: "operation:batch", taskName: "batch-work", revision: "2", implementationDigest: durableDigest }; },
    async reconcileEffect() { return { state: "indeterminate" } as const; },
    async send(message: TaskMessage) { return { accepted: true, messageId: message.id }; },
    async *inbox() { yield* [] as TaskMessage[]; },
  }
  const groupId = "11111111-1111-4111-8111-111111111111" as GroupId;
  const batch = { id: "22222222-2222-4222-8222-222222222222" as BatchId, inputs: [1, 2] };
  const group = (await Harness.builder(contracts).state(host).spawner(host).task(definition).build()).group<number>({ kind: "collect-all" }, groupId);
  expect((await group.spawnMany(definition, batch)).map(entry => entry.admission.kind)).toEqual(["indeterminate", "indeterminate"]);
  expect((await group.spawnMany(definition, batch)).map(entry => entry.admission.kind)).toEqual(["indeterminate", "indeterminate"]);
  expect(admittedManifests).toBe(1);
  expect(ids).toEqual([0, 1].map(index => contracts.batchMemberOperationId(groupId, batch.id, index)));
  const completed = [];
  for await (const entry of group.asCompleted()) completed.push(entry);
  expect(completed).toHaveLength(2);
  expect(completed.map(entry => entry.admission.kind)).toEqual(["indeterminate", "indeterminate"]);
  expect((await group.join()).complete).toBeFalse();
  const reconstructed = (await Harness.builder(contracts).state(host).spawner(host).task(definition).build()).group<number>({ kind: "collect-all" }, groupId);
  expect((await reconstructed.reconcileBatchId(definition, batch.id))?.map(entry => entry.admission.kind)).toEqual(["indeterminate", "indeterminate"]);
  expect(await reconstructed.reconcileBatchId(definition, "33333333-3333-4333-8333-333333333333" as BatchId)).toBeNull();
  expect((await reconstructed.join()).entries).toHaveLength(2);
  const cancellation = await reconstructed.cancelBatchId(definition, batch.id);
  expect(cancellation?.entries.map(entry => entry.status.kind)).toEqual(["indeterminate", "indeterminate"]);
});

test("replays a retained batch without requalifying its immutable placement", async () => {
  let parsedInputs = 0;
  const countingInput = await defineRuntimeSchema("counting-input", { type: "number" }, (value: unknown): number => {
    parsedInputs++;
    if (typeof value !== "number") throw new TypeError("invalid input");
    return value;
  });
  const definition = await TaskDefinition.resumable<number, number, number>("retained-placement", "1", {
    state: numberSchema, initial: input => input,
    async transition(_context, state) { return { kind: "finish", output: state }; },
  }, { implementationDigest: durableDigest, input: countingInput, output: numberSchema });
  const routeIdentity = await policyIdentity("test.batch.execution", "1", Uint8Array.from({ length: 32 }, () => 8));
  const placement = {
    provider: routeIdentity,
    build: { kind: "artifact" as const,
      provider: { namespace: "test", family: "objects", version: "1" }, key: [1], version: null },
    environment: { kind: "sandbox" as const,
      provider: { namespace: "test", family: "machines", version: "1" }, key: [2], version: null },
    readiness_revision: Array.from({ length: 32 }, () => 3),
  };
  let retained: import("../src/index.js").BatchAdmissionRequest | undefined;
  let qualifications = 0;
  let unavailable = false;
  const replay = (request: import("../src/index.js").BatchAdmissionRequest): HostBatchReplay => ({
    taskName: request.taskName, revision: request.revision,
    implementationDigest: request.implementationDigest, inputDigest: request.inputDigest,
    entries: request.members.map((member, index) => ({ key: { batchId: request.batchId, index },
      admission: { kind: "indeterminate" as const, operationId: member.operation_id } })),
  });
  const spawner: HarnessRuntimeSpawner = {
    policyIdentity: () => null,
    executionIdentity: () => routeIdentity,
    async admitBatch(request) { retained = request; return replay(request); },
    async loadBatch(batchId) { return retained?.batchId === batchId ? retained.canonical : null; },
    async reconcileBatch(request) { return replay(request); },
  };
  const state: HarnessRuntimeState = {
    policyIdentity: () => null,
    executionIdentity: () => routeIdentity,
    async attach() { throw new Error("no accepted member"); },
    async reconcileEffect() { return { state: "indeterminate" }; },
    async send(message) { return { accepted: true, messageId: message.id }; },
    async *inbox() { yield* []; },
  };
  const execution: HarnessExecutionProvider = {
    identity: () => routeIdentity, spawner: () => spawner, state: () => state,
    async qualifyTask() { return placement; },
    async qualifyBatch() {
      qualifications++;
      if (unavailable) throw new Error("execution provider unavailable");
      return placement;
    },
  };
  const batchId = "44444444-4444-4444-8444-444444444444" as BatchId;
  const groupId = "55555555-5555-4555-8555-555555555555" as GroupId;
  const batch = { id: batchId, inputs: [1, 2] };
  const first = (await Harness.builder(contracts).execution(execution).task(definition).build())
    .group<number>({ kind: "collect-all" }, groupId);
  expect((await first.spawnMany(definition, batch)).map(entry => entry.admission.kind))
    .toEqual(["indeterminate", "indeterminate"]);
  expect(parsedInputs).toBe(2);
  expect(qualifications).toBe(1);
  unavailable = true;
  const reopened = (await Harness.builder(contracts).execution(execution).task(definition).build())
    .group<number>({ kind: "collect-all" }, groupId);
  expect((await reopened.spawnMany(definition, batch)).map(entry => entry.admission.kind))
    .toEqual(["indeterminate", "indeterminate"]);
  expect(parsedInputs).toBe(4);
  expect(qualifications).toBe(1);
});

test("asks the durable batch owner to retain cancellation after a failed member", async () => {
  const definition = await TaskDefinition.resumable<number, number, number>("cancel-batch", "1", {
    state: numberSchema, initial: input => input,
    async transition(_context, state) { return { kind: "finish", output: state }; },
  }, { implementationDigest: durableDigest, input: numberSchema, output: numberSchema });
  let retained: import("../src/index.js").BatchAdmissionRequest | undefined;
  let replay: HostBatchReplay | undefined;
  let declarations = 0;
  let requested = 0;
  const tasks = new Map<string, Task<number>>();
  const host: HarnessRuntimeSpawner & HarnessRuntimeState = {
    policyIdentity: () => null,
    async admitBatch(request) {
      retained = request;
      expect(request.canonical.group_policy).toBe("cancel-on-failure");
      const [first, second] = request.members;
      if (!first || !second) throw new Error("expected two batch members");
      tasks.set(first.operation_id, new Task<number>(first.operation_id as TaskId,
        async () => { throw new Error("first member failed"); }));
      tasks.set(second.operation_id, new Task<number>(second.operation_id as TaskId,
        async signal => new Promise<number>((_resolve, reject) => {
          signal.addEventListener("abort", () => { requested++; reject(new Error("owner cancelled sibling")); }, { once: true });
        })));
      replay = { taskName: request.taskName, revision: request.revision,
        implementationDigest: request.implementationDigest, inputDigest: request.inputDigest,
        entries: request.members.map((member, index) => ({ key: { batchId: request.batchId, index },
          admission: { kind: "accepted" as const, task: tasks.get(member.operation_id)! } })) };
      return replay;
    },
    async reconcileBatch() { if (!replay) throw new Error("batch was not retained"); return replay; },
    async loadBatch(batchId) { return retained?.batchId === batchId ? retained.canonical : null; },
    async cancelBatch(batchId) {
      if (retained?.batchId !== batchId) return null;
      declarations++;
      await tasks.get(retained.members[1]!.operation_id)!.cancel();
      return { groupId: retained.groupId, batchId,
        entries: retained.members.map((_member, index) => ({ key: { batchId, index },
          status: { kind: "requested" as const } })) };
    },
    async attach(id) {
      const member = retained?.members.find(candidate => candidate.operation_id === id);
      const task = tasks.get(id);
      if (!member || !task) throw new Error("unknown batch member");
      return { task, operationId: member.operation_id, taskName: definition.name,
        revision: definition.revision, implementationDigest: durableDigest, admission: member };
    },
    async reconcileEffect() { return { state: "indeterminate" } as const; },
    async send(message: TaskMessage) { return { accepted: true, messageId: message.id }; },
    async *inbox() { yield* [] as TaskMessage[]; },
  }
  const groupId = "11111111-1111-4111-8111-111111111112" as GroupId;
  const batchId = "22222222-2222-4222-8222-222222222223" as BatchId;
  const group = (await Harness.builder(contracts).state(host).spawner(host).task(definition).build())
    .group<number>({ kind: "cancel-on-failure" }, groupId);
  const admitted = await group.spawnMany(definition, { id: batchId, inputs: [1, 2] });
  expect(admitted.map(entry => entry.admission.kind)).toEqual(["accepted", "accepted"]);
  const completed = await group.join();
  expect(completed.entries.map(entry => entry.outcome?.kind)).toEqual(["failed", "cancelled"]);
  expect(completed.cancellation).toBe("requested");
  expect(completed.complete).toBeTrue();
  expect(declarations).toBe(1);
  expect(requested).toBe(1);
});

test("keeps every batch slot reconcilable after a lost manifest acknowledgement", async () => {
  const definition = await TaskDefinition.resumable<number, number, number>("batch-lost-ack", "1", {
    state: numberSchema, initial: input => input,
    async transition(_context, state) { return { kind: "finish", output: state }; },
  }, { implementationDigest: durableDigest, input: numberSchema, output: numberSchema });
  let retained: import("../src/index.js").BatchAdmissionRequest | undefined;
  const host: HarnessRuntimeSpawner & HarnessRuntimeState = {
    policyIdentity: () => null,
    async admitBatch(request) { retained = request; throw new AdmissionUncertainError(request.batchId); },
    async loadBatch(batchId) { return retained?.batchId === batchId ? retained.canonical : null; },
    async reconcileBatch(request) {
      if (retained === undefined) throw new Error("batch admission was not retained");
      expect(request.inputDigest).toEqual(retained.inputDigest);
      return { taskName: request.taskName, revision: request.revision,
        implementationDigest: request.implementationDigest, inputDigest: request.inputDigest,
        entries: request.members.map((member, index) => ({ key: { batchId: request.batchId, index },
          admission: { kind: "indeterminate" as const, operationId: member.operation_id } })) };
    },
    async attach() { throw new Error("no member accepted"); },
    async reconcileEffect() { return { state: "indeterminate" }; },
    async send(message) { return { accepted: true, messageId: message.id }; },
    async *inbox() { yield* []; },
  }
  const batch = { id: "33333333-3333-4333-8333-333333333333" as BatchId, inputs: [1, 2, 3] };
  const group = (await Harness.builder(contracts).state(host).spawner(host).task(definition).build())
    .group<number>({ kind: "collect-all" }, "44444444-4444-4444-8444-444444444444" as GroupId);
  expect((await group.spawnMany(definition, batch)).map(entry => entry.admission.kind))
    .toEqual(["indeterminate", "indeterminate", "indeterminate"]);
  expect(retained?.members).toHaveLength(3);
  expect((await group.reconcileBatch(definition, batch)).map(entry => entry.admission.kind))
    .toEqual(["indeterminate", "indeterminate", "indeterminate"]);
});

test("rejects incomplete and unrelated host batch replay without retaining it", async () => {
  const definition = await TaskDefinition.resumable<number, number, number>("replay", "1", {
    state: numberSchema, initial: input => input,
    async transition(_context, state) { return { kind: "finish", output: state }; },
  }, { implementationDigest: durableDigest, input: numberSchema, output: numberSchema });
  const batch = { id: "55555555-5555-4555-8555-555555555555" as BatchId, inputs: [1, 2] };
  let entries: HostBatchReplay["entries"] = [];
  let retained: import("../src/index.js").BatchAdmissionRequest | undefined;
  let wrongDigest = false;
  const host: HarnessRuntimeSpawner & HarnessRuntimeState = {
    policyIdentity: () => null,
    async admitBatch(request) {
      retained = request;
      return { taskName: request.taskName, revision: request.revision,
        implementationDigest: request.implementationDigest, inputDigest: request.inputDigest,
        entries: request.members.map((member, index) => ({ key: { batchId: request.batchId, index },
          admission: { kind: "indeterminate" as const, operationId: member.operation_id } })) };
    },
    async loadBatch(batchId) { return retained?.batchId === batchId ? retained.canonical : null; },
    async reconcileBatch(request) { return { taskName: "replay", revision: "1", implementationDigest: durableDigest,
      inputDigest: wrongDigest ? Array(request.inputDigest.length).fill(0) as number[] : request.inputDigest, entries }; },
    async attach() { throw new Error("unused"); },
    async reconcileEffect() { return { state: "indeterminate" }; },
    async send(message) { return { accepted: true, messageId: message.id }; },
    async *inbox() { yield* []; },
  }
  const group = (await Harness.builder(contracts).state(host).spawner(host).task(definition).build())
    .group<number>({ kind: "collect-all" }, "66666666-6666-4666-8666-666666666666" as GroupId);
  await group.spawnMany(definition, batch);
  const entry = (index: number, operationId: string = contracts.batchMemberOperationId(group.id, batch.id, index)) =>
    ({ key: { batchId: batch.id, index }, admission: { kind: "indeterminate" as const, operationId } });
  entries = [entry(0)];
  await expect(group.reconcileBatch(definition, batch)).rejects.toThrow();
  entries = [entry(0), entry(1), entry(2)];
  await expect(group.reconcileBatch(definition, batch)).rejects.toThrow();
  entries = [entry(0), entry(2)];
  await expect(group.reconcileBatch(definition, batch)).rejects.toThrow();
  entries = [entry(0), entry(0)];
  await expect(group.reconcileBatch(definition, batch)).rejects.toThrow();
  entries = [entry(0), entry(1, "wrong-operation")];
  await expect(group.reconcileBatch(definition, batch)).rejects.toThrow();
  entries = [entry(1), entry(0)];
  wrongDigest = true;
  await expect(group.reconcileBatch(definition, batch)).rejects.toThrow();
  wrongDigest = false;
  entries = [{ key: { batchId: batch.id, index: 0 },
    admission: { kind: "rejected", reason: { code: "unsupported", message: "denied" } },
    outcome: { kind: "succeeded", value: 1 } }, entry(1)];
  await expect(group.reconcileBatch(definition, batch)).rejects.toThrow();
  entries = [entry(1), entry(0)];
  expect((await group.reconcileBatch(definition, batch)).map(value => value.key.index)).toEqual([0, 1]);
  await expect(group.reconcileBatch(definition, { id: batch.id, inputs: [1, 3] }))
    .rejects.toThrow();
  expect((await group.join()).entries).toHaveLength(2);
});

test("non-canonical durable batch inputs reject entries before host admission", async () => {
  const definition = await TaskDefinition.resumable<number, number, number>("canonical-input", "1", {
    state: numberSchema, initial: input => input,
    async transition(_context, state) { return { kind: "finish", output: state }; },
  }, { implementationDigest: durableDigest, input: numberSchema, output: numberSchema });
  let admitted = 0;
  const host: HarnessRuntimeSpawner & HarnessRuntimeState = {
    policyIdentity: () => null,
    async admitBatch() { admitted++; throw new Error("must not admit"); },
    async attach() { throw new Error("unused"); },
    async reconcileEffect() { return { state: "indeterminate" }; },
    async send(message) { return { accepted: true, messageId: message.id }; },
    async *inbox() { yield* []; },
  }
  const group = (await Harness.builder(contracts).state(host).spawner(host).task(definition).build())
    .group<number>({ kind: "collect-all" }, "77777777-7777-4777-8777-777777777777" as GroupId);
  const inputs = [1, undefined] as unknown as number[];
  const entries = await group.spawnMany(definition, { id: "88888888-8888-4888-8888-888888888888" as BatchId, inputs: inputs });
  expect(entries.map(entry => entry.admission.kind)).toEqual(["rejected", "rejected"]);
  expect(entries.every(entry => entry.admission.kind === "rejected"
    && entry.admission.reason.code === "invalid_input")).toBeTrue();
  expect(admitted).toBe(0);
});

test("rejects host policy drift during attachment and batch reconciliation", async () => {
  const definition = await TaskDefinition.resumable<number, number, number>("drift-replay", "1", {
    state: numberSchema, initial: input => input,
    async transition(_context, state) { return { kind: "finish", output: state }; },
  }, { implementationDigest: durableDigest, input: numberSchema, output: numberSchema });
  let currentIdentity: PolicyIdentity | null = null;
  let retained: import("../src/index.js").BatchAdmissionRequest | undefined;
  const host: HarnessRuntimeSpawner & HarnessRuntimeState = {
    policyIdentity: () => currentIdentity,
    async admitBatch(request) {
      retained = request;
      return { taskName: request.taskName, revision: request.revision,
        implementationDigest: request.implementationDigest, inputDigest: request.inputDigest,
        entries: request.members.map((member, index) => ({ key: { batchId: request.batchId, index },
          admission: { kind: "indeterminate" as const, operationId: member.operation_id } })) };
    },
    async loadBatch(batchId) { return retained?.batchId === batchId ? retained.canonical : null; },
    async attach(id) {
      currentIdentity = approvalPolicyIdentity;
      return { task: new Task(id, async () => 1), operationId: "drift-op", taskName: "drift-replay",
        revision: "1", implementationDigest: durableDigest };
    },
    async reconcileBatch(request) {
      currentIdentity = approvalPolicyIdentity;
      return { taskName: "drift-replay", revision: "1", implementationDigest: durableDigest,
        inputDigest: request.inputDigest,
        entries: [{ key: { batchId: request.batchId, index: 0 }, admission: { kind: "indeterminate" as const,
          operationId: request.members[0]!.operation_id } }] };
    },
    async reconcileEffect() { return { state: "indeterminate" }; },
    async send(message) { return { accepted: true, messageId: message.id }; },
    async *inbox() { yield* []; },
  }
  const runtime = await Harness.builder(contracts).state(host).spawner(host).task(definition).build();
  await expect(runtime.attach(definition, "task:drift" as TaskId)).rejects.toThrow();
  currentIdentity = null;
  const group = runtime.group<number>({ kind: "collect-all" }, "99999999-9999-4999-8999-999999999999" as GroupId);
  const batch = { id: "aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa" as BatchId, inputs: [1] };
  await group.spawnMany(definition, batch);
  await expect(group.reconcileBatch(definition, batch))
    .rejects.toThrow();
  expect((await group.join()).entries).toHaveLength(1);
});

test("rejects the entire immutable batch before publication when an input is invalid", async () => {
  const definition = await TaskDefinition.resumable<number, number, number>("validate-input", "1", {
    state: numberSchema,
    initial: input => input,
    async transition(_context, state) { return { kind: "finish", output: state }; },
  }, { implementationDigest: durableDigest, input: await defineRuntimeSchema("positive", { type: "number", exclusiveMinimum: 0 }, value => {
    if (typeof value !== "number" || value <= 0) throw new TypeError("positive number required");
    return value;
  }), output: numberSchema });
  const admitted: string[] = [];
  const host: HarnessRuntimeSpawner & HarnessRuntimeState = {
    policyIdentity: () => null,
    async admitBatch(request) { admitted.push(...request.members.map(member => member.operation_id)); throw new Error("must not admit"); },
    async attach(id) { return { task: new Task(id, async () => undefined), operationId: "operation:partial", taskName: "validate-input", revision: "1", implementationDigest: durableDigest }; },
    async reconcileEffect() { return { state: "indeterminate" } as const; },
    async send(message: TaskMessage) { return { accepted: true, messageId: message.id }; },
    async *inbox() { yield* [] as TaskMessage[]; },
  }
  const group = (await Harness.builder(contracts).state(host).spawner(host).task(definition).build()).group<number>({ kind: "collect-all" },
    "bbbbbbbb-bbbb-4bbb-8bbb-bbbbbbbbbbbb" as GroupId);
  const entries = await group.spawnMany(definition, { id: "cccccccc-cccc-4ccc-8ccc-cccccccccccc" as BatchId, inputs: [1, -1, 2] });
  expect(entries.map(entry => entry.admission.kind)).toEqual(["rejected", "rejected", "rejected"]);
  expect(admitted).toHaveLength(0);
  expect((await group.join()).entries).toHaveLength(0);
});

test("durable tool calls preserve explicit unresolved and terminal outcomes", async () => {
  const operation = "01010101-0101-0101-0101-010101010101" as OperationId;
  const task = "task:durable-tool" as TaskId;
  let localExecutions = 0;
  const tool = await numberTool("durable-count", "count", () => { localExecutions++; return 99; });
  const observed: unknown[] = [];
  const outcomes: Outcome<unknown, OperationId>[] = [
    { kind: "indeterminate", operationId: operation },
    { kind: "failed", error: { message: "executor rejected" } },
    { kind: "succeeded", value: 7 },
  ];
  const host: HarnessRuntimeSpawner & HarnessRuntimeState = {
    policyIdentity: () => null,
    async attach() { throw new Error("not used"); },
    async reconcileEffect() { return { state: "indeterminate" }; },
    async send(message) { return { accepted: true, messageId: message.id }; },
    async *inbox() { yield* []; },
    async executeTool(taskId, operationId, definition, input) {
      observed.push({ taskId, operationId, name: definition.definition.name, input });
      return outcomes.shift() ?? { kind: "cancelled", receipt: { requested: true, taskId } };
    },
  }
  const runtime = await Harness.builder(contracts).state(host).spawner(host).tool(tool).grant("tool:call:durable-count").build();
  const context = new TaskContext(runtime, new AbortController().signal, task, true);
  expect(() => context.call(runtime.tool(tool), 2)).toThrow();
  expect(await context.callDurable(operation, runtime.tool(tool), 2)).toEqual({ kind: "indeterminate", operationId: operation });
  expect(await context.callDurable(operation, runtime.tool(tool), 2)).toEqual({ kind: "failed", error: { message: "executor rejected" } });
  expect(await context.callDurable(operation, runtime.tool(tool), 2)).toEqual({ kind: "succeeded", value: 7 });
  expect(await context.callDurable(operation, runtime.tool(tool), 2)).toEqual({ kind: "cancelled", receipt: { requested: true, taskId: task } });
  expect(localExecutions).toBe(0);
  expect(observed).toHaveLength(4);
});

test("durable approval is routed only through the owner host and preserves its outcome", async () => {
  let dispatched = 0;
  const tool = await numberTool("guarded-durable", "guarded", () => 1);
  const host: HarnessRuntimeSpawner & HarnessRuntimeState = {
    policyIdentity: () => approvalPolicyIdentity,
    async attach() { throw new Error("not used"); },
    async reconcileEffect() { return { state: "indeterminate" }; },
    async send(message) { return { accepted: true, messageId: message.id }; },
    async *inbox() { yield* []; },
    async executeTool(_taskId, operationId) {
      dispatched++;
      return { kind: "indeterminate", operationId };
    },
  }
  const runtime = await Harness.builder(contracts).state(host).spawner(host).tool(tool)
    .policy({ identity: () => approvalPolicyIdentity,
      evaluate: async () => ({ kind: "require-approval", prompt: "approve exact tool call" }) })
    .interactions({ route: async () => { throw new Error("local approval must not be used"); } })
    .grant("tool:call:guarded-durable").build();
  const context = new TaskContext(runtime, new AbortController().signal, "task:approval" as TaskId, true);
  const operation = "02020202-0202-0202-0202-020202020202" as OperationId;
  const outcome = await context.callDurable(operation,
    runtime.tool(tool), 1);
  expect(outcome).toEqual({ kind: "indeterminate", operationId: operation });
  expect(dispatched).toBe(1);
});

test("routes recovery, effect reconciliation, and typed messaging through a durable host", async () => {
  const messages: TaskMessage[] = [];
  const recoveredId = "task:recovered" as TaskId;
  const payload = await mailboxFile(10);
  const host: HarnessRuntimeSpawner & HarnessRuntimeState = {
    policyIdentity: () => null,
    async attach(id) { return { task: new Task(id, async () => 99), operationId: "operation:recovered", taskName: "hosted", revision: "1", implementationDigest: durableDigest }; },
    async reconcileEffect() { return { state: "succeeded", result: payload } as const; },
    async send(message: TaskMessage) { messages.push(message); return { accepted: true, messageId: message.id }; },
    async *inbox() { for (const message of messages) yield message; },
  }
  const definition = await TaskDefinition.live<void, number>("hosted", "1", async context => {
    const effect = await context.reconcileEffect("effect:one" as EffectId);
    if (effect.state !== "succeeded") return -1;
    await context.send(recoveredId, payload, "message:one" as OperationId);
    for await (const message of context.inbox()) return message.value.descriptor.byte_length;
    return -1;
  });
  const runtime = await Harness.builder(contracts).state(host).spawner(host).task(definition).build();
  expect(await runtime.spawn(definition, undefined).result()).toEqual({ kind: "succeeded", value: 10 });
  expect(messages[0]?.value).toEqual(payload);
  expect(() => new TaskContext(runtime, new AbortController().signal).send(recoveredId, { body: "inline secret" } as unknown as FileRef)).toThrow();
  expect(await (await runtime.attach([...runtime.running.keys()][0]!)).result()).toEqual({ kind: "succeeded", value: 10 });
});
