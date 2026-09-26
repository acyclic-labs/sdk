import { describe, expect, test } from "bun:test";
import {
  Batch,
  AgentHarness,
  AdmissionUncertainError,
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
  type HarnessExecutionProvider,
  type HostBatchReplay,
  type HarnessRuntimeSpawner,
  type HarnessRuntimeState,
  type TaskAdmissionRecord,
  type TaskAdmissionWire,
  type WorkflowAdmissionWire,
  type MessageId,
  type Outcome,
  type OperationId,
  type RuntimeTaskId,
  type TaskMessage,
  type ModelMessage,
  type ModelToolDefinition,
  type FileRef,
  type VolumeRef,
  type ConversationMessageId,
} from "../src/index.js";

const contracts = await NativeContracts.create();

test("Rust and TypeScript share strict v2 task admission and execution placement fixtures", async () => {
  const admissionText = (await Bun.file(new URL("../../../../fixtures/harness/v2/task-admission.json", import.meta.url)).text()).trim();
  const admission = JSON.parse(admissionText) as TaskAdmissionWire;
  expect(new TextDecoder().decode(contracts.encodeCanonicalJson(contracts.validate("task_admission", admission))))
    .toBe(admissionText);
  expect(() => contracts.validate("task_admission", { ...admission, input: "wrong" })).toThrow();
  const placementText = (await Bun.file(new URL("../../../../fixtures/harness/v2/execution-placement.json", import.meta.url)).text()).trim();
  const placement = JSON.parse(placementText) as import("../src/index.js").ExecutionPlacementWire;
  expect(contracts.validate("execution_placement", placement)).toEqual(placement);
  expect(() => contracts.validate("execution_placement", { ...placement, readiness_revision: [] })).toThrow();
});

test("Rust and TypeScript share the pinned v2 workflow admission fixture", async () => {
  const fixture = (await Bun.file(new URL("../../../../fixtures/harness/v2/workflow-admission.json", import.meta.url)).text()).trim();
  const admission = JSON.parse(fixture) as WorkflowAdmissionWire;
  expect(new TextDecoder().decode(contracts.encodeCanonicalJson(contracts.validate("workflow_admission", admission))))
    .toBe(fixture);
  expect(() => contracts.validate("workflow_admission", { ...admission, request_digest: Array(32).fill(0) })).toThrow();
});

test("native task and workflow admissions preserve Rust u64 values as BigInt", async () => {
  const taskText = (await Bun.file(new URL("../../../../fixtures/harness/v2/task-admission.json", import.meta.url)).text()).trim();
  const task = contracts.validate("task_admission", {
    ...JSON.parse(taskText), run_limits: { concurrency: 2, max_steps: 3, deadline_epoch_ms: 4 },
  });
  expect(typeof task.limits.file_bytes).toBe("bigint");
  expect(task.run_limits.max_steps).toBe(3n);
  const workflowText = (await Bun.file(new URL("../../../../fixtures/harness/v2/workflow-admission.json", import.meta.url)).text()).trim();
  const workflow = contracts.validate("workflow_admission", JSON.parse(workflowText));
  expect(workflow.initial.revision).toBe(0n);
});
const testModel = { provider: "fixture", name: "fixture", revision: "1", options: {} } as const;
const fixtureMessageId = (value: string): ConversationMessageId => value as ConversationMessageId;

const durableDigest = "ab".repeat(32);
const approvalPolicyIdentity = policyIdentity("approval-policy", "1", new Uint8Array(32).fill(1));
const denyPolicyIdentity = policyIdentity("deny-policy", "1", new Uint8Array(32).fill(2));
const scopedPolicyIdentity = policyIdentity("scoped-policy", "1", new Uint8Array(32).fill(3));
const numberSchema = defineRuntimeSchema("number", { type: "number" }, (value: unknown): number => {
  if (typeof value !== "number") throw new TypeError("invalid result");
  return value;
});

test("fork capture binding requires parent publication authority", () => {
  const preparer = { parentSnapshot: () => ({ parent: { kind: "conversation" as const, id: "parent" }, revision: 1n }),
    async prepare() { throw new Error("unused"); }, async reconcile() { return null; } };
  expect(() => Harness.builder(contracts).forkPreparer(preparer).build()).toThrow("fork:publish");
  const bound = Harness.builder(contracts).forkPreparer(preparer).grant("fork:publish").build();
  expect(bound).toBeInstanceOf(AgentHarness);
  expect(() => bound.scoped(ExecutionScope.create().onlyGrants("fork:publish")).atParentSnapshot(preparer)).toThrow("not bound");
  expect(() => bound.atParentSnapshot(preparer)).toThrow("advance");
  expect(() => bound.atParentSnapshot({ ...preparer,
    parentSnapshot: () => ({ parent: { kind: "conversation" as const, id: "other" }, revision: 2n }) })).toThrow("another parent");
  expect(bound.atParentSnapshot({ ...preparer,
    parentSnapshot: () => ({ parent: { kind: "conversation" as const, id: "parent" }, revision: 2n }) })).toBeInstanceOf(AgentHarness);
});

test("grouped provider bindings use the same authority and limit admission", () => {
  const preparer = { parentSnapshot: () => ({ parent: { kind: "conversation" as const, id: "parent" }, revision: 1n }),
    async prepare() { throw new Error("unused"); }, async reconcile() { return null; } };
  expect(() => Harness.builder(contracts).bindings({ forkPreparer: preparer }).build()).toThrow("fork:publish");
  const runtime = Harness.builder(contracts).bindings({
    forkPreparer: preparer, grants: ["fork:publish"], limits: { attachments: 1 },
  }).build();
  expect(runtime.scope.grants).toContain("fork:publish");
  expect(runtime.components.limits?.attachments).toBe(1);
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

test("resumable tools are pinned at registration and execute only through durable owner state", async () => {
  const definition = { name: "resumable-file-tool", revision: "1", description: "read a pinned file",
    inputSchema: { type: "number" }, outputSchema: { type: "number" },
    parseInput: parseNumber, parseOutput: parseNumber };
  const machine = { name: definition.name, version: definition.revision, digest: Array(32).fill(7) as number[] };
  expect(() => Harness.builder(contracts).resumableTool(definition, machine).build()).toThrow("owner-host");
  expect(() => Harness.builder(contracts).resumableTool(definition, { ...machine, digest: Array(32).fill(0) })).toThrow();
  const operation = "12345678-1234-4234-8234-123456789abc" as OperationId;
  const taskId = "22345678-1234-4234-8234-123456789abc" as RuntimeTaskId;
  const state: HarnessRuntimeState = {
    policyIdentity: () => null,
    async attach() { throw new Error("not used"); },
    async reconcileEffect() { return { state: "indeterminate" }; },
    async executeTool(observedTask, observedOperation, tool, input) {
      expect(observedTask).toBe(taskId);
      expect(observedOperation).toBe(operation);
      expect(tool.machine).toEqual(machine);
      return { kind: "succeeded", value: input };
    },
    async send(message) { return { accepted: true, messageId: message.id }; },
    async *inbox() { yield* []; },
  };
  const runtime = Harness.builder(contracts).state(state).resumableTool(definition, machine)
    .grant("tool:call:resumable-file-tool").build();
  const tool = runtime.tool(definition);
  await expect(runtime.call(tool, 2)).rejects.toThrow("requires callDurable");
  expect(await runtime.callDurable(operation, tool, 2, taskId)).toEqual({ kind: "succeeded", value: 2 });
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
  const attached = { ...file, volume: { ...file.volume,
    class: "agent_private" as const,
    owner: { kind: "agent", id: "03030303-0303-0303-0303-030303030303" as AgentId } } } as FileRef;
  let granted = true;
  const lazy = composeContentBindings(contracts, [{ volume: file.volume, content: first }], {
    mount(volume) {
      if (!granted || volume.owner.kind !== "agent" || volume.owner.id !== attached.volume.owner.id) {
        throw new Error("owner read grant is unavailable");
      }
      return { ...binding(3), requireDirectoryPath() { throw new Error("directory was not granted"); }, requireFileRead(reference: FileRef) {
        if (reference.path !== attached.path) throw new Error("exact file was not granted");
      } };
    },
  });
  expect([...await lazy.read(attached)]).toEqual([3, 3]);
  await expect(lazy.directory!.list(attached.volume as VolumeRef<"agent_private">,
    "", "", null, null, 1)).rejects.toThrow("directory was not granted");
  await expect(lazy.read({ ...attached, path: "other.txt" })).rejects.toThrow("exact file was not granted");
  expect(lazy.writer).toBeUndefined();
  granted = false;
  await expect(lazy.read(attached)).rejects.toThrow("owner read grant is unavailable");
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

test("artifact binding is independent of conversation content and retains owner grants", async () => {
  const file = await mailboxFile(1);
  const bytes = new Uint8Array(1);
  const artifacts = {
    validate: () => {}, verify: () => {},
    read: async () => bytes,
    fileReadCapability: () => "artifact:read",
    volumeReadCapability: () => "artifact:read",
    directoryReadCapability: () => "artifact:read",
    writer: { volume: file.volume, writeCapability: () => "artifact:write", stage: async () => file },
  };
  const content = { ...artifacts, read: async () => { throw new Error("wrong content provider"); } };
  const runtime = Harness.builder(contracts).content(content).artifacts(artifacts)
    .grant("artifact:read", "artifact:write").build();
  const context = new TaskContext(runtime, new AbortController().signal);
  expect([...await context.readArtifact(file)]).toEqual([0]);
  await expect(context.readFile(file)).rejects.toThrow("wrong content provider");
  expect(await context.stageArtifact("artifact-operation", file.path, bytes,
    file.descriptor.media_type, file.display_name)).toEqual(file);
  const denied = Harness.builder(contracts).artifacts(artifacts).build();
  await expect(new TaskContext(denied, new AbortController().signal).readArtifact(file))
    .rejects.toThrow("task scope cannot read this file");
  const needsArtifact = TaskDefinition.live("artifact-task", "1", () => 1,
    { requirements: ["artifacts:write"] });
  expect(() => Harness.builder(contracts).artifacts(artifacts).task(needsArtifact).build())
    .toThrow("unsatisfied task requirement");
  expect(() => Harness.builder(contracts).artifacts(artifacts).grant("artifact:write")
    .task(needsArtifact).build()).not.toThrow();
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
    const selected = builder.selectModelTool("versioned-tool", "2").model(testModel, {
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
    const admissionId = "dddddddd-dddd-4ddd-8ddd-dddddddddddd";
    const host: HarnessRuntimeHost = {
      policyIdentity: () => null,
      async admitResumable(_operationId, task, input) {
        if (typeof input !== "number") throw new TypeError("test host expected a number");
        admitted = { input, revision: task.revision };
        return { kind: "accepted" as const, task: new Task("task:durable" as RuntimeTaskId, async () => 6) };
      },
      async attach(id) { return { task: new Task(id, async () => 6), operationId: admissionId, taskName: "durable", revision: "1", implementationDigest: durableDigest }; },
      async reconcileEffect() { return { state: "indeterminate" } as const; },
      async send(message: TaskMessage) { return { accepted: true, messageId: message.id }; },
      async *inbox() { yield* [] as TaskMessage[]; },
    };
    const hosted = Harness.builder(contracts).host(host).task(definition).build();
    const result = await hosted.admit(definition, 3, admissionId);
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

  test("resumable transitions validate newly saved state and explicit waits", async () => {
    const context = new TaskContext(Harness.builder(contracts).build(), new AbortController().signal);
    const invalid = TaskDefinition.resumable<number, number, number>("invalid-checkpoint", "1", {
      state: numberSchema,
      initial: () => "invalid" as unknown as number,
      async transition() { return { kind: "continue", state: "invalid" as unknown as number }; },
    }, { implementationDigest: durableDigest, input: numberSchema, output: numberSchema });
    if (invalid.implementation.kind !== "resumable") throw new TypeError("expected resumable definition");
    await expect(Promise.resolve(invalid.implementation.component.initial(1))).rejects.toThrow();
    await expect(invalid.implementation.component.transition(context, 1)).rejects.toThrow();

    const wait = TaskDefinition.resumable<number, number, number>("timer-checkpoint", "1", {
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
      kind: "wait", state: 2, operationId: "11111111-1111-1111-1111-111111111111",
      deadline: new Date("2030-01-01T00:00:00.000Z"),
    });
  });

  test("host-backed handles preserve authoritative events and cancellation", async () => {
    const taskId = "task:remote" as RuntimeTaskId;
    const observed: number[] = [];
    let cancelled = false;
    const terminal = { id: "event:terminal", taskId, sequence: 43,
      event: { kind: "settled" as const, outcome: { kind: "succeeded" as const, value: 7 } } };
    const task = Task.fromHost(taskId, {
      operationId: "operation:remote",
      async result() { return { kind: "succeeded", value: 7 } as const; },
      async *events(fromSequence) {
        observed.push(fromSequence);
        if (fromSequence <= 42) yield { id: "event:remote", taskId, sequence: 42, event: { kind: "started" } as const };
      },
      async terminalEvent() { return terminal; },
      async cancel() { cancelled = true; return { requested: true, taskId }; },
    });
    expect(await task.result()).toEqual({ kind: "succeeded", value: 7 });
    const events = [];
    for await (const event of task.events(41)) events.push(event);
    expect(events.map(event => event.sequence)).toEqual([42, 43]);
    expect(observed).toEqual([41]);
    const afterTerminal = [];
    for await (const event of task.events(44)) afterTerminal.push(event);
    expect(afterTerminal).toEqual([]);
    expect(observed).toEqual([41, 44]);
    for (const outcome of [
      { kind: "failed" as const, error: { message: "failed" } },
      { kind: "cancelled" as const, receipt: { requested: true, taskId } },
    ]) {
      const settled = { ...terminal, event: { kind: "settled" as const, outcome } };
      const replayed = Task.fromHost(taskId, {
        operationId: "operation:remote",
        async result() { return outcome; },
        async *events(fromSequence) { if (fromSequence <= settled.sequence) yield settled; },
        async terminalEvent() { return settled; },
        async cancel() { return { requested: true, taskId }; },
      });
      const replay = [];
      for await (const event of replayed.events()) replay.push(event);
      expect(replay).toEqual([settled]);
      const past = [];
      for await (const event of replayed.events(settled.sequence + 1)) past.push(event);
      expect(past).toEqual([]);
    }
    expect(await task.cancel()).toEqual({ requested: true, taskId });
    expect(cancelled).toBeTrue();
    const disconnected = Task.fromHost(taskId, {
      operationId: "operation:remote",
      async result() { throw new Error("transport offline"); },
      async *events() { yield* []; },
      async terminalEvent() { return { id: "event:indeterminate", taskId, sequence: 1,
        event: { kind: "settled" as const, outcome: { kind: "indeterminate" as const, operationId: "operation:remote" } } }; },
      async cancel() { return { requested: false, taskId }; },
    });
    expect(await disconnected.result()).toEqual({ kind: "indeterminate", operationId: "operation:remote" });
    const disconnectedEvents = [];
    for await (const event of disconnected.events()) disconnectedEvents.push(event.event);
    expect(disconnectedEvents).toEqual([{ kind: "settled", outcome: { kind: "indeterminate", operationId: "operation:remote" } }]);
    const outOfOrder = Task.fromHost(taskId, {
      operationId: "operation:remote",
      async result() { return { kind: "succeeded", value: 7 } as const; },
      async *events() { yield { id: "event:later", taskId, sequence: 5, event: { kind: "started" } as const }; },
      async terminalEvent() { return { ...terminal, sequence: 2 }; },
      async cancel() { return { requested: false, taskId }; },
    });
    await expect(async () => {
      for await (const _event of outOfOrder.events()) { /* consume */ }
    }).toThrow("out-of-order terminal event");
  });

  test("reconciles lost durable admission acknowledgement by stable operation ID", async () => {
    const definition = TaskDefinition.resumable<number, number, number>("retryable", "1", {
      state: numberSchema,
      initial: input => input,
      async transition(_context, state) { return { kind: "finish", output: state }; },
    }, { implementationDigest: durableDigest, input: numberSchema, output: numberSchema });
    const observed = new Map<string, Task<number>>();
    const onceId = "eeeeeeee-eeee-4eee-8eee-eeeeeeeeeeee";
    const missingId = "ffffffff-ffff-4fff-8fff-ffffffffffff";
    const rejectedId = "12345678-1234-4234-8234-123456789abc";
    const invalidId = "12345678-1234-4234-8234-123456789abd";
    let loseAcknowledgement = true;
    let reject = false;
    const host: HarnessRuntimeHost = {
      policyIdentity: () => null,
      async admitResumable(operationId: string) {
        if (reject) return { kind: "rejected" as const, reason: { code: "unsupported" as const, message: "policy" } };
        let task = observed.get(operationId);
        if (!task) { task = new Task("task:once" as RuntimeTaskId, async () => "wrong type" as unknown as number); observed.set(operationId, task); }
        if (loseAcknowledgement) { loseAcknowledgement = false; throw new AdmissionUncertainError(operationId, new Error("ack lost")); }
        return { kind: "accepted" as const, task };
      },
      async reconcileAdmission(operationId) {
        const task = observed.get(operationId);
        return task ? { task, operationId, taskName: "retryable", revision: "1",
          implementationDigest: durableDigest } : null;
      },
      async attach(id) { return { task: observed.values().next().value as Task<unknown>, operationId: onceId, taskName: "retryable", revision: "1", implementationDigest: durableDigest }; },
      async reconcileEffect() { return { state: "indeterminate" } as const; },
      async send(message: TaskMessage) { return { accepted: true, messageId: message.id }; },
      async *inbox() { yield* [] as TaskMessage[]; },
    };
    const runtime = Harness.builder(contracts).host(host).task(definition).build();
    const first = await runtime.admit(definition, 1, onceId);
    expect(first).toEqual({ kind: "indeterminate", operationId: onceId });
    expect(await runtime.reconcileAdmission(missingId)).toBeNull();
    expect(await (await runtime.reconcileAdmission(onceId))?.result())
      .toMatchObject({ kind: "failed", error: { message: expect.stringContaining("tool value failed validation") } });
    const second = await runtime.admit(definition, 1, onceId);
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
    expect(await runtime.admit(definition, 2, rejectedId)).toMatchObject({ kind: "rejected", reason: { code: "unsupported" } });
    reject = false;
    host.admitResumable = async () => { throw new TypeError("invalid admission request"); };
    await expect(runtime.admit(definition, 2, invalidId)).rejects.toThrow("invalid admission request");
  });

  test("separate state and spawner bind without a combined host", async () => {
    const definition = TaskDefinition.resumable<number, number, number>("split", "1", {
      state: numberSchema,
      initial: input => input,
      async transition(_context, value) { return { kind: "finish", output: value }; },
    }, { implementationDigest: durableDigest, input: numberSchema, output: numberSchema });
    const id = "task:split" as RuntimeTaskId;
    const admissionId = "12345678-1234-4234-8234-123456789abe";
    let retained: TaskAdmissionRecord | undefined;
    let corruptAdmission = false;
    const operation = "03030303-0303-0303-0303-030303030303" as OperationId;
    const tool = defineTool<number, number>({ name: "split-tool", revision: "1", description: "split",
      inputSchema: { type: "number" }, outputSchema: { type: "number" }, parseInput: parseNumber,
      parseOutput: parseNumber }, () => { throw new Error("local tool must not run"); });
    const ownerTask = new Task(id, async () => 7);
    const untrustedTask = new Task(id, async () => 99);
    const state: HarnessRuntimeState = {
      policyIdentity: () => null,
      async attach(taskId) {
        if (taskId !== id) throw new Error("unknown task");
        return { task: ownerTask, operationId: admissionId, taskName: "split",
          revision: "1", implementationDigest: durableDigest,
          admission: corruptAdmission && retained ? { ...retained, input: 4 } : retained };
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
      async reconcileAdmission() { return { task: untrustedTask, operationId: admissionId,
        taskName: "split", revision: "1", implementationDigest: durableDigest, admission: retained }; },
    };
    const runtime = Harness.builder(contracts).state(state).spawner(spawner).task(definition)
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
    await expect(runtime.admit(definition, 3, admissionId)).rejects.toThrow("exact spawner request");
  });

  test("qualified execution is pinned and retries use the retained placement", async () => {
    const definition = TaskDefinition.resumable<number, number, number>("placed", "1", {
      state: numberSchema, initial: input => input,
      async transition(_context, value) { return { kind: "finish", output: value }; },
    }, { implementationDigest: durableDigest, input: numberSchema, output: numberSchema });
    const localClosure = TaskDefinition.live<number, number>("local-closure", "1", (_context, value) => value);
    const routeIdentity = policyIdentity("test.execution", "1", Uint8Array.from({ length: 32 }, () => 7));
    const placement = {
      provider: routeIdentity,
      build: { kind: "artifact" as const,
        provider: { namespace: "test", family: "objects", version: "1" }, key: [1], version: null },
      environment: { kind: "sandbox" as const,
        provider: { namespace: "test", family: "machines", version: "1" }, key: [2], version: null },
      readiness_revision: Array.from({ length: 32 }, () => 3),
    };
    const operationId = "12345678-1234-4234-8234-123456789abc";
    const taskId = "task:placed" as RuntimeTaskId;
    const ownerTask = new Task(taskId, async () => 7);
    let retained: TaskAdmissionRecord | undefined;
    let qualifications = 0;
    let corruptOwner = false;
    const attachment = () => ({ task: ownerTask, operationId, taskName: "placed", revision: "1",
      implementationDigest: durableDigest, admission: retained });
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
    const runtime = Harness.builder(contracts).execution(execution).task(definition).task(localClosure).build();
    expect(() => runtime.spawn(localClosure, 3)).toThrow("live task closures cannot cross an execution provider");
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
    await expect(runtime.admit(definition, 3, operationId)).rejects.toThrow("exact spawner request");
  });

  test("preserves unresolved batch admission and stable scoped entry identities", async () => {
    const definition = TaskDefinition.resumable<number, number, number>("batch-work", "2", {
      state: numberSchema,
      initial: input => input,
      async transition(_context, state) { return { kind: "finish", output: state }; },
    }, { implementationDigest: durableDigest, input: numberSchema, output: numberSchema });
    const ids: string[] = [];
    let admittedManifests = 0;
    let retainedBatch: import("../src/index.js").BatchAdmissionRequest | undefined;
    const host: HarnessRuntimeHost = {
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
    };
    const groupId = "11111111-1111-4111-8111-111111111111" as GroupId;
    const batch = new Batch("22222222-2222-4222-8222-222222222222" as BatchId, [1, 2]);
    const group = Harness.builder(contracts).host(host).task(definition).build().group<number>(GroupPolicies.collectAll, groupId);
    expect((await group.spawnMany(definition, batch)).map(entry => entry.admission.kind)).toEqual(["indeterminate", "indeterminate"]);
    expect((await group.spawnMany(definition, batch)).map(entry => entry.admission.kind)).toEqual(["indeterminate", "indeterminate"]);
    expect(admittedManifests).toBe(1);
    expect(ids).toEqual([0, 1].map(index => contracts.batchMemberOperationId(groupId, batch.id, index)));
    const completed = [];
    for await (const entry of group.asCompleted()) completed.push(entry);
    expect(completed).toHaveLength(2);
    expect(completed.map(entry => entry.admission.kind)).toEqual(["indeterminate", "indeterminate"]);
    expect((await group.join()).complete).toBeFalse();
    const reconstructed = Harness.builder(contracts).host(host).task(definition).build().group<number>(GroupPolicies.collectAll, groupId);
    expect((await reconstructed.reconcileBatchId(definition, batch.id))?.map(entry => entry.admission.kind)).toEqual(["indeterminate", "indeterminate"]);
    expect(await reconstructed.reconcileBatchId(definition, "33333333-3333-4333-8333-333333333333" as BatchId)).toBeNull();
    expect((await reconstructed.join()).entries).toHaveLength(2);
    const cancellation = await reconstructed.cancelBatchId(definition, batch.id);
    expect(cancellation?.entries.map(entry => entry.status.kind)).toEqual(["indeterminate", "indeterminate"]);
  });

  test("asks the durable batch owner to retain cancellation after a failed member", async () => {
    const definition = TaskDefinition.resumable<number, number, number>("cancel-batch", "1", {
      state: numberSchema, initial: input => input,
      async transition(_context, state) { return { kind: "finish", output: state }; },
    }, { implementationDigest: durableDigest, input: numberSchema, output: numberSchema });
    let retained: import("../src/index.js").BatchAdmissionRequest | undefined;
    let replay: HostBatchReplay | undefined;
    let declarations = 0;
    let requested = 0;
    const tasks = new Map<string, Task<number>>();
    const host: HarnessRuntimeHost = {
      policyIdentity: () => null,
      async admitBatch(request) {
        retained = request;
        expect(request.canonical.group_policy).toBe("cancel-on-failure");
        const [first, second] = request.members;
        if (!first || !second) throw new Error("expected two batch members");
        tasks.set(first.operation_id, new Task<number>(first.operation_id as RuntimeTaskId,
          async () => { throw new Error("first member failed"); }));
        tasks.set(second.operation_id, new Task<number>(second.operation_id as RuntimeTaskId,
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
          revision: definition.revision, implementationDigest: durableDigest };
      },
      async reconcileEffect() { return { state: "indeterminate" } as const; },
      async send(message: TaskMessage) { return { accepted: true, messageId: message.id }; },
      async *inbox() { yield* [] as TaskMessage[]; },
    };
    const groupId = "11111111-1111-4111-8111-111111111112" as GroupId;
    const batchId = "22222222-2222-4222-8222-222222222223" as BatchId;
    const group = Harness.builder(contracts).host(host).task(definition).build()
      .group<number>(GroupPolicies.cancelOnFailure, groupId);
    const admitted = await group.spawnMany(definition, new Batch(batchId, [1, 2]));
    expect(admitted.map(entry => entry.admission.kind)).toEqual(["accepted", "accepted"]);
    const completed = await group.join();
    expect(completed.entries.map(entry => entry.outcome?.kind)).toEqual(["failed", "cancelled"]);
    expect(completed.cancellation).toBe("requested");
    expect(completed.complete).toBeTrue();
    expect(declarations).toBe(1);
    expect(requested).toBe(1);
  });

  test("keeps every batch slot reconcilable after a lost manifest acknowledgement", async () => {
    const definition = TaskDefinition.resumable<number, number, number>("batch-lost-ack", "1", {
      state: numberSchema, initial: input => input,
      async transition(_context, state) { return { kind: "finish", output: state }; },
    }, { implementationDigest: durableDigest, input: numberSchema, output: numberSchema });
    let retained: import("../src/index.js").BatchAdmissionRequest | undefined;
    const host: HarnessRuntimeHost = {
      policyIdentity: () => null,
      async admitBatch(request) { retained = request; throw new AdmissionUncertainError(request.batchId); },
      async loadBatch(batchId) { return retained?.batchId === batchId ? retained.canonical : null; },
      async reconcileBatch(request) {
        expect(request.inputDigest).toEqual(retained?.inputDigest);
        return { taskName: request.taskName, revision: request.revision,
          implementationDigest: request.implementationDigest, inputDigest: request.inputDigest,
          entries: request.members.map((member, index) => ({ key: { batchId: request.batchId, index },
            admission: { kind: "indeterminate" as const, operationId: member.operation_id } })) };
      },
      async attach() { throw new Error("no member accepted"); },
      async reconcileEffect() { return { state: "indeterminate" }; },
      async send(message) { return { accepted: true, messageId: message.id }; },
      async *inbox() { yield* []; },
    };
    const batch = new Batch("33333333-3333-4333-8333-333333333333" as BatchId, [1, 2, 3]);
    const group = Harness.builder(contracts).host(host).task(definition).build()
      .group<number>(GroupPolicies.collectAll, "44444444-4444-4444-8444-444444444444" as GroupId);
    expect((await group.spawnMany(definition, batch)).map(entry => entry.admission.kind))
      .toEqual(["indeterminate", "indeterminate", "indeterminate"]);
    expect(retained?.members).toHaveLength(3);
    expect((await group.reconcileBatch(definition, batch)).map(entry => entry.admission.kind))
      .toEqual(["indeterminate", "indeterminate", "indeterminate"]);
  });

  test("rejects incomplete and unrelated host batch replay without retaining it", async () => {
    const definition = TaskDefinition.resumable<number, number, number>("replay", "1", {
      state: numberSchema, initial: input => input,
      async transition(_context, state) { return { kind: "finish", output: state }; },
    }, { implementationDigest: durableDigest, input: numberSchema, output: numberSchema });
    const batch = new Batch("55555555-5555-4555-8555-555555555555" as BatchId, [1, 2]);
    let entries: HostBatchReplay["entries"] = [];
    let retained: import("../src/index.js").BatchAdmissionRequest | undefined;
    let wrongDigest = false;
    const host: HarnessRuntimeHost = {
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
    };
    const group = Harness.builder(contracts).host(host).task(definition).build()
      .group<number>(GroupPolicies.collectAll, "66666666-6666-4666-8666-666666666666" as GroupId);
    await group.spawnMany(definition, batch);
    const entry = (index: number, operationId: string = contracts.batchMemberOperationId(group.id, batch.id, index)) =>
      ({ key: { batchId: batch.id, index }, admission: { kind: "indeterminate" as const, operationId } });
    entries = [entry(0)];
    await expect(group.reconcileBatch(definition, batch)).rejects.toThrow("incomplete batch");
    entries = [entry(0), entry(1), entry(2)];
    await expect(group.reconcileBatch(definition, batch)).rejects.toThrow("incomplete batch");
    entries = [entry(0), entry(2)];
    await expect(group.reconcileBatch(definition, batch)).rejects.toThrow("invalid batch entry identity");
    entries = [entry(0), entry(0)];
    await expect(group.reconcileBatch(definition, batch)).rejects.toThrow("invalid batch entry identity");
    entries = [entry(0), entry(1, "wrong-operation")];
    await expect(group.reconcileBatch(definition, batch)).rejects.toThrow("unrelated batch admission");
    entries = [entry(1), entry(0)];
    wrongDigest = true;
    await expect(group.reconcileBatch(definition, batch)).rejects.toThrow("input digest differs");
    wrongDigest = false;
    entries = [{ key: { batchId: batch.id, index: 0 },
      admission: { kind: "rejected", reason: { code: "unsupported", message: "denied" } },
      outcome: { kind: "succeeded", value: 1 } }, entry(1)];
    await expect(group.reconcileBatch(definition, batch)).rejects.toThrow("outcome cannot precede");
    entries = [entry(1), entry(0)];
    expect((await group.reconcileBatch(definition, batch)).map(value => value.key.index)).toEqual([0, 1]);
    await expect(group.reconcileBatch(definition, new Batch(batch.id, [1, 3])))
      .rejects.toThrow("another admission request");
    expect((await group.join()).entries).toHaveLength(2);
  });

  test("non-canonical durable batch inputs reject entries before host admission", async () => {
    const definition = TaskDefinition.resumable<number, number, number>("canonical-input", "1", {
      state: numberSchema, initial: input => input,
      async transition(_context, state) { return { kind: "finish", output: state }; },
    }, { implementationDigest: durableDigest, input: numberSchema, output: numberSchema });
    let admitted = 0;
    const host: HarnessRuntimeHost = {
      policyIdentity: () => null,
      async admitBatch() { admitted++; throw new Error("must not admit"); },
      async attach() { throw new Error("unused"); },
      async reconcileEffect() { return { state: "indeterminate" }; },
      async send(message) { return { accepted: true, messageId: message.id }; },
      async *inbox() { yield* []; },
    };
    const group = Harness.builder(contracts).host(host).task(definition).build()
      .group<number>(GroupPolicies.collectAll, "77777777-7777-4777-8777-777777777777" as GroupId);
    const inputs = [1, undefined] as unknown as number[];
    const entries = await group.spawnMany(definition, new Batch("88888888-8888-4888-8888-888888888888" as BatchId, inputs));
    expect(entries.map(entry => entry.admission.kind)).toEqual(["rejected", "rejected"]);
    expect(entries.every(entry => entry.admission.kind === "rejected"
      && entry.admission.reason.code === "invalid_input")).toBeTrue();
    expect(admitted).toBe(0);
  });

  test("rejects host policy drift during attachment and batch reconciliation", async () => {
    const definition = TaskDefinition.resumable<number, number, number>("drift-replay", "1", {
      state: numberSchema, initial: input => input,
      async transition(_context, state) { return { kind: "finish", output: state }; },
    }, { implementationDigest: durableDigest, input: numberSchema, output: numberSchema });
    let currentIdentity: ReturnType<HarnessRuntimeHost["policyIdentity"]> = null;
    let retained: import("../src/index.js").BatchAdmissionRequest | undefined;
    const host: HarnessRuntimeHost = {
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
    };
    const runtime = Harness.builder(contracts).host(host).task(definition).build();
    await expect(runtime.attach(definition, "task:drift" as RuntimeTaskId)).rejects.toThrow("policy implementation changed");
    currentIdentity = null;
    const group = runtime.group<number>(GroupPolicies.collectAll, "99999999-9999-4999-8999-999999999999" as GroupId);
    const batch = new Batch("aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa" as BatchId, [1]);
    await group.spawnMany(definition, batch);
    await expect(group.reconcileBatch(definition, batch))
      .rejects.toThrow("policy implementation changed");
    expect((await group.join()).entries).toHaveLength(1);
  });

  test("rejects the entire immutable batch before publication when an input is invalid", async () => {
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
      async admitBatch(request) { admitted.push(...request.members.map(member => member.operation_id)); throw new Error("must not admit"); },
      async attach(id) { return { task: new Task(id, async () => undefined), operationId: "operation:partial", taskName: "validate-input", revision: "1", implementationDigest: durableDigest }; },
      async reconcileEffect() { return { state: "indeterminate" } as const; },
      async send(message: TaskMessage) { return { accepted: true, messageId: message.id }; },
      async *inbox() { yield* [] as TaskMessage[]; },
    };
    const group = Harness.builder(contracts).host(host).task(definition).build().group<number>(GroupPolicies.collectAll,
      "bbbbbbbb-bbbb-4bbb-8bbb-bbbbbbbbbbbb" as GroupId);
    const entries = await group.spawnMany(definition, new Batch("cccccccc-cccc-4ccc-8ccc-cccccccccccc" as BatchId, [1, -1, 2]));
    expect(entries.map(entry => entry.admission.kind)).toEqual(["rejected", "rejected", "rejected"]);
    expect(admitted).toHaveLength(0);
    expect((await group.join()).entries).toHaveLength(0);
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
    const operationId = "78eb6d34-0b1d-46e9-b282-9f0379b2b25e";
    expect(await approved.call(approved.tool(tool), 4, signal, undefined, operationId, "provider-call")).toBe(4);
    expect(await approved.call(approved.tool(tool), 4, signal, undefined, operationId, "provider-call")).toBe(4);
    expect(seen[2]).toEqual(seen[3]);
    await expect(approved.call(approved.tool(tool), 4, signal, undefined, "model-tool:turn:0", "provider-call"))
      .rejects.toThrow();
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

  test("pins live batch requests and never starts a duplicate entry", async () => {
    let runs = 0;
    const task = TaskDefinition.live<number, number>("live-batch", "1", async (_context, value) => {
      runs++;
      return value;
    });
    const group = Harness.builder(contracts).task(task).build().group<number>(GroupPolicies.collectAll,
      "group:live-stable" as GroupId);
    const batch = new Batch("batch:live-stable" as BatchId, [1, 2]);
    const first = await group.spawnMany(task, batch);
    const repeated = await group.spawnMany(task, batch);
    expect(repeated.map(entry => entry.admission)).toEqual(first.map(entry => entry.admission));
    await expect(group.spawnMany(task, new Batch(batch.id, [1, 3])))
      .rejects.toThrow("another admission request");
    await group.join();
    expect(runs).toBe(2);
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
    expect((await group.join()).entries.at(-1)?.outcome).toEqual({ kind: "succeeded", value: "late" });
    const succeeding = runtime.group<string>(GroupPolicies.collectAll);
    await succeeding.spawnMany(task, new Batch("batch:first-success" as BatchId, [{ delay: 0 }, { delay: 10, value: "winner" }, { delay: 50, value: "late" }]));
    expect((await succeeding.firstSuccess()).value).toBe("winner");
    expect((await succeeding.join()).entries.at(-1)?.outcome).toEqual({ kind: "succeeded", value: "late" });
  });

  test("executes model tool calls with durable task ownership and typed receipts", async () => {
    let modelStep = 0; let sender: RuntimeTaskId | undefined; let toolOperationId: string | undefined;
    const payload = await mailboxFile(3);
    const host: HarnessRuntimeHost = {
      policyIdentity: () => null,
      async attach(id) { return { task: new Task(id, async () => undefined), operationId: "operation:run", taskName: "acyclic.default-agent", revision: "1", implementationDigest: durableDigest }; },
      async reconcileEffect() { return { state: "succeeded", result: payload } as const; },
      async send(message: TaskMessage) { sender = message.sender; return { accepted: true, messageId: message.id }; },
      async *inbox() { yield* [] as TaskMessage[]; },
    };
    const tool = defineTool<number, number>({ name: "double", revision: "1", description: "double", inputSchema: {}, outputSchema: {}, parseInput: parseNumber, parseOutput: parseNumber }, async (context, input) => {
      toolOperationId = context.operationId;
      const effect = await context.reconcileEffect("effect:tool" as EffectId);
      await context.send(context.taskId!, payload);
      return effect.state === "succeeded" ? input * 4 : 0;
    });
    const runtime = Harness.builder(contracts).host(host).tool(tool).grant("tool:call:double").model(testModel, {
      async *generate() { if (modelStep++ === 0) { yield { kind: "tool_call" as const, callId: "call", name: "double", arguments: 3 }; yield { kind: "completed" as const, metadata: {} }; } else { yield { kind: "content" as const, delta: "done" }; yield { kind: "completed" as const, metadata: { tokens: 1 } }; } },
      async reconcile() { return undefined; },
    }).build();
    const output = await runtime.run("go");
    expect(output.text).toBe("done");
    expect(output.receipts).toEqual([{ kind: "model-completed", metadata: {} }, { kind: "tool", step: 0, callId: "call", name: "double", arguments: 3, value: 12, projection: 12 }, { kind: "model-completed", metadata: { tokens: 1 } }]);
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
    const runtime = Harness.builder(contracts).model(testModel, {
      async *generate(request) { observed = request.messages; yield { kind: "completed" as const, metadata: {} }; },
      async reconcile() { return undefined; },
    }).build();
    await runtime.run({ prompt: "describe", content: [{ kind: "file", file, policy: "native" }] });
    expect(observed[0]?.content).toEqual([{ kind: "text", text: "describe" }, { kind: "file", file, policy: "native" }]);
  });

  test("model identity and options are pinned at binding, including scoped overrides", async () => {
    const rootIdentity = { provider: "root", name: "model", revision: "3", options: { mode: "original" } };
    const seen: unknown[] = [];
    const provider = { async *generate(request: { model: unknown }) { seen.push(request.model); yield { kind: "completed" as const, metadata: {} }; },
      async reconcile() { return undefined; } };
    const runtime = Harness.builder(contracts).model(rootIdentity, provider).build();
    rootIdentity.options.mode = "mutated";
    await runtime.run("root");
    expect(seen[0]).toEqual({ provider: "root", name: "model", revision: "3", options: { mode: "original" } });
    const childIdentity = { provider: "child", name: "model", revision: "4", options: { mode: "scoped" } };
    const scoped = runtime.scoped(ExecutionScope.create().model(childIdentity, provider));
    childIdentity.options.mode = "mutated";
    await scoped.run("child");
    expect(seen[1]).toEqual({ provider: "child", name: "model", revision: "4", options: { mode: "scoped" } });
    expect(() => Harness.builder(contracts).model({ provider: "", name: "model", revision: "1", options: {} }, provider)).toThrow("identity");
  });

  test("selected context preserves canonical roles without a synthetic user duplicate", async () => {
    let observed: readonly ModelMessage[] = [];
    const runtime = Harness.builder(contracts).model(testModel, {
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
    const runtime = Harness.builder(contracts).limits({ context_messages: 1, render_bytes: 8 }).model(testModel, {
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
    const runtime = Harness.builder(contracts).limits({ model_events_per_step: 2, tool_calls_per_step: 1 }).model(testModel, {
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
    }).model(testModel, {
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
    }).model(testModel, {
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
    const runtime = Harness.builder(contracts).tool(tool).grant("tool:call:again").model(testModel, {
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
