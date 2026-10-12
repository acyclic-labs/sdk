import { test, expect } from "bun:test";
import { Harness, AgentHarness, ExecutionScope, NativeContracts, defineRuntimeSchema, TaskDefinition, defineTool, type OperationId, type TaskId, type HarnessRuntimeState, policyIdentity, type AgentId, type FileRef, composeContentBindings, DEFAULT_LIMITS, type VolumeRef, TaskContext, type HarnessRuntimeSpawner, type ModelToolDefinition, type Outcome, type BatchId, type ComponentIdentity, type HarnessExecutionProvider, type EffectiveScope, HarnessBuilder, type GroupId, Task } from "../src/index.js";
import { contracts, parseNumber, jsonProjection, mailboxFile, testModel, parseNull, approvalPolicyIdentity, numberTool, scopedPolicyIdentity, denyPolicyIdentity, numberSchema, parseString } from "./support/runtime-fixture.js";
import { syntheticAccounting } from "./support/model-accounting.mjs";

test("fork capture binding requires parent publication authority", async () => {
const snapshot = { parent: { kind: "conversation" as const, id: "parent" }, revision: 1n };
await expect(Harness.builder(contracts).parentSnapshot(snapshot).build()).rejects.toThrow();
const bound = await Harness.builder(contracts).parentSnapshot(snapshot).grant("fork:publish").limits({ attachments: 1 }).build();
expect(bound.components.limits.attachments).toBe(1);
expect(bound).toBeInstanceOf(AgentHarness);
const narrowed = await bound.scoped(new ExecutionScope().onlyGrants("fork:publish"));
expect(() => narrowed.atParentSnapshot(snapshot)).toThrow();
expect(() => bound.atParentSnapshot(snapshot)).toThrow();
expect(() => bound.atParentSnapshot({ ...snapshot, parent: { kind: "conversation" as const, id: "other" }, revision: 2n })).toThrow();
expect(bound.atParentSnapshot({ ...snapshot, parent: { kind: "conversation" as const, id: "parent" }, revision: 2n })).toBeInstanceOf(AgentHarness);
});

test("native tool JSON preserves special property names as data", async () => {
const value = Object.fromEntries([["__proto__", { safe: true }], ["nested", { value: 1 }], ["byte_length", -2]]);
const admitted = (await NativeContracts.create()).validateToolValue({ type: "object" }, value);
expect(Object.getOwnPropertyDescriptor(admitted, "__proto__")?.value).toEqual({ safe: true });
expect((admitted as { nested: { value: number } }).nested.value).toBe(1);
expect((admitted as { byte_length: number }).byte_length).toBe(-2);
expect(Object.getPrototypeOf(admitted)).toBe(Object.prototype);
});

test("native tool JSON rejects values that the WASM serializer would coerce", async () => {
expect(() => contracts.validateToolValue({ type: "object" }, { unsafe: Number.POSITIVE_INFINITY })).toThrow();
expect(() => contracts.validateToolValue({ type: "object" }, { unsafe: -0 })).toThrow();
expect(() => contracts.validateToolValue({ type: "object", maximum: Number.POSITIVE_INFINITY }, {})).toThrow();
});

test("native tool admission cannot reread a mutable getter after JSON preflight", async () => {
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

test("task and tool schemas enter the registry only as strict Rust JSON", async () => {
const unsafe = { type: "number", maximum: Number.POSITIVE_INFINITY };
const schema = await defineRuntimeSchema("unsafe", unsafe, parseNumber);
const task = await TaskDefinition.live("unsafe-schema-task", "1", (_context, value: number) => value,
  { input: schema });
expect(() => Harness.builder(contracts).task(task)).toThrow();
const tool = await defineTool({ name: "unsafe-schema-tool", revision: "1", description: "unsafe schema",
  inputSchema: unsafe, outputSchema: { type: "number" }, parseInput: parseNumber,
  projection: jsonProjection, parseOutput: parseNumber }, (_context, value) => value);
expect(() => Harness.builder(contracts).tool(tool)).toThrow();
});

test("resumable tools are pinned at registration and execute only through durable owner state", async () => {
const definition = { name: "resumable-file-tool", revision: "1", description: "read a pinned file",
  inputSchema: { type: "number" }, outputSchema: { type: "number" },
  parseInput: parseNumber, projection: jsonProjection, parseOutput: parseNumber };
const machine = { name: definition.name, version: definition.revision, digest: Array(32).fill(7) as number[] };
await expect(Harness.builder(contracts).resumableTool(definition, machine).build()).rejects.toThrow();
expect(() => Harness.builder(contracts).resumableTool(definition, { ...machine, digest: Array(32).fill(0) })).toThrow();
const operation = "12345678-1234-4234-8234-123456789abc" as OperationId;
const taskId = "22345678-1234-4234-8234-123456789abc" as TaskId;
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
const runtime = await Harness.builder(contracts).state(state).resumableTool(definition, machine)
  .grant("tool:call:resumable-file-tool").build();
const tool = runtime.tool(definition);
await expect(runtime.call(tool, 2)).rejects.toThrow();
expect(await runtime.callDurable(operation, tool, 2, taskId)).toEqual({ kind: "succeeded", value: 2 });
});

test("content routing distinguishes owner volumes on one provider and keeps one writer", async () => {
const file = await mailboxFile(2);
const other = { ...file, volume: { ...file.volume,
  id: "private", class: "agent_private", owner: { kind: "agent", id: "01010101-0101-0101-0101-010101010101" as AgentId } } } as FileRef;
const validated: number[] = [];
const binding = (marker: number) => ({
  validate: (_reference: FileRef) => { validated.push(marker); },
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
expect(() => contracts.verifyFileBytes(file, Uint8Array.of(2))).toThrow();
expect(() => composeContentBindings(contracts, [
  { volume: file.volume, content: first },
  { volume: file.volume, content: second },
])).toThrow();
expect(() => composeContentBindings(contracts, [
  { volume: other.volume, content: first },
  { volume: { provider: other.volume.provider, id: other.volume.id, class: "agent_private",
    owner: { kind: "agent", id: "01010101010101010101010101010101" as AgentId } }, content: second },
])).toThrow();
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
  "", "", null, null, 1)).rejects.toThrow();
await expect(lazy.read({ ...attached, path: "other.txt" })).rejects.toThrow();
expect(lazy.writer).toBeUndefined();
granted = false;
await expect(lazy.read(attached)).rejects.toThrow();
});

test("builder snapshots owner content and writer callbacks at admission", async () => {
const file = await mailboxFile(1);
const writer = {
  volume: file.volume,
  writeCapability: () => "write:original",
  stage: async () => file,
};
const binding = {
  validate: () => {},
  read: async () => Uint8Array.of(1),
  fileReadCapability: () => "read:original",
  volumeReadCapability: () => "volume:original",
  directoryReadCapability: () => "directory:original",
  writer,
};
const runtime = await Harness.builder(contracts).content(binding).build();
binding.read = async () => Uint8Array.of(9);
binding.fileReadCapability = () => "read:replacement";
writer.stage = async () => { throw new Error("writer swapped"); };
writer.writeCapability = () => "write:replacement";
expect([...await runtime.components.content!.read(file)]).toEqual([1]);
expect(runtime.components.content!.fileReadCapability(file)).toBe("read:original");
expect(runtime.components.content!.writer!.writeCapability()).toBe("write:original");
expect(await runtime.components.content!.writer!.stage("operation", file.path, Uint8Array.of(1), "application/octet-stream", file.display_name)).toBe(file);
});

test("artifact binding is independent of conversation content and retains owner grants", async () => {
const file = await mailboxFile(1);
const bytes = new Uint8Array(1);
const artifacts = {
  validate: () => {},
  read: async () => bytes,
  fileReadCapability: () => "artifact:read",
  volumeReadCapability: () => "artifact:read",
  directoryReadCapability: () => "artifact:read",
  writer: { volume: file.volume, writeCapability: () => "artifact:write", stage: async () => file },
};
const content = { ...artifacts, read: async () => { throw new Error("wrong content provider"); } };
const runtime = await Harness.builder(contracts).content(content).artifacts(artifacts)
  .grant("artifact:read", "artifact:write").build();
const context = new TaskContext(runtime, new AbortController().signal);
expect([...await context.readArtifact(file)]).toEqual([0]);
await expect(context.readFile(file)).rejects.toThrow();
expect(await context.stageArtifact("artifact-operation", file.path, bytes,
  file.descriptor.media_type, file.display_name)).toEqual(file);
const denied = await Harness.builder(contracts).artifacts(artifacts).build();
await expect(new TaskContext(denied, new AbortController().signal).readArtifact(file))
  .rejects.toThrow();
const needsArtifact = await TaskDefinition.live("artifact-task", "1", () => 1,
  { requirements: ["artifacts:write"] });
await expect(Harness.builder(contracts).artifacts(artifacts).task(needsArtifact).build())
  .rejects.toThrow();
await expect(Harness.builder(contracts).artifacts(artifacts).grant("artifact:write")
  .task(needsArtifact).build()).resolves.toBeDefined();
});

test("pins task dependencies and rejects missing versions and cycles at build time", async () => {
  const leaf = await TaskDefinition.live("leaf", "2", () => 1);
  const parent = await TaskDefinition.live("parent", "1", () => 2, { requirements: ["task:leaf@2"] });
  await expect(Harness.builder(contracts).task(parent).task(leaf).build()).resolves.toBeDefined();
  const stale = await TaskDefinition.live("stale", "1", () => 0, { requirements: ["task:leaf@1"] });
  await expect(Harness.builder(contracts).task(stale).task(leaf).build()).rejects.toThrow();
  const left = await TaskDefinition.live("left", "1", () => 1, { requirements: ["task:right@1"] });
  const right = await TaskDefinition.live("right", "1", () => 1, { requirements: ["task:left@1"] });
  await expect(Harness.builder(contracts).task(left).task(right).build()).rejects.toThrow();
});

test("admits exact grants but rejects unauthenticated extension requirements", async () => {
  const granted = await TaskDefinition.live("granted-task", "1", () => 1,
    { requirements: ["grant:task:run"] });
  await expect(Harness.builder(contracts).grant("task:run").task(granted).build()).resolves.toBeDefined();
  await expect(Harness.builder(contracts).task(granted).build()).rejects.toThrow();
  const extension = await TaskDefinition.live("extension-task", "1", () => 1,
    { requirements: ["extension:example.state@3"] });
  await expect(Harness.builder(contracts).task(extension).build()).rejects.toThrow();
});

test("requires capability grants and a complete durable binding for task dependencies", async () => {
  const modelTask = await TaskDefinition.live("model-task", "1", () => 1, { requirements: ["model"] });
  const provider = {
    ...syntheticAccounting(contracts), async *generate() { yield { kind: "completed" as const, metadata: {} }; },
    async reconcile() { return undefined; },
  };
  await expect(Harness.builder(contracts).model(testModel, provider).task(modelTask).build())
    .rejects.toThrow();
  await expect(Harness.builder(contracts).model(testModel, provider)
    .grant("model:generate").task(modelTask).build()).resolves.toBeDefined();
  await expect(Harness.builder(contracts).grant("model:generate").task(modelTask).build())
    .resolves.toBeDefined();

  const stateTask = await TaskDefinition.live("state-task", "1", () => 1, { requirements: ["state"] });
  const spawnerTask = await TaskDefinition.live("spawner-task", "1", () => 1, { requirements: ["spawner"] });
  const state = { policyIdentity: () => null } as unknown as HarnessRuntimeState;
  const spawner = { policyIdentity: () => null } as unknown as HarnessRuntimeSpawner;
  expect((await Harness.builder(contracts).state(state).task(stateTask).build()).components.state).toBe(state);
  await expect(Harness.builder(contracts).spawner(spawner).task(spawnerTask).build()).rejects.toThrow();
  await expect(Harness.builder(contracts).state(state).spawner(spawner)
    .task(stateTask).task(spawnerTask).build()).resolves.toBeDefined();
});

test("retains simultaneous pinned task revisions without an implicit latest", async () => {
  const first = await TaskDefinition.live<void, number>("versioned", "1", () => 1);
  const second = await TaskDefinition.live<void, number>("versioned", "2", () => 2);
  const needsFirst = await TaskDefinition.live<void, number>("needs-first", "1", () => 1,
    { requirements: ["task:versioned@1"] });
  const runtime = await Harness.builder(contracts).task(first).task(second).task(needsFirst).build();
  expect(Object.is(runtime.task("versioned@1"), first)).toBe(true);
  expect(Object.is(runtime.task("versioned@2"), second)).toBe(true);
  expect(runtime.task(first)).toBe(first);
  expect(() => runtime.task("versioned")).toThrow();
  expect(await runtime.spawn(first, undefined).result()).toEqual({ kind: "succeeded", value: 1 });
  expect(await runtime.spawn(second, undefined).result()).toEqual({ kind: "succeeded", value: 2 });
  expect(() => Harness.builder(contracts).task(first).task(first)).toThrow();
  await expect(TaskDefinition.live("bad@name", "1", () => 0)).rejects.toThrow();
});

test("pins tool revisions independently and selects the model-visible revision explicitly", async () => {
  const first = await defineTool<null, number>({ name: "versioned-tool", revision: "1", description: "first", inputSchema: {}, outputSchema: {}, parseInput: parseNull, projection: jsonProjection, parseOutput: parseNumber }, () => 1);
  const second = await defineTool<null, number>({ name: "versioned-tool", revision: "2", description: "second", inputSchema: {}, outputSchema: {}, parseInput: parseNull, projection: jsonProjection, parseOutput: parseNumber }, () => 2);
  const needsFirst = await TaskDefinition.live<void, number>("needs-first-tool", "1", () => 1,
    { requirements: ["tool:versioned-tool@1"] });
  const builder = Harness.builder(contracts).tool(first).tool(second).task(needsFirst).grant("tool:call:versioned-tool");
  const ambiguous = await builder.build();
  expect(ambiguous.tool("versioned-tool@1").definition).toMatchObject({ name: first.name, revision: first.revision });
  expect(ambiguous.tool("versioned-tool@2").definition).toMatchObject({ name: second.name, revision: second.revision });
  expect(() => ambiguous.tool("versioned-tool")).toThrow();
  expect(await ambiguous.call(ambiguous.tool("versioned-tool@1"), null)).toBe(1);
  expect(await ambiguous.call(ambiguous.tool("versioned-tool@2"), null)).toBe(2);
  let visible: readonly ModelToolDefinition[] = [];
  const selected = await builder.selectModelTool("versioned-tool", "2").model(testModel, {
    ...syntheticAccounting(contracts), async *generate(request) { visible = request.tools; yield { kind: "completed" as const, metadata: {} }; },
    async reconcile() { return undefined; },
  }).build();
  expect(selected.tool("versioned-tool").definition).toMatchObject({ name: second.name, revision: second.revision });
  const typedValue: number = await selected.call(selected.tool(second), null);
  expect(typedValue).toBe(2);
  expect(() => selected.tool({ ...second })).toThrow();
  await selected.run("select");
  expect(visible).toEqual([{ name: "versioned-tool", revision: "2", description: "second", inputSchema: {}, outputSchema: {}, projectionSchema: jsonProjection.schema }]);
  expect(() => Harness.builder(contracts).tool(first).tool(first)).toThrow();
  expect(() => builder.selectModelTool("versioned-tool", "3")).toThrow();
});

test("preserves task input and output types through nested execution", async () => {
  const double = await TaskDefinition.live<number, number>("double", "1", async (_context, value) => value * 2);
  const sum = await TaskDefinition.live<readonly number[], number>("sum", "1", async (context, values) => {
    const group = context.group<number>({ kind: "collect-all" });
    await group.spawnMany(context.task(double), { id: "batch:sum" as BatchId, inputs: values });
    return (await group.join()).entries.reduce((total, entry) => {
      if (entry.outcome?.kind !== "succeeded") throw new Error("child task failed");
      return total + entry.outcome.value;
    }, 0);
  });
  const runtime = await Harness.builder(contracts).task(double).task(sum).grant("task:spawn:double@1").build();

  const outcome: Outcome<number> = await runtime.spawn(sum, [1, 2, 3]).result();
  expect(outcome).toEqual({ kind: "succeeded", value: 12 });
});

test("a task context and its groups cannot invoke an ungranted descendant", async () => {
  const allowed = await TaskDefinition.live<number, number>("allowed-child", "1", (_context, value) => value + 1);
  const denied = await TaskDefinition.live<number, number>("denied-child", "1", (_context, value) => value + 100);
  const nested = await TaskDefinition.live<number, boolean>("narrowed-parent", "1", async (child, value) => {
    const allowedOutcome = await child.spawn(child.task(allowed), value).result();
    if (allowedOutcome.kind !== "succeeded" || allowedOutcome.value !== value + 1) return false;
    try { child.spawn(child.task(denied), value); return false; }
    catch { return true; }
  });
  const tool = await defineTool({ name: "child-tool", revision: "1", description: "child tool",
    inputSchema: { type: "null" }, outputSchema: { type: "number" },
    parseInput: parseNull, projection: jsonProjection, parseOutput: parseNumber }, () => 3);
  const owner = await Harness.builder(contracts).task(allowed).task(denied).task(nested).tool(tool)
    .grant("task:spawn:allowed-child@1", "task:spawn:denied-child@1", "tool:call:child-tool").build();
  const narrowed = await owner.scoped(new ExecutionScope().onlyGrants("task:spawn:allowed-child@1"));
  const context = new TaskContext(narrowed, new AbortController().signal);
  const allowedRef = context.task(allowed);
  const deniedRef = new TaskContext(owner, new AbortController().signal).task(denied);
  expect(allowedRef).toMatchObject({ name: "allowed-child", revision: "1" });
  expect("implementation" in allowedRef).toBe(false);
  expect(() => context.task(denied)).toThrow();
  expect(() => context.tool(tool)).toThrow();
  expect(await narrowed.spawn(nested, 1).result()).toEqual({ kind: "succeeded", value: true });
  expect(await context.spawn(allowedRef, 1).result()).toEqual({ kind: "succeeded", value: 2 });
  expect(() => context.spawn(deniedRef, 1)).toThrow();
  const group = context.group<number>({ kind: "collect-all" });
  const blocked = await group.spawnMany(deniedRef, { id: "batch:blocked" as BatchId, inputs: [1] });
  expect(blocked[0]?.admission).toMatchObject({ kind: "rejected", reason: { code: "unsupported" } });
  await expect(group.reconcileBatch(deniedRef, { id: "batch:blocked" as BatchId, inputs: [1] }))
    .rejects.toThrow();
  const admitted = await group.spawnMany(allowedRef, { id: "batch:allowed" as BatchId, inputs: [2] });
  expect(admitted[0]?.admission.kind).toBe("accepted");
  const durable = new TaskContext(narrowed, new AbortController().signal,
    "task:parent" as TaskId, true);
  expect(() => durable.admit(deniedRef, 1, "operation:blocked"))
    .toThrow();
  expect(owner.task(denied)).toBe(denied);
  expect(owner.tool(tool).definition.name).toBe("child-tool");
  expect(await owner.spawn(denied, 1).result()).toEqual({ kind: "succeeded", value: 101 });
});

test("routes typed tools through policy and interaction boundaries", async () => {
  const lengths = await defineTool<{ readonly text: string }, number>({
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
    projection: jsonProjection, parseOutput: parseNumber,
  }, (_context, input) => input.text.length);
  const runtime = await Harness.builder(contracts)
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
  const tool = await numberTool("approved-action", "pinned action", (_context, value) => value);
  const seen: number[][] = [];
  const route = async (interaction: import("../src/index.js").Interaction) => {
    if (interaction.kind !== "approval") throw new TypeError("expected an exact approval request");
    seen.push([...interaction.actionDigest]);
    return { kind: "approved" as const };
  };
  const builder = Harness.builder(contracts).tool(tool).grant("tool:call:approved-action")
    .policy({ identity: () => approvalPolicyIdentity,
      evaluate: async () => ({ kind: "require-approval", prompt: "approve action" }) });
  const approved = await builder.interactions({ route }).build();
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
  const unapproved = await builder.interactions({ route: async () => ({ kind: "accepted", text: "not an approval" }) }).build();
  await expect(unapproved.call(unapproved.tool(tool), 3)).rejects.toMatchObject({ kind: "invalid_response" });
  for (const kind of ["declined", "cancelled", "expired", "denied"] as const) {
    const rejected = await builder.interactions({ route: async () => ({ kind }) }).build();
    await expect(rejected.call(rejected.tool(tool), 3)).rejects.toMatchObject({ kind });
  }
  const unresolved = await Harness.builder(contracts).tool(tool).grant("tool:call:approved-action")
    .policy({ identity: () => approvalPolicyIdentity,
      evaluate: async () => ({ kind: "require-approval", prompt: "approve action" }) }).build();
  await expect(unresolved.call(unresolved.tool(tool), 3)).rejects.toMatchObject({ kind: "indeterminate" });
  const mismatched = await builder.interactions({ route: async () => ({ kind: "indeterminate", operationId: "wrong" }) }).build();
  await expect(mismatched.call(mismatched.tool(tool), 3)).rejects.toThrow();
});

test("scoped policies require separate exact approvals for each pinned layer", async () => {
  let executed = 0;
  const prompts: string[] = [];
  const digests: number[][] = [];
  let declineSecond = true;
  const tool = await numberTool("two-approvals", "two approvals", (_context, value) => { executed++; return value; });
  const root = await Harness.builder(contracts).tool(tool).grant("tool:call:two-approvals")
    .policy({ identity: () => approvalPolicyIdentity,
      evaluate: async () => ({ kind: "require-approval", prompt: "parent approval" }) })
    .interactions({ route: async interaction => {
      if (interaction.kind !== "approval") throw new TypeError("unexpected interaction");
      prompts.push(interaction.prompt);
      digests.push([...interaction.actionDigest]);
      return declineSecond && interaction.prompt === "child approval"
        ? { kind: "declined" } : { kind: "approved" };
    } }).build();
  const scoped = await root.scoped(new ExecutionScope().policy({ identity: () => scopedPolicyIdentity,
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

test("durable hosts reject unpinned or composition-mismatched policies", async () => {
  await expect(policyIdentity("policy", "1", new Uint8Array(32))).rejects.toThrow();
  await expect(policyIdentity("policy", "1", new Uint8Array(31).fill(1))).rejects.toThrow();
  const host: HarnessRuntimeSpawner & HarnessRuntimeState = {
    policyIdentity: () => approvalPolicyIdentity,
    async attach() { throw new Error("not used"); },
    async reconcileEffect() { return { state: "indeterminate" }; },
    async send(message) { return { accepted: true, messageId: message.id }; },
    async *inbox() { yield* []; },
  }
  await expect(Harness.builder(contracts).state(host).spawner(host).build()).rejects.toThrow();
  await expect(Harness.builder(contracts).state(host).spawner(host).policy({ identity: () => denyPolicyIdentity,
    evaluate: async () => ({ kind: "deny", reason: "denied" }) }).build()).rejects.toThrow();
  const tool = await defineTool<null, number>({ name: "policy-probe", revision: "1", description: "probe",
    inputSchema: {}, outputSchema: { type: "number" }, parseInput: parseNull, projection: jsonProjection, parseOutput: parseNumber }, () => 1);
  const runtime = await Harness.builder(contracts).state(host).spawner(host).tool(tool).grant("tool:call:policy-probe")
    .policy({ identity: () => approvalPolicyIdentity, evaluate: async () => ({ kind: "allow" }) }).build();
  const scoped = await runtime.scoped(new ExecutionScope().policy({ identity: () => scopedPolicyIdentity,
    evaluate: async () => ({ kind: "deny", reason: "child" }) }));
  await expect(scoped.call(scoped.tool(tool), null)).rejects.toThrow();
});

test("snapshots caller-owned policy identity before async Rust admission", async () => {
  const identity = {
    name: "mutable-policy",
    version: "1",
    digest: Array.from({ length: 32 }, () => 9) as unknown as ComponentIdentity["digest"],
  };
  const policy = {
    identity: () => identity,
    evaluate: async () => ({ kind: "allow" as const }),
  };
  const building = Harness.builder(contracts).policy(policy).build();
  identity.name = "mutated-before-rust-admission";
  const runtime = await building;
  expect(runtime.durablePolicyIdentity()).toEqual({
    name: "mutable-policy",
    version: "1",
    digest: Array.from({ length: 32 }, () => 9) as unknown as ComponentIdentity["digest"],
  });
});

test("snapshots policy and execution identities during runtime rechecks", async () => {
  const identityWithMutation = (name: string, mutation: string): ComponentIdentity => {
    const identity = {
      name,
      version: "1",
      digest: Array.from({ length: 32 }, () => 6),
    } as unknown as ComponentIdentity & { name: string };
    queueMicrotask(() => { identity.name = mutation; });
    return identity;
  };
  const tool = await numberTool("admission-snapshot", "admission snapshot", (_context, value) => value);
  const policy = {
    identity: () => identityWithMutation("runtime-policy", "mutated-policy"),
    evaluate: async () => ({ kind: "allow" as const }),
  };
  const runtime = await Harness.builder(contracts).policy(policy).tool(tool)
    .grant("tool:call:admission-snapshot").build();
  expect(await runtime.call(runtime.tool(tool), 7)).toBe(7);

  let routeCalls = 0;
  const executionIdentity = () => identityWithMutation("execution-route", `mutated-route-${++routeCalls}`);
  const state: HarnessRuntimeState = {
    policyIdentity: () => null,
    executionIdentity,
    async attach() { throw new Error("not used"); },
    async reconcileEffect() { return { state: "indeterminate" }; },
    async send(message) { return { accepted: true, messageId: message.id }; },
    async *inbox() { yield* []; },
  };
  const spawner: HarnessRuntimeSpawner = { policyIdentity: () => null, executionIdentity };
  const execution: HarnessExecutionProvider = {
    identity: executionIdentity,
    spawner: () => spawner,
    state: () => state,
    async qualifyTask() { throw new Error("not used"); },
    async qualifyBatch() { throw new Error("not used"); },
  };
  const executionRuntime = await Harness.builder(contracts).execution(execution).build();
  const placement = {
    provider: {
      name: "execution-route",
      version: "1",
      digest: Array.from({ length: 32 }, () => 6),
    },
    build: { kind: "artifact" as const,
      provider: { namespace: "test", family: "objects", version: "1" }, key: [1], version: null },
    environment: { kind: "sandbox" as const,
      provider: { namespace: "test", family: "machines", version: "1" }, key: [2], version: null },
    readiness_revision: Array.from({ length: 32 }, () => 3),
  } as import("../src/index.js").WasmExecutionPlacementWire;
  await expect(executionRuntime.validateExecutionPlacement(placement)).resolves.toBeUndefined();
});

test("snapshots nested policy identities before parent admission can yield", async () => {
  let childCalls = 0;
  const childPolicy = {
    identity: () => {
      const identity = {
        name: "mutable-child",
        version: "1",
        digest: Array.from({ length: 32 }, () => 7),
      } as unknown as ComponentIdentity & { name: string };
      queueMicrotask(() => { identity.name = `mutated-child-${++childCalls}`; });
      return identity;
    },
    evaluate: async () => ({ kind: "allow" as const }),
  };
  const tool = await numberTool("nested-admission-snapshot", "nested admission snapshot", (_context, value) => value);
  const runtime = await Harness.builder(contracts).policy({
    identity: () => approvalPolicyIdentity,
    evaluate: async () => ({ kind: "allow" as const }),
  }).tool(tool).grant("tool:call:nested-admission-snapshot").build();
  const scoped = await runtime.scoped(new ExecutionScope().policy(childPolicy));
  expect(await scoped.call(scoped.tool(tool), 9)).toBe(9);
});

test("policy implementation drift during evaluation cannot dispatch a tool", async () => {
  let current = approvalPolicyIdentity;
  let executed = false;
  const tool = await numberTool("pinned-evaluation", "pinned", (_context, value) => {
    executed = true;
    return value;
  });
  const runtime = await Harness.builder(contracts).tool(tool).grant("tool:call:pinned-evaluation")
    .policy({ identity: () => current, evaluate: async () => {
      current = scopedPolicyIdentity;
      return { kind: "allow" };
    } }).build();
  await expect(runtime.call(runtime.tool(tool), 1)).rejects.toThrow();
  expect(executed).toBe(false);
});

test("typed interactions validate answer JSON before granting a result type", async () => {
  const observed: unknown[] = [];
  const runtime = await Harness.builder(contracts).interactions({ route: async interaction => {
    observed.push(interaction);
    return { kind: "answered", value: 4 };
  } }).build();
  const context = new TaskContext(runtime, new AbortController().signal);
  const answer = await context.interactTyped({ id: "question:typed", kind: "question", prompt: "Count?" }, numberSchema);
  expect(answer).toEqual({ kind: "answered", value: 4 });
  expect(observed[0]).toMatchObject({ responseSchema: { type: "number" } });
  const invalid = await Harness.builder(contracts).interactions({ route: async () => ({ kind: "answered", value: "four" }) }).build();
  await expect(new TaskContext(invalid, new AbortController().signal)
    .interactTyped({ id: "question:invalid", kind: "question", prompt: "Count?" }, numberSchema))
    .rejects.toThrow();
});

test("nested scopes preserve inherited grants, limits, and policy", async () => {
  const observed: EffectiveScope[] = [];
  const tool = await defineTool<null, string>({ name: "guarded", revision: "1", description: "guarded", inputSchema: {}, outputSchema: {}, parseInput: parseNull, projection: jsonProjection, parseOutput: parseString }, () => "called");
  const deadline = new Date(Date.now() + 60_000);
  const parentPolicy = {
    identity: () => denyPolicyIdentity,
    async evaluate(_invocation: unknown, scope: EffectiveScope) {
      observed.push(scope);
      return { kind: "deny", reason: "parent denied" } as const;
    },
  };
  const runtime = await (await Harness.builder(contracts).policy(parentPolicy).tool(tool).grant("read", "write", "tool:call:guarded").build()).scoped(
    new ExecutionScope().withLimits({ concurrency: 2, deadline, maxSteps: 12 }),
  );
  const child = await runtime.scoped(new ExecutionScope().policy({ identity: () => scopedPolicyIdentity,
    evaluate: async () => ({ kind: "allow" }) }));

  await expect(child.call(child.tool("guarded"), null)).rejects.toThrow();
  expect(observed).toEqual([{ grants: ["read", "write", "tool:call:guarded"], limits: { concurrency: 2, deadline, maxSteps: 12 } }]);
  const withoutToolGrant = await runtime.scoped(new ExecutionScope().onlyGrants("read"));
  await expect(withoutToolGrant.call(withoutToolGrant.tool("guarded"), null))
    .rejects.toThrow();
  await expect(runtime.scoped(new ExecutionScope().grant("admin"))).rejects.toThrow();
  expect((await runtime.scoped(new ExecutionScope().onlyGrants())).scope.grants).toEqual([]);
  await expect(runtime.scoped(new ExecutionScope().withLimits({ concurrency: 3 }))).rejects.toThrow();
  await expect(runtime.scoped(new ExecutionScope().withLimits({ maxSteps: 13 }))).rejects.toThrow();
  expect(() => new HarnessBuilder(contracts).limits({ model_steps: 0 })).toThrow();
  await expect(runtime.scoped(new ExecutionScope().withLimits({ deadline: new Date(deadline.getTime() + 1) }))).rejects.toThrow();
  const grantless = await (await Harness.builder(contracts).tool(tool).build()).scoped(new ExecutionScope());
  await expect(grantless.call(grantless.tool("guarded"), null)).rejects.toThrow();
  await expect(grantless.scoped(new ExecutionScope().grant("admin"))).rejects.toThrow();
  const directlyRestricted = await Harness.builder(contracts).grant("read").build();
  await expect(directlyRestricted.scoped(new ExecutionScope().grant("admin"))).rejects.toThrow();
  expect(() => Reflect.construct(AgentHarness, [new Map(), new Map(), {}, new ExecutionScope().grant("read"), new Map(), new Map()]))
    .toThrow();
});

test("keeps rejected batch admissions explicit after close", async () => {
  const task = await TaskDefinition.live<number, number>("identity", "1", async (_context, value) => value);
  const group = (await Harness.builder(contracts).task(task).build()).group<number>({ kind: "collect-all" });
  group.close();
  const entries = await group.spawnMany(task, { id: "batch:test" as BatchId, inputs: [1, 2] });

  expect(entries.map(entry => entry.admission.kind)).toEqual(["rejected", "rejected"]);
});

test("pins live batch requests and never starts a duplicate entry", async () => {
  let runs = 0;
  const task = await TaskDefinition.live<number, number>("live-batch", "1", async (_context, value) => {
    runs++;
    return value;
  });
  const group = (await Harness.builder(contracts).task(task).build()).group<number>({ kind: "collect-all" },
    "group:live-stable" as GroupId);
  const batch = { id: "batch:live-stable" as BatchId, inputs: [1, 2] };
  const first = await group.spawnMany(task, batch);
  const repeated = await group.spawnMany(task, batch);
  expect(repeated.map(entry => entry.admission)).toEqual(first.map(entry => entry.admission));
  await expect(group.spawnMany(task, { id: batch.id, inputs: [1, 3] }))
    .rejects.toThrow();
  await group.join();
  expect(runs).toBe(2);
});

test("streams thousands of completed tasks once without rebuilding pending races", async () => {
  const task = await TaskDefinition.live<number, number>("bulk", "1", async (_context, value) => value);
  const group = (await Harness.builder(contracts).task(task).build()).group<number>({ kind: "collect-all" });
  const inputs = Array.from({ length: 2_048 }, (_value, index) => index);
  await group.spawnMany(task, { id: "batch:bulk" as BatchId, inputs: inputs });
  const completed: number[] = [];
  for await (const entry of group.asCompleted()) {
    if (entry.outcome?.kind === "succeeded") completed.push(entry.outcome.value);
  }
  expect(completed.length).toBe(inputs.length);
  expect(completed.sort((left, right) => left - right)).toEqual(inputs);
});

test("cancels hanging siblings as soon as a cancel-on-failure group fails", async () => {
  let aborted = false;
  const task = await TaskDefinition.live<"fail" | "hang", string>("work", "1", async (context, value) => {
    if (value === "fail") throw new Error("failed");
    context.signal.addEventListener("abort", () => { aborted = true; }, { once: true });
    await context.sleepUntil(new Date(Date.now() + 60_000));
    return "late";
  });
  const runtime = (await Harness.builder(contracts).task(task).build());
  await expect(runtime.group<string>({ kind: "cancel-on-failure" }).map<"fail" | "hang">(task, ["hang", "fail"])).rejects.toThrow();
  expect(aborted).toBeTrue();
});

test("observes cancellation when a wait starts after its signal was aborted", async () => {
  const runtime = await Harness.builder(contracts).build();
  const controller = new AbortController();
  controller.abort(new Error("already cancelled"));
  const task = new Task("task:pre-aborted" as TaskId, signal =>
    new TaskContext(runtime, signal).sleepUntil(new Date(Date.now() + 60_000)), controller);

  expect(await task.result()).toMatchObject({ kind: "cancelled" });
});

test("race returns the first terminal outcome; firstSuccess ignores early failures", async () => {
  const task = await TaskDefinition.live<{ readonly delay: number; readonly value?: string }, string>("race", "1", async (context, input) => {
    await context.sleepUntil(new Date(Date.now() + input.delay));
    if (input.value === undefined) throw new Error("early failure");
    return input.value;
  });
  const runtime = (await Harness.builder(contracts).task(task).build());
  const group = runtime.group<string>({ kind: "collect-all" });
  await group.spawnMany(task, { id: "batch:race" as BatchId, inputs: [{ delay: 0 }, { delay: 10, value: "winner" }, { delay: 50, value: "late" }] });
  const first = await group.race();
  expect(first.outcome?.kind).toBe("failed");
  expect((await group.join()).entries.at(-1)?.outcome).toEqual({ kind: "succeeded", value: "late" });
  const succeeding = runtime.group<string>({ kind: "collect-all" });
  await succeeding.spawnMany(task, { id: "batch:first-success" as BatchId, inputs: [{ delay: 0 }, { delay: 10, value: "winner" }, { delay: 50, value: "late" }] });
  expect((await succeeding.firstSuccess()).value).toBe("winner");
  expect((await succeeding.join()).entries.at(-1)?.outcome).toEqual({ kind: "succeeded", value: "late" });
});

test("tool registration snapshots its public definition", async () => {
  const mutable = { name: "echo", revision: "1", description: "first", inputSchema: {}, outputSchema: {}, parseInput: parseNull, projection: jsonProjection, parseOutput: parseString,
    handler: async () => "ok" };
  const runtime = await Harness.builder(contracts).tool(mutable).build();
  mutable.name = "different";
  mutable.description = "changed";
  expect(runtime.tool("echo").definition).toMatchObject({ name: "echo", description: "first" });
});

test("reused provider call IDs have distinct internal tool operations", async () => {
  const operations: string[] = [];
  const argumentsSeen: number[] = [];
  const tool = await defineTool<number, number>({ name: "again", revision: "1", description: "again",
    inputSchema: {}, outputSchema: {}, parseInput: parseNumber, projection: jsonProjection, parseOutput: parseNumber }, async (context, input) => {
    expect(context.callId).toBe("provider-call");
    operations.push(context.operationId);
    argumentsSeen.push(input);
    return input;
  });
  let step = 0;
  const runtime = await Harness.builder(contracts).tool(tool).grant("tool:call:again").model(testModel, {
    ...syntheticAccounting(contracts), async *generate() {
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
  expect(argumentsSeen).toEqual([1, 2]);
});

test("file batches check every scope and bound before the first owner read", async () => {
const first = await mailboxFile(1);
const denied = { ...first, path: "denied" };
let reads = 0;
const content = { validate: () => {},
  read: async () => { reads++; return Uint8Array.of(0); },
  fileReadCapability: (file: FileRef) => `read:${file.path}`,
  volumeReadCapability: () => "read:volume", directoryReadCapability: () => "read:directory" };
const runtime = await Harness.builder(contracts).content(content).grant(content.fileReadCapability(first)).build();
const context = new TaskContext(runtime, new AbortController().signal);
await expect(context.readFiles([first, denied])).rejects.toThrow();
expect(reads).toBe(0);
expect(await context.readFiles([first])).toEqual([Uint8Array.of(0)]);
expect(reads).toBe(1);
});
