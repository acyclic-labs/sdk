import { test, expect } from "bun:test";
import { type TaskId, type HarnessRuntimeState, policyIdentity, Harness, type WasmTaskAdmissionWire, type WorkflowAdmissionWire, type WasmNativeLimitsWire, DEFAULT_LIMITS } from "../src/index.js";
import { contracts } from "./support/runtime-fixture.js";
import { HARNESS_CHILD_PAGE_DEFAULT } from "../src/child-page-contract.js";

test("Rust owns child page bounds, slot ordering, and the generated facade default", async () => {
const parent = "12345678-1234-4234-8234-123456789abc" as TaskId;
const child = "22345678-1234-4234-8234-123456789abc" as TaskId;
const page = { revision: 7n, entries: [{ slot: "a", taskId: child }], nextAfter: null } as const;
expect(contracts.validateTaskChildrenPage(parent, 7n, null, 1_024, page)).toEqual(page);
let revisionReads = 0;
const changingRevisionPage = {
  get revision() {
    revisionReads += 1;
    return revisionReads === 1 ? 7n : 7;
  },
  entries: page.entries,
  nextAfter: null,
} as unknown as typeof page;
expect(contracts.validateTaskChildrenPage(parent, 7n, null, 1, changingRevisionPage)).toEqual(page);
expect(revisionReads).toBe(1);
expect(() => contracts.validateTaskChildrenPage(parent, 6n, null, 1, page)).toThrow();
expect(() => contracts.validateTaskChildrenPage(parent, 7n, "a", 1, page)).toThrow();
expect(contracts.validateTaskChildrenPage(parent, 7n, null, 1_000_001, page)).toEqual(page);
expect(() => contracts.validateTaskChildrenPage(parent, 7n, null, 0, page)).toThrow();
expect(() => contracts.validateTaskChildrenPage(parent, 7n, null, 1, {
  ...page, entries: [{ slot: "a", taskId: child }, { slot: "b", taskId: child }],
})).toThrow();
expect(() => contracts.validateTaskChildrenPage(parent, 7n, null, 1, {
  ...page, nextAfter: "b",
})).toThrow();
expect(() => contracts.validateTaskChildrenPage(parent, 7n, null, 1, {
  ...page, entries: [{ slot: "\u007f", taskId: child }],
})).toThrow();

let observedMaximum = 0;
const state: HarnessRuntimeState = {
  policyIdentity: () => null,
  async attach() { throw new Error("not used"); },
  async children(_parent, _revision, _after, maximum) {
    observedMaximum = maximum;
    return page;
  },
  async reconcileEffect() { return { state: "indeterminate" }; },
  async send(message) { return { accepted: true, messageId: message.id }; },
  async *inbox() { yield* []; },
};
const runtime = await Harness.builder(contracts).state(state).build();
expect(await runtime.children(parent)).toEqual(page);
expect(observedMaximum).toBe(HARNESS_CHILD_PAGE_DEFAULT);

const malformedRuntime = await Harness.builder(contracts).state({
  ...state,
  async children() {
    return { ...page, revision: 7 as unknown as bigint };
  },
}).build();
await expect(malformedRuntime.children(parent)).rejects.toBeInstanceOf(TypeError);
});

test("Rust and TypeScript share strict v2 task admission and execution placement fixtures", async () => {
const admissionText = (await Bun.file(new URL("../../../../fixtures/harness/v2/task-admission.json", import.meta.url)).text()).trim();
const admission = JSON.parse(admissionText) as WasmTaskAdmissionWire;
expect(new TextDecoder().decode(contracts.encodeCanonicalJson(contracts.validate("task_admission", admission))))
  .toBe(admissionText);
expect(() => contracts.validate("task_admission", { ...admission, input: "wrong" })).toThrow();
const placementText = (await Bun.file(new URL("../../../../fixtures/harness/v2/execution-placement.json", import.meta.url)).text()).trim();
const placement = JSON.parse(placementText) as import("../src/index.js").WasmExecutionPlacementWire;
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

test("Rust owns durable task and batch projection identities", async () => {
const limits: WasmNativeLimitsWire = {
  file_bytes: BigInt(DEFAULT_LIMITS.file_bytes), path_bytes: BigInt(DEFAULT_LIMITS.path_bytes),
  attachments: BigInt(DEFAULT_LIMITS.attachments), render_bytes: BigInt(DEFAULT_LIMITS.render_bytes),
  model_steps: BigInt(DEFAULT_LIMITS.model_steps), model_events_per_step: BigInt(DEFAULT_LIMITS.model_events_per_step),
  tool_calls_per_step: BigInt(DEFAULT_LIMITS.tool_calls_per_step), context_messages: BigInt(DEFAULT_LIMITS.context_messages),
};
const common = {
  name: "projection.task",
  version: "1",
  input_schema: { type: "integer" },
  output_schema: { type: "integer" },
  requirements: [],
  machine_digest: Array(32).fill(9),
  parent: null,
  resource_proofs: [],
  grants: [],
  limits,
  run_limits: { concurrency: null, max_steps: null, deadline_epoch_ms: null },
  policy: null,
  extensions: null,
  execution: null,
} as const;
const identities = contracts.taskAdmissionIdentities({
  name: common.name, version: common.version, input_schema: common.input_schema,
  output_schema: common.output_schema, requirements: common.requirements,
  machine_digest: common.machine_digest,
});
const task = contracts.admitTask({
  ...common,
  operation_id: "12345678-1234-4234-8234-123456789abc",
  input: 4,
});
expect(task.task).toEqual(identities.task);
expect(task.machine).toEqual(identities.machine);
expect(contracts.validate("task_admission", task)).toEqual(task);

const batchInput = {
  ...common,
  group_id: "22345678-1234-4234-8234-123456789abc",
  batch_id: "32345678-1234-4234-8234-123456789abc",
  group_policy: "collect-all",
  inputs: [4, 5],
} as const;
const batch = contracts.admitBatch(batchInput);
expect(contracts.validate("durable_batch_request", batch)).toEqual(batch);
expect(batch.inputs).toEqual([4n, 5n]);
expect(contracts.batchMemberOperationId(batch.group_id, batch.batch_id, 0))
  .not.toBe(contracts.batchMemberOperationId(batch.group_id, batch.batch_id, 1));
const projected = contracts.admitBatchRequest(batchInput);
expect(projected.canonical).toEqual(batch);
expect(projected.policy).toEqual({ kind: "collect-all" });
expect(projected.members).toHaveLength(2);
expect(contracts.batchMemberOperationId(batch.group_id, batch.batch_id, 65_536))
  .not.toBe(contracts.batchMemberOperationId(batch.group_id, batch.batch_id, 0));
expect(() => contracts.batchMemberOperationId(batch.group_id, batch.batch_id, 0x1_0000_0000))
  .toThrow();
expect(projected.members.map(member => member.operation_id)).toEqual([
  contracts.batchMemberOperationId(batch.group_id, batch.batch_id, 0),
  contracts.batchMemberOperationId(batch.group_id, batch.batch_id, 1),
]);
expect(projected.inputDigest).toEqual(Array.from(contracts.digestCanonicalJson(batch)));
expect(projected.implementationDigest).toBe("09".repeat(32));
expect(() => contracts.admitBatch({ ...common,
  group_id: batch.group_id, batch_id: batch.batch_id, group_policy: "collect-all", inputs: ["wrong"],
})).toThrow();
});
