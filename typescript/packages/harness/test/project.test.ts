import { expect, test } from "bun:test";
import { DEFAULT_LIMITS, ExecutionScope, Harness, NativeContracts, ParentProjectController, providerOperationId, resourceRef,
  type Authority, type ConversationMessageId, type OperationId, type ParentProjectBinding,
  type ProjectMergeReceipt, type ProjectWorkspaceProvider, type ResourceRef } from "../src/index.js";

const provider = { namespace: "test", family: "filesystem", version: "2" } as const;
const owner = { kind: "project", id: "project" } as const;
const parent = { provider, id: "root", class: "project", owner } as const;
const child = { provider, id: "child", class: "project", owner } as const;
const generation = (byte: number): ResourceRef<"generation"> => ({ kind: "generation", provider, key: [byte], version: null });
const contracts = await NativeContracts.create();

test("project workspace bindings need authority and scoped views cannot regain them", () => {
  const workspaces: ProjectWorkspaceProvider = {
    provider, project: parent,
    async forkProject() { throw new Error("unused"); },
    async prepareProjectMerge() { throw new Error("unused"); },
  };
  expect(() => Harness.builder(contracts).workspaces(workspaces).build()).toThrow("project workspaces require");
  const runtime = Harness.builder(contracts).bindings({ workspaces, grants: ["project:merge"] }).build();
  expect(runtime.projectWorkspaces()).toBe(workspaces);
  expect(() => runtime.scoped(ExecutionScope.create().onlyGrants()).projectWorkspaces()).toThrow("lacks project authority");
  expect(() => runtime.scoped(ExecutionScope.create().onlyGrants("project:merge")).projectWorkspaces()).toThrow("not bound");
});

test("Rust and TypeScript share the canonical v2 project merge receipt", async () => {
  const fixture = (await Bun.file(new URL("../../../../fixtures/harness/v2/project-merge-receipt.json", import.meta.url)).text()).trim();
  const decoded = JSON.parse(fixture) as ProjectMergeReceipt;
  const fixtureSequence: unknown = decoded.notice.sequence;
  if (typeof fixtureSequence !== "number" || !Number.isSafeInteger(fixtureSequence)) {
    throw new TypeError("golden merge notice sequence is not an exact JSON integer");
  }
  const receipt: ProjectMergeReceipt = {
    ...decoded,
    source_project: contracts.validate("volume_ref", decoded.source_project),
    source_generation: await resourceRef(decoded.source_generation),
    target_project: contracts.validate("volume_ref", decoded.target_project),
    expected_target_generation: await resourceRef(decoded.expected_target_generation),
    result_generation: await resourceRef(decoded.result_generation),
    provider_operation_id: providerOperationId(decoded.provider_operation_id),
    provider_proof: decoded.provider_proof,
    notice: { ...decoded.notice, sequence: BigInt(fixtureSequence) },
  };
  expect(receipt.child.kind).toBe("conversation");
  expect(receipt.source_project.class).toBe("project");
  expect(receipt.target_project.class).toBe("project");
  expect(receipt.notice.kind).toBe("merge");
  expect(receipt.provider_operation_id).toHaveLength(16);
  expect(receipt.provider_proof.provider).toEqual(receipt.target_project.provider);
  (await NativeContracts.create()).validate("conversation_message", receipt.notice, DEFAULT_LIMITS);
  expect(JSON.stringify(receipt, (_key, value: unknown) => {
    if (typeof value !== "bigint") return value;
    const exact = Number(value);
    if (!Number.isSafeInteger(exact)) throw new RangeError("golden JSON cannot encode an unsafe u64");
    return exact;
  })).toBe(fixture);
});

test("project fork and merge publication remain bound to the parent controller", async () => {
  let granted = true;
  let publications = 0;
  let notices = 0;
  const mergeOperation = "01010101-0101-0101-0101-010101010101" as OperationId;
  const raw = Object.freeze({ secret: "raw-plan" });
  const binding: ParentProjectBinding<typeof raw, string, "theirs", string, readonly string[]> = {
    parent: { kind: "conversation", id: "parent" }, project: parent,
    async authorize(capability, operation) {
      if (!granted || (capability === "project:merge" && operation !== "write")
        || (capability === "fork:publish" && operation !== "read")) throw new Error("parent grant missing");
    },
    async forkProject(source, selected) {
      expect(source).toEqual(generation(1));
      expect(selected).toEqual(child);
      return generation(2);
    },
    async prepareProjectMerge(selected) {
      expect(selected).toEqual(child);
      return { plan: raw, source: generation(2), target: generation(1), commonAncestor: generation(0) };
    },
    async describeProjectMergeConflicts(plan, conflicts) {
      expect(plan).toBe(raw);
      return [...conflicts];
    },
    async applyProjectMerge(plan, target, operationId, resolution) {
      expect(plan).toBe(raw);
      expect(target).toEqual(generation(1));
      expect(operationId).toBe(mergeOperation);
      expect(resolution).toBe("theirs");
      publications++;
      return "applied";
    },
    async publishProjectMergeReceipt(receipt) {
      expect(receipt.notice.kind).toBe("merge");
      notices++;
    },
  };
  const controller = new ParentProjectController(contracts, binding);
  expect(() => providerOperationId([])).toThrow("1–64");
  expect(await controller.forkProject(generation(1), child, "fork-1")).toEqual(generation(2));
  const plan = await controller.prepareProjectMerge(child);
  expect(Object.keys(plan)).toEqual(["child", "source", "target", "commonAncestor"]);
  expect(await controller.describeProjectMergeConflicts(plan, ["conflict"], false)).toEqual(["conflict"]);
  const other = new ParentProjectController(contracts, binding);
  await expect(other.applyProjectMerge(plan, mergeOperation, "theirs")).rejects.toThrow("another parent controller");
  await expect(controller.applyProjectMerge({ ...plan }, mergeOperation, "theirs")).rejects.toThrow("another parent controller");
  granted = false;
  await expect(controller.applyProjectMerge(plan, mergeOperation, "theirs")).rejects.toThrow("parent grant missing");
  expect(publications).toBe(0);
  granted = true;
  expect(await controller.applyProjectMerge(plan, mergeOperation, "theirs")).toBe("applied");
  await expect(controller.applyProjectMerge(plan, "03030303-0303-0303-0303-030303030303" as OperationId, "theirs"))
    .rejects.toThrow("another operation");
  expect(publications).toBe(1);
  const receipt: ProjectMergeReceipt = {
    operation_id: mergeOperation,
    child: { kind: "conversation", id: "child-conversation" },
    source_project: child, source_generation: plan.source,
    target_project: parent, expected_target_generation: plan.target,
    result_generation: generation(3), provider_operation_id: providerOperationId(Array(16).fill(1)),
    provider_proof: { provider, format: "filesystem.join.v2", statement: { generation: "3" } },
    notice: {
      id: "02020202-0202-0202-0202-020202020202" as ConversationMessageId, sequence: 1n, kind: "merge",
      content: { volume: parent, path: "notices/merge.txt", version: "1",
        descriptor: { sha256: Array(32).fill(1), byte_length: 0, media_type: "text/plain" }, display_name: "merge.txt" },
      attachments: { kind: "inline", items: [] }, reply_to: null, tool_call_id: null, extensions: {},
    },
  };
  let proofReads = 0;
  const unstableProof = Object.defineProperty({}, "unsafe", {
    enumerable: true,
    get: () => ++proofReads === 1 ? 1 : Number.POSITIVE_INFINITY,
  }) as Readonly<{ unsafe: number }>;
  let admittedReceipt: ProjectMergeReceipt | undefined;
  try {
    admittedReceipt = contracts.validate("project_merge_receipt", {
      ...receipt,
      provider_proof: { ...receipt.provider_proof, statement: unstableProof },
    });
  } catch { /* Rejecting an accessor is also safe. */ }
  if (admittedReceipt !== undefined) expect(admittedReceipt.provider_proof.statement).toEqual({ unsafe: 1 });
  expect(proofReads).toBeLessThanOrEqual(1);
  expect(() => contracts.validate("project_merge_receipt", { ...receipt,
    provider_proof: { ...receipt.provider_proof, statement: { unsafe: Number.MAX_SAFE_INTEGER + 1 } },
  })).toThrow();
  await expect(other.publishProjectMergeReceipt(plan, receipt)).rejects.toThrow("another parent controller");
  await expect(controller.publishProjectMergeReceipt(plan, { ...receipt,
    operation_id: "03030303-0303-0303-0303-030303030303" as OperationId })).rejects.toThrow("another operation");
  await expect(controller.publishProjectMergeReceipt(plan, { ...receipt,
    expected_target_generation: generation(9) })).rejects.toThrow("inspected parent plan");
  await expect(controller.publishProjectMergeReceipt(plan, { ...receipt,
    provider_proof: { ...receipt.provider_proof, provider: { ...provider, namespace: "foreign" } } })).rejects.toThrow("inconsistent identities");
  await expect(controller.publishProjectMergeReceipt(plan, { ...receipt,
    provider_proof: { ...receipt.provider_proof, statement: { unsafe: Number.POSITIVE_INFINITY } } })).rejects.toThrow();
  await expect(controller.publishProjectMergeReceipt(plan, { ...receipt,
    provider_operation_id: [] as unknown as typeof receipt.provider_operation_id })).rejects.toThrow();
  await expect(controller.publishProjectMergeReceipt(plan, { ...receipt,
    notice: { ...receipt.notice, kind: "assistant" } as unknown as typeof receipt.notice })).rejects.toThrow();
  expect(notices).toBe(0);
  await controller.publishProjectMergeReceipt(plan, receipt);
  expect(notices).toBe(1);
  expect(() => new ParentProjectController(contracts, { ...binding,
    parent: { kind: "agent", id: "child" } as unknown as Authority<"conversation"> })).toThrow("parent conversation");
  await expect(controller.prepareProjectMerge(parent)).rejects.toThrow("outside the parent project lineage");
});
