import { expect, test } from "bun:test";
import { approvalBinding, descriptorFor, interactionId, interactionResolution, interactionTicket, resolutionReceipt, responderGrant, viewerGrant, type AgentId, type FileRef, type InteractionTicket, type OperationId } from "../src/index.js";

test("standalone interaction identities and approval bindings use Rust admission", async () => {
  expect(String(await interactionId("01010101010101010101010101010101")))
    .toBe("01010101-0101-0101-0101-010101010101");
  await expect(interactionId("00000000-0000-0000-0000-000000000000")).rejects.toThrow();
  const admitted = await approvalBinding({
    operation_id: "02020202-0202-0202-0202-020202020202" as OperationId,
    action_digest: Array(32).fill(3),
  });
  expect(Object.isFrozen(admitted.action_digest)).toBe(true);
  await expect(approvalBinding({ ...admitted, action_digest: Array(32).fill(0) })).rejects.toThrow();
  await expect(approvalBinding({ ...admitted, extra: "unbound" } as typeof admitted)).rejects.toThrow();
});

async function reference(path: string, bytes: Uint8Array): Promise<FileRef> {
  return {
    volume: {
      provider: { namespace: "test", family: "filesystem", version: "2" },
      id: "private", class: "agent_private",
      owner: { kind: "agent", id: "08080808-0808-0808-0808-080808080808" as AgentId },
    },
    path, version: "immutable-1", descriptor: await descriptorFor(bytes, "application/json"), display_name: "interaction.json",
  };
}

test("interaction tickets are ref-only and approvals pin one exact action", async () => {
  const ticket: InteractionTicket = {
    id: await interactionId("01010101-0101-0101-0101-010101010101"),
    kind: "approval",
    request: await reference("interactions/approval.json", new TextEncoder().encode("{}")),
    deadline_unix_ms: 1000n,
    approval: { operation_id: "02020202-0202-0202-0202-020202020202" as OperationId, action_digest: Array(32).fill(3) },
  };
  const admitted = await interactionTicket(ticket);
  expect(responderGrant(admitted)).toBe("interaction:respond:01010101-0101-0101-0101-010101010101");
  expect(viewerGrant(admitted)).toBe("interaction:view:01010101-0101-0101-0101-010101010101");
  expect(Object.isFrozen(admitted.approval?.action_digest)).toBe(true);
  expect(Object.isFrozen(admitted.request.volume.provider)).toBe(true);
  await expect(interactionTicket({ ...ticket, approval: { ...ticket.approval!, action_digest: Array(32).fill(0) } })).rejects.toThrow();
  await expect(interactionResolution({ id: ticket.id, expected_version: 1n, outcome: { kind: "answered", answer: ticket.request } }, admitted)).rejects.toThrow();
  expect((await interactionResolution({ id: ticket.id, expected_version: 2n, outcome: { kind: "approved" } }, admitted)).expected_version).toBe(2n);
});

test("question answers stay as validated file refs with distinct terminal outcomes", async () => {
  const ticket = await interactionTicket({
    id: await interactionId("03030303-0303-0303-0303-030303030303"), kind: "question",
    request: await reference("interactions/question.json", new TextEncoder().encode("{}")),
    deadline_unix_ms: null, approval: null,
  });
  const answer = await reference("interactions/answer.json", new TextEncoder().encode('"ok"'));
  expect((await interactionResolution({ id: ticket.id, expected_version: 1n, outcome: { kind: "answered", answer } }, ticket)).outcome.kind).toBe("answered");
  await expect(interactionResolution({ id: ticket.id, expected_version: 1n, outcome: { kind: "approved" } }, ticket)).rejects.toThrow();
  await expect(interactionResolution({ id: ticket.id, expected_version: 1n, outcome: { kind: "expired" } }, ticket)).rejects.toThrow();
  expect((await interactionResolution({ id: ticket.id, expected_version: 1n, outcome: { kind: "indeterminate", operation_id: "04040404-0404-0404-0404-040404040404" as OperationId } }, ticket)).outcome.kind).toBe("indeterminate");
});

test("Rust and TypeScript share the canonical v2 interaction resolution fixture", async () => {
  const fixture = (await Bun.file(new URL("../../../../fixtures/harness/v2/interaction-resolution.json", import.meta.url)).text()).trim();
  const ticket = await interactionTicket({
    id: await interactionId("01010101-0101-0101-0101-010101010101"), kind: "approval",
    request: await reference("interactions/approval.json", new TextEncoder().encode("{}")),
    deadline_unix_ms: null,
    approval: { operation_id: "02020202-0202-0202-0202-020202020202" as OperationId, action_digest: Array(32).fill(3) },
  });
  const parsed = JSON.parse(fixture) as Omit<import("../src/index.js").InteractionResolution, "expected_version"> & { expected_version: number };
  const resolution = await interactionResolution({ ...parsed, expected_version: BigInt(parsed.expected_version) }, ticket);
  expect(JSON.stringify(resolution, (_key, value: unknown) => typeof value === "bigint" ? Number(value) : value)).toBe(fixture);
});

test("Rust and TypeScript share the canonical v2 interaction receipt fixture", async () => {
  const fixture = (await Bun.file(new URL("../../../../fixtures/harness/v2/resolution-receipt.json", import.meta.url)).text()).trim();
  const parsed = JSON.parse(fixture) as Omit<import("../src/index.js").ResolutionReceipt, "version" | "conversation_revision"> &
    { version: number; conversation_revision: number };
  const receipt = await resolutionReceipt({ ...parsed, version: BigInt(parsed.version),
    conversation_revision: BigInt(parsed.conversation_revision) });
  expect(JSON.stringify(receipt, (_key, value: unknown) => typeof value === "bigint" ? Number(value) : value)).toBe(fixture);
  await expect(resolutionReceipt({ ...receipt, version: 0n })).rejects.toThrow();
});
