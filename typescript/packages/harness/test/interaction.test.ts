import { expect, test } from "bun:test";
import { NativeContracts, descriptorFor, type AgentId, type FileRef, type InteractionTicket, type OperationId } from "../src/index.js";

const contracts = await NativeContracts.create();

test("standalone interaction identities and approval bindings use Rust admission", () => {
  expect(String(contracts.validateIdentity("interaction", "01010101010101010101010101010101")))
    .toBe("01010101-0101-0101-0101-010101010101");
  expect(() => contracts.validateIdentity("interaction", "00000000-0000-0000-0000-000000000000")).toThrow();
  const admitted = contracts.validate("approval_binding", {
    operation_id: "02020202-0202-0202-0202-020202020202" as OperationId,
    action_digest: Array(32).fill(3),
  });
  expect(() => contracts.validate("approval_binding", { ...admitted, action_digest: Array(32).fill(0) })).toThrow();
  expect(() => contracts.validate("approval_binding", { ...admitted, extra: "unbound" } as typeof admitted)).toThrow();
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
    id: contracts.validateIdentity("interaction", "01010101-0101-0101-0101-010101010101"),
    kind: "approval",
    request: await reference("interactions/approval.json", new TextEncoder().encode("{}")),
    deadline_unix_ms: 1000n,
    approval: { operation_id: "02020202-0202-0202-0202-020202020202" as OperationId, action_digest: Array(32).fill(3) },
  };
  const admitted = contracts.validate("interaction_ticket", ticket);
  expect(() => contracts.validate("interaction_ticket", { ...ticket, approval: { ...ticket.approval!, action_digest: Array(32).fill(0) } })).toThrow();
  expect(() => contracts.validate("interaction_resolution", { id: ticket.id, expected_version: 1n, outcome: { kind: "answered", answer: ticket.request } }, admitted)).toThrow();
  expect((contracts.validate("interaction_resolution", { id: ticket.id, expected_version: 2n, outcome: { kind: "approved" } }, admitted)).expected_version).toBe(2n);
});

test("question answers stay as validated file refs with distinct terminal outcomes", async () => {
  const ticket = contracts.validate("interaction_ticket", {
    id: contracts.validateIdentity("interaction", "03030303-0303-0303-0303-030303030303"), kind: "question",
    request: await reference("interactions/question.json", new TextEncoder().encode("{}")),
    deadline_unix_ms: null, approval: null,
  });
  const answer = await reference("interactions/answer.json", new TextEncoder().encode('"ok"'));
  expect((contracts.validate("interaction_resolution", { id: ticket.id, expected_version: 1n, outcome: { kind: "answered", answer } }, ticket)).outcome.kind).toBe("answered");
  expect(() => contracts.validate("interaction_resolution", { id: ticket.id, expected_version: 1n, outcome: { kind: "approved" } }, ticket)).toThrow();
  expect(() => contracts.validate("interaction_resolution", { id: ticket.id, expected_version: 1n, outcome: { kind: "expired" } }, ticket)).toThrow();
  expect((contracts.validate("interaction_resolution", { id: ticket.id, expected_version: 1n, outcome: { kind: "indeterminate", operation_id: "04040404-0404-0404-0404-040404040404" as OperationId } }, ticket)).outcome.kind).toBe("indeterminate");
});

test("Rust and TypeScript share the canonical v2 interaction resolution fixture", async () => {
  const fixture = (await Bun.file(new URL("../../../../fixtures/harness/v2/interaction-resolution.json", import.meta.url)).text()).trim();
  const ticket = contracts.validate("interaction_ticket", {
    id: contracts.validateIdentity("interaction", "01010101-0101-0101-0101-010101010101"), kind: "approval",
    request: await reference("interactions/approval.json", new TextEncoder().encode("{}")),
    deadline_unix_ms: null,
    approval: { operation_id: "02020202-0202-0202-0202-020202020202" as OperationId, action_digest: Array(32).fill(3) },
  });
  const parsed = JSON.parse(fixture) as Omit<import("../src/index.js").InteractionResolution, "expected_version"> & { expected_version: number };
  const resolution = contracts.validate("interaction_resolution", { ...parsed, expected_version: BigInt(parsed.expected_version) }, ticket);
  expect(JSON.stringify(resolution, (_key, value: unknown) => typeof value === "bigint" ? Number(value) : value)).toBe(fixture);
});

test("Rust and TypeScript share the canonical v2 interaction receipt fixture", async () => {
  const fixture = (await Bun.file(new URL("../../../../fixtures/harness/v2/resolution-receipt.json", import.meta.url)).text()).trim();
  const parsed = JSON.parse(fixture) as Omit<import("../src/index.js").ResolutionReceipt, "version" | "conversation_revision"> &
    { version: number; conversation_revision: number };
  const receipt = contracts.validate("resolution_receipt", { ...parsed, version: BigInt(parsed.version),
    conversation_revision: BigInt(parsed.conversation_revision) });
  expect(JSON.stringify(receipt, (_key, value: unknown) => typeof value === "bigint" ? Number(value) : value)).toBe(fixture);
  expect(() => contracts.validate("resolution_receipt", { ...receipt, version: 0n })).toThrow();
});
