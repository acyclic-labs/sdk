import { expect, test } from "bun:test";
import { readFileSync } from "node:fs";
import { fileURLToPath } from "node:url";
import { Harness, NativeContracts, parseIdentity, type AgentId, type OperationId } from "../src/index.js";
import { WasmReducer } from "../generated/wasm/acyclic_harness_wasm.js";

const wasm = readFileSync(
  fileURLToPath(new URL("../generated/wasm/acyclic_harness_wasm_bg.wasm", import.meta.url)),
);

test("local Rust contracts and reducer share one default WASM initialization", async () => {
  const harness = await Harness.create({
    authority: { kind: "conversation", id: crypto.randomUUID() },
    issuerId: "default-wasm", issuerKey: new Uint8Array(32).fill(11),
  });
  expect(harness.snapshot().revision).toBe(0n);
  harness.free();
});

test("raw WASM rejects unknown command, action, and snapshot fields", async () => {
  await NativeContracts.create();
  const authority = { kind: "conversation", id: "strict-raw-ingress" };
  const issuerKey = new Uint8Array(32).fill(12);
  const core = new WasmReducer(authority, "strict-raw-ingress", issuerKey, []);
  try {
    const scope = core.issueScope("root", ["conversation:bind"]);
    const command = {
      operation_id: "01010101-0101-0101-0101-010101010101",
      idempotency_key: "strict-raw-ingress",
      expected_revision: 0n,
      scope,
      causal_parent: null,
      action: { kind: "bind_conversation", agent: "02020202-0202-0202-0202-020202020202" },
    };
    expect(() => core.apply({ ...command, unexpected_envelope: true })).toThrow();
    expect(() => core.apply({ ...command,
      action: { ...command.action, unexpected_action: true } })).toThrow();
    expect(core.snapshot().revision).toBe(0n);
    expect(core.apply(command).result).toBe("applied");
    const snapshot = core.snapshot();
    expect(() => WasmReducer.restore({ ...snapshot, unexpected_snapshot: true },
      "strict-raw-ingress", issuerKey, [])).toThrow();
    expect(core.snapshot().revision).toBe(1n);
  } finally { core.free(); }
});

test("Rust-backed identity constructors preserve brands and canonical UUID spelling", async () => {
  const uuid = "01010101-0101-0101-0101-010101010101";
  const agent: AgentId = await parseIdentity("agent", uuid.toUpperCase());
  const operation: OperationId = await parseIdentity("operation", uuid);
  expect(String(agent)).toBe(uuid);
  expect(String(operation)).toBe(uuid);
  await expect(parseIdentity("agent", "not-an-agent")).rejects.toThrow();
  await expect(parseIdentity("operation", "not-an-operation")).rejects.toThrow();
});

test("Rust operation derivation preserves the prior WebCrypto identity and rejects malformed input", async () => {
  const contracts = await NativeContracts.create();
  const operation = await parseIdentity("operation", "11111111-1111-4111-8111-111111111111");
  expect(String(contracts.deriveOperationId(operation, "user-event")))
    .toBe("08f2f698-fb58-54a7-83b9-c0260831a246");
  expect(() => contracts.deriveOperationId("not-a-uuid" as OperationId, "user-event")).toThrow();
});

test("native digest-half identities and file descriptors avoid TypeScript codecs", async () => {
  const contracts = await NativeContracts.create();
  const digest = Uint8Array.from({ length: 32 }, (_value, index) => index + 1);
  expect(String(contracts.idFromDigest(digest, "operation"))).toBe("01020304-0506-0708-090a-0b0c0d0e0f10");
  expect(String(contracts.idFromDigest(digest, "interaction"))).toBe("11121314-1516-1718-191a-1b1c1d1e1f20");
  expect(() => contracts.idFromDigest(new Uint8Array(31), "operation")).toThrow();
  expect(() => contracts.idFromDigest(new Uint8Array(32), "interaction")).toThrow();
  const descriptor = contracts.fileDescriptor(new Uint8Array([1, 2, 3]), "application/octet-stream");
  expect(descriptor.byte_length).toBe(3);
  expect(descriptor.sha256).toHaveLength(32);
  expect(Object.isFrozen(descriptor.sha256)).toBe(true);
});

test("native Rust semantics execute through WASM", async () => {
  const harness = await Harness.create({
    authority: { kind: "conversation", id: "conversation-1" },
    issuerId: "test",
    issuerKey: new Uint8Array(32).fill(7),
    wasm,
    schemas: [
      {
        name: "example.message",
        version: 1,
        implementation_digest: Array(32).fill(9),
        fork_policy: "inherit",
        schema: {
          type: "object",
          required: ["text"],
          properties: { text: { type: "string" } },
        },
      },
    ],
  });
  const scope = harness.issueScope("root", ["event:append"]);
  const policyScope = harness.issueScopeWithPolicies("policy", [
    { level: "runtime", policy: { grants: ["event:append", "effect:run"], denies: [] } },
    { level: "invocation", policy: { grants: ["event:append"], denies: ["effect:run"] } },
  ]);
  expect(policyScope.capabilities).toEqual(["event:append"]);
  const content = harness.validateFileRef({
    volume: { provider: { namespace: "test", family: "filesystem", version: "2" },
      id: "extension", class: "agent_private", owner: { kind: "agent", id: "08080808-0808-0808-0808-080808080808" as AgentId } },
    path: "extension/event.json", version: "generation-1",
    descriptor: harness.fileDescriptor(new TextEncoder().encode('{"text":"hello"}'), "application/json"),
    display_name: "event.json",
  });
  const owner = "08080808-0808-0808-0808-080808080808" as AgentId;
  const reader = "09090909-0909-0909-0909-090909090909" as AgentId;
  const ownerScope = harness.issueScopeForAgent(owner, "private-owner", [
    harness.volumeCapability(content.volume, "read"),
    harness.volumeCapability(content.volume, "write"),
  ]);
  const delegated = harness.delegatePrivateFileRead(ownerScope, reader, "attached-reader", content);
  expect(delegated.agent).toBe(reader);
  expect(delegated.capabilities).toEqual([harness.fileReadCapability(content)]);
  const directory = harness.delegatePrivateDirectoryRead(
    ownerScope, reader, "attached-directory", content.volume, "extension",
  );
  expect(directory.agent).toBe(reader);
  expect(directory.capabilities).toEqual([harness.directoryReadCapability(content.volume, "extension")]);
  expect(() => harness.directoryReadCapability(content.volume, "../extension")).toThrow();
  expect(() => harness.directoryReadCapability(content.volume, ".system")).toThrow();
  expect(() => harness.delegatePrivateFileRead(
    harness.issueScopeForAgent(reader, "foreign", [harness.volumeCapability(content.volume, "read")]),
    reader, "invalid", content,
  )).toThrow();
  const first = harness.apply({
    authority: { kind: "conversation", id: "conversation-1" },
    operation_id: "01010101-0101-0101-0101-010101010101" as OperationId,
    idempotency_key: "append-1",
    expected_revision: 0n,
    scope,
    causal_parent: null,
    action: {
      kind: "append_custom",
      schema: "example.message",
      version: 1,
      content,
    },
  });
  expect(first.result).toBe("applied");
  expect(first.event.scope).toEqual({ id: scope.id, capabilities: scope.capabilities,
    issuer: scope.issuer, agent: scope.agent });
  expect("proof" in first.event.scope).toBe(false);
  expect("parent_proof" in first.event.scope).toBe(false);
  expect(first.event.attestation).toHaveLength(32);
  const snapshot = harness.snapshot();
  expect(snapshot.revision).toBe(1n);
  expect(() => harness.attenuate(scope, "forged", ["effect:run"])).toThrow();
  const restored = await Harness.restore(snapshot, {
    authority: { kind: "conversation", id: "conversation-1" },
    issuerId: "test",
    issuerKey: new Uint8Array(32).fill(7),
    wasm,
    schemas: [
      {
        name: "example.message",
        version: 1,
        implementation_digest: Array(32).fill(9),
        fork_policy: "inherit",
        schema: {
          type: "object",
          required: ["text"],
          properties: { text: { type: "string" } },
        },
      },
    ],
  });
  expect(restored.snapshot()).toEqual(snapshot);
  restored.free();
  harness.free();
});

test("large ref-only conversation history hydrates through bounded Rust pages", async () => {
  const agent = "08080808-0808-0808-0808-080808080808" as AgentId;
  const harness = await Harness.create({
    authority: { kind: "conversation", id: "paged-history" },
    issuerId: "paged-history", issuerKey: new Uint8Array(32).fill(5), wasm,
  });
  try {
    const scope = harness.issueScope("owner", ["conversation:bind", "conversation:append"]);
    const authority = { kind: "conversation" as const, id: "paged-history" };
    const bound = harness.apply({ authority, operation_id: crypto.randomUUID() as OperationId,
      idempotency_key: "bind", expected_revision: 0n, scope, causal_parent: null,
      action: { kind: "bind_conversation", agent } });
    expect(bound.event.revision).toBe(1n);
    const content = harness.validateFileRef({
      volume: { provider: { namespace: "test", family: "filesystem", version: "2" },
        id: "paged", class: "agent_private", owner: { kind: "agent", id: agent } },
      path: "messages/shared.txt", version: "generation-1",
      descriptor: harness.fileDescriptor(new TextEncoder().encode("shared"), "text/plain"),
      display_name: "shared.txt",
    });
    for (let index = 0; index < 1_025; index++) {
      harness.apply({ authority, operation_id: crypto.randomUUID() as OperationId,
        idempotency_key: `append:${index}`, expected_revision: BigInt(index + 1), scope,
        causal_parent: null, action: { kind: "append_conversation_message", message: {
          id: harness.conversationMessageId(crypto.randomUUID()), sequence: BigInt(index + 1),
          kind: "user", content, attachments: { kind: "inline", items: [] },
          reply_to: null, tool_call_id: null, extensions: {},
        } } });
    }
    const first = harness.conversationPage(0n);
    const second = harness.conversationPage(first.next_sequence!);
    expect(first.messages).toHaveLength(1_024);
    expect(first.next_sequence).toBe(1_024n);
    expect(second.messages).toHaveLength(1);
    expect(second.next_sequence).toBeNull();
    expect(first.event_revision).toBe(1_026n);
    expect(typeof first.event_revision).toBe("bigint");
    expect(typeof first.messages[0]!.sequence).toBe("bigint");
    expect(typeof first.messages[0]!.content.descriptor.byte_length).toBe("number");
    expect(first.messages[0]!.content.descriptor.sha256.every(byte => typeof byte === "number"
      && Number.isInteger(byte) && byte >= 0 && byte <= 255)).toBe(true);
    expect(second.total_messages).toBe(1_025n);
    const complete = harness.conversation();
    expect(complete.messages).toHaveLength(1_025);
    expect(complete.messages[1_024]!.sequence).toBe(1_025n);
    expect(typeof complete.messages[1_024]!.content.descriptor.byte_length).toBe("number");
  } finally {
    harness.free();
  }
});
