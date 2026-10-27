import { expect, test } from "bun:test";
import { BrowserAgentAuthority, initializeBrowserRuntime } from "../src/browser.js";
import * as contracts from "../generated/wasm/acyclic_harness_wasm.js";
import type { WasmFileRefWire } from "../generated/wasm/acyclic_harness_wasm.js";
import { MODEL_TEST_LIMITS as limits } from "./support/model-limits.mjs";
import { exerciseCheckpoint } from "./checkpoint-consumer.mjs";

await initializeBrowserRuntime();

// Immutable metadata for pure admission/planning; no storage or read grant is implied.
function fixtureFile(path: string, text = "question", mediaType = "text/plain"): WasmFileRefWire {
  return contracts.validateContract("file_ref", {
    volume: { provider: { namespace: "test", family: "filesystem", version: "2" },
      id: "scratch", class: "agent_private", owner: { kind: "agent", id: "08080808-0808-0808-0808-080808080808" } },
    path, version: "pinned-generation", descriptor: contracts.fileDescriptor(new TextEncoder().encode(text), mediaType),
    display_name: path.split("/").at(-1),
  }, undefined) as WasmFileRefWire;
}

test("authenticated Rust checkpoint restores state and exact retries through WASM", async () => {
  await exerciseCheckpoint({ authority: { kind: "agent", id: "checkpoint-consumer" },
    issuerId: "checkpoint-consumer", issuerKey: new Uint8Array(32).fill(23) });
});

test("WASM turn planner emits a fresh selection and rejects stale retry state", async () => {
  const agent = "08080808-0808-0808-0808-080808080808";
  const operation = "01010101-0101-0101-0101-010101010101";
  const conversation = { agent, messages: [] };
  const content = fixtureFile("turns/planner/user.txt");
  const fresh = contracts.prepareConversationTurn(conversation, operation, content,
    { kind: "inline", items: [] }, limits, null, false, true);
  expect(fresh.disposition).toBe("dispatch");
  expect(fresh.append_user).toBe(true);
  expect(fresh.selection.conversation_revision).toBe(1n);
  expect(() => contracts.prepareConversationTurn(conversation, operation, content,
    { kind: "inline", items: [] }, limits,
    { conversation_revision: 0n, message_ids: fresh.selection.message_ids }, false, true,
  )).toThrow();
});

test("WASM turn planner applies attachment limits to manifest counts", async () => {
  const agent = "08080808-0808-0808-0808-080808080808";
  const operation = "02020202-0202-0202-0202-020202020202";
  const conversation = { agent, messages: [] };
  const content = fixtureFile("turns/planner/manifest-user.txt");
  const manifest = fixtureFile("turns/planner/attachments.json", "[]", "application/vnd.acyclic.harness.attachments+json");
  expect(() => contracts.prepareConversationTurn(
    conversation, operation, content,
    { kind: "manifest", manifest, item_count: 2 },
    { ...limits, attachments: 1 }, null, false, true,
  )).toThrow();
});

test("public authority uses the shared WASM initialization", async () => {
  const harness = new BrowserAgentAuthority({ kind: "agent", id: crypto.randomUUID() },
    "default-wasm", new Uint8Array(32).fill(11), []);
  expect(harness.snapshot().revision).toBe(0n);
  harness.free();
});

test("raw WASM rejects unknown command, action, and snapshot fields", async () => {
  const authority = { kind: "conversation", id: "strict-raw-ingress" };
  const issuerKey = new Uint8Array(32).fill(12);
  const core = new BrowserAgentAuthority(authority, "strict-raw-ingress", issuerKey, []);
  try {
    const scope = core.issueScope("root", ["conversation:bind"], undefined);
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
    expect(() => BrowserAgentAuthority.restore({ ...snapshot, unexpected_snapshot: true },
      "strict-raw-ingress", issuerKey, [])).toThrow();
    expect(core.snapshot().revision).toBe(1n);
  } finally { core.free(); }
});

test("Rust-backed identity constructors preserve canonical UUID spelling", async () => {
  const uuid = "01010101-0101-0101-0101-010101010101";
  const agent = contracts.validateIdentity("agent", uuid.toUpperCase());
  const operation = contracts.validateIdentity("operation", uuid);
  expect(String(agent)).toBe(uuid);
  expect(String(operation)).toBe(uuid);
  // ERROR_CODE_INVALID = 4 in harness-core/proto/harness/v2/harness.proto.
  await expect(Promise.resolve().then(() => contracts.validateIdentity("agent", "not-an-agent"))).rejects.toMatchObject({ code: 4 });
  expect(() => contracts.validateIdentity("operation", "not-an-operation")).toThrow(Error);
});

test("Rust operation derivation preserves the prior WebCrypto identity and rejects malformed input", async () => {
  const operation = contracts.validateIdentity("operation", "11111111-1111-4111-8111-111111111111");
  expect(String(contracts.deriveOperationUuid(operation, "user-event")))
    .toBe("08f2f698-fb58-54a7-83b9-c0260831a246");
  expect(() => contracts.deriveOperationUuid("not-a-uuid", "user-event")).toThrow();
});

test("native digest-half identities and file descriptors avoid TypeScript codecs", async () => {
  const digest = Uint8Array.from({ length: 32 }, (_value, index) => index + 1);
  expect(String(contracts.uuidFromDigestHalf(digest, false))).toBe("01020304-0506-0708-090a-0b0c0d0e0f10");
  expect(String(contracts.uuidFromDigestHalf(digest, true))).toBe("11121314-1516-1718-191a-1b1c1d1e1f20");
  expect(() => contracts.uuidFromDigestHalf(new Uint8Array(31), false)).toThrow();
  expect(() => contracts.uuidFromDigestHalf(new Uint8Array(32), true)).toThrow();
  const descriptor = contracts.fileDescriptor(new Uint8Array([1, 2, 3]), "application/octet-stream");
  expect(descriptor.byte_length).toBe(3);
  expect(descriptor.sha256).toHaveLength(32);
});

test("native Rust semantics execute through WASM", async () => {
  const authority = { kind: "conversation", id: "conversation-1" };
  const schemas = [
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
    ];
  const issuerKey = new Uint8Array(32).fill(7);
  const harness = new BrowserAgentAuthority(authority, "test", issuerKey, schemas);
  const scope = harness.issueScope("root", ["event:append"], undefined);
  const policyScope = harness.issueScopeWithPolicies("policy", [
    { level: "runtime", policy: { grants: ["event:append", "effect:run"], denies: [] } },
    { level: "invocation", policy: { grants: ["event:append"], denies: ["effect:run"] } },
  ], undefined);
  expect(policyScope.capabilities).toEqual(["event:append"]);
  const content = contracts.validateContract("file_ref", {
    volume: { provider: { namespace: "test", family: "filesystem", version: "2" },
      id: "extension", class: "agent_private", owner: { kind: "agent", id: "08080808-0808-0808-0808-080808080808" } },
    path: "extension/event.json", version: "generation-1",
    descriptor: contracts.fileDescriptor(new TextEncoder().encode('{"text":"hello"}'), "application/json"),
    display_name: "event.json",
  }, undefined);
  const owner = "08080808-0808-0808-0808-080808080808";
  const reader = "09090909-0909-0909-0909-090909090909";
  const ownerScope = harness.issueScope("private-owner", [
    harness.volumeCapability(content.volume, "read"),
    harness.volumeCapability(content.volume, "write"),
  ], owner);
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
    harness.issueScope("foreign", [harness.volumeCapability(content.volume, "read")], reader),
    reader, "invalid", content,
  )).toThrow();
  const first = harness.apply({
    operation_id: "01010101-0101-0101-0101-010101010101",
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
  expect(typeof first.event.attestation).toBe("string");
  expect(atob(first.event.attestation).length).toBeGreaterThan(64);
  expect(typeof scope.workload).toBe("string");
  expect(() => harness.verifyScope({ ...scope, workload: "AAAA" })).toThrow();
  const snapshot = harness.snapshot();
  expect(snapshot.revision).toBe(1n);
  expect(() => harness.attenuate(scope, "forged", ["effect:run"])).toThrow();
  const restored = BrowserAgentAuthority.restore(snapshot, "test", issuerKey, schemas);
  expect(restored.snapshot()).toEqual(snapshot);
  restored.free();
  harness.free();
});

test("large ref-only conversation history hydrates through bounded Rust pages", async () => {
  const agent = "08080808-0808-0808-0808-080808080808";
  const harness = new BrowserAgentAuthority({ kind: "conversation", id: "paged-history" },
    "paged-history", new Uint8Array(32).fill(5), []);
  try {
    // Raw ABI requires an explicit finite page size. Zero is refused before reading.
    expect(() => harness.conversationPage(0n, 0)).toThrow();
    const scope = harness.issueScope("owner", ["conversation:bind", "conversation:append"], undefined);
    const bound = harness.apply({ operation_id: crypto.randomUUID(),
      idempotency_key: "bind", expected_revision: 0n, scope, causal_parent: null,
      action: { kind: "bind_conversation", agent } });
    expect(bound.event.revision).toBe(1n);
    const content = contracts.validateContract("file_ref", {
      volume: { provider: { namespace: "test", family: "filesystem", version: "2" },
        id: "paged", class: "agent_private", owner: { kind: "agent", id: agent } },
      path: "messages/shared.txt", version: "generation-1",
      descriptor: contracts.fileDescriptor(new TextEncoder().encode("shared"), "text/plain"),
      display_name: "shared.txt",
    }, undefined);
    for (let index = 0; index < 1_025; index++) {
      harness.apply({ operation_id: crypto.randomUUID(),
        idempotency_key: `append:${index}`, expected_revision: BigInt(index + 1), scope,
        causal_parent: null, action: { kind: "append_conversation_message", message: {
          id: contracts.validateConversationMessageId(crypto.randomUUID()), sequence: BigInt(index + 1),
          kind: "user", content, attachments: { kind: "inline", items: [] },
          reply_to: null, tool_call_id: null, extensions: {},
        } } });
    }
    const first = harness.conversationPage(0n, 1_024);
    const second = harness.conversationPage(first.next_sequence!, 1_024);
    expect(first.messages).toHaveLength(1_024);
    expect(first.next_sequence).toBe(1_024n);
    expect(second.messages).toHaveLength(1);
    expect(second.next_sequence).toBeNull();
    expect(first.event_revision).toBe(1_026n);
    expect(typeof first.event_revision).toBe("bigint");
    expect(typeof first.messages[0]!.sequence).toBe("bigint");
    expect(typeof first.messages[0]!.content.descriptor.byte_length).toBe("number");
    expect(first.messages[0]!.content.descriptor.sha256.every((byte: unknown) => typeof byte === "number"
      && Number.isInteger(byte) && byte >= 0 && byte <= 255)).toBe(true);
    expect(second.total_messages).toBe(1_025n);
    // Explicit pages cover every record; no unbounded conversation materialization.
    expect([...first.messages, ...second.messages]).toHaveLength(1_025);
    expect(second.messages[0]!.sequence).toBe(1_025n);
    expect(typeof second.messages[0]!.content.descriptor.byte_length).toBe("number");
  } finally {
    harness.free();
  }
});


test("default WASM turn planner preserves full history or rejects its bound", async () => {
  const agent = "08080808-0808-0808-0808-080808080808";
  const operation = "03030303-0303-0303-0303-030303030303";
  const historicalUser = "04040404-0404-0404-0404-040404040404";
  const content = fixtureFile("turns/planner/complete.txt");
  const common = { content, attachments: { kind: "inline" as const, items: [] }, tool_call_id: null, extensions: {} };
  const conversation = { agent, messages: [
    { ...common, id: historicalUser, sequence: 1n, kind: "user" as const, reply_to: null },
    { ...common, id: "05050505-0505-0505-0505-050505050505",
      sequence: 2n, kind: "assistant" as const, reply_to: historicalUser },
  ] };
  expect(() => contracts.prepareConversationTurn(conversation, operation, content,
    { kind: "inline", items: [] }, { ...limits, context_messages: 2 }, null, false, true)).toThrow();
  const prepared = contracts.prepareConversationTurn(conversation, operation, content,
    { kind: "inline", items: [] }, { ...limits, context_messages: 3 }, null, false, true);
  expect(prepared.selection.message_ids).toHaveLength(3);
  expect(prepared.selection.message_ids[0]).toBe(historicalUser);
  expect(prepared.append_user).toBe(true);
  expect(conversation.messages).toHaveLength(2);
});
