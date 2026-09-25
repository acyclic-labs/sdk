import { expect, test } from "bun:test";
import {
  conversationMessageId, decodeAttachmentManifest, descriptorFor,
  verifyFileBytes, DEFAULT_LIMITS,
  type ConversationMessage, type ConversationMessageId, type FileRef, type TaskOutcomeRecord,
} from "../src/conversation.js";
import type { AgentId } from "../src/index.js";
import { NativeContracts } from "../src/native-contracts.js";

const contracts = await NativeContracts.create();
const agent = "01010101-0101-0101-0101-010101010101" as AgentId;
const fixtureId = (value: string): ConversationMessageId => value as ConversationMessageId;

function fixtureMessage(value: unknown): ConversationMessage {
  if (value === null || typeof value !== "object" || !("sequence" in value)
    || typeof value.sequence !== "number" || !Number.isSafeInteger(value.sequence)) {
    throw new TypeError("fixture conversation sequence must be an exact JSON integer");
  }
  return { ...value, sequence: BigInt(value.sequence) } as ConversationMessage;
}

function fixtureJson(value: unknown): string {
  return JSON.stringify(value, (_key, part: unknown) => {
    if (typeof part !== "bigint") return part;
    const exact = Number(part);
    if (!Number.isSafeInteger(exact)) throw new RangeError("fixture JSON cannot encode an unsafe u64");
    return exact;
  });
}

test("Rust constructs exact descriptors for empty files and rejects invalid media types", async () => {
  const descriptor = await descriptorFor(new Uint8Array(), "application/octet-stream");
  expect(descriptor.byte_length).toBe(0);
  expect(descriptor.sha256.map(byte => byte.toString(16).padStart(2, "0")).join(""))
    .toBe("e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855");
  expect(Object.isFrozen(descriptor.sha256)).toBe(true);
  await expect(descriptorFor(new Uint8Array(), "not a media type")).rejects.toThrow();
});

test("standalone provider and file descriptor admission has one Rust contract", () => {
  const provider = contracts.validate("provider_ref", { namespace: "test", family: "filesystem", version: "2" });
  expect(provider.family).toBe("filesystem");
  expect(Object.isFrozen(provider)).toBe(true);
  expect(() => contracts.validate("provider_ref", { ...provider, namespace: "" })).toThrow();
  const descriptor = contracts.validate("file_descriptor", {
    sha256: Array(32).fill(0) as number[], byte_length: 0, media_type: "application/octet-stream",
  });
  expect(descriptor.sha256).toHaveLength(32);
  expect(Object.isFrozen(descriptor.sha256)).toBe(true);
  expect(() => contracts.validate("file_descriptor", { ...descriptor, sha256: [0] })).toThrow();
  expect(() => contracts.validate("file_descriptor", { ...descriptor, byte_length: Number.MAX_SAFE_INTEGER + 1 })).toThrow();
});

async function content(owner = agent): Promise<FileRef> {
  const bytes = new TextEncoder().encode("payload");
  return contracts.validate("file_ref", {
    volume: {
      provider: { namespace: "test", family: "filesystem", version: "2" },
      id: "scratch", class: "agent_private", owner: { kind: "agent", id: owner },
    },
    path: "messages/input.txt", version: "pinned-generation",
    descriptor: await descriptorFor(bytes, "text/plain"), display_name: "input.txt",
  });
}

test("file references reject traversal and verify exact bytes", async () => {
  const reference = await content();
  await verifyFileBytes(reference, new TextEncoder().encode("payload"));
  await expect(verifyFileBytes(reference, new TextEncoder().encode("changed"))).rejects.toThrow();
  await expect(verifyFileBytes({
    ...reference,
    descriptor: { ...reference.descriptor, sha256: [...reference.descriptor.sha256, 0] },
  }, new TextEncoder().encode("payload"))).rejects.toThrow();
  expect(() => contracts.validate("file_ref", { ...reference, path: "../escape" })).toThrow();
  expect(() => contracts.validate("file_ref", { ...reference, unexpected: true } as FileRef)).toThrow();
  expect(() => contracts.validate("conversation_message", {
    id: fixtureId("not-a-uuid"), sequence: 1n, kind: "user", content: reference,
    attachments: { kind: "inline", items: [] }, reply_to: null, tool_call_id: null, extensions: {},
  }, DEFAULT_LIMITS)).toThrow();
});

test("canonical message identity is branded only after Rust UUID validation", async () => {
  expect(String(await conversationMessageId("01010101-0101-0101-0101-010101010101")))
    .toBe("01010101-0101-0101-0101-010101010101");
  await expect(conversationMessageId("not-a-uuid")).rejects.toThrow();
});

test("manifest-backed attachment lists require a pinned typed file and exact count", async () => {
  const base = await content();
  const manifest = contracts.validate("file_ref", {
    ...base,
    descriptor: { ...base.descriptor, media_type: "application/vnd.acyclic.harness.attachments+json" },
  });
  expect(contracts.validate("attachments", { kind: "manifest", manifest, item_count: 200 }).kind).toBe("manifest");
  expect(() => contracts.validate("attachments", { kind: "manifest", manifest, item_count: -1 })).toThrow();
  expect(() => contracts.validate("attachments", { kind: "manifest", manifest: base, item_count: 1 })).toThrow();
});

test("native manifest decoding preserves exact TypeScript file-length types", async () => {
  const file = await content();
  const bytes = contracts.encodeAttachmentManifest([{ file, label: null }]);
  const manifest = contracts.validate("file_ref", { ...file, path: "attachments/list.json",
    descriptor: await descriptorFor(bytes, "application/vnd.acyclic.harness.attachments+json") });
  const items = await decodeAttachmentManifest(manifest, bytes, 1);
  expect(items).toHaveLength(1);
  expect(items[0]?.file.descriptor.byte_length).toBe(7);
  expect(typeof items[0]?.file.descriptor.byte_length).toBe("number");
});

test("configured limits reject oversized references before admission", async () => {
  const reference = await content();
  const message: ConversationMessage = {
    id: fixtureId("03030303-0303-0303-0303-030303030303"), sequence: 1n, kind: "user",
    content: reference, attachments: { kind: "inline", items: [] },
    reply_to: null, tool_call_id: null, extensions: {},
  };
  const limits = contracts.validate("limits", { ...DEFAULT_LIMITS, file_bytes: 6, render_bytes: 6 });
  expect(() => contracts.validate("conversation_message", message, limits)).toThrow();
  expect(() => contracts.validate("limits", { ...limits, attachments: 0 })).toThrow("harness limits are invalid");
});

test("Rust WASM preserves conversation positions beyond JavaScript's exact-number range", async () => {
  const reference = await content();
  const sequence = (1n << 53n) + 1n;
  const message: ConversationMessage = {
    id: fixtureId("04040404-0404-0404-0404-040404040404"), sequence, kind: "user",
    content: reference, attachments: { kind: "inline", items: [] },
    reply_to: null, tool_call_id: null, extensions: {},
  };
  const admitted = (await NativeContracts.create()).validate("conversation_message", message, DEFAULT_LIMITS);
  expect(admitted.sequence).toBe(sequence);
  expect(typeof admitted.sequence).toBe("bigint");
  const limits = (await NativeContracts.create()).validate("limits", DEFAULT_LIMITS);
  expect(typeof limits.file_bytes).toBe("number");
  expect(typeof limits.context_messages).toBe("number");
  const admittedFile = (await NativeContracts.create()).validate("file_ref", reference);
  expect(typeof admittedFile.descriptor.byte_length).toBe("number");
  expect(admittedFile.descriptor.sha256).toHaveLength(32);
  expect(admittedFile.descriptor.sha256.every(byte => typeof byte === "number"
    && Number.isInteger(byte) && byte >= 0 && byte <= 255)).toBe(true);
});

test("Rust JSON boundaries encode full-width integers and reject inexact model values", async () => {
  const exact = (1n << 53n) + 1n;
  const encoded = contracts.encodeCanonicalJson({ sequence: exact });
  expect(new TextDecoder().decode(encoded)).toBe('{"sequence":9007199254740993}');
  expect(contracts.decodeCanonicalJson(encoded)).toEqual({ sequence: exact });
  const fullU64 = (1n << 64n) - 1n;
  expect(contracts.decodeCanonicalJson(new TextEncoder().encode('{"sequence":18446744073709551615}')))
    .toEqual({ sequence: fullU64 });
  expect(() => contracts.decodeCanonicalJson(new TextEncoder().encode('{"sequence":18446744073709551617}')))
    .toThrow();
  expect(() => contracts.decodeModelJson(new TextEncoder().encode('{"value":18446744073709551617}')))
    .toThrow();
  const rustStructJson = new TextEncoder().encode('{"sequence":9007199254740993,"kind":"user"}');
  expect(() => contracts.decodeCanonicalJson(rustStructJson)).toThrow("not canonical");
  expect(contracts.decodeModelJson(new TextEncoder().encode(' { "value": 7 } '))).toEqual({ value: 7 });
  expect(() => contracts.decodeModelJson(new TextEncoder().encode('{"value":9007199254740993}')))
    .toThrow("JavaScript precision");
});

test("Rust and TypeScript share the canonical v2 message fixture", async () => {
  const fixture = (await Bun.file(new URL("../../../../fixtures/harness/v2/conversation-message.json", import.meta.url)).text()).trim();
  const decoded = fixtureMessage(JSON.parse(fixture) as unknown);
  expect(fixtureJson((await NativeContracts.create()).validate("conversation_message", decoded, DEFAULT_LIMITS))).toBe(fixture);
});

test("every v2 conversation kind shares one ordered cross-language fixture", async () => {
  const fixture = (await Bun.file(new URL("../../../../fixtures/harness/v2/conversation-kinds.json", import.meta.url)).text()).trim();
  const messages = (JSON.parse(fixture) as unknown[]).map(fixtureMessage);
  const canonical = messages.map(message => contracts.validate("conversation_message", message, DEFAULT_LIMITS));
  expect(messages).toHaveLength(9);
  expect(fixtureJson(canonical)).toBe(fixture);
});

test("Rust and TypeScript share the canonical v2 file reference fixture", async () => {
  const fixture = (await Bun.file(new URL("../../../../fixtures/harness/v2/file-ref.json", import.meta.url)).text()).trim();
  expect(JSON.stringify(contracts.validate("file_ref", JSON.parse(fixture) as FileRef))).toBe(fixture);
});

test("Rust and TypeScript share the canonical v2 task outcome fixture", async () => {
  const fixture = (await Bun.file(new URL("../../../../fixtures/harness/v2/task-outcome.json", import.meta.url)).text()).trim();
  const outcome = (await NativeContracts.create()).validate("task_outcome", JSON.parse(fixture) as TaskOutcomeRecord);
  expect(JSON.stringify(outcome)).toBe(fixture);
});
