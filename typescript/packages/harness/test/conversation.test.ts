import { expect, test } from "bun:test";
import * as contracts from "../generated/wasm/acyclic_harness_wasm.js";
import type { WasmFileRefWire } from "../generated/wasm/acyclic_harness_wasm.js";
import { initializeClientQueries } from "../src/client.js";
import { MODEL_TEST_LIMITS as fixtureLimits } from "./support/model-limits.mjs";

await initializeClientQueries();
const agent = "01010101-0101-0101-0101-010101010101";

// The removed facade's effectively infinite defaults are not frontend policy.
// These finite inputs are fixture bounds; Rust still owns numeric admission.
test("finite fixture limits use Rust admission and reject inexact numbers", () => {
  expect(contracts.validateContract("limits", fixtureLimits, undefined)).toEqual(fixtureLimits);
  expect(Object.values(fixtureLimits).every(value => typeof value === "number")).toBe(true);
  expect(() => contracts.validateContract("limits", {
    ...fixtureLimits, file_bytes: Number.MAX_SAFE_INTEGER + 1,
  }, undefined)).toThrow();
  expect(() => contracts.validateContract("limits", {
    ...fixtureLimits, path_bytes: 2 ** 32,
  }, undefined)).toThrow();
});

function fixtureMessage(value: unknown) {
  if (value === null || typeof value !== "object" || !("sequence" in value)
    || typeof value.sequence !== "number" || !Number.isSafeInteger(value.sequence)) {
    throw new TypeError("fixture conversation sequence must be an exact JSON integer");
  }
  return { ...value, sequence: BigInt(value.sequence) };
}

function fixtureJson(value: unknown): string {
  return JSON.stringify(value, (_key, part: unknown) => {
    if (typeof part !== "bigint") return part;
    const exact = Number(part);
    if (!Number.isSafeInteger(exact)) throw new RangeError("fixture JSON cannot encode an unsafe u64");
    return exact;
  });
}

async function content(owner = agent): Promise<WasmFileRefWire> {
  const bytes = new TextEncoder().encode("payload");
  return contracts.validateContract("file_ref", {
    volume: {
      provider: { namespace: "test", family: "filesystem", version: "2" },
      id: "scratch", class: "agent_private", owner: { kind: "agent", id: owner },
    },
    path: "messages/input.txt", version: "pinned-generation",
    descriptor: contracts.fileDescriptor(bytes, "text/plain"), display_name: "input.txt",
  }, undefined);
}

test("file references reject traversal and verify exact bytes", async () => {
  const reference = await content();
  contracts.verifyFileBytes(reference, new TextEncoder().encode("payload"));
  expect(() => contracts.verifyFileBytes(reference, new TextEncoder().encode("changed"))).toThrow();
  expect(() => contracts.verifyFileBytes({
    ...reference,
    descriptor: { ...reference.descriptor, sha256: [...reference.descriptor.sha256, 0] },
  }, new TextEncoder().encode("payload"))).toThrow();
  expect(() => contracts.validateContract("file_ref", { ...reference, path: "../escape" }, undefined)).toThrow();
  expect(() => contracts.validateContract("file_ref", { ...reference, unexpected: true }, undefined)).toThrow();
  expect(() => contracts.validateContract("conversation_message", {
    id: "not-a-uuid", sequence: 1n, kind: "user", content: reference,
    attachments: { kind: "inline", items: [] }, reply_to: null, tool_call_id: null, extensions: {},
  }, fixtureLimits)).toThrow();
});

test("canonical message spelling is admitted only after Rust UUID validation", () => {
  expect(String(contracts.validateConversationMessageId("01010101-0101-0101-0101-010101010101")))
    .toBe("01010101-0101-0101-0101-010101010101");
  expect(() => contracts.validateConversationMessageId("not-a-uuid")).toThrow();
});

test("manifest-backed attachment lists require a pinned typed file and exact count", async () => {
  const base = await content();
  const manifest = contracts.validateContract("file_ref", {
    ...base,
    descriptor: { ...base.descriptor, media_type: "application/vnd.acyclic.harness.attachments+json" },
  }, undefined);
  expect(contracts.validateContract("attachments", { kind: "manifest", manifest, item_count: 200 }, undefined).kind).toBe("manifest");
  expect(() => contracts.validateContract("attachments", { kind: "manifest", manifest, item_count: -1 }, undefined)).toThrow();
  expect(() => contracts.validateContract("attachments", { kind: "manifest", manifest: base, item_count: 1 }, undefined)).toThrow();
});

test("native manifest decoding preserves exact TypeScript file-length types", async () => {
  const file = await content();
  const bytes = contracts.encodeAttachmentManifest([{ file, label: null }]);
  const manifest = contracts.validateContract("file_ref", { ...file, path: "attachments/list.json",
    descriptor: contracts.fileDescriptor(bytes, "application/vnd.acyclic.harness.attachments+json") }, undefined);
  const items = contracts.decodeAttachmentManifest(manifest, bytes, 1);
  expect(items).toHaveLength(1);
  expect(items[0]?.file.descriptor.byte_length).toBe(7);
  expect(typeof items[0]?.file.descriptor.byte_length).toBe("number");
});

test("configured limits reject oversized references before admission", async () => {
  const reference = await content();
  const message = {
    id: "03030303-0303-0303-0303-030303030303", sequence: 1n, kind: "user",
    content: reference, attachments: { kind: "inline", items: [] },
    reply_to: null, tool_call_id: null, extensions: {},
  };
  const limits = contracts.validateContract("limits", { ...fixtureLimits, file_bytes: 6, render_bytes: 6 }, undefined);
  expect(() => contracts.validateContract("conversation_message", message, limits)).toThrow();
  expect(() => contracts.validateContract("limits", { ...limits, attachments: 0 }, undefined)).toThrow();
});

test("Rust WASM preserves conversation positions beyond JavaScript's exact-number range", async () => {
  const reference = await content();
  const sequence = (1n << 53n) + 1n;
  const message = {
    id: "04040404-0404-0404-0404-040404040404", sequence, kind: "user",
    content: reference, attachments: { kind: "inline", items: [] },
    reply_to: null, tool_call_id: null, extensions: {},
  };
  const admitted = contracts.validateContract("conversation_message", message, fixtureLimits);
  expect(admitted.sequence).toBe(sequence);
  expect(typeof admitted.sequence).toBe("bigint");
  const limits = contracts.validateContract("limits", fixtureLimits, undefined);
  expect(typeof limits.file_bytes).toBe("number");
  expect(typeof limits.context_messages).toBe("number");
  const admittedFile = contracts.validateContract("file_ref", reference, undefined) as WasmFileRefWire;
  expect(typeof admittedFile.descriptor.byte_length).toBe("number");
  expect(admittedFile.descriptor.sha256).toHaveLength(32);
  expect(admittedFile.descriptor.sha256.every(byte => typeof byte === "number"
    && Number.isInteger(byte) && byte >= 0 && byte <= 255)).toBe(true);
});

test("Rust JSON boundaries retain exact full-width integers and reject overflow", async () => {
  const exact = (1n << 53n) + 1n;
  const encoded = contracts.encodeCanonicalJson({ sequence: exact });
  expect(new TextDecoder().decode(encoded)).toBe('{"sequence":9007199254740993}');
  expect(contracts.decodeCanonicalJson(encoded)).toEqual(new Map([["sequence", exact]]));
  const fullU64 = (1n << 64n) - 1n;
  expect(contracts.decodeCanonicalJson(new TextEncoder().encode('{"sequence":18446744073709551615}')))
    .toEqual(new Map([["sequence", fullU64]]));
  expect(() => contracts.decodeCanonicalJson(new TextEncoder().encode('{"sequence":18446744073709551617}')))
    .toThrow();
  expect(() => contracts.decodeJson(new TextEncoder().encode('{"value":18446744073709551617}')))
    .toThrow();
  const rustStructJson = new TextEncoder().encode('{"sequence":9007199254740993,"kind":"user"}');
  expect(() => contracts.decodeCanonicalJson(rustStructJson)).toThrow();
  expect(contracts.decodeJson(new TextEncoder().encode(' { "value": 7 } '))).toEqual(new Map([["value", 7n]]));
  // Raw Rust JSON retains this exact integer; the removed model facade's
  // conversion to portable JS Number is intentionally not reproduced.
  expect(contracts.decodeJson(new TextEncoder().encode('{"value":9007199254740993}')))
    .toEqual(new Map([["value", exact]]));
});

test("Rust and TypeScript share the canonical v2 message fixture", async () => {
  const fixture = (await Bun.file(new URL("../../../../fixtures/harness/v2/conversation-message.json", import.meta.url)).text()).trim();
  const decoded = fixtureMessage(JSON.parse(fixture) as unknown);
  expect(fixtureJson(contracts.validateContract("conversation_message", decoded, fixtureLimits))).toBe(fixture);
});

test("every v2 conversation kind shares one ordered cross-language fixture", async () => {
  const fixture = (await Bun.file(new URL("../../../../fixtures/harness/v2/conversation-kinds.json", import.meta.url)).text()).trim();
  const messages = (JSON.parse(fixture) as unknown[]).map(fixtureMessage);
  const canonical = messages.map(message => contracts.validateContract("conversation_message", message, fixtureLimits));
  expect(messages).toHaveLength(9);
  expect(fixtureJson(canonical)).toBe(fixture);
});

test("Rust and TypeScript share the canonical v2 file reference fixture", async () => {
  const fixture = (await Bun.file(new URL("../../../../fixtures/harness/v2/file-ref.json", import.meta.url)).text()).trim();
  expect(JSON.stringify(contracts.validateContract("file_ref", JSON.parse(fixture), undefined))).toBe(fixture);
});

test("Rust and TypeScript reject the same malformed file references and forged content", async () => {
  const base = JSON.parse(await Bun.file(new URL("../../../../fixtures/harness/v2/file-ref.json", import.meta.url)).text()) as Record<string, unknown>;
  const corpus = JSON.parse(await Bun.file(new URL("../../../../fixtures/harness/v2/file-ref-security-cases.json", import.meta.url)).text()) as {
    cases: Array<{ name: string; pointer: string; value: unknown; check: "decode" | "bytes" }>;
  };
  expect(corpus.cases).toHaveLength(13);
  for (const { name, pointer, value, check } of corpus.cases) {
    const modified = structuredClone(base);
    const segments = pointer.split("/").slice(1);
    let parent: Record<string, unknown> = modified;
    for (const segment of segments.slice(0, -1)) parent = parent[segment] as Record<string, unknown>;
    parent[segments.at(-1)!] = value;
    if (check === "decode") {
      expect(() => contracts.validateContract("file_ref", modified, undefined)).toThrow();
    } else {
      const reference = contracts.validateContract("file_ref", modified, undefined);
      expect(() => contracts.verifyFileBytes(reference, new TextEncoder().encode("hello"))).toThrow();
    }
  }
});

test("Rust and TypeScript share the canonical private directory page fixture", async () => {
  const fixture = (await Bun.file(new URL("../../../../fixtures/harness/v2/private-directory-page.json", import.meta.url)).text()).trim();
  const page = contracts.validateContract("private_directory_page", JSON.parse(fixture), undefined);
  expect(JSON.stringify(page)).toBe(fixture);
});

test("Rust and TypeScript share the canonical v2 task outcome fixture", async () => {
  const fixture = (await Bun.file(new URL("../../../../fixtures/harness/v2/task-outcome.json", import.meta.url)).text()).trim();
  const outcome = contracts.validateContract("task_outcome", JSON.parse(fixture), undefined);
  expect(JSON.stringify(outcome)).toBe(fixture);
});
