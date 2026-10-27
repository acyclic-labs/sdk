import { expect, test } from "bun:test";
import { readFile } from "node:fs/promises";
import * as contracts from "../generated/wasm/acyclic_harness_wasm.js";
import { validateModelContent, validateUserInput,
  type WasmFileRefWire } from "../generated/wasm/acyclic_harness_wasm.js";
import { decodeClientJson } from "../src/client.js";
import { initializeModelProviders } from "../src/providers.js";
import { MODEL_TEST_LIMITS as fixtureLimits } from "./support/model-limits.mjs";

await initializeModelProviders();

// Metadata admission only: no read capability or filesystem IO is fabricated.
async function file(): Promise<WasmFileRefWire> {
  return contracts.validateContract("file_ref", {
    volume: { provider: { namespace: "test", family: "filesystem", version: "2" },
      id: "scratch", class: "agent_private",
      owner: { kind: "agent", id: "07070707-0707-0707-0707-070707070707" } },
    path: "messages/input.txt", version: "pinned-generation",
    descriptor: contracts.fileDescriptor(new TextEncoder().encode("input"), "text/plain"),
    display_name: "input.txt",
  }, undefined) as WasmFileRefWire;
}

test("tool results retain their pinned string schema and enforce the exact render bound", async () => {
  const definition = { name: "read", revision: "1", description: "read", inputSchema: { type: "string" },
    outputSchema: { type: "string" },
    projectionSchema: contracts.jsonToolProjectionSchema({ type: "string" }) };
  const limits = { ...fixtureLimits, render_bytes: 64 };
  const accepted = { kind: "json" as const, value: "x".repeat(38) };
  expect(contracts.encodeCanonicalJson(accepted).length).toBe(64);
  expect(() => contracts.validateToolProjection(definition, accepted, limits)).not.toThrow();
  const rejected = { kind: "json" as const, value: "x".repeat(39) };
  expect(contracts.encodeCanonicalJson(rejected).length).toBe(65);
  expect(() => contracts.validateToolProjection(definition, rejected, limits)).toThrow();
  expect(() => contracts.validateToolProjection(definition, { kind: "json", value: 42 }, limits)).toThrow();
});

test("WASM model validators preserve Rust limits and tool-name checks", async () => {
  const content = await file();
  expect(() => validateModelContent("hello", fixtureLimits)).not.toThrow();
  expect(() => validateModelContent({ kind: "file", file: content, policy: "reference" }, fixtureLimits)).not.toThrow();
  expect(() => validateModelContent({ kind: "tool_call", callId: "call", name: "lookup", arguments: { value: 1 } }, fixtureLimits)).not.toThrow();
  expect(() => validateModelContent({ kind: "tool_call", callId: "call", name: "bad name", arguments: {} }, fixtureLimits)).toThrow();
  expect(() => validateModelContent({ kind: "text", text: "x".repeat(fixtureLimits.render_bytes + 1) }, fixtureLimits)).toThrow();
  expect(() => validateModelContent({ kind: "unknown", value: true } as never, fixtureLimits)).toThrow();
});

test("WASM user-input validator rejects empty, tool, and malformed content", async () => {
  const content = await file();
  expect(() => validateUserInput("hello")).not.toThrow();
  expect(() => validateUserInput({ kind: "file", file: content, policy: { native: {
    intent: { kind: "image", detail: "auto" }, maximum_bytes: 64, maximum_work: 8, configuration: null,
  } } })).not.toThrow();
  expect(() => validateUserInput("")).toThrow();
  expect(() => validateUserInput([])).toThrow();
  expect(() => validateUserInput({ kind: "tool_result", callId: "call", name: "lookup", content: { kind: "json", value: {} } })).toThrow();
  expect(() => validateUserInput({ kind: "file", file: { ...content, path: "../escape" }, policy: "reference" })).toThrow();
});

async function nativeOptionPart() {
  const media = await file();
  const options = { ...media, path: "messages/options.json", display_name: "options.json",
    descriptor: contracts.fileDescriptor(new TextEncoder().encode("{}"), "application/json") };
  const configuration = {
    source: { authority: { kind: "agent" as const, id: "07070707-0707-0707-0707-070707070707" }, revision: 9_007_199_254_740_993n },
    configuration: { extension: { name: "fixture.options", version: 1 },
      schema_digest: Array(32).fill(1), content: options },
    implementation_digest: Array(32).fill(2),
  };
  return { kind: "file" as const, file: media, policy: { native: {
    intent: { kind: "image" as const, detail: "auto" as const },
    maximum_bytes: 64, maximum_work: 8, configuration,
  } } };
}

test("Rust inventory includes nested tool media and options while preserving original revisions", async () => {
  const part = await nativeOptionPart();
  const inventory = contracts.modelContentInventory({ kind: "tool_result", callId: "call", name: "inspect",
    content: { kind: "parts", parts: [{ kind: "text", text: "attachment" }, part] } }, fixtureLimits);
  expect(inventory.files).toEqual([part.file, part.policy.native.configuration.configuration.content]);
  expect(inventory.nativeConfigurations).toEqual([part.policy.native.configuration]);
  expect(inventory.nativeConfigurations[0]?.source.revision).toBe(9_007_199_254_740_993n);
  expect(contracts.modelContentInventory("plain text", fixtureLimits)).toEqual({ files: [], nativeConfigurations: [] });
});

test("generated context selections and updates use native schemas, placement and bounds", () => {
  const selection = { source: { kind: "attribute" as const, attribute: {
    type_name: "example.role", type_revision: "1", state_revision: "2",
    schema: { type: "string" }, value: "reviewer",
  } }, extent: { kind: "whole" as const }, representation: "full" as const };
  contracts.validateContextSelection(selection, fixtureLimits);
  expect(() => contracts.validateContextSelection({ ...selection, source: {
    kind: "attribute", attribute: { ...selection.source.attribute, value: 3 },
  } }, fixtureLimits)).toThrow();
  expect(() => contracts.validateContextSelection(selection, {
    ...fixtureLimits, render_bytes: 1,
  })).toThrow();
  const base = { messages: [{ role: "user" as const, content: "task" }], metadata: {} };
  const rendered = [{ role: "system" as const, content: "reviewer" }];
  const rebuilt = contracts.applyContextProjection(base, rendered, "prompt", "prepend", fixtureLimits);
  const update = contracts.applyContextProjection(base, rendered, "update", "prepend", fixtureLimits);
  expect(rebuilt.messages[0]).toEqual(update.messages[1]);
  expect(base.messages).toHaveLength(1);
  expect(() => contracts.applyContextProjection(base, rendered, "prompt", "prepend", {
    ...fixtureLimits, render_bytes: 1,
  })).toThrow();
});

test("Rust constructs exact descriptors for empty files and rejects invalid media types", async () => {
  const descriptor = contracts.fileDescriptor(new Uint8Array(), "application/octet-stream");
  expect(descriptor.byte_length).toBe(0);
  expect(descriptor.sha256.map(byte => byte.toString(16).padStart(2, "0")).join(""))
    .toBe("e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855");
  expect(() => contracts.fileDescriptor(new Uint8Array(), "not a media type")).toThrow();
});

test("standalone provider and file descriptor admission has one Rust contract", () => {
  const provider = contracts.validateContract("provider_ref", { namespace: "test", family: "filesystem", version: "2" }, undefined);
  expect(provider.family).toBe("filesystem");
  expect(() => contracts.validateContract("provider_ref", { ...provider, namespace: "" }, undefined)).toThrow();
  const descriptor = contracts.validateContract("file_descriptor", {
    sha256: Array(32).fill(0) as number[], byte_length: 0, media_type: "application/octet-stream",
  }, undefined);
  expect(descriptor.sha256).toHaveLength(32);
  expect(() => contracts.validateContract("file_descriptor", { ...descriptor, sha256: [0] }, undefined)).toThrow();
  expect(() => contracts.validateContract("file_descriptor", { ...descriptor, byte_length: Number.MAX_SAFE_INTEGER + 1 }, undefined)).toThrow();
});

// Pure tool admission goes through the retained Rust validator and client codec.
// Exact client integers are BigInt; the deleted JS facade normalized them to Number.
test("native tool JSON preserves special property names as data", async () => {
  const value = Object.fromEntries([["__proto__", { safe: true }], ["nested", { value: 1 }], ["byte_length", -2]]);
  const admitted = decodeClientJson(contracts.encodeCanonicalJson(contracts.validateToolValue({ type: "object" }, value)));
  expect(Object.getOwnPropertyDescriptor(admitted, "__proto__")?.value).toEqual({ safe: true });
  expect((admitted as { nested: { value: bigint } }).nested.value).toBe(1n);
  expect((admitted as { byte_length: bigint }).byte_length).toBe(-2n);
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
try { admitted = decodeClientJson(contracts.encodeCanonicalJson(contracts.validateToolValue({ type: "object" }, value))); }
catch { /* Rejecting an accessor is also safe. */ }
if (admitted !== undefined) expect(admitted).toEqual({ unsafe: 1n });
expect(reads).toBeLessThanOrEqual(1);
});

// Ported pure context oracles; Rust retains JSON maps and exact integers.
test("generated skill parser and immutable projection share Rust validation and bounds", () => {
  const provider = { namespace: "discovery", family: "filesystem", version: "2" };
  const source: contracts.PinnedContextPath = {
    root: { volume: { provider, id: "skills", class: "session_shared", owner: { kind: "session", id: "test" } }, directory: "" },
    generation: { kind: "generation", provider, key: [1], version: "pinned" }, path: "inspect/SKILL.md",
  };
  const prefix = new TextEncoder().encode("---\r\nname: inspect\r\ndescription: >-\r\n  Inspect selected\r\n  files\r\nmetadata: {category: review}\r\n---\r\nSECRET BODY");
  const skill = contracts.parseSkillMetadata(prefix, source);
  expect(skill.description).toBe("Inspect selected files");
  expect(contracts.encodeCanonicalJson(skill.fields)).toEqual(
    contracts.encodeCanonicalJson({ metadata: { category: "review" } }));
  expect(contracts.encodeCanonicalJson(skill.source)).toEqual(contracts.encodeCanonicalJson(source));
  const snapshot = { instructions: [], skills: [skill] };
  const base = { messages: [{ role: "user" as const, content: "inspect" }], metadata: {} };
  const projected = contracts.projectDiscoveredContext(snapshot, base, "prepend", fixtureLimits);
  expect(base.messages).toHaveLength(1);
  expect(projected.messages).toHaveLength(2);
  expect(JSON.stringify(projected)).toContain("Inspect selected files");
  expect(JSON.stringify(projected)).not.toContain("SECRET BODY");
  expect(() => contracts.projectDiscoveredContext(snapshot, base, "prepend", { ...fixtureLimits, render_bytes: 1 })).toThrow();
  expect(() => contracts.projectDiscoveredContext({ ...snapshot, skills: [{ ...skill, name: "Invalid" }] }, base, "prepend", fixtureLimits)).toThrow();
  expect(() => contracts.parseSkillMetadata(new TextEncoder().encode("---\nname: inspect\ndescription: &x hidden\nmetadata: *x\n---\n"), source)).toThrow();
  expect(() => contracts.parseSkillMetadata(prefix, { ...source, path: ".system/private" })).toThrow();
  const name = "a".repeat(128);
  const description = "d".repeat(2048);
  const payload = "x".repeat(70000);
  const items = Array(3000).fill("0").join(",");
  const admitted = contracts.parseSkillMetadata(new TextEncoder().encode(
    `---\nname: ${name}\ndescription: ${description}\npayload: ${payload}\nitems: [${items}]\nnested: ${"[".repeat(32)}0${"]".repeat(32)}\n---\n`,
  ), source);
  expect(admitted.name).toBe(name);
  expect(admitted.description).toBe(description);
  const fields = decodeClientJson(contracts.encodeCanonicalJson(admitted.fields)) as Record<string, unknown>;
  expect(fields.payload).toBe(payload);
  expect(fields.items).toHaveLength(3000);
  // The original case admits >70 KiB of metadata; this is a finite fixture bound.
  expect(() => contracts.projectDiscoveredContext({ instructions: [], skills: [admitted] },
    base, "prepend", { ...fixtureLimits, render_bytes: 131_072 })).not.toThrow();
});
test("generated context composition preserves existing file wires without reinterpreting metadata", async () => {
  const reference = await file();
  const part = { kind: "file" as const, file: reference, policy: "reference" as const };
  const opaque = Object.fromEntries([["sha256", "note"], ["byte_length", 7n], ["media_type", "note"],
    ["__proto__", { marker: "literal" }]]);
  const base = { messages: [{ role: "user" as const, content: part },
    { role: "user" as const, content: [{ kind: "text" as const, text: "attached" }, part] },
    { role: "assistant" as const, content: { kind: "tool_call" as const, call_id: "call", name: "inspect", arguments: opaque } },
    { role: "tool" as const, content: { kind: "tool_result" as const, call_id: "call", name: "inspect", content: { kind: "json" as const, value: opaque } } }],
  metadata: Object.fromEntries([["attachment", reference], ["__proto__", reference]]) };
  for (const placement of ["prepend", "append"] as const) {
    const projections = [
      contracts.projectDiscoveredContext({ instructions: [reference], skills: [] }, base, placement, fixtureLimits),
      contracts.applyContextProjection(base, [{ role: "system", content: part }], "prompt", placement, fixtureLimits),
    ];
    for (const projected of projections) {
      expect(projected.metadata).toEqual(base.metadata);
      expect(Object.hasOwn(projected.metadata, "__proto__")).toBe(true);
      const calls = projected.messages.filter(message => !Array.isArray(message.content)
        && typeof message.content !== "string" && message.content.kind === "tool_call");
      expect(contracts.encodeCanonicalJson(calls[0]?.content)).toEqual(contracts.encodeCanonicalJson(
        { kind: "tool_call", call_id: "call", name: "inspect", arguments: opaque }));
      const files = projected.messages.flatMap(message => {
        const parts = typeof message.content === "string" ? []
          : Array.isArray(message.content) ? message.content : [message.content];
        return parts.flatMap(part => part.kind === "file" ? [part.file] : []);
      });
      expect(files).toHaveLength(3);
      for (const file of files) expect(typeof file.descriptor.byte_length).toBe("number");
      expect(() => contracts.prepareModelRequest({ model: { provider: "mock", name: "files", revision: "1", options: {} },
        messages: projected.messages, tools: [{ name: "inspect", revision: "1", description: "Inspect files",
          inputSchema: {}, outputSchema: {}, projectionSchema: contracts.jsonToolProjectionSchema({}) }], maxOutputTokens: 4096 }, fixtureLimits)).not.toThrow();
    }
  }
});

test("native and WASM request construction preserve exact Unicode and paired tool bytes", async () => {
  const fixture = await readFile(new URL("../../../../rust/crates/harness/fixtures/model-request.json", import.meta.url));
  const wire = decodeClientJson(fixture) as unknown as contracts.ModelRequest;
  const request = {
    model: wire.model,
    messages: wire.messages,
    tools: wire.tools.map((tool) => ({
      name: tool.name, revision: tool.revision, description: tool.description,
      inputSchema: tool.input_schema, outputSchema: tool.output_schema, projectionSchema: tool.projection_schema,
    })),
    maxOutputTokens: wire.max_output_tokens,
  };
  const bytes = contracts.prepareModelRequest(request, fixtureLimits);
  expect(bytes).toEqual(Uint8Array.from(fixture));
  const prefix = await readFile(new URL("../../../../rust/crates/harness/fixtures/model-prefix.json", import.meta.url));
  expect(contracts.encodeModelPrefix(bytes, null, undefined, fixtureLimits)).toEqual(Uint8Array.from(prefix));
  expect(() => contracts.prepareModelRequest({ ...request, maxOutputTokens: 0 }, fixtureLimits)).toThrow();
  expect(() => contracts.prepareModelRequest({ ...request, messages: request.messages.slice(0, 2) }, fixtureLimits)).toThrow();
  expect(() => contracts.prepareModelRequest({ ...request, tools: [...request.tools, ...request.tools] }, fixtureLimits)).toThrow();
});
