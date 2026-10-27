import { syntheticAccounting } from "./support/model-accounting.mjs";
import { expect, test } from "bun:test";
import fc from "fast-check";
import { readFile } from "node:fs/promises";
import { DEFAULT_LIMITS, jsonToolProjection, NativeContracts, descriptorFor, type AgentId, type FileRef, type PinnedContextPath } from "../src/index.js";
import { prepareModelRequest, encodeModelPrefix, WasmReducer, validateModelContent, validateUserInput } from "../generated/wasm/acyclic_harness_wasm.js";
import * as harnessWasm from "../generated/wasm/acyclic_harness_wasm.js";
import { publicModelContent } from "../src/projection.js";
import type { ModelContent as NativeModelContent } from "../generated/wasm/acyclic_harness_wasm.js";
import type { ModelRequest as ModelRequestWire } from "../generated/wasm/acyclic_harness_wasm.js";
import initWasm from "../generated/wasm/acyclic_harness_wasm.js";
import { assertHarnessWasmExports, ensureHarnessWasm } from "../src/wasm-runtime.js";

const contracts = await NativeContracts.create();
const jsonProjection = await jsonToolProjection({});
const rawWasmExports = await initWasm();
const agent = "07070707-0707-0707-0707-070707070707" as AgentId;

test("generated skill parser and immutable projection share Rust validation and bounds", () => {
  const provider = { namespace: "discovery", family: "filesystem", version: "2" };
  const source: PinnedContextPath = {
    root: { volume: { provider, id: "skills", class: "session_shared", owner: { kind: "session", id: "test" } }, directory: "" },
    generation: { kind: "generation", provider, key: [1], version: "pinned" }, path: "inspect/SKILL.md",
  };
  const prefix = new TextEncoder().encode("---\r\nname: inspect\r\ndescription: >-\r\n  Inspect selected\r\n  files\r\nmetadata: {category: review}\r\n---\r\nSECRET BODY");
  const skill = contracts.parseSkillMetadata(prefix, source);
  expect(skill.description).toBe("Inspect selected files");
  expect(skill.fields).toEqual({ metadata: { category: "review" } });
  expect(skill.source).toEqual(source);
  const snapshot = { instructions: [], skills: [skill] };
  const base = { messages: [{ role: "user" as const, content: "inspect" }], metadata: {} };
  const projected = contracts.projectDiscoveredContext(snapshot, base, "prepend", DEFAULT_LIMITS);
  expect(base.messages).toHaveLength(1);
  expect(projected.messages).toHaveLength(2);
  expect(JSON.stringify(projected)).toContain("Inspect selected files");
  expect(JSON.stringify(projected)).not.toContain("SECRET BODY");
  expect(() => contracts.projectDiscoveredContext(snapshot, base, "prepend", { ...DEFAULT_LIMITS, render_bytes: 1 })).toThrow();
  expect(() => contracts.projectDiscoveredContext({ ...snapshot, skills: [{ ...skill, name: "Invalid" }] }, base, "prepend", DEFAULT_LIMITS)).toThrow();
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
  expect(admitted.fields.payload).toBe(payload);
  expect(admitted.fields.items).toHaveLength(3000);
  expect(() => contracts.projectDiscoveredContext({ instructions: [], skills: [admitted] },
    base, "prepend", DEFAULT_LIMITS)).not.toThrow();
});

test("generated context selections and updates use native schemas, placement and bounds", () => {
  const selection = { source: { kind: "attribute" as const, attribute: {
    type_name: "example.role", type_revision: "1", state_revision: "2",
    schema: { type: "string" }, value: "reviewer",
  } }, extent: { kind: "whole" as const }, representation: "full" as const };
  contracts.validateContextSelection(selection, DEFAULT_LIMITS);
  expect(() => contracts.validateContextSelection({ ...selection, source: {
    kind: "attribute", attribute: { ...selection.source.attribute, value: 3 },
  } }, DEFAULT_LIMITS)).toThrow();
  expect(() => contracts.validateContextSelection(selection, {
    ...DEFAULT_LIMITS, render_bytes: 1,
  })).toThrow();
  const base = { messages: [{ role: "user" as const, content: "task" }], metadata: {} };
  const rendered = [{ role: "system" as const, content: "reviewer" }];
  const rebuilt = contracts.applyContextProjection(base, rendered, "prompt", "prepend", DEFAULT_LIMITS);
  const update = contracts.applyContextProjection(base, rendered, "update", "prepend", DEFAULT_LIMITS);
  expect(rebuilt.messages[0]).toEqual(update.messages[1]);
  expect(base.messages).toHaveLength(1);
  expect(() => contracts.applyContextProjection(base, rendered, "prompt", "prepend", {
    ...DEFAULT_LIMITS, render_bytes: 1,
  })).toThrow();
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
      contracts.projectDiscoveredContext({ instructions: [reference], skills: [] }, base, placement, DEFAULT_LIMITS),
      contracts.applyContextProjection(base, [{ role: "system", content: part }], "prompt", placement, DEFAULT_LIMITS),
    ];
    for (const projected of projections) {
      expect(projected.metadata).toEqual(base.metadata);
      expect(Object.hasOwn(projected.metadata, "__proto__")).toBe(true);
      const calls = projected.messages.filter(message => !Array.isArray(message.content)
        && typeof message.content !== "string" && message.content.kind === "tool_call");
      expect(calls[0]?.content).toMatchObject({ arguments: opaque });
      const files = projected.messages.flatMap(message => {
        const parts = typeof message.content === "string" ? []
          : Array.isArray(message.content) ? message.content : [message.content];
        return parts.flatMap(part => part.kind === "file" ? [part.file] : []);
      });
      expect(files).toHaveLength(3);
      for (const file of files) expect(typeof file.descriptor.byte_length).toBe("number");
      expect(() => prepareModelRequest({ model: { provider: "mock", name: "files", revision: "1", options: {} },
        messages: projected.messages, tools: [{ name: "inspect", revision: "1", description: "Inspect files",
          inputSchema: {}, outputSchema: {}, projectionSchema: jsonProjection.schema }], maxOutputTokens: 4096 }, DEFAULT_LIMITS)).not.toThrow();
    }
  }
});

test("native and WASM request construction preserve exact Unicode and paired tool bytes", async () => {
  const fixture = await readFile(new URL("../../../../rust/crates/harness/fixtures/model-request.json", import.meta.url));
  const wire = contracts.decodeModelJson(fixture) as unknown as ModelRequestWire;
  const request = {
    model: wire.model,
    messages: wire.messages,
    tools: wire.tools.map((tool) => ({
      name: tool.name, revision: tool.revision, description: tool.description,
      inputSchema: tool.input_schema, outputSchema: tool.output_schema, projectionSchema: tool.projection_schema,
    })),
    maxOutputTokens: wire.max_output_tokens,
  };
  const bytes = prepareModelRequest(request, DEFAULT_LIMITS);
  expect(bytes).toEqual(Uint8Array.from(fixture));
  const prefix = await readFile(new URL("../../../../rust/crates/harness/fixtures/model-prefix.json", import.meta.url));
  expect(encodeModelPrefix(bytes, null, undefined, DEFAULT_LIMITS)).toEqual(Uint8Array.from(prefix));
  expect(() => prepareModelRequest({ ...request, maxOutputTokens: 0 }, DEFAULT_LIMITS)).toThrow();
  expect(() => prepareModelRequest({ ...request, messages: request.messages.slice(0, 2) }, DEFAULT_LIMITS)).toThrow();
  expect(() => prepareModelRequest({ ...request, tools: [...request.tools, ...request.tools] }, DEFAULT_LIMITS)).toThrow();
});

test("tool results retain their pinned string schema and enforce the exact render bound", async () => {
  const definition = { name: "read", revision: "1", description: "read", inputSchema: { type: "string" },
    outputSchema: { type: "string" }, projection: await jsonToolProjection({ type: "string" }) };
  const limits = { ...DEFAULT_LIMITS, render_bytes: 64 };
  const accepted = { kind: "json" as const, value: "x".repeat(38) };
  expect(contracts.encodeCanonicalJson(accepted).length).toBe(64);
  expect(() => contracts.validateToolProjection(definition, accepted, limits)).not.toThrow();
  const rejected = { kind: "json" as const, value: "x".repeat(39) };
  expect(contracts.encodeCanonicalJson(rejected).length).toBe(65);
  expect(() => contracts.validateToolProjection(definition, rejected, limits)).toThrow();
  expect(() => contracts.validateToolProjection(definition, { kind: "json", value: 42 }, limits)).toThrow();
});

test("stale WASM modules fail compatibility checks before model dispatch", () => {
  expect(() => assertHarnessWasmExports(harnessWasm)).not.toThrow();
  expect(() => assertHarnessWasmExports({ ...harnessWasm, validateModelContent: undefined })).toThrow();
  expect(() => assertHarnessWasmExports({ ...harnessWasm, validateContract: undefined })).toThrow();
  expect(() => assertHarnessWasmExports({ ...harnessWasm, validateUserInput: undefined })).toThrow();
  expect(() => assertHarnessWasmExports({ ...harnessWasm, admitModelEvent: undefined })).toThrow();
  expect(() => assertHarnessWasmExports({ ...harnessWasm, WasmContentStore: undefined })).toThrow();
  expect(() => assertHarnessWasmExports(rawWasmExports)).not.toThrow();
  expect(() => assertHarnessWasmExports({ ...rawWasmExports, wasmcontentstore_stage: undefined })).toThrow();
});

const constructedWasmClasses = [
  ["WasmContentStore", "wasmcontentstore_new"],
  ["WasmStream", "wasmstream_new"],
  ["WasmHypothesisClient", "wasmhypothesisclient_new"],
  ["WasmAgentClient", "wasmagentclient_new"],
  ["WasmWireSession", "wasmwiresession_new"],
  ["WasmPageWindow", "wasmpagewindow_new"],
  ["WasmReducer", "wasmreducer_new"],
  ["WasmProviderRegistry", "wasmproviderregistry_new"],
] as const;

function checkConstructorRemoval(check: (value: unknown) => void): void {
  // Every absent constructor, alone or in combination, must fail before dispatch.
  const removed = (missing: readonly (typeof constructedWasmClasses)[number][]) => {
    const raw: Record<string, unknown> = { ...rawWasmExports };
    const wrappers: Record<string, unknown> = { ...harnessWasm };
    for (const [name, symbol] of missing) {
      delete raw[symbol];
      delete wrappers[name];
    }
    expect(() => check(raw)).toThrow();
    expect(() => check(wrappers)).toThrow();
  };
  for (const entry of constructedWasmClasses) removed([entry]);
  fc.assert(fc.property(fc.subarray([...constructedWasmClasses], { minLength: 1 }), removed), { numRuns: 64 });
}

test("all constructed WASM classes are required before dispatch", () => {
  checkConstructorRemoval(assertHarnessWasmExports);
});

test("constructor-removal oracle rejects the former method-only check", () => {
  const methodOnly = (value: unknown) => {
    const exports = { ...(value as Record<string, unknown>) };
    for (const [, symbol] of constructedWasmClasses) {
      exports[symbol] ??= (rawWasmExports as unknown as Record<string, unknown>)[symbol];
    }
    assertHarnessWasmExports(exports);
  };
  expect(() => checkConstructorRemoval(methodOnly)).toThrow();
});

test("fresh WASM initialization accepts the raw content-store ABI", async () => {
  await ensureHarnessWasm();
});

async function file(): Promise<FileRef> {
  return contracts.validate("file_ref", {
    volume: {
      provider: { namespace: "test", family: "filesystem", version: "2" },
      id: "scratch", class: "agent_private", owner: { kind: "agent", id: agent },
    },
    path: "messages/input.txt", version: "pinned-generation",
    descriptor: await descriptorFor(new TextEncoder().encode("input"), "text/plain"),
    display_name: "input.txt",
  });
}

async function nativeOptionPart() {
  const media = await file();
  const options = { ...media, path: "messages/options.json", display_name: "options.json",
    descriptor: await descriptorFor(new TextEncoder().encode("{}"), "application/json") };
  const configuration = {
    source: { authority: { kind: "agent" as const, id: agent }, revision: 9_007_199_254_740_993n },
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
    content: { kind: "parts", parts: [{ kind: "text", text: "attachment" }, part] } }, DEFAULT_LIMITS);
  expect(inventory.files).toEqual([part.file, part.policy.native.configuration.configuration.content]);
  expect(inventory.nativeConfigurations).toEqual([part.policy.native.configuration]);
  expect(inventory.nativeConfigurations[0]?.source.revision).toBe(9_007_199_254_740_993n);
  expect(contracts.modelContentInventory("plain text", DEFAULT_LIMITS)).toEqual({ files: [], nativeConfigurations: [] });
});

test("synthetic accounting counts nested media and native options with wide revisions", async () => {
  const part = await nativeOptionPart();
  const request: import("../src/model.js").ModelRequest = {
    model: { provider: "mock", name: "nested-accounting", revision: "1", options: {} },
    messages: [
      { role: "assistant", content: { kind: "tool_call", callId: "call", name: "inspect", arguments: {} } },
      { role: "tool", content: { kind: "tool_result", callId: "call", name: "inspect",
        content: { kind: "parts", parts: [part] } } },
    ],
    tools: [{ name: "inspect", revision: "1", description: "inspect", inputSchema: {}, outputSchema: {},
      projectionSchema: {} }],
  };
  const serializedInput = prepareModelRequest(request, DEFAULT_LIMITS);
  const count = await syntheticAccounting(contracts).countTokens({ ...request, serializedInput });
  expect(count.requestDigest).toEqual(contracts.digestCanonicalJson(contracts.decodeCanonicalJson(serializedInput)));
  expect(count.fixedTokens).toBe(serializedInput.byteLength + 512);
  expect(count.messageTokens).toEqual([512, 512 + part.file.descriptor.byte_length
    + part.policy.native.configuration.configuration.content.descriptor.byte_length]);
});

test("public model content preserves original option revisions across Rust context projection", async () => {
  const part = await nativeOptionPart();
  const projected = contracts.applyContextProjection({ messages: [], metadata: {} },
    [{ role: "user", content: part }], "prompt", "append", DEFAULT_LIMITS);
  expect(publicModelContent(projected.messages[0]!.content as NativeModelContent)).toEqual(part);
  const result = contracts.applyContextProjection({ messages: [], metadata: {} }, [
    { role: "assistant", content: { kind: "tool_call", callId: "call", name: "inspect", arguments: {} } },
    { role: "tool", content: { kind: "tool_result", callId: "call", name: "inspect",
      content: { kind: "parts", parts: [part] } } },
  ], "prompt", "append", DEFAULT_LIMITS);
  expect(publicModelContent(result.messages[1]!.content as NativeModelContent))
    .toEqual({ kind: "tool_result", callId: "call", name: "inspect", content: { kind: "parts", parts: [part] } });
});

test("WASM model validators preserve Rust limits and tool-name checks", async () => {
  const content = await file();
  expect(() => validateModelContent("hello", DEFAULT_LIMITS)).not.toThrow();
  expect(() => validateModelContent({ kind: "file", file: content, policy: "reference" }, DEFAULT_LIMITS)).not.toThrow();
  expect(() => validateModelContent({ kind: "tool_call", callId: "call", name: "lookup", arguments: { value: 1 } }, DEFAULT_LIMITS)).not.toThrow();
  expect(() => validateModelContent({ kind: "tool_call", callId: "call", name: "bad name", arguments: {} }, DEFAULT_LIMITS)).toThrow();
  expect(() => validateModelContent({ kind: "text", text: "x".repeat(DEFAULT_LIMITS.render_bytes + 1) }, DEFAULT_LIMITS)).toThrow();
  expect(() => validateModelContent({ kind: "unknown", value: true } as never, DEFAULT_LIMITS)).toThrow();
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

test("Rust model event admission preserves per-step bounds and cumulative UTF-8 output bytes", () => {
  const limits = { ...DEFAULT_LIMITS, model_events_per_step: 2, tool_calls_per_step: 2, file_bytes: 3, render_bytes: 3 };
  let admitted = contracts.admitModelEvent({ kind: "content", delta: "é" }, limits);
  let state = admitted.state;
  expect(admitted.event).toEqual({ kind: "content", delta: "é" });
  expect(state).toEqual({ count: 1, calls: [], completed: false, text_bytes: 2 });
  const completedLimits = { ...limits, model_events_per_step: 3 };
  admitted = contracts.admitModelEvent({ kind: "completed", metadata: {} }, completedLimits, state);
  state = admitted.state;
  expect(state.completed).toBe(true);
  expect(() => contracts.admitModelEvent({ kind: "content", delta: "x" }, completedLimits, state)).toThrow();
  expect(() => contracts.admitModelEvent({ kind: "content", delta: "é" }, limits,
    { count: 0, calls: [], completed: false, text_bytes: 2 })).toThrow();
  expect(() => contracts.admitModelEvent({ kind: "content", delta: "x" }, limits,
    { count: 2, calls: [], completed: false, text_bytes: 0 })).toThrow();
});

test("completed message admission retains exact text without adding it to streamed bytes", () => {
  const limits = { ...DEFAULT_LIMITS, file_bytes: 3, render_bytes: 3 };
  const streamed = contracts.admitModelEvent({ kind: "content", delta: "old" }, limits);
  const admitted = contracts.admitModelEvent({ kind: "message_completed", text: "é" }, limits, streamed.state);
  expect(admitted.event).toEqual({ kind: "message_completed", text: "é" });
  expect(admitted.state.text_bytes).toBe(3);
  expect(() => contracts.admitModelEvent({ kind: "message_completed", text: "long" }, limits, streamed.state)).toThrow();
});

test("authoritative messages preserve the native bounded stream admission", () => {
  const limits = { ...DEFAULT_LIMITS, file_bytes: 4, render_bytes: 4 };
  const first = contracts.admitModelEvent({ kind: "content", delta: "é" }, limits);
  const full = contracts.admitModelEvent({ kind: "content", delta: "ok" }, limits, first.state);
  expect(full.state.text_bytes).toBe(4);
  expect(() => contracts.admitModelEvent({ kind: "content", delta: "x" }, limits, full.state)).toThrow();
  expect(full.state.count).toBe(2);
  const final = contracts.admitModelEvent({ kind: "message_completed", text: "" }, limits, full.state);
  expect(final.event).toEqual({ kind: "message_completed", text: "" });
  expect(final.state.text_bytes).toBe(4);
  expect(() => contracts.admitModelEvent({ kind: "message_completed", text: "ééx" }, limits, final.state)).toThrow();
  expect(final.state.count).toBe(3);
});

test("Rust model event admission rejects duplicate tool identities and accepts camelCase call IDs", () => {
  const limits = { ...DEFAULT_LIMITS, tool_calls_per_step: 1 };
  const first = contracts.admitModelEvent({ kind: "tool_call", callId: "call-1", name: "lookup", arguments: {} }, limits);
  expect(first.event).toEqual({ kind: "tool_call", callId: "call-1", name: "lookup", arguments: {} });
  expect(first.state.calls).toEqual(["call-1"]);
  expect(() => contracts.admitModelEvent({ kind: "tool_call", callId: "call-1", name: "lookup", arguments: {} }, limits, first.state)).toThrow();
  expect(() => contracts.admitModelEvent({ kind: "tool_call", callId: "bad/name", name: "lookup", arguments: {} }, limits)).toThrow();
});

test("model event admission detaches getter-backed provider events", () => {
  let reads = 0;
  const event = {
    kind: "content" as const,
    get delta(): string {
      reads += 1;
      return reads === 1 ? "ok" : "x".repeat(DEFAULT_LIMITS.file_bytes + 1);
    },
  };
  const admitted = contracts.admitModelEvent(event, DEFAULT_LIMITS);
  expect(admitted.event).toEqual({ kind: "content", delta: "ok" });
  expect(reads).toBe(1);
});

test("model event admission rejects unknown event fields", () => {
  expect(() => contracts.admitModelEvent({ kind: "content", delta: "ok", extra: true } as never, DEFAULT_LIMITS)).toThrow();
});

test("model admission enforces portable tool integers and preserves full-width completion metadata", () => {
  const maximum = 9_007_199_254_740_991n;
  const value = maximum + 2n;
  const call = (cursor: bigint) => ({ kind: "tool_call" as const, callId: "bigint-call", name: "lookup",
    arguments: { tokens: 1n, cursor } });
  const admitted = contracts.admitModelEvent(call(maximum), DEFAULT_LIMITS);
  expect(admitted.event).toEqual({ ...call(maximum), arguments: { tokens: 1, cursor: Number(maximum) } });
  for (const cursor of [maximum + 1n, value]) {
    expect(() => contracts.admitModelEvent(call(cursor), DEFAULT_LIMITS)).toThrow("inexact number");
  }
  const literal = { "$serde_json::private::RawValue": "null", ["__proto__"]: { literal: true } };
  const completed = contracts.admitModelEvent({ kind: "completed", metadata: { ...literal, tokens: 1n, cursor: value } }, DEFAULT_LIMITS);
  expect(completed.event).toEqual({ kind: "completed", metadata: { ...literal, tokens: 1, cursor: value } });
});


test("public projection validator enforces complete JSON and Parts envelope byte bounds", () => {
  const definition = { name: "inspect", revision: "1", description: "Inspect", inputSchema: {},
    outputSchema: {}, projection: { schema: { type: "object" },
      project: () => ({ kind: "json" as const, value: null }) } };
  for (const projection of [
    { kind: "json", value: "é\0🦀" },
    { kind: "parts", parts: [{ kind: "text", text: "é\0🦀" }] },
  ] as const) {
    const exact = contracts.encodeCanonicalJson(projection).byteLength;
    expect(() => contracts.validateToolProjection(definition, projection,
      { ...DEFAULT_LIMITS, render_bytes: exact })).not.toThrow();
    expect(() => contracts.validateToolProjection(definition, projection,
      { ...DEFAULT_LIMITS, render_bytes: exact - 1 })).toThrow();
  }
});
