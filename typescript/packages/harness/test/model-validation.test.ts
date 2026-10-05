import { expect, test } from "bun:test";
import {
  DEFAULT_LIMITS, NativeContracts, descriptorFor, type AgentId, type FileRef, type NativeLimitsWire,
} from "../src/index.js";
import {
  validateModelContent, validateUserInput,
  prepareModelRequest, encodeCanonicalJson, digestCanonicalJson,
} from "../generated/wasm/acyclic_harness_wasm.js";
import type { WasmModelLimitsInput, WasmModelRequestWire } from "../generated/wasm/acyclic_harness_wasm.js";
import * as harnessWasm from "../generated/wasm/acyclic_harness_wasm.js";
import initWasm from "../generated/wasm/acyclic_harness_wasm.js";
import { assertHarnessWasmExports, ensureHarnessWasm } from "../src/wasm-runtime.js";

const contracts = await NativeContracts.create();
const rawWasmExports = await initWasm();
const agent = "07070707-0707-0707-0707-070707070707" as AgentId;

function modelRequest(): WasmModelRequestWire {
  return {
    model: { provider: "mock", name: "swarm", revision: "1", options: {} },
    messages: [
      { role: "user", content: "hello" },
      { role: "assistant", content: {
        kind: "tool_call", call_id: "fork-1", name: "fork", arguments: { child: "a" },
      } },
      { role: "tool", content: {
        kind: "tool_result", call_id: "fork-1", name: "fork", value: { accepted: true },
      } },
      { role: "assistant", content: "completed" },
    ],
    tools: [{
      name: "fork", revision: "1", description: "create a child",
      input_schema: { type: "object" }, output_schema: { type: "object" },
      model_output_schema: { type: "object" },
    }],
    max_output_tokens: 100,
  };
}

function modelLimits(
  file_bytes = DEFAULT_LIMITS.file_bytes,
  render_bytes = DEFAULT_LIMITS.render_bytes,
): WasmModelLimitsInput {
  return { ...DEFAULT_LIMITS, file_bytes, render_bytes };
}

test("stale WASM modules fail compatibility checks before model dispatch", () => {
  expect(() => assertHarnessWasmExports(harnessWasm)).not.toThrow();
  expect(() => assertHarnessWasmExports({ ...harnessWasm, validateModelContent: undefined })).toThrow("required validators");
  expect(() => assertHarnessWasmExports({ ...harnessWasm, validateContract: undefined })).toThrow("required validators");
  expect(() => assertHarnessWasmExports({ ...harnessWasm, validateUserInput: undefined })).toThrow("required validators");
  expect(() => assertHarnessWasmExports({ ...harnessWasm, admitModelEvent: undefined })).toThrow("required validators");
  expect(() => assertHarnessWasmExports({ ...harnessWasm, prepareModelRequest: undefined })).toThrow("required validators");
  expect(() => assertHarnessWasmExports({ ...harnessWasm, validateToolProjection: undefined })).toThrow("required validators");
  expect(() => assertHarnessWasmExports({ ...harnessWasm, WasmContentStore: undefined })).toThrow("required validators");
  expect(() => assertHarnessWasmExports(rawWasmExports)).not.toThrow();
  expect(() => assertHarnessWasmExports({ ...rawWasmExports, wasmcontentstore_stage: undefined })).toThrow("required validators");
});

test("fresh WASM initialization accepts the raw content-store ABI", async () => {
  await ensureHarnessWasm();
});

test("generated WASM model admission enforces the aggregate request byte limit", () => {
  const request = modelRequest();
  const bytes = encodeCanonicalJson(request);
  expect(bytes.byteLength).toBeGreaterThan(1);
  expect(() => prepareModelRequest(request, modelLimits(bytes.byteLength - 1, bytes.byteLength - 1), null)).toThrow("aggregate model request exceeds byte limit");
  expect(() => prepareModelRequest(request, modelLimits(bytes.byteLength, bytes.byteLength), null)).not.toThrow();
});

test("generated WASM model admission requires complete tool-call/result pairings", () => {
  const request = modelRequest();
  const incomplete = { ...request, messages: request.messages.slice(0, 2) };
  expect(() => prepareModelRequest(incomplete, modelLimits(), null)).toThrow("unfinished tool calls");

  const mismatched = {
    ...request,
    messages: request.messages.map((message, index) => index === 2
      ? { ...message, content: {
        kind: "tool_result" as const, call_id: "other", name: "fork", value: { accepted: true },
      } }
      : message),
  };
  expect(() => prepareModelRequest(mismatched, modelLimits(), null)).toThrow("tool result is not paired");
});

test("raw WASM and the TypeScript facade reject unknown and schema-invalid tool results", () => {
  const request = modelRequest();
  const limits = {
    ...DEFAULT_LIMITS,
    file_bytes: BigInt(DEFAULT_LIMITS.file_bytes),
    path_bytes: BigInt(DEFAULT_LIMITS.path_bytes),
    attachments: BigInt(DEFAULT_LIMITS.attachments),
    render_bytes: BigInt(DEFAULT_LIMITS.render_bytes),
    model_steps: BigInt(DEFAULT_LIMITS.model_steps),
    model_events_per_step: BigInt(DEFAULT_LIMITS.model_events_per_step),
    tool_calls_per_step: BigInt(DEFAULT_LIMITS.tool_calls_per_step),
    context_messages: BigInt(DEFAULT_LIMITS.context_messages),
  } satisfies NativeLimitsWire;
  // Establish a valid control through both entry points first, so each
  // rejection below proves the intended admission rule rather than an import,
  // ABI, or limits failure.
  expect(() => prepareModelRequest(request, modelLimits(), null)).not.toThrow();
  expect(() => contracts.prepareModelRequest(request, limits, null)).not.toThrow();
  const unknownResult = {
    ...request,
    messages: request.messages.map((message, index) => index === 2
      ? { ...message, content: {
        kind: "tool_result" as const, call_id: "fork-1", name: "missing-tool", value: { accepted: true },
      } }
      : message),
  };
  expect(() => prepareModelRequest(unknownResult, modelLimits(), null))
    .toThrow("tool result names unknown tool missing-tool");
  expect(() => contracts.prepareModelRequest(unknownResult, limits, null))
    .toThrow("tool result names unknown tool missing-tool");

  const schemaInvalid = {
    ...request,
    tools: request.tools.map(tool => ({
      ...tool,
      model_output_schema: {
        type: "object", properties: { accepted: { type: "boolean" } },
        required: ["accepted"], additionalProperties: false,
      } as const,
    })),
    messages: request.messages.map((message, index) => index === 2
      ? { ...message, content: {
        kind: "tool_result" as const, call_id: "fork-1", name: "fork", value: { accepted: "yes" },
      } }
      : message),
  };
  expect(() => prepareModelRequest(schemaInvalid, modelLimits(), null))
    .toThrow("tool projection failed validation");
  expect(() => contracts.prepareModelRequest(schemaInvalid, limits, null))
    .toThrow("tool projection failed validation");
});

test("generated WASM model admission captures canonical request bytes and manifest identity", () => {
  const request = modelRequest();
  const prepared = prepareModelRequest(request, modelLimits(), null) as {
    request_json: string;
    manifest_json: string;
    request_digest: readonly number[];
  };
  const decoder = new TextDecoder();
  expect(prepared.request_json).toBe(decoder.decode(encodeCanonicalJson(request)));
  // Compare detached byte values so this assertion remains exact across
  // ArrayBuffer and ArrayBufferLike lib definitions.
  expect(Array.from(prepared.request_digest)).toEqual(Array.from(digestCanonicalJson(request)));

  const manifest = JSON.parse(prepared.manifest_json) as {
    version: number;
    binding_digest: readonly number[];
    request_digest: readonly number[];
    messages: readonly { position: number; role: string; digest: readonly number[]; files: readonly unknown[] }[];
  };
  expect(manifest.version).toBe(3);
  expect(manifest.binding_digest).toHaveLength(32);
  expect(manifest.request_digest).toEqual(prepared.request_digest);
  expect(manifest.messages.map(message => ({ position: message.position, role: message.role })))
    .toEqual(request.messages.map((message, position) => ({ position, role: message.role })));
  expect(manifest.messages.every(message => message.digest.length === 32)).toBe(true);
  expect(manifest.messages.every(message => message.files.length === 0)).toBe(true);

  const throughFacade = contracts.prepareModelRequest(request, {
    file_bytes: BigInt(DEFAULT_LIMITS.file_bytes),
    path_bytes: BigInt(DEFAULT_LIMITS.path_bytes),
    attachments: BigInt(DEFAULT_LIMITS.attachments),
    render_bytes: BigInt(DEFAULT_LIMITS.render_bytes),
    model_steps: BigInt(DEFAULT_LIMITS.model_steps),
    model_events_per_step: BigInt(DEFAULT_LIMITS.model_events_per_step),
    tool_calls_per_step: BigInt(DEFAULT_LIMITS.tool_calls_per_step),
    context_messages: BigInt(DEFAULT_LIMITS.context_messages),
  } satisfies NativeLimitsWire);
  expect(throughFacade.requestJson).toBe(prepared.request_json);
  expect(throughFacade.requestDigest).toEqual(prepared.request_digest);

  const schema = { type: "object", additionalProperties: false, properties: { mode: { type: "string" } } } as const;
  const policy = {
    name: "model-options",
    version: "1",
    digest: Array.from({ length: 32 }, (_, index) => index + 1),
    schema,
  };
  const withPolicy = contracts.prepareModelRequest(request, {
    file_bytes: BigInt(DEFAULT_LIMITS.file_bytes),
    path_bytes: BigInt(DEFAULT_LIMITS.path_bytes),
    attachments: BigInt(DEFAULT_LIMITS.attachments),
    render_bytes: BigInt(DEFAULT_LIMITS.render_bytes),
    model_steps: BigInt(DEFAULT_LIMITS.model_steps),
    model_events_per_step: BigInt(DEFAULT_LIMITS.model_events_per_step),
    tool_calls_per_step: BigInt(DEFAULT_LIMITS.tool_calls_per_step),
    context_messages: BigInt(DEFAULT_LIMITS.context_messages),
  } satisfies NativeLimitsWire, policy);
  const policyManifest = JSON.parse(withPolicy.manifestJson) as Record<string, unknown>;
  expect(policyManifest.model_option_policy).toEqual({ name: policy.name, version: policy.version, digest: policy.digest });
  expect(policyManifest.model_option_schema_digest).toEqual(Array.from(contracts.digestCanonicalJson(schema)));
});

test("native facade refuses forged model-input manifest fields and binding identity", () => {
  const request = modelRequest();
  const limits = {
    file_bytes: BigInt(DEFAULT_LIMITS.file_bytes),
    path_bytes: BigInt(DEFAULT_LIMITS.path_bytes),
    attachments: BigInt(DEFAULT_LIMITS.attachments),
    render_bytes: BigInt(DEFAULT_LIMITS.render_bytes),
    model_steps: BigInt(DEFAULT_LIMITS.model_steps),
    model_events_per_step: BigInt(DEFAULT_LIMITS.model_events_per_step),
    tool_calls_per_step: BigInt(DEFAULT_LIMITS.tool_calls_per_step),
    context_messages: BigInt(DEFAULT_LIMITS.context_messages),
  } satisfies NativeLimitsWire;
  const nativeSlot = contracts as unknown as { native: Record<string, unknown> };
  const originalNative = nativeSlot.native;
  const admitted = harnessWasm.prepareModelRequest(request, modelLimits(), null) as {
    request_json: string; manifest_json: string; request_digest: readonly number[];
  };
  try {
    const manifest = JSON.parse(admitted.manifest_json) as Record<string, unknown>;
    nativeSlot.native = {
      ...originalNative,
      prepareModelRequest: () => ({
        ...admitted,
        manifest_json: new TextDecoder().decode(encodeCanonicalJson({ ...manifest, unexpected: true })),
      }),
    };
    // The native validator owns manifest schema errors; keep this assertion
    // independent of serde's diagnostic wording.
    expect(() => contracts.prepareModelRequest(request, limits, null)).toThrow();

    nativeSlot.native = {
      ...originalNative,
      prepareModelRequest: () => ({
        ...admitted,
        manifest_json: new TextDecoder().decode(encodeCanonicalJson({ ...manifest, binding_digest: Array(32).fill(0) })),
      }),
    };
    expect(() => contracts.prepareModelRequest(request, limits, null)).toThrow();
  } finally {
    nativeSlot.native = originalNative;
  }
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
  expect(() => validateUserInput({ kind: "file", file: content, policy: "native" })).not.toThrow();
  expect(() => validateUserInput("")).toThrow();
  expect(() => validateUserInput([])).toThrow();
  expect(() => validateUserInput({ kind: "tool_result", callId: "call", name: "lookup", value: {} })).toThrow();
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
  expect(() => contracts.admitModelEvent({ kind: "content", delta: "x" }, completedLimits, state)).toThrow("after completion");
  expect(() => contracts.admitModelEvent({ kind: "content", delta: "é" }, limits,
    { count: 0, calls: [], completed: false, text_bytes: 2 })).toThrow("assistant output exceeds file limit");
  expect(() => contracts.admitModelEvent({ kind: "content", delta: "x" }, limits,
    { count: 2, calls: [], completed: false, text_bytes: 0 })).toThrow("model event limit exceeded");
});

test("Rust model event admission rejects duplicate tool identities and accepts camelCase call IDs", () => {
  const limits = { ...DEFAULT_LIMITS, tool_calls_per_step: 1 };
  const first = contracts.admitModelEvent({ kind: "tool_call", callId: "call-1", name: "lookup", arguments: {} }, limits);
  expect(first.event).toEqual({ kind: "tool_call", callId: "call-1", name: "lookup", arguments: {} });
  expect(first.state.calls).toEqual(["call-1"]);
  expect(() => contracts.admitModelEvent({ kind: "tool_call", callId: "call-1", name: "lookup", arguments: {} }, limits, first.state)).toThrow("repeated");
  expect(() => contracts.admitModelEvent({ kind: "tool_call", callId: "bad/name", name: "lookup", arguments: {} }, limits)).toThrow("identity");
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

test("model event admission preserves full-width BigInts in provider JSON", () => {
  const value = 9_007_199_254_740_993n;
  const toolCall = contracts.admitModelEvent({
    kind: "tool_call", callId: "bigint-call", name: "lookup", arguments: { tokens: 1n, cursor: value },
  }, DEFAULT_LIMITS);
  expect(toolCall.event).toEqual({
    kind: "tool_call", callId: "bigint-call", name: "lookup", arguments: { tokens: 1, cursor: value },
  });

  const completed = contracts.admitModelEvent({ kind: "completed", metadata: { tokens: 1n, cursor: value } }, DEFAULT_LIMITS);
  expect(completed.event).toEqual({ kind: "completed", metadata: { tokens: 1, cursor: value } });
});
