import { expect, test } from "bun:test";
import {
  DEFAULT_LIMITS, NativeContracts, descriptorFor, type AgentId, type FileRef,
} from "../src/index.js";
import {
  validateModelContent, validateUserInput,
} from "../generated/wasm/acyclic_harness_wasm.js";
import * as harnessWasm from "../generated/wasm/acyclic_harness_wasm.js";
import initWasm from "../generated/wasm/acyclic_harness_wasm.js";
import { assertHarnessWasmExports, ensureHarnessWasm } from "../src/wasm-runtime.js";

const contracts = await NativeContracts.create();
const rawWasmExports = await initWasm();
const agent = "07070707-0707-0707-0707-070707070707" as AgentId;

test("stale WASM modules fail compatibility checks before model dispatch", () => {
  expect(() => assertHarnessWasmExports(harnessWasm)).not.toThrow();
  expect(() => assertHarnessWasmExports({ ...harnessWasm, validateModelContent: undefined })).toThrow("required validators");
  expect(() => assertHarnessWasmExports({ ...harnessWasm, validateUserInput: undefined })).toThrow("required validators");
  expect(() => assertHarnessWasmExports({ ...harnessWasm, admitModelEvent: undefined })).toThrow("required validators");
  expect(() => assertHarnessWasmExports({ ...harnessWasm, WasmContentStore: undefined })).toThrow("required validators");
  expect(() => assertHarnessWasmExports(rawWasmExports)).not.toThrow();
  expect(() => assertHarnessWasmExports({ ...rawWasmExports, wasmcontentstore_stage: undefined })).toThrow("required validators");
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

test("WASM model validators preserve Rust limits and tool-name checks", async () => {
  const content = await file();
  expect(() => validateModelContent("hello", DEFAULT_LIMITS)).not.toThrow();
  expect(() => validateModelContent({ kind: "file", file: content, policy: "reference" }, DEFAULT_LIMITS)).not.toThrow();
  expect(() => validateModelContent({ kind: "tool_call", callId: "call", name: "lookup", arguments: { value: 1 } }, DEFAULT_LIMITS)).not.toThrow();
  expect(() => validateModelContent({ kind: "tool_call", callId: "call", name: "bad name", arguments: {} }, DEFAULT_LIMITS)).toThrow();
  expect(() => validateModelContent({ kind: "text", text: "x".repeat(DEFAULT_LIMITS.render_bytes + 1) }, DEFAULT_LIMITS)).toThrow();
  expect(() => validateModelContent({ kind: "unknown", value: true }, DEFAULT_LIMITS)).toThrow();
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
