import { expect, test } from "bun:test";
import * as contracts from "../generated/wasm/acyclic_harness_wasm.js";
import { initializeModelProviders } from "../src/providers.js";
import { MODEL_TEST_LIMITS as fixtureLimits } from "./support/model-limits.mjs";

await initializeModelProviders();

test("Rust model event admission preserves per-step bounds and cumulative UTF-8 output bytes", () => {
  const limits = { ...fixtureLimits, model_events_per_step: 2, tool_calls_per_step: 2, file_bytes: 3, render_bytes: 3 };
  let admitted = contracts.admitModelEvent({ kind: "content", delta: "é" }, limits, null);
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
  const limits = { ...fixtureLimits, file_bytes: 3, render_bytes: 3 };
  const streamed = contracts.admitModelEvent({ kind: "content", delta: "old" }, limits, null);
  const admitted = contracts.admitModelEvent({ kind: "message_completed", text: "é" }, limits, streamed.state);
  expect(admitted.event).toEqual({ kind: "message_completed", text: "é" });
  expect(admitted.state.text_bytes).toBe(3);
  expect(() => contracts.admitModelEvent({ kind: "message_completed", text: "long" }, limits, streamed.state)).toThrow();
});

test("authoritative messages preserve the native bounded stream admission", () => {
  const limits = { ...fixtureLimits, file_bytes: 4, render_bytes: 4 };
  const first = contracts.admitModelEvent({ kind: "content", delta: "é" }, limits, null);
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

test("model event admission detaches getter-backed provider events", () => {
  let reads = 0;
  const event = {
    kind: "content" as const,
    get delta(): string {
      reads += 1;
      return reads === 1 ? "ok" : "x".repeat(fixtureLimits.file_bytes + 1);
    },
  };
  const admitted = contracts.admitModelEvent(event, fixtureLimits, null);
  expect(admitted.event).toEqual({ kind: "content", delta: "ok" });
  expect(reads).toBe(1);
});

test("model event admission rejects unknown event fields", () => {
  expect(() => contracts.admitModelEvent({ kind: "content", delta: "ok", extra: true } as never, fixtureLimits, null)).toThrow();
});

test("public projection validator enforces complete JSON and Parts envelope byte bounds", () => {
  const definition = { name: "inspect", revision: "1", description: "Inspect", inputSchema: {},
    outputSchema: {}, projectionSchema: { type: "object" } };
  for (const projection of [
    { kind: "json", value: "é\0🦀" },
    { kind: "parts", parts: [{ kind: "text", text: "é\0🦀" }] },
  ] as const) {
    const exact = contracts.encodeCanonicalJson(projection).byteLength;
    expect(() => contracts.validateToolProjection(definition, projection,
      { ...fixtureLimits, render_bytes: exact })).not.toThrow();
    expect(() => contracts.validateToolProjection(definition, projection,
      { ...fixtureLimits, render_bytes: exact - 1 })).toThrow();
  }
});

test("Rust model event admission rejects duplicate tool identities and accepts camelCase call IDs", () => {
  const limits = { ...fixtureLimits, tool_calls_per_step: 1 };
  const first = contracts.admitModelEvent({ kind: "tool_call", callId: "call-1", name: "lookup", arguments: {} }, limits, null);
  expect(contracts.decodeClientJson(contracts.encodeCanonicalJson(first.event))).toEqual({ kind: "tool_call", call_id: "call-1", name: "lookup", arguments: {} });
  expect(first.state.calls).toEqual(["call-1"]);
  expect(() => contracts.admitModelEvent({ kind: "tool_call", callId: "call-1", name: "lookup", arguments: {} }, limits, first.state)).toThrow();
  expect(() => contracts.admitModelEvent({ kind: "tool_call", callId: "bad/name", name: "lookup", arguments: {} }, limits, null)).toThrow();
});
