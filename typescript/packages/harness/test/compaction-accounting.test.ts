import { expect, test } from "bun:test";
import {
  DEFAULT_LIMITS, NativeContracts, type ModelTokenCount,
} from "../src/index.js";
import type { WasmModelRequestWire } from "../generated/wasm/acyclic_harness_wasm.js";

const contracts = await NativeContracts.create();

test("canonical accounting rejects lossy UTF-16 requests, values and keys", () => {
  for (const value of ["replacement \ufffd", "paired \ud83d\ude00", "\u0000"]) {
    const object = { [value]: value };
    expect(contracts.decodeModelJson(contracts.encodeCanonicalJson(object))).toEqual(object);
  }
  for (const invalid of ["\ud800", "\udc00", "\ud800x", "x\udc00"]) {
    expect(() => contracts.decodeModelJson(new TextEncoder().encode(JSON.stringify({ value: invalid })))).toThrow();
    for (const value of [{ value: invalid }, { [invalid]: 1 }, new Map([[invalid, 1]])]) {
      expect(() => contracts.encodeCanonicalJson(value)).toThrow();
      expect(() => contracts.digestCanonicalJson(value)).toThrow();
    }
    const replacement = new TextDecoder().decode(new TextEncoder().encode(invalid));
    const request: WasmModelRequestWire = {
      model: { provider: "synthetic", name: "byte-counter", revision: "1", options: {} },
      messages: [{ role: "user", content: replacement }], tools: [], max_output_tokens: 1_024,
    };
    const count: ModelTokenCount = {
      request_digest: Array.from(contracts.digestCanonicalJson(request)) as ModelTokenCount["request_digest"],
      fixed_tokens: 100, message_tokens: [new TextEncoder().encode(replacement).byteLength + 10],
    };
    expect(contracts.validateModelTokenCount(request, count, DEFAULT_LIMITS)).toBeGreaterThan(100n);
    expect(() => contracts.validateModelTokenCount({ ...request,
      messages: [{ role: "user", content: invalid }] }, count, DEFAULT_LIMITS)).toThrow();
  }
});

test("generated optional current-input marker matches actual context projection output", () => {
  const source = { instructions: [], skills: [] };
  const standalone = contracts.projectDiscoveredContext(source,
    { messages: [], metadata: {} }, "prepend", DEFAULT_LIMITS);
  expect(Object.hasOwn(standalone, "current_input_index")).toBe(false);
  const current = contracts.projectDiscoveredContext(source,
    { messages: [{ role: "user", content: "current" }], metadata: {}, current_input_index: 0 },
    "prepend", DEFAULT_LIMITS);
  expect(current.current_input_index).toBe(0);
});

test("generated compaction policy validates actual capacity and finite output in Rust", () => {
  const policy = contracts.defaultCompactionPolicy();
  expect(policy.kind).toBe("threshold");
  if (policy.kind !== "threshold") throw new Error("expected stock threshold policy");
  expect(policy.config.response_reserve_tokens).toBe(16_384);
  expect(policy.config.recent_tokens).toBe(20_000);
  expect(policy.config.retention).toEqual({ roles: ["system"], native_media: true });
  expect(Object.isFrozen(policy.config)).toBe(true);
  const capacity = { context_tokens: 65_536, output_tokens: 4_096 };
  expect(contracts.validateThresholdCompaction(policy.config, capacity)).toBe(4_096);
  const replacement = { ...policy.config, response_reserve_tokens: 1_024, recent_tokens: 2_048 };
  expect(contracts.validateThresholdCompaction(replacement, capacity)).toBe(1_024);
  expect(() => contracts.validateThresholdCompaction(policy.config,
    { ...capacity, context_tokens: 32_768 })).toThrow();
  expect(() => contracts.validateThresholdCompaction(policy.config,
    { ...capacity, output_tokens: 0 })).toThrow();
  for (const output of [0, -1, 0.5, 1.5, 4_097, 4_294_967_296, Infinity, NaN]) {
    expect(() => contracts.validateThresholdCompaction(policy.config, capacity, output)).toThrow();
  }
});

test("generated provider accounting binds canonical requests, dimensions and portable sums", () => {
  const request: WasmModelRequestWire = {
    model: { provider: "synthetic", name: "byte-counter", revision: "1", options: {} },
    messages: [{ role: "user", content: "Ã©ðŸ¦€" }],
    tools: [],
    max_output_tokens: 1_024,
  };
  // This consumer's declared token units are UTF-8 bytes plus fixed framing.
  const count: ModelTokenCount = {
    request_digest: Array.from(contracts.digestCanonicalJson(request)) as ModelTokenCount["request_digest"],
    fixed_tokens: 100,
    message_tokens: [new TextEncoder().encode("Ã©ðŸ¦€").byteLength + 10],
  };
  expect(contracts.validateModelTokenCount(request, count, DEFAULT_LIMITS)).toBe(116n);
  expect(contracts.validateModelTokenCount(request,
    { ...count, fixed_tokens: 0xffff_ffff, message_tokens: [0xffff_ffff] }, DEFAULT_LIMITS))
    .toBe(8_589_934_590n);
  expect(() => contracts.validateModelTokenCount(
    { ...request, messages: [{ role: "user", content: "changed" }] }, count, DEFAULT_LIMITS)).toThrow();
  for (const messageTokens of [[], [1, 2]]) {
    expect(() => contracts.validateModelTokenCount(request,
      { ...count, message_tokens: messageTokens }, DEFAULT_LIMITS)).toThrow();
  }
  const wrongDigest = count.request_digest.slice() as ModelTokenCount["request_digest"];
  wrongDigest[0] = (wrongDigest[0] ?? 0) ^ 1;
  expect(() => contracts.validateModelTokenCount(request,
    { ...count, request_digest: wrongDigest }, DEFAULT_LIMITS)).toThrow();
  for (const invalid of [-1, 0.5, 4_294_967_296, Infinity, NaN]) {
    expect(() => contracts.validateModelTokenCount(request,
      { ...count, fixed_tokens: invalid }, DEFAULT_LIMITS)).toThrow();
    expect(() => contracts.validateModelTokenCount(request,
      { ...count, message_tokens: [invalid] }, DEFAULT_LIMITS)).toThrow();
  }
  expect(() => contracts.validateModelTokenCount(request,
    { ...count, unexpected: true } as ModelTokenCount, DEFAULT_LIMITS)).toThrow();
});


test("Rust execution checkpoint wire preserves ref-only observations and rejects malformed admission", () => {
  const file = {
    volume: { provider: { namespace: "test", family: "memory", version: "1" },
      id: "private", class: "agent_private", owner: { kind: "agent", id: "07070707-0707-0707-0707-070707070707" } },
    path: "context.json", version: "1",
    descriptor: contracts.fileDescriptor(new TextEncoder().encode("null"), "application/json"),
    display_name: "context.json",
  } as const;
  const event = { kind: "context_compacted", step: 7, projection: file, compaction: file, accounting: file } as const;
  const bytes = contracts.encodeCanonicalJson(event);
  const decoded = contracts.decodeExecutionEventJson(bytes);
  expect(decoded).toEqual(event);
  expect(contracts.encodeCanonicalJson(decoded)).toEqual(bytes);
  expect(Object.isFrozen(decoded)).toBe(true);
  if (decoded.kind !== "context_compacted") throw new Error("wrong generated event kind");
  expect(decoded.accounting.descriptor.byte_length).toBe(4);
  expect(Object.isFrozen(decoded.projection.descriptor)).toBe(true);
  for (const malformed of [
    { ...event, step: -1 }, { ...event, step: 0.5 }, { ...event, step: 4_294_967_296 },
    { ...event, accounting: null }, { ...event, projection: { ...file, path: "../private" } },
    { ...event, inline_context: "hidden" }, { ...event, kind: "invented_checkpoint" },
  ]) {
    expect(() => contracts.decodeExecutionEventJson(contracts.encodeCanonicalJson(malformed))).toThrow();
  }
  expect(() => contracts.decodeExecutionEventJson(new TextEncoder().encode(JSON.stringify(event)))).toThrow();
});
