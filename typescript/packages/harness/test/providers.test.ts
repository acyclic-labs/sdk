import { expect, test } from "bun:test";
import fc from "fast-check";
import {
  DEFAULT_LIMITS, ModelRegistry, initializeModelProviders,
  type ModelEvent, type ModelProvider, type ModelRequest,
} from "../src/index.js";
import { prepareModelRequest } from "../generated/wasm/acyclic_harness_wasm.js";

test("Rust model registry implements the public TS provider contract", async () => {
  await initializeModelProviders();
  let submitted: Record<string, unknown> | undefined;
  let headers: Headers | undefined;
  const registry = new ModelRegistry({}, DEFAULT_LIMITS, async (url, init) => {
    if (String(url).endsWith("/count_tokens")) return Response.json({ input_tokens: 10 });
    headers = new Headers(init?.headers);
    if (!(init?.body instanceof Uint8Array)) throw new Error("Rust fetch body must be bytes");
    submitted = JSON.parse(new TextDecoder().decode(init.body)) as Record<string, unknown>;
    return new Response([
      'data: {"choices":[{"delta":{"content":"hi"}}]}',
      'data: {"choices":[{"delta":{"tool_calls":[{"index":0,"id":"call","function":{"name":"echo","arguments":"{}"}}]},"finish_reason":"tool_calls"}]}',
      'data: {"choices":[],"usage":{"total_tokens":3}}',
      "data: [DONE]",
    ].join("\n\n") + "\n\n");
  });
  registry.register("ollama", {
    headers: { "x-owner": "project" },
    adapter: "chat", authentication: "optional", base_url: "http://localhost:11434/v1", max_event_bytes: 4096,
    models: { fixture: { capacity: { context_tokens: 4096, output_tokens: 512 } } },
    token_counter: { path: "count_tokens", adapter: "chat", response_field: "input_tokens", framing_tokens: 0 },
  });
  const selected = registry.resolve("ollama/fixture", { temperature: 0.1 });
  const provider: ModelProvider = selected;
  const model = selected.model;
  const request: ModelRequest = { model, messages: [{ role: "user", content: "hello" }], tools: [] };
  const admitted = { ...request, serializedInput: prepareModelRequest(request, DEFAULT_LIMITS) };
  expect(provider.contextCapacity(model)).toEqual({ contextTokens: 4096, outputTokens: 512 });
  using prepared = await provider.prepare(admitted);
  const count = await prepared.countTokens();
  expect(count.inputTokens).toBe(10n);
  expect(count.requestDigest).toBeInstanceOf(Uint8Array);
  expect(count.requestDigest.length).toBe(32);
  expect(count.messageTokens.length).toBe(1);
  expect(submitted).toBeUndefined();
  const events: ModelEvent[] = [];
  const dispatch = { operationId: "00000000-0000-4000-8000-000000000001", step: 0 };
  for await (const event of prepared.generate(dispatch)) events.push(event);
  expect(events[0]).toEqual({ kind: "content", delta: "hi" });
  const call = events.find(event => event.kind === "tool_call");
  expect(call?.kind).toBe("tool_call");
  if (call?.kind !== "tool_call") throw new Error("missing tool call");
  expect(call.callId).toStartWith(`${dispatch.operationId}:`);
  expect(call.name).toBe("echo");
  expect(call.arguments).toEqual({});
  expect(call.metadata).toBeDefined();
  expect(events.at(-1)).toEqual({ kind: "completed", metadata: { finish_reason: "tool_calls", usage: { total_tokens: 3n } } });
  expect(submitted?.stream).toBe(true);
  expect(submitted?.temperature).toBe(0.1);
  expect(headers?.get("x-owner")).toBe("project");
  expect(await provider.reconcile({ ...dispatch, requestDigest: count.requestDigest, observed: [] })).toBeUndefined();
  selected.free();
  registry.free();
});

test("generated alloy bindings use full-width durable turns and minimum capacity", async () => {
  await initializeModelProviders();
  fc.assert(fc.property(fc.array(fc.integer({min:1024,max:65536}), {minLength:2,maxLength:6}), fc.bigInt({min:0n,max:0xffff_ffff_ffff_ffffn}), (capacities, ordinal) => {
    const registry = new ModelRegistry({}, DEFAULT_LIMITS, async () => { throw new Error("alloy resolution performed HTTP"); });
    registry.register("local", {
      adapter:"chat", authentication:"optional", base_url:"http://localhost/v1", max_event_bytes:4096,
      token_counter:{path:"count_tokens",adapter:"chat",response_field:"input_tokens",framing_tokens:0},
      models:Object.fromEntries(capacities.map((capacity,index) => [`model-${index}`, {capacity:{context_tokens:capacity,output_tokens:512},features:{tools:true,images:true}}])),
    });
    const models = capacities.map((_,index) => `local/model-${index}`);
    const alloy = registry.resolveAlloy({models,select:"round_robin"}, {tools:true,images:true});
    const operation = "00000000-0000-4000-8000-000000000001";
    const expected = Number(ordinal % BigInt(models.length));
    const selected = alloy.select(operation, ordinal, "worker");
    const resumed = alloy.select(operation, ordinal, "worker");
    expect(selected.model.name).toBe(`model-${expected}`);
    expect(resumed.model).toEqual(selected.model);
    expect(alloy.capacity).toEqual({contextTokens:Math.min(...capacities),outputTokens:512});
    expect(selected.contextCapacity(selected.model)).toEqual(alloy.capacity);
    selected.free(); resumed.free(); alloy.free(); registry.free();
  }), {numRuns:64});
});

test("the alloy binding oracle detects truncating the durable ordinal", async () => {
  // u64::MAX is odd; conversion through an unsafe JS Number rounds it to even.
  const ordinal = 0xffff_ffff_ffff_ffffn;
  const expected = Number(ordinal % 2n);
  expect(() => expect(Number(BigInt(Number(ordinal)) % 2n)).toBe(expected)).toThrow();
});
