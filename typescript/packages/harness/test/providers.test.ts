import { expect, test } from "bun:test";
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
  const count = await provider.countTokens(admitted);
  expect(count.inputTokens).toBe(10n);
  expect(count.requestDigest).toBeInstanceOf(Uint8Array);
  expect(count.requestDigest.length).toBe(32);
  expect(count.messageTokens.length).toBe(1);
  expect(submitted).toBeUndefined();
  const events: ModelEvent[] = [];
  const dispatch = { operationId: "00000000-0000-4000-8000-000000000001", step: 0 };
  for await (const event of provider.generate(admitted, dispatch)) events.push(event);
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
