import { expect, test } from "bun:test";
import { OpenAiCompatibleProvider, type ModelRequest } from "../src/index.js";

test("OpenAI-compatible streams project content, tools, and completion", async () => {
  let submitted: Record<string, unknown> | undefined;
  const provider = new OpenAiCompatibleProvider({
    baseUrl: "https://example.test/v1",
    fetcher: async (_, init) => {
      submitted = JSON.parse(String(init?.body)) as Record<string, unknown>;
      const chunks = [
        { id: "run", model: "owned", choices: [{ delta: { content: "hi" } }] },
        { choices: [{ delta: { tool_calls: [{ index: 0, id: "call", function: { name: "echo", arguments: "{\"value\":" } }] } }] },
        { usage: { total_tokens: 3 }, choices: [{ delta: { tool_calls: [{ index: 0, function: { arguments: "1}" } }] } }] },
      ].map(value => `data: ${JSON.stringify(value)}\n\n`).join("") + "data: [DONE]\n\n";
      return new Response(chunks, { headers: { "content-type": "text/event-stream" } });
    },
  });
  const request: ModelRequest = {
    model: { provider: "openai-compatible", name: "owned", revision: "r1", options: { stream: false } },
    messages: [{ role: "user", content: "hello" }],
    tools: [{ name: "echo", revision: "1", description: "Echo", inputSchema: { type: "object" }, outputSchema: {} }],
  };
  const events = [];
  for await (const event of provider.generate(request)) events.push(event);
  expect(submitted?.stream).toBe(true);
  expect(events).toEqual([
    { kind: "content", delta: "hi" },
    { kind: "tool_call", callId: "call", name: "echo", arguments: { value: 1 } },
    { kind: "completed", metadata: { id: "run", model: "owned", usage: { total_tokens: 3 } } },
  ]);
  expect(await provider.reconcile({ operationId: "one", step: 0, requestDigest: new Uint8Array(32), observed: [] })).toBeUndefined();
});

test("OpenAI-compatible streams reject truncated EOF", async () => {
  const provider = new OpenAiCompatibleProvider({
    baseUrl: "https://example.test/v1",
    fetcher: async () => new Response('data: {"choices":[{"delta":{"content":"partial"}}]}\n\n'),
  });
  const request: ModelRequest = {
    model: { provider: "openai-compatible", name: "owned", revision: "r1", options: {} },
    messages: [],
    tools: [],
  };
  const consume = async () => {
    for await (const _ of provider.generate(request)) { /* consume */ }
  };
  await expect(consume()).rejects.toThrow("terminal marker");
});
