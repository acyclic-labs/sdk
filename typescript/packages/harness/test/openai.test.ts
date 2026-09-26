import { expect, test } from "bun:test";
import { OpenAiCompatibleProvider, descriptorFor, type FileRef, type ModelEvent, type ModelRequest } from "../src/index.js";

test("OpenAI-compatible endpoint admission is unambiguous and credential-free", async () => {
  for (const baseUrl of [
    "javascript:alert(1)", "ftp://example.test/v1", "http://example.test/v1",
    "https://user:password@example.test/v1", "https://example.test/v1?token=x",
    "https://example.test/v1#fragment", "https://example.test/v1?", "https://example.test/v1#",
  ]) {
    expect(() => new OpenAiCompatibleProvider({ baseUrl })).toThrow();
  }
  let endpoint = "";
  const provider = new OpenAiCompatibleProvider({ baseUrl: "http://127.0.0.1:1234/v1", fetcher: async input => {
    endpoint = String(input);
    return new Response("data: [DONE]\n\n", { headers: { "content-type": "text/event-stream" } });
  } });
  for await (const event of provider.generate({
    model: { provider: "local", name: "local", revision: "1", options: {} }, messages: [], tools: [],
  })) { void event; }
  expect(endpoint).toBe("http://127.0.0.1:1234/v1/chat/completions");
});

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

test("OpenAI-compatible tool payloads use Rust canonical JSON before HTTP dispatch", async () => {
  let submitted = 0;
  const provider = new OpenAiCompatibleProvider({
    baseUrl: "https://example.test/v1",
    fetcher: async () => { submitted++; return new Response("data: [DONE]\n\n"); },
  });
  const request: ModelRequest = {
    model: { provider: "openai-compatible", name: "owned", revision: "r1", options: {} },
    messages: [{ role: "assistant", content: { kind: "tool_call", callId: "call", name: "echo",
      arguments: { unsafe: Number.POSITIVE_INFINITY } } }],
    tools: [],
  };
  const consume = async () => { for await (const _ of provider.generate(request)) { /* consume */ } };
  await expect(consume()).rejects.toThrow();
  expect(submitted).toBe(0);
});

test("OpenAI-compatible streams bound individual events and forward cancellation", async () => {
  const controller = new AbortController();
  let forwarded: AbortSignal | null | undefined;
  const provider = new OpenAiCompatibleProvider({
    baseUrl: "https://example.test/v1", maxEventBytes: 64,
    fetcher: async (_url, init) => {
      forwarded = init?.signal;
      return new Response(`data: ${"x".repeat(100)}\n\n`);
    },
  });
  const request: ModelRequest = {
    model: { provider: "openai-compatible", name: "owned", revision: "r1", options: {} },
    messages: [], tools: [], signal: controller.signal,
  };
  const consume = async () => { for await (const _ of provider.generate(request)) { /* consume */ } };
  await expect(consume()).rejects.toThrow("event exceeds");
  expect(forwarded).toBe(controller.signal);
});

test("OpenAI-compatible streams reject malformed tool calls before completion", async () => {
  const request: ModelRequest = {
    model: { provider: "openai-compatible", name: "owned", revision: "r1", options: {} },
    messages: [],
    tools: [],
  };
  const malformed = [
    [{ index: -1, id: "call", function: { name: "echo", arguments: "{}" } }],
    [{ index: 0, function: { name: "echo", arguments: "{}" } }],
    [{ index: 0, id: "call", function: { arguments: "{}" } }],
    [{ index: 0, id: "call", function: { name: "echo", arguments: "{" } }],
    [{ index: 0, id: "call", function: { name: "echo", arguments: "[]" } }],
    [{ index: 0, id: "call", function: { name: "echo", arguments: '{"value":9007199254740993}' } }],
    [
      { index: 0, id: "same", function: { name: "echo", arguments: "{}" } },
      { index: 1, id: "same", function: { name: "echo", arguments: "{}" } },
    ],
    [
      { index: 0, id: "valid", function: { name: "echo", arguments: "{}" } },
      { index: 1, id: "invalid", function: { name: "echo", arguments: "{" } },
    ],
  ];
  for (const toolCalls of malformed) {
    const provider = new OpenAiCompatibleProvider({
      baseUrl: "https://example.test/v1",
      fetcher: async () => new Response(`data: ${JSON.stringify({ choices: [{ delta: { tool_calls: toolCalls } }] })}\n\ndata: [DONE]\n\n`),
    });
    const events: ModelEvent[] = [];
    const consume = async () => {
      for await (const event of provider.generate(request)) events.push(event);
    };
    await expect(consume()).rejects.toThrow();
    expect(events.some(event => event.kind === "tool_call" || event.kind === "completed")).toBe(false);
  }
});

test("OpenAI-compatible projection verifies native images and keeps refs out of provider JSON", async () => {
  const bytes = new Uint8Array([137, 80, 78, 71]);
  const file: FileRef = {
    volume: { provider: { namespace: "test", family: "filesystem", version: "2" }, id: "images", class: "project", owner: { kind: "project", id: "project" } },
    path: "image.png", version: "one", descriptor: await descriptorFor(bytes, "image/png"), display_name: "image.png",
  };
  let submitted: Record<string, unknown> | undefined;
  const provider = new OpenAiCompatibleProvider({
    baseUrl: "https://example.test/v1",
    resolveFile: async () => bytes,
    fetcher: async (_, init) => {
      submitted = JSON.parse(String(init?.body)) as Record<string, unknown>;
      return new Response("data: [DONE]\n\n");
    },
  });
  const request: ModelRequest = {
    model: { provider: "openai-compatible", name: "owned", revision: "r1", options: {} },
    messages: [{ role: "user", content: [{ kind: "text", text: "inspect" }, { kind: "file", file, policy: "native" }] }],
    tools: [],
  };
  for await (const _ of provider.generate(request)) { /* consume */ }
  const messages = submitted?.messages as Array<{ content: Array<Record<string, unknown>> }>;
  expect(messages[0]?.content[0]).toEqual({ type: "text", text: "inspect" });
  expect(messages[0]?.content[1]).toEqual({ type: "image_url", image_url: { url: "data:image/png;base64,iVBORw==" } });
  expect(JSON.stringify(submitted)).not.toContain("byte_length");

  const corrupt = new OpenAiCompatibleProvider({ baseUrl: "https://example.test/v1", resolveFile: async () => new Uint8Array([0]), fetcher: async () => { throw new Error("must not send"); } });
  const consume = async () => { for await (const _ of corrupt.generate(request)) { /* consume */ } };
  await expect(consume()).rejects.toThrow("file content does not match its descriptor");
});

test("OpenAI-compatible projection rejects unsupported files before provider dispatch", async () => {
  const bytes = new Uint8Array([0]);
  const file: FileRef = {
    volume: { provider: { namespace: "test", family: "filesystem", version: "2" }, id: "files", class: "project", owner: { kind: "project", id: "project" } },
    path: "archive.zip", version: "one", descriptor: await descriptorFor(bytes, "application/zip"), display_name: "archive.zip",
  };
  const provider = new OpenAiCompatibleProvider({ baseUrl: "https://example.test/v1", resolveFile: async () => bytes, fetcher: async () => { throw new Error("must not send"); } });
  const request: ModelRequest = {
    model: { provider: "openai-compatible", name: "owned", revision: "r1", options: {} },
    messages: [{ role: "user", content: { kind: "file", file, policy: "native" } }], tools: [],
  };
  const consume = async () => { for await (const _ of provider.generate(request)) { /* consume */ } };
  await expect(consume()).rejects.toThrow("unsupported file content");
});

test("OpenAI-compatible projection preserves tool call/result linkage", async () => {
  let submitted: Record<string, unknown> | undefined;
  const provider = new OpenAiCompatibleProvider({
    baseUrl: "https://example.test/v1",
    fetcher: async (_, init) => {
      submitted = JSON.parse(String(init?.body)) as Record<string, unknown>;
      return new Response("data: [DONE]\n\n");
    },
  });
  const request: ModelRequest = {
    model: { provider: "openai-compatible", name: "owned", revision: "r1", options: {} },
    messages: [
      { role: "assistant", content: { kind: "tool_call", callId: "call-1", name: "lookup", arguments: { q: "test" } } },
      { role: "tool", content: { kind: "tool_result", callId: "call-1", name: "lookup", value: { count: 2 } } },
    ],
    tools: [],
  };
  for await (const _ of provider.generate(request)) { /* consume */ }
  expect(submitted?.messages).toEqual([
    { role: "assistant", content: null, tool_calls: [{ id: "call-1", type: "function", function: { name: "lookup", arguments: '{"q":"test"}' } }] },
    { role: "tool", tool_call_id: "call-1", content: '{"count":2}' },
  ]);
});
