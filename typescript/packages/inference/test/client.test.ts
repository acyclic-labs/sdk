import { expect, test } from "bun:test";
import { create, toJsonString } from "@bufbuild/protobuf";
import {
  ContextViewSchema,
  CreateContextRequestSchema,
  GenerateRunRequestSchema,
  GenerateRunResponseSchema,
  HttpInferenceTransport,
  InferenceClient,
  InferenceTransportError,
  InspectContextRequestSchema,
  InspectRunRequestSchema,
  InspectWarmRequestSchema,
  ListModelsResponseSchema,
  MutateContextRequestSchema,
  MutationReceiptSchema,
  ReleaseWarmRequestSchema,
  RenewWarmRequestSchema,
  RetainWarmRequestSchema,
  RunEventSchema,
  RunTerminal,
  RunViewSchema,
  WarmViewSchema,
  WatchRunRequestSchema,
  type InferenceTransport,
} from "../src/index.js";

const bytes = (value: number) => new Uint8Array([value]);

test("generated lifecycle client covers contexts, warm commitments, runs, watch, and cancellation", async () => {
  const called: string[] = [];
  const transport: InferenceTransport = {
    async listModels() { called.push("models"); return create(ListModelsResponseSchema); },
    async createContext() { called.push("create"); return create(MutationReceiptSchema, { revision: bytes(1) }); },
    async inspectContext(request) { called.push(`inspect:${request.revision[0]}`); return create(ContextViewSchema, { revision: request.revision }); },
    async mutateContext() { called.push("mutate"); return create(MutationReceiptSchema, { revision: bytes(2) }); },
    async retainWarm() { called.push("retain"); return create(WarmViewSchema, { commitment: bytes(3) }); },
    async inspectWarm() { called.push("inspect-warm"); return create(WarmViewSchema); },
    async renewWarm() { called.push("renew"); return create(WarmViewSchema); },
    async releaseWarm() { called.push("release"); return create(WarmViewSchema); },
    async generateRun() { called.push("generate"); return create(GenerateRunResponseSchema, { run: { runId: bytes(4) } }); },
    async inspectRun() { called.push("inspect-run"); return create(RunViewSchema); },
    async *watchRun(request) { called.push(`watch:${request.fromSequence}`); yield create(RunEventSchema, { sequence: 0n, event: { case: "terminal", value: RunTerminal.COMPLETED } }); },
    async cancelRun() { called.push("cancel"); return create(RunViewSchema, { cancellationRequested: true }); },
  };
  const client = new InferenceClient(transport);
  await client.listModels();
  await client.createContext(create(CreateContextRequestSchema));
  await client.inspectContext(bytes(1));
  await client.mutateContext(create(MutateContextRequestSchema, { action: { case: "fork", value: {} } }));
  await client.retainWarm(create(RetainWarmRequestSchema));
  await client.inspectWarm(bytes(3));
  await client.renewWarm(create(RenewWarmRequestSchema));
  await client.releaseWarm(create(ReleaseWarmRequestSchema));
  await client.generate(create(GenerateRunRequestSchema));
  await client.inspectRun(bytes(4));
  for await (const event of client.watchRun(bytes(4), 7n)) expect(event.sequence).toBe(0n);
  expect((await client.cancelRun(bytes(4))).cancellationRequested).toBeTrue();
  expect(called).toEqual(["models", "create", "inspect:1", "mutate", "retain", "inspect-warm", "renew", "release", "generate", "inspect-run", "watch:7", "cancel"]);
});

test("HTTP lifecycle transport requires authorization and parses bounded run events", async () => {
  const terminal = toJsonString(RunEventSchema, create(RunEventSchema, {
    sequence: 0n,
    event: { case: "terminal", value: RunTerminal.COMPLETED },
  }));
  const fetcher: typeof fetch = async (input, init) => {
    expect(new Headers(init?.headers).get("authorization")).toBe("Bearer test");
    const url = String(input);
    if (url.endsWith("/models/list")) {
      return new Response(toJsonString(ListModelsResponseSchema, create(ListModelsResponseSchema)));
    }
    if (url.endsWith("/contexts/create") || url.endsWith("/contexts/mutate")) {
      return new Response(toJsonString(MutationReceiptSchema, create(MutationReceiptSchema)));
    }
    if (url.endsWith("/contexts/inspect")) {
      return new Response(toJsonString(ContextViewSchema, create(ContextViewSchema)));
    }
    if (url.includes("/warm/")) {
      return new Response(toJsonString(WarmViewSchema, create(WarmViewSchema)));
    }
    if (url.endsWith("/runs/generate")) {
      return new Response(toJsonString(GenerateRunResponseSchema, create(GenerateRunResponseSchema)));
    }
    if (url.endsWith("/runs/inspect") || url.endsWith("/runs/cancel")) {
      return new Response(toJsonString(RunViewSchema, create(RunViewSchema)));
    }
    if (url.endsWith("/runs/watch")) return new Response(`${terminal}\n`);
    throw new Error(`unexpected route ${url}`);
  };
  const transport = new HttpInferenceTransport("https://example.test", () => ({ authorization: "Bearer test" }), fetcher);
  await transport.listModels();
  await transport.createContext(create(CreateContextRequestSchema));
  await transport.inspectContext(create(InspectContextRequestSchema));
  await transport.mutateContext(create(MutateContextRequestSchema));
  await transport.retainWarm(create(RetainWarmRequestSchema));
  await transport.inspectWarm(create(InspectWarmRequestSchema));
  await transport.renewWarm(create(RenewWarmRequestSchema));
  await transport.releaseWarm(create(ReleaseWarmRequestSchema));
  await transport.generateRun(create(GenerateRunRequestSchema));
  await transport.inspectRun(create(InspectRunRequestSchema));
  await transport.cancelRun(create(InspectRunRequestSchema));
  const events = [];
  for await (const event of transport.watchRun(create(WatchRunRequestSchema, { runId: bytes(4) }))) events.push(event);
  expect(events).toHaveLength(1);

  const unauthorized = new HttpInferenceTransport("https://example.test", () => ({}), fetcher);
  await expect(unauthorized.listModels()).rejects.toBeInstanceOf(InferenceTransportError);
});

test("HTTP transport applies one byte ceiling per message without conflating network chunks", async () => {
  const terminal = toJsonString(RunEventSchema, create(RunEventSchema, {
    sequence: 0n,
    event: { case: "terminal", value: RunTerminal.COMPLETED },
  }));
  const maximumMessageBytes = new TextEncoder().encode(terminal).byteLength + 16;
  const request = create(WatchRunRequestSchema, { runId: bytes(4) });
  const transport = new HttpInferenceTransport(
    "https://example.test",
    () => ({ authorization: "Bearer test" }),
    async () => new Response(`${terminal}\n${terminal}\n`),
    maximumMessageBytes,
  );
  const events = [];
  for await (const event of transport.watchRun(request)) events.push(event);
  expect(events).toHaveLength(2);
  expect(transport.maximumMessageBytes).toBe(maximumMessageBytes);

  const oversizedEvent = toJsonString(RunEventSchema, create(RunEventSchema, {
    sequence: 0n,
    event: { case: "progress", value: { kind: "x".repeat(maximumMessageBytes) } },
  }));
  const split = Math.floor(oversizedEvent.length / 2);
  const stream = new ReadableStream<Uint8Array>({
    start(controller) {
      const encoder = new TextEncoder();
      controller.enqueue(encoder.encode(oversizedEvent.slice(0, split)));
      controller.enqueue(encoder.encode(`${oversizedEvent.slice(split)}\n`));
      controller.close();
    },
  });
  const oversized = new HttpInferenceTransport(
    "https://example.test",
    () => ({ authorization: "Bearer test" }),
    async () => new Response(stream),
    maximumMessageBytes,
  );
  await expect(async () => {
    for await (const _event of oversized.watchRun(request)) { /* exhaust */ }
  }).toThrow("run event exceeds configured bound");
});

test("HTTP transport rejects insecure endpoints and bounded request, unary, and error bodies", async () => {
  expect(() => new HttpInferenceTransport("http://example.test", () => ({}))).toThrow(TypeError);
  expect(() => new HttpInferenceTransport("https://example.test?", () => ({}))).toThrow(TypeError);
  expect(() => new HttpInferenceTransport("https://example.test#", () => ({}))).toThrow(TypeError);

  let calls = 0;
  const requestBound = new HttpInferenceTransport(
    "https://example.test",
    () => ({ authorization: "Bearer test" }),
    async () => { calls += 1; return new Response("{}"); },
    32,
  );
  await expect(requestBound.createContext(create(CreateContextRequestSchema, {
    model: "x".repeat(64),
  }))).rejects.toThrow("request exceeds configured bound");
  expect(calls).toBe(0);

  const unaryBound = new HttpInferenceTransport(
    "https://example.test",
    () => ({ authorization: "Bearer test" }),
    async () => new Response(" ".repeat(65)),
    64,
  );
  await expect(unaryBound.listModels()).rejects.toThrow("unary response exceeds configured bound");

  const errorBound = new HttpInferenceTransport(
    "https://example.test",
    () => ({ authorization: "Bearer test" }),
    async () => new Response("x".repeat(65), { status: 400 }),
    64,
  );
  await expect(errorBound.listModels()).rejects.toThrow("error response exceeds configured bound");

  const whitespaceBound = new HttpInferenceTransport(
    "https://example.test",
    () => ({ authorization: "Bearer test" }),
    async () => new Response(`${" ".repeat(65)}{}\n`),
    64,
  );
  await expect(async () => {
    for await (const _event of whitespaceBound.watchRun(create(WatchRunRequestSchema))) { /* exhaust */ }
  }).toThrow("run event exceeds configured bound");
});
