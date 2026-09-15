import { expect, test } from "bun:test";
import { create, toJsonString } from "@bufbuild/protobuf";
import {
  ContextViewSchema,
  contextRevision,
  CreateContextRequestSchema,
  GenerateRunRequestSchema,
  GenerateRunResponseSchema,
  HttpInferenceTransport,
  Inference,
  InferenceClient,
  InferenceTransportError,
  WarmState,
  InspectContextRequestSchema,
  InspectRunRequestSchema,
  InspectWarmRequestSchema,
  ItemKind,
  ItemSchema,
  ListModelsResponseSchema,
  MutateContextRequestSchema,
  MutationReceiptSchema,
  ReleaseWarmRequestSchema,
  RenewWarmRequestSchema,
  RetainWarmRequestSchema,
  RunEventSchema,
  RunResultSchema,
  RunTerminal,
  RunViewSchema,
  runId,
  WarmViewSchema,
  WatchRunRequestSchema,
  type InferenceTransport,
} from "../src/index.js";

const bytes = (value: number) => new Uint8Array([value]);
const revision = (value: number) => new Uint8Array(32).fill(value);
const runIdentity = (value: number) => new Uint8Array(16).fill(value);
const receipt = (value: number) => create(MutationReceiptSchema, { revision: revision(value), commandDigest: revision(value + 32), sequence: 1n });
const contextView = (value: Uint8Array, model = "model") => create(ContextViewSchema, {
  revision: value,
  lineage: revision(20),
  executionProfile: revision(21),
  contentDigest: revision(22),
  model,
  provenance: { origin: { case: "created", value: {} } },
});
const warmView = (commitment: Uint8Array, context = revision(1), expiresAtMs = 10n) => create(WarmViewSchema, {
  commitment,
  context,
  modelProfile: revision(23),
  latencyProfile: revision(24),
  expiresAtMs,
  state: WarmState.ACTIVE,
  evidenceDigest: revision(25),
  admissionReceiptId: revision(26),
  sequence: 1n,
});

test("generated lifecycle client covers contexts, warm commitments, runs, watch, and cancellation", async () => {
  const called: string[] = [];
  const transport: InferenceTransport = {
    async listModels() { called.push("models"); return create(ListModelsResponseSchema); },
    async createContext() { called.push("create"); return receipt(1); },
    async inspectContext(request) { called.push(`inspect:${request.revision[0]}`); return contextView(request.revision); },
    async mutateContext() { called.push("mutate"); return receipt(2); },
    async retainWarm(request) { called.push("retain"); return warmView(revision(3), request.context); },
    async inspectWarm(request) { called.push("inspect-warm"); return warmView(request.commitment); },
    async renewWarm(request) { called.push("renew"); return warmView(request.commitment, revision(1), request.expiresAtMs); },
    async releaseWarm(request) { called.push("release"); return warmView(request.commitment); },
    async generateRun() { called.push("generate"); return create(GenerateRunResponseSchema, { run: { runId: runIdentity(4), input: revision(1), model: "model" } }); },
    async inspectRun(request) { called.push("inspect-run"); return create(RunViewSchema, { runId: request.runId, input: revision(1), model: "model" }); },
    async *watchRun(request) { called.push(`watch:${request.fromSequence}`); yield create(RunEventSchema, { sequence: request.fromSequence, event: { case: "terminal", value: RunTerminal.COMPLETED } }); },
    async cancelRun(request) { called.push("cancel"); return create(RunViewSchema, { runId: request.runId, input: revision(1), model: "model", cancellationRequested: true }); },
  };
  const client = new InferenceClient(transport);
  await client.listModels();
  await client.createContext(create(CreateContextRequestSchema));
  await client.inspectContext(revision(1));
  await client.mutateContext(create(MutateContextRequestSchema, { action: { case: "fork", value: {} } }));
  await client.retainWarm(create(RetainWarmRequestSchema, { context: revision(1) }));
  await client.inspectWarm(revision(3));
  await client.renewWarm(create(RenewWarmRequestSchema, { commitment: revision(3), expiresAtMs: 20n }));
  await client.releaseWarm(create(ReleaseWarmRequestSchema, { commitment: revision(3) }));
  await client.generate(create(GenerateRunRequestSchema, { identity: { clientInstance: runIdentity(3), requestId: runIdentity(4) }, context: revision(1) }));
  await client.inspectRun(runIdentity(4));
  for await (const event of client.watchRun(runIdentity(4), 7n)) expect(event.sequence).toBe(7n);
  expect((await client.cancelRun(runIdentity(4))).cancellationRequested).toBeTrue();
  expect(called).toEqual(["models", "create", "inspect:1", "mutate", "retain", "inspect-warm", "renew", "release", "generate", "inspect-run", "watch:7", "cancel"]);
});

test("high-level handles preserve typed context, run, and warm identities", async () => {
  let revisionCounter = 0;
  const transport: InferenceTransport = {
    async listModels() { return create(ListModelsResponseSchema); },
    async createContext(request) { expect(request.items[0]?.kind).toBe(ItemKind.USER); return receipt(++revisionCounter); },
    async inspectContext(request) { const view = contextView(request.revision); view.items.push(create(ItemSchema, { kind: ItemKind.USER })); return view; },
    async mutateContext(request) { expect(request.action.case).not.toBeUndefined(); return receipt(++revisionCounter); },
    async retainWarm(request) { return warmView(revision(7), request.context); },
    async inspectWarm(request) { return warmView(request.commitment, revision(3)); },
    async renewWarm(request) { return warmView(request.commitment, revision(3), request.expiresAtMs); },
    async releaseWarm(request) { return warmView(request.commitment, revision(3)); },
    async generateRun(request) { return create(GenerateRunResponseSchema, { run: { runId: request.identity!.requestId, input: request.context, model: "model" } }); },
    async inspectRun(request) { return create(RunViewSchema, { runId: request.runId, input: revision(3), model: "model", result: create(RunResultSchema, { output: bytes(9), terminal: RunTerminal.COMPLETED, context: create(ContextViewSchema, { revision: revision(10) }) }) }); },
    async *watchRun(request) { yield create(RunEventSchema, { sequence: request.fromSequence, event: { case: "terminal", value: RunTerminal.COMPLETED } }); },
    async cancelRun(request) { return create(RunViewSchema, { runId: request.runId, input: revision(3), model: "model", cancellationRequested: true }); },
  };
  const inference = new Inference(new InferenceClient(transport));
  const item = create(ItemSchema, { kind: ItemKind.USER, payload: bytes(1) });
  const context = await inference.create("model", [item]);
  expect((await context.items())[0]?.kind).toBe(ItemKind.USER);
  const fork = await context.append(item).then(value => value.fork());
  expect(fork.id()).toEqual(revision(3));
  const run = await fork.generate(item, { maximumOutput: 32n });
  expect((await inference.attach(context.id())).id()).toEqual(context.id());
  expect((await inference.recoverRun(run.id())).id()).toEqual(run.id());
  const result = await run.result();
  expect(result.terminal).toBe("completed");
  expect(result.output).toEqual(bytes(9));
  expect(result.continuationValid).toBeTrue();
  expect(result.context?.id()).toEqual(revision(10));
  expect((await run.cancel()).cancellationRequested).toBeTrue();
  const events = []; for await (const event of run.events({ from: 4n })) events.push(event.sequence);
  expect(events).toEqual([4n]);
  const warm = await fork.retain({ latencyProfile: revision(2), expiresAtMs: 10n });
  expect(warm.id()).toEqual(revision(7));
  await warm.inspect();
  expect((await warm.renew(20n)).expiresAtMs).toBe(20n);
  expect((await warm.release()).commitment).toEqual(revision(7));
});

test("run recovery rejects substituted or malformed streams and observes an inclusive zero cursor", async () => {
  const id = runIdentity(4);
  let inspectCount = 0;
  const transport: InferenceTransport = {
    async listModels() { return create(ListModelsResponseSchema); }, async createContext() { return create(MutationReceiptSchema); },
    async inspectContext() { return create(ContextViewSchema); }, async mutateContext() { return create(MutationReceiptSchema); },
    async retainWarm() { return create(WarmViewSchema); }, async inspectWarm() { return create(WarmViewSchema); },
    async renewWarm() { return create(WarmViewSchema); }, async releaseWarm() { return create(WarmViewSchema); },
    async generateRun() { return create(GenerateRunResponseSchema); },
    async inspectRun(request) { inspectCount += 1; return create(RunViewSchema, { runId: request.runId, input: revision(1), model: "model", lastSequence: 0n, ...(inspectCount > 1 ? { result: create(RunResultSchema, { terminal: RunTerminal.COMPLETED }) } : {}) }); },
    async *watchRun(request) { expect(request.fromSequence).toBe(0n); yield create(RunEventSchema, { sequence: 0n, event: { case: "terminal", value: RunTerminal.COMPLETED } }); },
    async cancelRun(request) { return create(RunViewSchema, { runId: request.runId, input: revision(1), model: "model" }); },
  };
  const result = await new Inference(new InferenceClient(transport)).run(runId(id)).result();
  expect(result.terminal).toBe("completed");

  const substituted = new InferenceClient({ ...transport, async inspectRun() { return create(RunViewSchema, { runId: runIdentity(9), input: revision(1), model: "model" }); } });
  await expect(substituted.inspectRun(id)).rejects.toThrow("run identity differs");

  const substitutedGeneration = new InferenceClient({ ...transport, async generateRun(request) { return create(GenerateRunResponseSchema, { run: { runId: runIdentity(9), input: request.context, model: "model" } }); } });
  await expect(substitutedGeneration.generate(create(GenerateRunRequestSchema, { identity: { clientInstance: runIdentity(2), requestId: id }, context: revision(1) }))).rejects.toThrow("run identity differs");

  const malformed = new InferenceClient({ ...transport, async *watchRun() { yield create(RunEventSchema, { sequence: 2n }); } });
  await expect(async () => { for await (const _event of malformed.watchRun(id)) { /* exhaust */ } }).toThrow("run event order or shape differs");

  const truncated = new InferenceClient({ ...transport, async *watchRun() { yield create(RunEventSchema, { sequence: 0n, event: { case: "progress", value: { kind: "queued" } } }); } });
  await expect(async () => { for await (const _event of truncated.watchRun(id)) { /* exhaust */ } }).toThrow("run stream ended before terminal");

  expect(() => contextRevision(bytes(1))).toThrow("exactly 32 bytes");
  const substitutedContext = new InferenceClient({ ...transport, async inspectContext() { return contextView(revision(9)); } });
  await expect(substitutedContext.inspectContext(revision(1))).rejects.toThrow("context revision differs");
  const substitutedWarm = new InferenceClient({ ...transport, async inspectWarm() { return warmView(revision(9)); } });
  await expect(substitutedWarm.inspectWarm(revision(8))).rejects.toThrow("warm commitment differs");
  const malformedReceipt = new InferenceClient({ ...transport, async createContext() { return create(MutationReceiptSchema); } });
  await expect(malformedReceipt.createContext(create(CreateContextRequestSchema))).rejects.toThrow("mutation revision");
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
