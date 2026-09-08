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
    if (url.endsWith("/runs/watch")) return new Response(`${terminal}\n`);
    throw new Error(`unexpected route ${url}`);
  };
  const transport = new HttpInferenceTransport("https://example.test", () => ({ authorization: "Bearer test" }), fetcher);
  await transport.listModels();
  const events = [];
  for await (const event of transport.watchRun({ $typeName: "inference.customer.v1.WatchRunRequest", runId: bytes(4), fromSequence: 0n })) events.push(event);
  expect(events).toHaveLength(1);

  const unauthorized = new HttpInferenceTransport("https://example.test", () => ({}), fetcher);
  await expect(unauthorized.listModels()).rejects.toBeInstanceOf(InferenceTransportError);
});
