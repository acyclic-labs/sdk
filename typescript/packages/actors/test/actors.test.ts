import { describe, expect, test } from "bun:test";
import { create } from "@bufbuild/protobuf";
import fc from "fast-check";
import { readFileSync } from "node:fs";
import { HttpActorsClient, CreateActorRequestSchema, InspectActorRequestSchema, type OperationEvent } from "../src/index.js";
import { observeInterceptors } from "../src/observe.js";

describe("Actors v1 generated transport", () => {
  test("uses the generated request shape on the Rust-owned create route", async () => {
    let posted = "";
    const client = new HttpActorsClient({
      endpoint: "https://actors.example.test/",
      token: "secret",
      fetcher: async (input, init) => {
        expect(String(input)).toBe("https://actors.example.test/v1/actors/create");
        posted = String(init?.body);
        return new Response(JSON.stringify({ actor: { actorId: "a", codeSha256: "AQ==", homeRegion: "eu" } }));
      },
    });
    const response = await client.createActor(create(CreateActorRequestSchema, {
      codeSha256: new Uint8Array(32).fill(1), homeRegion: "eu", idempotencyKey: "create-a",
      limits: { handlerTimeoutMillis: 1000n, memoryBytes: 1024n, checkpointBytes: 1024n },
    }));
    expect(posted).toContain("create-a");
    expect(response.actor?.actorId).toBe("a");
  });
  test("refuses redirects and header-unsafe or oversized bearer tokens", async () => {
    for (const token of [" ", "a\nb", "a\rb", "a\0b", "x".repeat(12 * 1024 + 1)]) {
      expect(() => new HttpActorsClient({ endpoint: "https://actors.example.test", token })).toThrow(TypeError);
    }
    let redirect: RequestRedirect | undefined;
    const client = new HttpActorsClient({ endpoint: "https://actors.example.test", token: "x".repeat(12 * 1024), fetcher: async (_input, init) => {
      redirect = init?.redirect;
      return new Response(JSON.stringify({}));
    } });
    await client.inspectActor(create(InspectActorRequestSchema, { actorId: "a" }));
    expect(redirect).toBe("error");
  });
  test("accepts a response exactly when its size is within the bound, however it is chunked", async () => {
    await fc.assert(fc.asyncProperty(fc.integer({ min: 2, max: 64 }), fc.integer({ min: 1, max: 64 }), fc.array(fc.integer({ min: 1, max: 8 }), { minLength: 1 }), async (size, maximumResponseBytes, cuts) => {
      const body = new TextEncoder().encode(`${" ".repeat(size - 2)}{}`);
      const client = new HttpActorsClient({ endpoint: "https://actors.example.test", token: "secret", maximumResponseBytes, fetcher: async () => new Response(new ReadableStream<Uint8Array>({
        start(controller) {
          for (let offset = 0, index = 0; offset < body.byteLength; index += 1) {
            const end = offset + (cuts[index % cuts.length] ?? 1);
            controller.enqueue(body.slice(offset, end));
            offset = end;
          }
          controller.close();
        },
      })) });
      const inspected = client.inspectActor(create(InspectActorRequestSchema, { actorId: "a" }));
      if (size <= maximumResponseBytes) await inspected;
      else await expect(inspected).rejects.toThrow("exceeds configured bound");
    }), { numRuns: 100 });
  });
  test("observes one secret-free event per call, and nothing without an observer", async () => {
    const events: OperationEvent[] = [];
    const fetcher = async (_input: RequestInfo | URL, init?: RequestInit) => new Response(String(init?.body).includes("missing") ? "{}" : JSON.stringify({}), { status: String(init?.body).includes("missing") ? 404 : 200 });
    const client = new HttpActorsClient({ endpoint: "https://actors.example.test", token: "secret", fetcher, observer: { onOperation: event => events.push(event) } });
    await client.inspectActor(create(InspectActorRequestSchema, { actorId: "present" }));
    await expect(client.inspectActor(create(InspectActorRequestSchema, { actorId: "missing" }))).rejects.toThrow();
    expect(events.map(({ op, ok, code, requestBytes, responseBytes }) => ({ op, ok, code, requestBytes, responseBytes }))).toEqual([
      { op: "inspectActor", ok: true, code: undefined, requestBytes: 21, responseBytes: 2 },
      { op: "inspectActor", ok: false, code: 404, requestBytes: 21, responseBytes: 2 },
    ]);
    for (const event of events) expect(Object.keys(event).every(key => ["family", "op", "durationMs", "ok", "code", "requestBytes", "responseBytes", "work"].includes(key))).toBe(true);
    expect(JSON.stringify(events)).not.toMatch(/secret|present|missing|v1\/actors/);

    const interceptors = [(next: (request: { method: { localName: string } }) => Promise<unknown>) => next];
    expect(observeInterceptors(interceptors, undefined, "actors")).toBe(interceptors);
    performance.clearMeasures();
    await new HttpActorsClient({ endpoint: "https://actors.example.test", token: "secret", fetcher }).inspectActor(create(InspectActorRequestSchema, { actorId: "present" }));
    expect(performance.getEntriesByType("measure")).toEqual([]);
    process.env.ACYCLIC_PERF = "1";
    try { await new HttpActorsClient({ endpoint: "https://actors.example.test", token: "secret", fetcher }).inspectActor(create(InspectActorRequestSchema, { actorId: "present" })); }
    finally { delete process.env.ACYCLIC_PERF; }
    expect(performance.getEntriesByType("measure").map(entry => entry.name)).toEqual(["acyclic.actors.inspectActor"]);
  });
  test("keeps every package's observer helper identical", () => {
    const source = (name: string) => readFileSync(new URL(`../../${name}/src/observe.ts`, import.meta.url), "utf8");
    for (const name of ["filesystem", "harness", "inference", "machines", "objects", "stream", "workers"]) expect(source(name)).toBe(source("actors"));
  });
});
