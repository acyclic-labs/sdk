import { expect, test } from "bun:test";
import { create, toJsonString } from "@bufbuild/protobuf";
import { CredentialCache, CredentialSnapshot } from "@acyclic-labs/actors/credential";
import {
  HttpInferenceTransport, InferenceTransportError, InspectRunRequestSchema,
  ListModelsResponseSchema, RunEventSchema, WatchRunRequestSchema,
} from "../src/index.js";

const endpoint = "https://inference.example";
const models = () => toJsonString(ListModelsResponseSchema, create(ListModelsResponseSchema));

/** Exercise the injected transport against a real HTTP server; only routing is adapted. */
function localFetch(url: string): typeof fetch {
  return (input, init) => fetch(String(input).replace(endpoint, url), init);
}

function source() {
  let revision = crypto.randomUUID();
  let now = 1;
  let refreshes = 0;
  const cache = new CredentialCache(async () => {
    refreshes += 1;
    return new CredentialSnapshot(crypto.randomUUID(), revision, now + 10);
  }, () => ({ revision, nowUnixSeconds: now }));
  return {
    cache, refreshes: () => refreshes,
    change: () => { revision = crypto.randomUUID(); cache.invalidate(); },
    expire: () => { now += 10; },
  };
}

test("late HTTP rejection retains a newer lease and policy denial leaves it reusable", async () => {
  const owner = source();
  const admitted = Promise.withResolvers<void>();
  const reply = Promise.withResolvers<void>();
  let calls = 0;
  let status = 200;
  const server = Bun.serve({ hostname: "127.0.0.1", port: 0, async fetch() {
    calls += 1;
    if (calls === 1) { admitted.resolve(); await reply.promise; return new Response(null, { status: 401 }); }
    return new Response(status === 200 ? models() : null, { status });
  } });
  try {
    const transport = new HttpInferenceTransport(endpoint, { credential: owner.cache.provider }, localFetch(server.url.origin));
    const old = transport.listModels();
    // Own the rejection immediately while the original server attempt is held.
    const refused = expect(old).rejects.toMatchObject({ status: 401 });
    await admitted.promise;
    owner.change();
    await transport.listModels();
    reply.resolve();
    await refused;
    for (const denied of [403, 500]) {
      status = denied;
      await expect(transport.listModels()).rejects.toBeInstanceOf(InferenceTransportError);
    }
    status = 200;
    await transport.listModels();
    expect(owner.refreshes()).toBe(2);
    status = 401;
    await expect(transport.listModels()).rejects.toMatchObject({ status: 401 });
    status = 200;
    await transport.listModels();
    expect(owner.refreshes()).toBe(3);
  } finally { reply.resolve(); await server.stop(true); }
});

test("changed source and absolute expiry refuse unary bodies before JSON decoding", async () => {
  for (const stop of ["change", "expire"] as const) {
    const owner = source();
    const admitted = Promise.withResolvers<void>();
    const reply = Promise.withResolvers<void>();
    const server = Bun.serve({ hostname: "127.0.0.1", port: 0, async fetch() {
      admitted.resolve();
      await reply.promise;
      // Invalid JSON distinguishes credential refusal from a decode failure.
      return new Response("{");
    } });
    try {
      const transport = new HttpInferenceTransport(endpoint, { credential: owner.cache.provider }, localFetch(server.url.origin));
      const result = expect(transport.listModels()).rejects.toThrow("credential source changed or expired");
      await admitted.promise;
      owner[stop]();
      reply.resolve();
      await result;
      expect(owner.refreshes()).toBe(1);
    } finally { reply.resolve(); await server.stop(true); }
  }
});

test("a buffered stream cannot admit another event after its original lease expires", async () => {
  const owner = source();
  const event = toJsonString(RunEventSchema, create(RunEventSchema, {
    sequence: 1n, event: { case: "progress", value: { kind: "working" } },
  }));
  const server = Bun.serve({ hostname: "127.0.0.1", port: 0, fetch: () => new Response(`${event}\n${event}\n`) });
  try {
    const transport = new HttpInferenceTransport(endpoint, { credential: owner.cache.provider }, localFetch(server.url.origin));
    const events = transport.watchRun(create(WatchRunRequestSchema))[Symbol.asyncIterator]();
    expect((await events.next()).done).toBe(false);
    owner.expire();
    await expect(events.next()).rejects.toThrow("credential source changed or expired");
    expect(owner.refreshes()).toBe(1);
  } finally { await server.stop(true); }
});

test("cancelled credential resolution aborts its original producer before HTTP dispatch", async () => {
  const started = Promise.withResolvers<void>();
  const stopped = Promise.withResolvers<void>();
  const owner = new CredentialCache((_rejected, signal) => {
    started.resolve();
    return new Promise<CredentialSnapshot>((_resolve, reject) => {
      signal.addEventListener("abort", () => { stopped.resolve(); reject(signal.reason); }, { once: true });
    });
  }, () => ({ revision: "original", nowUnixSeconds: 1 }));
  let calls = 0;
  const transport = new HttpInferenceTransport(endpoint, { credential: owner.provider }, async () => {
    calls += 1;
    return new Response(models());
  });
  const controller = new AbortController();
  const result = expect(transport.inspectRun(create(InspectRunRequestSchema), controller.signal)).rejects.toThrow();
  await started.promise;
  controller.abort();
  await result;
  await stopped.promise;
  expect(calls).toBe(0);
});

test("authorization remains callable without erasing the transport's exact lease", async () => {
  const owner = source();
  let status = 401;
  const server = Bun.serve({ hostname: "127.0.0.1", port: 0, fetch: () => new Response(status === 200 ? models() : null, { status }) });
  try {
    const transport = new HttpInferenceTransport(endpoint, { credential: owner.cache.provider }, localFetch(server.url.origin));
    const original = new Headers(await transport.authorization()).get("authorization");
    await expect(transport.listModels()).rejects.toMatchObject({ status: 401 });
    expect(new Headers(await transport.authorization()).get("authorization")).not.toBe(original);
    status = 200;
    // An extracted callback retains the provider and the current cached lease.
    const callback = new HttpInferenceTransport(endpoint, transport.authorization, localFetch(server.url.origin));
    await callback.listModels();
    expect(owner.refreshes()).toBe(2);
  } finally { await server.stop(true); }
});

test("a response arriving after cancellation releases its original body without rejecting the lease", async () => {
  const owner = source();
  const admitted = Promise.withResolvers<void>();
  const reply = Promise.withResolvers<void>();
  let cancelled = 0;
  const body = new ReadableStream<Uint8Array>({ cancel() { cancelled += 1; } });
  const transport = new HttpInferenceTransport(endpoint, { credential: owner.cache.provider }, async () => {
    admitted.resolve();
    await reply.promise;
    return new Response(body, { status: 401 });
  });
  const controller = new AbortController();
  const result = expect(transport.inspectRun(create(InspectRunRequestSchema), controller.signal)).rejects.toMatchObject({ name: "AbortError" });
  try {
    await admitted.promise;
    controller.abort();
    reply.resolve();
    await result;
    expect(cancelled).toBe(1);
    await owner.cache.provider();
    expect(owner.refreshes()).toBe(1);
  } finally { reply.resolve(); await body.cancel(); }
});

test("cold resolution, warm reuse and concurrent expiry preserve singleflight across HTTP callers", async () => {
  const owner = source();
  let calls = 0;
  const server = Bun.serve({ hostname: "127.0.0.1", port: 0, fetch() { calls += 1; return new Response(models()); } });
  try {
    const transport = new HttpInferenceTransport(endpoint, { credential: owner.cache.provider }, localFetch(server.url.origin));
    for (const count of [1, 100, 1000]) {
      await Promise.all(Array.from({ length: count }, () => transport.listModels()));
      expect(owner.refreshes()).toBe(1);
    }
    owner.expire();
    await Promise.all(Array.from({ length: 1000 }, () => transport.listModels()));
    expect(owner.refreshes()).toBe(2);
    expect(calls).toBe(2101);
  } finally { await server.stop(true); }
});
