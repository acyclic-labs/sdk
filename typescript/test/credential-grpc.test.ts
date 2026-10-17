import { expect, test } from "bun:test";
import fc from "fast-check";
import { credentialGetter, credentialGetterKinds } from "./credential-getters.js";
import { credentialTlsFixture } from "./credential-tls.js";
import { StreamService } from "../packages/stream/generated/proto/stream/v1/stream_pb.js";
import { FilesystemService } from "../packages/filesystem/generated/proto/filesystem/v2/filesystem_pb.js";
import { BucketsService } from "../packages/objects/generated/proto/objects/v1/objects_pb.js";
import { MachinesService } from "../packages/machines/generated/proto/machines/v1/machines_pb.js";
import { createStreamGrpcClient } from "../packages/stream/src/grpc.js";
import { DefaultStreamProvider } from "../packages/stream/src/default.js";
import { Stream } from "../packages/stream/src/client.js";
import { createFilesystemGrpcClient } from "../packages/filesystem/src/grpc.js";
import { createObjectsV1GrpcClients } from "../packages/objects/src/v1-grpc.js";
import { createMachinesGrpcClient } from "../packages/machines/src/grpc.js";
import { ActorsService } from "../packages/actors/generated/proto/actors/v1/actors_pb.js";
import { ActorsClient } from "../packages/actors/src/client.js";

test("every product gRPC client resolves current credentials and stops on failure", async () => {
  const sent = new Map<string, string[]>();
  const observe = (product: string, context: { requestHeader: Headers }) => { sent.get(product)!.push(context.requestHeader.get("authorization") ?? ""); return {}; };
  const { endpoint, certificate, close } = await credentialTlsFixture(router => {
    router.service(StreamService, { tail(_request, context) { observe("streams", context); return { tail: 0n }; } });
    router.service(FilesystemService, { handshake(_request, context) { return observe("fs", context); } });
    router.service(BucketsService, { headBucket(_request, context) { return observe("objects", context); } });
    router.service(MachinesService, { listMachines(_request, context) { return observe("machines", context); } });
  });
  const clients = credentialGetterKinds.flatMap(kind => ["streams", "fs", "objects", "machines"].map(product => {
    const observed = sent.get(product) ?? [];
    sent.set(product, observed);
    let calls = 0;
    let values: readonly string[] = [];
    const callback = async () => values[calls++]!;
    const options = credentialGetter({ endpoint, caCertificate: certificate, token: "stale" }, callback, kind);
    let invoke: () => Promise<unknown>;
    let release: (() => void) | undefined;
    switch (product) {
      case "streams": { const client = createStreamGrpcClient(options); invoke = () => client.tail({ path: "credential/test" }); break; }
      case "fs": { const client = createFilesystemGrpcClient(options); invoke = () => client.filesystem.handshake({}); release = client.close; break; }
      case "objects": { const client = createObjectsV1GrpcClients(options); invoke = () => client.buckets.headBucket({}); release = client.close; break; }
      case "machines": { const client = createMachinesGrpcClient(options); invoke = () => client.machines.listMachines({}); release = client.close; break; }
      default: throw new Error("unknown fixture product");
    }
    return { kind, invoke, release, observed, setValues(next: readonly string[]) { values = next; calls = 0; },
      check(count: number) { expect(calls).toBe(count); expect(options.credentialReads()).toBe(1); } };
  }));
  try {
    await fc.assert(fc.asyncProperty(fc.array(fc.stringMatching(/^[a-zA-Z0-9_-]{1,32}$/), { minLength: 2, maxLength: 8 }), fc.constantFrom(...credentialGetterKinds), async (values, kind) => {
      const encode = TextEncoder.prototype.encode;
      let validations = 0;
      TextEncoder.prototype.encode = function(value) {
        if (values.includes(value ?? "")) validations++;
        return encode.call(this, value);
      };
      try { await Promise.all(clients.filter(client => client.kind === kind).map(async client => {
        client.setValues(values);
        const start = client.observed.length;
        for (const _value of values) await client.invoke();
        expect(client.observed.slice(start)).toEqual(values.map(value => `Bearer ${value}`));
        client.check(values.length);
        await expect(client.invoke()).rejects.toThrow();
        expect(client.observed.length).toBe(start + values.length);
      }));
      // One UTF-8 validation per callback result across the four products.
      // Wrapping the Streams resolver again exceeds this exact budget.
      expect(validations).toBe(values.length * 4);
      } finally { TextEncoder.prototype.encode = encode; }
    }), { numRuns: 16, examples: credentialGetterKinds.map(kind => [["fresh-a", "fresh-b"], kind]) });
  } finally { for (const client of clients) client.release?.(); await close(); }
});

test("Actors sends renewed credentials through the native Rust bridge", async () => {
  const sent: string[] = [];
  const { endpoint, certificate, close } = await credentialTlsFixture(router => {
    router.service(ActorsService, { inspectActor(_request, context) {
      sent.push(context.requestHeader.get("authorization") ?? "");
      return {};
    } });
  });
  const actor = { actorId: "actor-a" as Parameters<ActorsClient["inspectActor"]>[0]["actorId"] };
  try {
    await fc.assert(fc.asyncProperty(fc.array(fc.stringMatching(/^[a-zA-Z0-9_-]{1,32}$/), { minLength: 2, maxLength: 6 }), fc.constantFrom(...credentialGetterKinds), async (values, kind) => {
      let calls = 0;
      const options = credentialGetter({ endpoint, token: "stale", caCertificate: Buffer.from(certificate) }, () => values[calls++]!, kind);
      const client = new ActorsClient(options);
      const start = sent.length;
      for (const _value of values) await client.inspectActor(actor);
      expect(sent.slice(start)).toEqual(values.map(value => `Bearer ${value}`));
      expect(calls).toBe(values.length);
      expect(options.credentialReads()).toBe(1);
      await expect(client.inspectActor(actor)).rejects.toThrow();
      expect(sent.length).toBe(start + values.length);

    }), { numRuns: 8, examples: credentialGetterKinds.map(kind => [["fresh-a", "fresh-b"], kind]) });
  } finally { await close(); }
});

test("default Streams selection snapshots providers once and renews through its native client", async () => {
  const sent: string[] = [];
  const { endpoint, certificate, close } = await credentialTlsFixture(router => {
    router.service(StreamService, { tail(_request, context) {
      sent.push(context.requestHeader.get("authorization") ?? "");
      return { tail: 7n };
    } });
  });
  try {
    await fc.assert(fc.asyncProperty(fc.array(fc.stringMatching(/^[a-zA-Z0-9_-]{1,32}$/), { minLength: 2, maxLength: 6 }), fc.constantFrom(...credentialGetterKinds), async (values, kind) => {
      let calls = 0;
      const callback = () => values[calls++]!;
      const base = { endpoint, token: "stale", caCertificate: Buffer.from(certificate) };
      const options = credentialGetter({ ...base }, callback, kind);
      const client = new DefaultStreamProvider(options);
      const start = sent.length;
      for (const _value of values) expect(await client.tail("credential/test")).toBe(7n);
      expect(sent.slice(start)).toEqual(values.map(value => `Bearer ${value}`));
      expect(calls).toBe(values.length);
      expect(options.credentialReads()).toBe(1);
      await expect(client.tail("credential/test")).rejects.toThrow();
      expect(sent.length).toBe(start + values.length);
      calls = 0;
      const envOptions = credentialGetter({ ...base }, callback, kind);
      const fromEnv = Stream.fromEnv(envOptions);
      const envStart = sent.length;
      for (const _value of values) expect(await fromEnv.bytes("credential/test").tail()).toBe(7n);
      expect(envOptions.credentialReads()).toBe(1);
      expect(sent.slice(envStart)).toEqual(values.map(value => `Bearer ${value}`));
      await expect(fromEnv.bytes("credential/test").tail()).rejects.toThrow();
      expect(sent.length).toBe(envStart + values.length);
    }), { numRuns: 8, examples: credentialGetterKinds.map(kind => [["fresh-a", "fresh-b"], kind]) });
  } finally { await close(); }
});
