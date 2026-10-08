import assert from "node:assert/strict";
import { createSecureServer } from "node:http2";
import { spawn, spawnSync } from "node:child_process";
import { fileURLToPath } from "node:url";
import { zstdCompressSync } from "node:zlib";
import { create, fromBinary, toBinary } from "@bufbuild/protobuf";
import { createClient, Code, ConnectError } from "@connectrpc/connect";
import { createGrpcTransport, Http2SessionManager, connectNodeAdapter } from "@connectrpc/connect-node";
import * as wire from "@acyclic-labs/objects/proto";
import { GrpcObjectsV1 } from "@acyclic-labs/objects/grpc";
import { MemoryObjectsV1, ObjectsV1Error } from "@acyclic-labs/objects";
import { ObjectsV1Memory } from "../generated/wasm/acyclic_objects_wasm.js";
import { lifecycle } from "./v1-lifecycle.mjs";

const root = fileURLToPath(new URL("../../../../", import.meta.url));
const file = fileURLToPath(import.meta.url);
if (process.argv.includes("--client")) {
  let config = "";
  for await (const chunk of process.stdin) config += chunk;
  const options = JSON.parse(config);
  const client = new GrpcObjectsV1(options);
  const wrong = new GrpcObjectsV1({ ...options, token: "wrong" });
  const bounded = new GrpcObjectsV1(options, 32);
  try {
    const bucket = { name: "bounded.inputs" };
    await client.createBucket(create(wire.CreateBucketRequestSchema, { name: bucket.name }));
    const session = new Http2SessionManager(new URL(options.endpoint), {}, { ca: options.caCertificate });
    try {
      const retiredService = { ...wire.BucketsService, typeName: "acyclic.objects.v2.BucketsService" };
      retiredService.methods = wire.BucketsService.methods.map(method => ({ ...method, parent: retiredService }));
      const obsolete = createClient(retiredService, createGrpcTransport({ baseUrl: options.endpoint, sessionManager: session, defaultTimeoutMs: 2000 }));
      await assert.rejects(obsolete.headBucket({ bucket }, { headers: { authorization: `Bearer ${options.token}` } }), error => error instanceof ConnectError && error.code === Code.Unimplemented);
    } finally { session.abort(); }
    const streamed = create(wire.PutObjectHeaderSchema, { bucket, objectKey: "streamed" });
    const payload = new Uint8Array(135000).map((_, index) => index % 251);
    async function* source() { yield new Uint8Array(0); yield payload; }
    assert.equal((await client.putStream(streamed, source())).size, 135000n);
    assert.deepEqual((await client.get(create(wire.GetObjectRequestSchema, { bucket, objectKey: "streamed" }), 135000n)).body, payload);
    async function* failedSource() { yield new Uint8Array([1]); throw new ObjectsV1Error(wire.ErrorCode.ACCESS_DENIED); }
    await assert.rejects(client.putStream(streamed, failedSource()), error => error.code === wire.ErrorCode.ACCESS_DENIED);
    assert.deepEqual((await client.get(create(wire.GetObjectRequestSchema, { bucket, objectKey: "streamed" }), 135000n)).body, payload);
    let ready;
    const pulled = new Promise(resolve => { ready = resolve; });
    const cancellation = new AbortController();
    async function* pendingSource() { yield new Uint8Array([1]); ready(); await new Promise(() => {}); }
    const cancelled = client.putStream(create(wire.PutObjectHeaderSchema, { bucket, objectKey: "cancelled" }), pendingSource(), cancellation.signal);
    const rejection = assert.rejects(cancelled, error => error.code === wire.ErrorCode.UNAVAILABLE);
    await pulled;
    cancellation.abort();
    await rejection;
    await assert.rejects(client.head(create(wire.HeadObjectRequestSchema, { bucket, objectKey: "cancelled" })), error => error.code === wire.ErrorCode.NOT_FOUND);
    const upload = await client.createMultipart(create(wire.CreateMultipartRequestSchema, { bucket, objectKey: "part-streamed" }));
    const partHeader = create(wire.UploadPartHeaderSchema, { bucket, objectKey: "part-streamed", uploadId: upload.uploadId, partNumber: 1 });
    const receipt = await client.uploadPartStream(partHeader, source());
    await assert.rejects(client.uploadPartStream(partHeader, failedSource()), error => error.code === wire.ErrorCode.ACCESS_DENIED);
    assert.deepEqual((await client.listParts(create(wire.ListPartsRequestSchema, { bucket, objectKey: "part-streamed", uploadId: upload.uploadId, limit: 10 }))).parts, [receipt]);
    await client.abortMultipart(create(wire.AbortMultipartRequestSchema, { bucket, objectKey: "part-streamed", uploadId: upload.uploadId }));
    await client.delete(create(wire.DeleteObjectRequestSchema, { bucket, objectKey: "streamed" }));
    await client.put(create(wire.PutObjectHeaderSchema, { bucket, objectKey: "large" }), new Uint8Array(135000));
    await assert.rejects(bounded.get(create(wire.GetObjectRequestSchema, { bucket, objectKey: "large" }), 135000n), error => error.code === wire.ErrorCode.QUOTA_EXCEEDED);
    assert.equal((await client.head(create(wire.HeadObjectRequestSchema, { bucket, objectKey: "large" }))).object.size, 135000n);
    await client.delete(create(wire.DeleteObjectRequestSchema, { bucket, objectKey: "large" }));
    for (const codec of [0, 3]) {
      const objectKey = `invalid-codec-${codec}`;
      await client.put(create(wire.PutObjectHeaderSchema, { bucket, objectKey }), new Uint8Array([1]));
      let timer;
      try {
        await assert.rejects(Promise.race([
          client.get(create(wire.GetObjectRequestSchema, { bucket, objectKey }), 1n),
          new Promise((_, reject) => { timer = setTimeout(() => reject(new Error("invalid codec waited for peer EOF")), 2000); }),
        ]), error => error.code === wire.ErrorCode.UNAVAILABLE);
      } finally { clearTimeout(timer); }
      await client.delete(create(wire.DeleteObjectRequestSchema, { bucket, objectKey }));
    }
    await client.deleteBucket(create(wire.DeleteBucketRequestSchema, { bucket }));
    await lifecycle(client);
    await assert.rejects(wrong.headBucket(create(wire.HeadBucketRequestSchema, { bucket: { name: "customer.inputs" } })), error => error.code === wire.ErrorCode.ACCESS_DENIED);
  } finally { client.close(); wrong.close(); bounded.close(); }
  console.log(`${process.versions.bun ? "Bun" : "Node"} Objects v1 TLS gRPC: all 13 operations, streaming, canonical errors and authentication pass`);
} else {
  await MemoryObjectsV1.create();
  const generated = spawnSync("cargo", ["run", "--quiet", "--locked", "-p", "acyclic-actors", "--example", "conformance-certificate"], { cwd: root, encoding: "utf8" });
  if (generated.status !== 0) throw new Error(generated.stderr);
  const identity = JSON.parse(generated.stdout);
  const routes = {
    CreateBucket: "buckets/create", HeadBucket: "buckets/head", DeleteBucket: "buckets/delete",
    PutObject: "objects/put", GetObject: "objects/get", HeadObject: "objects/head", DeleteObject: "objects/delete", ListObjects: "objects/list",
    CreateMultipart: "multipart/create", UploadPart: "multipart/upload-part", ListParts: "multipart/list-parts", CompleteMultipart: "multipart/complete", AbortMultipart: "multipart/abort",
  };
  let memory;
  const seen = new Set();
  const cancelledInvalidCodecs = new Map();
  function failure(error) {
    if (error instanceof ConnectError) return error;
    const code = typeof error.code === "number" ? error.code : wire.ErrorCode.UNAVAILABLE;
    return new ConnectError("Objects fixture rejected request", Code.Unknown, undefined, [{ desc: wire.ErrorDetailSchema, value: create(wire.ErrorDetailSchema, { code }) }]);
  }
  const adapter = connectNodeAdapter({ routes(router) {
    for (const service of [wire.BucketsService, wire.ObjectsService, wire.MultipartService]) {
      const implementation = {};
      for (const method of service.methods) {
        const check = context => {
          if (context.requestHeader.get("authorization") !== "Bearer fixture") throw new ConnectError("missing bearer", Code.Unauthenticated);
          seen.add(method.name);
        };
        if (method.methodKind === "server_streaming") {
          implementation[method.localName] = async function* (query, context) {
            try {
              check(context);
              const result = await memory.invoke(routes[method.name], toBinary(method.input, query), new Uint8Array(0), 64n * 1024n * 1024n);
              if (method.name === "GetObject" && /^invalid-codec-[03]$/.test(query.objectKey)) {
                const codec = Number(query.objectKey.at(-1));
                try {
                  yield fromBinary(method.output, result[0]);
                  yield create(wire.GetObjectResponseSchema, { frame: { case: "body", value: create(wire.BodySchema, { codec, data: new Uint8Array([1]), decodedLength: 1n }) } });
                  // Never send EOF: zero hangs open; unknown sends an endless valid body stream.
                  while (!context.signal.aborted) {
                    await new Promise(resolve => {
                      const done = () => { clearTimeout(timer); context.signal.removeEventListener("abort", done); resolve(); };
                      const timer = codec === 0 ? undefined : setTimeout(done, 10);
                      context.signal.addEventListener("abort", done, { once: true });
                      if (context.signal.aborted) done();
                    });
                    if (!context.signal.aborted) yield create(wire.GetObjectResponseSchema, { frame: { case: "body", value: create(wire.BodySchema, { codec: wire.Codec.NONE, data: new Uint8Array([1]), decodedLength: 1n }) } });
                  }
                } finally { if (context.signal.aborted) cancelledInvalidCodecs.get(codec)(); }
                return;
              }
              // Compress every other frame so downloads reassemble mixed codecs in order.
              for (const [index, frame] of result.entries()) {
                const message = fromBinary(method.output, frame);
                if (message.frame?.case === "body" && index % 2 === 1) {
                  const { data } = message.frame.value;
                  message.frame.value = create(wire.BodySchema, { codec: wire.Codec.ZSTD, data: zstdCompressSync(data), decodedLength: BigInt(data.length) });
                }
                yield message;
              }
            } catch (error) { throw failure(error); }
          };
        } else implementation[method.localName] = async (query, context) => {
          try {
            check(context);
            let bytes;
            let body = new Uint8Array(0);
            if (method.methodKind === "client_streaming") {
              let header;
              const parts = [];
              let length = 0;
              let complete = false;
              for await (const { frame } of query) {
                if (header === undefined) { assert.equal(frame.case, "header"); header = frame.value; }
                else if (frame.case === "complete") { assert.equal(complete, false); assert.equal(frame.value, true); complete = true; }
                else { assert.equal(complete, false); assert.equal(frame.case, "body"); assert.ok(frame.value.length <= 65536); length += frame.value.length; assert.ok(length <= 64 * 1024 * 1024); parts.push(frame.value); }
              }
              assert.ok(header);
              assert.ok(complete);
              bytes = toBinary(method.name === "PutObject" ? wire.PutObjectHeaderSchema : wire.UploadPartHeaderSchema, header);
              body = new Uint8Array(length);
              let offset = 0;
              for (const part of parts) { body.set(part, offset); offset += part.length; }
            } else bytes = toBinary(method.input, query);
            const result = await memory.invoke(routes[method.name], bytes, body, 0n);
            assert.equal(result.length, 1);
            return fromBinary(method.output, result[0]);
          } catch (error) { throw failure(error); }
        };
      }
      router.service(service, implementation);
    }
  } });
  const server = createSecureServer({ key: identity.key, cert: identity.certificate }, adapter);
  await new Promise(resolve => server.listen(0, "localhost", resolve));
  const options = { endpoint: `https://localhost:${server.address().port}`, token: "fixture", caCertificate: identity.certificate };
  try {
    for (const runtime of [process.execPath, "bun"]) {
      memory = new ObjectsV1Memory(64n * 1024n * 1024n, 10000);
      seen.clear();
      cancelledInvalidCodecs.clear();
      const cancelled = [0, 3].map(codec => new Promise(resolve => cancelledInvalidCodecs.set(codec, resolve)));
      await new Promise((resolve, reject) => {
        const child = spawn(runtime, [file, "--client"], { cwd: root, stdio: ["pipe", "inherit", "inherit"] });
        child.stdin.end(JSON.stringify(options));
        child.on("error", reject);
        child.on("exit", code => code === 0 ? resolve() : reject(new Error(`${runtime} exited ${code}`)));
      });
      assert.deepEqual([...seen].sort(), Object.keys(routes).sort());
      let timer;
      try {
        await Promise.race([
          Promise.all(cancelled),
          new Promise((_, reject) => { timer = setTimeout(() => reject(new Error("invalid codec RPC did not cancel")), 2000); }),
        ]);
      } finally { clearTimeout(timer); }
    }
  } finally { await new Promise(resolve => server.close(resolve)); }
}
