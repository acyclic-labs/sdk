import { lifecycle } from "./v2-lifecycle.mjs";
import { test } from "node:test";
import assert from "node:assert/strict";
import { createServer } from "node:http";
import { once } from "node:events";
import { create, fromBinary, toBinary } from "@bufbuild/protobuf";
import * as wire from "@acyclic-labs/objects/proto";
import { MemoryObjectsV2 } from "@acyclic-labs/objects";
import { HttpObjectsV2 } from "@acyclic-labs/objects/http";
import { OBJECTS_ROUTES } from "../dist/generated-client.js";
import { ObjectsV2Memory, decode_objects_v2_json, encode_objects_v2_json, objects_v2_http_body_frame_bytes, objects_v2_http_json_frame_bytes, objects_v2_http_type, validate_objects_v2_response } from "../generated/wasm/acyclic_objects_wasm.js";

const expectedRoutes = Object.values(OBJECTS_ROUTES).map(({ path }) => path.replace(/^\/?v2\/objects\//, ""));
const make = (name, value) => create(wire[`${name}Schema`], value);
const fail = (code) => error => error.code === code;
const bytes = new Uint8Array(135000).map((_, index) => index % 251);
const frameLimits = () => ({ json: objects_v2_http_json_frame_bytes(), body: objects_v2_http_body_frame_bytes() });

async function fixture() {
  await MemoryObjectsV2.create(); // initialize the shared WASM runtime
  const memory = new ObjectsV2Memory(64n * 1024n * 1024n, 10000);
  const seen = new Set();
  const server = createServer(async (request, response) => {
    try {
      if (request.headers.authorization !== "Bearer fixture") throw { code: wire.ErrorCode.ACCESS_DENIED };
      assert.equal(request.method, "POST");
      const route = request.url.replace(/^\/v2\/objects\//, "");
      seen.add(route);
      const input = objects_v2_http_type(route, false);
      const output = objects_v2_http_type(route, true);
      const chunks = [];
      for await (const chunk of request) chunks.push(chunk);
      const data = Buffer.concat(chunks);
      let query;
      let body = new Uint8Array(0);
      if (route === "objects/put" || route === "multipart/upload-part") {
        assert.equal(request.headers["content-type"], "application/x-ndjson");
        assert.equal(data.at(-1), 10);
        const lines = data.toString("utf8").trimEnd().split("\n");
        const schema = route === "objects/put" ? wire.PutObjectRequestSchema : wire.UploadPartRequestSchema;
        const headerSchema = route === "objects/put" ? wire.PutObjectHeaderSchema : wire.UploadPartHeaderSchema;
        const frames = lines.map(line => fromBinary(schema, decode_objects_v2_json(input, Buffer.from(line), frameLimits().json)));
        assert.equal(frames.shift().frame.case, "header");
        assert.deepEqual(frames.pop().frame, { case: "complete", value: true });
        const first = fromBinary(schema, decode_objects_v2_json(input, Buffer.from(lines[0]), frameLimits().json));
        query = toBinary(headerSchema, first.frame.value);
        body = Buffer.concat(frames.map(({ frame }) => { assert.equal(frame.case, "body"); assert.ok(frame.value.length <= frameLimits().body); return frame.value; }));
      } else query = decode_objects_v2_json(input, data, 16 * 1024 * 1024);
      const result = await memory.invoke(route, query, body, 64n * 1024n * 1024n);
      const encoded = result.map(frame => Buffer.from(encode_objects_v2_json(output, frame, 16 * 1024 * 1024)));
      response.writeHead(200, { "content-type": route === "objects/get" ? "application/x-ndjson" : "application/json" });
      const payload = route === "objects/get" ? Buffer.concat(encoded.flatMap(frame => [frame, Buffer.from("\n")])) : encoded[0];
      for (let offset = 0; offset < payload.length; offset += 97) response.write(payload.subarray(offset, offset + 97));
      response.end();
    } catch (error) {
      const code = typeof error.code === "number" ? error.code : wire.ErrorCode.UNAVAILABLE;
      response.writeHead(code === wire.ErrorCode.ACCESS_DENIED ? 403 : 400, { "content-type": "application/json" });
      response.end(encode_objects_v2_json("ErrorDetail", toBinary(wire.ErrorDetailSchema, make("ErrorDetail", { code })), 1024));
    }
  });
  server.listen(0, "127.0.0.1");
  await once(server, "listening");
  return { endpoint: `http://127.0.0.1:${server.address().port}`, seen, close: () => new Promise(resolve => server.close(resolve)) };
}

test("Objects v2 Rust WASM provider covers every operation and atomic retry semantics", async () => {
  await lifecycle(await MemoryObjectsV2.create());
});
test("Objects v2 HTTP covers every operation, frame boundaries, errors and authentication", async () => {
  const server = await fixture();
  try {
    await lifecycle(new HttpObjectsV2({ endpoint: server.endpoint, token: "fixture" }));
    assert.deepEqual([...server.seen].sort(), expectedRoutes.sort());
    await assert.rejects(new HttpObjectsV2({ endpoint: server.endpoint, token: "wrong" }).headBucket(make("HeadBucketRequest", { bucket: { name: "customer.inputs" } })), fail(wire.ErrorCode.ACCESS_DENIED));
  } finally { await server.close(); }
});
test("Objects v2 rejects invalid requests before HTTP and bounds response allocations", async () => {
  let calls = 0;
  const fetcher = async () => { calls++; return new Response(new Uint8Array(100), { headers: { "content-type": "application/json" } }); };
  const client = new HttpObjectsV2({ endpoint: "https://objects.example", token: "fixture", maximumResponseBytes: 16, fetch: fetcher });
  await assert.rejects(client.put(make("PutObjectHeader", { bucket: { name: "bad..bucket" }, objectKey: "data" }), bytes), fail(wire.ErrorCode.INVALID_ARGUMENT));
  assert.equal(calls, 0);
  await assert.rejects(client.headBucket(make("HeadBucketRequest", { bucket: { name: "customer.inputs" } })), fail(wire.ErrorCode.QUOTA_EXCEEDED));
  assert.equal(calls, 1);
  for (const endpoint of ["http://objects.example", "https://user@objects.example", "https://objects.example/?query=1", "https://objects.example/#fragment"]) assert.throws(() => new HttpObjectsV2({ endpoint, token: "fixture" }));
  await assert.rejects(MemoryObjectsV2.create(0x1_0000_0000n), fail(wire.ErrorCode.QUOTA_EXCEEDED));
});
test("Objects v2 HTTP cancels oversized downloads at the header before pulling the body", async () => {
  await MemoryObjectsV2.create();
  const frame = make("GetObjectResponse", { frame: { case: "header", value: { object: { etag: "opaque", size: 1000000n, lastModified: { seconds: 0n, nanos: 0 } } } } });
  const header = Buffer.concat([Buffer.from(encode_objects_v2_json("GetObjectResponse", toBinary(wire.GetObjectResponseSchema, frame), frameLimits().json)), Buffer.from("\n")]);
  let pulls = 0;
  let cancelled = false;
  const fetcher = async () => new Response(new ReadableStream({
    pull(controller) { pulls++; if (pulls === 1) controller.enqueue(header); else throw new Error("body must not be pulled"); },
    cancel() { cancelled = true; },
  }, { highWaterMark: 0 }), { headers: { "content-type": "application/x-ndjson" } });
  const client = new HttpObjectsV2({ endpoint: "https://objects.example", token: "fixture", fetch: fetcher });
  await assert.rejects(client.get(make("GetObjectRequest", { bucket: { name: "customer.inputs" }, objectKey: "data" }), 1n), fail(wire.ErrorCode.QUOTA_EXCEEDED));
  assert.equal(pulls, 1);
  assert.equal(cancelled, true);
});

test("Objects v2 validates remote metadata, ranges, framing and terminal errors", async () => {
  await MemoryObjectsV2.create();
  const bodyFrameBytes = frameLimits().body;
  const info = { etag: "opaque", size: 1n, lastModified: { seconds: 0n, nanos: 0 } };
  const header = make("GetObjectResponse", { frame: { case: "header", value: { object: info } } });
  const body = make("GetObjectResponse", { frame: { case: "body", value: new Uint8Array([1]) } });
  const oversizedHeader = make("GetObjectResponse", { frame: { case: "header", value: { object: { ...info, size: BigInt(bodyFrameBytes + 1) } } } });
  const oversizedBody = make("GetObjectResponse", { frame: { case: "body", value: new Uint8Array(bodyFrameBytes + 1) } });
  const encode = frame => Buffer.from(encode_objects_v2_json("GetObjectResponse", toBinary(wire.GetObjectResponseSchema, frame), frameLimits().json));
  const lines = frames => Buffer.concat(frames.flatMap(frame => [encode(frame), Buffer.from("\n")]));
  const query = make("GetObjectRequest", { bucket: { name: "customer.inputs" }, objectKey: "data" });
  for (const [payload, media, code] of [
    [lines([header]), "application/x-ndjson", wire.ErrorCode.UNAVAILABLE],
    [lines([header, header, body]), "application/x-ndjson", wire.ErrorCode.UNAVAILABLE],
    [Buffer.concat([encode(header), Buffer.from("\n"), encode(body)]), "application/x-ndjson", wire.ErrorCode.UNAVAILABLE],
    [lines([header, body]), "application/json", wire.ErrorCode.UNAVAILABLE],
    [lines([header, make("GetObjectResponse", { frame: { case: "error", value: { code: wire.ErrorCode.ACCESS_DENIED } } })]), "application/x-ndjson", wire.ErrorCode.ACCESS_DENIED],
    [lines([make("GetObjectResponse", { frame: { case: "header", value: { object: { ...info, size: 999999999n } } } })]), "application/x-ndjson", wire.ErrorCode.QUOTA_EXCEEDED],
    [lines([make("GetObjectResponse", { frame: { case: "header", value: { object: info, contentRange: { start: 0n, end: 0n, total: 1n } } } }), body]), "application/x-ndjson", wire.ErrorCode.UNAVAILABLE],
    [Buffer.from('{"unknown":true}\n'), "application/x-ndjson", wire.ErrorCode.UNAVAILABLE],
  ]) {
    const client = new HttpObjectsV2({ endpoint: "https://objects.example", token: "fixture", fetch: async () => new Response(payload, { headers: { "content-type": media } }) });
    await assert.rejects(client.get(query, 16n), fail(code));
  }
  const oversizedClient = new HttpObjectsV2({ endpoint: "https://objects.example", token: "fixture", fetch: async () => new Response(lines([oversizedHeader, oversizedBody]), { headers: { "content-type": "application/x-ndjson" } }) });
  await assert.rejects(oversizedClient.get(query, 65537n), fail(wire.ErrorCode.UNAVAILABLE));
  const oversizedQuery = toBinary(wire.GetObjectRequestSchema, query);
  assert.throws(() => validate_objects_v2_response("objects/get", oversizedQuery, toBinary(wire.GetObjectResponseSchema, oversizedBody), BigInt(bodyFrameBytes + 1)));
  const corrupt = new HttpObjectsV2({ endpoint: "https://objects.example", token: "fixture", fetch: async () => new Response('{"bucket":{"name":"different.bucket"},"createdAt":"1970-01-01T00:00:00Z"}', { headers: { "content-type": "application/json" } }) });
  await assert.rejects(corrupt.headBucket(make("HeadBucketRequest", { bucket: { name: "customer.inputs" } })), fail(wire.ErrorCode.UNAVAILABLE));
});
