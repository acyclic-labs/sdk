import assert from "node:assert/strict";
import { test } from "node:test";
import { create, fromJsonString, toJsonString } from "@bufbuild/protobuf";
import { HttpObjectsV1 } from "../dist/v1-http.js";
import * as wire from "../generated/proto/objects/v1/objects_pb.js";

const make = (name, value) => create(wire[`${name}Schema`], value);
const upload = [
  make("PutObjectsRequest", { frame: { case: "header", value: { objects: [{ bucket: { name: "batch" }, objectKey: "key" }] } } }),
  make("PutObjectsRequest", { frame: { case: "chunk", value: { index: 0, frame: { case: "body", value: new Uint8Array([1, 2]) } } } }),
  make("PutObjectsRequest", { frame: { case: "chunk", value: { index: 0, frame: { case: "complete", value: true } } } }),
  make("PutObjectsRequest", { frame: { case: "complete", value: true } }),
];
const query = make("GetObjectsRequest", { objects: [{ request: { bucket: { name: "batch" }, objectKey: "key" }, maximumBytes: 2n }] });

test("canonical HTTP batch bindings use one credential and preserve individual outcomes", async () => {
  let credentials = 0;
  const calls = [];
  const info = { etag: "receipt", size: 2n, lastModified: { seconds: 0n, nanos: 0 } };
  const download = [
    make("GetObjectsResponse", { index: 0, frame: { case: "header", value: { object: info } } }),
    make("GetObjectsResponse", { index: 0, frame: { case: "body", value: { data: new Uint8Array([1, 2]), decodedLength: 2n } } }),
    make("GetObjectsResponse", { index: 0, frame: { case: "complete", value: true } }),
    make("GetObjectsResponse", { index: 1, frame: { case: "error", value: { code: wire.ErrorCode.NOT_FOUND } } }),
  ];
  const client = new HttpObjectsV1({ endpoint: "http://127.0.0.1", credential: () => { credentials++; return "batch-token"; }, fetch: async (url, init) => {
    calls.push(url.pathname);
    assert.equal(init.headers.authorization, "Bearer batch-token");
    const body = new TextDecoder().decode(init.body);
    if (url.pathname.endsWith("put-batch")) {
      assert.equal(init.headers["content-type"], "application/x-ndjson");
      const frames = body.trimEnd().split("\n").map(line => fromJsonString(wire.PutObjectsRequestSchema, line));
      assert.deepEqual(frames, upload);
      return new Response(toJsonString(wire.PutObjectsResponseSchema, make("PutObjectsResponse", { results: [{ result: { case: "object", value: info } }] })), { headers: { "content-type": "application/json" } });
    }
    assert.deepEqual(fromJsonString(wire.GetObjectsRequestSchema, body), query);
    const encoded = new TextEncoder().encode(download.map(frame => toJsonString(wire.GetObjectsResponseSchema, frame)).join("\n") + "\n");
    return new Response(new ReadableStream({ start(controller) {
      for (const byte of encoded) controller.enqueue(new Uint8Array([byte]));
      controller.close();
    } }), { headers: { "content-type": "application/x-ndjson" } });
  } });
  const put = await client.putBatch(upload);
  assert.equal(put.results[0].result.value.size, 2n);
  assert.deepEqual(await client.getBatch(query), download);
  assert.equal(credentials, 2);
  assert.deepEqual(calls, ["/v1/objects/objects/put-batch", "/v1/objects/objects/get-batch"]);
});

test("batch HTTP NDJSON rejects missing terminators, wrong media and exhausted response bounds", async () => {
  const frame = toJsonString(wire.GetObjectsResponseSchema, make("GetObjectsResponse", { frame: { case: "complete", value: true } }));
  for (const [body, media, maximumResponseBytes] of [[frame, "application/x-ndjson", 1024], [frame + "\n", "application/json", 1024], [frame + "\n", "application/x-ndjson", 1]]) {
    const client = new HttpObjectsV1({ endpoint: "http://127.0.0.1", token: "token", maximumResponseBytes, fetch: async () => new Response(body, { headers: { "content-type": media } }) });
    await assert.rejects(client.getBatch(query), error => [wire.ErrorCode.UNAVAILABLE, wire.ErrorCode.QUOTA_EXCEEDED].includes(error.code));
  }
});

test("batch HTTP refuses oversized requests before dispatch", async () => {
  let calls = 0;
  const client = new HttpObjectsV1({ endpoint: "http://127.0.0.1", token: "token", maximumRequestBytes: 1, fetch: async () => { calls++; throw new Error("unexpected dispatch"); } });
  await assert.rejects(client.putBatch(upload), error => error.code === wire.ErrorCode.QUOTA_EXCEEDED);
  assert.equal(calls, 0);
});

test("batch HTTP admits canonical 1000-object headers and stops before later body serialization", async () => {
  const object = { bucket: { name: "batch" }, objectKey: "key", metadata: { contentType: "x".repeat(8192) }, preconditions: { condition: { case: "ifMatch", value: "x".repeat(8192) } } };
  const header = make("PutObjectsRequest", { frame: { case: "header", value: { objects: Array.from({ length: 1000 }, () => object) } } });
  let calls = 0;
  const client = new HttpObjectsV1({ endpoint: "http://127.0.0.1", token: "token", fetch: async (_url, init) => {
    calls++;
    assert.ok(init.body.byteLength > 12 * 1024 * 1024);
    return new Response("{}", { headers: { "content-type": "application/json" } });
  } });
  await client.putBatch([header]);
  assert.equal(calls, 1);
  const oversized = make("PutObjectsRequest", { frame: { case: "header", value: { objects: Array.from({ length: 1000 }, () => ({ ...object, objectKey: "x".repeat(1024) })) } } });
  await assert.rejects(client.putBatch([oversized]));
  assert.equal(calls, 1);
  let examined = 0;
  const bodies = Array.from({ length: 1000 }, () => {
    const frame = make("PutObjectsRequest", { frame: { case: "chunk", value: { frame: { case: "body", value: new Uint8Array(65536) } } } });
    Object.defineProperty(frame, "frame", { get() { examined++; throw new Error("later frame processed"); } });
    return frame;
  });
  const bounded = new HttpObjectsV1({ endpoint: "http://127.0.0.1", token: "token", maximumRequestBytes: 1, fetch: async () => { throw new Error("dispatch"); } });
  await assert.rejects(bounded.putBatch([upload[0], ...bodies]), error => error.code === wire.ErrorCode.QUOTA_EXCEEDED);
  assert.equal(examined, 0);
  // Eager serialization is caught even when a later frame's data would be discarded.
  assert.throws(() => bodies.map(frame => frame.frame), /later frame processed/);
  assert.ok(examined > 0);
});