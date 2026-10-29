import { ownFixtureServer } from "../../../../scripts/fixture-server.mjs";
import { bytes, lifecycle } from "./v1-lifecycle.mjs";
import { test } from "node:test";
import assert from "node:assert/strict";
import { createServer } from "node:http";
import { once } from "node:events";
import { create, fromBinary, toBinary } from "@bufbuild/protobuf";
import * as wire from "@acyclic-labs/objects/proto";
import { MemoryObjectsV1, blobDigest, verified } from "@acyclic-labs/objects";
import { HttpObjectsV1 } from "@acyclic-labs/objects/http";
import { ObjectsV1Memory, objects_v1_http_type, decode_objects_v1_json, encode_objects_v1_json, verify_objects_v1 } from "../generated/wasm/acyclic_objects_wasm.js";

const expectedRoutes = ["blobs/put", "blobs/put-manifest", "blobs/get", "blobs/head", "blobs/delete", "blobs/list", "blobs/retain", "blobs/release", "blobs/collect", "blobs/usage"];
const fail = (code) => error => error.code === code;
const json = (name, schema, value) => Buffer.from(encode_objects_v1_json(name, toBinary(schema, create(schema, value)), 1 << 20));
const replying = payload => async () => new Response(payload, { headers: { "content-type": "application/json" } });

async function fixture() {
  await MemoryObjectsV1.create(); // initialize the shared WASM runtime
  const memory = new ObjectsV1Memory();
  const seen = new Set();
  const server = createServer(async (request, response) => {
    try {
      if (request.headers.authorization !== "Bearer fixture") throw { code: wire.ErrorCode.ACCESS_DENIED };
      assert.equal(request.method, "POST");
      assert.equal(request.headers["content-type"], "application/json");
      const route = request.url.replace(/^\/v1\/objects\//, "");
      seen.add(route);
      const chunks = [];
      for await (const chunk of request) chunks.push(chunk);
      const input = decode_objects_v1_json(objects_v1_http_type(route, false), Buffer.concat(chunks), 1 << 30);
      const payload = Buffer.from(encode_objects_v1_json(objects_v1_http_type(route, true), await memory.invoke(route, input), 1 << 30));
      response.writeHead(200, { "content-type": "application/json" });
      for (let offset = 0; offset < payload.length; offset += 4099) response.write(payload.subarray(offset, offset + 4099));
      response.end();
    } catch (error) {
      const code = typeof error.code === "number" ? error.code : wire.ErrorCode.UNAVAILABLE;
      response.writeHead(code === wire.ErrorCode.ACCESS_DENIED ? 403 : 400, { "content-type": "application/json" });
      response.end(json("ErrorDetail", wire.ErrorDetailSchema, { code }));
    }
  });
  const close = ownFixtureServer(server);
  try {
    server.listen(0, "127.0.0.1");
    await once(server, "listening");
    return { endpoint: `http://127.0.0.1:${server.address().port}`, seen, close };
  } catch (error) { await close(); throw error; }
}

test("Objects v1 Rust WASM memory provider serves the whole blob lifecycle", async () => {
  await lifecycle(await MemoryObjectsV1.create());
});

test("Objects v1 owned binary bindings retain validation and caller bytes", async () => {
  await MemoryObjectsV1.create();
  const memory = new ObjectsV1Memory();
  const body = new Uint8Array(1024 * 1024).fill(37);
  const digest = await blobDigest(body);
  const input = toBinary(wire.PutBlobRequestSchema, create(wire.PutBlobRequestSchema, { digest, body }));
  const original = input.slice();
  const output = await memory.invoke("blobs/put", input);
  verify_objects_v1("blobs/put", input, output);
  assert.deepEqual(input, original);
  const request = toBinary(wire.GetBlobRequestSchema, create(wire.GetBlobRequestSchema, { digest, maximumBytes: BigInt(body.length) }));
  const reply = await memory.invoke("blobs/get", request);
  verify_objects_v1("blobs/get", request, reply);
  const decoded = fromBinary(wire.GetBlobResponseSchema, reply);
  assert.deepEqual(decoded.body, body);
  decoded.body[0] ^= 1;
  const corrupt = toBinary(wire.GetBlobResponseSchema, decoded);
  assert.throws(() => verify_objects_v1("blobs/get", request, corrupt), fail(wire.ErrorCode.UNAVAILABLE));
  await assert.rejects(memory.invoke("blobs/put", new Uint8Array([0xff])), fail(wire.ErrorCode.INVALID_ARGUMENT));
  assert.throws(() => verify_objects_v1("blobs/get", request, new Uint8Array([0xff])), fail(wire.ErrorCode.UNAVAILABLE));
});
test("Objects v1 HTTP serves every canonical route, typed errors and authentication", async () => {
  const server = await fixture();
  try {
    await lifecycle(new HttpObjectsV1({ endpoint: server.endpoint, token: "fixture" }));
    assert.deepEqual([...server.seen].sort(), [...expectedRoutes].sort());
    await assert.rejects(new HttpObjectsV1({ endpoint: server.endpoint, token: "wrong" }).usage(), fail(wire.ErrorCode.ACCESS_DENIED));
  } finally { await server.close(); }
});
test("Objects v1 never reports a reply that does not answer its request", async () => {
  await MemoryObjectsV1.create();
  const digest = await blobDigest(bytes);
  const other = await blobDigest(new Uint8Array([1]));
  const endpoint = "https://objects.example";
  const client = payload => new HttpObjectsV1({ endpoint, token: "fixture", fetch: replying(payload) });
  // Created must name exactly the published digest and length.
  for (const value of [{ digest: other, length: BigInt(bytes.length) }, { digest, length: 1n }]) {
    await assert.rejects(client(json("PutResponse", wire.PutResponseSchema, { outcome: { case: "created", value } })).put(bytes), fail(wire.ErrorCode.UNAVAILABLE));
  }
  // An unknown outcome stays unknown: no digest is reported.
  const unknown = await client(json("PutResponse", wire.PutResponseSchema, { outcome: { case: "unknown", value: true } })).put(bytes);
  assert.equal(verified(unknown), undefined);
  await assert.rejects(client(json("PutResponse", wire.PutResponseSchema, {})).put(bytes), fail(wire.ErrorCode.UNAVAILABLE));
  // A service-side digest refusal keeps its own code.
  const refusing = new HttpObjectsV1({ endpoint, token: "fixture", fetch: async () => new Response(json("ErrorDetail", wire.ErrorDetailSchema, { code: wire.ErrorCode.DIGEST_MISMATCH }), { status: 400 }) });
  await assert.rejects(refusing.put(bytes), fail(wire.ErrorCode.DIGEST_MISMATCH));
  const info = { digest, kind: wire.Kind.BLOB, length: 4n };
  for (const reply of [
    { info: { ...info, digest: other }, start: 0n, body: new Uint8Array(4) },
    { info, start: 1n, body: new Uint8Array(4) },
    { info, start: 0n, body: new Uint8Array(3) },
    // A whole plain blob must hash to its digest.
    { info, start: 0n, body: new Uint8Array(4) },
  ]) {
    await assert.rejects(client(json("GetBlobResponse", wire.GetBlobResponseSchema, reply)).get({ digest, maximumBytes: 4n }), fail(wire.ErrorCode.UNAVAILABLE));
  }
  await assert.rejects(client(json("BlobInfo", wire.BlobInfoSchema, { ...info, digest: other })).head(digest), fail(wire.ErrorCode.UNAVAILABLE));
  await assert.rejects(client(json("ListBlobsResponse", wire.ListBlobsResponseSchema, { digests: [digest, other, digest] })).list({ limit: 2 }), fail(wire.ErrorCode.UNAVAILABLE));
});
test("Objects v1 HTTP bounds responses and refuses unsafe endpoints", async () => {
  let calls = 0;
  const fetcher = async () => { calls++; return new Response(new Uint8Array(100), { headers: { "content-type": "application/json" } }); };
  const client = new HttpObjectsV1({ endpoint: "https://objects.example", token: "fixture", maximumResponseBytes: 16, fetch: fetcher });
  await assert.rejects(client.usage(), fail(wire.ErrorCode.QUOTA_EXCEEDED));
  assert.equal(calls, 1);
  for (const endpoint of ["http://objects.example", "https://user@objects.example", "https://objects.example/?query=1", "https://objects.example/#fragment"]) assert.throws(() => new HttpObjectsV1({ endpoint, token: "fixture" }));
});
test("Objects v1 HTTP bounds a call by the caller's deadline alone", async () => {
  await MemoryObjectsV1.create();
  const endpoint = "https://objects.example";
  const hanging = async (_url, init) => new Promise((_, reject) => init.signal.addEventListener("abort", () => reject(init.signal.reason)));
  await assert.rejects(new HttpObjectsV1({ endpoint, token: "fixture", timeoutMs: 20, fetch: hanging }).usage(), fail(wire.ErrorCode.DEADLINE_ELAPSED));
  // Negative control: a reply inside a long caller deadline is served.
  const late = async () => { await new Promise(resolve => setTimeout(resolve, 50)); return replying(json("UsageResponse", wire.UsageResponseSchema, { blobs: 2n }))(); };
  assert.equal((await new HttpObjectsV1({ endpoint, token: "fixture", timeoutMs: 0x7fff_ffff, fetch: late }).usage()).blobs, 2n);
  for (const timeoutMs of [0, 0x8000_0000, 1.5]) assert.throws(() => new HttpObjectsV1({ endpoint, token: "fixture", timeoutMs }), RangeError);
});
