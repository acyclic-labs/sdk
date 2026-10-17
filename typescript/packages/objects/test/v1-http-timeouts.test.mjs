import { test } from "node:test";
import assert from "node:assert/strict";
import { create, toBinary } from "@bufbuild/protobuf";
import * as wire from "@acyclic-labs/objects/proto";
import { MemoryObjectsV1 } from "@acyclic-labs/objects";
import { HttpObjectsV1 } from "@acyclic-labs/objects/http";
import { encode_objects_v1_json } from "../generated/wasm/acyclic_objects_wasm.js";

const endpoint = "https://objects.example";
const bucket = { name: "customer.inputs" };
const make = (name, value) => create(wire[`${name}Schema`], value);
const encoded = (name, value) => encode_objects_v1_json(name, toBinary(wire[`${name}Schema`], make(name, value)), 128 * 1024);
const unavailable = error => error.code === wire.ErrorCode.UNAVAILABLE;

function delayedJson(type, value, delay) {
  return async (_url, { signal }) => new Promise((resolve, reject) => {
    const abort = () => { clearTimeout(timer); reject(signal.reason); };
    const timer = setTimeout(() => {
      signal.removeEventListener("abort", abort);
      resolve(new Response(encoded(type, value), { headers: { "content-type": "application/json" } }));
    }, delay);
    signal.addEventListener("abort", abort, { once: true });
    if (signal.aborted) abort();
  });
}

test("HTTP transfer configuration rejects non-finite and out-of-range deadlines", () => {
  for (const transferTimeoutMs of [0, -1, 0.5, NaN, Infinity, 1_800_001, 0x1_0000_0000]) {
    assert.throws(() => new HttpObjectsV1({ endpoint, token: "fixture", transferTimeoutMs }), RangeError);
  }
  for (const transferTimeoutMs of [1, 120_000, 1_800_000]) {
    assert.doesNotThrow(() => new HttpObjectsV1({ endpoint, token: "fixture", transferTimeoutMs }));
  }
});

test("HTTP PUT and multipart parts use the selected whole-request deadline", async () => {
  await MemoryObjectsV1.create();
  const data = new TextEncoder().encode("body");
  const info = { etag: "opaque", size: 4n, lastModified: { seconds: 0n, nanos: 0 } };
  for (const multipart of [false, true]) {
    const fetch = delayedJson(multipart ? "UploadedPart" : "ObjectInfo",
      multipart ? { partNumber: 1, etag: "part", size: 4n } : info, 160);
    const call = provider => multipart
      ? provider.uploadPart(make("UploadPartHeader", { bucket, objectKey: "parts", uploadId: "upload", partNumber: 1 }), data)
      : provider.put(make("PutObjectHeader", { bucket, objectKey: "value" }), data);
    const short = new HttpObjectsV1({ endpoint, token: "fixture", fetch, transferTimeoutMs: 40 });
    const long = new HttpObjectsV1({ endpoint, token: "fixture", fetch, transferTimeoutMs: 1000 });
    await assert.rejects(call(short), unavailable);
    assert.equal((await call(long)).size, 4n);
  }
});

test("HTTP metadata keeps its default deadline when the transfer budget is shortened", async () => {
  await MemoryObjectsV1.create();
  const value = { bucket, createdAt: { seconds: 0n, nanos: 0 } };
  const provider = new HttpObjectsV1({ endpoint, token: "fixture", transferTimeoutMs: 40,
    fetch: delayedJson("Bucket", value, 160) });
  assert.deepEqual(await provider.headBucket(make("HeadBucketRequest", { bucket })), make("Bucket", value));
});

test("HTTP download progress cannot reset the deadline and abort releases the reader", async () => {
  await MemoryObjectsV1.create();
  const line = value => new TextEncoder().encode(new TextDecoder().decode(encoded("GetObjectResponse", value)) + "\n");
  const header = line({ frame: { case: "header", value: { object: {
    etag: "opaque", size: 4n, lastModified: { seconds: 0n, nanos: 0 },
  } } } });
  let emitted = 0;
  let aborted = 0;
  let activeTimers = 0;
  const fetch = async (_url, { signal }) => {
    let timer;
    let abort;
    const cleanup = () => {
      if (timer !== undefined) { clearInterval(timer); timer = undefined; activeTimers--; }
      signal.removeEventListener("abort", abort);
    };
    const stream = new ReadableStream({
      start(controller) {
        let index = 0;
        controller.enqueue(header);
        abort = () => { aborted++; cleanup(); controller.error(signal.reason); };
        signal.addEventListener("abort", abort, { once: true });
        const data = new TextEncoder().encode("body");
        timer = setInterval(() => {
          emitted++;
          controller.enqueue(line({ frame: { case: "body", value: {
            codec: wire.Codec.NONE, data: data.subarray(index, index + 1), decodedLength: 1n,
          } } }));
          index++;
          if (index === data.length) { cleanup(); controller.close(); }
        }, 75);
        activeTimers++;
        if (signal.aborted) abort();
      },
      cancel() { cleanup(); },
    });
    return new Response(stream, { headers: { "content-type": "application/x-ndjson" } });
  };
  const query = make("GetObjectRequest", { bucket, objectKey: "value" });
  const short = new HttpObjectsV1({ endpoint, token: "fixture", fetch, transferTimeoutMs: 200 });
  await assert.rejects(short.get(query, 4n), unavailable);
  assert.ok(emitted > 0 && emitted < 4);
  assert.equal(aborted, 1);
  assert.equal(activeTimers, 0);
  const long = new HttpObjectsV1({ endpoint, token: "fixture", fetch, transferTimeoutMs: 1000 });
  assert.deepEqual((await long.get(query, 4n)).body, new TextEncoder().encode("body"));
  assert.equal(aborted, 1);
  assert.equal(activeTimers, 0);
});
