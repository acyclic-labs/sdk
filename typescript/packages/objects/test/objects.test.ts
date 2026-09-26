import { describe, expect, test } from "bun:test";
import { Buffer } from "node:buffer";
import { HttpObjectsProvider, MemoryObjectsProvider, ObjectError, Objects, ObjectsTransportError, bytesCodec, idempotencyKey, jsonCodec, type IdempotencyKey } from "../src/index.js";

const key = (value: string) => value as IdempotencyKey;
const numberJson = jsonCodec(value => {
  if (typeof value !== "number") throw new TypeError("expected a JSON number");
  return value;
});
const answerJson = jsonCodec(value => {
  if (value === null || typeof value !== "object" || !("answer" in value) || typeof value.answer !== "number") {
    throw new TypeError("expected an answer object");
  }
  return { answer: value.answer };
});

describe("objects", () => {
  test("retains independent bytes when native Buffers are stored and returned", async () => {
    const provider = new MemoryObjectsProvider();
    const bucket = await provider.createBucket("bytes");
    const target = { kind: "bucket" as const, bucket };
    const metadata = { contentType: "", contentEncoding: "", cacheControl: "", contentDisposition: "",
      contentLanguage: "", expiresUnixSeconds: undefined, user: new Map<string, string>() };
    const input = Buffer.from([1, 2, 3]);
    await provider.put(bucket, "record", input, metadata);
    input[0] = 9;
    const read = await provider.get(target, "record");
    expect(read.body).toEqual(Uint8Array.from([1, 2, 3]));
    read.body[0] = 8;
    expect((await provider.get(target, "record", undefined, { start: 1, endExclusive: 3 })).body)
      .toEqual(Uint8Array.from([2, 3]));
  });
  test("opens the local provider without exposing provider setup", async () => {
    const bucket = await Objects.memory().createBucket("memory");
    await bucket.put("answer", 42, numberJson);
    expect((await bucket.get("answer", numberJson)).value).toBe(42);
  });

  test("keeps values typed across versions, snapshots, ranges, and forks", async () => {
    const objects = new Objects(new MemoryObjectsProvider());
    const bucket = await objects.createBucket("artifacts", { idempotencyKey: key("create") });
    const first = await bucket.put("result.json", { answer: 42 }, answerJson, { idempotencyKey: key("put") });
    expect((await bucket.get("result.json", answerJson)).value.answer).toBe(42);
    const snapshot = await bucket.snapshot({ idempotencyKey: key("snapshot") });
    await bucket.put("result.json", { answer: 43 }, answerJson, { condition: { kind: "ifVersion", versionId: first.versionId } });
    expect((await snapshot.get("result.json", answerJson)).value.answer).toBe(42);
    const fork = await snapshot.fork("experiment");
    expect((await fork.get("result.json", answerJson)).value.answer).toBe(42);
  });

  test("publishes multipart data only at completion", async () => {
    const provider = new MemoryObjectsProvider();
    const objects = new Objects(provider);
    const bucket = await objects.createBucket("large");
    const userMetadata = new Map([["owner", "creation"]]);
    const createKey = idempotencyKey("create-upload");
    const upload = await bucket.createMultipart("parts", { metadata: { contentType: "text/plain", user: userMetadata }, idempotencyKey: createKey });
    userMetadata.set("owner", "mutated");
    await expect(bucket.createMultipart("parts", { metadata: { contentType: "application/json" }, idempotencyKey: createKey })).rejects.toMatchObject({ code: "idempotency_mismatch" });
    const firstBody = new Uint8Array(5 * 1024 * 1024);
    firstBody.set(new TextEncoder().encode("type"));
    const first = await upload.uploadPart(1, firstBody, { idempotencyKey: idempotencyKey("part-one") });
    expect(await upload.uploadPart(1, firstBody, { idempotencyKey: idempotencyKey("part-one") })).toEqual(first);
    const parts = [first, await upload.uploadPart(2, new TextEncoder().encode("safe"))];
    expect(await upload.listParts()).toEqual(parts);
    await expect(upload.complete([])).rejects.toMatchObject({ code: "invalid_part" });
    await expect(upload.complete([first, first])).rejects.toMatchObject({ code: "invalid_part" });
    await expect(upload.complete([...parts].reverse())).rejects.toMatchObject({ code: "invalid_part" });
    const completed = await upload.complete(parts, { idempotencyKey: idempotencyKey("complete-upload") });
    expect(completed.metadata.user.get("owner")).toBe("creation");
    const combined = (await provider.get(bucket.target, "parts")).body;
    expect(combined.byteLength).toBe(firstBody.byteLength + 4);
    expect(new TextDecoder().decode(combined.subarray(0, 4))).toBe("type");
    expect(new TextDecoder().decode(combined.subarray(-4))).toBe("safe");
  });

  test("binds multipart operations to the complete retained upload identity", async () => {
    const provider = new MemoryObjectsProvider();
    const objects = new Objects(provider);
    const first = await (await objects.createBucket("first-upload")).createMultipart("first");
    const second = await (await objects.createBucket("second-upload")).createMultipart("second");
    const forged = { ...first.upload, uploadId: second.upload.uploadId };

    await expect(provider.listParts(forged)).rejects.toMatchObject({ code: "not_found" });
    await expect(provider.uploadPart(forged, 1, new Uint8Array([1]))).rejects.toMatchObject({ code: "not_found" });
    await expect(provider.completeMultipart(forged, [{ partNumber: 1, etag: '"AQ=="' as never, size: 1 }])).rejects.toMatchObject({ code: "not_found" });
    expect(await provider.abortMultipart(forged)).toBeFalse();
    expect(await second.abort()).toBeTrue();
  });

  test("JSON codecs admit only exact JSON values", () => {
    const codec = jsonCodec();
    expect(codec.decode(codec.encode({ answer: 42, nested: [true, null] }))).toEqual({ answer: 42, nested: [true, null] });
    expect(() => codec.encode(Number.NaN as never)).toThrow("finite");
    expect(() => codec.encode(new Date() as never)).toThrow("plain records");
    expect(() => codec.encode(undefined as never)).toThrow("not JSON-safe");
    const cyclic: Record<string, unknown> = {}; cyclic.self = cyclic;
    expect(() => codec.encode(cyclic as never)).toThrow("cycles");
    const substituted = Object.defineProperty({ answer: 42 }, "toJSON", { value: () => "substituted" });
    expect(() => codec.encode(substituted as never)).toThrow("must not define toJSON");
    let reads = 0;
    const accessor = Object.defineProperty({ answer: 42 }, "toJSON", { get: () => reads++ === 0 ? undefined : () => "substituted" });
    expect(() => codec.encode(accessor as never)).toThrow("must not define toJSON");
    expect(() => answerJson.decode(new TextEncoder().encode('{"answer":"wrong"}'))).toThrow("expected an answer object");
  });

  test("keeps listings stable and conditions, delete markers, ranges, and replay explicit", async () => {
    const provider = new MemoryObjectsProvider();
    const bucket = await new Objects(provider).createBucket("contracts");
    const first = await bucket.put("a/one", new Uint8Array([1, 2, 3]), bytesCodec, { condition: { kind: "ifAbsent" }, idempotencyKey: key("put-one") });
    expect(await bucket.head("a/one")).toEqual(first);
    expect(await bucket.head("a/one", { ifMatch: first.etag })).toEqual(first);
    await expect(bucket.head("a/one", { ifNoneMatch: first.etag })).rejects.toMatchObject({ code: "precondition_failed" });
    expect(await bucket.put("a/one", new Uint8Array([1, 2, 3]), bytesCodec, { condition: { kind: "ifAbsent" }, idempotencyKey: key("put-one") })).toEqual(first);
    await expect(bucket.put("a/one", new Uint8Array([4]), bytesCodec, { condition: { kind: "ifAbsent" } })).rejects.toMatchObject({ code: "precondition_failed" });
    await bucket.put("b/two", new Uint8Array([4, 5]), bytesCodec);
    const firstPage = await bucket.listPage({ pageSize: 1 });
    await bucket.put("c/three", new Uint8Array([6]), bytesCodec);
    const secondPage = await bucket.listPage({ pageSize: 1, continuation: firstPage.continuation });
    expect([...firstPage.entries, ...secondPage.entries].map(entry => entry.objectKey)).toEqual(["a/one", "b/two"]);
    expect((await bucket.listPage({ delimiter: "/" })).commonPrefixes).toEqual(["a/", "b/", "c/"]);
    const directoryPages = [];
    for await (const page of bucket.pages({ delimiter: "/", pageSize: 1 })) directoryPages.push(page);
    expect(directoryPages).toHaveLength(3);
    expect(directoryPages.flatMap(page => page.commonPrefixes)).toEqual(["a/", "b/", "c/"]);
    await expect((async () => { for await (const _entry of bucket.list({ delimiter: "/" })) void _entry; })()).rejects.toThrow("entry-only");
    const range = await bucket.get("a/one", bytesCodec, { range: { start: 1, endExclusive: 3 } });
    expect([...range.value]).toEqual([2, 3]);
    expect(range.contentRange).toEqual({ start: 1, endExclusive: 3, total: 3 });
    const deleted = await bucket.delete("a/one", { condition: { kind: "ifMatch", etag: first.etag } });
    expect(deleted.marker?.deleteMarker).toBeTrue();
    await expect(bucket.get("a/one", bytesCodec)).rejects.toBeInstanceOf(ObjectError);
    expect((await bucket.listPage({ versions: true })).entries.some(entry => entry.version.deleteMarker)).toBeTrue();
    await expect(bucket.put("different", new Uint8Array([9]), bytesCodec, { idempotencyKey: key("put-one") })).rejects.toMatchObject({ code: "idempotency_mismatch" });
  });

  test("managed transport rejects insecure configuration, oversized bodies, and malformed contracts", async () => {
    expect(() => new HttpObjectsProvider({ endpoint: "http://example.test", token: "x" })).toThrow(TypeError);
    expect(() => new HttpObjectsProvider({ endpoint: "https://example.test", token: "x", maximumResponseBytes: 0 })).toThrow(RangeError);
    const malformed = new HttpObjectsProvider({ endpoint: "https://example.test", token: "x", fetcher: async () => new Response(JSON.stringify({ bucketId: "only" })) });
    await expect(malformed.createBucket("x")).rejects.toBeInstanceOf(ObjectsTransportError);
    const bounded = new HttpObjectsProvider({ endpoint: "https://example.test", token: "x", maximumResponseBytes: 2, fetcher: async () => new Response("true") });
    await expect(bounded.deleteBucket({ bucketId: "id" as never, name: "x" })).rejects.toThrow("configured bound");
  });

  test("enforces one live bucket identity per name across creates and forks", async () => {
    const objects = new Objects(new MemoryObjectsProvider());
    const source = await objects.createBucket("source");
    const snapshot = await source.snapshot();
    await source.put("visible", new Uint8Array([1]), bytesCodec);
    expect((await source.snapshot()).head("visible")).resolves.toMatchObject({ size: 1n });
    await objects.createBucket("taken");
    await expect(objects.createBucket("taken")).rejects.toMatchObject({ code: "bucket_exists" });
    await expect(snapshot.fork("taken")).rejects.toMatchObject({ code: "bucket_exists" });
  });

  test("requires every retained object version to be purged before bucket deletion", async () => {
    const provider = new MemoryObjectsProvider();
    const bucket = await new Objects(provider).createBucket("versioned");
    const version = await bucket.put("artifact", new Uint8Array([1]), bytesCodec);
    const marker = (await bucket.delete("artifact")).marker!;
    await expect(bucket.deleteBucket()).rejects.toMatchObject({ code: "bucket_not_empty" });
    await bucket.delete("artifact", { versionId: marker.versionId });
    await bucket.delete("artifact", { versionId: version.versionId });
    expect(await bucket.deleteBucket()).toBeTrue();
  });

  test("binds snapshot destruction to the complete snapshot identity", async () => {
    const provider = new MemoryObjectsProvider();
    const objects = new Objects(provider);
    const source = await objects.createBucket("source");
    const foreign = await objects.createBucket("foreign");
    await source.put("retained", new Uint8Array([1]), bytesCodec);
    const snapshot = await source.snapshot();
    const operation = key("destroy-snapshot");
    expect(await provider.destroySnapshot({
      snapshotId: snapshot.reference.snapshotId,
      sourceBucketId: foreign.reference.bucketId,
    }, operation)).toBeFalse();
    expect(await snapshot.head("retained")).toMatchObject({ size: 1n });
    expect(await snapshot.destroy({ idempotencyKey: key("destroy-valid-snapshot") })).toBeTrue();
  });

  test("binds snapshot listing continuations to the complete snapshot identity", async () => {
    const provider = new MemoryObjectsProvider();
    const objects = new Objects(provider);
    const source = await objects.createBucket("listing-source");
    const foreign = await objects.createBucket("listing-foreign");
    await source.put("a", new Uint8Array([1]), bytesCodec);
    await source.put("b", new Uint8Array([2]), bytesCodec);
    const snapshot = await source.snapshot();
    const first = await snapshot.listPage({ pageSize: 1 });
    const forged = {
      kind: "snapshot" as const,
      snapshot: {
        snapshotId: snapshot.reference.snapshotId,
        sourceBucketId: foreign.reference.bucketId,
      },
    };
    await expect(provider.list(forged, "", undefined, false, 1, first.continuation)).rejects.toMatchObject({ code: "invalid_continuation" });
  });

  test("requires multipart uploads to finish or abort before bucket deletion", async () => {
    const bucket = await new Objects(new MemoryObjectsProvider()).createBucket("uploading");
    const upload = await bucket.createMultipart("artifact");
    await expect(bucket.deleteBucket()).rejects.toMatchObject({ code: "bucket_not_empty" });
    expect(await upload.abort()).toBeTrue();
    expect(await bucket.deleteBucket()).toBeTrue();
  });

  test("constructs the hosted client from explicit or process environment", () => {
    expect(Objects.fromEnv({ endpoint: "https://objects.example", token: "token" }).provider).toBeInstanceOf(HttpObjectsProvider);
    expect(() => Objects.fromEnv({ endpoint: "https://objects.example", token: "" })).toThrow("token is required");
  });

  test("does not expose server error bodies in transport errors", async () => {
    const provider = new HttpObjectsProvider({ endpoint: "https://example.test", token: "x", fetcher: async () => new Response("reflected-secret", { status: 403 }) });
    await expect(provider.deleteBucket({ bucketId: "id" as never, name: "x" })).rejects.toMatchObject({ message: "HTTP 403", status: 403 });
  });

  test("cancels oversized streaming transport responses at the configured bound", async () => {
    let cancelled = false;
    const body = new ReadableStream<Uint8Array>({
      start(controller) { controller.enqueue(new Uint8Array([1, 2, 3])); },
      cancel() { cancelled = true; throw new Error("cancel failed"); },
    });
    const provider = new HttpObjectsProvider({ endpoint: "https://example.test", token: "x", maximumResponseBytes: 2, fetcher: async () => new Response(body) });
    await expect(provider.deleteBucket({ bucketId: "id" as never, name: "x" })).rejects.toBeInstanceOf(ObjectsTransportError);
    expect(cancelled).toBeTrue();
  });
});
