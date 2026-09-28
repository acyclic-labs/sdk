import { describe, expect, test } from "bun:test";
import { HttpObjectsProvider, ObjectError, ObjectsTransportError } from "../src/index.js";

const bucket = { bucketId: "bucket" as never, name: "objects" };

describe("hosted Objects WASM response projection", () => {
  test("preserves Rust-owned hosted domain errors and rejects unknown codes", async () => {
    const provider = (body: unknown) => new HttpObjectsProvider({
      endpoint: "https://example.test",
      token: "token",
      fetcher: async () => new Response(JSON.stringify(body), { status: 409 }),
    });
    await expect(provider({ error: { code: "bucket_exists", message: "name taken" } }).createBucket("taken"))
      .rejects.toMatchObject({ code: "bucket_exists", message: "name taken" });
    await expect(provider({ code: null, message: null, error: { code: "bucket_exists", message: "name taken" } }).createBucket("taken"))
      .rejects.toMatchObject({ code: "bucket_exists", message: "name taken" });
    await expect(provider({ code: "precondition_failed", message: "bucket has objects" }).deleteBucket(bucket))
      .rejects.toMatchObject({ code: "bucket_not_empty", message: "bucket has objects" });
    await expect(provider({ code: "new_server_code" }).createBucket("taken"))
      .rejects.toBeInstanceOf(ObjectsTransportError);
    await expect(provider({ code: "not_found" }).headBucket(bucket))
      .rejects.toBeInstanceOf(ObjectError);
  });

  test("forwards Rust head validators and version selection", async () => {
    let requestBody = "";
    const provider = new HttpObjectsProvider({
      endpoint: "https://example.test",
      token: "token",
      fetcher: async (_input, init) => {
        requestBody = String(init?.body);
        return new Response(JSON.stringify({
          versionId: "version",
          etag: "etag",
          size: { $bigint: "0" },
          deleteMarker: false,
          metadata: { user: { $map: [] } },
        }));
      },
    });

    await provider.head(bucket, "key", { versionId: "version" as never, ifMatch: "etag" as never, ifNoneMatch: "other" as never });

    expect(JSON.parse(requestBody)).toEqual({
      target: bucket,
      objectKey: "key",
      versionId: "version",
      ifMatch: "etag",
      ifNoneMatch: "other",
    });
  });

  test("encodes request bigint, bytes, map, and metadata wrappers in Rust", async () => {
    let requestBody = "";
    const provider = new HttpObjectsProvider({
      endpoint: "https://example.test",
      token: "token",
      fetcher: async (_input, init) => {
        requestBody = String(init?.body);
        return new Response(JSON.stringify({
          versionId: "version",
          etag: "etag",
          size: { $bigint: "2" },
          deleteMarker: false,
          createdAt: "2030-01-02T03:04:05.000Z",
          metadata: { user: { $map: [] } },
        }));
      },
    });

    await provider.put(bucket, "key", new Uint8Array([0, 255]), {
      contentType: "text/plain",
      contentEncoding: "",
      cacheControl: "",
      contentDisposition: "",
      contentLanguage: "",
      expiresUnixSeconds: 9007199254740993n,
      user: new Map([["owner", "test"]]),
    }, { kind: "ifVersion", versionId: "old" as never }, "request-key" as never);

    expect(JSON.parse(requestBody)).toEqual({
      bucket,
      objectKey: "key",
      body: { $bytes: "AP8=" },
      metadata: {
        contentType: "text/plain",
        contentEncoding: "",
        cacheControl: "",
        contentDisposition: "",
        contentLanguage: "",
        expiresUnixSeconds: { $bigint: "9007199254740993" },
        user: { $map: [["owner", "test"]] },
      },
      condition: { kind: "ifVersion", versionId: "old" },
      idempotencyKey: "request-key",
    });
  });

  test("rejects request cycles before fetching", async () => {
    const cyclic: Record<string, unknown> = {};
    cyclic.self = cyclic;
    let fetched = false;
    const provider = new HttpObjectsProvider({
      endpoint: "https://example.test",
      token: "token",
      fetcher: async () => {
        fetched = true;
        return new Response("{}");
      },
    });

    await expect(provider.createBucket(cyclic as never)).rejects.toThrow("cycles");
    expect(fetched).toBeFalse();
  });

  test("rejects accessor properties without invoking getters", async () => {
    let reads = 0;
    const request = Object.defineProperty({}, "name", {
      enumerable: true,
      get() {
        reads += 1;
        throw new Error("getter must not run");
      },
    });
    const provider = new HttpObjectsProvider({
      endpoint: "https://example.test",
      token: "token",
      fetcher: async () => new Response("{}"),
    });

    await expect(provider.createBucket(request as never)).rejects.toThrow("accessor");
    expect(reads).toBe(0);
  });

  test("projects large integers, bytes, maps, and metadata defaults in Rust", async () => {
    const provider = new HttpObjectsProvider({
      endpoint: "https://example.test",
      token: "token",
      fetcher: async () => new Response(JSON.stringify({
        version: {
          versionId: "version",
          etag: "etag",
          ["__proto__"]: "retained-wire-key",
          size: { $bigint: "18446744073709551615" },
          deleteMarker: false,
          createdAt: "2030-01-02T03:04:05.123456789Z",
          metadata: { user: { $map: [["owner", "test"]] } },
        },
        body: { $bytes: "AP+A" },
      })),
    });

    const result = await provider.get(bucket, "key");
    expect(result.version.size).toBe(18446744073709551615n);
    expect(result.version.createdAt).toBeInstanceOf(Date);
    expect(result.version.createdAt?.toISOString()).toBe("2030-01-02T03:04:05.123Z");
    expect(result.version.createdAtUnixNanos).toBe(1893553445123456789n);
    expect(result.body).toEqual(new Uint8Array([0, 255, 128]));
    expect(Object.prototype.hasOwnProperty.call(result.version, "__proto__")).toBeTrue();
    expect((result.version as Record<string, unknown>)["__proto__"]).toBe("retained-wire-key");
    expect(result.version.metadata).toEqual({
      contentType: "",
      contentEncoding: "",
      cacheControl: "",
      contentDisposition: "",
      contentLanguage: "",
      expiresUnixSeconds: undefined,
      user: new Map([["owner", "test"]]),
    });
  });

  test("rejects malformed wrapper bytes before projection", async () => {
    const provider = new HttpObjectsProvider({
      endpoint: "https://example.test",
      token: "token",
      fetcher: async () => new Response(JSON.stringify({
        version: {
          versionId: "version",
          etag: "etag",
          size: { $bigint: "1" },
          deleteMarker: false,
          metadata: { user: { $map: [] } },
        },
        body: { $bytes: "A" },
      })),
    });

    await expect(provider.get(bucket, "key")).rejects.toBeInstanceOf(ObjectsTransportError);
  });

  test("rejects malformed creation timestamps before projection", async () => {
    const provider = new HttpObjectsProvider({
      endpoint: "https://example.test",
      token: "token",
      fetcher: async () => new Response(JSON.stringify({
        versionId: "version",
        etag: "etag",
        size: { $bigint: "1" },
        deleteMarker: false,
        createdAt: "not-a-date",
        metadata: { user: { $map: [] } },
      })),
    });

    await expect(provider.head(bucket, "key")).rejects.toBeInstanceOf(ObjectsTransportError);
  });
});
