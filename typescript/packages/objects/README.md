# @acyclic-labs/objects

Typed access to immutable object versions, buckets, snapshots, and multipart uploads. Use the in-memory provider for local tests or the HTTPS provider for a hosted Objects service.

```sh
npm install @acyclic-labs/objects
```

```ts
import { MemoryObjectsProvider, Objects, jsonCodec } from "@acyclic-labs/objects";

const objects = new Objects(new MemoryObjectsProvider());
const bucket = await objects.createBucket("documents");
const json = jsonCodec(value => {
  if (value === null || typeof value !== "object" || !("title" in value) || typeof value.title !== "string") {
    throw new TypeError("expected a document with a title");
  }
  return { title: value.title };
});
const version = await bucket.put("welcome.json", { title: "Hello" }, json);
const { value } = await bucket.get("welcome.json", json, { versionId: version.versionId });
console.log(value.title);
```

`jsonCodec()` returns general JSON values. Pass a parser when reads should return a narrower type; the parser checks stored data before the codec promises that type.

For a service, construct `new Objects(new HttpObjectsProvider({ endpoint, token }))` or use `Objects.fromEnv()` with `ACYCLIC_OBJECTS_ENDPOINT` and `ACYCLIC_OBJECTS_TOKEN`. The endpoint must be HTTPS. `BucketRef`, `SnapshotRef`, and version IDs are identities, not names; retain them for subsequent calls.

`put` accepts `condition` (`ifAbsent`, `ifMatch`, or `ifVersion`) and an idempotency key. `bucket.snapshot()` freezes a whole-bucket read view; `bucket.createMultipart()` handles larger bodies and captures its condition when the upload is created. Non-final multipart parts must be at least 5 MiB. Listings are paginated; use `bucket.pages()` when delimiter prefixes matter. `MemoryObjectsProvider` runs the canonical Rust provider through WebAssembly; it is process-local and not durable.

[API source](https://github.com/acyclic-labs/sdk/tree/main/typescript/packages/objects/src) · [Protocol](https://github.com/acyclic-labs/sdk/tree/main/proto/objects)
