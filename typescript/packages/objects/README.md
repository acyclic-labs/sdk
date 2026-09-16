# @acyclic-labs/objects

Typed access to immutable object versions, buckets, snapshots, and multipart uploads. Use the in-memory provider for local tests or the HTTPS provider for a hosted Objects service.

```sh
npm install @acyclic-labs/objects
```

```ts
import { MemoryObjectsProvider, Objects, jsonCodec } from "@acyclic-labs/objects";

const objects = new Objects(new MemoryObjectsProvider());
const bucket = await objects.createBucket("documents");
const json = jsonCodec<{ title: string }>();
const version = await bucket.put("welcome.json", { title: "Hello" }, json);
const { value } = await bucket.get("welcome.json", json, { versionId: version.versionId });
console.log(value.title);
```

For a service, construct `new Objects(new HttpObjectsProvider({ endpoint, token }))` or use `Objects.fromEnv()` with `ACYCLIC_OBJECTS_ENDPOINT` and `ACYCLIC_OBJECTS_TOKEN`. The endpoint must be HTTPS. `BucketRef`, `SnapshotRef`, and version IDs are identities, not names; retain them for subsequent calls.

`put` accepts `condition` (`ifAbsent`, `ifMatch`, or `ifVersion`) and an idempotency key. `bucket.snapshot()` freezes a whole-bucket read view; `bucket.createMultipart()` handles larger bodies. Listings are paginated; use `bucket.pages()` when delimiter prefixes matter. `MemoryObjectsProvider` is process-local and not durable.

[API source](https://github.com/acyclic-labs/sdk/tree/main/typescript/packages/objects/src) · [Protocol](https://github.com/acyclic-labs/sdk/tree/main/proto/objects)
