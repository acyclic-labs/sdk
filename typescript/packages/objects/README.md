# @acyclic-labs/objects

For Node/Bun gRPC, import `createObjectsGrpcClients` from
`@acyclic-labs/objects/grpc` with `{ endpoint, token }`. The returned
`buckets`, `objects`, `multipart`, and `snapshots` clients expose every
canonical RPC, including client-streaming uploads and server-streaming downloads.
Optional `caCertificate` adds a private PEM CA; `maximumMessageBytes` bounds
each message. Browser applications use `HttpObjectsProvider`.

Typed access to immutable object versions, buckets, snapshots, and multipart uploads. Use the in-memory provider for local tests or the HTTPS provider for a hosted Objects service.

```sh
npm install @acyclic-labs/objects
```

```ts
import { MemoryObjectsProvider, Objects, jsonCodec } from "@acyclic-labs/objects";

const objects = new Objects(new MemoryObjectsProvider());
const bucket = await objects.createBucket("documents");
const json = jsonCodec((value: unknown): { readonly title: string } => {
  if (typeof value !== "object" || value === null || Array.isArray(value) || !("title" in value) || typeof value.title !== "string") {
    throw new TypeError("expected a document with a string title");
  }
  return { title: value.title };
});
const version = await bucket.put("welcome.json", { title: "Hello" }, json);
const { value } = await bucket.get("welcome.json", json, { versionId: version.versionId });
console.log(value.title);
```

`jsonCodec()` without arguments returns a codec for `JsonValue`. To get a more specific value type, pass a parser that validates the decoded JSON and returns that type. The parser runs for every decode, so malformed stored data is rejected instead of being treated as the requested TypeScript type.

For a service, construct `new Objects(new HttpObjectsProvider({ endpoint, token }))` or use `Objects.fromEnv()` with `ACYCLIC_OBJECTS_ENDPOINT` and `ACYCLIC_OBJECTS_TOKEN`. The endpoint must be HTTPS. `BucketRef`, `SnapshotRef`, and version IDs are identities, not names; retain them for subsequent calls.

`put` accepts `condition` (`ifAbsent`, `ifMatch`, or `ifVersion`) and an idempotency key. `bucket.snapshot()` freezes a whole-bucket read view; `bucket.createMultipart()` handles larger bodies. Listings are paginated; use `bucket.pages()` when delimiter prefixes matter. `MemoryObjectsProvider` is process-local and not durable.

[API source](https://github.com/acyclic-labs/sdk/tree/main/typescript/packages/objects/src) · [Protocol](https://github.com/acyclic-labs/sdk/tree/main/proto/objects)
