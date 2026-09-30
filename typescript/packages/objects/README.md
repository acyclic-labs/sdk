# @acyclic-labs/objects

## Canonical logical Objects v2

The unmerged source branch exposes `MemoryObjectsV2`, `HttpObjectsV2` and generated
request/response types at the default package export. `./proto` contains v2
Protobuf messages, `./http` the browser HTTP client, and `./grpc` the complete
Node/Bun gRPC client. Existing `./v2` subpaths resolve to the same contract.

Objects use logical bucket names and current keys, eventual reads/listing,
single-key conditions, bounded ranges, multipart uploads and opaque ETags.
Public versions, history, snapshots and forks are retired.

```ts
import { create } from "@bufbuild/protobuf";
import {
  MemoryObjectsV2, CreateBucketRequestSchema, PutObjectHeaderSchema,
  GetObjectRequestSchema,
} from "@acyclic-labs/objects";

const objects = await MemoryObjectsV2.create();
const bucket = await objects.createBucket(create(CreateBucketRequestSchema, {
  name: "documents",
}));
await objects.put(create(PutObjectHeaderSchema, {
  bucket: bucket.bucket, objectKey: "welcome.txt",
  preconditions: { condition: { case: "ifAbsent", value: true } },
}), new TextEncoder().encode("Hello"));
const value = await objects.get(create(GetObjectRequestSchema, {
  bucket: bucket.bucket, objectKey: "welcome.txt",
}), 1024n);
console.log(new TextDecoder().decode(value.body));
```

Construct `HttpObjectsV2` with `{ endpoint, token }` for HTTPS services. Node/Bun
use `GrpcObjectsV2` or `createObjectsV2GrpcClients` from `./grpc`. The latter
exposes every canonical RPC, including streamed PUT/part uploads and GET, and
owns a session that callers close. Optional `caCertificate` adds a private PEM
CA; request/download/message bounds are explicit. Browsers use HTTP.

## Breaking transition and published history

The v1 TypeScript implementation and its WASM projection are removed from this
source branch. Published 0.1.5 packages and their tagged Git history are unchanged.
The default export transition requires a new breaking package version before
publication; the branch's coordinated version update remains pending. The source
candidate must not replace a published 0.1.5 distribution. Local client tests do
not establish Cloud acceptance or package publication.
