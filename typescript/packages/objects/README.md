# @acyclic-labs/objects

## Objects v1 blobs

`MemoryObjectsV1`, `HttpObjectsV1` and the generated request/response types are
at the default export; `./proto` holds the Protobuf messages, `./http` the
browser HTTP client and `./grpc` the Node/Bun gRPC client.

The caller's account is the whole namespace. A blob is named by the BLAKE3-256
digest of its bytes, so a put is idempotent by content: `created` and
`alreadyExists` are durable and carry the verified digest and length, while
`unknown` means the outcome could not be determined and repeating the same put
is safe. Manifests join existing parts, retained roots keep blobs alive, and
collection reclaims what nothing references. Every reply is checked against its
request by the same Rust rules the Rust clients use.

```ts
import { MemoryObjectsV1, verified } from "@acyclic-labs/objects";

const objects = await MemoryObjectsV1.create();
const put = verified(await objects.put(new TextEncoder().encode("Hello")));
if (!put) throw new Error("outcome unknown; repeating the put is safe");
await objects.retain(new TextEncoder().encode("documents"), [put.digest]);
const value = await objects.get({ digest: put.digest, maximumBytes: 1024n });
console.log(new TextDecoder().decode(value.body));
```

Construct `HttpObjectsV1` with `{ endpoint, token }` for HTTPS services. Node/Bun
use `GrpcObjectsV1` or `createObjectsV1GrpcClients` from `./grpc`, which owns a
session callers close; optional `caCertificate` adds a private PEM CA.

## Breaking transition and published history

The historical TypeScript implementation and its WASM projection are removed
from this source branch. Published 0.1.5 packages and their tagged Git history are unchanged.
The default export transition requires a new breaking package version before
publication; the branch's coordinated version update remains pending. The source
candidate must not replace a published 0.1.5 distribution. Local client tests do
not establish Cloud acceptance or package publication.
