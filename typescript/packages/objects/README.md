# @acyclic-labs/objects

The current-value Objects API uses opaque stable IDs and replaceable content. `ReplaceableObjects` exposes only PUT, current GET, and verification; FS owns versioned file semantics.

```sh
npm install @acyclic-labs/objects
```

```ts
import { ReplaceableObjects, bytesCodec, idempotencyKey, objectId } from "@acyclic-labs/objects";

const objects = ReplaceableObjects.memory();
const object = objectId("opaque-object-id");
const receipt = await objects.replacePut({ object, body: bytesCodec.encode(new TextEncoder().encode("Hello")), idempotencyKey: idempotencyKey("welcome-1") });
const current = await objects.getCurrent(object, { consistency: "weak" });
console.log(receipt.etag, new TextDecoder().decode(current.body));
```

`ReplaceableObjects.fromEnv()` uses `ACYCLIC_OBJECTS_ENDPOINT` and `ACYCLIC_OBJECTS_TOKEN`. The hosted adapter calls `POST /v2/objects/put`, `/get-current`, and `/verify-current` over HTTPS. A missing or 5xx PUT response is ambiguous: retry the exact request and idempotency key.

`ObjectId` and `ETag` are opaque. The public current-value facade does not expose object versions, snapshots, listings, or delete operations. The older `Objects` API and `./proto` export remain available as migration compatibility; new hosted integrations should use `ReplaceableObjects` and `./proto/v2`.

The legacy `Objects`/`HttpObjectsProvider` surface remains documented in its source for migration only. It retains the v1 permanent-version protocol and should not be used by new hosted integrations.

[API source](https://github.com/acyclic-labs/sdk/tree/main/typescript/packages/objects/src) · [Protocol](https://github.com/acyclic-labs/sdk/tree/main/proto/objects)
