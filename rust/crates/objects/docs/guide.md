# Objects v2 Rust guide

[`acyclic-objects` v2](../src/v2/mod.rs#L1) is a logical bucket/key contract. The current object is
addressed by bucket and key. Reads and listings may lag mutations, while one
object publication and its conditions are atomic.

The default client configuration includes the gRPC transport. HTTP and local
providers use the same provider traits as the transport-independent types.

## A transport-independent in-memory example

[`MemoryObjects::with_default_bucket`](../src/v2/memory.rs#L94) provides a
deterministic in-memory implementation for local tests and examples.

```rust
use acyclic_objects::{wire, MemoryObjects, ObjectsProvider};
use bytes::Bytes;

#[tokio::main(flavor = "current_thread")]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let (provider, bucket) = MemoryObjects::with_default_bucket();
    provider
        .put(
            wire::PutObjectHeader {
                bucket: Some(bucket.clone()),
                object_key: "hello.txt".into(),
                ..Default::default()
            },
            Bytes::from_static(b"hello"),
        )
        .await?;
    let object = provider
        .get(
            wire::GetObjectRequest {
                bucket: Some(bucket),
                object_key: "hello.txt".into(),
                ..Default::default()
            },
            1024,
        )
        .await?;
    assert_eq!(object.body, Bytes::from_static(b"hello"));
    Ok(())
}
```

The [`ObjectsProvider`](../src/v2/mod.rs#L158) trait is shared by memory, native local, gRPC, and HTTP
implementations. [`get`](../src/v2/mod.rs#L183) requires an explicit allocation bound. Streaming
transports expose a validated header followed by bounded chunks; dropping a
download cancels the caller's observation, while an upload source failure
aborts publication.

## Current values, conditions, and retries

[`wire::Preconditions`](../src/generated/acyclic.objects.v2.rs#L36) and mutation identities let PUT and delete requests carry current-value preconditions and a mutation
identity. The service evaluates the condition atomically with publication.
Retrying the same logical mutation with the same identity returns the retained
receipt; changing the request under that identity is an idempotency mismatch.
Multipart parts stay private until an explicit completion manifest publishes
the object. Aborting or an incomplete upload never publishes a current value.

Listings are live, bounded queries over current keys in lexical order. A page
can be truncated and resumed with its continuation token; it is not a captured
snapshot. [`wire::ObjectInfo`](../src/generated/acyclic.objects.v2.rs#L57) and timestamps are validated by `response`, but the
crate exposes no public object-version pointer.

## Durability and native local storage

Enable `local` on a native target to use `LocalObjects::open`. Its private
checksummed journal records logical state changes and exact retry receipts;
immutable body segments retain current bodies and staged multipart parts. A
v1 store is rejected without conversion or overwrite. Reopen repairs only an
incomplete final record; an uncertain append makes later operations unavailable
until the owner closes and reopens the store.

[`LocalObjects::collect_garbage`](../src/v2/local.rs#L84) fences physical readers and mutations,
authenticates retained segments before deletion, and compacts private
checkpoints. Current objects, staged parts, receipts, and pagination
authentication survive checkpoint replacement. These are native storage
behaviors, not public object history.

## Route and legacy-topic coverage

[`acyclic_objects::v2::HTTP_ROUTES`](../src/v2/mod.rs#L20) is the Rust-owned inventory of 13 operations
under `/v2/objects/`: bucket create/head/delete; object put/get/head/delete/list;
and multipart create/upload-part/list-parts/complete/abort.

The legacy guide topics map to the current source as follows:

| Topic | Rust-owned status |
| --- | --- |
| Quickstart | Use the in-memory example above, then qualify a transport separately. |
| Conditional operations | `ObjectsProvider` preconditions and mutation identities. |
| Listing consistency | Live bounded listings; reads/listings may lag mutations. |
| Object metadata | `wire::ObjectInfo`, `response::object_info`, and timestamp validation. |
| Lifecycle and durability limits | `LocalObjects`, `LocalObjectsLimits`, reopen rules, and garbage collection. |
| S3 compatibility | No S3-compatible endpoint or adapter is exposed by this crate. |
| Snapshots and forks | Not applicable: v2 has no public versions, snapshots, or forks. |

The published v1 descriptor and vectors remain immutable compatibility history.
They are not active v2 APIs and do not add version or snapshot semantics to the
current logical contract.
