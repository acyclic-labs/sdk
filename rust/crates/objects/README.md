# acyclic-objects

## Unreleased Objects v2 transition

The unmerged source exposes logical bucket/key contracts at the crate root and
under `acyclic_objects::v2`.
Its generated types and `response` validators are available with
`default-features = false`; services can validate persisted bucket metadata,
object metadata and timestamps without enabling a transport or JSON reflection.
The `grpc` and `http` features expose the corresponding v2 clients, also available
at the crate root as `GrpcObjects` and `HttpObjects`. These source APIs are not a
published v2 release. This breaking transition requires a new package version
before publication.

## Logical Objects clients

Objects v2 addresses the current object through a bucket name and object key.
It has no public object versions, snapshots, or forks. The transport-independent
`v2::ObjectsProvider` interface is implemented by the gRPC, HTTP, deterministic
memory and native local providers. Reads and listings may lag mutations; single-object publication
and its conditions are atomic.

```sh
cargo add acyclic-objects
```

The default `grpc` feature exposes the authenticated remote client. Enable `http`
for `HttpObjects`. Both transports require HTTPS and a caller-supplied bearer
credential. For a private-CA gRPC endpoint, pass the caller-supplied PEM
certificate to `GrpcObjects::connect`.

```rust,no_run
# #[cfg(feature = "grpc")]
use acyclic_objects::GrpcObjects;

# #[cfg(feature = "grpc")]
# async fn example() -> Result<(), Box<dyn std::error::Error>> {
let ca_pem = std::fs::read("trusted-ca.pem")?;
let client = GrpcObjects::connect(
    "https://objects.example", "account-token", Some(&ca_pem),
).await?;
# let _ = client;
# Ok(())
# }
```

Use conditional writes and idempotency keys for retryable mutations. Listings
are bounded, query-bound pages over current keys rather than captured snapshots.
Multipart uploads require an explicit completion manifest; staging and aborting
parts never publishes an object. The canonical v2 schema is
`proto/objects/v2/objects.proto`. `v2::conformance::verify` exercises all 13
operations against a disposable, immediately visible test fixture. Local tests
do not establish live service acceptance.

## Native v2 durability foundation

Enable `local` for `v2::local::LocalObjects`. Its private checksummed journal
records logical state changes and exact retry receipts; immutable segments hold
current bodies and staged multipart parts. Reopen validates live physical bodies,
repairs only an incomplete final record, and preserves current keys and retry
outcomes. An uncertain append makes subsequent reads and mutations unavailable
until the owner closes and reopens the store. A v1 store is rejected without
conversion or overwrite.

`collect_garbage` fences physical readers and mutations, authenticates every
retained segment before deletion, and compacts the journal into bounded private
checkpoints. Current objects, staged parts, exact receipts and pagination
authentication survive checkpoint replacement. Interrupted replacement recovers
the old or new complete journal; uncertain replacement requires reopen.

The v2 provider inlines bodies up to 64 KiB in authenticated journal records,
groups native writes, and scans current state when constructing deltas. Native
filesystem roots now compose this provider through the logical v2 adapter.
Existing v1 roots fail closed; no old-data upgrade path is provided.

## Published v1 history

The active provider, conformance module and generated Rust bindings for v1 have
been removed. Root `wire`, `FILE_DESCRIPTOR_SET`, provider types and `LocalObjects`
select v2. Local capacity and synchronization options remain available; private
segment storage is independent of the retired engine.

The published v1 schema and conformance vectors remain as history. Its unchanged
descriptor is archived at `compatibility/objects/v1/objects_descriptor.bin`,
outside the Objects crate. The RPC matrix records its 17 operations as retired
and verifies that they have no active SDK exposure. This transition still needs
a breaking package version and final required qualification before merge.
