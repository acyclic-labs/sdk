# acyclic-objects

## Unreleased Objects v2 transition

The unmerged source exposes logical bucket/key contracts under `acyclic_objects::v2`.
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
`v2::ObjectsProvider` interface is implemented by the gRPC, HTTP and deterministic
memory providers. Reads and listings may lag mutations; single-object publication
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

## Remaining native v1 migration

The root `wire`, descriptor, provider types, standalone v1 conformance module and
optional `local` provider still support remaining native durable consumers.
Their migration is pending; the idiomatic v1 gRPC wrappers have been removed.
The v1 descriptors and schemas remain intact for published compatibility
history. Enable `local` only for that existing durable provider. The v2 memory
provider is for deterministic local tests and does not provide persistence.

The local provider commits bodies up to 64 KiB inside the journal record itself, so one append and one flush make a small object durable; larger bodies are published as immutable segments first. Once inline bytes outweigh the rest of the journal, and on every garbage collection, the provider compacts the journal: live inline bodies move into segments and the rest are dropped.

The local provider fails closed after an uncertain journal write: reads, mutations, and garbage collection return unavailable until the owner closes and reopens the store. Reopen repairs only an incomplete final frame; corruption or a host read error fails recovery. Retrying a mutation after an unavailable result should use its original idempotency key because the last outcome may be uncertain.
