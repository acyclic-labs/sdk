# acyclic-objects

## Unreleased Objects v1 transition

The unmerged source exposes logical bucket/key contracts at the crate root and
under `acyclic_objects::v1`.
Its generated types and `response` validators are available with
`default-features = false`; services can validate persisted bucket metadata,
object metadata and timestamps without enabling a transport or JSON reflection.
The `grpc` and `http` features expose the corresponding v1 clients, also available
at the crate root as `GrpcObjects` and `HttpObjects`. These source APIs are not a
published v1 release. This breaking transition requires a new package version
before publication.

## Logical Objects clients

Objects v1 addresses the current object through a bucket name and object key.
It has no public object versions, snapshots, or forks. The transport-independent
`v1::ObjectsProvider` interface is implemented by the gRPC, HTTP, deterministic
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

For endpoints requiring mutual TLS, use
`GrpcObjects::connect_with_identity(endpoint, token, ca, certificate_pem, private_key_pem)`.
The certificate argument is a PEM client chain; the key is its PEM private key.
Both must be nonempty and at most 64 KiB. This preserves server verification,
bearer authentication, message bounds and deadlines. The server still owns client
identity authorization. Credentials are supplied in memory, with no SDK file or
infrastructure lookup.

Use conditional writes and idempotency keys for retryable mutations. Listings
are bounded, query-bound pages over current keys rather than captured snapshots.
Multipart uploads require an explicit completion manifest; staging and aborting
parts never publishes an object. The canonical v1 schema is
`proto/objects/v1/objects.proto`. `v1::conformance::verify` exercises all 13
operations against a disposable, immediately visible test fixture. Local tests
do not establish live service acceptance.

## Native v1 durability foundation

Enable `local` for `v1::local::LocalObjects`. Its private checksummed journal
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

The v1 provider inlines bodies up to 64 KiB in authenticated journal records,
groups native writes, and scans current state when constructing deltas. Native
filesystem roots now compose this provider through the logical v1 adapter.
Existing v1 roots fail closed; no old-data upgrade path is provided.

## Current contract

Root `wire`, `FILE_DESCRIPTOR_SET`, provider types and `LocalObjects` select the sole v1 contract.
Local capacity and synchronization options remain available; private segment storage is independent of the public wire contract.
