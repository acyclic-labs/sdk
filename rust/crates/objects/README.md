# acyclic-objects

## Objects v2

The crate root and `acyclic_objects::v2` expose logical bucket/key contracts,
generated types, response validators, and provider implementations. Providers
validate bucket metadata, object metadata, and timestamps before publishing
state, then expose the same `ObjectsProvider` operations through remote,
deterministic memory, and native local implementations. The crate root exports
`GrpcObjects`, `HttpObjects`, and `LocalObjects` alongside the provider-neutral
contracts.

## Logical Objects clients

Objects v2 addresses the current object through a bucket name and object key.
It has no public object versions, snapshots, or forks. The transport-independent
`v2::ObjectsProvider` interface is implemented by the gRPC, HTTP, deterministic
memory and native local providers. Reads and listings may lag mutations; single-object publication
and its conditions are atomic.

```sh
cargo add acyclic-objects --git https://github.com/acyclic-labs/sdk --rev 9ab26d5cbac047b516cb85fc1a5a2cebb2eda3ca
```

The authenticated remote clients use HTTPS and a caller-supplied bearer
credential. `HttpObjects` also accepts loopback HTTP for local development. For
a private-CA gRPC endpoint, pass the caller-supplied PEM certificate to
`GrpcObjects::connect`.

```rust,no_run
use acyclic_objects::GrpcObjects;

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
parts never publishes an object. The canonical v2 schema is
`proto/objects/v2/objects.proto`. `v2::conformance::verify` exercises all 13
operations against a disposable, immediately visible test fixture.

## Native v2 durability foundation

Use `v2::local::LocalObjects` for native local storage. Its private checksummed journal
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
and verifies that they have no active SDK exposure.
