# acyclic-objects

The Objects contract, typed provider interface, and first-party gRPC, memory, and optional local providers.

## Current-value contract

Hosted customer code should use the re-exported `replaceable` API. An
`ObjectId` is a stable bucket identity plus opaque object key. `replace_put`
replaces the current bytes at that identity and returns a `PutReceipt` with the
new `ETag` and size. `get_current` and `verify_current` select the current
visible value; no customer-visible version identity is required or exposed by
these operations. Checksums validate bytes and never choose object identity.

Reads may be eventually visible after a successful replacement. Use the
returned `ETag` with `CurrentGetRequest.if_none_match` when waiting for the new
value to become visible. Every transport failure during a replacement is
ambiguous; retry with the exact same idempotency key. Reusing a key with a
different body, condition, or object is rejected as an idempotency mismatch.

The older permanently-versioned provider and wire types remain in this source
release so FS and hosted migration code can be upgraded without a flag day.
They are compatibility internals for the next prerelease and are not the
customer-facing current-value contract.

```sh
cargo add acyclic-objects
```

The default `grpc` feature exposes the authenticated remote client. Enable `local` for the durable embedded provider. The memory provider is for deterministic local tests, not persistence. For a private-CA HTTPS endpoint, use `Client::connect_with_ca_certificate` and pass the caller-supplied PEM certificate; do not disable TLS verification.

```rust,no_run
# #[cfg(feature = "grpc")]
use acyclic_objects::Client;

# #[cfg(feature = "grpc")]
# async fn example() -> Result<(), Box<dyn std::error::Error>> {
let ca_pem = std::fs::read("trusted-ca.pem")?;
let client = Client::connect_with_ca_certificate(
    "https://objects.example", "account-token", ca_pem,
).await?;
# let _ = client;
# Ok(())
# }
```

Use conditional writes and idempotency keys for retryable mutations. Listings are paginated and bound to a captured view; multipart uploads require an explicit completion manifest. The [crate API](https://docs.rs/acyclic-objects/latest/acyclic_objects/) exposes the transport-independent provider surface, and the [v1 protocol](https://github.com/acyclic-labs/sdk/tree/main/proto/objects) defines compatibility semantics.

The local provider commits bodies up to 64 KiB inside the journal record itself, so one append and one flush make a small object durable; larger bodies are published as immutable segments first. Once inline bytes outweigh the rest of the journal, and on every garbage collection, the provider compacts the journal: live inline bodies move into segments and the rest are dropped.

The local provider fails closed after an uncertain journal write: reads, mutations, and garbage collection return unavailable until the owner closes and reopens the store. Reopen repairs only an incomplete final frame; corruption or a host read error fails recovery. Retrying a mutation after an unavailable result should use its original idempotency key because the last outcome may be uncertain.
