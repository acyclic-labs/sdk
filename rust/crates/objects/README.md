# acyclic-objects

The versioned Objects contract, typed provider interface, and first-party gRPC, memory, and optional local providers. Object versions and whole-bucket snapshots are immutable identities; a bucket name is not a substitute for its `BucketRef`.

```sh
cargo add acyclic-objects
```

The default `grpc` feature exposes the authenticated remote client. Enable `local` for the durable embedded provider. The memory provider is for deterministic local tests, not persistence. For a private-CA HTTPS endpoint, use `Client::connect_with_ca_certificate` and pass the caller-supplied PEM certificate; do not disable TLS verification.

```rust,no_run
use acyclic_objects::Client;

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

The local provider fails closed after an uncertain journal write: reads, mutations, and garbage collection return unavailable until the owner closes and reopens the store. Reopen repairs only an incomplete final frame; corruption or a host read error fails recovery. Retrying a mutation after an unavailable result should use its original idempotency key because the last outcome may be uncertain.
