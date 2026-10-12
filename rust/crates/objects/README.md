# acyclic-objects

## Unreleased Objects v1 transition

The unmerged source exposes logical bucket/key contracts at the crate root and
under `acyclic_objects::v1`.

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
    None,
).await?;
# let _ = client;
# Ok(())
# }
```

For endpoints requiring mutual TLS, use
`GrpcObjects::connect(endpoint, token, ca, Some((certificate_pem, private_key_pem)))`.
## Native v1 durability foundation

Enable `local` for `v1::local::LocalObjects`. Its private checksummed journal
records logical state changes and exact retry receipts; immutable segments hold
current bodies and staged multipart parts. Reopen validates live physical bodies,
repairs only an incomplete final record, and preserves current keys and retry
outcomes. An uncertain append makes subsequent reads and mutations unavailable
until the owner closes and reopens the store.
