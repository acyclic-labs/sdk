# Machines RSA mTLS fixture acceptance

This acceptance probe binds an installed TypeScript Machines package to the
Rust-owned RSA/mTLS fixture. It exercises each generated Machines RPC through
the installed gRPC client, including the server-streaming operation watch.

Build and run the source-owned fixture from the SDK checkout:

```powershell
$env:ACYCLIC_FIXTURE_SECONDS = "60"
cargo run --locked --offline --manifest-path rust/crates/sdk-examples/Cargo.toml --bin machines-rsa-fixture
```

Save the single JSON line emitted by the fixture to a temporary file while the
process is live, then run the package test from the Machines package:

```powershell
$env:ACYCLIC_MACHINES_RSA_FIXTURE_FILE = "C:\path\to\fixture-connection.json"
bun test test/rsa-fixture.test.ts
```

The fixture generates a fresh RSA-2048 CA, a `localhost` server certificate,
and a client certificate with the correct server/client extended key usages.
The client test supplies the CA and client identity to Connect's TLS transport;
it does not disable certificate or hostname verification. Credentials are
ephemeral and local to the bounded process.

The test calls these 19 modeled RPCs:

`QualifyImage`, `Create`, `Checkpoint`, `Fork`, `ForkMachine`, `Suspend`,
`Wake`, `SetSuspensionPolicy`, `DestroyMachine`, `DestroyCheckpoint`,
`Recover`, `InspectMachine`, `InspectCheckpoint`, `ListMachines`, `Events`,
`Usage`, `Cancel`, `InspectOperation`, and `WatchOperation`.

Source binding:

- Rust fixture helper: `rust/crates/sdk-examples/src/tls_fixture.rs`
- Rust fixture binary: `rust/crates/sdk-examples/src/bin/machines-rsa-fixture.rs`
- TypeScript probe: `typescript/packages/machines/test/rsa-fixture.test.ts`
- Generated operation source: `typescript/packages/machines/src/generated-client.ts`

The fixture is loopback-only qualification evidence. It makes no hosted
endpoint, registry publication, or service-availability claim.
