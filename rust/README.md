# Acyclic Rust SDK

Objects, Streams, Actors, Workers and fs have native Rust crates. Swarm currently exposes a public HTTP/SSE contract without a dedicated Rust SDK crate; its example uses that actual HTTP endpoint with reqwest.

Each fenced example is a complete program. To run one in this checkout, create `examples/sdk-services/examples/objects.rs` (or `streams.rs`, `actors.rs`, `workers.rs`, `fs.rs`, `swarm.rs`), paste the corresponding block, and run `cargo run -p sdk-service-examples --example objects` with the matching filename. [The example manifest](../examples/sdk-services/Cargo.toml) supplies the dependencies. For a separate application, use these SDK crate paths and the workspace dependency versions in your own manifest. Set `ACYCLIC_API_KEY`, the service's HTTPS `*_ENDPOINT` and its existing resource variable (`OBJECTS_BUCKET`, `STREAM_PATH`, `ACTOR_ID`, `JOB_ID` or `FS_WORKSPACE`). The manifest uses the repository's stable toolchain and workspace dependency versions.

From this checkout, `cargo test -p sdk-service-examples --doc --locked` compiles all six exact examples without contacting a deployment. On shared lane machines, run that cargo command through build-slot in a Linux source checkout. [TypeScript guide](../typescript/packages/sdk/README.md) · [llms.txt](../llms.txt)

## Objects

Account-scoped content-addressed blobs (BLAKE3-256 digests), with logical range reads, manifests and retained references. This example stores a blob through the hosted HTTP provider and reads it back.

```rust,no_run
use acyclic_objects::{Blobs, ByteRange, HttpObjects};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let objects = HttpObjects::new(
        &std::env::var("OBJECTS_ENDPOINT")?,
        &std::env::var("ACYCLIC_API_KEY")?,
        1024 * 1024, None,
    )?;
    let put = objects.put(b"hello".to_vec().into(), Vec::new()).await?;
    let digest = put.verified().ok_or("outcome unknown; retrying is safe")?.digest;
    let object = objects.get(digest, ByteRange::FULL, 1024).await?;
    println!("{digest}: {:?}", object.body());
    Ok(())
}
```

## Streams

Append-only record streams with forks and atomic commits. This example reads the tail position of an existing stream through the hosted HTTP provider.

```rust,no_run
use acyclic_stream::{Stream, http::HttpStream};
use std::sync::Arc;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let provider = HttpStream::new(
        &std::env::var("STREAM_ENDPOINT")?,
        &std::env::var("ACYCLIC_API_KEY")?,
        1024 * 1024,
    )?;
    let client = Stream::new(Arc::new(provider), std::env::var("STREAM_PATH")?)?;
    println!("{}", client.tail().await?);
    Ok(())
}
```

## Actors

Durable actor identities, invocation and subscriptions. This example inspects an existing actor using the Rust-backed client.

```rust,no_run
use acyclic_actors::{http::Client, wire};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let actors = Client::new(
        &std::env::var("ACTORS_ENDPOINT")?,
        &std::env::var("ACYCLIC_API_KEY")?,
        1024 * 1024,
    )?;
    let actor = actors.inspect_actor(&wire::InspectActorRequest {
        actor_id: std::env::var("ACTOR_ID")?, ..Default::default()
    }).await?;
    println!("{actor:?}");
    Ok(())
}
```

## Workers

Workers are named script deployments, alongside actor namespace deployments. This example publishes `worker.mjs` (a module exporting `default.fetch`, without an actor export), deploys a named script, invokes it and deletes it.

Uses the canonical Objects and Actors contracts. Publication stores exact module bytes as an Objects blob retained under the code's own root until unpublished. No mutation is automatically retried.

```rust,no_run
use acyclic_actors::{http, wire};
use acyclic_objects::HttpObjects;
use acyclic_workers::deployment::Client;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let endpoint = std::env::var("EDGE_ENDPOINT")?;
    let token = std::env::var("ACYCLIC_API_KEY")?;
    let workers = Client::new(http::Client::new(&endpoint, &token, 1024 * 1024)?,
        HttpObjects::new(&endpoint, &token, 1024 * 1024, None)?);
    let digest = workers.publish(std::fs::read("worker.mjs")?.into()).await?;
    let deployment = workers.deploy(&wire::DeploymentRequest {
        name: "example-worker".into(), code_sha256: digest.to_vec(),
        worker_limits: Some(wire::WorkerLimits { timeout_millis: 30000, memory_bytes: 268435456, cpu_millis: 1048576 }),
        idempotency_key: "deploy-worker".into(), ..Default::default()
    }).await?.deployment.ok_or("deploy returned no deployment")?;
    println!("{:?}", workers.invoke(&wire::InvokeWorkerRequest {
        worker_name: deployment.name.clone(), method: "GET".into(), url: "/".into(), ..Default::default()
    }).await?);
    workers.delete(&wire::DeleteWorkerRequest { worker_name: deployment.name, idempotency_key: "delete-worker".into(), ..Default::default() }).await?;
    Ok(())
}
```

## fs

Versioned filesystem workspaces and generations. This example opens an existing workspace through the hosted filesystem provider.

```rust,no_run
use acyclic_fs::{HostedFs, HostedFsOptions};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let filesystem = HostedFs::connect(HostedFsOptions::new(
        std::env::var("FS_ENDPOINT")?,
        std::env::var("ACYCLIC_API_KEY")?,
    )).await?;
    let workspace = filesystem.open_workspace(&std::env::var("FS_WORKSPACE")?).await?;
    println!("{}: {:?}", workspace.name(), workspace.head().await?.id());
    Ok(())
}
```
