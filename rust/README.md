# Acyclic Rust SDK

Objects, Streams, Actors, Workers and fs have native Rust crates. Swarm currently exposes a public HTTP/SSE contract without a dedicated Rust SDK crate; its example uses that actual HTTP endpoint with reqwest.

Each fenced example is a complete program. To run one in this checkout, create `examples/sdk-services/examples/objects.rs` (or `streams.rs`, `actors.rs`, `workers.rs`, `fs.rs`, `swarm.rs`), paste the corresponding block, and run `cargo run -p sdk-service-examples --example objects` with the matching filename. [The example manifest](../examples/sdk-services/Cargo.toml) supplies the dependencies. For a separate application, use these SDK crate paths and the workspace dependency versions in your own manifest. Set `ACYCLIC_API_KEY`, the service's HTTPS `*_ENDPOINT` and its existing resource variable (`OBJECTS_BUCKET`, `STREAM_PATH`, `ACTOR_ID`, `JOB_ID` or `FS_WORKSPACE`). The manifest uses the repository's stable toolchain and workspace dependency versions.

From this checkout, `cargo test -p sdk-service-examples --doc --locked` compiles all six exact examples without contacting a deployment. On Linux systems, run that cargo command through the shell in a Linux source checkout. [TypeScript guide](../typescript/packages/sdk/README.md) · [llms.txt](../llms.txt)

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

Workers are named script deployments, alongside actor namespace deployments.
The [Workers example](crates/workers/README.md) prepares one selected manifest
entry, publishes its complete immutable closure through the lifecycle client's
shared entry store and submits the resulting canonical Actors request. Each
deployment retains its own stable intent root until confirmed deletion. Modules
load lazily from the same account-scoped Objects provider.

```rust,no_run
use acyclic_actors::{http, wire};
use acyclic_objects::{HttpObjects, RootId};
use acyclic_workers::{
    deployment::Client,
    manifest::{self, PreparedEntry},
};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let endpoint = std::env::var("EDGE_ENDPOINT")?;
    let token = std::env::var("ACYCLIC_API_KEY")?;
    let workers = Client::new(
        http::Client::new(&endpoint, &token, 1024 * 1024)?,
        HttpObjects::new(&endpoint, &token, 1024 * 1024, None)?,
    );
    let prepared = PreparedEntry::new(
        manifest::parse(&std::fs::read("deployment.json")?)?,
        "production",
        "example-worker",
        std::fs::read("worker.mjs")?.into(),
        vec![(
            "worker_bg.wasm".into(),
            std::fs::read("worker_bg.wasm")?.into(),
        )],
    )?;
    let root = RootId::new(b"deployment:deploy-example-worker".to_vec())?;
    let published = workers
        .entries()
        .publish_deployment(&root, prepared, "deploy-example-worker")
        .await?;
    let request = published.request.into();
    let deployment = workers
        .deploy(&request)
        .await?
        .deployment
        .ok_or("deploy returned no deployment")?;
    println!(
        "{:?}",
        workers
            .invoke(&wire::InvokeWorkerRequest {
                worker_name: deployment.name.clone(),
                method: "GET".into(),
                url: "/".into(),
                ..Default::default()
            })
            .await?
    );
    workers
        .delete(&wire::DeleteWorkerRequest {
            worker_name: deployment.name,
            idempotency_key: "delete-worker".into(),
            ..Default::default()
        })
        .await?;
    workers.entries().release(&root).await?;
    Ok(())
}
```

## fs

Versioned workspaces and generations run in the client. Compose
`acyclic_fs::StreamAuthorityStore` with `acyclic_fs::LogicalObjectStore` using the
client's scoped Streams and Objects providers. Metadata publication uses Streams
commits; immutable bytes travel directly to Objects. The same library serves
embedded clients and agent tools without a filesystem service.

```rust,no_run
use acyclic_fs::model::{Lifecycle, VolumeConfig};
use acyclic_fs::{CancellationToken, Fs, LocalOptions, WorkBudget};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let root = std::env::var("FS_ROOT")?;
    let fs = Fs::local(LocalOptions::new(&root)).await?;
    let cancellation = CancellationToken::default();
    let _workspace = fs
        .create_volume(
            VolumeConfig::portable(Lifecycle::Durable),
            WorkBudget::UNBOUNDED,
            &cancellation,
        )
        .await?
        .value;
    println!("Created a durable workspace in {root}");
    Ok(())
}
```
