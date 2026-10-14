# Acyclic Rust SDK

Objects, Streams, Actors, Workers and fs have native Rust crates. Swarm currently exposes a public HTTP/SSE contract without a dedicated Rust SDK crate; its example uses that actual HTTP endpoint with reqwest.

Each fenced example is a complete program. To run one in this checkout, create `examples/sdk-services/examples/objects.rs` (or `streams.rs`, `actors.rs`, `workers.rs`, `fs.rs`, `swarm.rs`), paste the corresponding block, and run `cargo run -p sdk-service-examples --example objects` with the matching filename. [The example manifest](../examples/sdk-services/Cargo.toml) supplies the dependencies. For a separate application, use these SDK crate paths and the workspace dependency versions in your own manifest. Set `ACYCLIC_API_KEY`, the service's HTTPS `*_ENDPOINT` and its existing resource variable (`OBJECTS_BUCKET`, `STREAM_PATH`, `ACTOR_ID`, `JOB_ID` or `FS_WORKSPACE`). The manifest uses the repository's stable toolchain and workspace dependency versions.

From this checkout, `cargo test -p sdk-service-examples --doc --locked` compiles all six exact examples without contacting a deployment. On shared lane machines, run that cargo command through build-slot in a Linux source checkout. [TypeScript guide](../typescript/packages/sdk/README.md) · [llms.txt](../llms.txt)

## Objects

Logical buckets and objects, with conditional writes and multipart uploads. This example reads an existing bucket through the hosted HTTP provider.

```rust,no_run
use acyclic_objects::{HttpObjects, ObjectsProvider, wire};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let objects = HttpObjects::new(
        &std::env::var("OBJECTS_ENDPOINT")?,
        &std::env::var("ACYCLIC_API_KEY")?,
        1024 * 1024,
    )?;
    let bucket = objects.head_bucket(wire::HeadBucketRequest {
        bucket: Some(wire::BucketRef { name: std::env::var("OBJECTS_BUCKET")? }),
    }).await?;
    println!("{bucket:?}");
    Ok(())
}
```

## Streams

Append-only record streams with forks and atomic commits. This example reads the tail position of an existing stream through the hosted HTTP provider.

```rust,no_run
use acyclic_stream::{StreamClient, http::HttpStream};
use std::sync::Arc;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let provider = HttpStream::new(
        &std::env::var("STREAM_ENDPOINT")?,
        &std::env::var("ACYCLIC_API_KEY")?,
        1024 * 1024,
    )?;
    let client = StreamClient::new(Arc::new(provider));
    println!("{}", client.stream(std::env::var("STREAM_PATH")?)?.tail().await?);
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
        actor_id: std::env::var("ACTOR_ID")?,
    }).await?;
    println!("{actor:?}");
    Ok(())
}
```

## Workers

Cloud Workers are stateless Actors. This example publishes `worker.mjs` (a module exporting `default.fetch`, without an actor export), creates a deployment, invokes it and deletes it. The account must already own the `actors-code` Objects bucket.

```rust,no_run
use acyclic_actors::{http, wire};
use acyclic_objects::HttpObjects;
use acyclic_workers::deployment::Client;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let endpoint = std::env::var("EDGE_ENDPOINT")?;
    let token = std::env::var("ACYCLIC_API_KEY")?;
    let workers = Client::new(http::Client::new(&endpoint, &token, 1024 * 1024)?,
        HttpObjects::new(&endpoint, &token, 1024 * 1024)?, "actors-code".into());
    let digest = workers.publish(std::fs::read("worker.mjs")?.into(), "publish-worker".into()).await?;
    let actor = workers.create(&wire::CreateActorRequest {
        code_sha256: digest.to_vec(), home_region: "la".into(),
        limits: Some(wire::ActorLimits { handler_timeout_millis: 30000, memory_bytes: 268435456, checkpoint_bytes: 1048576 }),
        idempotency_key: "create-worker".into(), ..Default::default()
    }).await?.actor.ok_or("create returned no deployment")?;
    println!("{:?}", workers.invoke(&wire::InvokeActorRequest {
        actor_id: actor.actor_id.clone(), method: "GET".into(), url: "/".into(), ..Default::default()
    }).await?);
    workers.delete(&wire::DeleteActorRequest { actor_id: actor.actor_id, idempotency_key: "delete-worker".into() }).await?;
    Ok(())
}
```

## fs

Versioned filesystem workspaces and generations. This example opens an existing workspace through the hosted filesystem provider.

```rust,no_run
use acyclic_fs::{Fs, HostedFsOptions};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let filesystem = Fs::hosted(HostedFsOptions::new(
        std::env::var("FS_ENDPOINT")?,
        std::env::var("ACYCLIC_API_KEY")?,
    )).await?;
    let workspace = filesystem.open_workspace(&std::env::var("FS_WORKSPACE")?).await?;
    println!("{}: {:?}", workspace.name(), workspace.head().await?.id());
    Ok(())
}
```

## Swarm

List visible swarms with the canonical `GET /v1/swarms` route.

```rust,no_run
#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let endpoint = reqwest::Url::parse(&std::env::var("SWARM_ENDPOINT")?)?;
    if endpoint.scheme() != "https" {
        return Err("SWARM_ENDPOINT must use HTTPS".into());
    }
    let client = reqwest::Client::builder()
        .redirect(reqwest::redirect::Policy::none())
        .timeout(std::time::Duration::from_secs(30))
        .build()?;
    let swarms = client.get(endpoint.join("/v1/swarms")?)
        .bearer_auth(std::env::var("ACYCLIC_API_KEY")?)
        .send().await?.error_for_status()?
        .json::<serde_json::Value>().await?;
    println!("{swarms}");
    Ok(())
}
```
