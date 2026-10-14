# acyclic-workers

Cloud Workers are stateless Actors. This example publishes `worker.mjs` (a module exporting `default.fetch`, without an actor export), creates a deployment, invokes it and deletes it. The account must already own the `actors-code` Objects bucket.

Uses the canonical Objects and Actors contracts. Publication stores exact module bytes at `sha256/<digest>`. No mutation is automatically retried.

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

Golden fixture provenance is recorded in `rust/crates/workers/tests/fixtures/cloud-workers/golden.json`. Local tests verify serialization and client composition; live deployment qualification belongs to the coordinator.
