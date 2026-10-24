# acyclic-workers

Workers are named script deployments, alongside actor namespace deployments. This example publishes `worker.mjs` (a module exporting `default.fetch`, without an actor export), deploys a named script, invokes it and deletes it. The account must already own the `actors-code` Objects bucket.

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
        HttpObjects::new(&endpoint, &token, 1024 * 1024, None)?, "actors-code".into());
    let digest = workers.publish(std::fs::read("worker.mjs")?.into(), "publish-worker".into()).await?;
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

Golden fixture provenance is recorded in `rust/crates/workers/tests/fixtures/cloud-workers/golden.json`. Local tests verify serialization and client composition; live deployment qualification belongs to the coordinator.
