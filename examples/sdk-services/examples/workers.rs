//! Publishes, invokes and retires a deployment through Workers and Objects.

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
