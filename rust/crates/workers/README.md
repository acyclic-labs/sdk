# acyclic-workers

Workers are named script deployments, alongside actor namespace deployments.
The native lifecycle client exposes the shared `EntryStore` over its original
account-scoped Objects provider. Prepare the selected manifest entry and all its
declared modules, retain that closure under the deployment's stable intent root,
then submit the returned canonical Actors request.

This example selects `production/example-worker` from `deployment.json`, whose
artifact is `worker.mjs` and whose module assets include `worker_bg.wasm`.
`worker.mjs` exports `default.fetch`. No mutation is automatically retried.

```rust,no_run
use acyclic_actors::{http, wire};
use acyclic_objects::{HttpObjects, RootId};
use acyclic_workers::{deployment::Client, manifest::{self, PreparedEntry}};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let endpoint = std::env::var("EDGE_ENDPOINT")?;
    let token = std::env::var("ACYCLIC_API_KEY")?;
    let workers = Client::new(http::Client::new(&endpoint, &token, 1024 * 1024)?,
        HttpObjects::new(&endpoint, &token, 1024 * 1024, None)?);
    let prepared = PreparedEntry::new(
        manifest::parse(&std::fs::read("deployment.json")?)?,
        "production", "example-worker", std::fs::read("worker.mjs")?.into(),
        vec![("worker_bg.wasm".into(), std::fs::read("worker_bg.wasm")?.into())],
    )?;
    let root = RootId::new(b"deployment:deploy-example-worker".to_vec())?;
    let published = workers.entries()
        .publish_deployment(&root, prepared, "deploy-example-worker").await?;
    let request = published.request.into();
    let deployment = workers.deploy(&request).await?
        .deployment.ok_or("deploy returned no deployment")?;
    println!("{:?}", workers.invoke(&wire::InvokeWorkerRequest {
        worker_name: deployment.name.clone(), method: "GET".into(), url: "/".into(), ..Default::default()
    }).await?);
    workers.delete(&wire::DeleteWorkerRequest { worker_name: deployment.name, idempotency_key: "delete-worker".into(), ..Default::default() }).await?;
    workers.entries().release(&root).await?;
    Ok(())
}
```

Keep the same intent root and request when reconciling a lost deployment
acknowledgement. Release that root only after confirmed deletion; another
deployment retaining the same bytes has its own root. The shared real Objects
conformance case covers publication, lazy loading, independent retention and
typed provider refusals.

`manifest::PreparedEntry` admits one environment and entry, its code and its
declared sibling modules. It moves the selected configuration and owned payload
bytes into a versioned immutable descriptor. Unselected entries, bindings,
grants and secret references are excluded from the attachment identity.

`entry::EntryStore` uses an existing account-scoped Objects provider to publish
the sealed payload batch, publish the descriptor with its exact outgoing edges,
and retain the closure under the caller's stable deployment root. Two deployments
with identical content use separate roots. Repeating publication with the same
root and content reconciles the same immutable object; release only after that
deployment no longer names the attachment. Retention supplies durability through
the original Objects contract.

Loading reads one descriptor and checks its selected names and retention edges.
Code and declared modules load lazily through that same provider and verify the
original manifest digest and size. Each requested module uses one Objects read;
unknown module names fail before I/O. The original Objects results, revisions
and byte custody remain intact. No builder directory is needed. Objects identity
does not convey account authority.

`publish_deployment(root, prepared, idempotency_key)` lowers the same admitted
selection and returns its canonical Actors request only after the closure is
retained. The request's `entrySource` contains the exact Objects digest, byte
length, environment and entry. It is separate from the original account and
resource authority. Invalid configuration or intent fails before publication.

Actors deployment/update requests and deployment/actor observations carry this
same selection. A service must persist it with the accepted configuration and
return it unchanged when reconciling an intent or restoring an actor.
`load_deployment` and `load_actor` consume those original observations through
the existing Objects loader, verify the selected entry and accepted code digest,
and then load requested modules lazily. A missing or mismatched selection fails
closed. These contracts do not install a deployment service or execution host.
