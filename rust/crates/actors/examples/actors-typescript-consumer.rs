#![allow(missing_docs, reason = "executable documentation scenario")]

use acyclic_actors::{validate_deployment, wire};

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let request = wire::DeploymentRequest {
        secrets: Vec::new(),
        fetch: None,
        worker_limits: None,
        name: "example".into(),
        account_id: Vec::new(),
        jurisdiction: String::new(),
        expected_revision: 0,
        code_sha256: vec![0x11; 32],

        bindings: vec![wire::Binding {
            state_transaction: false,
            target_account: Vec::new(),
            actor_namespace: None,
            worker_service: None,
            name: "database".into(),
            capability: "read".into(),
            resource: "tenant-db".into(),
        }],
        limits: Some(wire::ActorLimits {
            handler_timeout_millis: 1_000,
            memory_bytes: 1024,
            checkpoint_bytes: 1024,
        }),
        subscriptions: vec![wire::SubscriptionSpec {
            subscription_id: "events".into(),
            stream_path: "agents/a/events".into(),
            start: Some(wire::SubscriptionStart {
                start: Some(wire::subscription_start::Start::Cursor(0)),
            }),
            placement_anchor: true,
        }],
        idempotency_key: "deploy-typescript-consumer".into(),
    };
    validate_deployment(&request)?;
    let limits = request
        .limits
        .as_ref()
        .ok_or("validated request omitted limits")?;
    println!(
        "{}",
        serde_json::json!({
            "validated": true,
            "request": {
                "code_sha256": request.code_sha256.as_slice(),
                "name": request.name,
                "jurisdiction": request.jurisdiction,
                "account_id": request.account_id,
                "expected_revision": request.expected_revision.to_string(),
                "bindings": request.bindings.iter().map(|binding| serde_json::json!({
                    "name": binding.name,
                    "capability": binding.capability,
                    "resource": binding.resource,
                })).collect::<Vec<_>>(),
                "limits": {
                    "handler_timeout_millis": limits.handler_timeout_millis,
                    "memory_bytes": limits.memory_bytes,
                    "checkpoint_bytes": limits.checkpoint_bytes,
                },
                "subscriptions": request.subscriptions.iter().map(|subscription| serde_json::json!({
                    "subscription_id": subscription.subscription_id,
                    "stream_path": subscription.stream_path,
                    "start": { "cursor": 0 },
                    "placement_anchor": subscription.placement_anchor,
                })).collect::<Vec<_>>(),
                "idempotency_key": request.idempotency_key,
            }
        })
    );
    Ok(())
}
