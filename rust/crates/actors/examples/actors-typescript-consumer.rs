//! Deterministic Rust-owned Actors request for generated TypeScript consumers.

use acyclic_actors::{
    domain::{ActorLimits, CodeSha256, CreateActorRequest},
    wire,
};

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let request = CreateActorRequest::new(
        CodeSha256::new(vec![0x11; 32])?,
        "eu".into(),
        vec![],
        ActorLimits::new(1_000, 1024, 1024)?,
        vec![],
        "create-typescript-consumer".into(),
    )?;
    let wire: wire::CreateActorRequest = request.into();
    let limits = wire
        .limits
        .as_ref()
        .ok_or("validated request omitted limits")?;
    println!(
        "{}",
        serde_json::json!({
            "validated": true,
            "request": {
                "code_sha256": wire.code_sha256.to_vec(),
                "home_region": wire.home_region,
                "bindings": wire.bindings.iter().map(|binding| serde_json::json!({
                    "name": binding.name,
                    "capability": binding.capability,
                    "resource": binding.resource,
                })).collect::<Vec<_>>(),
                "limits": {
                    "handler_timeout_millis": limits.handler_timeout_millis,
                    "memory_bytes": limits.memory_bytes,
                    "checkpoint_bytes": limits.checkpoint_bytes,
                },
                "subscriptions": wire.subscriptions.iter().map(|subscription| serde_json::json!({
                    "subscription_id": subscription.subscription_id,
                    "stream_path": subscription.stream_path,
                    "placement_anchor": subscription.placement_anchor,
                })).collect::<Vec<_>>(),
                "idempotency_key": wire.idempotency_key,
            },
        })
    );
    Ok(())
}
