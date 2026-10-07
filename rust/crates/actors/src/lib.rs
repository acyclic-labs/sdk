#![doc = include_str!("../README.md")]

use std::collections::HashSet;

/// Rust-owned semantic projections for generated SDK metadata.
pub mod domain;

/// Authenticated operations over the platform's Rust-owned transport.
pub mod client;

/// Rust-owned Actors contract declarations and schema renderer.
pub mod contract;

/// Native gRPC client for the canonical Actors v1 service.
///
/// The client applies bearer authentication and TLS configuration to every
/// generated RPC. Browser bindings use the generated TypeScript transport;
/// this adapter is intentionally unavailable on `wasm32`.
#[cfg(not(target_arch = "wasm32"))]
pub mod grpc;
/// Native HTTP client for the canonical Actors v1 Protobuf JSON service.
///
/// The client exposes the same eight operations with bounded responses and
/// bearer authentication. Browser bindings use the generated TypeScript
/// transport; this adapter is intentionally unavailable on `wasm32`.
#[cfg(not(target_arch = "wasm32"))]
pub mod http;

/// Rust-owned Actors v1 wire types and schema metadata.
pub mod wire;

/// Canonical version-one descriptor set.
pub const FILE_DESCRIPTOR_SET: &[u8] =
    include_bytes!(concat!(env!("OUT_DIR"), "/acyclic-actors-v1.bin"));
/// Maximum subscriptions on one Actor contract.
pub const MAX_SUBSCRIPTIONS: usize = 64;
/// Maximum named bindings on one Actor contract.
pub const MAX_BINDINGS: usize = 64;

/// Rust-owned route names used by the TypeScript transport generator.
pub const HTTP_ROUTES: &[(&str, &str)] = &[
    ("createActor", "v1/actors/create"),
    ("updateActor", "v1/actors/update"),
    ("inspectActor", "v1/actors/inspect"),
    ("addSubscription", "v1/actors/subscriptions/add"),
    ("removeSubscription", "v1/actors/subscriptions/remove"),
    ("resumeSubscription", "v1/actors/subscriptions/resume"),
    ("checkpointActor", "v1/actors/checkpoint"),
    ("invokeActor", "v1/actors/invoke"),
];

/// Invalid customer-authored Actors request.
#[derive(Clone, Copy, Debug, Eq, PartialEq, thiserror::Error)]
pub enum ContractError {
    /// A required field is absent or malformed.
    #[error("required field is absent or malformed")]
    InvalidArgument,
    /// A collection exceeds its contract bound.
    #[error("Actors v1 collection limit exceeded")]
    LimitExceeded,
    /// A subscription or binding name occurs more than once.
    #[error("duplicate subscription or binding name")]
    DuplicateName,
}

fn subscription(value: &wire::SubscriptionSpec) -> bool {
    !value.subscription_id.is_empty()
        && !value.stream_path.is_empty()
        && matches!(
            value.start.as_ref().and_then(|start| start.start.as_ref()),
            Some(wire::subscription_start::Start::Cursor(_))
                | Some(wire::subscription_start::Start::CurrentHead(true))
        )
}

/// Validates a customer-authored Actor creation request before admission.
pub fn validate_create(request: &wire::CreateActorRequest) -> Result<(), ContractError> {
    if !domain::valid_code_sha256(&request.code_sha256)
        || request.home_region.is_empty()
        || request.idempotency_key.is_empty()
        || !request.limits.as_ref().is_some_and(|limits| {
            limits.handler_timeout_millis > 0
                && limits.memory_bytes > 0
                && limits.checkpoint_bytes > 0
        })
    {
        return Err(ContractError::InvalidArgument);
    }
    if request.subscriptions.len() > MAX_SUBSCRIPTIONS || request.bindings.len() > MAX_BINDINGS {
        return Err(ContractError::LimitExceeded);
    }
    let mut names = HashSet::new();
    let mut anchors = 0;
    for item in &request.subscriptions {
        if !subscription(item) {
            return Err(ContractError::InvalidArgument);
        }
        if !names.insert(&item.subscription_id) {
            return Err(ContractError::DuplicateName);
        }
        anchors += usize::from(item.placement_anchor);
    }
    if anchors > 1 {
        return Err(ContractError::InvalidArgument);
    }
    names.clear();
    for item in &request.bindings {
        if item.name.is_empty() || item.capability.is_empty() || item.resource.is_empty() {
            return Err(ContractError::InvalidArgument);
        }
        if !names.insert(&item.name) {
            return Err(ContractError::DuplicateName);
        }
    }
    Ok(())
}

/// Validates a full compare-and-replace Actor configuration mutation.
/// The service checks checkpoint compatibility or migration before activation.
pub fn validate_update(request: &wire::UpdateActorRequest) -> Result<(), ContractError> {
    if request.actor_id.is_empty()
        || !domain::valid_code_sha256(&request.code_sha256)
        || request.idempotency_key.is_empty()
        || !request.limits.as_ref().is_some_and(|limits| {
            limits.handler_timeout_millis > 0
                && limits.memory_bytes > 0
                && limits.checkpoint_bytes > 0
        })
    {
        return Err(ContractError::InvalidArgument);
    }
    if request.bindings.len() > MAX_BINDINGS {
        return Err(ContractError::LimitExceeded);
    }
    let mut names = HashSet::new();
    for item in &request.bindings {
        if item.name.is_empty() || item.capability.is_empty() || item.resource.is_empty() {
            return Err(ContractError::InvalidArgument);
        }
        if !names.insert(&item.name) {
            return Err(ContractError::DuplicateName);
        }
    }
    Ok(())
}

/// Validates a newly authored subscription; its cursor may not later be rewound.
pub fn validate_add_subscription(
    request: &wire::AddSubscriptionRequest,
) -> Result<(), ContractError> {
    if request.actor_id.is_empty()
        || request.idempotency_key.is_empty()
        || !request.subscription.as_ref().is_some_and(subscription)
    {
        return Err(ContractError::InvalidArgument);
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn subscription_start_is_exactly_one_choice() {
        let valid = wire::SubscriptionSpec {
            subscription_id: "events".into(),
            stream_path: "agents/a/events".into(),
            start: Some(wire::SubscriptionStart {
                start: Some(wire::subscription_start::Start::Cursor(0)),
            }),
            placement_anchor: true,
        };
        assert!(subscription(&valid));
        let mut invalid = valid;
        invalid.start = None;
        assert!(!subscription(&invalid));

        let mut create = wire::CreateActorRequest {
            code_sha256: vec![1; 32].into(),
            home_region: "eu".into(),
            bindings: vec![],
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
            idempotency_key: "create-a".into(),
        };
        assert_eq!(validate_create(&create), Ok(()));
        let duplicate = create.subscriptions.clone();
        create.subscriptions.extend(duplicate);
        assert_eq!(validate_create(&create), Err(ContractError::DuplicateName));
    }
}
