#![doc = include_str!("../README.md")]
#![doc = include_str!("../docs/guide.md")]

use std::collections::HashSet;

/// Generated independent negotiation bindings for native transports.
#[cfg(not(target_arch = "wasm32"))]
pub mod control_wire {
    #![allow(
        missing_docs,
        clippy::all,
        clippy::pedantic,
        reason = "generated control bindings"
    )]
    pub mod protocol {
        /// Generated protocol identity and capability handshake messages.
        pub mod v1 {
            include!("generated/acyclic.protocol.v1.rs");
        }
    }
    /// Generated transport service used for capability handshakes.
    pub mod transport {
        /// Version-one protocol handshake RPC bindings.
        pub mod v1 {
            include!("generated/acyclic.transport.v1.rs");
        }
    }
}

/// Platform-aware Actors client that selects and verifies the best transport.
pub mod client;
#[cfg(not(target_arch = "wasm32"))]
/// Direct authenticated gRPC transport for the Actors v1 service.
pub mod grpc;
/// Direct authenticated HTTP transport for the Actors v1 service.
pub mod http;

/// The platform-aware client and its connection helpers.
pub use client::{
    Client, ConnectError, DEFAULT_HTTP_RESPONSE_BYTES, DEFAULT_TRANSPORT, Error, connect,
    connect_with_ca_certificate,
};

/// Generated Actors v1 wire types. The documented schema is `proto/actors/v1/actors.proto`.
pub mod wire {
    #![allow(missing_docs, reason = "generated from the public Actors schema")]
    #![allow(clippy::all, clippy::pedantic, reason = "generated protobuf bindings")]
    include!("generated/acyclic.actors.v1.rs");
}

/// File descriptor set for the immutable Actors v1 wire contract.
pub const FILE_DESCRIPTOR_SET: &[u8] = include_bytes!("generated/acyclic-actors-v1.bin");
/// Maximum number of subscription specifications accepted by one Actor contract.
pub const MAX_SUBSCRIPTIONS: usize = 64;
/// Maximum number of named binding specifications accepted by one Actor contract.
pub const MAX_BINDINGS: usize = 64;

/// Stable operation names and relative HTTP paths for the Actors v1 fallback transport.
///
/// Each tuple contains the operation identifier used by generated clients and the
/// path appended to a configured service endpoint.
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
    /// A required field, digest, limit, or subscription start is absent or malformed.
    #[error("required field is absent or malformed")]
    InvalidArgument,
    /// The subscription or binding collection exceeds its contract bound.
    #[error("Actors v1 collection limit exceeded")]
    LimitExceeded,
    /// Two subscriptions or two bindings use the same name in one request.
    #[error("duplicate subscription or binding name")]
    DuplicateName,
}

fn digest(value: &[u8]) -> bool {
    value.len() == 32 && value.iter().any(|byte| *byte != 0)
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

/// Validates an Actor creation request against the v1 admission invariants.
///
/// The request needs a nonzero 32-byte code digest, home region, idempotency
/// key, and positive handler, memory, and checkpoint limits. Subscription and
/// binding counts are bounded by [`MAX_SUBSCRIPTIONS`] and [`MAX_BINDINGS`];
/// subscription identifiers and binding names are unique, and at most one
/// subscription may carry the placement anchor.
pub fn validate_create(request: &wire::CreateActorRequest) -> Result<(), ContractError> {
    if !digest(&request.code_sha256)
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

/// Validates the request fields for a full Actor configuration replacement.
///
/// The actor identifier, nonzero 32-byte code digest, idempotency key, and all
/// three positive resource limits are required. Binding names, capabilities,
/// and resources must be nonempty and unique, and the binding count is bounded
/// by [`MAX_BINDINGS`]. Checkpoint compatibility is evaluated by the service
/// when the replacement is activated.
pub fn validate_update(request: &wire::UpdateActorRequest) -> Result<(), ContractError> {
    if request.actor_id.is_empty()
        || !digest(&request.code_sha256)
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

/// Validates a subscription addition before it is attached to an Actor.
///
/// The actor identifier and idempotency key must be present. The subscription
/// must have a nonempty identifier and stream path and select exactly one
/// supported start mode: a cursor or the current head.
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
            code_sha256: vec![1; 32],
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
