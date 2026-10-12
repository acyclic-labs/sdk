//! Actors contract and admission.
use crate::wire;
use std::collections::HashSet;

/// Maximum subscriptions on one Actor contract.
pub const MAX_SUBSCRIPTIONS: usize = 64;
/// Maximum named bindings on one Actor contract.
pub const MAX_BINDINGS: usize = 64;

fn valid_idempotency_key(value: &str) -> bool {
    !value.is_empty() && value.len() <= 256
}

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

impl ContractError {
    /// Returns the stable error code shared by every SDK bridge.
    pub const fn code_name(self) -> &'static str {
        match self {
            Self::InvalidArgument => "invalid_argument",
            Self::LimitExceeded => "limit_exceeded",
            Self::DuplicateName => "duplicate_name",
        }
    }
}

impl From<std::convert::Infallible> for ContractError {
    fn from(value: std::convert::Infallible) -> Self {
        match value {}
    }
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
        || !valid_idempotency_key(&request.idempotency_key)
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
pub fn validate_update(request: &wire::UpdateActorRequest) -> Result<(), ContractError> {
    if request.actor_id.is_empty()
        || !domain::valid_code_sha256(&request.code_sha256)
        || !valid_idempotency_key(&request.idempotency_key)
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

/// Validates a terminal Actor deletion. Identity and idempotency rules match
pub fn validate_delete(request: &wire::DeleteActorRequest) -> Result<(), ContractError> {
    if request.actor_id.is_empty() || !valid_idempotency_key(&request.idempotency_key) {
        return Err(ContractError::InvalidArgument);
    }
    Ok(())
}

/// Validates a newly authored subscription; its cursor may not later be rewound.
pub fn validate_add_subscription(
    request: &wire::AddSubscriptionRequest,
) -> Result<(), ContractError> {
    if request.actor_id.is_empty()
        || !valid_idempotency_key(&request.idempotency_key)
        || !request.subscription.as_ref().is_some_and(subscription)
    {
        return Err(ContractError::InvalidArgument);
    }
    Ok(())
}

mod generated {
    #![allow(
        missing_docs,
        reason = "Contract derivation emits the public schema declarations and documentation belongs to the semantic Rust source"
    )]

    #[allow(
        dead_code,
        reason = "Public semantic constructors and accessors are consumed by SDK bridges"
    )]
    #[allow(
        unexpected_cfgs,
        reason = "Kani supplies the cfg predicate during formal verification"
    )]
    #[allow(
        clippy::needless_pass_by_value,
        reason = "Contract derivation conversion signatures preserve owned semantic values"
    )]
    #[allow(
        clippy::redundant_closure,
        reason = "Contract derivation conversion attributes require closure-shaped validators"
    )]
    #[allow(
        clippy::unnecessary_fallible_conversions,
        reason = "Contract derivation conversion attributes preserve fallible ingress boundaries"
    )]
    pub mod domain {
        include!("domain/mod.rs");
    }
    pub use domain::*;

    include!("contract_definitions.rs");
}

pub(crate) use generated::ActorsFile;
#[allow(
    unused_imports,
    reason = "The semantic domain module is a public bridge namespace for SDK consumers"
)]
pub use generated::domain;

/// Renders the canonical Actors protobuf input for the maintained prost/tonic
pub fn render_proto_files(root: impl AsRef<std::path::Path>) -> std::io::Result<()> {
    ActorsFile::render(root.as_ref())
}

#[allow(
    unused_imports,
    reason = "These generated Proto aliases are consumed by the native and WASM bridges"
)]
pub use generated::{
    ActorLimitsProto, ActorObservationProto, ActorState, AddSubscriptionRequestProto,
    AddSubscriptionResponseProto, BindingProto, CheckpointActorRequestProto,
    CheckpointActorResponseProto, CreateActorRequestProto, CreateActorResponseProto,
    DeleteActorRequestProto, DeleteActorResponseProto, ErrorCode, HeaderProto,
    InspectActorRequestProto, InspectActorResponseProto, InvokeActorRequestProto,
    InvokeActorResponseProto, RemoveSubscriptionRequestProto, RemoveSubscriptionResponseProto,
    ResumeSubscriptionRequestProto, ResumeSubscriptionResponseProto, ServiceErrorProto,
    SubscriptionObservationProto, SubscriptionSpecProto, SubscriptionStartProto, SubscriptionState,
    UpdateActorRequestProto, UpdateActorResponseProto, subscription_start,
};

/// Actors service operations generated from the protobuf contract.
#[allow(
    unused_imports,
    reason = "The generated service descriptor is consumed by transport adapters"
)]
pub use generated::ActorsService;

#[cfg(test)]
mod tests {
    use super::render_proto_files;
    use std::{
        fs, io,
        path::{Path, PathBuf},
    };

    fn snapshot(root: &Path, directory: &Path) -> io::Result<Vec<(PathBuf, Vec<u8>)>> {
        let mut entries = Vec::new();
        for entry in fs::read_dir(directory)? {
            let entry = entry?;
            let path = entry.path();
            if path.is_dir() {
                entries.extend(snapshot(root, &path)?);
            } else {
                entries.push((
                    path.strip_prefix(root)
                        .map_err(io::Error::other)?
                        .to_owned(),
                    fs::read(path)?,
                ));
            }
        }
        entries.sort_by(|left, right| left.0.cmp(&right.0));
        Ok(entries)
    }

    #[test]
    fn render_proto_files_is_byte_deterministic() {
        let root = std::env::temp_dir().join(format!(
            "acyclic-actors-contract-determinism-{}",
            std::process::id()
        ));
        let first = root.join("first");
        let second = root.join("second");
        let _ = fs::remove_dir_all(&root);

        render_proto_files(&first).expect("first contract render");
        render_proto_files(&second).expect("second contract render");

        assert_eq!(
            snapshot(&first, &first).expect("first generated snapshot"),
            snapshot(&second, &second).expect("second generated snapshot")
        );
        fs::remove_dir_all(root).expect("remove deterministic-render fixture");
    }
}
