//! Actors contract and admission.
use crate::wire;
pub use acyclic_actors_guest_wire::admission::ContractError;
pub(super) use acyclic_actors_guest_wire::admission::{
    valid_idempotency_key, valid_jurisdiction, validate_fork_identity, validate_idempotency_key,
    validate_subscription_identity,
};
use std::collections::HashSet;

fn valid_actor_scope(namespace: Option<&str>, id: &str) -> bool {
    crate::addressing::NamespaceId::validate(
        namespace
            .unwrap_or(crate::addressing::DEFAULT_ACTOR_NAMESPACE)
            .as_bytes(),
    )
    .is_ok()
        && crate::addressing::validate_actor_id(id.as_bytes()).is_ok()
}

fn subscription(value: &wire::SubscriptionSpec) -> bool {
    !value.subscription_id.is_empty()
        && !value.stream_path.is_empty()
        && value
            .start
            .as_ref()
            .is_some_and(|start| domain::SubscriptionStart::try_from(start.clone()).is_ok())
}

fn valid_entry_source(source: Option<&wire::EntrySource>, name: &str) -> bool {
    source.is_none_or(|source| {
        source.entry == name
            && source.digest.len() == 32
            && source.length > 0
            && domain::EntrySource::validate_selection(&source.environment, &source.entry).is_ok()
    })
}

/// Validates an explicit namespace or named script deployment before admission.
pub fn validate_deployment(request: &wire::DeploymentRequest) -> Result<(), ContractError> {
    if !request.publication.is_empty() {
        let publication: crate::live_update::Publication =
            crate::live_update::decode(&request.publication)
                .map_err(|_| ContractError::InvalidArgument)?;
        if (!request.account_id.is_empty()
            && publication.deployment().account != request.account_id)
            || !publication
                .artifacts()
                .get(&request.name)
                .is_some_and(|artifact| artifact.code.as_slice() == request.code_sha256)
        {
            return Err(ContractError::InvalidArgument);
        }
    }
    if !valid_jurisdiction(&request.jurisdiction, request.worker_limits.is_some())
        || domain::CodeSha256::new(&request.code_sha256).is_err()
        || !valid_idempotency_key(&request.idempotency_key)
        || !valid_entry_source(request.entry_source.as_ref(), &request.name)
        || !match (&request.limits, &request.worker_limits) {
            (Some(limits), None) => {
                crate::addressing::NamespaceId::validate_deployment(request.name.as_bytes()).is_ok()
                    && domain::ActorLimits::try_from(limits.clone()).is_ok()
            }
            (None, Some(limits)) => {
                crate::addressing::validate_dns_label(request.name.as_bytes()).is_ok()
                    && domain::WorkerLimits::try_from(limits.clone()).is_ok()
                    && request.subscriptions.is_empty()
            }
            _ => false,
        }
    {
        return Err(ContractError::InvalidArgument);
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
    drop(names);
    validate_configuration(
        &request.bindings,
        &request.secrets,
        request.fetch.as_ref(),
        request.worker_limits.is_some(),
    )
}

/// Validates a full compare-and-replace Actor configuration mutation.
pub fn validate_update(request: &wire::UpdateNamespaceRequest) -> Result<(), ContractError> {
    if crate::addressing::NamespaceId::validate_deployment(request.namespace_id.as_bytes()).is_err()
        || domain::CodeSha256::new(&request.code_sha256).is_err()
        || !valid_idempotency_key(&request.idempotency_key)
        || !valid_entry_source(request.entry_source.as_ref(), &request.namespace_id)
        || !request
            .limits
            .as_ref()
            .is_some_and(|limits| domain::ActorLimits::try_from(limits.clone()).is_ok())
    {
        return Err(ContractError::InvalidArgument);
    }
    validate_configuration(
        &request.bindings,
        &request.secrets,
        request.fetch.as_ref(),
        false,
    )
}

fn validate_configuration(
    bindings: &[wire::Binding],
    secrets: &[wire::SecretSelection],
    fetch: Option<&wire::FetchBinding>,
    worker: bool,
) -> Result<(), ContractError> {
    let mut names = HashSet::with_capacity(bindings.len() + secrets.len());
    for binding in bindings {
        if domain::Binding::try_from(binding.clone()).is_err()
            || (worker && binding.state_transaction)
        {
            return Err(ContractError::InvalidArgument);
        }
        if !names.insert(binding.name.as_str()) {
            return Err(ContractError::DuplicateName);
        }
    }
    for secret in secrets {
        domain::SecretSelection::try_from(secret.clone())
            .map_err(|_| ContractError::InvalidArgument)?;
        if !names.insert(secret.name.as_str()) {
            return Err(ContractError::DuplicateName);
        }
    }
    if let Some(fetch) = fetch {
        domain::FetchBinding::try_from(fetch.clone())
            .map_err(|_| ContractError::InvalidArgument)?;
        if names.contains(fetch.name.as_str()) {
            return Err(ContractError::DuplicateName);
        }
    }
    Ok(())
}

/// Validates a Actor storage deletion, leaving the address reusable. Identity and idempotency rules match
pub fn validate_delete(request: &wire::DeleteAllRequest) -> Result<(), ContractError> {
    if !valid_actor_scope(request.namespace.as_deref(), &request.actor_id) {
        return Err(ContractError::InvalidArgument);
    }
    validate_idempotency_key(&request.idempotency_key)
}

/// Validates a newly authored subscription; its cursor may not later be rewound.
pub fn validate_add_subscription(
    request: &wire::AddSubscriptionRequest,
) -> Result<(), ContractError> {
    if !valid_actor_scope(request.namespace.as_deref(), &request.actor_id)
        || !valid_idempotency_key(&request.idempotency_key)
        || !request.subscription.as_ref().is_some_and(subscription)
    {
        return Err(ContractError::InvalidArgument);
    }
    Ok(())
}

/// Validates subscription removal with the same scope and retry identity rules as addition.
pub fn validate_remove_subscription(
    request: &wire::RemoveSubscriptionRequest,
) -> Result<(), ContractError> {
    validate_subscription_control(
        request.namespace.as_deref(),
        &request.actor_id,
        &request.subscription_id,
        &request.idempotency_key,
    )
}

/// Validates resumption under the same scope and retry rules as removal.
pub fn validate_resume_subscription(
    request: &wire::ResumeSubscriptionRequest,
) -> Result<(), ContractError> {
    validate_subscription_control(
        request.namespace.as_deref(),
        &request.actor_id,
        &request.subscription_id,
        &request.idempotency_key,
    )
}

fn validate_subscription_control(
    namespace: Option<&str>,
    actor: &str,
    subscription: &str,
    identity: &str,
) -> Result<(), ContractError> {
    if !valid_actor_scope(namespace, actor) {
        return Err(ContractError::InvalidArgument);
    }
    validate_subscription_identity(subscription, identity)
}

/// Validates a fork: both Actors share one account and namespace, the child
/// differs from the parent, and each child subscription id occurs once.
pub fn validate_fork(request: &wire::ForkActorRequest) -> Result<(), ContractError> {
    let namespace = request.namespace.as_deref();
    if !valid_actor_scope(namespace, &request.actor_id)
        || crate::addressing::validate_actor_id(request.child_actor_id.as_bytes()).is_err()
        || !request.subscriptions.iter().all(subscription)
    {
        return Err(ContractError::InvalidArgument);
    }
    validate_fork_identity(
        &request.actor_id,
        &request.child_actor_id,
        &request.idempotency_key,
        request
            .subscriptions
            .iter()
            .map(|value| value.subscription_id.as_str()),
    )
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
    CheckpointActorResponseProto, DeleteAllRequestProto, DeleteAllResponseProto,
    DeleteWorkerRequestProto, DeleteWorkerResponseProto, DeploymentObservationProto,
    DeploymentRequestProto, DeploymentResponseProto, EgressRuleProto, EntrySourceProto, ErrorCode,
    FetchBindingProto, ForkActorRequestProto, ForkActorResponseProto, HeaderProto,
    InspectActorRequestProto, InspectActorResponseProto, InvocationOrderProto,
    InvokeActorRequestProto, InvokeActorResponseProto, InvokeWorkerRequestProto,
    RemoveSubscriptionRequestProto, RemoveSubscriptionResponseProto,
    ResumeSubscriptionRequestProto, ResumeSubscriptionResponseProto, SecretSelectionProto,
    ServiceErrorProto, SubscriptionObservationProto, SubscriptionSpecProto, SubscriptionStartProto,
    SubscriptionState, UpdateNamespaceRequestProto, UpdateNamespaceResponseProto,
    WorkerLimitsProto, subscription_start,
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
