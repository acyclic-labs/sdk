//! Actors contract and admission.
use crate::wire;
use std::collections::HashSet;

/// Validate the limit alternative for the selected Actors deployment operation.
pub fn validate_deployment_operation(
    operation: &str,
    deployment: &wire::DeploymentRequest,
) -> Result<(), &'static str> {
    // RPC descriptors use UpperCamelCase and HTTP routes use lowerCamelCase.
    if operation.eq_ignore_ascii_case("DeployNamespace") {
        if deployment.limits.is_none() || deployment.worker_limits.is_some() {
            return Err("namespace deploy requires actor limits");
        }
    } else if operation.eq_ignore_ascii_case("DeployWorker")
        && (deployment.worker_limits.is_none()
            || deployment.limits.is_some()
            || !deployment.subscriptions.is_empty()
            || !deployment.jurisdiction.is_empty())
    {
        return Err("Worker deploy cannot carry actor subscriptions or jurisdiction");
    }
    Ok(())
}

pub(super) fn validate_idempotency_key(value: &str) -> Result<(), ContractError> {
    (!value.is_empty() && value.len() <= 256)
        .then_some(())
        .ok_or(ContractError::InvalidArgument)
}

fn valid_actor_scope(namespace: Option<&str>, id: &str) -> bool {
    crate::addressing::NamespaceId::validate(
        namespace
            .unwrap_or(crate::addressing::DEFAULT_ACTOR_NAMESPACE)
            .as_bytes(),
    )
    .is_ok()
        && crate::addressing::validate_actor_id(id.as_bytes()).is_ok()
}

/// Invalid customer-authored Actors request.
#[derive(Clone, Copy, Debug, Eq, PartialEq, thiserror::Error)]
pub enum ContractError {
    /// A required field is absent or malformed.
    #[error("required field is absent or malformed")]
    InvalidArgument,
    /// A subscription or binding name occurs more than once.
    #[error("duplicate subscription or binding name")]
    DuplicateName,
}

impl ContractError {
    /// Returns the stable error code shared by every SDK bridge.
    pub const fn code_name(self) -> &'static str {
        match self {
            Self::InvalidArgument => "invalid_argument",
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
        && value
            .start
            .as_ref()
            .is_some_and(|start| domain::SubscriptionStart::try_from(start.clone()).is_ok())
}

pub(crate) fn declared_subscription_id(id: &str) -> bool {
    !id.is_empty() && id != crate::invocation::HOST_INBOX
}

/// Validate an actor manifest's initial bindings, including the host-reserved
/// invocation ID, unique declared IDs and at most one placement anchor.
pub fn validate_subscription_manifest(
    subscriptions: &[wire::SubscriptionSpec],
) -> Result<(), ContractError> {
    let mut names = HashSet::new();
    let mut anchors = 0;
    for item in subscriptions {
        if !declared_subscription_id(&item.subscription_id) || !subscription(item) {
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
    Ok(())
}

fn valid_entry_source(source: Option<&wire::EntrySource>, name: &str) -> bool {
    source.is_none_or(|source| {
        source.entry == name
            && source.digest.len() == 32
            && source.length > 0
            && domain::EntrySource::validate_selection(&source.environment, &source.entry).is_ok()
    })
}

pub(super) fn valid_jurisdiction(jurisdiction: &str, worker: bool) -> bool {
    if worker {
        jurisdiction.is_empty()
    } else {
        matches!(jurisdiction, "" | "eu" | "us" | "fedramp")
    }
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
        || validate_idempotency_key(&request.idempotency_key).is_err()
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
    validate_subscription_manifest(&request.subscriptions)?;
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
        || validate_idempotency_key(&request.idempotency_key).is_err()
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
        || validate_idempotency_key(&request.idempotency_key).is_err()
        || !request.subscription.as_ref().is_some_and(|spec| {
            declared_subscription_id(&spec.subscription_id) && subscription(spec)
        })
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

pub(super) fn validate_subscription_identity(
    subscription: &str,
    key: &str,
) -> Result<(), ContractError> {
    if subscription.is_empty() {
        return Err(ContractError::InvalidArgument);
    }
    validate_idempotency_key(key)
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

pub(super) fn validate_fork_identity<'a>(
    parent: &str,
    child: &str,
    key: &str,
    mut subscriptions: impl Iterator<Item = &'a str>,
) -> Result<(), ContractError> {
    if parent == child {
        return Err(ContractError::InvalidArgument);
    }
    validate_idempotency_key(key)?;
    let mut seen = HashSet::new();
    subscriptions
        .all(|id| seen.insert(id))
        .then_some(())
        .ok_or(ContractError::DuplicateName)
}

mod generated {
    #![allow(
        missing_docs,
        reason = "Contract derivation emits the public schema declarations and documentation belongs to the semantic Rust source"
    )]

    #[allow(
        clippy::needless_pass_by_value,
        reason = "Contract derivation conversion signatures preserve owned semantic values"
    )]
    pub mod domain {
        include!("domain/mod.rs");
    }
    pub use domain::*;

    include!("contract_definitions.rs");
}

pub(crate) use generated::ActorsFile;
pub use generated::domain;

/// Renders the canonical Actors protobuf input for the maintained prost/tonic
pub fn render_proto_files(root: impl AsRef<std::path::Path>) -> std::io::Result<()> {
    ActorsFile::render(root.as_ref())
}

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
pub use generated::ActorsService;

#[cfg(test)]
mod tests;
