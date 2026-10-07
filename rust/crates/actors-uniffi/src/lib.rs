//! Rust-owned UniFFI metadata for the Actors client.
//!
//! This facade exposes nominal semantic values, an explicit cancellation
//! handle, client connection, and all eight Actors operations. All request
//! construction, validation, transport selection, response conversion, and
//! cancellation remain in `acyclic-actors`.
pub mod cancellation_metadata;
pub mod kotlin_generation_metadata;
pub mod ruby_generation_metadata;


use std::sync::Arc;

use acyclic_actors::{client, domain};
use tokio_util::sync::CancellationToken;

uniffi::setup_scaffolding!();

/// Errors crossing the generated foreign-language boundary.
#[derive(Debug, thiserror::Error, uniffi::Error)]
pub enum BindingError {
    /// Endpoint, credential, or trust configuration was rejected by Rust.
    #[error("Actors configuration failed: {detail_message}")]
    Configuration { detail_message: String },
    /// The Rust-owned transport could not be constructed or reached.
    #[error("Actors transport failed: {detail_message}")]
    Transport { detail_message: String },
    /// The service rejected an operation.
    #[error("Actors service failed: {detail_message}")]
    Service {
        grpc_code: i32,
        service_code: Option<i32>,
        detail_message: String,
    },
    /// Rust's canonical request validator rejected an operation.
    #[error("Actors contract rejected the request: {detail_message}")]
    Contract { detail_message: String },
    /// Rust could not project a wire value into its semantic domain.
    #[error("Actors semantic conversion failed: {detail_message}")]
    Semantic { detail_message: String },
    /// The Rust-owned operation observed cancellation.
    #[error("Actors operation cancelled")]
    Cancelled,
}

fn domain_error(error: domain::DomainError) -> BindingError {
    match error {
        domain::DomainError::Contract(error) => BindingError::Contract {
            detail_message: error.to_string(),
        },
        error => BindingError::Semantic {
            detail_message: error.to_string(),
        },
    }
}

/// Validates an Actor identity through the Rust-owned semantic constructor.
/// Generated nominal foreign factories call this function before constructing
/// their local wrapper; the predicate is intentionally not duplicated there.
#[uniffi::export]
pub fn validate_actor_id(value: String) -> Result<(), BindingError> {
    domain::ActorId::new(value)
        .map(|_| ())
        .map_err(domain_error)
}

/// Validates a code digest through the Rust-owned semantic constructor.
#[uniffi::export]
pub fn validate_code_sha256(value: Vec<u8>) -> Result<(), BindingError> {
    domain::CodeSha256::new(value)
        .map(|_| ())
        .map_err(domain_error)
}

/// Validates a positive unsigned integer through the Rust-owned constructor.
#[uniffi::export]
pub fn validate_positive_u64(value: u64) -> Result<(), BindingError> {
    domain::PositiveU64::new(value)
        .map(|_| ())
        .map_err(domain_error)
}

fn client_error(error: client::Error) -> BindingError {
    match error {
        client::Error::Configuration(detail_message) => {
            BindingError::Configuration { detail_message }
        }
        client::Error::Transport(detail_message) => BindingError::Transport { detail_message },
        client::Error::Service { grpc_code, detail } => BindingError::Service {
            grpc_code,
            service_code: detail.as_ref().map(|value| value.code),
            detail_message: detail
                .map(|value| value.message)
                .unwrap_or_else(|| "service rejected operation".to_owned()),
        },
        client::Error::Contract(error) => BindingError::Contract {
            detail_message: error.to_string(),
        },
        client::Error::Semantic(error) => domain_error(error),
        client::Error::Cancelled => BindingError::Cancelled,
    }
}

/// A Rust-owned non-empty Actor identity.
#[derive(Debug, uniffi::Object)]
pub struct ActorId {
    inner: domain::ActorId,
}

#[uniffi::export]
impl ActorId {
    /// Constructs an Actor identity through the canonical Rust domain type.
    #[uniffi::constructor]
    pub fn new(value: String) -> Result<Arc<Self>, BindingError> {
        domain::ActorId::new(value)
            .map(|inner| Arc::new(Self { inner }))
            .map_err(domain_error)
    }

    /// Returns the exact wire spelling.
    pub fn value(&self) -> String {
        self.inner.as_str().to_owned()
    }
}

/// A Rust-owned non-zero 32-byte SHA-256 digest.
#[derive(Debug, uniffi::Object)]
pub struct CodeSha256 {
    inner: domain::CodeSha256,
}

#[uniffi::export]
impl CodeSha256 {
    /// Constructs a digest through the canonical Rust domain type.
    #[uniffi::constructor]
    pub fn new(value: Vec<u8>) -> Result<Arc<Self>, BindingError> {
        domain::CodeSha256::new(value)
            .map(|inner| Arc::new(Self { inner }))
            .map_err(domain_error)
    }

    /// Returns the exact digest bytes.
    pub fn value(&self) -> Vec<u8> {
        self.inner.as_bytes().to_vec()
    }
}

/// A Rust-owned strictly positive unsigned 64-bit value.
#[derive(Debug, uniffi::Object)]
pub struct PositiveU64 {
    inner: domain::PositiveU64,
}

#[uniffi::export]
impl PositiveU64 {
    /// Constructs a positive value through the canonical Rust domain type.
    #[uniffi::constructor]
    pub fn new(value: u64) -> Result<Arc<Self>, BindingError> {
        domain::PositiveU64::new(value)
            .map(|inner| Arc::new(Self { inner }))
            .map_err(domain_error)
    }

    /// Returns the exact unsigned value.
    pub fn value(&self) -> u64 {
        self.inner.get()
    }
}


/// Rust-owned resource binding. The opaque object contains the canonical
/// validated domain value; foreign callers cannot construct an invalid binding.
#[derive(Debug, uniffi::Object)]
pub struct Binding {
    inner: domain::Binding,
}

#[uniffi::export]
impl Binding {
    #[uniffi::constructor]
    pub fn new(
        name: String,
        capability: String,
        resource: String,
    ) -> Result<Arc<Self>, BindingError> {
        domain::Binding::new(name, capability, resource)
            .map(|inner| Arc::new(Self { inner }))
            .map_err(domain_error)
    }

    pub fn name(&self) -> String { self.inner.name().to_owned() }
    pub fn capability(&self) -> String { self.inner.capability().to_owned() }
    pub fn resource(&self) -> String { self.inner.resource().to_owned() }
}

/// Rust-owned positive Actor resource limits.
#[derive(Debug, uniffi::Object)]
pub struct ActorLimits {
    inner: domain::ActorLimits,
}

#[uniffi::export]
impl ActorLimits {
    #[uniffi::constructor]
    pub fn new(
        handler_timeout_millis: u64,
        memory_bytes: u64,
        checkpoint_bytes: u64,
    ) -> Result<Arc<Self>, BindingError> {
        domain::ActorLimits::new(handler_timeout_millis, memory_bytes, checkpoint_bytes)
            .map(|inner| Arc::new(Self { inner }))
            .map_err(domain_error)
    }

    pub fn handler_timeout_millis(&self) -> u64 { self.inner.handler_timeout_millis() }
    pub fn memory_bytes(&self) -> u64 { self.inner.memory_bytes() }
    pub fn checkpoint_bytes(&self) -> u64 { self.inner.checkpoint_bytes() }
}

/// Rust-owned subscription start selector, preserving cursor zero and
/// current-head presence semantics.
#[derive(Clone, Debug, uniffi::Enum)]
pub enum SubscriptionStart {
    Cursor { cursor: u64 },
    CurrentHead { current_head: bool },
}

fn domain_subscription_start(value: SubscriptionStart) -> domain::SubscriptionStart {
    match value {
        SubscriptionStart::Cursor { cursor } => domain::SubscriptionStart::cursor(cursor),
        SubscriptionStart::CurrentHead { current_head } => {
            domain::SubscriptionStart::current_head(current_head)
        }
    }
}

/// Rust-owned validated subscription specification.
#[derive(Debug, uniffi::Object)]
pub struct SubscriptionSpec {
    inner: domain::SubscriptionSpec,
}

#[uniffi::export]
impl SubscriptionSpec {
    #[uniffi::constructor]
    pub fn new(
        subscription_id: String,
        stream_path: String,
        start: SubscriptionStart,
        placement_anchor: bool,
    ) -> Result<Arc<Self>, BindingError> {
        domain::SubscriptionSpec::new(
            subscription_id,
            stream_path,
            domain_subscription_start(start),
            placement_anchor,
        )
        .map(|inner| Arc::new(Self { inner }))
        .map_err(domain_error)
    }

    pub fn subscription_id(&self) -> String { self.inner.subscription_id().to_owned() }
    pub fn stream_path(&self) -> String { self.inner.stream_path().to_owned() }
    pub fn placement_anchor(&self) -> bool { self.inner.placement_anchor() }
}

/// Wire header projected from the canonical header alias.
#[derive(Clone, Debug, uniffi::Record)]
pub struct Header {
    pub name: String,
    pub value: String,
}

fn domain_headers(headers: Vec<Header>) -> Vec<domain::Header> {
    headers
        .into_iter()
        .map(|header| domain::Header::new(header.name, header.value))
        .collect()
}

/// Opaque validated create request.
#[derive(Debug, uniffi::Object)]
pub struct CreateActorRequest {
    inner: domain::CreateActorRequest,
}

#[uniffi::export]
impl CreateActorRequest {
    #[uniffi::constructor]
    pub fn new(
        code_sha256: Arc<CodeSha256>,
        home_region: String,
        bindings: Vec<Arc<Binding>>,
        limits: Arc<ActorLimits>,
        subscriptions: Vec<Arc<SubscriptionSpec>>,
        idempotency_key: String,
    ) -> Result<Arc<Self>, BindingError> {
        domain::CreateActorRequest::new(
            code_sha256.inner.clone(),
            home_region,
            bindings.into_iter().map(|value| value.inner.clone()).collect(),
            limits.inner.clone(),
            subscriptions.into_iter().map(|value| value.inner.clone()).collect(),
            idempotency_key,
        )
        .map(|inner| Arc::new(Self { inner }))
        .map_err(domain_error)
    }
}

/// Opaque validated update request.
#[derive(Debug, uniffi::Object)]
pub struct UpdateActorRequest {
    inner: domain::UpdateActorRequest,
}

#[uniffi::export]
impl UpdateActorRequest {
    #[uniffi::constructor]
    pub fn new(
        actor_id: Arc<ActorId>,
        code_sha256: Arc<CodeSha256>,
        bindings: Vec<Arc<Binding>>,
        limits: Arc<ActorLimits>,
        expected_configuration_revision: u64,
        idempotency_key: String,
    ) -> Result<Arc<Self>, BindingError> {
        domain::UpdateActorRequest::new(
            actor_id.inner.clone(),
            code_sha256.inner.clone(),
            bindings.into_iter().map(|value| value.inner.clone()).collect(),
            limits.inner.clone(),
            expected_configuration_revision,
            idempotency_key,
        )
        .map(|inner| Arc::new(Self { inner }))
        .map_err(domain_error)
    }
}

/// Opaque inspect request. The legacy ActorId overload remains on ActorsClient.
#[derive(Debug, uniffi::Object)]
pub struct InspectActorRequest {
    inner: domain::InspectActorRequest,
}

#[uniffi::export]
impl InspectActorRequest {
    #[uniffi::constructor]
    pub fn new(actor_id: Arc<ActorId>) -> Arc<Self> {
        Arc::new(Self {
            inner: domain::InspectActorRequest::new(actor_id.inner.clone()),
        })
    }
}

/// Opaque validated add-subscription request.
#[derive(Debug, uniffi::Object)]
pub struct AddSubscriptionRequest {
    inner: domain::AddSubscriptionRequest,
}

#[uniffi::export]
impl AddSubscriptionRequest {
    #[uniffi::constructor]
    pub fn new(
        actor_id: Arc<ActorId>,
        subscription: Arc<SubscriptionSpec>,
        idempotency_key: String,
    ) -> Result<Arc<Self>, BindingError> {
        domain::AddSubscriptionRequest::new(
            actor_id.inner.clone(),
            subscription.inner.clone(),
            idempotency_key,
        )
        .map(|inner| Arc::new(Self { inner }))
        .map_err(domain_error)
    }
}

/// Opaque remove-subscription request.
#[derive(Debug, uniffi::Object)]
pub struct RemoveSubscriptionRequest {
    inner: domain::RemoveSubscriptionRequest,
}

#[uniffi::export]
impl RemoveSubscriptionRequest {
    #[uniffi::constructor]
    pub fn new(
        actor_id: Arc<ActorId>,
        subscription_id: String,
        idempotency_key: String,
    ) -> Arc<Self> {
        Arc::new(Self {
            inner: domain::RemoveSubscriptionRequest::new(
                actor_id.inner.clone(),
                subscription_id,
                idempotency_key,
            ),
        })
    }
}

/// Opaque resume-subscription request.
#[derive(Debug, uniffi::Object)]
pub struct ResumeSubscriptionRequest {
    inner: domain::ResumeSubscriptionRequest,
}

#[uniffi::export]
impl ResumeSubscriptionRequest {
    #[uniffi::constructor]
    pub fn new(
        actor_id: Arc<ActorId>,
        subscription_id: String,
        idempotency_key: String,
    ) -> Arc<Self> {
        Arc::new(Self {
            inner: domain::ResumeSubscriptionRequest::new(
                actor_id.inner.clone(),
                subscription_id,
                idempotency_key,
            ),
        })
    }
}

/// Opaque checkpoint request.
#[derive(Debug, uniffi::Object)]
pub struct CheckpointActorRequest {
    inner: domain::CheckpointActorRequest,
}

#[uniffi::export]
impl CheckpointActorRequest {
    #[uniffi::constructor]
    pub fn new(actor_id: Arc<ActorId>, idempotency_key: String) -> Arc<Self> {
        Arc::new(Self {
            inner: domain::CheckpointActorRequest::new(actor_id.inner.clone(), idempotency_key),
        })
    }
}

/// Opaque invocation request.
#[derive(Debug, uniffi::Object)]
pub struct InvokeActorRequest {
    inner: domain::InvokeActorRequest,
}

#[uniffi::export]
impl InvokeActorRequest {
    #[uniffi::constructor]
    pub fn new(
        actor_id: Arc<ActorId>,
        method: String,
        url: String,
        body: Vec<u8>,
        headers: Vec<Header>,
    ) -> Arc<Self> {
        Arc::new(Self {
            inner: domain::InvokeActorRequest::new(
                actor_id.inner.clone(),
                method,
                url,
                body,
                domain_headers(headers),
            ),
        })
    }
}

/// Typed invocation response with byte-preserving body and headers.
#[derive(Clone, Debug, uniffi::Record)]
pub struct InvokeActorResponse {
    pub status: u32,
    pub body: Vec<u8>,
    pub headers: Vec<Header>,
}

fn invoke_actor_response(value: domain::InvokeActorResponse) -> InvokeActorResponse {
    InvokeActorResponse {
        status: value.status(),
        body: value.body().to_vec(),
        headers: value
            .headers()
            .iter()
            .map(|header| Header {
                name: header.name().to_owned(),
                value: header.value().to_owned(),
            })
            .collect(),
    }
}


/// Known Actors state values emitted by the canonical Rust domain.
#[derive(Clone, Debug, uniffi::Enum)]
pub enum ActorState {
    Unspecified,
    Active,
    Hibernated,
    Paused,
}

fn actor_state(value: domain::ActorState) -> ActorState {
    match value {
        domain::ActorState::Unspecified => ActorState::Unspecified,
        domain::ActorState::Active => ActorState::Active,
        domain::ActorState::Hibernated => ActorState::Hibernated,
        domain::ActorState::Paused => ActorState::Paused,
    }
}

/// Known subscription state values emitted by the canonical Rust domain.
#[derive(Clone, Debug, uniffi::Enum)]
pub enum SubscriptionState {
    Unspecified,
    Active,
    Paused,
}

fn subscription_state(value: domain::SubscriptionState) -> SubscriptionState {
    match value {
        domain::SubscriptionState::Unspecified => SubscriptionState::Unspecified,
        domain::SubscriptionState::Active => SubscriptionState::Active,
        domain::SubscriptionState::Paused => SubscriptionState::Paused,
    }
}

/// Strongly typed projection of one observed subscription.
#[derive(Clone, Debug, uniffi::Record)]
pub struct SubscriptionObservation {
    pub subscription_id: String,
    pub stream_path: String,
    pub state: SubscriptionState,
    pub delivered_cursor: u64,
    pub completed_cursor: u64,
    pub recoverable_cursor: u64,
    pub placement_anchor: bool,
    pub retry_count: u32,
    pub failure_code: String,
    pub failed_cursor: Option<u64>,
}

fn subscription_observation(value: &domain::SubscriptionObservation) -> SubscriptionObservation {
    SubscriptionObservation {
        subscription_id: value.subscription_id().to_owned(),
        stream_path: value.stream_path().to_owned(),
        state: subscription_state(value.state()),
        delivered_cursor: value.delivered_cursor(),
        completed_cursor: value.completed_cursor(),
        recoverable_cursor: value.recoverable_cursor(),
        placement_anchor: value.placement_anchor(),
        retry_count: value.retry_count(),
        failure_code: value.failure_code().to_owned(),
        failed_cursor: value.failed_cursor(),
    }
}

/// Strongly typed projection of the canonical Actor observation.
#[derive(Clone, Debug, uniffi::Record)]
pub struct ActorObservation {
    pub actor_id: Arc<ActorId>,
    pub code_sha256: Arc<CodeSha256>,
    pub home_region: String,
    pub state: ActorState,
    pub subscriptions: Vec<SubscriptionObservation>,
    pub checkpoint_unix_millis: Option<u64>,
    pub checkpoint_epoch: u64,
    pub configuration_revision: u64,
}

fn actor_observation(value: &domain::ActorObservation) -> ActorObservation {
    ActorObservation {
        actor_id: Arc::new(ActorId {
            inner: value.actor_id().clone(),
        }),
        code_sha256: Arc::new(CodeSha256 {
            inner: value.code_sha256().clone(),
        }),
        home_region: value.home_region().to_owned(),
        state: actor_state(value.state()),
        subscriptions: value
            .subscriptions()
            .iter()
            .map(subscription_observation)
            .collect(),
        checkpoint_unix_millis: value.checkpoint_unix_millis(),
        checkpoint_epoch: value.checkpoint_epoch(),
        configuration_revision: value.configuration_revision(),
    }
}

/// Explicit Rust-owned cancellation for an in-flight Actors operation.
#[derive(Debug, uniffi::Object)]
pub struct CancellationHandle {
    token: CancellationToken,
}

#[uniffi::export]
impl CancellationHandle {
    /// Creates a fresh, one-shot cancellation handle.
    #[uniffi::constructor]
    pub fn new() -> Arc<Self> {
        Arc::new(Self {
            token: CancellationToken::new(),
        })
    }

    /// Cancels the Rust-owned operation associated with this handle.
    pub fn cancel(&self) {
        self.token.cancel();
    }

    /// Returns whether cancellation has been requested.
    pub fn is_cancelled(&self) -> bool {
        self.token.is_cancelled()
    }
}

/// Opaque Rust-owned Actors client.
#[derive(uniffi::Object)]
pub struct ActorsClient {
    inner: Arc<client::Client>,
}

#[uniffi::export(async_runtime = "tokio")]
impl ActorsClient {
    /// Executes the legacy inspect operation through the canonical Rust client.
    pub async fn inspect_actor(
        &self,
        actor_id: Arc<ActorId>,
        cancellation: Option<Arc<CancellationHandle>>,
    ) -> Result<Option<ActorObservation>, BindingError> {
        let request = domain::InspectActorRequest::new(actor_id.inner.clone());
        self.inspect_inner(&request, cancellation).await
    }

    pub async fn inspect_actor_request(
        &self,
        request: Arc<InspectActorRequest>,
        cancellation: Option<Arc<CancellationHandle>>,
    ) -> Result<Option<ActorObservation>, BindingError> {
        self.inspect_inner(&request.inner, cancellation).await
    }

    pub async fn create_actor(
        &self,
        request: Arc<CreateActorRequest>,
        cancellation: Option<Arc<CancellationHandle>>,
    ) -> Result<Option<ActorObservation>, BindingError> {
        let token = cancellation.as_ref().map(|handle| handle.token.clone());
        let response = client::run_with_cancellation(self.inner.create_actor(&request.inner), token)
            .await
            .map_err(client_error)?;
        Ok(response.actor().map(actor_observation))
    }

    pub async fn update_actor(
        &self,
        request: Arc<UpdateActorRequest>,
        cancellation: Option<Arc<CancellationHandle>>,
    ) -> Result<Option<ActorObservation>, BindingError> {
        let token = cancellation.as_ref().map(|handle| handle.token.clone());
        let response = client::run_with_cancellation(self.inner.update_actor(&request.inner), token)
            .await
            .map_err(client_error)?;
        Ok(response.actor().map(actor_observation))
    }

    pub async fn add_subscription(
        &self,
        request: Arc<AddSubscriptionRequest>,
        cancellation: Option<Arc<CancellationHandle>>,
    ) -> Result<Option<ActorObservation>, BindingError> {
        let token = cancellation.as_ref().map(|handle| handle.token.clone());
        let response = client::run_with_cancellation(
            self.inner.add_subscription(&request.inner),
            token,
        )
        .await
        .map_err(client_error)?;
        Ok(response.actor().map(actor_observation))
    }

    pub async fn remove_subscription(
        &self,
        request: Arc<RemoveSubscriptionRequest>,
        cancellation: Option<Arc<CancellationHandle>>,
    ) -> Result<Option<ActorObservation>, BindingError> {
        let token = cancellation.as_ref().map(|handle| handle.token.clone());
        let response = client::run_with_cancellation(
            self.inner.remove_subscription(&request.inner),
            token,
        )
        .await
        .map_err(client_error)?;
        Ok(response.actor().map(actor_observation))
    }

    pub async fn resume_subscription(
        &self,
        request: Arc<ResumeSubscriptionRequest>,
        cancellation: Option<Arc<CancellationHandle>>,
    ) -> Result<Option<ActorObservation>, BindingError> {
        let token = cancellation.as_ref().map(|handle| handle.token.clone());
        let response = client::run_with_cancellation(
            self.inner.resume_subscription(&request.inner),
            token,
        )
        .await
        .map_err(client_error)?;
        Ok(response.actor().map(actor_observation))
    }

    pub async fn checkpoint_actor(
        &self,
        request: Arc<CheckpointActorRequest>,
        cancellation: Option<Arc<CancellationHandle>>,
    ) -> Result<Option<ActorObservation>, BindingError> {
        let token = cancellation.as_ref().map(|handle| handle.token.clone());
        let response = client::run_with_cancellation(
            self.inner.checkpoint_actor(&request.inner),
            token,
        )
        .await
        .map_err(client_error)?;
        Ok(response.actor().map(actor_observation))
    }

    pub async fn invoke_actor(
        &self,
        request: Arc<InvokeActorRequest>,
        cancellation: Option<Arc<CancellationHandle>>,
    ) -> Result<InvokeActorResponse, BindingError> {
        let token = cancellation.as_ref().map(|handle| handle.token.clone());
        let response = client::run_with_cancellation(self.inner.invoke_actor(&request.inner), token)
            .await
            .map_err(client_error)?;
        Ok(invoke_actor_response(response))
    }
}

impl ActorsClient {
    async fn inspect_inner(
        &self,
        request: &domain::InspectActorRequest,
        cancellation: Option<Arc<CancellationHandle>>,
    ) -> Result<Option<ActorObservation>, BindingError> {
        let token = cancellation.as_ref().map(|handle| handle.token.clone());
        let response = client::run_with_cancellation(self.inner.inspect_actor(request), token)
            .await
            .map_err(client_error)?;
        Ok(response.actor().map(actor_observation))
    }
}

/// Connects through the canonical Rust transport and returns an opaque client.
#[uniffi::export(async_runtime = "tokio")]
pub async fn connect_actors(
    endpoint: String,
    token: String,
    cancellation: Option<Arc<CancellationHandle>>,
) -> Result<Arc<ActorsClient>, BindingError> {
    let token_handle = cancellation.as_ref().map(|handle| handle.token.clone());
    client::run_with_cancellation(client::connect(&endpoint, &token), token_handle)
        .await
        .map(actors_client)
        .map_err(client_error)
}

/// Connects with an optional caller-pinned CA certificate through the
/// canonical Rust transport and returns an opaque client.
#[uniffi::export(async_runtime = "tokio")]
pub async fn connect_actors_with_ca(
    endpoint: String,
    token: String,
    ca_certificate: Option<Vec<u8>>,
    cancellation: Option<Arc<CancellationHandle>>,
) -> Result<Arc<ActorsClient>, BindingError> {
    let token_handle = cancellation.as_ref().map(|handle| handle.token.clone());
    client::run_with_cancellation(
        client::connect_with_ca_certificate(&endpoint, &token, ca_certificate.as_deref()),
        token_handle,
    )
    .await
    .map(actors_client)
    .map_err(client_error)
}

fn actors_client(inner: client::Client) -> Arc<ActorsClient> {
    Arc::new(ActorsClient {
        inner: Arc::new(inner),
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn direct_domain_contract_errors_retain_contract_category() {
        let error = domain::PositiveU64::new(0).expect_err("zero is not positive");

        assert!(matches!(
            domain_error(error),
            BindingError::Contract { .. }
        ));
    }
}


