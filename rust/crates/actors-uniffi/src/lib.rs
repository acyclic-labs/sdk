//! Rust-owned UniFFI metadata for the Actors client.
//!
//! This first vertical slice exposes nominal semantic values, an explicit
//! cancellation handle, client connection, and `inspect_actor`. All request
//! construction, validation, transport selection, response conversion, and
//! cancellation remain in `acyclic-actors`.

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
    BindingError::Semantic {
        detail_message: error.to_string(),
    }
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
    /// Inspects an Actor through the canonical Rust client and preserves the
    /// response's optional Actor observation.
    pub async fn inspect_actor(
        &self,
        actor_id: Arc<ActorId>,
        cancellation: Option<Arc<CancellationHandle>>,
    ) -> Result<Option<ActorObservation>, BindingError> {
        let request = domain::InspectActorRequest::new(actor_id.inner.clone());
        let token = cancellation.as_ref().map(|handle| handle.token.clone());
        let response = client::run_with_cancellation(self.inner.inspect_actor(&request), token)
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
