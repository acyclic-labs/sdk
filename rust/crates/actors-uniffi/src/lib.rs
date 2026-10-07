//! UniFFI exports for the Rust-owned Actors semantic client.
//!
//! The domain records and enums are declared in `acyclic-actors::domain` and
//! compiled with that crate's opt-in `uniffi` feature. This crate owns only the
//! foreign boundary: typed error conversion, cancellation, connection, and
//! the eight shared client operations.

use std::sync::Arc;

use acyclic_actors::{client, domain};
use tokio_util::sync::CancellationToken;

mod nominal;

uniffi::setup_scaffolding!();

// Re-export the producer declarations so Rust and generated foreign callers
// see one public type identity. These are not facade-owned definitions.
pub use domain::{
    ActorId, ActorLimits, ActorObservation, ActorState, AddSubscriptionRequest,
    AddSubscriptionResponse, Binding, CheckpointActorRequest, CheckpointActorResponse, CodeSha256,
    CreateActorRequest, CreateActorResponse, ErrorCode, Header, InspectActorRequest,
    InspectActorResponse, InvokeActorRequest, InvokeActorResponse, RemoveSubscriptionRequest,
    RemoveSubscriptionResponse, ResumeSubscriptionRequest, ResumeSubscriptionResponse,
    ServiceError, SubscriptionObservation, SubscriptionSpec, SubscriptionStart, SubscriptionState,
    UpdateActorRequest, UpdateActorResponse,
};

pub use domain::subscription_start::CurrentHeadMarker;
pub use nominal::{
    CommitId, IdempotencyKey, NOMINAL_METADATA, NominalCarrier, NominalMetadata, StreamPath,
    nominal_metadata_toml,
};

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

/// Validates an Actor identity through the canonical Rust constructor.
#[uniffi::export]
pub fn validate_actor_id(value: String) -> Result<(), BindingError> {
    domain::ActorId::new(value)
        .map(|_| ())
        .map_err(domain_error)
}

/// Validates a code digest through the canonical Rust constructor.
#[uniffi::export]
pub fn validate_code_sha256(value: Vec<u8>) -> Result<(), BindingError> {
    domain::CodeSha256::new(value)
        .map(|_| ())
        .map_err(domain_error)
}

/// Validates a positive unsigned integer through the canonical Rust constructor.
#[uniffi::export]
pub fn validate_positive_u64(value: u64) -> Result<(), BindingError> {
    domain::PositiveU64::new(value)
        .map(|_| ())
        .map_err(domain_error)
}

/// Validates the true-only current-head marker through Rust.
#[uniffi::export]
pub fn validate_current_head_marker(value: bool) -> Result<(), BindingError> {
    domain::subscription_start::CurrentHeadMarker::try_from(value)
        .map(|_| ())
        .map_err(domain_error)
}

/// Validates a Stream path through the canonical Rust constructor.
#[uniffi::export]
pub fn validate_stream_path(value: String) -> Result<(), BindingError> {
    StreamPath::new(value)
        .map(|_| ())
        .map_err(|error| BindingError::Semantic {
            detail_message: error.to_string(),
        })
}

/// Validates an exact-width Stream commit identity through Rust.
#[uniffi::export]
pub fn validate_commit_id(value: Vec<u8>) -> Result<(), BindingError> {
    CommitId::new(value)
        .map(|_| ())
        .map_err(|error| BindingError::Semantic {
            detail_message: error.to_string(),
        })
}

/// Validates a non-empty bounded retry identity through Rust.
#[uniffi::export]
pub fn validate_idempotency_key(value: Vec<u8>) -> Result<(), BindingError> {
    IdempotencyKey::new(value)
        .map(|_| ())
        .map_err(|error| BindingError::Semantic {
            detail_message: error.to_string(),
        })
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

/// Opaque connected Actors client backed by the shared Rust client.
#[derive(uniffi::Object)]
pub struct ActorsClient {
    inner: Arc<client::Client>,
}

#[uniffi::export(async_runtime = "tokio")]
impl ActorsClient {
    /// Executes the canonical create operation.
    pub async fn create_actor(
        &self,
        request: domain::CreateActorRequest,
        cancellation: Option<Arc<CancellationHandle>>,
    ) -> Result<domain::CreateActorResponse, BindingError> {
        self.run(self.inner.create_actor(&request), cancellation)
            .await
    }

    /// Executes the canonical update operation.
    pub async fn update_actor(
        &self,
        request: domain::UpdateActorRequest,
        cancellation: Option<Arc<CancellationHandle>>,
    ) -> Result<domain::UpdateActorResponse, BindingError> {
        self.run(self.inner.update_actor(&request), cancellation)
            .await
    }

    /// Executes the canonical inspect operation.
    pub async fn inspect_actor(
        &self,
        request: domain::InspectActorRequest,
        cancellation: Option<Arc<CancellationHandle>>,
    ) -> Result<domain::InspectActorResponse, BindingError> {
        self.run(self.inner.inspect_actor(&request), cancellation)
            .await
    }

    /// Executes the canonical add-subscription operation.
    pub async fn add_subscription(
        &self,
        request: domain::AddSubscriptionRequest,
        cancellation: Option<Arc<CancellationHandle>>,
    ) -> Result<domain::AddSubscriptionResponse, BindingError> {
        self.run(self.inner.add_subscription(&request), cancellation)
            .await
    }

    /// Executes the canonical remove-subscription operation.
    pub async fn remove_subscription(
        &self,
        request: domain::RemoveSubscriptionRequest,
        cancellation: Option<Arc<CancellationHandle>>,
    ) -> Result<domain::RemoveSubscriptionResponse, BindingError> {
        self.run(self.inner.remove_subscription(&request), cancellation)
            .await
    }

    /// Executes the canonical resume-subscription operation.
    pub async fn resume_subscription(
        &self,
        request: domain::ResumeSubscriptionRequest,
        cancellation: Option<Arc<CancellationHandle>>,
    ) -> Result<domain::ResumeSubscriptionResponse, BindingError> {
        self.run(self.inner.resume_subscription(&request), cancellation)
            .await
    }

    /// Executes the canonical checkpoint operation.
    pub async fn checkpoint_actor(
        &self,
        request: domain::CheckpointActorRequest,
        cancellation: Option<Arc<CancellationHandle>>,
    ) -> Result<domain::CheckpointActorResponse, BindingError> {
        self.run(self.inner.checkpoint_actor(&request), cancellation)
            .await
    }

    /// Executes the canonical invocation operation.
    pub async fn invoke_actor(
        &self,
        request: domain::InvokeActorRequest,
        cancellation: Option<Arc<CancellationHandle>>,
    ) -> Result<domain::InvokeActorResponse, BindingError> {
        self.run(self.inner.invoke_actor(&request), cancellation)
            .await
    }
}

impl ActorsClient {
    async fn run<T, Operation>(
        &self,
        operation: Operation,
        cancellation: Option<Arc<CancellationHandle>>,
    ) -> Result<T, BindingError>
    where
        Operation: std::future::Future<Output = Result<T, client::Error>>,
    {
        let token = cancellation.as_ref().map(|handle| handle.token.clone());
        client::run_with_cancellation(operation, token)
            .await
            .map_err(client_error)
    }
}

/// Connects through the canonical Rust transport and returns an opaque client.
#[uniffi::export(async_runtime = "tokio")]
pub async fn connect_actors(
    endpoint: String,
    token: String,
    cancellation: Option<Arc<CancellationHandle>>,
) -> Result<Arc<ActorsClient>, BindingError> {
    connect_inner(client::connect(&endpoint, &token), cancellation).await
}

/// Connects with an optional caller-pinned CA certificate.
#[uniffi::export(async_runtime = "tokio")]
pub async fn connect_actors_with_ca(
    endpoint: String,
    token: String,
    ca_certificate: Option<Vec<u8>>,
    cancellation: Option<Arc<CancellationHandle>>,
) -> Result<Arc<ActorsClient>, BindingError> {
    connect_inner(
        client::connect_with_ca_certificate(&endpoint, &token, ca_certificate.as_deref()),
        cancellation,
    )
    .await
}

async fn connect_inner<Operation>(
    operation: Operation,
    cancellation: Option<Arc<CancellationHandle>>,
) -> Result<Arc<ActorsClient>, BindingError>
where
    Operation: std::future::Future<Output = Result<client::Client, client::Error>>,
{
    let token = cancellation.as_ref().map(|handle| handle.token.clone());
    client::run_with_cancellation(operation, token)
        .await
        .map(|inner| {
            Arc::new(ActorsClient {
                inner: Arc::new(inner),
            })
        })
        .map_err(client_error)
}
