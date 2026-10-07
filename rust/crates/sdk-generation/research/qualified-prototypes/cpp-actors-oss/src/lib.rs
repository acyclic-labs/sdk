//! Qualification fixture: CXX is the maintained bridge; Actors remains the Rust source of truth.
use std::sync::Arc;
use acyclic_actors::{client, domain, wire};
use tokio_util::sync::CancellationToken;

#[cxx::bridge(namespace = "acyclic::actors")]
mod ffi {
    /// Structured boundary error category; payload and validation remain Rust-owned.
    enum ErrorKind {
        NoError,
        InvalidArgument,
        LimitExceeded,
        DuplicateName,
        Cancelled,
        Configuration,
        Transport,
        Service,
        Semantic,
    }

    struct PositiveU64Result {
        ok: bool,
        value: u64,
        error: ErrorKind,
    }

    struct ClientConnectResult {
        connected: bool,
        error: ErrorKind,
    }

    extern "Rust" {
        type ActorsClient;
        type ActorsError;
        type ActorObservationView;
        type PositiveU64View;
        type ActorsOperation;

        fn actors_client_new() -> Box<ActorsClient>;
        fn actors_client_is_connected(client: &ActorsClient) -> bool;
        fn actors_client_connect_probe(endpoint: &str, token: &str) -> ClientConnectResult;

        fn actors_cancelled_error() -> Box<ActorsError>;
        fn actors_error_kind(error: &ActorsError) -> ErrorKind;
        fn actors_error_message(error: &ActorsError) -> String;

        fn actors_sample_observation() -> Box<ActorObservationView>;
        fn actor_observation_checkpoint_epoch(observation: &ActorObservationView) -> u64;
        fn actor_observation_has_checkpoint(observation: &ActorObservationView) -> bool;
        fn actor_observation_checkpoint(observation: &ActorObservationView) -> u64;

        fn actors_validate_positive_u64(value: u64) -> PositiveU64Result;
        fn positive_u64_value(value: &PositiveU64View) -> u64;

        fn actors_operation_new() -> Box<ActorsOperation>;
        fn actors_operation_cancel(operation: &mut ActorsOperation);
        fn actors_operation_is_cancelled(operation: &ActorsOperation) -> bool;
    }
}

pub struct ActorsClient {
    // The maintained client type is owned by Rust; a real constructor is async.
    inner: Option<client::Client>,
}

pub struct ActorsError {
    kind: ffi::ErrorKind,
    message: String,
}

pub struct ActorObservationView(domain::ActorObservation);
pub struct PositiveU64View(domain::PositiveU64);
pub struct ActorsOperation {
    cancellation: CancellationToken,
}

fn actors_client_new() -> Box<ActorsClient> {
    Box::new(ActorsClient { inner: None })
}

fn actors_client_is_connected(client: &ActorsClient) -> bool {
    client.inner.is_some()
}

fn actors_client_connect_probe(endpoint: &str, token: &str) -> ffi::ClientConnectResult {
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("qualification runtime must construct");
    match runtime.block_on(client::connect(endpoint, token)) {
        Ok(client) => {
            let _ = client.transport();
            ffi::ClientConnectResult {
                connected: true,
                error: ffi::ErrorKind::NoError,
            }
        }
        Err(error) => ffi::ClientConnectResult {
            connected: false,
            error: match error {
                client::Error::Configuration(_) => ffi::ErrorKind::Configuration,
                client::Error::Transport(_) => ffi::ErrorKind::Transport,
                client::Error::Service { .. } => ffi::ErrorKind::Service,
                client::Error::Contract(_) => ffi::ErrorKind::InvalidArgument,
                client::Error::Semantic(_) => ffi::ErrorKind::Semantic,
                client::Error::Cancelled => ffi::ErrorKind::Cancelled,
            },
        },
    }
}

fn actors_cancelled_error() -> Box<ActorsError> {
    Box::new(ActorsError::from_client(client::Error::Cancelled))
}

fn actors_error_kind(error: &ActorsError) -> ffi::ErrorKind {
    error.kind
}

fn actors_error_message(error: &ActorsError) -> String {
    error.message.clone()
}

fn actors_sample_observation() -> Box<ActorObservationView> {
    let wire = wire::ActorObservation {
        actor_id: "actor-1".to_owned(),
        code_sha256: vec![1_u8; 32].into(),
        home_region: "eu".to_owned(),
        state: 1,
        subscriptions: Vec::new(),
        checkpoint_unix_millis: Some(u64::MAX),
        checkpoint_epoch: u64::MAX,
        configuration_revision: 7,
    };
    Box::new(ActorObservationView(
        domain::ActorObservation::try_from(wire).expect("fixture is valid by Actors domain rules"),
    ))
}

fn actor_observation_checkpoint_epoch(observation: &ActorObservationView) -> u64 {
    observation.0.checkpoint_epoch()
}

fn actor_observation_has_checkpoint(observation: &ActorObservationView) -> bool {
    observation.0.checkpoint_unix_millis().is_some()
}

fn actor_observation_checkpoint(observation: &ActorObservationView) -> u64 {
    observation.0.checkpoint_unix_millis().expect("presence checked by caller")
}

fn actors_validate_positive_u64(value: u64) -> ffi::PositiveU64Result {
    match domain::PositiveU64::new(value) {
        Ok(value) => ffi::PositiveU64Result {
            ok: true,
            value: value.get(),
            error: ffi::ErrorKind::NoError,
        },
        Err(error) => ffi::PositiveU64Result {
            ok: false,
            value: 0,
            error: match error {
                domain::DomainError::Contract(acyclic_actors::ContractError::LimitExceeded) => {
                    ffi::ErrorKind::LimitExceeded
                }
                domain::DomainError::Contract(acyclic_actors::ContractError::DuplicateName) => {
                    ffi::ErrorKind::DuplicateName
                }
                _ => ffi::ErrorKind::InvalidArgument,
            },
        },
    }
}

fn positive_u64_value(value: &PositiveU64View) -> u64 {
    value.0.get()
}

fn actors_operation_new() -> Box<ActorsOperation> {
    Box::new(ActorsOperation {
        cancellation: CancellationToken::new(),
    })
}

fn actors_operation_cancel(operation: &mut ActorsOperation) {
    operation.cancellation.cancel();
}

fn actors_operation_is_cancelled(operation: &ActorsOperation) -> bool {
    operation.cancellation.is_cancelled()
}

impl ActorsError {
    fn from_client(error: client::Error) -> Self {
        let (kind, message) = match &error {
            client::Error::Configuration(_) => (ffi::ErrorKind::Configuration, error.to_string()),
            client::Error::Transport(_) => (ffi::ErrorKind::Transport, error.to_string()),
            client::Error::Service { .. } => (ffi::ErrorKind::Service, error.to_string()),
            client::Error::Contract(acyclic_actors::ContractError::InvalidArgument) => {
                (ffi::ErrorKind::InvalidArgument, error.to_string())
            }
            client::Error::Contract(acyclic_actors::ContractError::LimitExceeded) => {
                (ffi::ErrorKind::LimitExceeded, error.to_string())
            }
            client::Error::Contract(acyclic_actors::ContractError::DuplicateName) => {
                (ffi::ErrorKind::DuplicateName, error.to_string())
            }
            client::Error::Semantic(_) => (ffi::ErrorKind::Semantic, error.to_string()),
            client::Error::Cancelled => (ffi::ErrorKind::Cancelled, error.to_string()),
        };
        Self { kind, message }
    }
}

// Keep Arc imported in the source fixture to document that CXX's opaque ownership
// is the intended replacement for a hand-written raw-pointer registry.
#[allow(dead_code)]
fn _opaque_owner_marker(_: Arc<ActorsClient>) {}
