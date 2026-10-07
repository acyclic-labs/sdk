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

    struct RemoteConformanceResult {
        ok: bool,
        operations_completed: u32,
        authentication_rejected: bool,
        error: ErrorKind,
        message: String,
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
        fn actors_live_conformance_probe(
            endpoint: &str,
            token: &str,
            ca_certificate: &str,
        ) -> RemoteConformanceResult;

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

struct ProbeFailure {
    completed: u32,
    kind: ffi::ErrorKind,
    message: String,
}

impl ProbeFailure {
    fn client(completed: u32, error: client::Error) -> Self {
        let error = ActorsError::from_client(error);
        Self {
            completed,
            kind: error.kind,
            message: error.message,
        }
    }

    fn semantic(completed: u32, message: impl Into<String>) -> Self {
        Self {
            completed,
            kind: ffi::ErrorKind::Semantic,
            message: message.into(),
        }
    }
}

fn assert_remote_observation(
    observation: Option<&domain::ActorObservation>,
) -> Result<(), String> {
    let observation = observation.ok_or_else(|| "remote response omitted actor".to_owned())?;
    if observation.actor_id().as_str() != "actor-a"
        || observation.code_sha256().as_bytes() != &[1_u8; 32]
        || observation.home_region() != "eu"
        || observation.state() != domain::ActorState::Active
        || !observation.subscriptions().is_empty()
        || observation.checkpoint_unix_millis().is_some()
        || observation.checkpoint_epoch() != 9
        || observation.configuration_revision() != 0
    {
        return Err("remote response changed the canonical Actor observation".into());
    }
    Ok(())
}

async fn run_live_conformance(
    endpoint: &str,
    token: &str,
    ca_certificate: &str,
) -> Result<(u32, bool), ProbeFailure> {
    let client = client::connect_with_ca_certificate(endpoint, token, Some(ca_certificate.as_bytes()))
        .await
        .map_err(|error| ProbeFailure::client(0, error))?;
    let actor_id = domain::ActorId::new("actor-a".into())
        .map_err(|error| ProbeFailure::semantic(0, error.to_string()))?;
    let code_sha256 = domain::CodeSha256::new(vec![1; 32])
        .map_err(|error| ProbeFailure::semantic(0, error.to_string()))?;
    let limits = domain::ActorLimits::new(1_000, 1024, 1024)
        .map_err(|error| ProbeFailure::semantic(0, error.to_string()))?;
    let subscription = domain::SubscriptionSpec::new(
        "input".into(),
        "events/input".into(),
        domain::SubscriptionStart::Cursor {
            cursor: 9_007_199_254_740_993,
        },
        false,
    )
    .map_err(|error| ProbeFailure::semantic(0, error.to_string()))?;
    let create = domain::CreateActorRequest::new(
        code_sha256.clone(),
        "eu".into(),
        vec![],
        limits,
        vec![],
        "create-cxx".into(),
    )
    .map_err(|error| ProbeFailure::semantic(0, error.to_string()))?;
    let create_response = client
        .create_actor(&create)
        .await
        .map_err(|error| ProbeFailure::client(0, error))?;
    assert_remote_observation(create_response.actor())
        .map_err(|error| ProbeFailure::semantic(0, error))?;
    let mut completed = 1;

    let update = domain::UpdateActorRequest::new(
        actor_id.clone(),
        code_sha256.clone(),
        vec![],
        limits,
        0,
        "update-cxx".into(),
    )
    .map_err(|error| ProbeFailure::semantic(completed, error.to_string()))?;
    let update_response = client
        .update_actor(&update)
        .await
        .map_err(|error| ProbeFailure::client(completed, error))?;
    assert_remote_observation(update_response.actor())
        .map_err(|error| ProbeFailure::semantic(completed, error))?;
    completed += 1;

    let inspect = domain::InspectActorRequest::new(actor_id.clone());
    let inspect_response = client
        .inspect_actor(&inspect)
        .await
        .map_err(|error| ProbeFailure::client(completed, error))?;
    assert_remote_observation(inspect_response.actor())
        .map_err(|error| ProbeFailure::semantic(completed, error))?;
    completed += 1;

    let add = domain::AddSubscriptionRequest::new(
        actor_id.clone(),
        subscription,
        "subscribe-cxx".into(),
    )
    .map_err(|error| ProbeFailure::semantic(completed, error.to_string()))?;
    let add_response = client
        .add_subscription(&add)
        .await
        .map_err(|error| ProbeFailure::client(completed, error))?;
    assert_remote_observation(add_response.actor())
        .map_err(|error| ProbeFailure::semantic(completed, error))?;
    completed += 1;

    let remove = domain::RemoveSubscriptionRequest::new(
        actor_id.clone(),
        "input".into(),
        "remove-cxx".into(),
    );
    let remove_response = client
        .remove_subscription(&remove)
        .await
        .map_err(|error| ProbeFailure::client(completed, error))?;
    assert_remote_observation(remove_response.actor())
        .map_err(|error| ProbeFailure::semantic(completed, error))?;
    completed += 1;

    let resume = domain::ResumeSubscriptionRequest::new(
        actor_id.clone(),
        "input".into(),
        "resume-cxx".into(),
    );
    let resume_response = client
        .resume_subscription(&resume)
        .await
        .map_err(|error| ProbeFailure::client(completed, error))?;
    assert_remote_observation(resume_response.actor())
        .map_err(|error| ProbeFailure::semantic(completed, error))?;
    completed += 1;

    // The canonical fixture intentionally pins this key so every client family
    // exercises the same request identity and response path.
    let checkpoint = domain::CheckpointActorRequest::new(actor_id.clone(), "checkpoint-a".into());
    let checkpoint_response = client
        .checkpoint_actor(&checkpoint)
        .await
        .map_err(|error| ProbeFailure::client(completed, error))?;
    assert_remote_observation(checkpoint_response.actor())
        .map_err(|error| ProbeFailure::semantic(completed, error))?;
    completed += 1;

    let invoke = domain::InvokeActorRequest::new(
        actor_id,
        "POST".into(),
        "/".into(),
        br#"{}"#.to_vec(),
        vec![domain::Header {
            name: "content-type".into(),
            value: "application/json".into(),
        }],
    );
    let invoke_response = client
        .invoke_actor(&invoke)
        .await
        .map_err(|error| ProbeFailure::client(completed, error))?;
    if invoke_response.status() != 201
        || invoke_response
            .headers()
            .first()
            .is_none_or(|header| header.name != "location")
    {
        return Err(ProbeFailure::semantic(
            completed,
            "remote invoke response changed status or headers",
        ));
    }
    completed += 1;

    let denied = client::connect_with_ca_certificate(
        endpoint,
        "wrong",
        Some(ca_certificate.as_bytes()),
    )
    .await
    .map_err(|error| ProbeFailure::client(completed, error))?;
    let authentication_rejected = matches!(
        denied.inspect_actor(&domain::InspectActorRequest::new(
            domain::ActorId::new("actor-a".into())
                .map_err(|error| ProbeFailure::semantic(completed, error.to_string()))?,
        ))
        .await,
        Err(client::Error::Service { .. })
    );
    if !authentication_rejected {
        return Err(ProbeFailure::semantic(
            completed,
            "remote accepted an invalid bearer credential",
        ));
    }
    Ok((completed, authentication_rejected))
}

fn actors_live_conformance_probe(
    endpoint: &str,
    token: &str,
    ca_certificate: &str,
) -> ffi::RemoteConformanceResult {
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("qualification runtime must construct");
    match runtime.block_on(run_live_conformance(endpoint, token, ca_certificate)) {
        Ok((completed, authentication_rejected)) => ffi::RemoteConformanceResult {
            ok: true,
            operations_completed: completed,
            authentication_rejected,
            error: ffi::ErrorKind::NoError,
            message: "all eight Actors operations completed over authenticated TLS".into(),
        },
        Err(error) => ffi::RemoteConformanceResult {
            ok: false,
            operations_completed: error.completed,
            authentication_rejected: false,
            error: error.kind,
            message: error.message,
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
