//! Qualification fixture: CXX is the maintained bridge; Actors remains the Rust source of truth.
use std::future::Future;
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
        type ActorOperationResult;

        fn actors_client_new() -> Box<ActorsClient>;
        fn actors_client_connect(
            endpoint: &str,
            token: &str,
            ca_certificate: &str,
        ) -> Box<ActorsClient>;
        fn actors_client_is_connected(client: &ActorsClient) -> bool;
        fn actors_client_error_kind(client: &ActorsClient) -> ErrorKind;
        fn actors_client_error_message(client: &ActorsClient) -> String;
        fn actors_client_connect_probe(endpoint: &str, token: &str) -> ClientConnectResult;
        fn actors_create_actor(client: &ActorsClient) -> Box<ActorOperationResult>;
        fn actors_update_actor(client: &ActorsClient) -> Box<ActorOperationResult>;
        fn actors_inspect_actor(client: &ActorsClient) -> Box<ActorOperationResult>;
        fn actors_add_subscription(client: &ActorsClient) -> Box<ActorOperationResult>;
        fn actors_remove_subscription(client: &ActorsClient) -> Box<ActorOperationResult>;
        fn actors_resume_subscription(client: &ActorsClient) -> Box<ActorOperationResult>;
        fn actors_checkpoint_actor(client: &ActorsClient) -> Box<ActorOperationResult>;
        fn actors_invoke_actor(client: &ActorsClient) -> Box<ActorOperationResult>;
        fn actors_inspect_actor_with_cancel(
            client: &ActorsClient,
            operation: &ActorsOperation,
        ) -> Box<ActorOperationResult>;
        fn actor_operation_result_empty() -> Box<ActorOperationResult>;
        fn actor_operation_ok(result: &ActorOperationResult) -> bool;
        fn actor_operation_error(result: &ActorOperationResult) -> ErrorKind;
        fn actor_operation_message(result: &ActorOperationResult) -> String;
        fn actor_operation_actor_id(result: &ActorOperationResult) -> String;
        fn actor_operation_home_region(result: &ActorOperationResult) -> String;
        fn actor_operation_active(result: &ActorOperationResult) -> bool;
        fn actor_operation_subscriptions_empty(result: &ActorOperationResult) -> bool;
        fn actor_operation_configuration_revision(result: &ActorOperationResult) -> u64;
        fn actor_operation_checkpoint_epoch(result: &ActorOperationResult) -> u64;
        fn actor_operation_has_checkpoint(result: &ActorOperationResult) -> bool;
        fn actor_operation_status(result: &ActorOperationResult) -> u32;
        fn actor_operation_has_location_header(result: &ActorOperationResult) -> bool;
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
        fn actors_operation_cancel(operation: &ActorsOperation);
        fn actors_operation_is_cancelled(operation: &ActorsOperation) -> bool;
    }
}

pub struct ActorsClient {
    // The maintained client type is owned by Rust; a real constructor is async.
    inner: Option<client::Client>,
    last_error: Option<ActorsError>,
    runtime: Option<tokio::runtime::Runtime>,
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
pub struct ActorOperationResult {
    ok: bool,
    error: ffi::ErrorKind,
    message: String,
    actor_id: String,
    home_region: String,
    active: bool,
    subscriptions_empty: bool,
    configuration_revision: u64,
    checkpoint_epoch: u64,
    has_checkpoint: bool,
    checkpoint: u64,
    status: u32,
    has_location_header: bool,
}

fn actors_client_new() -> Box<ActorsClient> {
    Box::new(ActorsClient {
        inner: None,
        last_error: None,
        runtime: None,
    })
}

fn actors_client_connect(endpoint: &str, token: &str, ca_certificate: &str) -> Box<ActorsClient> {
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("qualification runtime must construct");
    match runtime.block_on(client::connect_with_ca_certificate(
        endpoint,
        token,
        Some(ca_certificate.as_bytes()),
    )) {
        Ok(client) => Box::new(ActorsClient {
            inner: Some(client),
            last_error: None,
            runtime: Some(runtime),
        }),
        Err(error) => Box::new(ActorsClient {
            inner: None,
            last_error: Some(ActorsError::from_client(error)),
            runtime: Some(runtime),
        }),
    }
}

fn actors_client_is_connected(client: &ActorsClient) -> bool {
    client.inner.is_some()
}

fn actors_client_error_kind(client: &ActorsClient) -> ffi::ErrorKind {
    client
        .last_error
        .as_ref()
        .map_or(ffi::ErrorKind::NoError, |error| error.kind)
}

fn actors_client_error_message(client: &ActorsClient) -> String {
    client
        .last_error
        .as_ref()
        .map_or_else(String::new, |error| error.message.clone())
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

fn empty_operation_result() -> Box<ActorOperationResult> {
    Box::new(ActorOperationResult {
        ok: false,
        error: ffi::ErrorKind::NoError,
        message: String::new(),
        actor_id: String::new(),
        home_region: String::new(),
        active: false,
        subscriptions_empty: false,
        configuration_revision: 0,
        checkpoint_epoch: 0,
        has_checkpoint: false,
        checkpoint: 0,
        status: 0,
        has_location_header: false,
    })
}

fn operation_error(error: client::Error) -> Box<ActorOperationResult> {
    let error = ActorsError::from_client(error);
    let mut result = empty_operation_result();
    result.error = error.kind;
    result.message = error.message;
    result
}

fn operation_semantic(message: impl Into<String>) -> Box<ActorOperationResult> {
    let mut result = empty_operation_result();
    result.error = ffi::ErrorKind::Semantic;
    result.message = message.into();
    result
}

fn observation_operation_result(
    observation: Option<&domain::ActorObservation>,
) -> Box<ActorOperationResult> {
    let Some(observation) = observation else {
        return operation_semantic("remote response omitted actor");
    };
    let mut result = empty_operation_result();
    result.ok = true;
    result.error = ffi::ErrorKind::NoError;
    result.actor_id = observation.actor_id().as_str().to_string();
    result.home_region = observation.home_region().to_string();
    result.active = observation.state() == domain::ActorState::Active;
    result.subscriptions_empty = observation.subscriptions().is_empty();
    result.configuration_revision = observation.configuration_revision();
    result.checkpoint_epoch = observation.checkpoint_epoch();
    result.has_checkpoint = observation.checkpoint_unix_millis().is_some();
    result.checkpoint = observation.checkpoint_unix_millis().unwrap_or_default();
    result
}

fn invoke_operation_result(response: &domain::InvokeActorResponse) -> Box<ActorOperationResult> {
    let mut result = empty_operation_result();
    result.ok = response.status() == 201
        && response
            .headers()
            .first()
            .is_some_and(|header| header.name == "location");
    result.error = if result.ok {
        ffi::ErrorKind::NoError
    } else {
        ffi::ErrorKind::Semantic
    };
    result.status = response.status();
    result.has_location_header = response
        .headers()
        .first()
        .is_some_and(|header| header.name == "location");
    if !result.ok {
        result.message = "remote invoke response changed status or headers".into();
    }
    result
}

fn block_on_client<T, F>(facade: &ActorsClient, operation: F) -> Result<T, client::Error>
where
    F: Future<Output = Result<T, client::Error>>,
{
    let Some(runtime) = facade.runtime.as_ref() else {
        return Err(client::Error::Configuration("client is not connected".into()));
    };
    runtime.block_on(operation)
}

fn canonical_actor_id() -> Result<domain::ActorId, String> {
    domain::ActorId::new("actor-a".into()).map_err(|error| error.to_string())
}

fn canonical_code_sha256() -> Result<domain::CodeSha256, String> {
    domain::CodeSha256::new(vec![1; 32]).map_err(|error| error.to_string())
}

fn canonical_limits() -> Result<domain::ActorLimits, String> {
    domain::ActorLimits::new(1_000, 1024, 1024).map_err(|error| error.to_string())
}

fn canonical_subscription() -> Result<domain::SubscriptionSpec, String> {
    domain::SubscriptionSpec::new(
        "input".into(),
        "events/input".into(),
        domain::SubscriptionStart::Cursor {
            cursor: 9_007_199_254_740_993,
        },
        false,
    )
    .map_err(|error| error.to_string())
}

fn actors_create_actor(facade: &ActorsClient) -> Box<ActorOperationResult> {
    let Some(client) = facade.inner.as_ref() else {
        return facade
            .last_error
            .as_ref()
            .map(|error| {
                let mut result = empty_operation_result();
                result.error = error.kind;
                result.message = error.message.clone();
                result
            })
            .unwrap_or_else(|| operation_semantic("client is not connected"));
    };
    let request = match (
        canonical_code_sha256(),
        canonical_limits(),
    ) {
        (Ok(code_sha256), Ok(limits)) => domain::CreateActorRequest::new(
            code_sha256,
            "eu".into(),
            vec![],
            limits,
            vec![],
            "create-cxx".into(),
        ),
        (Err(error), _) | (_, Err(error)) => return operation_semantic(error),
    };
    let request = match request {
        Ok(request) => request,
        Err(error) => return operation_semantic(error.to_string()),
    };
    match block_on_client(facade, client.create_actor(&request)) {
        Ok(response) => observation_operation_result(response.actor()),
        Err(error) => operation_error(error),
    }
}

fn actors_update_actor(facade: &ActorsClient) -> Box<ActorOperationResult> {
    let Some(client) = facade.inner.as_ref() else {
        return operation_semantic("client is not connected");
    };
    let request = match (
        canonical_actor_id(),
        canonical_code_sha256(),
        canonical_limits(),
    ) {
        (Ok(actor_id), Ok(code_sha256), Ok(limits)) => domain::UpdateActorRequest::new(
            actor_id,
            code_sha256,
            vec![],
            limits,
            0,
            "update-cxx".into(),
        ),
        (Err(error), _, _) | (_, Err(error), _) | (_, _, Err(error)) => {
            return operation_semantic(error)
        }
    };
    let request = match request {
        Ok(request) => request,
        Err(error) => return operation_semantic(error.to_string()),
    };
    match block_on_client(facade, client.update_actor(&request)) {
        Ok(response) => observation_operation_result(response.actor()),
        Err(error) => operation_error(error),
    }
}

fn actors_inspect_actor(facade: &ActorsClient) -> Box<ActorOperationResult> {
    let Some(client) = facade.inner.as_ref() else {
        return operation_semantic("client is not connected");
    };
    let request = match canonical_actor_id() {
        Ok(actor_id) => domain::InspectActorRequest::new(actor_id),
        Err(error) => return operation_semantic(error),
    };
    match block_on_client(facade, client.inspect_actor(&request)) {
        Ok(response) => observation_operation_result(response.actor()),
        Err(error) => operation_error(error),
    }
}

fn actors_add_subscription(facade: &ActorsClient) -> Box<ActorOperationResult> {
    let Some(client) = facade.inner.as_ref() else {
        return operation_semantic("client is not connected");
    };
    let request = match (
        canonical_actor_id(),
        canonical_subscription(),
    ) {
        (Ok(actor_id), Ok(subscription)) => domain::AddSubscriptionRequest::new(
            actor_id,
            subscription,
            "subscribe-cxx".into(),
        ),
        (Err(error), _) | (_, Err(error)) => return operation_semantic(error),
    };
    let request = match request {
        Ok(request) => request,
        Err(error) => return operation_semantic(error.to_string()),
    };
    match block_on_client(facade, client.add_subscription(&request)) {
        Ok(response) => observation_operation_result(response.actor()),
        Err(error) => operation_error(error),
    }
}

fn actors_remove_subscription(facade: &ActorsClient) -> Box<ActorOperationResult> {
    let Some(client) = facade.inner.as_ref() else {
        return operation_semantic("client is not connected");
    };
    let request = match canonical_actor_id() {
        Ok(actor_id) => domain::RemoveSubscriptionRequest::new(
            actor_id,
            "input".into(),
            "remove-cxx".into(),
        ),
        Err(error) => return operation_semantic(error),
    };
    match block_on_client(facade, client.remove_subscription(&request)) {
        Ok(response) => observation_operation_result(response.actor()),
        Err(error) => operation_error(error),
    }
}

fn actors_resume_subscription(facade: &ActorsClient) -> Box<ActorOperationResult> {
    let Some(client) = facade.inner.as_ref() else {
        return operation_semantic("client is not connected");
    };
    let request = match canonical_actor_id() {
        Ok(actor_id) => domain::ResumeSubscriptionRequest::new(
            actor_id,
            "input".into(),
            "resume-cxx".into(),
        ),
        Err(error) => return operation_semantic(error),
    };
    match block_on_client(facade, client.resume_subscription(&request)) {
        Ok(response) => observation_operation_result(response.actor()),
        Err(error) => operation_error(error),
    }
}

fn actors_checkpoint_actor(facade: &ActorsClient) -> Box<ActorOperationResult> {
    let Some(client) = facade.inner.as_ref() else {
        return operation_semantic("client is not connected");
    };
    let request = match canonical_actor_id() {
        Ok(actor_id) => domain::CheckpointActorRequest::new(actor_id, "checkpoint-a".into()),
        Err(error) => return operation_semantic(error),
    };
    match block_on_client(facade, client.checkpoint_actor(&request)) {
        Ok(response) => observation_operation_result(response.actor()),
        Err(error) => operation_error(error),
    }
}

fn actors_invoke_actor(facade: &ActorsClient) -> Box<ActorOperationResult> {
    let Some(client) = facade.inner.as_ref() else {
        return operation_semantic("client is not connected");
    };
    let request = match canonical_actor_id() {
        Ok(actor_id) => domain::InvokeActorRequest::new(
            actor_id,
            "POST".into(),
            "/".into(),
            br#"{}"#.to_vec(),
            vec![domain::Header {
                name: "content-type".into(),
                value: "application/json".into(),
            }],
        ),
        Err(error) => return operation_semantic(error),
    };
    match block_on_client(facade, client.invoke_actor(&request)) {
        Ok(response) => invoke_operation_result(&response),
        Err(error) => operation_error(error),
    }
}

fn actors_inspect_actor_with_cancel(
    facade: &ActorsClient,
    operation: &ActorsOperation,
) -> Box<ActorOperationResult> {
    let Some(client) = facade.inner.as_ref() else {
        return operation_semantic("client is not connected");
    };
    let request = match canonical_actor_id() {
        Ok(actor_id) => domain::InspectActorRequest::new(actor_id),
        Err(error) => return operation_semantic(error),
    };
    match block_on_client(facade, client::run_with_cancellation(
        client.inspect_actor(&request),
        Some(operation.cancellation.clone()),
    )) {
        Ok(response) => observation_operation_result(response.actor()),
        Err(error) => operation_error(error),
    }
}

// Keep the operation result opaque across CXX. These accessors expose only the
// assertions needed by the qualification consumer; the Rust result remains
// the sole owner of response fields and their projection rules.
fn actor_operation_result_empty() -> Box<ActorOperationResult> {
    empty_operation_result()
}

fn actor_operation_ok(result: &ActorOperationResult) -> bool {
    result.ok
}

fn actor_operation_error(result: &ActorOperationResult) -> ffi::ErrorKind {
    result.error
}

fn actor_operation_message(result: &ActorOperationResult) -> String {
    result.message.clone()
}

fn actor_operation_actor_id(result: &ActorOperationResult) -> String {
    result.actor_id.clone()
}

fn actor_operation_home_region(result: &ActorOperationResult) -> String {
    result.home_region.clone()
}

fn actor_operation_active(result: &ActorOperationResult) -> bool {
    result.active
}

fn actor_operation_subscriptions_empty(result: &ActorOperationResult) -> bool {
    result.subscriptions_empty
}

fn actor_operation_configuration_revision(result: &ActorOperationResult) -> u64 {
    result.configuration_revision
}

fn actor_operation_checkpoint_epoch(result: &ActorOperationResult) -> u64 {
    result.checkpoint_epoch
}

fn actor_operation_has_checkpoint(result: &ActorOperationResult) -> bool {
    result.has_checkpoint
}

fn actor_operation_status(result: &ActorOperationResult) -> u32 {
    result.status
}

fn actor_operation_has_location_header(result: &ActorOperationResult) -> bool {
    result.has_location_header
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

fn actors_operation_cancel(operation: &ActorsOperation) {
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
