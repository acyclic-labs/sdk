//! Shared authenticated Actors client over a tonic transport.
//!
//! Native callers obtain their transport from [`crate::grpc::connect`].
//! Browser callers use the maintained `tonic-web-wasm-client` gRPC-Web
//! transport. Operation methods below are shared for both transports.

use crate::domain;
use crate::wire;
use prost::Message;
use std::future::Future;
use tokio_util::sync::CancellationToken;
use tonic::codegen::{Body, Bytes, StdError};

/// Maximum encoded request or response accepted by the Actors client.
pub const MAX_MESSAGE_BYTES: usize = 16 * 1024 * 1024;

/// Error returned by the platform Actors client.
#[derive(Debug, thiserror::Error)]
pub enum Error {
    /// Endpoint, credential, or caller-supplied trust configuration is invalid.
    #[error("Actors client configuration failed: {0}")]
    Configuration(String),
    /// A selected transport could not be constructed or reached.
    #[error("Actors transport failure: {0}")]
    Transport(String),
    /// The service rejected an operation and may have supplied semantic detail.
    #[error("Actors service failure")]
    Service {
        /// Numeric gRPC status code.
        grpc_code: i32,
        /// Rust-owned semantic error detail, when present.
        detail: Option<wire::Error>,
    },
    /// The request failed the canonical Rust-owned admission validator.
    #[error("Actors request failed canonical contract validation: {0}")]
    Contract(#[from] crate::ContractError),
    /// The wire payload could not be admitted as the typed Actors model.
    #[error("Actors semantic contract rejected the payload: {0}")]
    Semantic(#[from] domain::DomainError),
    /// The caller cancelled the operation before the transport completed.
    #[error("Actors operation cancelled")]
    Cancelled,
}

impl Error {
    /// Returns the stable cross-platform error code exposed by SDK bridges.
    #[must_use]
    pub const fn code_name(&self) -> &'static str {
        match self {
            Self::Configuration(_) => "invalid_argument",
            Self::Transport(_) => "unavailable",
            Self::Service { grpc_code, .. } => Self::grpc_code_name(*grpc_code),
            Self::Contract(error) => error.code_name(),
            Self::Semantic(error) => error.code_name(),
            Self::Cancelled => "cancelled",
        }
    }

    /// Maps a gRPC status number to the canonical SDK error code.
    #[must_use]
    pub const fn grpc_code_name(code: i32) -> &'static str {
        match code {
            0 => "ok",
            1 => "cancelled",
            3 => "invalid_argument",
            4 => "deadline_exceeded",
            5 => "not_found",
            6 => "already_exists",
            7 => "permission_denied",
            8 => "resource_exhausted",
            9 => "failed_precondition",
            10 => "aborted",
            11 => "out_of_range",
            12 => "unimplemented",
            13 => "internal",
            14 => "unavailable",
            15 => "data_loss",
            16 => "unauthenticated",
            _ => "unknown",
        }
    }
}

impl From<std::convert::Infallible> for Error {
    fn from(value: std::convert::Infallible) -> Self {
        match value {}
    }
}

/// Alias used by connection helpers.
pub type ConnectError = Error;

type GeneratedClient<T> = wire::actors_service_client::ActorsServiceClient<T>;

#[allow(
    clippy::needless_pass_by_value,
    reason = "tonic map_err supplies owned status values"
)]
fn service_error(status: tonic::Status) -> Error {
    Error::Service {
        grpc_code: status.code() as i32,
        detail: if status.details().is_empty() {
            None
        } else {
            wire::Error::decode(status.details()).ok()
        },
    }
}

fn validate_none<T>(_: &T) -> Result<(), crate::ContractError> {
    Ok(())
}

#[cfg(target_arch = "wasm32")]
#[derive(Clone)]
struct BrowserBearerAuth(tonic::metadata::MetadataValue<tonic::metadata::Ascii>);

#[cfg(target_arch = "wasm32")]
impl tonic::service::Interceptor for BrowserBearerAuth {
    fn call(
        &mut self,
        mut request: tonic::Request<()>,
    ) -> Result<tonic::Request<()>, tonic::Status> {
        request
            .metadata_mut()
            .insert("authorization", self.0.clone());
        Ok(request)
    }
}

#[cfg(target_arch = "wasm32")]
type BrowserClient = GeneratedClient<
    tonic::codegen::InterceptedService<tonic_web_wasm_client::Client, BrowserBearerAuth>,
>;

#[cfg(any(target_arch = "wasm32", test))]
fn browser_transport_base_url(endpoint: &str) -> String {
    endpoint.trim_end_matches('/').to_owned()
}

#[cfg(not(target_arch = "wasm32"))]
enum Backend {
    Grpc(crate::grpc::Client),
}

#[cfg(target_arch = "wasm32")]
enum Backend {
    GrpcWeb(BrowserClient),
}

/// Connected Actors client whose operation methods are shared by all targets.
pub struct Client {
    inner: Backend,
}

impl Client {
    /// Returns `grpc` for native transport and `grpc-web` for browser transport.
    #[must_use]
    pub const fn transport(&self) -> &'static str {
        match &self.inner {
            #[cfg(not(target_arch = "wasm32"))]
            Backend::Grpc(_) => "grpc",
            #[cfg(target_arch = "wasm32")]
            Backend::GrpcWeb(_) => "grpc-web",
        }
    }
}

#[cfg(target_arch = "wasm32")]
impl Client {
    /// Constructs a browser client synchronously over the maintained gRPC-Web transport.
    pub fn from_browser(endpoint: &str, token: &str) -> Result<Self, Error> {
        let endpoint = url::Url::parse(endpoint).map_err(|_| {
            Error::Configuration(
                "Actors endpoint must be a valid HTTPS URL; HTTP is limited to loopback tests"
                    .into(),
            )
        })?;
        let host = endpoint.host_str().ok_or_else(|| {
            Error::Configuration(
                "Actors endpoint must include a host; HTTP is limited to loopback tests".into(),
            )
        })?;
        let loopback = host.eq_ignore_ascii_case("localhost")
            || host
                .parse::<std::net::IpAddr>()
                .is_ok_and(|address| address.is_loopback());
        let valid_scheme = match endpoint.scheme() {
            "https" => true,
            "http" => loopback,
            _ => false,
        };
        if !valid_scheme
            || !endpoint.username().is_empty()
            || endpoint.password().is_some()
            || endpoint.query().is_some()
            || endpoint.fragment().is_some()
        {
            return Err(Error::Configuration(
                "Actors endpoint must use HTTPS; HTTP is limited to loopback tests and userinfo, query, and fragment are forbidden".into(),
            ));
        }
        if !crate::valid_token(token) {
            return Err(Error::Configuration(
                "invalid Actors bearer credential".into(),
            ));
        }
        let mut authorization: tonic::metadata::MetadataValue<tonic::metadata::Ascii> =
            format!("Bearer {token}")
                .parse()
                .map_err(|_| Error::Configuration("invalid Actors bearer credential".into()))?;
        authorization.set_sensitive(true);
        let transport =
            tonic_web_wasm_client::Client::new(browser_transport_base_url(endpoint.as_str()));
        let inner = wire::actors_service_client::ActorsServiceClient::with_interceptor(
            transport,
            BrowserBearerAuth(authorization),
        )
        .max_decoding_message_size(MAX_MESSAGE_BYTES)
        .max_encoding_message_size(MAX_MESSAGE_BYTES);
        Ok(Self {
            inner: Backend::GrpcWeb(inner),
        })
    }
}

#[cfg(test)]
mod browser_transport_tests {
    #[test]
    fn browser_base_url_has_one_join_boundary() {
        for (input, expected) in [
            ("https://actors.example", "https://actors.example"),
            ("https://actors.example/", "https://actors.example"),
            ("https://actors.example/api/", "https://actors.example/api"),
            ("http://localhost:4317/", "http://localhost:4317"),
        ] {
            assert_eq!(super::browser_transport_base_url(input), expected);
        }
    }
}

/// Connect using the target's canonical authenticated gRPC transport.
pub async fn connect(endpoint: &str, token: &str) -> Result<Client, ConnectError> {
    connect_with_ca_certificate(endpoint, token, None).await
}

/// Connect with an optional caller-pinned private CA on native targets.
#[allow(
    clippy::needless_return,
    reason = "target-specific branches return distinct native and browser backends"
)]
pub async fn connect_with_ca_certificate(
    endpoint: &str,
    token: &str,
    ca: Option<&[u8]>,
) -> Result<Client, ConnectError> {
    #[cfg(not(target_arch = "wasm32"))]
    {
        let inner = crate::grpc::connect_with_ca_certificate(endpoint, token, ca)
            .await
            .map_err(|error| match error {
                crate::grpc::ConnectError::InsecureEndpoint => {
                    Error::Configuration("Actors endpoints must use https".into())
                }
                crate::grpc::ConnectError::InvalidCredential => {
                    Error::Configuration("invalid Actors bearer credential".into())
                }
                crate::grpc::ConnectError::InvalidCaCertificate => {
                    Error::Configuration("invalid Actors private CA certificate".into())
                }
                crate::grpc::ConnectError::Transport(error) => Error::Transport(error.to_string()),
            })?;
        return Ok(Client {
            inner: Backend::Grpc(inner),
        });
    }

    #[cfg(target_arch = "wasm32")]
    {
        if ca.is_some() {
            return Err(Error::Configuration(
                "caller-provided CA certificates are unsupported in browser builds".into(),
            ));
        }
        return Client::from_browser(endpoint, token);
    }
}

/// Runs an Actors operation until it completes or its monotonic cancellation
/// token is cancelled. A cancelled token stays cancelled; callers that begin
/// another operation must provide a fresh token.
pub async fn run_with_cancellation<T, Operation>(
    operation: Operation,
    cancellation: Option<CancellationToken>,
) -> Result<T, Error>
where
    Operation: Future<Output = Result<T, Error>>,
{
    let Some(cancellation) = cancellation else {
        return operation.await;
    };
    if cancellation.is_cancelled() {
        return Err(Error::Cancelled);
    }
    tokio::select! {
        biased;
        _ = cancellation.cancelled() => Err(Error::Cancelled),
        result = operation => result,
    }
}

macro_rules! operation {
    ($name:ident, $wire_name:ident, $request:ty, $wire_request:ty, $response:ty, $wire_response:ty, $validate:path) => {
        async fn $wire_name<T>(
            client: &mut GeneratedClient<T>,
            request: &$wire_request,
        ) -> Result<$wire_response, Error>
        where
            T: tonic::client::GrpcService<tonic::body::Body> + Clone,
            T::Error: Into<StdError>,
            T::ResponseBody: Body<Data = Bytes> + Send + 'static,
            <T::ResponseBody as Body>::Error: Into<StdError> + Send,
        {
            $validate(request).map_err(Error::Contract)?;
            client
                .$name(request.clone())
                .await
                .map(tonic::Response::into_inner)
                .map_err(service_error)
        }

        impl Client {
            /// Executes the strongly typed semantic operation for native and WASM transports.
            pub async fn $name(&self, request: &$request) -> Result<$response, Error> {
                let wire_request: $wire_request = request.clone().into();
                #[cfg(not(target_arch = "wasm32"))]
                {
                    let mut client = match &self.inner {
                        Backend::Grpc(client) => client.clone(),
                    };
                    let response = $wire_name(&mut client, &wire_request).await?;
                    Ok(response.try_into()?)
                }
                #[cfg(target_arch = "wasm32")]
                {
                    let mut client = match &self.inner {
                        Backend::GrpcWeb(client) => client.clone(),
                    };
                    let response = $wire_name(&mut client, &wire_request).await?;
                    Ok(response.try_into()?)
                }
            }
        }
    };
}

operation!(
    create_actor,
    wire_create_actor,
    domain::CreateActorRequest,
    wire::CreateActorRequest,
    domain::CreateActorResponse,
    wire::CreateActorResponse,
    crate::validate_create
);
operation!(
    update_actor,
    wire_update_actor,
    domain::UpdateActorRequest,
    wire::UpdateActorRequest,
    domain::UpdateActorResponse,
    wire::UpdateActorResponse,
    crate::validate_update
);
operation!(
    inspect_actor,
    wire_inspect_actor,
    domain::InspectActorRequest,
    wire::InspectActorRequest,
    domain::InspectActorResponse,
    wire::InspectActorResponse,
    validate_none
);

operation!(
    add_subscription,
    wire_add_subscription,
    domain::AddSubscriptionRequest,
    wire::AddSubscriptionRequest,
    domain::AddSubscriptionResponse,
    wire::AddSubscriptionResponse,
    crate::validate_add_subscription
);
operation!(
    remove_subscription,
    wire_remove_subscription,
    domain::RemoveSubscriptionRequest,
    wire::RemoveSubscriptionRequest,
    domain::RemoveSubscriptionResponse,
    wire::RemoveSubscriptionResponse,
    validate_none
);
operation!(
    resume_subscription,
    wire_resume_subscription,
    domain::ResumeSubscriptionRequest,
    wire::ResumeSubscriptionRequest,
    domain::ResumeSubscriptionResponse,
    wire::ResumeSubscriptionResponse,
    validate_none
);
operation!(
    checkpoint_actor,
    wire_checkpoint_actor,
    domain::CheckpointActorRequest,
    wire::CheckpointActorRequest,
    domain::CheckpointActorResponse,
    wire::CheckpointActorResponse,
    validate_none
);
operation!(
    invoke_actor,
    wire_invoke_actor,
    domain::InvokeActorRequest,
    wire::InvokeActorRequest,
    domain::InvokeActorResponse,
    wire::InvokeActorResponse,
    validate_none
);

#[cfg(test)]
mod tests {
    use super::*;
    use std::{
        future::Future,
        pin::Pin,
        sync::{
            Arc,
            atomic::{AtomicBool, AtomicUsize, Ordering},
        },
        task::{Context, Poll},
    };

    struct MustNotPoll {
        polls: Arc<AtomicUsize>,
    }

    impl Future for MustNotPoll {
        type Output = Result<(), Error>;

        fn poll(self: Pin<&mut Self>, _: &mut Context<'_>) -> Poll<Self::Output> {
            self.polls.fetch_add(1, Ordering::SeqCst);
            Poll::Ready(Ok(()))
        }
    }

    #[tokio::test]
    async fn pre_cancelled_operation_is_never_polled() {
        let cancellation = CancellationToken::new();
        cancellation.cancel();
        let polls = Arc::new(AtomicUsize::new(0));

        let result = run_with_cancellation(
            MustNotPoll {
                polls: Arc::clone(&polls),
            },
            Some(cancellation),
        )
        .await;

        assert!(matches!(result, Err(Error::Cancelled)));
        assert_eq!(polls.load(Ordering::SeqCst), 0);
    }

    struct CancelOnPoll {
        cancellation: CancellationToken,
        dropped: Arc<AtomicBool>,
    }

    impl Future for CancelOnPoll {
        type Output = Result<(), Error>;

        fn poll(self: Pin<&mut Self>, _: &mut Context<'_>) -> Poll<Self::Output> {
            self.cancellation.cancel();
            Poll::Pending
        }
    }

    impl Drop for CancelOnPoll {
        fn drop(&mut self) {
            self.dropped.store(true, Ordering::SeqCst);
        }
    }

    #[tokio::test]
    async fn cancellation_drops_a_pending_operation() {
        let cancellation = CancellationToken::new();
        let dropped = Arc::new(AtomicBool::new(false));

        let result = run_with_cancellation(
            CancelOnPoll {
                cancellation: cancellation.clone(),
                dropped: Arc::clone(&dropped),
            },
            Some(cancellation),
        )
        .await;

        assert!(matches!(result, Err(Error::Cancelled)));
        assert!(dropped.load(Ordering::SeqCst));
    }

    #[test]
    fn service_error_preserves_raw_status_and_detail_codes() {
        for (status_code, detail_code) in [
            (tonic::Code::PermissionDenied, 0),
            (tonic::Code::Unknown, 99),
        ] {
            let detail = wire::Error {
                code: detail_code,
                message: format!("detail-{detail_code}"),
            };
            let status = tonic::Status::with_details(
                status_code,
                "service rejected request",
                detail.encode_to_vec().into(),
            );

            match service_error(status) {
                Error::Service {
                    grpc_code,
                    detail: Some(actual),
                } => {
                    assert_eq!(grpc_code, status_code as i32);
                    assert_eq!(actual.code, detail_code);
                    assert_eq!(actual.message, format!("detail-{detail_code}"));
                }
                other => panic!("expected structured service error, got {other:?}"),
            }
        }
    }

    #[test]
    fn service_error_ignores_empty_or_malformed_details() {
        let statuses = [
            tonic::Status::unknown("empty details"),
            tonic::Status::with_details(
                tonic::Code::Unavailable,
                "malformed details",
                vec![0xff, 0x00].into(),
            ),
        ];

        for status in statuses {
            let expected_code = status.code() as i32;
            match service_error(status) {
                Error::Service { grpc_code, detail } => {
                    assert_eq!(grpc_code, expected_code);
                    assert!(detail.is_none());
                }
                other => panic!("expected service error, got {other:?}"),
            }
        }
    }

    #[test]
    fn error_code_names_are_shared_by_native_and_browser_bridges() {
        for (code, name) in [
            (0, "ok"),
            (1, "cancelled"),
            (3, "invalid_argument"),
            (4, "deadline_exceeded"),
            (5, "not_found"),
            (6, "already_exists"),
            (7, "permission_denied"),
            (8, "resource_exhausted"),
            (9, "failed_precondition"),
            (10, "aborted"),
            (11, "out_of_range"),
            (12, "unimplemented"),
            (13, "internal"),
            (14, "unavailable"),
            (15, "data_loss"),
            (16, "unauthenticated"),
            (17, "unknown"),
        ] {
            let error = Error::Service {
                grpc_code: code,
                detail: None,
            };
            assert_eq!(error.code_name(), name);
        }
        assert_eq!(
            Error::Configuration(String::new()).code_name(),
            "invalid_argument"
        );
        assert_eq!(Error::Transport(String::new()).code_name(), "unavailable");
        assert_eq!(
            Error::Contract(crate::ContractError::LimitExceeded).code_name(),
            "limit_exceeded"
        );
        assert_eq!(
            Error::Semantic(domain::DomainError::InvalidBinding).code_name(),
            "invalid_binding"
        );
        assert_eq!(Error::Cancelled.code_name(), "cancelled");
    }
}
