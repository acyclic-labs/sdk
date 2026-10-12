//! Shared authenticated Actors client over a tonic transport.

use crate::domain;
use crate::wire;
use prost::Message;
use std::future::Future;
use tokio_util::sync::CancellationToken;

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
    #[error("{message}")]
    Service {
        /// Numeric gRPC status code.
        grpc_code: i32,
        /// Original gRPC status message.
        message: String,
        /// Exact gRPC status detail bytes, including malformed payloads.
        raw_details: Vec<u8>,
        /// Rust-owned semantic error detail, when present.
        detail: Option<wire::Error>,
    },
    /// The wire payload could not be admitted as the typed Actors model.
    #[error("Actors semantic contract rejected the payload: {0}")]
    Semantic(#[from] domain::DomainError),
    /// The caller cancelled the operation before the transport completed.
    #[error("Actors operation cancelled")]
    Cancelled,
}

/// Lossless Rust-owned diagnostic projection shared by both JavaScript bridges.
#[derive(Clone, Debug, Default, serde::Serialize, ts_rs::TS)]
#[serde(rename_all = "camelCase")]
#[ts(rename_all = "camelCase", optional_fields = nullable)]
pub struct ErrorMetadata {
    /// Stable SDK error category.
    pub code: String,
    /// Original status message or canonical admission diagnostic.
    pub message: String,
    /// Numeric gRPC status reported by the maintained transport.
    pub grpc_code: Option<i32>,
    /// Canonical gRPC category.
    pub grpc_name: Option<String>,
    /// Decoded int32 service code, including future values unknown to this SDK version.
    pub service_code: Option<i32>,
    /// Decoded service diagnostic, separate from the gRPC status message.
    pub service_message: Option<String>,
    /// Canonical admission category, when applicable.
    pub contract_code: Option<String>,
    /// Exact detail bytes for service failures, including empty details.
    #[serde(with = "serde_bytes")]
    #[ts(optional, type = "Uint8Array | null")]
    pub raw_details: Option<Vec<u8>>,
}

impl Error {
    /// Project diagnostics without discarding transport or service information.
    #[must_use]
    pub fn metadata(self) -> ErrorMetadata {
        let code = self.code_name().to_owned();
        let mut metadata = ErrorMetadata {
            code: code.clone(),
            ..ErrorMetadata::default()
        };
        match self {
            Self::Configuration(message) | Self::Transport(message) => {
                metadata.message = message;
            }
            Self::Service {
                grpc_code,
                detail,
                raw_details,
                message,
            } => {
                metadata.message = message;
                metadata.grpc_code = Some(grpc_code);
                metadata.grpc_name = Some(code);
                metadata.service_code = detail.as_ref().map(|value| value.code);
                metadata.service_message = detail.map(|value| value.message);
                metadata.raw_details = Some(raw_details);
            }
            Self::Cancelled => {
                metadata.message = String::from("Actors operation cancelled");
                metadata.grpc_code = Some(1);
                metadata.grpc_name = Some(code);
            }
            Self::Semantic(domain::DomainError::Contract(error)) => {
                metadata.message = error.to_string();
                metadata.contract_code = Some(code);
            }
            Self::Semantic(error) => metadata.message = error.to_string(),
        }
        metadata
    }

    /// Returns the stable cross-platform error code exposed by SDK bridges.
    #[must_use]
    pub const fn code_name(&self) -> &'static str {
        match self {
            Self::Configuration(_) => "invalid_argument",
            Self::Transport(_) => "unavailable",
            Self::Service { grpc_code, .. } => Self::grpc_code_name(*grpc_code),
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

#[allow(
    clippy::needless_pass_by_value,
    reason = "tonic map_err supplies owned status values"
)]
fn service_error(status: tonic::Status) -> Error {
    Error::Service {
        grpc_code: status.code() as i32,
        message: status.message().to_owned(),
        raw_details: status.details().to_vec(),
        detail: if status.details().is_empty() {
            None
        } else {
            wire::Error::decode(status.details()).ok()
        },
    }
}

/// Connected Actors client whose operation methods are shared by all targets.
pub struct Client {
    #[cfg(not(target_arch = "wasm32"))]
    inner: crate::grpc::Client,
    #[cfg(target_arch = "wasm32")]
    inner: wire::actors_service_client::ActorsServiceClient<
        tonic::codegen::InterceptedService<tonic_web_wasm_client::Client, acyclic_workers::BearerAuth>,
    >,
}

impl Client {
    /// Returns `grpc` for native transport and `grpc-web` for browser transport.
    #[must_use]
    pub const fn transport(&self) -> &'static str {
        if cfg!(target_arch = "wasm32") { "grpc-web" } else { "grpc" }
    }
}

/// Connect trusting exclusively the supplied PEM CA on native targets, or platform roots if absent.
pub async fn connect(
    endpoint: &str,
    token: &str,
    ca: Option<&[u8]>,
) -> Result<Client, Error> {
    #[cfg(not(target_arch = "wasm32"))]
    {
        let inner = crate::grpc::connect(endpoint, token, ca)
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
                crate::grpc::ConnectError::PlatformVerifier(error) => {
                    Error::Transport(error.to_string())
                }
                crate::grpc::ConnectError::Transport(error) => Error::Transport(format!("{error:?}")),
            })?;
        Ok(Client { inner })
    }

    #[cfg(target_arch = "wasm32")]
    {
        if ca.is_some() {
            return Err(Error::Configuration(
                "caller-provided CA certificates are unsupported in browser builds".into(),
            ));
        }
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
        let authorization = acyclic_workers::BearerAuth::new(token)
            .map_err(|()| Error::Configuration("invalid Actors bearer credential".into()))?;
        let transport = tonic_web_wasm_client::Client::new_with_options(
            endpoint.as_str().trim_end_matches('/').to_owned(),
            tonic_web_wasm_client::options::FetchOptions::new()
                .redirect(tonic_web_wasm_client::options::Redirect::Error)
                .response_limits(
                    tonic_web_wasm_client::limits::UnaryResponseLimits::new(MAX_MESSAGE_BYTES)
                        .ok_or_else(|| {
                            Error::Configuration("response budget exceeds platform size".into())
                        })?,
                ),
        );
        let inner = wire::actors_service_client::ActorsServiceClient::with_interceptor(
            transport,
            authorization,
        )
        .max_decoding_message_size(MAX_MESSAGE_BYTES)
        .max_encoding_message_size(MAX_MESSAGE_BYTES);
        Ok(Client { inner })
    }
}

/// Runs an Actors operation until it completes or its monotonic cancellation token is cancelled.
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
    tokio::select! {
        biased;
        _ = cancellation.cancelled() => Err(Error::Cancelled),
        result = operation => result,
    }
}

macro_rules! operations {
    ($apply:ident) => { $apply! {
        ("CreateActor", create_actor, CreateActorRequest, CreateActorResponse),
        ("UpdateActor", update_actor, UpdateActorRequest, UpdateActorResponse),
        ("InspectActor", inspect_actor, InspectActorRequest, InspectActorResponse),
        ("AddSubscription", add_subscription, AddSubscriptionRequest, AddSubscriptionResponse),
        ("RemoveSubscription", remove_subscription, RemoveSubscriptionRequest, RemoveSubscriptionResponse),
        ("ResumeSubscription", resume_subscription, ResumeSubscriptionRequest, ResumeSubscriptionResponse),
        ("CheckpointActor", checkpoint_actor, CheckpointActorRequest, CheckpointActorResponse),
        ("InvokeActor", invoke_actor, InvokeActorRequest, InvokeActorResponse),
        ("DeleteActor", delete_actor, DeleteActorRequest, DeleteActorResponse)
    } };
}

macro_rules! client_operations {
    ($(($name:literal, $method:ident, $input:ident, $output:ident)),* $(,)?) => {
        impl Client {
            $(
            #[doc = concat!("Executes the semantic ", $name, " operation.")]
            pub async fn $method(&self, request: &domain::$input) -> Result<domain::$output, Error> {
                let input = wire::$input::from(request.clone());
                let response = self.inner.clone().$method(input).await
                    .map(tonic::Response::into_inner).map_err(service_error)?;
                Ok(response.try_into()?)
            }
            )*

            /// Dispatches bounded protobuf bytes through the semantic client.
            pub async fn call(&self, method: &str, bytes: &[u8]) -> Result<Vec<u8>, Error> {
                if bytes.len() > MAX_MESSAGE_BYTES {
                    return Err(Error::Configuration("request exceeds message ceiling".into()));
                }
                match method {
                    $( $name => {
                        let input = wire::$input::decode(bytes)
                            .map_err(|error| Error::Configuration(error.to_string()))?;
                        let input = domain::$input::try_from(input)
                            .map_err(|error| Error::Configuration(error.to_string()))?;
                        let output = self.$method(&input).await?;
                        Ok(wire::$output::from(output).encode_to_vec())
                    }, )*
                    _ => Err(Error::Configuration("unknown Actors service method".into())),
                }
            }
        }
    };
}
operations!(client_operations);

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
                    message,
                    raw_details,
                } => {
                    assert_eq!(message, "service rejected request");
                    assert_eq!(raw_details, detail.encode_to_vec());
                    assert_eq!(grpc_code, status_code as i32);
                    assert_eq!(actual.code, detail_code);
                    assert_eq!(actual.message, format!("detail-{detail_code}"));
                }
                other => panic!("expected structured service error, got {other:?}"),
            }
        }
    }

    #[test]
    fn service_metadata_preserves_status_message_and_lossless_detail_bytes() {
        let decoded = wire::Error {
            code: 99,
            message: String::from("future service detail"),
        }
        .encode_to_vec();
        for bytes in [decoded, Vec::new(), vec![255, 0]] {
            let status = tonic::Status::with_details(
                tonic::Code::PermissionDenied,
                "original grpc diagnostic",
                bytes.clone().into(),
            );
            let error = service_error(status);
            let metadata = error.metadata();
            assert_eq!(metadata.code, "permission_denied");
            assert_eq!(metadata.message, "original grpc diagnostic");
            assert_eq!(metadata.grpc_code, Some(7));
            assert_eq!(metadata.raw_details, Some(bytes.clone()));
            if bytes.first() == Some(&8) {
                assert_eq!(metadata.service_code, Some(99));
                assert_eq!(
                    metadata.service_message.as_deref(),
                    Some("future service detail")
                );
            } else {
                assert!(metadata.service_code.is_none());
                assert!(metadata.service_message.is_none());
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
            match service_error(status.clone()) {
                Error::Service {
                    grpc_code,
                    detail,
                    message,
                    raw_details,
                } => {
                    assert_eq!(message, status.message());
                    assert_eq!(raw_details, status.details());
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
                message: String::new(),
                raw_details: Vec::new(),
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
            Error::Semantic(crate::ContractError::LimitExceeded.into()).code_name(),
            "limit_exceeded"
        );
        assert_eq!(
            Error::Semantic(domain::DomainError::InvalidBinding).code_name(),
            "invalid_binding"
        );
        assert_eq!(Error::Cancelled.code_name(), "cancelled");
    }
}
