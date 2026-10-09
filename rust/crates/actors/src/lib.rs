#![doc = include_str!("../README.md")]

use std::collections::HashSet;

/// Rust-owned semantic projections for generated SDK metadata.
pub use contract::domain;

/// Authenticated operations over the platform's Rust-owned transport.
pub mod client;

/// Rust-owned Actors contract declarations and schema renderer.
pub mod contract;

/// Native gRPC client for the canonical Actors v1 service.
///
/// The client applies bearer authentication and TLS configuration to every
/// generated RPC. Browser bindings use the Rust-owned gRPC-Web transport through
/// [`client::Client`]; this adapter is unavailable on `wasm32`.
#[cfg(not(target_arch = "wasm32"))]
pub mod grpc;
/// Native HTTP client for the canonical Actors v1 Protobuf JSON service.
///
/// The client exposes the same eight operations with bounded responses and
/// bearer authentication. Browser bindings use the Rust-owned gRPC-Web transport
/// through [`client::Client`]; this adapter is unavailable on `wasm32`.
#[cfg(not(target_arch = "wasm32"))]
pub mod http;

/// Rust-owned Actors v1 wire types and schema metadata.
#[allow(
    clippy::allow_attributes_without_reason,
    clippy::doc_markdown,
    clippy::too_many_lines,
    dead_code,
    missing_docs,
    reason = "tonic emits this module and its generated server dispatch code"
)]
pub mod wire;

/// Canonical version-one descriptor set.
pub const FILE_DESCRIPTOR_SET: &[u8] =
    include_bytes!(concat!(env!("OUT_DIR"), "/acyclic-actors-v1.bin"));
/// Maximum subscriptions on one Actor contract.
pub const MAX_SUBSCRIPTIONS: usize = 64;
/// Maximum named bindings on one Actor contract.
pub const MAX_BINDINGS: usize = 64;

/// Largest bearer credential, in bytes, that an SDK client accepts. It matches the
/// Acyclic platform's maximum bearer (12 KiB), so every platform-issued credential fits.
pub const MAX_BEARER_TOKEN_BYTES: usize = 12 * 1024;

/// Bearer credentials must be nonblank and at most [`MAX_BEARER_TOKEN_BYTES`]; the HTTP and gRPC
/// header parsers additionally reject control characters such as CR, LF, and NUL.
pub(crate) fn valid_token(token: &str) -> bool {
    !token.trim().is_empty()
        && token.len() <= MAX_BEARER_TOKEN_BYTES
        && !token
            .chars()
            .any(|character| matches!(character, '\r' | '\n' | '\0'))
}

/// Idempotency keys are present and bounded consistently across operations.
pub(crate) fn valid_idempotency_key(value: &str) -> bool {
    !value.is_empty() && value.len() <= 256
}

/// Rust-owned route names used by the TypeScript transport generator.
pub const HTTP_ROUTES: &[(&str, &str)] = &[
    ("createActor", "v1/actors/create"),
    ("updateActor", "v1/actors/update"),
    ("inspectActor", "v1/actors/inspect"),
    ("addSubscription", "v1/actors/subscriptions/add"),
    ("removeSubscription", "v1/actors/subscriptions/remove"),
    ("resumeSubscription", "v1/actors/subscriptions/resume"),
    ("checkpointActor", "v1/actors/checkpoint"),
    ("invokeActor", "v1/actors/invoke"),
];

/// Invalid customer-authored Actors request.
#[derive(Clone, Copy, Debug, Eq, PartialEq, thiserror::Error)]
pub enum ContractError {
    /// A required field is absent or malformed.
    #[error("required field is absent or malformed")]
    InvalidArgument,
    /// A collection exceeds its contract bound.
    #[error("Actors v1 collection limit exceeded")]
    LimitExceeded,
    /// A subscription or binding name occurs more than once.
    #[error("duplicate subscription or binding name")]
    DuplicateName,
}

impl ContractError {
    /// Returns the stable error code shared by every SDK bridge.
    #[must_use]
    pub const fn code_name(self) -> &'static str {
        match self {
            Self::InvalidArgument => "invalid_argument",
            Self::LimitExceeded => "limit_exceeded",
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
        && matches!(
            value.start.as_ref().and_then(|start| start.start.as_ref()),
            Some(wire::subscription_start::Start::Cursor(_))
                | Some(wire::subscription_start::Start::CurrentHead(true))
        )
}

/// Validates a customer-authored Actor creation request before admission.
pub fn validate_create(request: &wire::CreateActorRequest) -> Result<(), ContractError> {
    if !domain::valid_code_sha256(&request.code_sha256)
        || request.home_region.is_empty()
        || !valid_idempotency_key(&request.idempotency_key)
        || !request.limits.as_ref().is_some_and(|limits| {
            limits.handler_timeout_millis > 0
                && limits.memory_bytes > 0
                && limits.checkpoint_bytes > 0
        })
    {
        return Err(ContractError::InvalidArgument);
    }
    if request.subscriptions.len() > MAX_SUBSCRIPTIONS || request.bindings.len() > MAX_BINDINGS {
        return Err(ContractError::LimitExceeded);
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
    names.clear();
    for item in &request.bindings {
        if item.name.is_empty() || item.capability.is_empty() || item.resource.is_empty() {
            return Err(ContractError::InvalidArgument);
        }
        if !names.insert(&item.name) {
            return Err(ContractError::DuplicateName);
        }
    }
    Ok(())
}

/// Validates a full compare-and-replace Actor configuration mutation.
/// The service checks checkpoint compatibility or migration before activation.
pub fn validate_update(request: &wire::UpdateActorRequest) -> Result<(), ContractError> {
    if request.actor_id.is_empty()
        || !domain::valid_code_sha256(&request.code_sha256)
        || !valid_idempotency_key(&request.idempotency_key)
        || !request.limits.as_ref().is_some_and(|limits| {
            limits.handler_timeout_millis > 0
                && limits.memory_bytes > 0
                && limits.checkpoint_bytes > 0
        })
    {
        return Err(ContractError::InvalidArgument);
    }
    if request.bindings.len() > MAX_BINDINGS {
        return Err(ContractError::LimitExceeded);
    }
    let mut names = HashSet::new();
    for item in &request.bindings {
        if item.name.is_empty() || item.capability.is_empty() || item.resource.is_empty() {
            return Err(ContractError::InvalidArgument);
        }
        if !names.insert(&item.name) {
            return Err(ContractError::DuplicateName);
        }
    }
    Ok(())
}

/// Validates a newly authored subscription; its cursor may not later be rewound.
pub fn validate_add_subscription(
    request: &wire::AddSubscriptionRequest,
) -> Result<(), ContractError> {
    if request.actor_id.is_empty()
        || !valid_idempotency_key(&request.idempotency_key)
        || !request.subscription.as_ref().is_some_and(subscription)
    {
        return Err(ContractError::InvalidArgument);
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn subscription_start_is_exactly_one_choice() {
        let valid = wire::SubscriptionSpec {
            subscription_id: "events".into(),
            stream_path: "agents/a/events".into(),
            start: Some(wire::SubscriptionStart {
                start: Some(wire::subscription_start::Start::Cursor(0)),
            }),
            placement_anchor: true,
        };
        assert!(subscription(&valid));
        let mut invalid = valid;
        invalid.start = None;
        assert!(!subscription(&invalid));

        let mut create = wire::CreateActorRequest {
            code_sha256: vec![1; 32],
            home_region: "eu".into(),
            bindings: vec![],
            limits: Some(wire::ActorLimits {
                handler_timeout_millis: 1_000,
                memory_bytes: 1024,
                checkpoint_bytes: 1024,
            }),
            subscriptions: vec![wire::SubscriptionSpec {
                subscription_id: "events".into(),
                stream_path: "agents/a/events".into(),
                start: Some(wire::SubscriptionStart {
                    start: Some(wire::subscription_start::Start::Cursor(0)),
                }),
                placement_anchor: true,
            }],
            idempotency_key: "create-a".into(),
        };
        assert_eq!(validate_create(&create), Ok(()));
        let duplicate = create.subscriptions.clone();
        create.subscriptions.extend(duplicate);
        assert_eq!(validate_create(&create), Err(ContractError::DuplicateName));
    }
    #[cfg(not(target_arch = "wasm32"))]
    #[test]
    fn clients_share_endpoint_and_credential_policy() {
        let long = "t".repeat(MAX_BEARER_TOKEN_BYTES + 1);
        for token in ["", " ", "a\r\nb", "a\0b", long.as_str()] {
            assert!(matches!(
                http::Client::new("https://example.test", token, 1),
                Err(http::Error::InvalidArgument)
            ));
        }
        for endpoint in [
            "http://localhost:1",
            "http://127.0.0.2:1",
            "http://[::1]:1",
            "https://example.test",
        ] {
            assert!(http::Client::new(endpoint, &"t".repeat(MAX_BEARER_TOKEN_BYTES), 1).is_ok());
        }
        for endpoint in [
            "http://example.test",
            "http://10.0.0.1",
            "https://u@example.test",
        ] {
            assert!(http::Client::new(endpoint, "t", 1).is_err());
        }
    }

    /// Serves one RPC whose OK headers precede a 50 ms body ending in an UNAVAILABLE trailer.
    #[cfg(not(target_arch = "wasm32"))]
    async fn slow_status_server() -> std::io::Result<std::net::SocketAddr> {
        use std::{future::Future as _, pin::Pin, task::Poll};

        #[derive(Clone)]
        struct SlowStatus;
        impl tonic::server::NamedService for SlowStatus {
            const NAME: &'static str = "acyclic.actors.v1.ActorsService";
        }
        impl tonic::codegen::Service<tonic::codegen::http::Request<tonic::body::Body>> for SlowStatus {
            type Response = tonic::codegen::http::Response<tonic::body::Body>;
            type Error = std::convert::Infallible;
            type Future = std::future::Ready<Result<Self::Response, Self::Error>>;
            fn poll_ready(
                &mut self,
                _: &mut std::task::Context<'_>,
            ) -> Poll<Result<(), Self::Error>> {
                Poll::Ready(Ok(()))
            }
            fn call(
                &mut self,
                _: tonic::codegen::http::Request<tonic::body::Body>,
            ) -> Self::Future {
                let sleep = tokio::time::sleep(std::time::Duration::from_millis(50));
                let body = tonic::body::Body::new(Trailers(Some(Box::pin(sleep))));
                std::future::ready(Ok(tonic::codegen::http::Response::new(body)))
            }
        }
        struct Trailers(Option<Pin<Box<tokio::time::Sleep>>>);
        impl http_body::Body for Trailers {
            type Data = tonic::codegen::Bytes;
            type Error = std::convert::Infallible;
            fn poll_frame(
                mut self: Pin<&mut Self>,
                cx: &mut std::task::Context<'_>,
            ) -> Poll<Option<Result<http_body::Frame<Self::Data>, Self::Error>>> {
                let Some(sleep) = self.0.as_mut() else {
                    return Poll::Ready(None);
                };
                std::task::ready!(sleep.as_mut().poll(cx));
                self.0 = None;
                let mut trailers = tonic::codegen::http::HeaderMap::new();
                trailers.insert("grpc-status", 14.into());
                Poll::Ready(Some(Ok(http_body::Frame::trailers(trailers))))
            }
        }
        let incoming = tonic::transport::server::TcpIncoming::bind(([127, 0, 0, 1], 0).into())?;
        let address = incoming.local_addr()?;
        tokio::spawn(
            tonic::transport::Server::builder()
                .add_service(SlowStatus)
                .serve_with_incoming(incoming),
        );
        Ok(address)
    }

    /// Sends one empty request through a fresh [`grpc::TracedChannel`].
    #[cfg(not(target_arch = "wasm32"))]
    async fn traced_call(
        address: std::net::SocketAddr,
        rpc: &'static str,
    ) -> Result<tonic::codegen::http::Response<tonic::body::Body>, Box<dyn std::error::Error>> {
        use tonic::codegen::Service as _;
        let mut channel = grpc::TracedChannel::new(
            tonic::transport::Endpoint::from_shared(format!("http://{address}"))?.connect_lazy(),
        );
        std::future::poll_fn(|cx| channel.poll_ready(cx)).await?;
        let request = tonic::codegen::http::Request::builder()
            .uri(format!(
                "http://{address}/acyclic.actors.v1.ActorsService/{rpc}"
            ))
            .extension(tonic::GrpcMethod::new(
                "acyclic.actors.v1.ActorsService",
                rpc,
            ))
            .body(tonic::body::Body::empty())?;
        Ok(channel.call(request).await?)
    }

    #[cfg(not(target_arch = "wasm32"))]
    #[tokio::test]
    async fn client_calls_emit_spans_without_credentials() -> Result<(), Box<dyn std::error::Error>>
    {
        use http_body::Body as _;
        use std::pin::Pin;
        use std::sync::{Arc, Mutex};
        use tokio::io::{AsyncReadExt as _, AsyncWriteExt as _};
        use tracing_subscriber::layer::{Context, SubscriberExt as _};

        type Seen = Arc<Mutex<Vec<(&'static str, &'static str, String)>>>;
        struct Capture(Seen);
        struct Fields<'a>(&'static str, &'a Seen);
        impl tracing::field::Visit for Fields<'_> {
            fn record_debug(&mut self, field: &tracing::field::Field, value: &dyn std::fmt::Debug) {
                let value = format!("{value:?}").trim_matches('"').to_owned();
                self.1.lock().unwrap().push((self.0, field.name(), value));
            }
        }
        impl<S> tracing_subscriber::Layer<S> for Capture
        where
            S: tracing::Subscriber + for<'a> tracing_subscriber::registry::LookupSpan<'a>,
        {
            fn on_new_span(
                &self,
                attrs: &tracing::span::Attributes<'_>,
                _: &tracing::span::Id,
                _: Context<'_, S>,
            ) {
                attrs.record(&mut Fields(attrs.metadata().name(), &self.0));
            }
            fn on_record(
                &self,
                id: &tracing::span::Id,
                values: &tracing::span::Record<'_>,
                ctx: Context<'_, S>,
            ) {
                values.record(&mut Fields(ctx.span(id).unwrap().name(), &self.0));
            }
            fn on_close(&self, id: tracing::span::Id, ctx: Context<'_, S>) {
                let name = ctx.span(&id).unwrap().name();
                self.0.lock().unwrap().push((name, "closed", String::new()));
            }
        }
        // A second live dispatcher keeps callsites consulting this test's subscriber.
        let _second = tracing::Dispatch::new(tracing_subscriber::registry());
        let seen = Seen::default();
        let _guard = tracing::subscriber::set_default(
            tracing_subscriber::registry().with(Capture(Arc::clone(&seen))),
        );

        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
        let address = listener.local_addr()?;
        tokio::spawn(async move {
            let (mut http, _) = listener.accept().await?;
            let mut buffer = vec![0; 65_536];
            let _ = http.read(&mut buffer).await?;
            http.write_all(b"HTTP/1.1 503 Service Unavailable\r\ncontent-length: 0\r\n\r\n")
                .await?;
            drop(http);
            // The gRPC connection fails its HTTP/2 handshake.
            drop(listener.accept().await?);
            Ok::<_, std::io::Error>(())
        });
        let client = http::Client::new(&format!("http://{address}"), "secret-token", 1024)?;
        assert!(matches!(
            client
                .create_actor(&wire::CreateActorRequest::default())
                .await,
            Err(http::Error::Service { status: 503, .. })
        ));
        assert!(traced_call(address, "CreateActor").await.is_err());

        // The span stays open while a slow body runs and records a status sent in trailers.
        let address = slow_status_server().await?;
        let mut body = traced_call(address, "GetActor").await?.into_body();
        let grpc_closed = || {
            seen.lock()
                .unwrap()
                .iter()
                .filter(|(s, f, _)| (*s, *f) == ("acyclic.actors.grpc.transport", "closed"))
                .count()
        };
        assert_eq!(grpc_closed(), 1);
        let frame = std::future::poll_fn(|cx| Pin::new(&mut body).poll_frame(cx)).await;
        assert!(frame.is_some_and(|frame| frame.is_ok_and(|frame| frame.is_trailers())));
        assert_eq!(grpc_closed(), 2);

        let seen = seen.lock().unwrap();
        for expected in [
            ("acyclic.actors.http.call", "route", "v1/actors/create"),
            ("acyclic.actors.http.call", "http.status", "503"),
            ("acyclic.actors.http.call", "error.kind", "service"),
            ("acyclic.actors.grpc.transport", "rpc", "CreateActor"),
            ("acyclic.actors.grpc.transport", "error.kind", "transport"),
            ("acyclic.actors.grpc.transport", "rpc.code", "14"),
            ("acyclic.actors.grpc.transport", "error.kind", "status"),
        ] {
            assert!(
                seen.iter()
                    .any(|(s, f, v)| (*s, *f, v.as_str()) == expected),
                "{expected:?} not in {seen:?}"
            );
        }
        assert!(
            seen.iter()
                .all(|(_, field, value)| *field != "authorization" && !value.contains("secret"))
        );
        Ok(())
    }
}

#[cfg(all(test, not(target_arch = "wasm32")))]
mod grpc_tests;
