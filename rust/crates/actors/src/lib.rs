#![doc = include_str!("../README.md")]

/// Rust-owned semantic projections for generated SDK metadata.
pub use contract::{
    domain, ContractError, MAX_BINDINGS, MAX_SUBSCRIPTIONS,
    validate_add_subscription, validate_create, validate_delete, validate_update,
};

/// Authenticated operations over the platform's Rust-owned transport.
pub mod client;

/// Rust-owned Actors contract declarations and schema renderer.
pub mod contract;

/// Native gRPC client for the canonical Actors v1 service.
#[cfg(not(target_arch = "wasm32"))]
pub mod grpc;
/// Native HTTP client for the canonical Actors v1 Protobuf JSON service.
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
pub use acyclic_workers::MAX_BEARER_TOKEN_BYTES;

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
    ("deleteActor", "v1/actors/delete"),
];

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn subscription_start_is_exactly_one_choice() {
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
        let start = create.subscriptions[0].start.take();
        assert_eq!(validate_create(&create), Err(ContractError::InvalidArgument));
        create.subscriptions[0].start = start;
        let duplicate = create.subscriptions.clone();
        create.subscriptions.extend(duplicate);
        assert_eq!(validate_create(&create), Err(ContractError::DuplicateName));
    }
    #[test]
    fn delete_requires_actor_and_bounded_idempotency_key() {
        let valid = wire::DeleteActorRequest {
            actor_id: "actor-a".into(),
            idempotency_key: "delete-a".into(),
        };
        assert_eq!(validate_delete(&valid), Ok(()));
        let semantic = domain::DeleteActorRequest::try_from(valid.clone()).expect("valid delete");
        assert_eq!(semantic.actor_id().as_str(), "actor-a");
        assert_eq!(semantic.idempotency_key(), "delete-a");
        assert_eq!(wire::DeleteActorRequest::from(semantic), valid);
        for invalid in [
            wire::DeleteActorRequest {
                actor_id: String::new(),
                ..valid.clone()
            },
            wire::DeleteActorRequest {
                idempotency_key: String::new(),
                ..valid.clone()
            },
            wire::DeleteActorRequest {
                idempotency_key: "k".repeat(257),
                ..valid.clone()
            },
        ] {
            assert_eq!(
                validate_delete(&invalid),
                Err(ContractError::InvalidArgument)
            );
            assert!(domain::DeleteActorRequest::try_from(invalid).is_err());
        }
        let boundary = wire::DeleteActorRequest {
            idempotency_key: "k".repeat(256),
            ..valid
        };
        assert_eq!(validate_delete(&boundary), Ok(()));
        assert_eq!(
            domain::DeleteActorResponse::try_from(wire::DeleteActorResponse {}),
            Ok(domain::DeleteActorResponse::default())
        );
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
            grpc::transport_span,
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
        use tokio::io::{AsyncReadExt as _, AsyncWriteExt as _};
        use tracing_subscriber::layer::SubscriberExt as _;

        use acyclic_grpc_observability::test_support::Capture;
        // A second live dispatcher keeps callsites consulting this test's subscriber.
        let _second = tracing::Dispatch::new(tracing_subscriber::registry());
        let seen = Capture::default();
        let _guard = tracing::subscriber::set_default(
            tracing_subscriber::registry().with(seen.clone()),
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
        let grpc_closed = || seen.closed("acyclic.actors.grpc.transport");
        assert_eq!(grpc_closed(), 1);
        let frame = std::future::poll_fn(|cx| Pin::new(&mut body).poll_frame(cx)).await;
        assert!(frame.is_some_and(|frame| frame.is_ok_and(|frame| frame.is_trailers())));
        assert_eq!(grpc_closed(), 2);

        let seen = seen.0.lock().unwrap();
        let fields = || seen.spans.iter().flat_map(|span| span.fields.iter()
            .map(move |(field, value)| (span.name, field.as_str(), value.as_str())));
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
                fields().any(|actual| actual == expected),
                "{expected:?} not in {:?}", seen.spans
            );
        }
        assert!(
            fields().all(|(_, field, value)| field != "authorization" && !value.contains("secret"))
        );
        Ok(())
    }
}

#[cfg(all(test, not(target_arch = "wasm32")))]
mod grpc_tests;
