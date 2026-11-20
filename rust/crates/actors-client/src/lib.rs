//! Authenticated Actors v1 clients over gRPC and HTTP.
//!
//! The client surface is independent of the local runtime: it depends only on
//! the canonical contract, auth addressing and the platform transport.

pub use acyclic_actors_contract::{FILE_DESCRIPTOR_SET, contract, domain, wire};
pub use acyclic_actors_inventory::HTTP_ROUTES;
pub use acyclic_auth::addressing;

/// Shared authenticated client over a tonic transport.
pub mod client;
/// Fresh actor identities share one entropy source.
pub mod identity;
/// Namespace-scoped actor IDs and capability-bound stubs.
pub mod namespace;

/// Native gRPC client for the canonical Actors v1 service.
#[cfg(not(target_arch = "wasm32"))]
pub mod grpc;
/// Native HTTP client for the canonical Actors v1 Protobuf JSON service.
#[cfg(not(target_arch = "wasm32"))]
pub mod http;

#[cfg(all(test, not(target_arch = "wasm32")))]
mod grpc_tests;

#[cfg(all(test, not(target_arch = "wasm32")))]
mod tests {
    use super::*;

    #[test]
    fn clients_share_endpoint_and_credential_policy() {
        for token in ["", " ", "a\r\nb", "a\0b"] {
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
            assert!(http::Client::new(endpoint, &"t".repeat(64 * 1024), 1).is_ok());
        }
        for endpoint in [
            "http://example.test",
            "http://10.0.0.1",
            "https://u@example.test",
        ] {
            assert!(http::Client::new(endpoint, "t", 1).is_err());
        }
    }

    /// Serves one RPC whose OK headers precede a caller-released UNAVAILABLE trailer.
    #[cfg(not(target_arch = "wasm32"))]
    async fn slow_status_server()
    -> std::io::Result<(std::net::SocketAddr, tokio::sync::oneshot::Sender<()>)> {
        use std::task::Poll;

        #[derive(Clone)]
        struct SlowStatus(
            std::sync::Arc<std::sync::Mutex<Option<tokio::sync::oneshot::Receiver<()>>>>,
        );
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
                // One RPC holds the caller's release; any later one finds it closed.
                let release = self
                    .0
                    .lock()
                    .ok()
                    .and_then(|mut slot| slot.take())
                    .unwrap_or_else(|| tokio::sync::oneshot::channel().1);
                let body = acyclic_grpc_observability::tests::unavailable_body_after(release);
                std::future::ready(Ok(tonic::codegen::http::Response::new(body)))
            }
        }
        let incoming = tonic::transport::server::TcpIncoming::bind(([127, 0, 0, 1], 0).into())?;
        let address = incoming.local_addr()?;
        let (release, released) = tokio::sync::oneshot::channel();
        tokio::spawn(
            tonic::transport::Server::builder()
                .add_service(SlowStatus(std::sync::Arc::new(std::sync::Mutex::new(
                    Some(released),
                ))))
                .serve_with_incoming(incoming),
        );
        Ok((address, release))
    }

    /// Sends one empty request through a fresh [`grpc::TracedChannel`].
    #[cfg(not(target_arch = "wasm32"))]
    async fn traced_call(
        address: std::net::SocketAddr,
        rpc: &'static str,
    ) -> Result<tonic::codegen::http::Response<tonic::body::Body>, Box<dyn std::error::Error>> {
        use tonic::codegen::Service as _;
        let mut channel = grpc::TracedChannel::new(
            acyclic_auth::channel::guard(
                tonic::transport::Endpoint::from_shared(format!("http://{address}"))?
                    .connect_lazy(),
            ),
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

        use acyclic_test_support::trace_capture::Capture;
        let seen = Capture::default();
        let _guard =
            tracing::subscriber::set_default(tracing_subscriber::registry().with(seen.clone()));

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
                .deploy_namespace(&wire::DeploymentRequest {
                    publication: Vec::new(),
                    entry_source: None,
                    name: "tracing".into(),
                    code_sha256: vec![1; 32],
                    limits: Some(wire::ActorLimits {
                        handler_timeout_millis: Some(1),
                        memory_bytes: Some(1),
                        checkpoint_bytes: Some(1)
                    }),
                    idempotency_key: "tracing".into(),
                    ..Default::default()
                })
                .await,
            Err(http::Error::Service { status: 503, .. })
        ));
        assert!(traced_call(address, "DeployNamespace").await.is_err());

        // The span stays open while a slow body runs and records a status sent in trailers.
        let grpc_closed = || seen.closed("acyclic.actors.grpc.transport");
        for (index, producer_dropped) in [false, true].into_iter().enumerate() {
            let (address, release) = slow_status_server().await?;
            let mut body = traced_call(address, "GetActor").await?.into_body();
            assert!(
                futures::poll!(std::future::poll_fn(|cx| Pin::new(&mut body).poll_frame(cx)))
                    .is_pending()
            );
            assert_eq!(grpc_closed(), index + 1);
            if producer_dropped {
                drop(release);
            } else {
                release.send(()).unwrap();
            }
            let frame = std::future::poll_fn(|cx| Pin::new(&mut body).poll_frame(cx)).await;
            assert!(frame.is_some_and(|frame| frame.is_ok_and(|frame| frame.is_trailers())));
            assert_eq!(grpc_closed(), index + 2);
        }

        let seen = seen.0.lock().unwrap();
        let fields = || {
            seen.spans.iter().flat_map(|span| {
                span.fields
                    .iter()
                    .map(move |(field, value)| (span.name, field.as_str(), value.as_str()))
            })
        };
        for expected in [
            (
                "acyclic.actors.http.call",
                "route",
                "v1/actors/namespaces/deploy",
            ),
            ("acyclic.actors.http.call", "http.status", "503"),
            ("acyclic.actors.http.call", "error.kind", "service"),
            ("acyclic.actors.grpc.transport", "rpc", "DeployNamespace"),
            ("acyclic.actors.grpc.transport", "error.kind", "transport"),
            ("acyclic.actors.grpc.transport", "rpc.code", "14"),
            ("acyclic.actors.grpc.transport", "error.kind", "status"),
        ] {
            assert!(
                fields().any(|actual| actual == expected),
                "{expected:?} not in {:?}",
                seen.spans
            );
        }
        assert!(
            fields().all(|(_, field, value)| field != "authorization" && !value.contains("secret"))
        );
        Ok(())
    }
}
