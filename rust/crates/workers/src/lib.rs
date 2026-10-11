#![doc = include_str!("../README.md")]


#[cfg(all(feature = "browser-binding", target_arch = "wasm32"))]
mod account_binding;
#[cfg(all(feature = "node-binding", not(target_arch = "wasm32")))]
mod account_binding_native;
#[cfg(all(feature = "node-binding", not(target_arch = "wasm32")))]
mod customer_binding;
mod admission;
mod client_config;
/// Source-defined binding configuration, diagnostics and message ceiling.
pub use client_config::{
    Failure, MAX_MESSAGE_BYTES, WorkersCallOptions, WorkersClientOptions,
    WorkersTransportPreference, export_client_typescript,
};
/// Target-selected Rust remote client; FFI features affect only exported bindings.
pub mod client_binding;
pub use admission::{
    ContractError, MAX_INLINE_BYTES, MAX_JOB_ATTEMPTS, MAX_MODULE_BYTES,
    validate_invoke_deployment, validate_invoke_version, validate_publish, validate_result,
    validate_select, validate_submit,
};
pub use client_binding::Client;
pub use tokio_util::sync::CancellationToken;

/// Executable Rust-owned Workers declarations and schema.
pub mod contract;
/// Strong Workers semantic types projected into TypeScript.
pub use contract::domain;

#[cfg(not(target_arch = "wasm32"))]
pub mod grpc;
#[cfg(not(target_arch = "wasm32"))]
pub mod http;

/// Wire shadows and tonic adapters derived from the Rust declarations.
#[allow(
    missing_docs,
    clippy::all,
    clippy::pedantic,
    clippy::allow_attributes_without_reason,
    reason = "maintained contract/tonic generated wire surface"
)]
pub mod wire {
    pub use crate::contract::*;
    include!(concat!(env!("OUT_DIR"), "/rust/acyclic.workers.v1.rs"));
}
/// Canonical version-one descriptor derived from executable Rust declarations.
pub const FILE_DESCRIPTOR_SET: &[u8] =
    include_bytes!(concat!(env!("OUT_DIR"), "/acyclic-workers-v1.bin"));

/// Public JavaScript entrypoint declaration emitted into the TypeScript package.
/// A module may implement either handler or both; durable jobs never call `fetch`.
pub const MODULE_TYPESCRIPT_CONTRACT: &str = r#"// Generated from acyclic-workers::MODULE_TYPESCRIPT_CONTRACT. Do not edit.
export interface WorkerJobContext {
  readonly jobId: string;
  /** Starts at 1 and increases when an accepted job is retried. */
  readonly attempt: number;
  readonly signal: AbortSignal;
}
export interface WorkerModule {
  fetch?(request: Request): Response | Promise<Response>;
  run?(input: Uint8Array, context: WorkerJobContext): Uint8Array | Promise<Uint8Array>;
}
"#;

/// Largest bearer credential, in bytes, that an SDK client accepts. It matches the
/// Acyclic platform's maximum bearer (12 KiB), so every platform-issued credential fits.
pub const MAX_BEARER_TOKEN_BYTES: usize = 12 * 1024;

/// Bearer credentials must be nonblank and at most [`MAX_BEARER_TOKEN_BYTES`]; the HTTP and gRPC
/// header parsers additionally reject control characters such as CR, LF, and NUL.
fn valid_token(token: &str) -> bool {
    !token.trim().is_empty() && token.len() <= MAX_BEARER_TOKEN_BYTES
}

/// Rust-owned route names used by the TypeScript transport generator.
pub const HTTP_ROUTES: &[(&str, &str)] = &[
    ("publishVersion", "v1/workers/versions/publish"),
    ("selectDeployment", "v1/workers/deployments/select"),
    ("submitJob", "v1/workers/jobs/submit"),
    ("inspectJob", "v1/workers/jobs/inspect"),
    ("cancelJob", "v1/workers/jobs/cancel"),
    ("invokeVersion", "v1/workers/versions/{sha256hex}/invoke"),
    ("invokeDeployment", "v1/workers/deployments/{alias}/invoke"),
];

#[cfg(all(test, not(target_arch = "wasm32")))]
mod tests {
    use super::*;
    use sha2::{Digest, Sha256};

    #[test]
    fn grpc_service_details_retain_future_codes_and_original_status() {
        use prost::Message as _;
        for code in [1, 99, -7] {
            let detail = wire::Error {
                code,
                message: "quota 100%: caf\u{e9} / %25".into(),
            };
            let bytes = detail.encode_to_vec();
            let status = tonic::Status::with_details(
                tonic::Code::PermissionDenied,
                "denied 100%: caf\u{e9} %25",
                bytes.clone().into(),
            );
            assert_eq!(grpc::error_detail(&status), Some(detail));
            assert_eq!(status.message(), "denied 100%: caf\u{e9} %25");
            assert_eq!(status.details(), bytes);
            assert_eq!(status.code(), tonic::Code::PermissionDenied);
        }
        for bytes in [
            Vec::new(),
            vec![255],
            wire::Error {
                code: 0,
                message: "unspecified".into(),
            }
            .encode_to_vec(),
        ] {
            let status =
                tonic::Status::with_details(tonic::Code::Unknown, "original", bytes.clone().into());
            assert_eq!(grpc::error_detail(&status), None);
            assert_eq!(status.details(), bytes);
        }
    }

    #[cfg(not(target_arch = "wasm32"))]
    #[tokio::test]
    #[allow(
        clippy::too_many_lines,
        reason = "Keep the HTTP service-detail boundary cases in one fixture"
    )]
    async fn http_service_details_retain_body_codes_and_bounds()
    -> Result<(), Box<dyn std::error::Error>> {
        use tokio::io::{AsyncReadExt as _, AsyncWriteExt as _};
        let message = "quota 100%: caf\u{e9} / %25";
        let mut cases = vec![
            (Vec::new(), None, false),
            (b"not JSON %25".to_vec(), None, false),
            (vec![255], None, false),
            (
                br#"{"code":0,"message":"unspecified"}"#.to_vec(),
                None,
                false,
            ),
        ];
        for code in [1, 99, -7] {
            cases.push((
                serde_json::to_vec(&serde_json::json!({ "code": code, "message": message }))?,
                Some(code),
                false,
            ));
        }
        cases.push((
            serde_json::to_vec(&serde_json::json!({ "code": 99, "message": message }))?,
            Some(99),
            true,
        ));
        for (body, expected_code, chunked) in cases {
            for maximum in [body.len().max(1), 1] {
                let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
                let address = listener.local_addr()?;
                let client = http::Client::new(&format!("http://{address}"), "secret", maximum)?;
                let sent = body.clone();
                let mut server = tokio::spawn(async move {
                    let (mut stream, _) = listener.accept().await?;
                    let mut headers = Vec::new();
                    while !headers.ends_with(b"\r\n\r\n") {
                        if headers.len() == 4096 {
                            return Err(std::io::Error::other("fixture request headers too large"));
                        }
                        headers.push(stream.read_u8().await?);
                    }
                    let headers = std::str::from_utf8(&headers).map_err(std::io::Error::other)?;
                    let length = headers
                        .lines()
                        .filter_map(|line| line.split_once(':'))
                        .find(|(name, _)| name.eq_ignore_ascii_case("content-length"))
                        .and_then(|(_, value)| value.trim().parse::<usize>().ok())
                        .filter(|length| *length <= 4096)
                        .ok_or_else(|| {
                            std::io::Error::other("fixture request length missing or invalid")
                        })?;
                    stream.read_exact(&mut vec![0; length]).await?;
                    let framing = if chunked {
                        "transfer-encoding: chunked".into()
                    } else {
                        format!("content-length: {}", sent.len())
                    };
                    let mut response = format!("HTTP/1.1 503 Service Unavailable\r\ncontent-type: application/json\r\n{framing}\r\nconnection: close\r\n\r\n").into_bytes();
                    if chunked {
                        response.extend_from_slice(format!("{:x}\r\n", sent.len()).as_bytes());
                    }
                    response.extend_from_slice(&sent);
                    if chunked {
                        response.extend_from_slice(b"\r\n0\r\n\r\n");
                    }
                    stream.write_all(&response).await?;
                    Ok::<_, std::io::Error>(())
                });
                let result = tokio::time::timeout(std::time::Duration::from_secs(5), async {
                    let response = client
                        .inspect_job(&wire::InspectJobRequest {
                            job_id: "job".into(),
                        })
                        .await;
                    (&mut server).await??;
                    Ok::<_, Box<dyn std::error::Error>>(response)
                })
                .await;
                let error = match result {
                    Ok(response) => response?.unwrap_err(),
                    Err(error) => {
                        server.abort();
                        let _ = server.await;
                        return Err(error.into());
                    }
                };
                if body.len() > maximum {
                    assert!(matches!(error, http::Error::ResponseTooLarge));
                } else {
                    let http::Error::Service {
                        status,
                        detail,
                        raw_details,
                    } = error
                    else {
                        panic!("expected service error")
                    };
                    assert_eq!(status, 503);
                    assert_eq!(raw_details, body);
                    assert_eq!(detail.as_ref().map(|value| value.code), expected_code);
                    if let Some(detail) = detail {
                        assert_eq!(detail.message, message);
                    }
                }
            }
        }
        Ok(())
    }

    #[test]
    fn alias_selection_requires_positive_revision_when_present() {
        let mut request = wire::SelectDeploymentRequest {
            alias: "current".into(),
            version_sha256: vec![1; 32],
            idempotency_key: "select-a".into(),
            expected_revision: None,
        };
        assert_eq!(validate_select(&request), Ok(()));
        request.expected_revision = Some(7);
        assert_eq!(validate_select(&request), Ok(()));
        request.expected_revision = Some(0);
        assert_eq!(
            validate_select(&request),
            Err(ContractError::InvalidArgument)
        );
    }

    #[test]
    fn publication_is_bound_to_exact_bytes() {
        let bytes = b"export default { fetch() { return new Response('ok') } }";
        let mut request = wire::PublishVersionRequest {
            javascript_module: bytes.to_vec(),
            expected_sha256: Sha256::digest(bytes).to_vec(),
            idempotency_key: "publish-a".into(),
        };
        assert_eq!(validate_publish(&request), Ok(()));
        let mut modified = request.javascript_module.to_vec();
        modified.push(b' ');
        request.javascript_module = modified;
        assert_eq!(
            validate_publish(&request),
            Err(ContractError::DigestMismatch)
        );
    }

    #[test]
    fn job_is_pinned_or_resolved_at_acceptance() {
        let mut request = wire::SubmitJobRequest {
            target: Some(wire::JobTarget {
                target: Some(wire::job_target::Target::DeploymentAlias("current".into())),
            }),
            input: Some(wire::Payload {
                source: Some(wire::payload::Source::InlineBytes(vec![])),
            }),
            limits: Some(wire::JobLimits {
                timeout_millis: 1000,
                memory_bytes: 1024,
                output_bytes: 1024,
            }),
            retry: Some(wire::RetryPolicy {
                max_attempts: 2,
                backoff_millis: 0,
            }),
            idempotency_key: "job-a".into(),
        };
        assert_eq!(validate_submit(&request), Ok(()));
        request.input = Some(wire::Payload {
            source: Some(wire::payload::Source::Object(wire::ObjectRef {
                bucket: "customer-input".into(),
                key: "video/input.mp4".into(),
            })),
        });
        assert_eq!(validate_submit(&request), Ok(()));
        request.input = Some(wire::Payload {
            source: Some(wire::payload::Source::Object(wire::ObjectRef {
                bucket: "customer-input".into(),
                key: String::new(),
            })),
        });
        assert_eq!(
            validate_submit(&request),
            Err(ContractError::InvalidArgument)
        );
    }

    #[test]
    fn result_is_bounded_by_the_accepted_budget() {
        let limits = wire::JobLimits {
            timeout_millis: 1000,
            memory_bytes: 1024,
            output_bytes: 2,
        };
        assert_eq!(
            validate_result(&wire::JobResult { body: vec![1, 2] }, &limits),
            Ok(())
        );
        assert_eq!(
            validate_result(
                &wire::JobResult {
                    body: vec![1, 2, 3]
                },
                &limits
            ),
            Err(ContractError::LimitExceeded)
        );
    }

    #[test]
    fn clients_reject_unsafe_endpoints_and_credentials() {
        let long = "t".repeat(MAX_BEARER_TOKEN_BYTES + 1);
        for token in ["", " ", "a\r\nb", "a\0b", long.as_str()] {
            assert!(matches!(
                http::Client::new("https://example.test", token, 1),
                Err(http::Error::InvalidArgument)
            ));
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
            const NAME: &'static str = "acyclic.workers.v1.WorkersService";
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
                "http://{address}/acyclic.workers.v1.WorkersService/{rpc}"
            ))
            .extension(tonic::GrpcMethod::new(
                "acyclic.workers.v1.WorkersService",
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
            client.submit_job(&wire::SubmitJobRequest::default()).await,
            Err(http::Error::Service { status: 503, .. })
        ));
        assert!(traced_call(address, "SubmitJob").await.is_err());

        // The span stays open while a slow body runs and records a status sent in trailers.
        let address = slow_status_server().await?;
        let mut body = traced_call(address, "GetJob").await?.into_body();
        let grpc_closed = || {
            seen.lock()
                .unwrap()
                .iter()
                .filter(|(s, f, _)| (*s, *f) == ("acyclic.workers.grpc.transport", "closed"))
                .count()
        };
        assert_eq!(grpc_closed(), 1);
        let frame = std::future::poll_fn(|cx| Pin::new(&mut body).poll_frame(cx)).await;
        assert!(frame.is_some_and(|frame| frame.is_ok_and(|frame| frame.is_trailers())));
        assert_eq!(grpc_closed(), 2);

        let seen = seen.lock().unwrap();
        for expected in [
            (
                "acyclic.workers.http.call",
                "route",
                "v1/workers/jobs/submit",
            ),
            ("acyclic.workers.http.call", "http.status", "503"),
            ("acyclic.workers.http.call", "error.kind", "service"),
            ("acyclic.workers.grpc.transport", "rpc", "SubmitJob"),
            ("acyclic.workers.grpc.transport", "error.kind", "transport"),
            ("acyclic.workers.grpc.transport", "rpc.code", "14"),
            ("acyclic.workers.grpc.transport", "error.kind", "status"),
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
