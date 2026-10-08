#![doc = include_str!("../README.md")]

use sha2::{Digest, Sha256};

pub mod grpc;
pub mod http;

/// Generated Workers v1 wire types. The documented schema is `proto/workers/v1/workers.proto`.
pub mod wire {
    #![allow(missing_docs, reason = "generated from the public Workers schema")]
    #![allow(
        clippy::all,
        clippy::pedantic,
        clippy::allow_attributes_without_reason,
        reason = "generated protobuf bindings"
    )]
    include!("generated/acyclic.workers.v1.rs");
}

/// Canonical version-one descriptor set.
pub const FILE_DESCRIPTOR_SET: &[u8] = include_bytes!("generated/acyclic-workers-v1.bin");
/// Largest inline JavaScript module admitted by the first public contract.
pub const MAX_MODULE_BYTES: usize = 1024 * 1024;
/// Largest inline job input or result.
pub const MAX_INLINE_BYTES: usize = 1024 * 1024;
/// Largest bounded retry count.
pub const MAX_JOB_ATTEMPTS: u32 = 8;

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

/// Invalid customer-authored Workers request.
#[derive(Clone, Copy, Debug, Eq, PartialEq, thiserror::Error)]
pub enum ContractError {
    /// A required field is absent or malformed.
    #[error("required field is absent or malformed")]
    InvalidArgument,
    /// An authored byte payload exceeds its contract bound.
    #[error("inline payload exceeds the Workers v1 limit")]
    LimitExceeded,
    /// The supplied digest does not identify the exact module bytes.
    #[error("module SHA-256 does not match exact bytes")]
    DigestMismatch,
}

fn digest(value: &[u8]) -> bool {
    value.len() == 32 && value.iter().any(|byte| *byte != 0)
}

fn name(value: &str) -> bool {
    !value.is_empty()
        && value != "."
        && value != ".."
        && value.len() <= 256
        && value
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'_' | b'.'))
}

/// Validates an immutable JavaScript module publication before service admission.
pub fn validate_publish(request: &wire::PublishVersionRequest) -> Result<(), ContractError> {
    if request.javascript_module.is_empty() || !name(&request.idempotency_key) {
        return Err(ContractError::InvalidArgument);
    }
    if request.javascript_module.len() > MAX_MODULE_BYTES {
        return Err(ContractError::LimitExceeded);
    }
    if !digest(&request.expected_sha256) {
        return Err(ContractError::InvalidArgument);
    }
    if Sha256::digest(&request.javascript_module).as_slice() != request.expected_sha256 {
        return Err(ContractError::DigestMismatch);
    }
    Ok(())
}

/// Validates a compare-and-select deployment alias mutation.
pub fn validate_select(request: &wire::SelectDeploymentRequest) -> Result<(), ContractError> {
    if !name(&request.alias)
        || !digest(&request.version_sha256)
        || !name(&request.idempotency_key)
        || request.expected_revision == Some(0)
    {
        return Err(ContractError::InvalidArgument);
    }
    Ok(())
}

/// Validates a durable job request before authoritative resolution of its target.
pub fn validate_submit(request: &wire::SubmitJobRequest) -> Result<(), ContractError> {
    let target = request
        .target
        .as_ref()
        .and_then(|value| value.target.as_ref());
    let valid_target = match target {
        Some(wire::job_target::Target::DeploymentAlias(alias)) => name(alias),
        Some(wire::job_target::Target::VersionSha256(value)) => digest(value),
        None => false,
    };
    if !valid_target || !name(&request.idempotency_key) {
        return Err(ContractError::InvalidArgument);
    }
    let Some(input) = request
        .input
        .as_ref()
        .and_then(|value| value.source.as_ref())
    else {
        return Err(ContractError::InvalidArgument);
    };
    match input {
        wire::payload::Source::InlineBytes(bytes) if bytes.len() > MAX_INLINE_BYTES => {
            return Err(ContractError::LimitExceeded);
        }
        wire::payload::Source::Object(reference)
            if reference.bucket.is_empty()
                || reference.bucket.len() > 63
                || reference.key.is_empty()
                || reference.key.len() > 1024 =>
        {
            return Err(ContractError::InvalidArgument);
        }
        _ => {}
    }
    let Some(retry) = request.retry.as_ref() else {
        return Err(ContractError::InvalidArgument);
    };
    if retry.max_attempts == 0
        || retry.max_attempts > MAX_JOB_ATTEMPTS
        || !request.limits.as_ref().is_some_and(|limits| {
            limits.timeout_millis > 0
                && limits.memory_bytes > 0
                && limits.output_bytes > 0
                && limits.output_bytes <= MAX_INLINE_BYTES as u64
        })
    {
        return Err(ContractError::InvalidArgument);
    }
    Ok(())
}

/// Validates exact job output against the accepted job's bounded output budget.
pub fn validate_result(
    result: &wire::JobResult,
    limits: &wire::JobLimits,
) -> Result<(), ContractError> {
    if limits.output_bytes == 0 || limits.output_bytes > MAX_INLINE_BYTES as u64 {
        return Err(ContractError::InvalidArgument);
    }
    if result.body.len() as u64 > limits.output_bytes {
        return Err(ContractError::LimitExceeded);
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

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
        request.javascript_module.push(b' ');
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
            assert!(http::Client::new(endpoint, &"t".repeat(8192), 1).is_ok());
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
        rpc: &str,
    ) -> Result<tonic::codegen::http::Response<tonic::body::Body>, Box<dyn std::error::Error>> {
        use tonic::codegen::Service as _;
        let mut channel = grpc::TracedChannel(
            tonic::transport::Endpoint::from_shared(format!("http://{address}"))?.connect_lazy(),
        );
        std::future::poll_fn(|cx| channel.poll_ready(cx)).await?;
        let request = tonic::codegen::http::Request::builder()
            .uri(format!(
                "http://{address}/acyclic.workers.v1.WorkersService/{rpc}"
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
                .filter(|(s, f, _)| (*s, *f) == ("acyclic.workers.grpc.call", "closed"))
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
            ("acyclic.workers.grpc.call", "rpc", "SubmitJob"),
            ("acyclic.workers.grpc.call", "error.kind", "transport"),
            ("acyclic.workers.grpc.call", "rpc.code", "14"),
            ("acyclic.workers.grpc.call", "error.kind", "status"),
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
