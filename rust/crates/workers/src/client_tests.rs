//! Verify authenticated automatic transport selection against actual HTTP endpoints.
#![cfg(not(target_arch = "wasm32"))]

use acyclic_sdk_contract_wire::{BindingFamily, transport_control as control};
use std::{
    error::Error,
    io,
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    },
};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tonic::{
    Request, Response, Status,
    transport::{Identity, Server, ServerTlsConfig},
};

type TestResult<T> = Result<T, Box<dyn Error + Send + Sync>>;

async fn endpoint(
    family: BindingFamily,
    digest: String,
    requests: usize,
) -> TestResult<(String, tokio::task::JoinHandle<TestResult<Vec<String>>>)> {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
    let address = listener.local_addr()?;
    let task = tokio::spawn(async move {
        let mut observed = Vec::new();
        for index in 0..requests {
            let (mut stream, _) = listener.accept().await?;
            let mut bytes = Vec::new();
            while !bytes.windows(4).any(|part| part == b"\r\n\r\n") {
                let mut chunk = [0; 1024];
                let count = stream.read(&mut chunk).await?;
                if count == 0 {
                    return Err(io::Error::other("fixture closed before the HTTP headers").into());
                }
                let new_len = bytes
                    .len()
                    .checked_add(count)
                    .filter(|length| *length <= 64 * 1024)
                    .ok_or_else(|| io::Error::other("fixture request exceeded 64 KiB"))?;
                let received = chunk
                    .get(..count)
                    .ok_or_else(|| io::Error::other("fixture returned an invalid read length"))?;
                bytes.extend_from_slice(received);
                debug_assert_eq!(bytes.len(), new_len);
            }
            let request = String::from_utf8(bytes)?;
            if !request
                .to_ascii_lowercase()
                .contains("authorization: bearer fixture-token\r\n")
            {
                return Err(io::Error::other("fixture request omitted bearer authentication").into());
            }
            let request_line = request
                .lines()
                .next()
                .ok_or_else(|| io::Error::other("fixture request omitted its request line"))?;
            observed.push(request_line.to_owned());
            let body = if index == 0 {
                serde_json::json!({
                    "protocol": { "version": control::control_protocol_version(family), "descriptorDigest": digest },
                    "supported": { "capabilities": [{"name": family.name(), "version": control::control_protocol_version(family)}] }
                })
                .to_string()
            } else {
                "{}".to_owned()
            };
            let response = format!(
                "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{}",
                body.len(),
                body
            );
            stream.write_all(response.as_bytes()).await?;
            stream.shutdown().await?;
        }
        Ok::<Vec<String>, Box<dyn Error + Send + Sync>>(observed)
    });
    Ok((format!("http://{address}"), task))
}

#[tokio::test]
async fn workers_choose_verified_http_without_consumer_feature_flags() -> TestResult<()> {
    let family = BindingFamily::Workers;
    let (endpoint, server) =
        endpoint(family, control::archived_descriptor_digest(family), 1).await?;
    let client = crate::connect(&endpoint, "fixture-token").await?;
    assert_eq!(client.transport(), "http");
    let observed = tokio::time::timeout(std::time::Duration::from_secs(5), server)
        .await
        .map_err(|error| io::Error::other(format!("fixture timed out: {error}")))??;
    assert_eq!(observed, ["GET /v1/sdk/workers/handshake HTTP/1.1"]);
    Ok(())
}

#[tokio::test]
async fn incompatible_identity_is_terminal_before_any_application_request() -> TestResult<()> {
    let (endpoint, server) =
        endpoint(BindingFamily::Workers, "substituted-descriptor".into(), 1).await?;
    assert!(crate::connect(&endpoint, "fixture-token").await.is_err());
    let observed = tokio::time::timeout(std::time::Duration::from_secs(5), server)
        .await
        .map_err(|error| io::Error::other(format!("fixture timed out: {error}")))??;
    assert_eq!(observed, ["GET /v1/sdk/workers/handshake HTTP/1.1"]);
    Ok(())
}

#[derive(Clone, Copy)]
enum TlsMode {
    Valid,
    WrongIdentity,
    Unauthorized,
}

struct ControlFixture {
    mode: TlsMode,
    calls: Arc<AtomicUsize>,
}

#[tonic::async_trait]
impl crate::control_wire::transport::v1::protocol_service_server::ProtocolService
    for ControlFixture
{
    async fn handshake(
        &self,
        request: Request<crate::control_wire::protocol::v1::HandshakeRequest>,
    ) -> Result<Response<crate::control_wire::protocol::v1::HandshakeResponse>, Status> {
        self.calls.fetch_add(1, Ordering::SeqCst);
        assert_eq!(
            request.metadata().get("authorization").unwrap(),
            "Bearer fixture-token"
        );
        if matches!(self.mode, TlsMode::Unauthorized) {
            return Err(Status::unauthenticated("invalid credential"));
        }
        let family = BindingFamily::Workers;
        let version = control::control_protocol_version(family);
        let presented = request.get_ref().protocol.as_ref().unwrap();
        assert_eq!(presented.version, version);
        assert_eq!(
            presented.descriptor_digest,
            control::archived_descriptor_digest(family)
        );
        let required = &request.get_ref().required.as_ref().unwrap().capabilities;
        assert_eq!(required.len(), 1);
        assert_eq!(required[0].name, family.name());
        assert_eq!(required[0].version, version);
        Ok(Response::new(
            crate::control_wire::protocol::v1::HandshakeResponse {
                protocol: Some(crate::control_wire::protocol::v1::ProtocolIdentity {
                    version: version.into(),
                    descriptor_digest: match self.mode {
                        TlsMode::WrongIdentity => "wrong-archive".into(),
                        TlsMode::Valid | TlsMode::Unauthorized => {
                            control::archived_descriptor_digest(family)
                        }
                    },
                }),
                supported: Some(crate::control_wire::protocol::v1::CapabilitySet {
                    capabilities: vec![crate::control_wire::protocol::v1::Capability {
                        name: family.name().into(),
                        version: version.into(),
                    }],
                }),
            },
        ))
    }
}

struct WorkerFixture {
    application_calls: Arc<AtomicUsize>,
}

impl WorkerFixture {
    fn reject<T>(&self, request: &Request<T>) -> Result<(), Status> {
        self.application_calls.fetch_add(1, Ordering::SeqCst);
        assert_eq!(
            request.metadata().get("authorization").unwrap(),
            "Bearer fixture-token"
        );
        Err(Status::unimplemented("fixture operation rejected"))
    }
}

#[tonic::async_trait]
impl crate::wire::workers_service_server::WorkersService for WorkerFixture {
    async fn publish_version(
        &self,
        request: Request<crate::wire::PublishVersionRequest>,
    ) -> Result<Response<crate::wire::PublishVersionResponse>, Status> {
        self.reject(&request)?;
        unreachable!()
    }

    async fn select_deployment(
        &self,
        request: Request<crate::wire::SelectDeploymentRequest>,
    ) -> Result<Response<crate::wire::SelectDeploymentResponse>, Status> {
        self.reject(&request)?;
        unreachable!()
    }

    async fn submit_job(
        &self,
        request: Request<crate::wire::SubmitJobRequest>,
    ) -> Result<Response<crate::wire::SubmitJobResponse>, Status> {
        self.reject(&request)?;
        unreachable!()
    }

    async fn inspect_job(
        &self,
        request: Request<crate::wire::InspectJobRequest>,
    ) -> Result<Response<crate::wire::InspectJobResponse>, Status> {
        self.reject(&request)?;
        unreachable!()
    }

    async fn cancel_job(
        &self,
        request: Request<crate::wire::CancelJobRequest>,
    ) -> Result<Response<crate::wire::CancelJobResponse>, Status> {
        self.reject(&request)?;
        unreachable!()
    }

    async fn invoke_version(
        &self,
        request: Request<crate::wire::InvokeVersionRequest>,
    ) -> Result<Response<crate::wire::InvokeResponse>, Status> {
        self.reject(&request)?;
        unreachable!()
    }

    async fn invoke_deployment(
        &self,
        request: Request<crate::wire::InvokeDeploymentRequest>,
    ) -> Result<Response<crate::wire::InvokeResponse>, Status> {
        self.reject(&request)?;
        unreachable!()
    }
}

async fn tls_endpoint(
    mode: TlsMode,
) -> (
    String,
    String,
    Arc<AtomicUsize>,
    Arc<AtomicUsize>,
    tokio::sync::oneshot::Sender<()>,
    tokio::task::JoinHandle<Result<(), tonic::transport::Error>>,
) {
    let certificate =
        rcgen::generate_simple_self_signed(["localhost".to_owned(), "127.0.0.1".to_owned()])
            .unwrap();
    let pem = certificate.cert.pem();
    let identity = Identity::from_pem(pem.clone(), certificate.signing_key.serialize_pem());
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let endpoint = format!(
        "https://127.0.0.1:{}",
        listener.local_addr().unwrap().port()
    );
    let control_calls = Arc::new(AtomicUsize::new(0));
    let application_calls = Arc::new(AtomicUsize::new(0));
    let control = ControlFixture {
        mode,
        calls: control_calls.clone(),
    };
    let workers = WorkerFixture {
        application_calls: application_calls.clone(),
    };
    let (shutdown, receiver) = tokio::sync::oneshot::channel();
    let incoming = futures::stream::unfold(listener, |listener| async {
        Some((listener.accept().await.map(|(stream, _)| stream), listener))
    });
    let server = tokio::spawn(async move {
        Server::builder()
            .tls_config(ServerTlsConfig::new().identity(identity))
            .unwrap()
            .add_service(
                crate::control_wire::transport::v1::protocol_service_server::ProtocolServiceServer::new(control),
            )
            .add_service(crate::wire::workers_service_server::WorkersServiceServer::new(workers))
            .serve_with_incoming_shutdown(incoming, async {
                let _ = receiver.await;
            })
            .await
    });
    (
        endpoint,
        pem,
        control_calls,
        application_calls,
        shutdown,
        server,
    )
}

#[tokio::test]
async fn native_prefers_verified_grpc_and_never_replays_application_errors() {
    let (endpoint, ca, control_calls, application_calls, shutdown, server) =
        tls_endpoint(TlsMode::Valid).await;
    let client = crate::connect_with_ca_certificate(&endpoint, "fixture-token", Some(ca.as_bytes()))
        .await
        .unwrap();
    assert_eq!(client.transport(), "grpc");
    assert_eq!(control_calls.load(Ordering::SeqCst), 1);
    let error = client
        .inspect_job(&crate::wire::InspectJobRequest::default())
        .await
        .unwrap_err();
    assert!(matches!(
        error,
        crate::Error::Service {
            grpc_code: Some(12),
            http_status: None,
            ..
        }
    ));
    assert_eq!(application_calls.load(Ordering::SeqCst), 1);
    shutdown.send(()).unwrap();
    tokio::time::timeout(std::time::Duration::from_secs(5), server)
        .await
        .unwrap()
        .unwrap()
        .unwrap();
}

#[tokio::test]
async fn native_workers_authentication_and_identity_failures_are_terminal() {
    for (mode, expected) in [
        (TlsMode::WrongIdentity, "DescriptorMismatch"),
        (TlsMode::Unauthorized, "invalid credential"),
    ] {
        let (endpoint, ca, control_calls, application_calls, shutdown, server) =
            tls_endpoint(mode).await;
        let error = crate::connect_with_ca_certificate(&endpoint, "fixture-token", Some(ca.as_bytes()))
            .await
            .err()
            .unwrap();
        assert!(matches!(error, crate::Error::Configuration(_)));
        assert!(error.to_string().contains(expected), "{error}");
        assert_eq!(control_calls.load(Ordering::SeqCst), 1);
        assert_eq!(application_calls.load(Ordering::SeqCst), 0);
        shutdown.send(()).unwrap();
        tokio::time::timeout(std::time::Duration::from_secs(5), server)
            .await
            .unwrap()
            .unwrap()
            .unwrap();
    }
}
