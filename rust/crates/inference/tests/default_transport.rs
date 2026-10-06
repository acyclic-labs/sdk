//! Authenticated transport selection and HTTP operation coverage.
#![cfg(not(target_arch = "wasm32"))]

use acyclic_inference::{client, wire};
use acyclic_sdk_contract_wire::{BindingFamily, transport_control as control};
use std::{io, ops::Range};
use tokio::io::{AsyncReadExt, AsyncWriteExt};

type TestResult<T> = Result<T, Box<dyn std::error::Error + Send + Sync>>;

fn checked_slice<'a, T>(value: &'a [T], range: Range<usize>, label: &str) -> TestResult<&'a [T]> {
    value
        .get(range)
        .ok_or_else(|| io::Error::other(format!("fixture slice out of bounds: {label}")).into())
}

#[derive(Default)]
struct ControlService {
    digest: String,
}

#[tonic::async_trait]
impl acyclic_inference::control_wire::transport::v1::protocol_service_server::ProtocolService
    for ControlService
{
    async fn handshake(
        &self,
        request: tonic::Request<
            acyclic_inference::control_wire::protocol::v1::HandshakeRequest,
        >,
    ) -> Result<tonic::Response<acyclic_inference::control_wire::protocol::v1::HandshakeResponse>, tonic::Status> {
        let auth = request
            .metadata()
            .get("authorization")
            .and_then(|value| value.to_str().ok());
        if auth != Some("Bearer fixture-token") {
            return Err(tonic::Status::unauthenticated("invalid fixture credential"));
        }
        let family = BindingFamily::Inference;
        let version = control::control_protocol_version(family);
        Ok(tonic::Response::new(
            acyclic_inference::control_wire::protocol::v1::HandshakeResponse {
                protocol: Some(acyclic_inference::control_wire::protocol::v1::ProtocolIdentity {
                    version: version.into(),
                    descriptor_digest: self.digest.clone(),
                }),
                supported: Some(acyclic_inference::control_wire::protocol::v1::CapabilitySet {
                    capabilities: vec![acyclic_inference::control_wire::protocol::v1::Capability {
                        name: family.name().into(),
                        version: version.into(),
                    }],
                }),
            },
        ))
    }
}

#[tokio::test]
async fn native_client_verifies_authenticated_tls_grpc_before_selection() -> TestResult<()> {
    use rcgen::generate_simple_self_signed;
    use tokio::net::TcpListener;
    use tokio_stream::wrappers::TcpListenerStream;
    use tonic::transport::{Identity, Server, ServerTlsConfig};

    let certified = generate_simple_self_signed(["localhost".to_owned()])?;
    let certificate_pem = certified.cert.pem();
    let private_key_pem = certified.signing_key.serialize_pem();
    let listener = TcpListener::bind("127.0.0.1:0").await?;
    let endpoint = format!("https://localhost:{}", listener.local_addr()?.port());
    let (shutdown_tx, shutdown_rx) = tokio::sync::oneshot::channel();
    let server_certificate_pem = certificate_pem.clone();
    let digest = control::archived_descriptor_digest(BindingFamily::Inference);
    let server = tokio::spawn(async move {
        Server::builder()
            .tls_config(
                ServerTlsConfig::new()
                    .identity(Identity::from_pem(server_certificate_pem, private_key_pem)),
            )
            ?
            .add_service(
                acyclic_inference::control_wire::transport::v1::protocol_service_server::ProtocolServiceServer::new(
                    ControlService { digest },
                ),
            )
            .serve_with_incoming_shutdown(TcpListenerStream::new(listener), async {
                let _ = shutdown_rx.await;
            })
            .await?;
        Ok::<(), Box<dyn std::error::Error + Send + Sync>>(())
    });
    let client = client::Client::connect_with_ca(&endpoint, "fixture-token", certificate_pem.as_bytes())
        .await
        ?;
    assert_eq!(client.transport(), client::Transport::Grpc);
    let _ = shutdown_tx.send(());
    server.await??;
    Ok(())
}

async fn read_request(stream: &mut tokio::net::TcpStream) -> TestResult<String> {
    let mut bytes = Vec::new();
    loop {
        let mut chunk = [0; 1024];
        let count = stream.read(&mut chunk).await?;
        assert!(count > 0 && bytes.len() + count <= 256 * 1024);
        bytes.extend_from_slice(checked_slice(&chunk, 0..count, "request chunk")?);
        if let Some(end) = bytes.windows(4).position(|part| part == b"\r\n\r\n") {
            let header_end = end + 4;
            let headers = String::from_utf8_lossy(checked_slice(
                &bytes,
                0..header_end,
                "request headers",
            )?);
            let content_length = headers
                .lines()
                .find_map(|line| {
                    let (name, value) = line.split_once(':')?;
                    name.eq_ignore_ascii_case("content-length")
                        .then(|| value.trim().parse::<usize>().ok())
                        .flatten()
                })
                .unwrap_or(0);
            while bytes.len() < header_end + content_length {
                let count = stream.read(&mut chunk).await?;
                assert!(count > 0 && bytes.len() + count <= 256 * 1024);
                bytes.extend_from_slice(checked_slice(&chunk, 0..count, "request body chunk")?);
            }
            return Ok(String::from_utf8(bytes)?);
        }
    }
}

async fn endpoint(
    status: u16,
    digest: String,
    requests: usize,
) -> TestResult<(String, tokio::task::JoinHandle<TestResult<Vec<String>>>)> {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
    let address = listener.local_addr()?;
    let task = tokio::spawn(async move {
        let mut observed = Vec::new();
        for index in 0..requests {
            let (mut stream, _) = listener.accept().await?;
            let request = read_request(&mut stream).await?;
            assert!(request.to_ascii_lowercase().contains("authorization: bearer fixture-token\r\n"));
            observed.push(
                request
                    .lines()
                    .next()
                    .ok_or_else(|| io::Error::other("fixture request has no request line"))?
                    .to_owned(),
            );
            let body = if index == 0 {
                serde_json::json!({
                    "protocol": {
                        "version": control::control_protocol_version(BindingFamily::Inference),
                        "descriptorDigest": digest,
                    },
                    "supported": { "capabilities": [{
                        "name": BindingFamily::Inference.name(),
                        "version": control::control_protocol_version(BindingFamily::Inference),
                    }] }
                }).to_string()
            } else if request.lines().next().is_some_and(|line| line.contains("/runs/watch")) {
                "[]".to_owned()
            } else {
                "{}".to_owned()
            };
            let response = format!(
                "HTTP/1.1 {status} OK\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{}",
                body.len(), body
            );
            stream.write_all(response.as_bytes()).await?;
            stream.shutdown().await?;
        }
        Ok(observed)
    });
    Ok((format!("http://{address}"), task))
}

#[tokio::test]
async fn default_client_verifies_http_then_executes_all_fourteen_operations() -> TestResult<()> {
    let (endpoint, server) = endpoint(
        200,
        control::archived_descriptor_digest(BindingFamily::Inference),
        15,
    )
    .await?;
    let client = client::Client::connect(&endpoint, "fixture-token")
        .await
        ?;
    assert_eq!(client.transport(), client::Transport::Http);
    client.list(&wire::ListModelsRequest::default()).await?;
    client.create_context(&wire::CreateContextRequest::default()).await?;
    client.inspect_context(&wire::InspectContextRequest::default()).await?;
    client.mutate_context(&wire::MutateContextRequest::default()).await?;
    client.retain_warm(&wire::RetainWarmRequest::default()).await?;
    client.inspect_warm(&wire::InspectWarmRequest::default()).await?;
    client.renew_warm(&wire::RenewWarmRequest::default()).await?;
    client.release_warm(&wire::ReleaseWarmRequest::default()).await?;
    client.generate_run(&wire::GenerateRunRequest::default()).await?;
    client.inspect_run(&wire::InspectRunRequest::default()).await?;
    assert!(client.watch_run(&wire::WatchRunRequest::default()).await?.is_empty());
    client.cancel_run(&wire::InspectRunRequest::default()).await?;
    client.create_evaluation(&wire::CreateEvaluationRequest::default()).await?;
    client.inspect_evaluation(&wire::InspectEvaluationRequest::default()).await?;
    let observed = tokio::time::timeout(std::time::Duration::from_secs(5), server)
        .await
        ???;
    assert_eq!(observed.first().ok_or_else(|| io::Error::other("missing handshake request"))?, "GET /v1/sdk/inference/handshake HTTP/1.1");
    assert_eq!(observed.len(), 15);
    assert_eq!(observed.get(11).ok_or_else(|| io::Error::other("missing watch request"))?, "POST /runs/watch HTTP/1.1");
    Ok(())
}

#[tokio::test]
async fn incompatible_http_identity_stops_before_any_application_call() -> TestResult<()> {
    let (endpoint, server) = endpoint(200, "substituted-descriptor".into(), 1).await?;
    assert!(client::Client::connect(&endpoint, "fixture-token")
        .await
        .is_err());
    let observed = tokio::time::timeout(std::time::Duration::from_secs(5), server)
        .await
        ???;
    assert_eq!(observed, ["GET /v1/sdk/inference/handshake HTTP/1.1"]);
    Ok(())
}

#[tokio::test]
async fn failed_http_auth_stops_before_any_application_call() -> TestResult<()> {
    let (endpoint, server) = endpoint(401, control::archived_descriptor_digest(BindingFamily::Inference), 1).await?;
    assert!(client::Client::connect(&endpoint, "fixture-token")
        .await
        .is_err());
    let observed = tokio::time::timeout(std::time::Duration::from_secs(5), server)
        .await
        ???;
    assert_eq!(observed, ["GET /v1/sdk/inference/handshake HTTP/1.1"]);
    Ok(())
}
