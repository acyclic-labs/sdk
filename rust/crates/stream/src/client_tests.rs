//! Real TLS transport selection tests with connection-scoped test trust.
use super::*;
use crate::control_wire::{
    protocol::v1 as protocol,
    transport::v1::protocol_service_server::{ProtocolService, ProtocolServiceServer},
};
use acyclic_sdk_contract_wire::{BindingFamily, transport_control as control};
use futures::StreamExt;
use std::{
    error::Error,
    io,
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    },
};
use tonic::{
    Request, Response, Status,
    transport::{Identity, Server, ServerTlsConfig},
};

type TestResult<T = ()> = Result<T, Box<dyn Error + Send + Sync>>;

fn test_error(message: impl Into<String>) -> io::Error {
    io::Error::other(message.into())
}

#[derive(Clone, Copy)]
enum Mode {
    Valid,
    WrongIdentity,
    Unauthorized,
}
struct Control {
    mode: Mode,
    calls: Arc<AtomicUsize>,
}
#[tonic::async_trait]
impl ProtocolService for Control {
    async fn handshake(
        &self,
        request: Request<protocol::HandshakeRequest>,
    ) -> Result<Response<protocol::HandshakeResponse>, Status> {
        self.calls.fetch_add(1, Ordering::SeqCst);
        let authorization = request
            .metadata()
            .get("authorization")
            .ok_or_else(|| Status::unauthenticated("missing authorization metadata"))?;
        if authorization != "Bearer fixture-token" {
            return Err(Status::unauthenticated("invalid credential"));
        }
        if matches!(self.mode, Mode::Unauthorized) {
            return Err(Status::unauthenticated("invalid credential"));
        }
        let name = request
            .metadata()
            .get(control::FAMILY_METADATA_KEY)
            .ok_or_else(|| Status::invalid_argument("missing binding family metadata"))?
            .to_str()
            .map_err(|_| Status::invalid_argument("binding family metadata is not ASCII"))?;
        let family = BindingFamily::ALL
            .iter()
            .find(|family| family.name() == name)
            .copied()
            .ok_or_else(|| Status::invalid_argument("unknown binding family"))?;
        let version = control::control_protocol_version(family);
        let presented = request
            .get_ref()
            .protocol
            .as_ref()
            .ok_or_else(|| Status::invalid_argument("missing protocol identity"))?;
        if presented.version != version {
            return Err(Status::failed_precondition("protocol version mismatch"));
        }
        if presented.descriptor_digest != control::archived_descriptor_digest(family) {
            return Err(Status::failed_precondition("protocol descriptor mismatch"));
        }
        let required = request
            .get_ref()
            .required
            .as_ref()
            .ok_or_else(|| Status::invalid_argument("missing required capabilities"))?;
        if required.capabilities.len() != 1 {
            return Err(Status::invalid_argument("expected one required capability"));
        }
        let required_capability = required
            .capabilities
            .first()
            .ok_or_else(|| Status::invalid_argument("missing required capability"))?;
        if required_capability.name != family.name() || required_capability.version != version {
            return Err(Status::failed_precondition("required capability mismatch"));
        }
        Ok(Response::new(protocol::HandshakeResponse {
            protocol: Some(protocol::ProtocolIdentity {
                version: version.into(),
                descriptor_digest: match self.mode {
                    Mode::WrongIdentity => "wrong-archive".into(),
                    _ => control::archived_descriptor_digest(family),
                },
            }),
            supported: Some(protocol::CapabilitySet {
                capabilities: vec![protocol::Capability {
                    name: family.name().into(),
                    version: version.into(),
                }],
            }),
        }))
    }
}

async fn endpoint(
    mode: Mode,
) -> TestResult<(
    String,
    String,
    Arc<AtomicUsize>,
    tokio::sync::oneshot::Sender<()>,
    tokio::task::JoinHandle<Result<(), tonic::transport::Error>>,
)> {
    let certificate = rcgen::generate_simple_self_signed([
        "localhost".to_owned(),
        "127.0.0.1".to_owned(),
    ])?;
    let pem = certificate.cert.pem();
    let identity = Identity::from_pem(pem.clone(), certificate.signing_key.serialize_pem());
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
    let endpoint = format!("https://127.0.0.1:{}", listener.local_addr()?.port());
    let calls = Arc::new(AtomicUsize::new(0));
    let service = Control {
        mode,
        calls: calls.clone(),
    };
    let (shutdown, receiver) = tokio::sync::oneshot::channel();
    let (ready, ready_receiver) = tokio::sync::oneshot::channel();
    let incoming = futures::stream::unfold(
        (listener, Some(ready)),
        |(listener, ready)| async move {
            if let Some(ready) = ready {
                let _ = ready.send(());
            }
            Some((
                listener.accept().await.map(|(stream, _)| stream),
                (listener, None),
            ))
        },
    );
    let server = tokio::spawn(async move {
        Server::builder()
            .tls_config(ServerTlsConfig::new().identity(identity))?
            .add_service(ProtocolServiceServer::new(service))
            .add_service(crate::wire::stream_service_server::StreamServiceServer::new(
                crate::grpc::Service::new(Arc::new(crate::MemoryStream::default())),
            ))
            .serve_with_incoming_shutdown(incoming, async {
                let _ = receiver.await;
            })
            .await
    });
    // Wait until tonic has entered its accept loop. Without this barrier the
    // first control probe can race server startup and be mistaken for an
    // endpoint without gRPC support, causing an HTTP fallback.
    ready_receiver
        .await
        .map_err(|_| test_error("TLS test server stopped before becoming ready"))?;
    Ok((endpoint, pem, calls, shutdown, server))
}

#[tokio::test]
async fn native_prefers_verified_grpc_and_preserves_stream_behavior() -> TestResult {
    let (endpoint, ca, calls, shutdown, server) = endpoint(Mode::Valid).await?;
    let client = connect_with_trust(&endpoint, "fixture-token", Some(ca.as_bytes())).await?;
    assert_eq!(client.transport(), "grpc");
    let stream = client
        .stream("fixture")
        .map_err(|error| test_error(format!("stream setup failed: {error}")))?;
    if !matches!(stream.tail().await, Err(crate::StreamError::NotFound)) {
        return Err(test_error("missing stream did not return NotFound").into());
    }
    stream
        .append(bytes::Bytes::from_static(b"rust-owned"))
        .await?;
    assert_eq!(stream.tail().await?, 1);
    let mut records = stream.read(0, 1).await?;
    let record = records
        .next()
        .await
        .ok_or_else(|| test_error("stream returned no record"))??;
    assert_eq!(record.sequence, 0);
    assert_eq!(record.value, bytes::Bytes::from_static(b"rust-owned"));
    assert!(records.next().await.is_none());
    assert_eq!(calls.load(Ordering::SeqCst), 1);
    shutdown
        .send(())
        .map_err(|_| test_error("TLS test server shutdown receiver dropped"))?;
    let server_result = tokio::time::timeout(std::time::Duration::from_secs(5), server).await??;
    server_result?;
    Ok(())
}

#[tokio::test]
async fn native_identity_and_authentication_failures_are_terminal() -> TestResult {
    for mode in [Mode::WrongIdentity, Mode::Unauthorized] {
        let (endpoint, ca, calls, shutdown, server) = endpoint(mode).await?;
        let Err(error) = connect_with_trust(&endpoint, "fixture-token", Some(ca.as_bytes())).await else {
            return Err(test_error("invalid handshake unexpectedly succeeded").into());
        };
        assert!(matches!(error, ConnectError::Negotiation(_)));
        assert_eq!(calls.load(Ordering::SeqCst), 1);
        shutdown
            .send(())
            .map_err(|_| test_error("TLS test server shutdown receiver dropped"))?;
        let server_result = tokio::time::timeout(std::time::Duration::from_secs(5), server).await??;
        server_result?;
    }
    Ok(())
}
