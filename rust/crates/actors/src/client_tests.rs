//! Real TLS transport selection tests with connection-scoped test trust.
use super::*;
use crate::control_wire::{
    protocol::v1 as protocol,
    transport::v1::protocol_service_server::{ProtocolService, ProtocolServiceServer},
};
use acyclic_sdk_contract_wire::{BindingFamily, transport_control as control};
use std::{
    error::Error as StdError,
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

type TestResult<T> = Result<T, Box<dyn StdError + Send + Sync>>;

fn assert_fixture_auth<T>(request: &Request<T>) {
    assert_eq!(
        request
            .metadata()
            .get("authorization")
            .and_then(|value| value.to_str().ok()),
        Some("Bearer fixture-token")
    );
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
        assert_fixture_auth(&request);
        if matches!(self.mode, Mode::Unauthorized) {
            return Err(Status::unauthenticated("invalid credential"));
        }
        let name = request
            .metadata()
            .get(control::FAMILY_METADATA_KEY)
            .ok_or_else(|| Status::invalid_argument("fixture family metadata is missing"))?
            .to_str()
            .map_err(|_| Status::invalid_argument("fixture family metadata is invalid"))?;
        let family = *BindingFamily::ALL
            .iter()
            .find(|family| family.name() == name)
            .ok_or_else(|| Status::invalid_argument("fixture family metadata is unknown"))?;
        let version = control::control_protocol_version(family);
        let presented = request
            .get_ref()
            .protocol
            .as_ref()
            .ok_or_else(|| Status::invalid_argument("fixture protocol identity is missing"))?;
        assert_eq!(presented.version, version);
        assert_eq!(
            presented.descriptor_digest,
            control::archived_descriptor_digest(family)
        );
        let required = &request
            .get_ref()
            .required
            .as_ref()
            .ok_or_else(|| Status::invalid_argument("fixture required capabilities are missing"))?
            .capabilities;
        assert_eq!(required.len(), 1);
        let required_capability = required
            .first()
            .ok_or_else(|| Status::invalid_argument("fixture required capability is missing"))?;
        assert_eq!(required_capability.name, family.name());
        assert_eq!(required_capability.version, version);
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

struct ActorFixture;
#[tonic::async_trait]
impl crate::wire::actors_service_server::ActorsService for ActorFixture {
    async fn inspect_actor(
        &self,
        request: Request<crate::wire::InspectActorRequest>,
    ) -> Result<Response<crate::wire::InspectActorResponse>, Status> {
        assert_fixture_auth(&request);
        Ok(Response::new(crate::wire::InspectActorResponse {
            actor: Some(crate::wire::ActorObservation {
                actor_id: request.into_inner().actor_id,
                home_region: "eu".into(),
                state: crate::wire::ActorState::Active as i32,
                ..Default::default()
            }),
        }))
    }
    async fn create_actor(
        &self,
        request: Request<crate::wire::CreateActorRequest>,
    ) -> Result<Response<crate::wire::CreateActorResponse>, Status> {
        assert_fixture_auth(&request);
        Err(Status::unimplemented("fixture operation rejected"))
    }
    async fn update_actor(
        &self,
        request: Request<crate::wire::UpdateActorRequest>,
    ) -> Result<Response<crate::wire::UpdateActorResponse>, Status> {
        assert_fixture_auth(&request);
        Err(Status::unimplemented("fixture operation rejected"))
    }
    async fn add_subscription(
        &self,
        request: Request<crate::wire::AddSubscriptionRequest>,
    ) -> Result<Response<crate::wire::AddSubscriptionResponse>, Status> {
        assert_fixture_auth(&request);
        Err(Status::unimplemented("fixture operation rejected"))
    }
    async fn remove_subscription(
        &self,
        request: Request<crate::wire::RemoveSubscriptionRequest>,
    ) -> Result<Response<crate::wire::RemoveSubscriptionResponse>, Status> {
        assert_fixture_auth(&request);
        Err(Status::unimplemented("fixture operation rejected"))
    }
    async fn resume_subscription(
        &self,
        request: Request<crate::wire::ResumeSubscriptionRequest>,
    ) -> Result<Response<crate::wire::ResumeSubscriptionResponse>, Status> {
        assert_fixture_auth(&request);
        Err(Status::unimplemented("fixture operation rejected"))
    }
    async fn checkpoint_actor(
        &self,
        request: Request<crate::wire::CheckpointActorRequest>,
    ) -> Result<Response<crate::wire::CheckpointActorResponse>, Status> {
        assert_fixture_auth(&request);
        Err(Status::unimplemented("fixture operation rejected"))
    }
    async fn invoke_actor(
        &self,
        request: Request<crate::wire::InvokeActorRequest>,
    ) -> Result<Response<crate::wire::InvokeActorResponse>, Status> {
        assert_fixture_auth(&request);
        Err(Status::unimplemented("fixture operation rejected"))
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
)>
{
    let certificate =
        rcgen::generate_simple_self_signed(["localhost".to_owned(), "127.0.0.1".to_owned()])
            ?;
    let pem = certificate.cert.pem();
    let identity = Identity::from_pem(pem.clone(), certificate.signing_key.serialize_pem());
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
    let endpoint = format!(
        "https://127.0.0.1:{}",
        listener.local_addr()?.port()
    );
    let calls = Arc::new(AtomicUsize::new(0));
    let service = Control {
        mode,
        calls: calls.clone(),
    };
    let (shutdown, receiver) = tokio::sync::oneshot::channel();
    let incoming = futures::stream::unfold(listener, |listener| async {
        Some((listener.accept().await.map(|(stream, _)| stream), listener))
    });
    let server = tokio::spawn(async move {
        Server::builder()
            .tls_config(ServerTlsConfig::new().identity(identity))
            ?
            .add_service(ProtocolServiceServer::new(service))
            .add_service(crate::wire::actors_service_server::ActorsServiceServer::new(ActorFixture))
            .serve_with_incoming_shutdown(incoming, async {
                let _ = receiver.await;
            })
            .await
    });
    Ok((endpoint, pem, calls, shutdown, server))
}

async fn http_endpoint() -> TestResult<(String, tokio::task::JoinHandle<TestResult<Vec<String>>>)> {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
    let address = listener.local_addr()?;
    let server = tokio::spawn(async move {
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
        let family = BindingFamily::Actors;
        let version = control::control_protocol_version(family);
        let body = serde_json::json!({
            "protocol": {
                "version": version,
                "descriptorDigest": control::archived_descriptor_digest(family),
            },
            "supported": {
                "capabilities": [{"name": family.name(), "version": version}],
            },
        })
        .to_string();
        let response = format!(
            "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{}",
            body.len(), body
        );
        stream.write_all(response.as_bytes()).await?;
        stream.shutdown().await?;
        let request_line = request
            .lines()
            .next()
            .ok_or_else(|| io::Error::other("fixture request omitted its request line"))?;
        Ok::<Vec<String>, Box<dyn StdError + Send + Sync>>(vec![request_line.to_owned()])
    });
    Ok((format!("http://{address}"), server))
}

#[tokio::test]
async fn native_chooses_verified_http_without_consumer_feature_flags() -> TestResult<()> {
    let (endpoint, server) = http_endpoint().await?;
    let client = crate::connect(&endpoint, "fixture-token").await?;
    assert_eq!(client.transport(), "http");
    let observed = tokio::time::timeout(std::time::Duration::from_secs(5), server).await???;
    assert_eq!(observed, ["GET /v1/sdk/actors/handshake HTTP/1.1"]);
    Ok(())
}

#[tokio::test]
async fn native_prefers_verified_grpc_and_never_replays_an_application_error() -> TestResult<()> {
    let (endpoint, ca, calls, shutdown, server) = endpoint(Mode::Valid).await?;
    let client = connect_with_trust(&endpoint, "fixture-token", Some(ca.as_bytes()))
        .await?;
    assert_eq!(client.transport(), "grpc");
    assert_eq!(calls.load(Ordering::SeqCst), 1);
    let actor = client
        .inspect_actor(&crate::wire::InspectActorRequest {
            actor_id: "fixture-actor".into(),
        })
        .await?
        .actor
        .ok_or_else(|| io::Error::other("fixture response omitted the actor"))?;
    assert_eq!(actor.actor_id, "fixture-actor");
    assert_eq!(actor.home_region, "eu");
    assert_eq!(actor.state, crate::wire::ActorState::Active as i32);
    let Err(error) = client
        .create_actor(&crate::wire::CreateActorRequest::default())
        .await
    else {
        return Err(io::Error::other("fixture operation unexpectedly succeeded").into());
    };
    assert!(matches!(
        error,
        Error::Service {
            grpc_code: Some(12),
            http_status: None,
            ..
        }
    ));
    assert_eq!(calls.load(Ordering::SeqCst), 1);
    shutdown
        .send(())
        .map_err(|_| io::Error::other("fixture server shutdown receiver dropped"))?;
    tokio::time::timeout(std::time::Duration::from_secs(5), server).await???;
    Ok(())
}

#[tokio::test]
async fn workers_verify_the_same_rust_control_contract_over_tls() -> TestResult<()> {
    let (endpoint, ca, calls, shutdown, server) = endpoint(Mode::Valid).await?;
    let verified = acyclic_workers::grpc::connect_verified_with_ca_certificate(
        &endpoint,
        "fixture-token",
        Some(ca.as_bytes()),
    )
    .await?;
    assert!(verified.is_some());
    assert_eq!(calls.load(Ordering::SeqCst), 1);
    shutdown
        .send(())
        .map_err(|_| io::Error::other("fixture server shutdown receiver dropped"))?;
    tokio::time::timeout(std::time::Duration::from_secs(5), server).await???;
    Ok(())
}

#[tokio::test]
async fn native_identity_and_authentication_failures_are_terminal() -> TestResult<()> {
    for (mode, expected) in [
        (Mode::WrongIdentity, "DescriptorMismatch"),
        (Mode::Unauthorized, "invalid credential"),
    ] {
        let (endpoint, ca, calls, shutdown, server) = endpoint(mode).await?;
        let Err(error) =
            connect_with_trust(&endpoint, "fixture-token", Some(ca.as_bytes())).await
        else {
            return Err(io::Error::other(format!(
                "fixture connection unexpectedly succeeded for {expected}"
            ))
            .into());
        };
        assert!(matches!(&error, Error::Configuration(_)));
        assert!(error.to_string().contains(expected), "{error}");
        assert_eq!(calls.load(Ordering::SeqCst), 1);
        shutdown
            .send(())
            .map_err(|_| io::Error::other("fixture server shutdown receiver dropped"))?;
        tokio::time::timeout(std::time::Duration::from_secs(5), server).await???;
    }
    Ok(())
}
