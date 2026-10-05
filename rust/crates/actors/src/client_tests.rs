//! Real TLS transport selection tests with connection-scoped test trust.
use super::*;
use crate::control_wire::{
    protocol::v1 as protocol,
    transport::v1::protocol_service_server::{ProtocolService, ProtocolServiceServer},
};
use acyclic_sdk_contract_wire::{BindingFamily, transport_control as control};
use std::sync::{
    Arc,
    atomic::{AtomicUsize, Ordering},
};
use tonic::{
    Request, Response, Status,
    transport::{Identity, Server, ServerTlsConfig},
};

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
        assert_eq!(
            request.metadata().get("authorization").unwrap(),
            "Bearer fixture-token"
        );
        if matches!(self.mode, Mode::Unauthorized) {
            return Err(Status::unauthenticated("invalid credential"));
        }
        let name = request
            .metadata()
            .get(control::FAMILY_METADATA_KEY)
            .unwrap()
            .to_str()
            .unwrap();
        let family = *BindingFamily::ALL
            .iter()
            .find(|family| family.name() == name)
            .unwrap();
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
        assert_eq!(
            request.metadata().get("authorization").unwrap(),
            "Bearer fixture-token"
        );
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
        assert_eq!(
            request.metadata().get("authorization").unwrap(),
            "Bearer fixture-token"
        );
        Err(Status::unimplemented("fixture operation rejected"))
    }
    async fn update_actor(
        &self,
        request: Request<crate::wire::UpdateActorRequest>,
    ) -> Result<Response<crate::wire::UpdateActorResponse>, Status> {
        assert_eq!(
            request.metadata().get("authorization").unwrap(),
            "Bearer fixture-token"
        );
        Err(Status::unimplemented("fixture operation rejected"))
    }
    async fn add_subscription(
        &self,
        request: Request<crate::wire::AddSubscriptionRequest>,
    ) -> Result<Response<crate::wire::AddSubscriptionResponse>, Status> {
        assert_eq!(
            request.metadata().get("authorization").unwrap(),
            "Bearer fixture-token"
        );
        Err(Status::unimplemented("fixture operation rejected"))
    }
    async fn remove_subscription(
        &self,
        request: Request<crate::wire::RemoveSubscriptionRequest>,
    ) -> Result<Response<crate::wire::RemoveSubscriptionResponse>, Status> {
        assert_eq!(
            request.metadata().get("authorization").unwrap(),
            "Bearer fixture-token"
        );
        Err(Status::unimplemented("fixture operation rejected"))
    }
    async fn resume_subscription(
        &self,
        request: Request<crate::wire::ResumeSubscriptionRequest>,
    ) -> Result<Response<crate::wire::ResumeSubscriptionResponse>, Status> {
        assert_eq!(
            request.metadata().get("authorization").unwrap(),
            "Bearer fixture-token"
        );
        Err(Status::unimplemented("fixture operation rejected"))
    }
    async fn checkpoint_actor(
        &self,
        request: Request<crate::wire::CheckpointActorRequest>,
    ) -> Result<Response<crate::wire::CheckpointActorResponse>, Status> {
        assert_eq!(
            request.metadata().get("authorization").unwrap(),
            "Bearer fixture-token"
        );
        Err(Status::unimplemented("fixture operation rejected"))
    }
    async fn invoke_actor(
        &self,
        request: Request<crate::wire::InvokeActorRequest>,
    ) -> Result<Response<crate::wire::InvokeActorResponse>, Status> {
        assert_eq!(
            request.metadata().get("authorization").unwrap(),
            "Bearer fixture-token"
        );
        Err(Status::unimplemented("fixture operation rejected"))
    }
}

async fn endpoint(
    mode: Mode,
) -> (
    String,
    String,
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
            .unwrap()
            .add_service(ProtocolServiceServer::new(service))
            .add_service(crate::wire::actors_service_server::ActorsServiceServer::new(ActorFixture))
            .serve_with_incoming_shutdown(incoming, async {
                let _ = receiver.await;
            })
            .await
    });
    (endpoint, pem, calls, shutdown, server)
}

#[tokio::test]
async fn native_prefers_verified_grpc_and_never_replays_an_application_error() {
    let (endpoint, ca, calls, shutdown, server) = endpoint(Mode::Valid).await;
    let client = connect_with_trust(&endpoint, "fixture-token", Some(ca.as_bytes()))
        .await
        .unwrap();
    assert_eq!(client.transport(), "grpc");
    assert_eq!(calls.load(Ordering::SeqCst), 1);
    let actor = client
        .inspect_actor(&crate::wire::InspectActorRequest {
            actor_id: "fixture-actor".into(),
        })
        .await
        .unwrap()
        .actor
        .unwrap();
    assert_eq!(actor.actor_id, "fixture-actor");
    assert_eq!(actor.home_region, "eu");
    assert_eq!(actor.state, crate::wire::ActorState::Active as i32);
    let error = client
        .create_actor(&crate::wire::CreateActorRequest::default())
        .await
        .unwrap_err();
    assert!(matches!(
        error,
        Error::Service {
            grpc_code: Some(12),
            http_status: None,
            ..
        }
    ));
    assert_eq!(calls.load(Ordering::SeqCst), 1);
    shutdown.send(()).unwrap();
    tokio::time::timeout(std::time::Duration::from_secs(5), server)
        .await
        .unwrap()
        .unwrap()
        .unwrap();
}

#[tokio::test]
async fn workers_verify_the_same_rust_control_contract_over_tls() {
    let (endpoint, ca, calls, shutdown, server) = endpoint(Mode::Valid).await;
    assert!(
        acyclic_workers::grpc::connect_verified_with_ca_certificate(
            &endpoint,
            "fixture-token",
            Some(ca.as_bytes())
        )
        .await
        .unwrap()
        .is_some()
    );
    assert_eq!(calls.load(Ordering::SeqCst), 1);
    shutdown.send(()).unwrap();
    tokio::time::timeout(std::time::Duration::from_secs(5), server)
        .await
        .unwrap()
        .unwrap()
        .unwrap();
}

#[tokio::test]
async fn native_identity_and_authentication_failures_are_terminal() {
    for (mode, expected) in [
        (Mode::WrongIdentity, "DescriptorMismatch"),
        (Mode::Unauthorized, "invalid credential"),
    ] {
        let (endpoint, ca, calls, shutdown, server) = endpoint(mode).await;
        let error = connect_with_trust(&endpoint, "fixture-token", Some(ca.as_bytes()))
            .await
            .err()
            .unwrap();
        assert!(matches!(&error, Error::Configuration(_)));
        assert!(error.to_string().contains(expected), "{error}");
        assert_eq!(calls.load(Ordering::SeqCst), 1);
        shutdown.send(()).unwrap();
        tokio::time::timeout(std::time::Duration::from_secs(5), server)
            .await
            .unwrap()
            .unwrap()
            .unwrap();
    }
}
