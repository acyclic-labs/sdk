//! Local RSA certificate and Machines RPC fixture support.
//!
//! The fixture is intended for installed language consumers. It uses a private
//! ephemeral RSA CA, signs separate server and client certificates, and enables
//! mutual TLS with normal certificate and hostname verification. It is local
//! qualification evidence only; it does not describe a hosted service.

use std::pin::Pin;
use std::sync::{Arc, Mutex};

use acyclic_machines::wire;
use futures::{Stream, stream};
use prost::Message;
use rcgen::{
    BasicConstraints, CertificateParams, ExtendedKeyUsagePurpose, IsCa, Issuer, KeyPair,
    KeyUsagePurpose, PKCS_RSA_SHA256, RsaKeySize,
};
use sha2::{Digest, Sha256};
use tokio::net::TcpListener;
use tokio::sync::oneshot;
use tokio_stream::wrappers::TcpListenerStream;
use tonic::transport::{Certificate, Identity, Server, ServerTlsConfig};
use tonic::{Request, Response, Status};

/// Ephemeral RSA certificate material for an authenticated local fixture.
#[derive(Clone, Debug)]
pub struct RsaTlsMaterial {
    /// PEM encoded certificate authority.
    pub ca_certificate: String,
    /// PEM encoded server certificate.
    pub server_certificate: String,
    /// PEM encoded server private key.
    pub server_private_key: String,
    /// PEM encoded client certificate.
    pub client_certificate: String,
    /// PEM encoded client private key.
    pub client_private_key: String,
}

impl RsaTlsMaterial {
    /// Generates a fresh RSA-2048 CA, server certificate, and client certificate.
    pub fn generate() -> Result<Self, rcgen::Error> {
        let mut ca_params = CertificateParams::new(Vec::<String>::new())?;
        ca_params.is_ca = IsCa::Ca(BasicConstraints::Unconstrained);
        ca_params.key_usages = vec![
            KeyUsagePurpose::DigitalSignature,
            KeyUsagePurpose::KeyCertSign,
        ];
        let ca_key = KeyPair::generate_rsa_for(&PKCS_RSA_SHA256, RsaKeySize::_2048)?;
        let ca_certificate = ca_params.self_signed(&ca_key)?;
        let issuer = Issuer::new(ca_params, ca_key);

        let mut server_params = CertificateParams::new(vec!["localhost".to_owned()])?;
        server_params.use_authority_key_identifier_extension = true;
        server_params.key_usages = vec![
            KeyUsagePurpose::DigitalSignature,
            KeyUsagePurpose::KeyEncipherment,
        ];
        server_params.extended_key_usages = vec![ExtendedKeyUsagePurpose::ServerAuth];
        let server_key = KeyPair::generate_rsa_for(&PKCS_RSA_SHA256, RsaKeySize::_2048)?;
        let server_certificate = server_params.signed_by(&server_key, &issuer)?;

        let mut client_params = CertificateParams::new(Vec::<String>::new())?;
        client_params.use_authority_key_identifier_extension = true;
        client_params.key_usages = vec![
            KeyUsagePurpose::DigitalSignature,
            KeyUsagePurpose::KeyEncipherment,
        ];
        client_params.extended_key_usages = vec![ExtendedKeyUsagePurpose::ClientAuth];
        let client_key = KeyPair::generate_rsa_for(&PKCS_RSA_SHA256, RsaKeySize::_2048)?;
        let client_certificate = client_params.signed_by(&client_key, &issuer)?;

        Ok(Self {
            ca_certificate: ca_certificate.pem(),
            server_certificate: server_certificate.pem(),
            server_private_key: server_key.serialize_pem(),
            client_certificate: client_certificate.pem(),
            client_private_key: client_key.serialize_pem(),
        })
    }
}

/// Stable IDs used by the all-method local Machines fixture.
pub const FIXTURE_MACHINE: [u8; 16] = [1; 16];
/// Stable operation ID used by the all-method local Machines fixture.
pub const FIXTURE_OPERATION: [u8; 16] = [2; 16];
/// Stable checkpoint ID used by the all-method local Machines fixture.
pub const FIXTURE_CHECKPOINT: [u8; 16] = [3; 16];

/// Every modeled Machines RPC that the bounded fixture implements.
///
/// Keep this list next to the service implementation so a qualification
/// receipt can distinguish a complete client probe from a partial smoke test.
pub const MACHINES_RPC_METHODS: &[&str] = &[
    "acyclic.machines.v1.MachinesService/QualifyImage",
    "acyclic.machines.v1.MachinesService/Create",
    "acyclic.machines.v1.MachinesService/Checkpoint",
    "acyclic.machines.v1.MachinesService/Fork",
    "acyclic.machines.v1.MachinesService/ForkMachine",
    "acyclic.machines.v1.MachinesService/Suspend",
    "acyclic.machines.v1.MachinesService/Wake",
    "acyclic.machines.v1.MachinesService/SetSuspensionPolicy",
    "acyclic.machines.v1.MachinesService/DestroyMachine",
    "acyclic.machines.v1.MachinesService/DestroyCheckpoint",
    "acyclic.machines.v1.MachinesService/Recover",
    "acyclic.machines.v1.MachinesService/InspectMachine",
    "acyclic.machines.v1.MachinesService/InspectCheckpoint",
    "acyclic.machines.v1.MachinesService/ListMachines",
    "acyclic.machines.v1.MachinesService/Events",
    "acyclic.machines.v1.MachinesService/Usage",
    "acyclic.machines.v1.MachinesService/Cancel",
    "acyclic.machines.v1.MachinesService/InspectOperation",
    "acyclic.machines.v1.MachinesService/WatchOperation",
];

/// One deterministic request/response observation from an installed client.
///
/// The hashes let the fixture prove that a concrete protobuf request reached a
/// concrete RPC and produced a concrete protobuf response without retaining
/// credentials or user payloads in the evidence artifact.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct MethodTranscript {
    /// Fully qualified generated RPC identity.
    pub rpc: &'static str,
    /// Number of encoded request bytes received by the fixture.
    pub request_bytes: usize,
    /// SHA-256 of the encoded request message.
    pub request_sha256: String,
    /// Number of encoded response bytes emitted by the fixture.
    pub response_bytes: usize,
    /// SHA-256 of the encoded response message, or the concatenated stream messages.
    pub response_sha256: String,
}

/// Shared append-only transcript used by the bounded local fixture.
pub type MethodTranscriptLog = Arc<Mutex<Vec<MethodTranscript>>>;

/// Creates an empty transcript for one fixture process.
pub fn new_method_transcript_log() -> MethodTranscriptLog {
    Arc::new(Mutex::new(Vec::new()))
}

fn digest(bytes: &[u8]) -> String {
    format!("sha256:{:x}", Sha256::digest(bytes))
}

fn record_transcript(
    log: &MethodTranscriptLog,
    rpc: &'static str,
    request_bytes: Vec<u8>,
    response_bytes: Vec<u8>,
) {
    log.lock()
        .expect("Machines fixture transcript mutex poisoned")
        .push(MethodTranscript {
            rpc,
            request_bytes: request_bytes.len(),
            request_sha256: digest(&request_bytes),
            response_bytes: response_bytes.len(),
            response_sha256: digest(&response_bytes),
        });
}

fn traced_response<Req, Resp>(
    log: &MethodTranscriptLog,
    rpc: &'static str,
    request: &Req,
    response: Resp,
) -> Response<Resp>
where
    Req: prost::Message,
    Resp: prost::Message,
{
    record_transcript(log, rpc, request.encode_to_vec(), response.encode_to_vec());
    Response::new(response)
}

fn machine(value: [u8; 16]) -> wire::MachineId {
    wire::MachineId {
        value: value.to_vec(),
    }
}
fn operation(value: [u8; 16]) -> wire::OperationId {
    wire::OperationId {
        value: value.to_vec(),
    }
}
fn checkpoint(value: [u8; 16]) -> wire::CheckpointId {
    wire::CheckpointId {
        value: value.to_vec(),
    }
}

fn contract() -> wire::MachineContract {
    wire::MachineContract {
        image: Some(wire::Image {
            kind: wire::ImageKind::Custom as i32,
            immutable_reference: Some(wire::image::ImmutableReference::CustomDigest(vec![7; 32])),
        }),
        capabilities: vec![
            wire::Capability::LiveCheckpoint as i32,
            wire::Capability::LiveFork as i32,
        ],
        compatibility: Some(wire::CompatibilityPolicy {
            mode: wire::CompatibilityMode::BestEffort as i32,
            required: Vec::new(),
        }),
        compatibility_revision: vec![8; 32],
        suspension: Some(wire::SuspensionPolicy {
            policy: Some(wire::suspension_policy::Policy::Manual(true)),
        }),
        expiration: Some(wire::ExpirationPolicy {
            kind: wire::ExpirationKind::Never as i32,
            value_ms: 0,
        }),
        network_policy_digest: vec![9; 32],
        budgets: Some(wire::Budgets {
            spend_micros: 0,
            concurrency: 1,
        }),
    }
}

fn state(value: [u8; 16]) -> wire::MachineState {
    wire::MachineState {
        machine: Some(machine(value)),
        status: wire::MachineStatus::Running as i32,
        contract: Some(contract()),
        endpoints: Vec::new(),
        last_checkpoint: Some(checkpoint(FIXTURE_CHECKPOINT)),
        created_at_unix_ms: 1,
        changed_at_unix_ms: 1,
    }
}

fn admission(machine_value: [u8; 16]) -> wire::MachineAdmission {
    wire::MachineAdmission {
        machine: Some(machine(machine_value)),
        operation: Some(operation(FIXTURE_OPERATION)),
        contract: Some(contract()),
    }
}

/// A deterministic service that responds on every generated Machines RPC.
///
/// Each method returns a typed response derived from the request identity. This
/// makes installed client probes exercise the complete generated surface while
/// keeping the fixture independent of hosted service availability.
#[derive(Clone)]
pub struct AllRoutesMachinesFixture {
    transcript: MethodTranscriptLog,
}

impl Default for AllRoutesMachinesFixture {
    fn default() -> Self {
        Self {
            transcript: new_method_transcript_log(),
        }
    }
}

impl AllRoutesMachinesFixture {
    /// Builds the fixture with a caller-owned transcript sink.
    pub fn with_transcript(transcript: MethodTranscriptLog) -> Self {
        Self { transcript }
    }
}

#[tonic::async_trait]
impl wire::machines_service_server::MachinesService for AllRoutesMachinesFixture {
    async fn qualify_image(
        &self,
        request: Request<wire::QualifyImageRequest>,
    ) -> Result<Response<wire::ImageQualification>, Status> {
        let request = request.into_inner();
        let response = wire::ImageQualification {
            image: request.image.clone(),
            capabilities: vec![wire::Capability::LiveCheckpoint as i32],
            compatibility_revision: vec![8; 32],
        };
        Ok(traced_response(
            &self.transcript,
            "acyclic.machines.v1.MachinesService/QualifyImage",
            &request,
            response,
        ))
    }
    async fn create(
        &self,
        request: Request<wire::CreateMachineRequest>,
    ) -> Result<Response<wire::MachineAdmission>, Status> {
        let value = request.into_inner();
        let capabilities = value
            .compatibility
            .as_ref()
            .map_or_else(Vec::new, |policy| policy.required.clone());
        let response = wire::MachineAdmission {
            machine: Some(machine(FIXTURE_MACHINE)),
            operation: Some(operation(FIXTURE_OPERATION)),
            contract: Some(wire::MachineContract {
                image: value.image.clone(),
                capabilities,
                compatibility: value.compatibility.clone(),
                compatibility_revision: vec![8; 32],
                suspension: value.suspension.clone(),
                expiration: value.expiration.clone(),
                network_policy_digest: value.network_policy_digest.clone(),
                budgets: value.budgets.clone(),
            }),
        };
        Ok(traced_response(
            &self.transcript,
            "acyclic.machines.v1.MachinesService/Create",
            &value,
            response,
        ))
    }
    async fn checkpoint(
        &self,
        request: Request<wire::CheckpointMachineRequest>,
    ) -> Result<Response<wire::CheckpointAdmission>, Status> {
        let value = request.into_inner();
        let response = wire::CheckpointAdmission {
            checkpoint: Some(checkpoint(FIXTURE_CHECKPOINT)),
            source: value.machine.clone(),
            operation: Some(operation(FIXTURE_OPERATION)),
            contract: Some(contract()),
        };
        Ok(traced_response(
            &self.transcript,
            "acyclic.machines.v1.MachinesService/Checkpoint",
            &value,
            response,
        ))
    }
    async fn fork(
        &self,
        request: Request<wire::ForkCheckpointRequest>,
    ) -> Result<Response<wire::ForkAdmission>, Status> {
        let value = request.into_inner();
        let response = wire::ForkAdmission {
            checkpoint: value.checkpoint.clone(),
            children: vec![machine([4; 16]), machine([5; 16])],
            operation: Some(operation(FIXTURE_OPERATION)),
            contract: Some(contract()),
        };
        Ok(traced_response(
            &self.transcript,
            "acyclic.machines.v1.MachinesService/Fork",
            &value,
            response,
        ))
    }
    async fn fork_machine(
        &self,
        request: Request<wire::ForkMachineRequest>,
    ) -> Result<Response<wire::ForkMachineAdmission>, Status> {
        let value = request.into_inner();
        let response = wire::ForkMachineAdmission {
            source: value.machine.clone(),
            children: vec![machine([4; 16]), machine([5; 16])],
            operation: Some(operation(FIXTURE_OPERATION)),
            contract: Some(contract()),
            fidelity: wire::ForkFidelity::MemoryAndDisk as i32,
        };
        Ok(traced_response(
            &self.transcript,
            "acyclic.machines.v1.MachinesService/ForkMachine",
            &value,
            response,
        ))
    }
    async fn suspend(
        &self,
        request: Request<wire::MachineMutationRequest>,
    ) -> Result<Response<wire::MutationAdmission>, Status> {
        let value = request.into_inner();
        let response = wire::MutationAdmission {
            operation: Some(operation(FIXTURE_OPERATION)),
            machine: value.machine.clone(),
            checkpoint: None,
        };
        Ok(traced_response(
            &self.transcript,
            "acyclic.machines.v1.MachinesService/Suspend",
            &value,
            response,
        ))
    }
    async fn wake(
        &self,
        request: Request<wire::MachineMutationRequest>,
    ) -> Result<Response<wire::MutationAdmission>, Status> {
        let value = request.into_inner();
        let response = wire::MutationAdmission {
            operation: Some(operation(FIXTURE_OPERATION)),
            machine: value.machine.clone(),
            checkpoint: None,
        };
        Ok(traced_response(
            &self.transcript,
            "acyclic.machines.v1.MachinesService/Wake",
            &value,
            response,
        ))
    }
    async fn set_suspension_policy(
        &self,
        request: Request<wire::SetSuspensionPolicyRequest>,
    ) -> Result<Response<wire::PolicyAdmission>, Status> {
        let value = request.into_inner();
        let response = wire::PolicyAdmission {
            machine: value.machine.clone(),
            operation: Some(operation(FIXTURE_OPERATION)),
            policy: value.policy.clone(),
        };
        Ok(traced_response(
            &self.transcript,
            "acyclic.machines.v1.MachinesService/SetSuspensionPolicy",
            &value,
            response,
        ))
    }
    async fn destroy_machine(
        &self,
        request: Request<wire::MachineMutationRequest>,
    ) -> Result<Response<wire::MutationAdmission>, Status> {
        let value = request.into_inner();
        let response = wire::MutationAdmission {
            operation: Some(operation(FIXTURE_OPERATION)),
            machine: value.machine.clone(),
            checkpoint: None,
        };
        Ok(traced_response(
            &self.transcript,
            "acyclic.machines.v1.MachinesService/DestroyMachine",
            &value,
            response,
        ))
    }
    async fn destroy_checkpoint(
        &self,
        request: Request<wire::CheckpointMutationRequest>,
    ) -> Result<Response<wire::MutationAdmission>, Status> {
        let value = request.into_inner();
        let response = wire::MutationAdmission {
            operation: Some(operation(FIXTURE_OPERATION)),
            machine: None,
            checkpoint: value.checkpoint.clone(),
        };
        Ok(traced_response(
            &self.transcript,
            "acyclic.machines.v1.MachinesService/DestroyCheckpoint",
            &value,
            response,
        ))
    }
    async fn recover(
        &self,
        request: Request<wire::RecoverRequest>,
    ) -> Result<Response<wire::RecoveredAdmission>, Status> {
        let value = request.into_inner();
        let response = wire::RecoveredAdmission {
            operation: Some(operation(FIXTURE_OPERATION)),
            result: Some(wire::recovered_admission::Result::Create(admission(
                FIXTURE_MACHINE,
            ))),
        };
        Ok(traced_response(
            &self.transcript,
            "acyclic.machines.v1.MachinesService/Recover",
            &value,
            response,
        ))
    }
    async fn inspect_machine(
        &self,
        request: Request<wire::InspectMachineRequest>,
    ) -> Result<Response<wire::MachineState>, Status> {
        let value = request.into_inner();
        let response = wire::MachineState {
            machine: value.machine.clone(),
            ..state(FIXTURE_MACHINE)
        };
        Ok(traced_response(
            &self.transcript,
            "acyclic.machines.v1.MachinesService/InspectMachine",
            &value,
            response,
        ))
    }
    async fn inspect_checkpoint(
        &self,
        request: Request<wire::InspectCheckpointRequest>,
    ) -> Result<Response<wire::CheckpointState>, Status> {
        let value = request.into_inner();
        let response = wire::CheckpointState {
            checkpoint: value.checkpoint.clone(),
            source: Some(machine(FIXTURE_MACHINE)),
            contract: Some(contract()),
            forkable: true,
            created_at_unix_ms: 1,
        };
        Ok(traced_response(
            &self.transcript,
            "acyclic.machines.v1.MachinesService/InspectCheckpoint",
            &value,
            response,
        ))
    }
    async fn list_machines(
        &self,
        request: Request<wire::ListMachinesRequest>,
    ) -> Result<Response<wire::MachinePage>, Status> {
        let value = request.into_inner();
        let response = wire::MachinePage {
            machines: vec![state(FIXTURE_MACHINE)],
            next: None,
        };
        Ok(traced_response(
            &self.transcript,
            "acyclic.machines.v1.MachinesService/ListMachines",
            &value,
            response,
        ))
    }
    async fn events(
        &self,
        request: Request<wire::EventsRequest>,
    ) -> Result<Response<wire::EventPage>, Status> {
        let value = request.into_inner();
        let response = wire::EventPage {
            events: vec![wire::MachineEvent {
                machine: value.machine.clone(),
                sequence: 1,
                observed_at_unix_ms: 1,
                kind: wire::EventKind::State as i32,
                state: wire::MachineStatus::Running as i32,
                pressure: wire::PressureKind::Unspecified as i32,
            }],
            next_sequence: 2,
        };
        Ok(traced_response(
            &self.transcript,
            "acyclic.machines.v1.MachinesService/Events",
            &value,
            response,
        ))
    }
    async fn usage(
        &self,
        request: Request<wire::UsageRequest>,
    ) -> Result<Response<wire::UsageReceipt>, Status> {
        let value = request.into_inner();
        let response = wire::UsageReceipt {
            machine: value.machine.clone(),
            start_unix_ms: value.start_unix_ms,
            end_unix_ms: value.end_unix_ms,
            elastic_cpu_ns: 1,
            dedicated_cpu_ns: 0,
            private_resident_byte_seconds: 1,
            durable_private_bytes: 1,
            lineage_receipt_sha256: vec![0; 32],
            egress_bytes: 1,
            receipt: vec![10],
        };
        Ok(traced_response(
            &self.transcript,
            "acyclic.machines.v1.MachinesService/Usage",
            &value,
            response,
        ))
    }
    async fn cancel(
        &self,
        request: Request<wire::OperationRequest>,
    ) -> Result<Response<wire::OperationState>, Status> {
        let value = request.into_inner();
        let response = wire::OperationState {
            operation: value.operation.clone(),
            status: wire::OperationStatus::Cancelled as i32,
        };
        Ok(traced_response(
            &self.transcript,
            "acyclic.machines.v1.MachinesService/Cancel",
            &value,
            response,
        ))
    }
    async fn inspect_operation(
        &self,
        request: Request<wire::OperationRequest>,
    ) -> Result<Response<wire::OperationState>, Status> {
        let value = request.into_inner();
        let response = wire::OperationState {
            operation: value.operation.clone(),
            status: wire::OperationStatus::Pending as i32,
        };
        Ok(traced_response(
            &self.transcript,
            "acyclic.machines.v1.MachinesService/InspectOperation",
            &value,
            response,
        ))
    }
    type WatchOperationStream =
        Pin<Box<dyn Stream<Item = Result<wire::OperationState, Status>> + Send + 'static>>;
    async fn watch_operation(
        &self,
        request: Request<wire::OperationRequest>,
    ) -> Result<Response<Self::WatchOperationStream>, Status> {
        let request = request.into_inner();
        let operation = request.operation.clone();
        let responses = [
            wire::OperationStatus::Pending,
            wire::OperationStatus::Succeeded,
        ]
        .into_iter()
        .map(|status| wire::OperationState {
            operation: operation.clone(),
            status: status as i32,
        })
        .collect::<Vec<_>>();
        let mut response_bytes = Vec::new();
        for response in &responses {
            response
                .encode(&mut response_bytes)
                .expect("encode operation state");
        }
        record_transcript(
            &self.transcript,
            "acyclic.machines.v1.MachinesService/WatchOperation",
            request.encode_to_vec(),
            response_bytes,
        );
        Ok(Response::new(Box::pin(stream::iter(
            responses.into_iter().map(Ok),
        ))))
    }
}

/// Starts a TLS-enabled all-routes Machines server on a loopback listener.
pub async fn serve_machines_rsa(
    listener: TcpListener,
    material: &RsaTlsMaterial,
    shutdown: oneshot::Receiver<()>,
) -> Result<(), tonic::transport::Error> {
    serve_machines_rsa_with_transcript(listener, material, shutdown, new_method_transcript_log())
        .await
}

/// Starts the fixture while appending each observed method to `transcript`.
pub async fn serve_machines_rsa_with_transcript(
    listener: TcpListener,
    material: &RsaTlsMaterial,
    shutdown: oneshot::Receiver<()>,
    transcript: MethodTranscriptLog,
) -> Result<(), tonic::transport::Error> {
    Server::builder()
        .tls_config(
            ServerTlsConfig::new()
                .identity(Identity::from_pem(
                    &material.server_certificate,
                    &material.server_private_key,
                ))
                .client_ca_root(Certificate::from_pem(&material.ca_certificate)),
        )?
        .add_service(wire::machines_service_server::MachinesServiceServer::new(
            AllRoutesMachinesFixture::with_transcript(transcript),
        ))
        .serve_with_incoming_shutdown(TcpListenerStream::new(listener), async {
            let _ = shutdown.await;
        })
        .await
}
