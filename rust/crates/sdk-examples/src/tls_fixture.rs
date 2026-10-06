//! Local RSA certificate and Machines RPC fixture support.
//!
//! The fixture is intended for installed language consumers. It uses a private
//! ephemeral RSA CA, signs separate server and client certificates, and enables
//! mutual TLS with normal certificate and hostname verification. It is local
//! qualification evidence only; it does not describe a hosted service.

use std::pin::Pin;
use std::sync::{Arc, Mutex};

use acyclic_machines::{Service as MachinesService, SimulatedMachines, wire};
use acyclic_sdk_contract_wire::{BindingFamily, transport_control};
use futures::{Stream, StreamExt, stream};
use prost::Message;
use rcgen::{
    BasicConstraints, CertificateParams, DistinguishedName, DnType, ExtendedKeyUsagePurpose, IsCa,
    Issuer, KeyPair, KeyUsagePurpose, PKCS_RSA_SHA256, RsaKeySize,
};
use sha2::{Digest, Sha256};
use tokio::net::TcpListener;
use tokio::sync::oneshot;
use tokio_stream::wrappers::TcpListenerStream;
use tonic::transport::{Certificate, Identity, Server, ServerTlsConfig};
use tonic::{Request, Response, Status};

#[allow(missing_docs, clippy::pedantic, clippy::large_enum_variant)]
mod generated_control {
    pub mod acyclic {
        pub mod protocol {
            pub mod v1 {
                include!(concat!(env!("OUT_DIR"), "/control/acyclic.protocol.v1.rs"));
            }
        }
        pub mod transport {
            pub mod v1 {
                include!(concat!(env!("OUT_DIR"), "/control/acyclic.transport.v1.rs"));
            }
        }
    }
}

use generated_control::acyclic::{
    protocol::v1 as control_protocol,
    transport::v1::protocol_service_server::{ProtocolService, ProtocolServiceServer},
};

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

fn fixture_distinguished_name(common_name: &str) -> DistinguishedName {
    let mut distinguished_name = DistinguishedName::new();
    distinguished_name.push(DnType::CommonName, common_name);
    distinguished_name
}

impl RsaTlsMaterial {
    /// Generates a fresh RSA-2048 CA, server certificate, and client certificate.
    pub fn generate() -> Result<Self, rcgen::Error> {
        let mut ca_params = CertificateParams::new(Vec::<String>::new())?;
        ca_params.distinguished_name = fixture_distinguished_name("Acyclic Fixture RSA CA");
        ca_params.is_ca = IsCa::Ca(BasicConstraints::Unconstrained);
        ca_params.key_usages = vec![
            KeyUsagePurpose::DigitalSignature,
            KeyUsagePurpose::KeyCertSign,
        ];
        let ca_key = KeyPair::generate_rsa_for(&PKCS_RSA_SHA256, RsaKeySize::_2048)?;
        let ca_certificate = ca_params.self_signed(&ca_key)?;
        let issuer = Issuer::new(ca_params, ca_key);

        let mut server_params = CertificateParams::new(vec!["localhost".to_owned()])?;
        server_params.distinguished_name = fixture_distinguished_name("Acyclic Fixture RSA Server");
        server_params.use_authority_key_identifier_extension = true;
        server_params.key_usages = vec![
            KeyUsagePurpose::DigitalSignature,
            KeyUsagePurpose::KeyEncipherment,
        ];
        server_params.extended_key_usages = vec![ExtendedKeyUsagePurpose::ServerAuth];
        let server_key = KeyPair::generate_rsa_for(&PKCS_RSA_SHA256, RsaKeySize::_2048)?;
        let server_certificate = server_params.signed_by(&server_key, &issuer)?;

        let mut client_params = CertificateParams::new(Vec::<String>::new())?;
        client_params.distinguished_name = fixture_distinguished_name("Acyclic Fixture RSA Client");
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

/// Every modeled Inference Runs RPC that the bounded fixture implements.
pub const INFERENCE_RUNS_RPC_METHODS: &[&str] = &[
    "inference.customer.v1.ModelsService/List",
    "inference.customer.v1.ContextsService/Create",
    "inference.customer.v1.ContextsService/Inspect",
    "inference.customer.v1.ContextsService/Mutate",
    "inference.customer.v1.WarmContextsService/Retain",
    "inference.customer.v1.WarmContextsService/Inspect",
    "inference.customer.v1.WarmContextsService/Renew",
    "inference.customer.v1.WarmContextsService/Release",
    "inference.customer.v1.RunsService/Generate",
    "inference.customer.v1.RunsService/Inspect",
    "inference.customer.v1.RunsService/Watch",
    "inference.customer.v1.RunsService/Cancel",
    "inference.customer.v1.EvaluationsService/Create",
    "inference.customer.v1.EvaluationsService/Inspect",
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
    /// Canonically encoded request bytes for Rust-owned typed collectors.
    pub request_base64: String,
    /// Number of encoded bytes in the first response frame.
    pub response_bytes: usize,
    /// SHA-256 of the first response frame.
    pub response_sha256: String,
    /// Canonically encoded first response frame for Rust-owned typed collectors.
    pub response_base64: String,
    /// Canonically encoded response frames in wire order.
    pub response_frames: Vec<TranscriptFrame>,
}

/// One canonical encoded response frame from a streaming RPC.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct TranscriptFrame {
    /// Number of encoded protobuf bytes in this frame.
    pub response_bytes: usize,
    /// SHA-256 of the encoded protobuf frame.
    pub response_sha256: String,
    /// Canonically encoded protobuf frame.
    pub response_base64: String,
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

pub(crate) fn record_transcript(
    log: &MethodTranscriptLog,
    rpc: &'static str,
    request_bytes: Vec<u8>,
    response_bytes: Vec<u8>,
) {
    record_stream_transcript(log, rpc, request_bytes, vec![response_bytes]);
}

pub(crate) fn record_stream_transcript(
    log: &MethodTranscriptLog,
    rpc: &'static str,
    request_bytes: Vec<u8>,
    response_frames: Vec<Vec<u8>>,
) {
    let first_response = response_frames.first().cloned().unwrap_or_default();
    log.lock()
        .expect("Machines fixture transcript mutex poisoned")
        .push(MethodTranscript {
            rpc,
            request_bytes: request_bytes.len(),
            request_sha256: digest(&request_bytes),
            request_base64: base64(&request_bytes),
            response_bytes: first_response.len(),
            response_sha256: digest(&first_response),
            response_base64: base64(&first_response),
            response_frames: response_frames
                .into_iter()
                .map(|response| TranscriptFrame {
                    response_bytes: response.len(),
                    response_sha256: digest(&response),
                    response_base64: base64(&response),
                })
                .collect(),
        });
}

fn base64(bytes: &[u8]) -> String {
    const ALPHABET: &[u8; 64] =
        b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";
    let mut output = String::with_capacity(bytes.len().div_ceil(3) * 4);
    for chunk in bytes.chunks(3) {
        let first = chunk[0];
        let second = chunk.get(1).copied().unwrap_or(0);
        let third = chunk.get(2).copied().unwrap_or(0);
        output.push(ALPHABET[(first >> 2) as usize] as char);
        output.push(ALPHABET[((first & 0x03) << 4 | second >> 4) as usize] as char);
        output.push(if chunk.len() > 1 {
            ALPHABET[((second & 0x0f) << 2 | third >> 6) as usize] as char
        } else {
            '='
        });
        output.push(if chunk.len() > 2 {
            ALPHABET[(third & 0x3f) as usize] as char
        } else {
            '='
        });
    }
    output
}

pub(crate) fn traced_response<Req, Resp>(
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

/// The all-routes fixture binds the generated wire service to the canonical
/// Rust state machine. The transcript is only an observation layer; it never
/// supplies response data or identities.
#[derive(Clone)]
pub struct AllRoutesMachinesFixture {
    transcript: MethodTranscriptLog,
    service: MachinesService<SimulatedMachines>,
}

impl Default for AllRoutesMachinesFixture {
    fn default() -> Self {
        Self {
            transcript: new_method_transcript_log(),
            service: MachinesService::new(Arc::new(SimulatedMachines::default())),
        }
    }
}

impl AllRoutesMachinesFixture {
    /// Builds the fixture with a caller-owned transcript sink.
    pub fn with_transcript(transcript: MethodTranscriptLog) -> Self {
        Self {
            transcript,
            service: MachinesService::new(Arc::new(SimulatedMachines::default())),
        }
    }
}

#[tonic::async_trait]
impl wire::machines_service_server::MachinesService for AllRoutesMachinesFixture {
    async fn qualify_image(
        &self,
        request: Request<wire::QualifyImageRequest>,
    ) -> Result<Response<wire::ImageQualification>, Status> {
        let value = request.into_inner();
        let response = self.service.qualify_image(Request::new(value.clone())).await?;
        Ok(traced_response(&self.transcript, MACHINES_RPC_METHODS[0], &value, response.into_inner()))
    }
    async fn create(
        &self,
        request: Request<wire::CreateMachineRequest>,
    ) -> Result<Response<wire::MachineAdmission>, Status> {
        let value = request.into_inner();
        let response = self.service.create(Request::new(value.clone())).await?;
        Ok(traced_response(&self.transcript, MACHINES_RPC_METHODS[1], &value, response.into_inner()))
    }
    async fn checkpoint(
        &self,
        request: Request<wire::CheckpointMachineRequest>,
    ) -> Result<Response<wire::CheckpointAdmission>, Status> {
        let value = request.into_inner();
        let response = self.service.checkpoint(Request::new(value.clone())).await?;
        Ok(traced_response(&self.transcript, MACHINES_RPC_METHODS[2], &value, response.into_inner()))
    }
    async fn fork(
        &self,
        request: Request<wire::ForkCheckpointRequest>,
    ) -> Result<Response<wire::ForkAdmission>, Status> {
        let value = request.into_inner();
        let response = self.service.fork(Request::new(value.clone())).await?;
        Ok(traced_response(&self.transcript, MACHINES_RPC_METHODS[3], &value, response.into_inner()))
    }
    async fn fork_machine(
        &self,
        request: Request<wire::ForkMachineRequest>,
    ) -> Result<Response<wire::ForkMachineAdmission>, Status> {
        let value = request.into_inner();
        let response = self.service.fork_machine(Request::new(value.clone())).await?;
        Ok(traced_response(&self.transcript, MACHINES_RPC_METHODS[4], &value, response.into_inner()))
    }
    async fn suspend(
        &self,
        request: Request<wire::MachineMutationRequest>,
    ) -> Result<Response<wire::MutationAdmission>, Status> {
        let value = request.into_inner();
        let response = self.service.suspend(Request::new(value.clone())).await?;
        Ok(traced_response(&self.transcript, MACHINES_RPC_METHODS[5], &value, response.into_inner()))
    }
    async fn wake(
        &self,
        request: Request<wire::MachineMutationRequest>,
    ) -> Result<Response<wire::MutationAdmission>, Status> {
        let value = request.into_inner();
        let response = self.service.wake(Request::new(value.clone())).await?;
        Ok(traced_response(&self.transcript, MACHINES_RPC_METHODS[6], &value, response.into_inner()))
    }
    async fn set_suspension_policy(
        &self,
        request: Request<wire::SetSuspensionPolicyRequest>,
    ) -> Result<Response<wire::PolicyAdmission>, Status> {
        let value = request.into_inner();
        let response = self.service.set_suspension_policy(Request::new(value.clone())).await?;
        Ok(traced_response(&self.transcript, MACHINES_RPC_METHODS[7], &value, response.into_inner()))
    }
    async fn destroy_machine(
        &self,
        request: Request<wire::MachineMutationRequest>,
    ) -> Result<Response<wire::MutationAdmission>, Status> {
        let value = request.into_inner();
        let response = self.service.destroy_machine(Request::new(value.clone())).await?;
        Ok(traced_response(&self.transcript, MACHINES_RPC_METHODS[8], &value, response.into_inner()))
    }
    async fn destroy_checkpoint(
        &self,
        request: Request<wire::CheckpointMutationRequest>,
    ) -> Result<Response<wire::MutationAdmission>, Status> {
        let value = request.into_inner();
        let response = self.service.destroy_checkpoint(Request::new(value.clone())).await?;
        Ok(traced_response(&self.transcript, MACHINES_RPC_METHODS[9], &value, response.into_inner()))
    }
    async fn recover(
        &self,
        request: Request<wire::RecoverRequest>,
    ) -> Result<Response<wire::RecoveredAdmission>, Status> {
        let value = request.into_inner();
        let response = self.service.recover(Request::new(value.clone())).await?;
        Ok(traced_response(&self.transcript, MACHINES_RPC_METHODS[10], &value, response.into_inner()))
    }
    async fn inspect_machine(
        &self,
        request: Request<wire::InspectMachineRequest>,
    ) -> Result<Response<wire::MachineState>, Status> {
        let value = request.into_inner();
        let response = self.service.inspect_machine(Request::new(value.clone())).await?;
        Ok(traced_response(&self.transcript, MACHINES_RPC_METHODS[11], &value, response.into_inner()))
    }
    async fn inspect_checkpoint(
        &self,
        request: Request<wire::InspectCheckpointRequest>,
    ) -> Result<Response<wire::CheckpointState>, Status> {
        let value = request.into_inner();
        let response = self.service.inspect_checkpoint(Request::new(value.clone())).await?;
        Ok(traced_response(&self.transcript, MACHINES_RPC_METHODS[12], &value, response.into_inner()))
    }
    async fn list_machines(
        &self,
        request: Request<wire::ListMachinesRequest>,
    ) -> Result<Response<wire::MachinePage>, Status> {
        let value = request.into_inner();
        let response = self.service.list_machines(Request::new(value.clone())).await?;
        Ok(traced_response(&self.transcript, MACHINES_RPC_METHODS[13], &value, response.into_inner()))
    }
    async fn events(
        &self,
        request: Request<wire::EventsRequest>,
    ) -> Result<Response<wire::EventPage>, Status> {
        let value = request.into_inner();
        let response = self.service.events(Request::new(value.clone())).await?;
        Ok(traced_response(&self.transcript, MACHINES_RPC_METHODS[14], &value, response.into_inner()))
    }
    async fn usage(
        &self,
        request: Request<wire::UsageRequest>,
    ) -> Result<Response<wire::UsageReceipt>, Status> {
        let value = request.into_inner();
        let response = self.service.usage(Request::new(value.clone())).await?;
        let mut receipt = response.into_inner();
        // The in-memory provider intentionally emits an empty local-simulation
        // receipt. This TLS fixture exercises the remote boundary, so carry a
        // deterministic authenticated receipt payload through the wire route.
        if receipt.receipt.is_empty() {
            receipt.receipt = b"acyclic-machines-rsa-fixture-usage-v1".to_vec();
            receipt.lineage_receipt_sha256 = Sha256::digest(&receipt.receipt).to_vec();
        }
        Ok(traced_response(&self.transcript, MACHINES_RPC_METHODS[15], &value, receipt))
    }
    async fn cancel(
        &self,
        request: Request<wire::OperationRequest>,
    ) -> Result<Response<wire::OperationState>, Status> {
        let value = request.into_inner();
        let response = self.service.cancel(Request::new(value.clone())).await?;
        Ok(traced_response(&self.transcript, MACHINES_RPC_METHODS[16], &value, response.into_inner()))
    }
    async fn inspect_operation(
        &self,
        request: Request<wire::OperationRequest>,
    ) -> Result<Response<wire::OperationState>, Status> {
        let value = request.into_inner();
        let response = self.service.inspect_operation(Request::new(value.clone())).await?;
        Ok(traced_response(&self.transcript, MACHINES_RPC_METHODS[17], &value, response.into_inner()))
    }
    type WatchOperationStream =
        Pin<Box<dyn Stream<Item = Result<wire::OperationState, Status>> + Send + 'static>>;
    async fn watch_operation(
        &self,
        request: Request<wire::OperationRequest>,
    ) -> Result<Response<Self::WatchOperationStream>, Status> {
        let value = request.into_inner();
        let response = self
            .service
            .watch_operation(Request::new(value.clone()))
            .await?;
        let frames = response.into_inner().collect::<Vec<_>>().await;
        let encoded = frames
            .iter()
            .filter_map(|frame| frame.as_ref().ok())
            .map(prost::Message::encode_to_vec)
            .collect::<Vec<_>>();
        record_stream_transcript(
            &self.transcript,
            "acyclic.machines.v1.MachinesService/WatchOperation",
            value.encode_to_vec(),
            encoded,
        );
        Ok(Response::new(Box::pin(stream::iter(
            frames,
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

/// Rust-owned control-plane handshake for the installed Machines fixture.
///
/// mTLS authenticates the installed client, so this control route intentionally
/// does not require a bearer credential. The protocol family, version, archived
/// descriptor digest, and required capability are still checked before the
/// generated Machines service is admitted.
#[derive(Clone, Copy, Debug, Default)]
struct MachinesProtocolFixture;

#[tonic::async_trait]
impl ProtocolService for MachinesProtocolFixture {
    async fn handshake(
        &self,
        request: Request<control_protocol::HandshakeRequest>,
    ) -> Result<Response<control_protocol::HandshakeResponse>, Status> {
        let family = BindingFamily::Machines;
        let expected_version = transport_control::control_protocol_version(family);
        let expected_digest = transport_control::archived_descriptor_digest(family);
        let metadata_family = request
            .metadata()
            .get(transport_control::FAMILY_METADATA_KEY)
            .and_then(|value| value.to_str().ok());
        if metadata_family != Some(family.name()) {
            return Err(Status::invalid_argument("wrong SDK family"));
        }
        let protocol = request
            .get_ref()
            .protocol
            .as_ref()
            .ok_or_else(|| Status::invalid_argument("protocol identity is required"))?;
        if protocol.version != expected_version || protocol.descriptor_digest != expected_digest {
            return Err(Status::failed_precondition("wrong Machines protocol identity"));
        }
        if request
            .get_ref()
            .required
            .as_ref()
            .is_some_and(|required| {
                required.capabilities.iter().any(|capability| {
                    capability.name != family.name() || capability.version != expected_version
                })
            })
        {
            return Err(Status::failed_precondition("required capability is unsupported"));
        }
        Ok(Response::new(control_protocol::HandshakeResponse {
            protocol: Some(control_protocol::ProtocolIdentity {
                version: expected_version.to_owned(),
                descriptor_digest: expected_digest,
            }),
            supported: Some(control_protocol::CapabilitySet {
                capabilities: vec![control_protocol::Capability {
                    name: family.name().to_owned(),
                    version: expected_version.to_owned(),
                }],
            }),
        }))
    }
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
        .add_service(ProtocolServiceServer::new(MachinesProtocolFixture))
        .add_service(wire::machines_service_server::MachinesServiceServer::new(
            AllRoutesMachinesFixture::with_transcript(transcript),
        ))
        .serve_with_incoming_shutdown(TcpListenerStream::new(listener), async {
            let _ = shutdown.await;
        })
        .await
}

/// Rust-owned implementations for the non-run Inference services.  Keeping
/// these in the same mTLS fixture is intentional: an installed SDK probe must
/// exercise the complete generated service surface against one authority.
#[derive(Clone)]
pub struct InferenceMetadataFixture {
    transcript: MethodTranscriptLog,
}

impl InferenceMetadataFixture {
    pub fn with_transcript(transcript: MethodTranscriptLog) -> Self {
        Self { transcript }
    }
}

#[tonic::async_trait]
impl acyclic_inference::wire::models_service_server::ModelsService
    for InferenceMetadataFixture
{
    async fn list(
        &self,
        request: Request<acyclic_inference::wire::ListModelsRequest>,
    ) -> Result<Response<acyclic_inference::wire::ListModelsResponse>, Status> {
        let request = request.into_inner();
        let response = acyclic_inference::wire::ListModelsResponse {
            models: vec![acyclic_inference::wire::ModelCapability {
                model: "fixture-model".to_owned(),
                execution_profile: vec![1; 32],
                maximum_context: 4096,
                maximum_output: 1024,
                features: vec!["generate".to_owned(), "stream".to_owned()],
                ..Default::default()
            }],
        };
        Ok(traced_response(
            &self.transcript,
            "inference.customer.v1.ModelsService/List",
            &request,
            response,
        ))
    }
}

#[tonic::async_trait]
impl acyclic_inference::wire::contexts_service_server::ContextsService
    for InferenceMetadataFixture
{
    async fn create(
        &self,
        request: Request<acyclic_inference::wire::CreateContextRequest>,
    ) -> Result<Response<acyclic_inference::wire::MutationReceipt>, Status> {
        let request = request.into_inner();
        let response = acyclic_inference::wire::MutationReceipt {
            revision: vec![1; 32],
            command_digest: vec![2; 32],
            sequence: 1,
            retained: true,
        };
        Ok(traced_response(
            &self.transcript,
            "inference.customer.v1.ContextsService/Create",
            &request,
            response,
        ))
    }

    async fn inspect(
        &self,
        request: Request<acyclic_inference::wire::InspectContextRequest>,
    ) -> Result<Response<acyclic_inference::wire::ContextView>, Status> {
        let request = request.into_inner();
        let response = acyclic_inference::wire::ContextView {
            revision: if request.revision.is_empty() { vec![1; 32] } else { request.revision.clone() },
            lineage: vec![3; 32],
            execution_profile: vec![4; 32],
            content_digest: vec![5; 32],
            model: "fixture-model".to_owned(),
            provenance: Some(acyclic_inference::wire::ContextProvenance {
                origin: Some(acyclic_inference::wire::context_provenance::Origin::Created(
                    acyclic_inference::wire::Empty {},
                )),
            }),
            ..Default::default()
        };
        Ok(traced_response(
            &self.transcript,
            "inference.customer.v1.ContextsService/Inspect",
            &request,
            response,
        ))
    }

    async fn mutate(
        &self,
        request: Request<acyclic_inference::wire::MutateContextRequest>,
    ) -> Result<Response<acyclic_inference::wire::MutationReceipt>, Status> {
        let request = request.into_inner();
        let response = acyclic_inference::wire::MutationReceipt {
            revision: vec![6; 32],
            command_digest: vec![7; 32],
            sequence: 2,
            retained: false,
        };
        Ok(traced_response(
            &self.transcript,
            "inference.customer.v1.ContextsService/Mutate",
            &request,
            response,
        ))
    }
}

fn warm_view() -> acyclic_inference::wire::WarmView {
    acyclic_inference::wire::WarmView {
        commitment: vec![8; 32],
        context: vec![1; 32],
        model_profile: vec![9; 32],
        latency_profile: vec![10; 32],
        expires_at_ms: 4_000,
        state: acyclic_inference::wire::WarmState::Active as i32,
        evidence_digest: vec![11; 32],
        admission_receipt_id: vec![12; 32],
        sequence: 1,
        idle_kv: None,
    }
}

#[tonic::async_trait]
impl acyclic_inference::wire::warm_contexts_service_server::WarmContextsService
    for InferenceMetadataFixture
{
    async fn retain(
        &self,
        request: Request<acyclic_inference::wire::RetainWarmRequest>,
    ) -> Result<Response<acyclic_inference::wire::WarmView>, Status> {
        let request = request.into_inner();
        Ok(traced_response(
            &self.transcript,
            "inference.customer.v1.WarmContextsService/Retain",
            &request,
            warm_view(),
        ))
    }

    async fn inspect(
        &self,
        request: Request<acyclic_inference::wire::InspectWarmRequest>,
    ) -> Result<Response<acyclic_inference::wire::WarmView>, Status> {
        let request = request.into_inner();
        Ok(traced_response(
            &self.transcript,
            "inference.customer.v1.WarmContextsService/Inspect",
            &request,
            warm_view(),
        ))
    }

    async fn renew(
        &self,
        request: Request<acyclic_inference::wire::RenewWarmRequest>,
    ) -> Result<Response<acyclic_inference::wire::WarmView>, Status> {
        let request = request.into_inner();
        Ok(traced_response(
            &self.transcript,
            "inference.customer.v1.WarmContextsService/Renew",
            &request,
            warm_view(),
        ))
    }

    async fn release(
        &self,
        request: Request<acyclic_inference::wire::ReleaseWarmRequest>,
    ) -> Result<Response<acyclic_inference::wire::WarmView>, Status> {
        let request = request.into_inner();
        let mut response = warm_view();
        response.state = acyclic_inference::wire::WarmState::Released as i32;
        Ok(traced_response(
            &self.transcript,
            "inference.customer.v1.WarmContextsService/Release",
            &request,
            response,
        ))
    }
}

fn evaluation_view(id: Vec<u8>) -> acyclic_inference::wire::EvaluationView {
    acyclic_inference::wire::EvaluationView {
        evaluation_id: if id.is_empty() { vec![13; 16] } else { id },
        state: acyclic_inference::wire::EvaluationState::Completed as i32,
        sequence: 1,
        ..Default::default()
    }
}

#[tonic::async_trait]
impl acyclic_inference::wire::evaluations_service_server::EvaluationsService
    for InferenceMetadataFixture
{
    async fn create(
        &self,
        request: Request<acyclic_inference::wire::CreateEvaluationRequest>,
    ) -> Result<Response<acyclic_inference::wire::EvaluationView>, Status> {
        let request = request.into_inner();
        Ok(traced_response(
            &self.transcript,
            "inference.customer.v1.EvaluationsService/Create",
            &request,
            evaluation_view(vec![13; 16]),
        ))
    }

    async fn inspect(
        &self,
        request: Request<acyclic_inference::wire::InspectEvaluationRequest>,
    ) -> Result<Response<acyclic_inference::wire::EvaluationView>, Status> {
        let request = request.into_inner();
        Ok(traced_response(
            &self.transcript,
            "inference.customer.v1.EvaluationsService/Inspect",
            &request,
            evaluation_view(request.evaluation_id.clone()),
        ))
    }
}

/// A bounded Rust-owned Inference Runs service for installed SDK consumers.
///
/// The service deliberately exposes the complete Runs RPC surface and keeps
/// the event sequence deterministic. It is intended for local mTLS
/// qualification; it does not model hosted model availability.
#[derive(Clone)]
pub struct InferenceRunsFixture {
    transcript: MethodTranscriptLog,
}

impl InferenceRunsFixture {
    /// Builds the fixture with a caller-owned transcript sink.
    pub fn with_transcript(transcript: MethodTranscriptLog) -> Self {
        Self { transcript }
    }
}

fn inference_run_view(run_id: Vec<u8>, cancelled: bool) -> acyclic_inference::wire::RunView {
    acyclic_inference::wire::RunView {
        run_id,
        input: vec![3; 32],
        model: "model.example.v1".to_owned(),
        last_sequence: u64::from(cancelled),
        cancellation_requested: cancelled,
        result: cancelled.then(|| acyclic_inference::wire::RunResult {
            output: Vec::new(),
            context: None,
            terminal: acyclic_inference::wire::RunTerminal::Cancelled as i32,
            receipt: None,
        }),
    }
}

#[tonic::async_trait]
impl acyclic_inference::wire::runs_service_server::RunsService for InferenceRunsFixture {
    async fn generate(
        &self,
        request: Request<acyclic_inference::wire::GenerateRunRequest>,
    ) -> Result<Response<acyclic_inference::wire::GenerateRunResponse>, Status> {
        let request = request.into_inner();
        let response = acyclic_inference::wire::GenerateRunResponse {
            run: Some(inference_run_view(vec![2; 16], false)),
        };
        Ok(traced_response(
            &self.transcript,
            "inference.customer.v1.RunsService/Generate",
            &request,
            response,
        ))
    }

    async fn inspect(
        &self,
        request: Request<acyclic_inference::wire::InspectRunRequest>,
    ) -> Result<Response<acyclic_inference::wire::RunView>, Status> {
        let request = request.into_inner();
        let response = inference_run_view(request.run_id.clone(), false);
        Ok(traced_response(
            &self.transcript,
            "inference.customer.v1.RunsService/Inspect",
            &request,
            response,
        ))
    }

    type WatchStream = Pin<
        Box<dyn Stream<Item = Result<acyclic_inference::wire::RunEvent, Status>> + Send + 'static>,
    >;

    async fn watch(
        &self,
        request: Request<acyclic_inference::wire::WatchRunRequest>,
    ) -> Result<Response<Self::WatchStream>, Status> {
        let request = request.into_inner();
        let events = vec![
            acyclic_inference::wire::RunEvent {
                sequence: 0,
                event: Some(acyclic_inference::wire::run_event::Event::Progress(
                    acyclic_inference::wire::RunProgress {
                        kind: "queued".to_owned(),
                    },
                )),
            },
            acyclic_inference::wire::RunEvent {
                sequence: 1,
                event: Some(acyclic_inference::wire::run_event::Event::Terminal(
                    acyclic_inference::wire::RunTerminal::Completed as i32,
                )),
            },
        ];
        let response_frames = events
            .iter()
            .map(|event| event.encode_to_vec())
            .collect::<Vec<_>>();
        record_stream_transcript(
            &self.transcript,
            "inference.customer.v1.RunsService/Watch",
            request.encode_to_vec(),
            response_frames,
        );
        Ok(Response::new(Box::pin(stream::iter(
            events.into_iter().map(Ok),
        ))))
    }

    async fn cancel(
        &self,
        request: Request<acyclic_inference::wire::InspectRunRequest>,
    ) -> Result<Response<acyclic_inference::wire::RunView>, Status> {
        let request = request.into_inner();
        let response = inference_run_view(request.run_id.clone(), true);
        Ok(traced_response(
            &self.transcript,
            "inference.customer.v1.RunsService/Cancel",
            &request,
            response,
        ))
    }
}

/// Starts a TLS-enabled bounded Inference Runs server on a loopback listener.
pub async fn serve_inference_runs_rsa_with_transcript(
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
        .add_service(
            acyclic_inference::wire::models_service_server::ModelsServiceServer::new(
                InferenceMetadataFixture::with_transcript(transcript.clone()),
            ),
        )
        .add_service(
            acyclic_inference::wire::contexts_service_server::ContextsServiceServer::new(
                InferenceMetadataFixture::with_transcript(transcript.clone()),
            ),
        )
        .add_service(
            acyclic_inference::wire::warm_contexts_service_server::WarmContextsServiceServer::new(
                InferenceMetadataFixture::with_transcript(transcript.clone()),
            ),
        )
        .add_service(
            acyclic_inference::wire::runs_service_server::RunsServiceServer::new(
                InferenceRunsFixture::with_transcript(transcript.clone()),
            ),
        )
        .add_service(
            acyclic_inference::wire::evaluations_service_server::EvaluationsServiceServer::new(
                InferenceMetadataFixture::with_transcript(transcript),
            ),
        )
        .serve_with_incoming_shutdown(TcpListenerStream::new(listener), async {
            let _ = shutdown.await;
        })
        .await
}
