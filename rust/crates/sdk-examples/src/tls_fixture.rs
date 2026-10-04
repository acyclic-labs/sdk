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
    BasicConstraints, CertificateParams, DistinguishedName, DnType, ExtendedKeyUsagePurpose, IsCa,
    Issuer, KeyPair, KeyUsagePurpose, PKCS_RSA_SHA256, RsaKeySize,
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
