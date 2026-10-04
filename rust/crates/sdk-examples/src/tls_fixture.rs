//! Local RSA certificate and Machines RPC fixture support.
//!
//! The fixture is intended for installed language consumers. It uses a private
//! ephemeral RSA CA, signs separate server and client certificates, and enables
//! mutual TLS with normal certificate and hostname verification. It is local
//! qualification evidence only; it does not describe a hosted service.

use std::{pin::Pin, sync::Arc};

use acyclic_machines::wire;
use futures::{Stream, stream};
use rcgen::{
    BasicConstraints, CertificateParams, ExtendedKeyUsagePurpose, IsCa, Issuer, KeyPair,
    KeyUsagePurpose, PKCS_RSA_SHA256, RsaKeySize,
};
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
            digest: vec![7; 32],
            reference: String::new(),
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
#[derive(Clone, Default)]
pub struct AllRoutesMachinesFixture;

#[tonic::async_trait]
impl wire::machines_service_server::MachinesService for AllRoutesMachinesFixture {
    async fn qualify_image(
        &self,
        request: Request<wire::QualifyImageRequest>,
    ) -> Result<Response<wire::ImageQualification>, Status> {
        Ok(Response::new(wire::ImageQualification {
            image: request.into_inner().image,
            capabilities: vec![wire::Capability::LiveCheckpoint as i32],
            compatibility_revision: vec![8; 32],
        }))
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
        Ok(Response::new(wire::MachineAdmission {
            machine: Some(machine(FIXTURE_MACHINE)),
            operation: Some(operation(FIXTURE_OPERATION)),
            contract: Some(wire::MachineContract {
                image: value.image,
                capabilities,
                compatibility: value.compatibility,
                compatibility_revision: vec![8; 32],
                suspension: value.suspension,
                expiration: value.expiration,
                network_policy_digest: value.network_policy_digest,
                budgets: value.budgets,
            }),
        }))
    }
    async fn checkpoint(
        &self,
        request: Request<wire::CheckpointMachineRequest>,
    ) -> Result<Response<wire::CheckpointAdmission>, Status> {
        Ok(Response::new(wire::CheckpointAdmission {
            checkpoint: Some(checkpoint(FIXTURE_CHECKPOINT)),
            source: request.into_inner().machine,
            operation: Some(operation(FIXTURE_OPERATION)),
            contract: Some(contract()),
        }))
    }
    async fn fork(
        &self,
        request: Request<wire::ForkCheckpointRequest>,
    ) -> Result<Response<wire::ForkAdmission>, Status> {
        Ok(Response::new(wire::ForkAdmission {
            checkpoint: request.into_inner().checkpoint,
            children: vec![machine([4; 16]), machine([5; 16])],
            operation: Some(operation(FIXTURE_OPERATION)),
            contract: Some(contract()),
        }))
    }
    async fn fork_machine(
        &self,
        request: Request<wire::ForkMachineRequest>,
    ) -> Result<Response<wire::ForkMachineAdmission>, Status> {
        Ok(Response::new(wire::ForkMachineAdmission {
            source: request.into_inner().machine,
            children: vec![machine([4; 16]), machine([5; 16])],
            operation: Some(operation(FIXTURE_OPERATION)),
            contract: Some(contract()),
            fidelity: wire::ForkFidelity::BestEffort as i32,
        }))
    }
    async fn suspend(
        &self,
        request: Request<wire::MachineMutationRequest>,
    ) -> Result<Response<wire::MutationAdmission>, Status> {
        Ok(Response::new(wire::MutationAdmission {
            operation: Some(operation(FIXTURE_OPERATION)),
            machine: request.into_inner().machine,
            checkpoint: None,
        }))
    }
    async fn wake(
        &self,
        request: Request<wire::MachineMutationRequest>,
    ) -> Result<Response<wire::MutationAdmission>, Status> {
        Ok(Response::new(wire::MutationAdmission {
            operation: Some(operation(FIXTURE_OPERATION)),
            machine: request.into_inner().machine,
            checkpoint: None,
        }))
    }
    async fn set_suspension_policy(
        &self,
        request: Request<wire::SetSuspensionPolicyRequest>,
    ) -> Result<Response<wire::PolicyAdmission>, Status> {
        let value = request.into_inner();
        Ok(Response::new(wire::PolicyAdmission {
            machine: value.machine,
            operation: Some(operation(FIXTURE_OPERATION)),
            policy: value.policy,
        }))
    }
    async fn destroy_machine(
        &self,
        request: Request<wire::MachineMutationRequest>,
    ) -> Result<Response<wire::MutationAdmission>, Status> {
        Ok(Response::new(wire::MutationAdmission {
            operation: Some(operation(FIXTURE_OPERATION)),
            machine: request.into_inner().machine,
            checkpoint: None,
        }))
    }
    async fn destroy_checkpoint(
        &self,
        request: Request<wire::CheckpointMutationRequest>,
    ) -> Result<Response<wire::MutationAdmission>, Status> {
        Ok(Response::new(wire::MutationAdmission {
            operation: Some(operation(FIXTURE_OPERATION)),
            machine: None,
            checkpoint: request.into_inner().checkpoint,
        }))
    }
    async fn recover(
        &self,
        _request: Request<wire::RecoverRequest>,
    ) -> Result<Response<wire::RecoveredAdmission>, Status> {
        Ok(Response::new(wire::RecoveredAdmission {
            operation: Some(operation(FIXTURE_OPERATION)),
            result: Some(wire::recovered_admission::Result::Create(admission(
                FIXTURE_MACHINE,
            ))),
        }))
    }
    async fn inspect_machine(
        &self,
        request: Request<wire::InspectMachineRequest>,
    ) -> Result<Response<wire::MachineState>, Status> {
        Ok(Response::new(wire::MachineState {
            machine: request.into_inner().machine,
            ..state(FIXTURE_MACHINE)
        }))
    }
    async fn inspect_checkpoint(
        &self,
        request: Request<wire::InspectCheckpointRequest>,
    ) -> Result<Response<wire::CheckpointState>, Status> {
        Ok(Response::new(wire::CheckpointState {
            checkpoint: request.into_inner().checkpoint,
            source: Some(machine(FIXTURE_MACHINE)),
            contract: Some(contract()),
            forkable: true,
            created_at_unix_ms: 1,
        }))
    }
    async fn list_machines(
        &self,
        _request: Request<wire::ListMachinesRequest>,
    ) -> Result<Response<wire::MachinePage>, Status> {
        Ok(Response::new(wire::MachinePage {
            machines: vec![state(FIXTURE_MACHINE)],
            next: None,
        }))
    }
    async fn events(
        &self,
        request: Request<wire::EventsRequest>,
    ) -> Result<Response<wire::EventPage>, Status> {
        Ok(Response::new(wire::EventPage {
            events: vec![wire::MachineEvent {
                machine: request.into_inner().machine,
                sequence: 1,
                observed_at_unix_ms: 1,
                kind: wire::EventKind::State as i32,
                state: wire::MachineStatus::Running as i32,
                pressure: wire::PressureKind::Unspecified as i32,
            }],
            next_sequence: 2,
        }))
    }
    async fn usage(
        &self,
        request: Request<wire::UsageRequest>,
    ) -> Result<Response<wire::UsageReceipt>, Status> {
        let value = request.into_inner();
        Ok(Response::new(wire::UsageReceipt {
            machine: value.machine,
            start_unix_ms: value.start_unix_ms,
            end_unix_ms: value.end_unix_ms,
            elastic_cpu_ns: 1,
            dedicated_cpu_ns: 0,
            private_resident_byte_seconds: 1,
            durable_private_bytes: 1,
            lineage_receipt_sha256: vec![0; 32],
            egress_bytes: 1,
            receipt: vec![10],
        }))
    }
    async fn cancel(
        &self,
        request: Request<wire::OperationRequest>,
    ) -> Result<Response<wire::OperationState>, Status> {
        Ok(Response::new(wire::OperationState {
            operation: request.into_inner().operation,
            status: wire::OperationStatus::Cancelled as i32,
        }))
    }
    async fn inspect_operation(
        &self,
        request: Request<wire::OperationRequest>,
    ) -> Result<Response<wire::OperationState>, Status> {
        Ok(Response::new(wire::OperationState {
            operation: request.into_inner().operation,
            status: wire::OperationStatus::Pending as i32,
        }))
    }
    type WatchOperationStream =
        Pin<Box<dyn Stream<Item = Result<wire::OperationState, Status>> + Send + 'static>>;
    async fn watch_operation(
        &self,
        request: Request<wire::OperationRequest>,
    ) -> Result<Response<Self::WatchOperationStream>, Status> {
        let operation = request.into_inner().operation;
        Ok(Response::new(Box::pin(stream::iter(
            [
                wire::OperationStatus::Pending,
                wire::OperationStatus::Succeeded,
            ]
            .into_iter()
            .map(move |status| {
                Ok(wire::OperationState {
                    operation: operation.clone(),
                    status: status as i32,
                })
            }),
        ))))
    }
}

/// Starts a TLS-enabled all-routes Machines server on a loopback listener.
pub async fn serve_machines_rsa(
    listener: TcpListener,
    material: &RsaTlsMaterial,
    shutdown: oneshot::Receiver<()>,
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
            AllRoutesMachinesFixture,
        ))
        .serve_with_incoming_shutdown(TcpListenerStream::new(listener), async {
            let _ = shutdown.await;
        })
        .await
}
