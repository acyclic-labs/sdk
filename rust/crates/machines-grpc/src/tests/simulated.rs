//! A real gRPC `MachinesService` over the process-local simulator, so the
//! public conformance suite exercises the transport rather than a scripted fake.

use super::*;
use acyclic_auth::secrets::SecretRef;
use acyclic_machines::results::ResultCut;
use acyclic_machines_conformance as conformance;
use std::collections::BTreeMap;
use std::sync::Mutex;
use std::time::Duration;
use tonic::transport::Server;
use tonic::{Request, Response, Status};
use wire::machines_service_server::{MachinesService, MachinesServiceServer};

/// Serves the simulator over the generated RPCs and remembers which
/// idempotency key produced each operation, so a watch can replay its outcome.
#[derive(Clone)]
struct SimulatedService {
    machines: SimulatedMachines,
    keys: Arc<Mutex<BTreeMap<OperationId, IdempotencyKey>>>,
}

impl SimulatedService {
    fn new(machines: SimulatedMachines) -> Self {
        Self {
            machines,
            keys: Arc::new(Mutex::new(BTreeMap::new())),
        }
    }

    fn record(&self, operation: OperationId, key: IdempotencyKey) {
        self.keys
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .insert(operation, key);
    }

    fn key_for(&self, operation: OperationId) -> Option<IdempotencyKey> {
        self.keys
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .get(&operation)
            .copied()
    }

    async fn source_contract(&self, source: ForkSource) -> Result<MachineContract, Status> {
        match source {
            ForkSource::Machine(machine) => Ok(self
                .machines
                .inspect_machine(machine)
                .await
                .map_err(|error| status(&error))?
                .contract),
            ForkSource::Checkpoint(checkpoint) => Ok(self
                .machines
                .inspect_checkpoint(checkpoint)
                .await
                .map_err(|error| status(&error))?
                .contract),
        }
    }
}

fn status(error: &ProviderError) -> Status {
    let message = error.to_string();
    match error.classification() {
        ProviderError::NotFound(_) => Status::not_found(message),
        ProviderError::Conflict(_) => Status::aborted(message),
        ProviderError::Unsupported(_) => Status::unimplemented(message),
        ProviderError::Invalid(_) => Status::invalid_argument(message),
        ProviderError::Rejected(_) => Status::failed_precondition(message),
        ProviderError::FailedPrecondition(precondition) => Status::with_details(
            tonic::Code::FailedPrecondition,
            message,
            prost::bytes::Bytes::from_static(precondition.wire().as_bytes()),
        ),
        ProviderError::Unavailable => Status::unavailable(message),
        ProviderError::Cancelled => Status::cancelled(message),
        ProviderError::Indeterminate(_)
        | ProviderError::OperationIndeterminate(_)
        | ProviderError::CommandIndeterminate(_)
        | ProviderError::FileIndeterminate(_)
        | ProviderError::DeadlineSourceLost { .. }
        | ProviderError::Failed
        | ProviderError::ForkFailed { .. }
        | ProviderError::Caused(_) => Status::internal(message),
    }
}

fn decode_key(value: Option<&wire::IdempotencyKey>) -> Result<IdempotencyKey, Status> {
    let value = value.ok_or_else(|| Status::invalid_argument("idempotency key is missing"))?;
    let bytes: [u8; 16] = value
        .value
        .as_slice()
        .try_into()
        .map_err(|_| Status::invalid_argument("idempotency key width is invalid"))?;
    IdempotencyKey::from_bytes(bytes).map_err(|error| Status::invalid_argument(error.to_string()))
}

fn decode_image(value: Option<&wire::Image>) -> Result<Image, Status> {
    use wire::image::ImmutableReference;
    let value = value.ok_or_else(|| Status::invalid_argument("image is missing"))?;
    match (
        wire::ImageKind::try_from(value.kind)
            .map_err(|_| Status::invalid_argument("image kind is invalid"))?,
        value.immutable_reference.as_ref(),
    ) {
        (wire::ImageKind::ManagedOci, Some(ImmutableReference::ManagedDigest(digest))) => {
            let digest: [u8; 32] = digest
                .as_slice()
                .try_into()
                .map_err(|_| Status::invalid_argument("managed image digest width is invalid"))?;
            Image::managed(digest).map_err(|error| Status::invalid_argument(error.to_string()))
        }
        (wire::ImageKind::Custom, Some(ImmutableReference::CustomDigest(digest))) => {
            let digest: [u8; 32] = digest
                .as_slice()
                .try_into()
                .map_err(|_| Status::invalid_argument("custom image digest width is invalid"))?;
            Image::custom(digest).map_err(|error| Status::invalid_argument(error.to_string()))
        }
        (wire::ImageKind::Checkpoint, Some(ImmutableReference::Checkpoint(id))) => {
            decode_checkpoint(Some(id))
                .map(Image::Checkpoint)
                .map_err(|error| Status::invalid_argument(error.to_string()))
        }
        _ => Err(Status::invalid_argument("image shape is invalid")),
    }
}

fn decode_compatibility(
    value: Option<&wire::CompatibilityPolicy>,
) -> Result<CompatibilityPolicy, Status> {
    let value = value.ok_or_else(|| Status::invalid_argument("compatibility policy is missing"))?;
    let required = value
        .required
        .iter()
        .copied()
        .map(decode_capability)
        .collect::<Result<std::collections::BTreeSet<_>, _>>()
        .map_err(|error| Status::invalid_argument(error.to_string()))?;
    match wire::CompatibilityMode::try_from(value.mode)
        .map_err(|_| Status::invalid_argument("compatibility mode is invalid"))?
    {
        wire::CompatibilityMode::BestEffort if required.is_empty() => {
            Ok(CompatibilityPolicy::BestEffort)
        }
        wire::CompatibilityMode::Require if !required.is_empty() => {
            Ok(CompatibilityPolicy::Require(required))
        }
        _ => Err(Status::invalid_argument(
            "compatibility policy is contradictory",
        )),
    }
}

fn decode_expiration(value: Option<&wire::ExpirationPolicy>) -> Result<ExpirationPolicy, Status> {
    let value = value.ok_or_else(|| Status::invalid_argument("expiration policy is missing"))?;
    match wire::ExpirationKind::try_from(value.kind)
        .map_err(|_| Status::invalid_argument("expiration kind is invalid"))?
    {
        wire::ExpirationKind::Never if value.value_ms == 0 => Ok(ExpirationPolicy::Never),
        wire::ExpirationKind::MaxAge if value.value_ms != 0 => Ok(ExpirationPolicy::MaxAge(
            Duration::from_millis(value.value_ms),
        )),
        wire::ExpirationKind::At if value.value_ms != 0 => {
            Ok(ExpirationPolicy::AtUnixMs(value.value_ms))
        }
        wire::ExpirationKind::Idle if value.value_ms != 0 => Ok(ExpirationPolicy::Idle(
            Duration::from_millis(value.value_ms),
        )),
        _ => Err(Status::invalid_argument(
            "expiration policy is contradictory",
        )),
    }
}

fn decode_secrets(
    value: &BTreeMap<String, wire::selection::SecretRef>,
) -> Result<BTreeMap<String, SecretRef>, Status> {
    value
        .iter()
        .map(|(name, reference)| {
            let reference = SecretRef::new(
                reference.scope.clone(),
                reference.name.clone(),
                reference.version,
            )
            .map_err(|error| Status::invalid_argument(error.to_string()))?;
            Ok((name.clone(), reference))
        })
        .collect()
}

fn encode_outcome(outcome: &MutationOutcome) -> Result<wire::MutationOutcome, Status> {
    use wire::mutation_outcome::Result as Outcome;
    let result = match outcome {
        MutationOutcome::Created(observation) => Outcome::Created(
            observation
                .clone()
                .try_into()
                .map_err(|error: ProviderError| status(&error))?,
        ),
        MutationOutcome::VolumeAttached(observation) => Outcome::VolumeAttached(
            observation
                .clone()
                .try_into()
                .map_err(|error: ProviderError| status(&error))?,
        ),
        MutationOutcome::Checkpointed(observation) => Outcome::Checkpointed(
            observation
                .clone()
                .try_into()
                .map_err(|error: ProviderError| status(&error))?,
        ),
        MutationOutcome::Forked {
            provenance,
            results,
        } => Outcome::Forked(wire::ForkedMachines {
            provenance: Some((*provenance).into()),
            results: Some((*results).into()),
        }),
        MutationOutcome::Suspended(machine) => Outcome::Suspended(encode_machine(*machine)),
        MutationOutcome::Woken(machine) => Outcome::Woken(encode_machine(*machine)),
        MutationOutcome::SuspensionPolicySet(machine, policy) => {
            Outcome::SuspensionPolicySet(wire::PolicySet {
                machine: Some(encode_machine(*machine)),
                policy: Some(encode_suspension(*policy).map_err(|error| status(&error))?),
            })
        }
        MutationOutcome::MachineDestroyed(machine) => {
            Outcome::MachineDestroyed(encode_machine(*machine))
        }
        MutationOutcome::CheckpointDestroyed(checkpoint) => {
            Outcome::CheckpointDestroyed(encode_checkpoint(*checkpoint))
        }
    };
    Ok(wire::MutationOutcome {
        result: Some(result),
    })
}

fn machine_admission(
    observation: &MachineObservation,
    operation: OperationId,
) -> Result<wire::MachineAdmission, Status> {
    Ok(wire::MachineAdmission {
        machine: Some(encode_machine(observation.id)),
        operation: Some(encode_operation(operation)),
        contract: Some(
            observation
                .contract
                .clone()
                .try_into()
                .map_err(|error: ProviderError| status(&error))?,
        ),
        provenance: None,
    })
}

fn checkpoint_admission(
    observation: &CheckpointObservation,
    operation: OperationId,
) -> Result<wire::CheckpointAdmission, Status> {
    Ok(wire::CheckpointAdmission {
        checkpoint: Some(encode_checkpoint(observation.id)),
        source: Some(encode_machine(observation.source)),
        operation: Some(encode_operation(operation)),
        contract: Some(
            observation
                .contract
                .clone()
                .try_into()
                .map_err(|error: ProviderError| status(&error))?,
        ),
    })
}

fn mutation_admission(machine: MachineId, operation: OperationId) -> wire::MutationAdmission {
    wire::MutationAdmission {
        operation: Some(encode_operation(operation)),
        machine: Some(encode_machine(machine)),
        checkpoint: None,
    }
}

fn checkpoint_mutation_admission(
    checkpoint: CheckpointId,
    operation: OperationId,
) -> wire::MutationAdmission {
    wire::MutationAdmission {
        operation: Some(encode_operation(operation)),
        machine: None,
        checkpoint: Some(encode_checkpoint(checkpoint)),
    }
}

fn fork_admissions(
    provenance: Provenance<ForkSource, ForkFidelity>,
    results: ResultCut,
    contract: MachineContract,
    operation: OperationId,
) -> Result<wire::MachineAdmissions, Status> {
    Ok(wire::MachineAdmissions {
        operation: Some(encode_operation(operation)),
        provenance: Some(provenance.into()),
        count: results.count,
        contract: Some(
            contract
                .try_into()
                .map_err(|error: ProviderError| status(&error))?,
        ),
    })
}

#[tonic::async_trait]
impl MachinesService for SimulatedService {
    type ReadFileStream =
        Pin<Box<dyn Stream<Item = Result<wire::ReadFileResponse, Status>> + Send>>;
    async fn read_file(
        &self,
        _request: Request<tonic::Streaming<wire::ReadFileRequest>>,
    ) -> Result<Response<Self::ReadFileStream>, Status> {
        Err(Status::unimplemented("read_file"))
    }

    type WriteFileStream =
        Pin<Box<dyn Stream<Item = Result<wire::WriteFileResponse, Status>> + Send>>;
    async fn write_file(
        &self,
        _request: Request<tonic::Streaming<wire::WriteFileRequest>>,
    ) -> Result<Response<Self::WriteFileStream>, Status> {
        Err(Status::unimplemented("write_file"))
    }

    type ExecStream = Pin<Box<dyn Stream<Item = Result<wire::ExecOutput, Status>> + Send>>;
    async fn exec(
        &self,
        request: Request<tonic::Streaming<wire::ExecInput>>,
    ) -> Result<Response<Self::ExecStream>, Status> {
        let mut input = request.into_inner();
        let frame = input
            .message()
            .await?
            .ok_or_else(|| Status::invalid_argument("exec input ended before start"))?;
        let Some(wire::exec_input::Input::Start(start)) = frame.input else {
            return Err(Status::invalid_argument("exec input did not start"));
        };
        let machine = decode_machine(start.machine.as_ref())
            .map_err(|error| Status::invalid_argument(error.to_string()))?;
        let command = ExecCommand::new(vec!["true".into()], None)
            .map_err(|error| Status::invalid_argument(error.to_string()))?;
        match self.machines.exec(machine, command).await {
            Ok(_) => Err(Status::unimplemented("exec")),
            Err(error) => Err(status(&error)),
        }
    }

    async fn read_output(
        &self,
        _request: Request<wire::ReadOutputRequest>,
    ) -> Result<Response<wire::MachineOutput>, Status> {
        Err(Status::unimplemented("read_output"))
    }

    async fn qualify_image(
        &self,
        request: Request<wire::QualifyImageRequest>,
    ) -> Result<Response<wire::ImageQualification>, Status> {
        let image = decode_image(request.into_inner().image.as_ref())?;
        let qualification = self
            .machines
            .qualify_image(image)
            .await
            .map_err(|error| status(&error))?;
        Ok(Response::new(wire::ImageQualification {
            image: Some(encode_image(&qualification.image).map_err(|error| status(&error))?),
            capabilities: qualification
                .capabilities
                .into_iter()
                .map(encode_capability)
                .collect(),
            compatibility_revision: qualification.compatibility_revision.to_vec(),
        }))
    }

    async fn create(
        &self,
        request: Request<wire::CreateMachineRequest>,
    ) -> Result<Response<wire::MachineAdmission>, Status> {
        let request = request.into_inner();
        let key = decode_key(request.idempotency_key.as_ref())?;
        let create = CreateMachine {
            idempotency_key: key,
            image: Some(decode_image(request.image.as_ref())?),
            selected_volume_ref: request.selected_volume_ref,
            secrets: decode_secrets(&request.secrets)?,
            compatibility: decode_compatibility(request.compatibility.as_ref())?,
            suspension: decode_suspension(request.suspension.as_ref())
                .map_err(|error| Status::invalid_argument(error.to_string()))?,
            expiration: decode_expiration(request.expiration.as_ref())?,
            network_policy_digest: request
                .network_policy_digest
                .as_slice()
                .try_into()
                .map_err(|_| Status::invalid_argument("network policy digest width is invalid"))?,
            budgets: request
                .budgets
                .map_or_else(Budgets::default, |value| Budgets {
                    spend_micros: value.spend_micros,
                    concurrency: value.concurrency,
                }),
        };
        let outcome = self
            .machines
            .create(create)
            .await
            .map_err(|error| status(&error))?;
        let MutationOutcome::Created(observation) = outcome else {
            return Err(Status::internal("create returned the wrong outcome"));
        };
        let operation = self
            .machines
            .recover_operation(key)
            .await
            .map_err(|error| status(&error))?;
        self.record(operation, key);
        Ok(Response::new(machine_admission(&observation, operation)?))
    }

    async fn attach_volume(
        &self,
        request: Request<wire::AttachVolumeRequest>,
    ) -> Result<Response<wire::MachineAdmission>, Status> {
        let request = request.into_inner();
        let key = decode_key(request.idempotency_key.as_ref())?;
        let machine = decode_machine(request.machine.as_ref())
            .map_err(|error| Status::invalid_argument(error.to_string()))?;
        let outcome = self
            .machines
            .attach_volume(
                machine,
                request.selected_volume_ref.unwrap_or_default(),
                key,
            )
            .await
            .map_err(|error| status(&error))?;
        let MutationOutcome::VolumeAttached(observation) = outcome else {
            return Err(Status::internal("attach_volume returned the wrong outcome"));
        };
        let operation = self
            .machines
            .recover_operation(key)
            .await
            .map_err(|error| status(&error))?;
        self.record(operation, key);
        Ok(Response::new(machine_admission(&observation, operation)?))
    }

    async fn checkpoint(
        &self,
        request: Request<wire::CheckpointMachineRequest>,
    ) -> Result<Response<wire::CheckpointAdmission>, Status> {
        let request = request.into_inner();
        let key = decode_key(request.idempotency_key.as_ref())?;
        let machine = decode_machine(request.machine.as_ref())
            .map_err(|error| Status::invalid_argument(error.to_string()))?;
        let outcome = self
            .machines
            .checkpoint(machine, key)
            .await
            .map_err(|error| status(&error))?;
        let MutationOutcome::Checkpointed(observation) = outcome else {
            return Err(Status::internal("checkpoint returned the wrong outcome"));
        };
        let operation = self
            .machines
            .recover_operation(key)
            .await
            .map_err(|error| status(&error))?;
        self.record(operation, key);
        Ok(Response::new(checkpoint_admission(
            &observation,
            operation,
        )?))
    }

    async fn fork(
        &self,
        request: Request<wire::ForkCheckpointRequest>,
    ) -> Result<Response<wire::MachineAdmissions>, Status> {
        let request = request.into_inner();
        let key = decode_key(request.idempotency_key.as_ref())?;
        let checkpoint = decode_checkpoint(request.checkpoint.as_ref())
            .map_err(|error| Status::invalid_argument(error.to_string()))?;
        let count = NonZeroU32::new(request.count)
            .ok_or_else(|| Status::invalid_argument("fork count is zero"))?;
        let outcome = self
            .machines
            .fork(checkpoint, count, key)
            .await
            .map_err(|error| status(&error))?;
        let MutationOutcome::Forked {
            provenance,
            results,
        } = outcome
        else {
            return Err(Status::internal("fork returned the wrong outcome"));
        };
        let operation = self
            .machines
            .recover_operation(key)
            .await
            .map_err(|error| status(&error))?;
        self.record(operation, key);
        let contract = self
            .machines
            .inspect_checkpoint(checkpoint)
            .await
            .map_err(|error| status(&error))?
            .contract;
        Ok(Response::new(fork_admissions(
            provenance, results, contract, operation,
        )?))
    }

    async fn fork_machine(
        &self,
        request: Request<wire::ForkMachineRequest>,
    ) -> Result<Response<wire::MachineAdmissions>, Status> {
        let request = request.into_inner();
        let key = decode_key(request.idempotency_key.as_ref())?;
        let machine = decode_machine(request.machine.as_ref())
            .map_err(|error| Status::invalid_argument(error.to_string()))?;
        let count = NonZeroU32::new(request.count)
            .ok_or_else(|| Status::invalid_argument("fork count is zero"))?;
        let outcome = self
            .machines
            .fork_machine(machine, count, key)
            .await
            .map_err(|error| status(&error))?;
        let MutationOutcome::Forked {
            provenance,
            results,
        } = outcome
        else {
            return Err(Status::internal("fork_machine returned the wrong outcome"));
        };
        let operation = self
            .machines
            .recover_operation(key)
            .await
            .map_err(|error| status(&error))?;
        self.record(operation, key);
        let contract = self
            .machines
            .inspect_machine(machine)
            .await
            .map_err(|error| status(&error))?
            .contract;
        Ok(Response::new(fork_admissions(
            provenance, results, contract, operation,
        )?))
    }

    async fn suspend(
        &self,
        request: Request<wire::MachineMutationRequest>,
    ) -> Result<Response<wire::MutationAdmission>, Status> {
        let request = request.into_inner();
        let key = decode_key(request.idempotency_key.as_ref())?;
        let machine = decode_machine(request.machine.as_ref())
            .map_err(|error| Status::invalid_argument(error.to_string()))?;
        let outcome = self
            .machines
            .suspend(machine, key)
            .await
            .map_err(|error| status(&error))?;
        let MutationOutcome::Suspended(machine) = outcome else {
            return Err(Status::internal("suspend returned the wrong outcome"));
        };
        let operation = self
            .machines
            .recover_operation(key)
            .await
            .map_err(|error| status(&error))?;
        self.record(operation, key);
        Ok(Response::new(mutation_admission(machine, operation)))
    }

    async fn wake(
        &self,
        request: Request<wire::MachineMutationRequest>,
    ) -> Result<Response<wire::MutationAdmission>, Status> {
        let request = request.into_inner();
        let key = decode_key(request.idempotency_key.as_ref())?;
        let machine = decode_machine(request.machine.as_ref())
            .map_err(|error| Status::invalid_argument(error.to_string()))?;
        let outcome = self
            .machines
            .wake(machine, key)
            .await
            .map_err(|error| status(&error))?;
        let MutationOutcome::Woken(machine) = outcome else {
            return Err(Status::internal("wake returned the wrong outcome"));
        };
        let operation = self
            .machines
            .recover_operation(key)
            .await
            .map_err(|error| status(&error))?;
        self.record(operation, key);
        Ok(Response::new(mutation_admission(machine, operation)))
    }

    async fn set_suspension_policy(
        &self,
        request: Request<wire::SetSuspensionPolicyRequest>,
    ) -> Result<Response<wire::PolicyAdmission>, Status> {
        let request = request.into_inner();
        let key = decode_key(request.idempotency_key.as_ref())?;
        let machine = decode_machine(request.machine.as_ref())
            .map_err(|error| Status::invalid_argument(error.to_string()))?;
        let policy = decode_suspension(request.policy.as_ref())
            .map_err(|error| Status::invalid_argument(error.to_string()))?;
        let outcome = self
            .machines
            .set_suspension_policy(machine, policy, key)
            .await
            .map_err(|error| status(&error))?;
        let MutationOutcome::SuspensionPolicySet(machine, policy) = outcome else {
            return Err(Status::internal(
                "set_suspension_policy returned the wrong outcome",
            ));
        };
        let operation = self
            .machines
            .recover_operation(key)
            .await
            .map_err(|error| status(&error))?;
        self.record(operation, key);
        Ok(Response::new(wire::PolicyAdmission {
            machine: Some(encode_machine(machine)),
            operation: Some(encode_operation(operation)),
            policy: Some(encode_suspension(policy).map_err(|error| status(&error))?),
        }))
    }

    async fn destroy_machine(
        &self,
        request: Request<wire::MachineMutationRequest>,
    ) -> Result<Response<wire::MutationAdmission>, Status> {
        let request = request.into_inner();
        let key = decode_key(request.idempotency_key.as_ref())?;
        let machine = decode_machine(request.machine.as_ref())
            .map_err(|error| Status::invalid_argument(error.to_string()))?;
        let outcome = self
            .machines
            .destroy_machine(machine, key)
            .await
            .map_err(|error| status(&error))?;
        let MutationOutcome::MachineDestroyed(machine) = outcome else {
            return Err(Status::internal(
                "destroy_machine returned the wrong outcome",
            ));
        };
        let operation = self
            .machines
            .recover_operation(key)
            .await
            .map_err(|error| status(&error))?;
        self.record(operation, key);
        Ok(Response::new(mutation_admission(machine, operation)))
    }

    async fn destroy_checkpoint(
        &self,
        request: Request<wire::CheckpointMutationRequest>,
    ) -> Result<Response<wire::MutationAdmission>, Status> {
        let request = request.into_inner();
        let key = decode_key(request.idempotency_key.as_ref())?;
        let checkpoint = decode_checkpoint(request.checkpoint.as_ref())
            .map_err(|error| Status::invalid_argument(error.to_string()))?;
        let outcome = self
            .machines
            .destroy_checkpoint(checkpoint, key)
            .await
            .map_err(|error| status(&error))?;
        let MutationOutcome::CheckpointDestroyed(checkpoint) = outcome else {
            return Err(Status::internal(
                "destroy_checkpoint returned the wrong outcome",
            ));
        };
        let operation = self
            .machines
            .recover_operation(key)
            .await
            .map_err(|error| status(&error))?;
        self.record(operation, key);
        Ok(Response::new(checkpoint_mutation_admission(
            checkpoint, operation,
        )))
    }

    async fn read_results(
        &self,
        request: Request<wire::ReadResultsRequest>,
    ) -> Result<Response<wire::ResultPage>, Status> {
        let request = request.into_inner();
        let cursor = decode_result_cursor(request.cursor.as_ref())
            .map_err(|error| Status::invalid_argument(error.to_string()))?;
        let credit = NonZeroU32::new(request.credit)
            .ok_or_else(|| Status::invalid_argument("result credit is zero"))?;
        let page = self
            .machines
            .read_results(cursor, credit)
            .await
            .map_err(|error| status(&error))?;
        Ok(Response::new(wire::ResultPage {
            cursor: Some(page.cursor.into()),
            machines: page
                .machines
                .into_iter()
                .map(TryInto::try_into)
                .collect::<Result<Vec<_>, ProviderError>>()
                .map_err(|error| status(&error))?,
            next: page.next.map(Into::into),
        }))
    }

    async fn recover(
        &self,
        request: Request<wire::RecoverRequest>,
    ) -> Result<Response<wire::RecoveredAdmission>, Status> {
        let request = request.into_inner();
        let key = decode_key(request.idempotency_key.as_ref())?;
        let outcome = self
            .machines
            .recover(key)
            .await
            .map_err(|error| status(&error))?;
        let operation = self
            .machines
            .recover_operation(key)
            .await
            .map_err(|error| status(&error))?;
        self.record(operation, key);
        let result = match outcome {
            MutationOutcome::Created(observation) => wire::recovered_admission::Result::Create(
                machine_admission(&observation, operation)?,
            ),
            MutationOutcome::VolumeAttached(observation) => {
                wire::recovered_admission::Result::AttachVolume(machine_admission(
                    &observation,
                    operation,
                )?)
            }
            MutationOutcome::Checkpointed(observation) => {
                wire::recovered_admission::Result::Checkpoint(checkpoint_admission(
                    &observation,
                    operation,
                )?)
            }
            MutationOutcome::Forked {
                provenance,
                results,
            } => {
                let contract = self.source_contract(provenance.source).await?;
                wire::recovered_admission::Result::Forked(fork_admissions(
                    provenance, results, contract, operation,
                )?)
            }
            MutationOutcome::Suspended(machine) => {
                wire::recovered_admission::Result::Suspend(mutation_admission(machine, operation))
            }
            MutationOutcome::Woken(machine) => {
                wire::recovered_admission::Result::Wake(mutation_admission(machine, operation))
            }
            MutationOutcome::SuspensionPolicySet(machine, policy) => {
                wire::recovered_admission::Result::SetSuspensionPolicy(wire::PolicyAdmission {
                    machine: Some(encode_machine(machine)),
                    operation: Some(encode_operation(operation)),
                    policy: Some(encode_suspension(policy).map_err(|error| status(&error))?),
                })
            }
            MutationOutcome::MachineDestroyed(machine) => {
                wire::recovered_admission::Result::DestroyMachine(mutation_admission(
                    machine, operation,
                ))
            }
            MutationOutcome::CheckpointDestroyed(checkpoint) => {
                wire::recovered_admission::Result::DestroyCheckpoint(checkpoint_mutation_admission(
                    checkpoint, operation,
                ))
            }
        };
        Ok(Response::new(wire::RecoveredAdmission {
            operation: Some(encode_operation(operation)),
            result: Some(result),
        }))
    }

    async fn inspect_machine(
        &self,
        request: Request<wire::InspectMachineRequest>,
    ) -> Result<Response<wire::MachineState>, Status> {
        let machine = decode_machine(request.into_inner().machine.as_ref())
            .map_err(|error| Status::invalid_argument(error.to_string()))?;
        let observation = self
            .machines
            .inspect_machine(machine)
            .await
            .map_err(|error| status(&error))?;
        Ok(Response::new(
            observation
                .try_into()
                .map_err(|error: ProviderError| status(&error))?,
        ))
    }

    async fn inspect_checkpoint(
        &self,
        request: Request<wire::InspectCheckpointRequest>,
    ) -> Result<Response<wire::CheckpointState>, Status> {
        let checkpoint = decode_checkpoint(request.into_inner().checkpoint.as_ref())
            .map_err(|error| Status::invalid_argument(error.to_string()))?;
        let observation = self
            .machines
            .inspect_checkpoint(checkpoint)
            .await
            .map_err(|error| status(&error))?;
        Ok(Response::new(
            observation
                .try_into()
                .map_err(|error: ProviderError| status(&error))?,
        ))
    }

    async fn list_machines(
        &self,
        request: Request<wire::ListMachinesRequest>,
    ) -> Result<Response<wire::MachinePage>, Status> {
        let request = request.into_inner();
        let after = request
            .after
            .as_ref()
            .map(|id| decode_machine(Some(id)))
            .transpose()
            .map_err(|error| Status::invalid_argument(error.to_string()))?;
        let page = self
            .machines
            .list_machines(after, request.limit)
            .await
            .map_err(|error| status(&error))?;
        Ok(Response::new(
            page.try_into()
                .map_err(|error: ProviderError| status(&error))?,
        ))
    }

    async fn events(
        &self,
        request: Request<wire::EventsRequest>,
    ) -> Result<Response<wire::EventPage>, Status> {
        let request = request.into_inner();
        let machine = decode_machine(request.machine.as_ref())
            .map_err(|error| Status::invalid_argument(error.to_string()))?;
        let page = self
            .machines
            .events(machine, Some(request.after_sequence), request.limit)
            .await
            .map_err(|error| status(&error))?;
        Ok(Response::new(wire::EventPage {
            events: page.events.into_iter().map(Into::into).collect(),
            next_sequence: page.next_sequence.unwrap_or(0),
        }))
    }

    type FollowEventsStream =
        Pin<Box<dyn Stream<Item = Result<wire::MachineEvent, Status>> + Send>>;
    async fn follow_events(
        &self,
        _request: Request<wire::FollowEventsRequest>,
    ) -> Result<Response<Self::FollowEventsStream>, Status> {
        Err(Status::unimplemented("follow_events"))
    }

    async fn usage(
        &self,
        _request: Request<wire::UsageRequest>,
    ) -> Result<Response<wire::UsageReceipt>, Status> {
        // The simulator has no provider-authenticated receipt, so the fixture
        // declares Usage unsupported rather than fabricating one.
        Err(Status::unimplemented("usage"))
    }

    async fn cancel(
        &self,
        request: Request<wire::OperationRequest>,
    ) -> Result<Response<wire::OperationState>, Status> {
        let operation = decode_operation(request.into_inner().operation.as_ref())
            .map_err(|error| Status::invalid_argument(error.to_string()))?;
        let observation = self
            .machines
            .cancel(operation)
            .await
            .map_err(|error| status(&error))?;
        Ok(Response::new(observation.into()))
    }

    async fn inspect_operation(
        &self,
        request: Request<wire::OperationRequest>,
    ) -> Result<Response<wire::OperationState>, Status> {
        let operation = decode_operation(request.into_inner().operation.as_ref())
            .map_err(|error| Status::invalid_argument(error.to_string()))?;
        let observation = self
            .machines
            .inspect_operation(operation)
            .await
            .map_err(|error| status(&error))?;
        Ok(Response::new(observation.into()))
    }

    type WatchOperationStream =
        Pin<Box<dyn Stream<Item = Result<wire::OperationState, Status>> + Send + 'static>>;
    async fn watch_operation(
        &self,
        request: Request<wire::OperationRequest>,
    ) -> Result<Response<Self::WatchOperationStream>, Status> {
        let operation = decode_operation(request.into_inner().operation.as_ref())
            .map_err(|error| Status::invalid_argument(error.to_string()))?;
        let observation = self
            .machines
            .inspect_operation(operation)
            .await
            .map_err(|error| status(&error))?;
        let outcome = match self.key_for(operation) {
            Some(key) => self.machines.recover(key).await.ok(),
            None => None,
        };
        let mut state: wire::OperationState = observation.into();
        state.outcome = outcome.as_ref().map(encode_outcome).transpose()?;
        Ok(Response::new(Box::pin(stream::once(
            async move { Ok(state) },
        ))))
    }
}

async fn serve_simulated() -> Result<
    (
        GrpcProvider,
        SimulatedMachines,
        tokio::sync::oneshot::Sender<()>,
        tokio::task::JoinHandle<Result<(), tonic::transport::Error>>,
    ),
    Box<dyn std::error::Error + Send + Sync>,
> {
    let machines = SimulatedMachines::default();
    let service = SimulatedService::new(machines.clone());
    let (incoming, connections) = tokio::sync::mpsc::channel(1);
    let (shutdown_tx, shutdown_rx) = tokio::sync::oneshot::channel();
    let server = tokio::spawn(async move {
        Server::builder()
            .add_service(MachinesServiceServer::new(service))
            .serve_with_incoming_shutdown(ReceiverStream::new(connections), async {
                let _ = shutdown_rx.await;
            })
            .await
    });
    let channel = TonicEndpoint::from_static("http://contract.fixture")
        .connect_with_connector(tower::service_fn(move |_| {
            let incoming = incoming.clone();
            async move {
                let (client, server) = tokio::io::duplex(65536);
                incoming
                    .send(Ok::<_, std::io::Error>(server))
                    .await
                    .map_err(|_| std::io::Error::other("fixture server closed"))?;
                Ok::<_, std::io::Error>(hyper_util::rt::TokioIo::new(client))
            }
        }))
        .await?;
    Ok((
        GrpcProvider::grpc_with_auth(channel, BearerAuth::new("opaque.account+/=")?),
        machines,
        shutdown_tx,
        server,
    ))
}

#[tokio::test]
async fn grpc_provider_passes_the_public_conformance_suite() -> Result<(), String> {
    let (provider, machines, shutdown, server) =
        serve_simulated().await.map_err(|e| e.to_string())?;
    let image = Image::managed([0x19; 32]).map_err(|error| error.to_string())?;
    let fixture =
        conformance::Fixture::new(provider, image, [0x19; 32]).declaring(move |capability| {
            match capability {
                // The simulator has no hosted guest admission and no
                // provider-authenticated usage receipt, so the deployment behind
                // this transport does not serve them.
                ProviderCapability::AttachVolume | ProviderCapability::Usage => {
                    Support::Unsupported
                }
                other => machines.capability(other),
            }
        });
    let result = conformance::verify(&fixture).await;
    let _ = shutdown.send(());
    server
        .await
        .map_err(|error| error.to_string())?
        .map_err(|error| error.to_string())?;
    result
}
