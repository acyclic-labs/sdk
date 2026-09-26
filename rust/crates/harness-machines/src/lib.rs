#![deny(unsafe_code)]
#![cfg_attr(test, allow(clippy::indexing_slicing, clippy::too_many_lines))]
#![doc = include_str!("../README.md")]

mod execution;
pub use execution::{AccessFuture, MachinesExecution, MachinesExecutionAccess, MachinesTaskBuild};

use acyclic_harness::{
    Error, IdempotencyKey, OperationId, Result,
    fork::{
        Capture, CapturedResource, ForkCaptureProvider, ForkRequest, ForkSeed, ForkSeedVerifier,
        ForkSelection, ResourceRevision,
    },
    resources::{ArtifactRef, CheckpointRef, ProviderRef, SandboxRef},
};
use acyclic_machines::{
    CheckpointId, CheckpointObservation, CreateMachine, IdempotencyKey as MachinesKey, Image,
    ImageQualification, MachineId, MachineObservation, MachinesProvider, MutationOutcome,
    OperationId as MachinesOperationId, OperationObservation, ProviderError,
};
use std::{future::Future, num::NonZeroU32, pin::Pin, sync::Arc};

/// Concrete adapter over any customer-hosted or managed Machines provider.
#[derive(Clone)]
pub struct MachinesHost {
    provider: Arc<dyn MachinesProvider>,
    provider_ref: ProviderRef,
}

impl MachinesHost {
    /// Binds one explicit provider implementation and immutable provider identity.
    pub fn new(provider: Arc<dyn MachinesProvider>, provider_ref: ProviderRef) -> Result<Self> {
        provider_ref.validate()?;
        if provider_ref.family() != "machines" {
            return Err(Error::Invalid(
                "Machines adapter requires the machines provider family".into(),
            ));
        }
        Ok(Self {
            provider,
            provider_ref,
        })
    }

    /// Qualifies an immutable image without creating a sandbox.
    pub async fn qualify(&self, artifact: &ArtifactRef) -> Result<ImageQualification> {
        self.provider
            .qualify_image(self.image(artifact)?)
            .await
            .map_err(|error| map_error(error, None))
    }

    /// Creates one durable sandbox from an immutable artifact.
    pub async fn create(
        &self,
        operation_id: OperationId,
        idempotency_key: &IdempotencyKey,
        artifact: &ArtifactRef,
        network_policy_digest: [u8; 32],
        configure: impl FnOnce(&mut CreateMachine),
    ) -> Result<SandboxRef> {
        if network_policy_digest == [0; 32] {
            return Err(Error::Invalid(
                "network policy digest must be an explicit nonzero commitment".into(),
            ));
        }
        let mut request = CreateMachine::new(
            machines_key(idempotency_key)?,
            self.image(artifact)?,
            network_policy_digest,
        );
        configure(&mut request);
        match self
            .provider
            .create(request)
            .await
            .map_err(|error| map_error(error, Some(operation_id)))?
        {
            MutationOutcome::Created(observation) => {
                SandboxRef::new(self.provider_ref.clone(), observation.id.as_bytes(), None)
            }
            _ => Err(Error::Storage(
                "Machines returned the wrong create outcome".into(),
            )),
        }
    }

    /// Reads the provider's durable sandbox observation.
    pub async fn inspect(&self, sandbox: &SandboxRef) -> Result<MachineObservation> {
        self.provider
            .inspect_machine(self.machine_id(sandbox)?)
            .await
            .map_err(|error| map_error(error, None))
    }

    /// Creates one immutable checkpoint reference.
    pub async fn checkpoint(
        &self,
        operation_id: OperationId,
        idempotency_key: &IdempotencyKey,
        sandbox: &SandboxRef,
    ) -> Result<CheckpointRef> {
        match self
            .provider
            .checkpoint(self.machine_id(sandbox)?, machines_key(idempotency_key)?)
            .await
            .map_err(|error| map_error(error, Some(operation_id)))?
        {
            MutationOutcome::Checkpointed(observation) => {
                CheckpointRef::new(self.provider_ref.clone(), observation.id.as_bytes(), None)
            }
            _ => Err(Error::Storage(
                "Machines returned the wrong checkpoint outcome".into(),
            )),
        }
    }

    /// Reads one immutable checkpoint observation.
    pub async fn inspect_checkpoint(
        &self,
        checkpoint: &CheckpointRef,
    ) -> Result<CheckpointObservation> {
        self.provider
            .inspect_checkpoint(self.checkpoint_id(checkpoint)?)
            .await
            .map_err(|error| map_error(error, None))
    }

    /// Forks a checkpoint and returns provider-owned sandbox references.
    pub async fn fork(
        &self,
        operation_id: OperationId,
        idempotency_key: &IdempotencyKey,
        checkpoint: &CheckpointRef,
        count: NonZeroU32,
        performance: acyclic_machines::Performance,
    ) -> Result<Vec<SandboxRef>> {
        match self
            .provider
            .fork(
                self.checkpoint_id(checkpoint)?,
                count,
                performance,
                machines_key(idempotency_key)?,
            )
            .await
            .map_err(|error| map_error(error, Some(operation_id)))?
        {
            MutationOutcome::Forked(values) => values
                .into_iter()
                .map(|value| SandboxRef::new(self.provider_ref.clone(), value.id.as_bytes(), None))
                .collect(),
            _ => Err(Error::Storage(
                "Machines returned the wrong fork outcome".into(),
            )),
        }
    }

    /// Cancels a provider operation identified by the Machines admission contract.
    pub async fn cancel(&self, operation_id: MachinesOperationId) -> Result<OperationObservation> {
        self.provider
            .cancel(operation_id)
            .await
            .map_err(|error| map_error(error, None))
    }

    fn image(&self, reference: &ArtifactRef) -> Result<Image> {
        reference.validate()?;
        self.validate_provider(reference.as_resource().provider())?;
        if reference.as_resource().version().is_some() {
            return Err(Error::Invalid(
                "Machines image identity cannot carry a version".into(),
            ));
        }
        let digest: [u8; 32] =
            reference.as_resource().key().try_into().map_err(|_| {
                Error::Invalid("machine image artifact key must be 32 bytes".into())
            })?;
        Image::custom(digest).map_err(|error| Error::Invalid(error.to_string()))
    }

    fn machine_id(&self, reference: &SandboxRef) -> Result<MachineId> {
        reference.validate()?;
        self.validate_provider(reference.as_resource().provider())?;
        if reference.as_resource().version().is_some() {
            return Err(Error::Invalid(
                "Machines sandbox identity cannot carry a version".into(),
            ));
        }
        parse_uuid(reference.as_resource().key(), MachineId::parse)
    }

    fn checkpoint_id(&self, reference: &CheckpointRef) -> Result<CheckpointId> {
        reference.validate()?;
        self.validate_provider(reference.as_resource().provider())?;
        if reference.as_resource().version().is_some() {
            return Err(Error::Invalid(
                "Machines checkpoint identity cannot carry a version".into(),
            ));
        }
        parse_uuid(reference.as_resource().key(), CheckpointId::parse)
    }

    fn validate_provider(&self, provider: &ProviderRef) -> Result<()> {
        provider.validate()?;
        if provider != &self.provider_ref {
            return Err(Error::Invalid(
                "resource belongs to another Machines provider".into(),
            ));
        }
        Ok(())
    }
}

/// Admission proof for exact Machines checkpoint revisions selected by a parent fork.
impl ForkSeedVerifier for MachinesHost {
    fn provider(&self) -> &ProviderRef {
        &self.provider_ref
    }

    fn verify<'a>(
        &'a self,
        seed: &'a ForkSeed,
    ) -> Pin<Box<dyn Future<Output = Result<()>> + Send + 'a>> {
        Box::pin(async move {
            seed.validate()?;
            for capture in &seed.resources {
                let revision = &capture.revision;
                if revision.provider() != &self.provider_ref {
                    continue;
                }
                let ResourceRevision::Process(checkpoint) = revision else {
                    return Err(Error::Unsupported(
                        "Machines fork verifier cannot prove this resource".into(),
                    ));
                };
                let observed = self.inspect_checkpoint(checkpoint).await?;
                if !observed.forkable {
                    return Err(Error::Conflict(
                        "selected Machines checkpoint is not forkable".into(),
                    ));
                }
            }
            Ok(())
        })
    }
}

/// Exact, read-only capture of a forkable process checkpoint. A child's
/// sandbox remains a separately admitted Machines operation, not a cloned
/// active process or an implicit side effect of conversation publication.
impl ForkCaptureProvider for MachinesHost {
    fn provider(&self) -> &ProviderRef {
        &self.provider_ref
    }

    fn capture<'a>(
        &'a self,
        _request: &'a ForkRequest,
        selection: &'a ForkSelection,
    ) -> Pin<Box<dyn Future<Output = Result<Capture>> + Send + 'a>> {
        Box::pin(async move {
            let ResourceRevision::Process(checkpoint) = &selection.revision else {
                return Ok(Capture::Unsupported(
                    "Machines captures only process checkpoints".into(),
                ));
            };
            let observed = self.inspect_checkpoint(checkpoint).await?;
            if !observed.forkable {
                return Ok(Capture::Unsupported(
                    "selected Machines checkpoint is not forkable".into(),
                ));
            }
            Ok(Capture::Captured(CapturedResource {
                source: selection.revision.clone(),
                revision: selection.revision.clone(),
            }))
        })
    }

    fn reconcile<'a>(
        &'a self,
        request: &'a ForkRequest,
        selection: &'a ForkSelection,
    ) -> Pin<Box<dyn Future<Output = Result<Option<Capture>>> + Send + 'a>> {
        // Inspection alone cannot have created a provider effect.
        Box::pin(async move { self.capture(request, selection).await.map(Some) })
    }
}

fn parse_uuid<T>(
    bytes: &[u8],
    parse: impl FnOnce(&str) -> std::result::Result<T, acyclic_machines::IdentityError>,
) -> Result<T> {
    let bytes: [u8; 16] = bytes
        .try_into()
        .map_err(|_| Error::Invalid("Machines reference key must be 16 bytes".into()))?;
    parse(&uuid::Uuid::from_bytes(bytes).to_string())
        .map_err(|error| Error::Invalid(error.to_string()))
}

fn machines_key(key: &IdempotencyKey) -> Result<MachinesKey> {
    IdempotencyKey::new(key.0.clone())?;
    let mut bytes = *blake3::hash(key.as_str().as_bytes()).as_bytes();
    if bytes[..16] == [0; 16] {
        bytes[0] = 1;
    }
    MachinesKey::parse(
        &uuid::Uuid::from_bytes(
            bytes[..16]
                .try_into()
                .map_err(|_| Error::Invalid("key digest".into()))?,
        )
        .to_string(),
    )
    .map_err(|error| Error::Invalid(error.to_string()))
}

fn map_error(error: ProviderError, operation: Option<OperationId>) -> Error {
    match error {
        ProviderError::NotFound(value) => Error::NotFound(value),
        ProviderError::Conflict(value) => Error::Conflict(value),
        ProviderError::Unsupported(value) => Error::Unsupported(value),
        ProviderError::Invalid(value) | ProviderError::Rejected(value) => Error::Invalid(value),
        ProviderError::Indeterminate(_) | ProviderError::Unavailable => operation.map_or_else(
            || Error::Storage("Machines outcome is unknown".into()),
            Error::Indeterminate,
        ),
        ProviderError::OperationIndeterminate(operation) => {
            Error::Indeterminate(OperationId::from_bytes(operation.as_bytes()))
        }
        ProviderError::Failed => Error::Storage("Machines operation failed".into()),
        ProviderError::Cancelled => Error::Storage("Machines operation was cancelled".into()),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use acyclic_harness::{
        AgentId,
        conversation::{VolumeClass, VolumeOwner, VolumeRef},
        core::{AggregateKind, Authority},
        fork::{CapturedResource, ForkPreparation},
        resources::{GenerationRef, StreamRef},
    };
    use acyclic_machines::SimulatedMachines;

    #[test]
    fn operation_indeterminate_preserves_the_exact_provider_operation() -> Result<()> {
        let operation = MachinesOperationId::parse("00000000-0000-0000-0000-000000000001")
            .map_err(|error| Error::Invalid(error.to_string()))?;
        assert_eq!(
            map_error(ProviderError::OperationIndeterminate(operation), None),
            Error::Indeterminate(OperationId::from_bytes(operation.as_bytes()))
        );
        Ok(())
    }

    #[tokio::test]
    async fn opaque_refs_round_trip_through_a_replaceable_provider() -> Result<()> {
        let provider_ref = ProviderRef::new("example", "machines", "1")?;
        let host = MachinesHost::new(Arc::new(SimulatedMachines::default()), provider_ref.clone())?;
        let artifact = ArtifactRef::new(provider_ref, [1; 32], None)?;
        let sandbox = host
            .create(
                OperationId::from_bytes([1; 16]),
                &IdempotencyKey::new("create-sandbox")?,
                &artifact,
                [2; 32],
                |_| {},
            )
            .await?;
        let observed = host.inspect(&sandbox).await?;
        assert_eq!(sandbox.as_resource().key(), observed.id.as_bytes());

        let foreign = SandboxRef::new(
            ProviderRef::new("other", "machines", "1")?,
            observed.id.as_bytes(),
            None,
        )?;
        assert!(matches!(
            host.inspect(&foreign).await,
            Err(Error::Invalid(_))
        ));
        Ok(())
    }

    #[tokio::test]
    async fn create_requires_explicit_network_policy_and_provider_ownership() -> Result<()> {
        let provider_ref = ProviderRef::new("example", "machines", "1")?;
        let host = MachinesHost::new(Arc::new(SimulatedMachines::default()), provider_ref)?;
        let foreign = ArtifactRef::new(ProviderRef::new("other", "machines", "1")?, [1; 32], None)?;
        assert!(matches!(
            host.qualify(&foreign).await,
            Err(Error::Invalid(_))
        ));

        let artifact =
            ArtifactRef::new(ProviderRef::new("example", "machines", "1")?, [1; 32], None)?;
        assert!(matches!(
            host.create(
                OperationId::from_bytes([2; 16]),
                &IdempotencyKey::new("zero-policy")?,
                &artifact,
                [0; 32],
                |_| {},
            )
            .await,
            Err(Error::Invalid(_))
        ));
        Ok(())
    }

    #[tokio::test]
    async fn fork_admission_proves_the_selected_checkpoint_exists() -> Result<()> {
        let machines = ProviderRef::new("example", "machines", "1")?;
        let simulator = Arc::new(SimulatedMachines::default());
        let host = MachinesHost::new(simulator.clone(), machines.clone())?;
        let artifact = ArtifactRef::new(machines.clone(), [1; 32], None)?;
        let sandbox = host
            .create(
                OperationId::from_bytes([1; 16]),
                &IdempotencyKey::new("fork-verifier-machine")?,
                &artifact,
                [2; 32],
                |_| {},
            )
            .await?;
        let checkpoint = host
            .checkpoint(
                OperationId::from_bytes([2; 16]),
                &IdempotencyKey::new("fork-verifier-checkpoint")?,
                &sandbox,
            )
            .await?;
        let parent = Authority {
            kind: AggregateKind::Conversation,
            id: "parent".into(),
        };
        let child = Authority {
            kind: AggregateKind::Conversation,
            id: "child".into(),
        };
        let filesystem = ProviderRef::new("example", "filesystem", "2")?;
        let owner = VolumeOwner::Project("project".into());
        let history = ResourceRevision::History(StreamRef::new(
            ProviderRef::new("example", "stream", "2")?,
            parent.stream_path()?.into_bytes(),
            Some("0".into()),
        )?);
        let project = ResourceRevision::Project {
            volume: VolumeRef::new(
                filesystem.clone(),
                "parent-project",
                VolumeClass::Project,
                owner.clone(),
            )?,
            generation: GenerationRef::new(filesystem.clone(), [1; 32], Some("1".into()))?,
        };
        let child_project = ResourceRevision::Project {
            volume: VolumeRef::new(
                filesystem.clone(),
                "child-project",
                VolumeClass::Project,
                owner,
            )?,
            generation: GenerationRef::new(filesystem.clone(), [2; 32], Some("1".into()))?,
        };
        let process = ResourceRevision::Process(checkpoint);
        let seed = ForkSeed {
            operation_id: OperationId::from_bytes([3; 16]),
            parent,
            parent_revision: 0,
            child,
            child_agent: AgentId::from_bytes([4; 16]),
            resources: vec![
                CapturedResource {
                    source: history.clone(),
                    revision: history,
                },
                CapturedResource {
                    source: project,
                    revision: child_project,
                },
                CapturedResource {
                    source: process.clone(),
                    revision: process,
                },
            ],
            omissions: Vec::new(),
            child_private_volume: VolumeRef::new(
                filesystem.clone(),
                "child-private",
                VolumeClass::AgentPrivate,
                VolumeOwner::Agent(AgentId::from_bytes([4; 16])),
            )?,
            child_private_generation: GenerationRef::new(filesystem, [3; 32], Some("1".into()))?,
            inherited_context: Vec::new(),
            shared_grants: Vec::new(),
            attached_agents: Vec::new(),
            reference_grants: Vec::new(),
            attachment_manifests: Vec::new(),
            inherited_through_sequence: 0,
            boundary: None,
        };
        host.verify(&seed).await?;
        let ResourceRevision::Project {
            volume: child_project,
            ..
        } = &seed.resources[1].revision
        else {
            return Err(Error::Invalid("test child project is missing".into()));
        };
        let request = ForkRequest {
            operation_id: seed.operation_id,
            parent: seed.parent.clone(),
            parent_revision: seed.parent_revision,
            child: seed.child.clone(),
            child_agent: seed.child_agent,
            attached_agents: Vec::new(),
            preparation: ForkPreparation {
                child_project_volume: child_project.clone(),
                child_private_volume: seed.child_private_volume.clone(),
                inherited_through_sequence: 0,
                maximum_inherited_messages: 1,
                maximum_inherited_bytes: 1_024,
                maximum_inherited_references: 1,
            },
            selections: seed
                .resources
                .iter()
                .map(|resource| ForkSelection {
                    required: true,
                    revision: resource.source.clone(),
                })
                .collect(),
            boundary: None,
        };
        request.validate()?;
        assert_eq!(
            host.capture(&request, &request.selections[2]).await?,
            Capture::Captured(seed.resources[2].clone()),
        );
        simulator
            .destroy_checkpoint(
                host.checkpoint_id(match &seed.resources[2].revision {
                    ResourceRevision::Process(reference) => reference,
                    _ => return Err(Error::Invalid("test checkpoint is missing".into())),
                })?,
                MachinesKey::new(),
            )
            .await
            .map_err(|error| map_error(error, None))?;
        assert!(matches!(host.verify(&seed).await, Err(Error::Conflict(_))));
        let mut missing = seed;
        let unavailable =
            ResourceRevision::Process(CheckpointRef::new(machines.clone(), [9; 16], None)?);
        missing.resources[2] = CapturedResource {
            source: unavailable.clone(),
            revision: unavailable,
        };
        assert!(matches!(
            host.verify(&missing).await,
            Err(Error::NotFound(_))
        ));
        let forged = ResourceRevision::Process(CheckpointRef::new(
            machines,
            [9; 16],
            Some("forged-generation".into()),
        )?);
        missing.resources[2] = CapturedResource {
            source: forged.clone(),
            revision: forged,
        };
        assert!(matches!(
            host.verify(&missing).await,
            Err(Error::Invalid(_))
        ));
        Ok(())
    }
}
