#![deny(unsafe_code)]
//! Explicit integration between provider-neutral harness references and Machines.

use acyclic_harness::{
    Error, IdempotencyKey, OperationId, Result,
    resources::{ArtifactRef, CheckpointRef, ProviderRef, SandboxRef},
};
use acyclic_machines::{
    CheckpointId, CheckpointObservation, CreateMachine, IdempotencyKey as MachinesKey, Image,
    ImageQualification, MachineId, MachineObservation, MachinesProvider, MutationOutcome,
    OperationId as MachinesOperationId, OperationObservation, ProviderError,
};
use std::{num::NonZeroU32, sync::Arc};

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
        let digest: [u8; 32] =
            reference.as_resource().key().try_into().map_err(|_| {
                Error::Invalid("machine image artifact key must be 32 bytes".into())
            })?;
        Image::custom(digest).map_err(|error| Error::Invalid(error.to_string()))
    }

    fn machine_id(&self, reference: &SandboxRef) -> Result<MachineId> {
        reference.validate()?;
        self.validate_provider(reference.as_resource().provider())?;
        parse_uuid(reference.as_resource().key(), MachineId::parse)
    }

    fn checkpoint_id(&self, reference: &CheckpointRef) -> Result<CheckpointId> {
        reference.validate()?;
        self.validate_provider(reference.as_resource().provider())?;
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
        ProviderError::Failed => Error::Storage("Machines operation failed".into()),
        ProviderError::Cancelled => Error::Storage("Machines operation was cancelled".into()),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use acyclic_machines::SimulatedMachines;

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
}
