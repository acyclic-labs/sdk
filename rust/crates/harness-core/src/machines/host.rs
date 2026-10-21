use super::*;

/// Concrete adapter over any customer-hosted or managed Machines provider.
#[derive(Clone)]
pub struct MachinesHost {
    pub(super) provider: Arc<dyn MachinesProvider>,
    pub(super) provider_ref: ProviderRef,
}

impl MachinesHost {
    /// Binds one explicit provider implementation and immutable provider identity.
    pub fn new(provider: Arc<dyn MachinesProvider>, provider_ref: ProviderRef) -> Result<Self> {
        provider_ref.validate(&mut None)?;
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
            MutationOutcome::Created(observation) => SandboxRef::new(
                self.provider_ref.clone(),
                observation.id.as_bytes().into(),
                None,
            ),
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
            MutationOutcome::Checkpointed(observation) => CheckpointRef::new(
                self.provider_ref.clone(),
                observation.id.as_bytes().into(),
                None,
            ),
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
    ) -> Result<Vec<SandboxRef>> {
        match self
            .provider
            .fork(
                self.checkpoint_id(checkpoint)?,
                count,
                machines_key(idempotency_key)?,
            )
            .await
            .map_err(|error| map_error(error, Some(operation_id)))?
        {
            MutationOutcome::Forked(values) => values
                .into_iter()
                .map(|value| {
                    SandboxRef::new(self.provider_ref.clone(), value.id.as_bytes().into(), None)
                })
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
        let digest = self
            .key(reference.as_resource())?
            .try_into()
            .map_err(|_| Error::Invalid("machine image artifact key must be 32 bytes".into()))?;
        Image::custom(digest).map_err(|error| Error::Invalid(error.to_string()))
    }
    fn machine_id(&self, reference: &SandboxRef) -> Result<MachineId> {
        parse_uuid(self.key(reference.as_resource())?, MachineId::parse)
    }
    pub(super) fn checkpoint_id(&self, reference: &CheckpointRef) -> Result<CheckpointId> {
        parse_uuid(self.key(reference.as_resource())?, CheckpointId::parse)
    }
    fn key<'a>(&self, reference: &'a crate::resources::ResourceRef) -> Result<&'a [u8]> {
        reference.validate(&mut None)?;
        if reference.provider() != &self.provider_ref || reference.version().is_some() {
            return Err(Error::Invalid(
                "Machines reference has a foreign provider or version".into(),
            ));
        }
        Ok(reference.key())
    }
}
