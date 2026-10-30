use super::*;

/// Concrete adapter over any customer-hosted or managed Machines provider.
#[derive(Clone)]
pub struct MachinesHost {
    pub(super) provider: Arc<dyn MachinesProvider>,
    pub(super) provider_ref: ProviderRef,
}

impl MachinesHost {
    /// Constructs the same task tool binding for local and hosted providers.
    /// The Machine and volume are the platform's original admitted selection.
    pub fn workspace(
        &self,
        admitted: MachineObservation,
        volume: &crate::conversation::VolumeRef,
        signed: crate::core::Scope,
        verifier: crate::core::AuthorityVerifier,
    ) -> Result<super::MachinesWorkspace> {
        let sandbox = SandboxRef::new(
            self.provider_ref.clone(),
            admitted.id.as_bytes().into(),
            None,
        )?;
        super::MachinesWorkspace::new(self.clone(), sandbox, admitted, volume, signed, verifier)
    }

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

    /// Creates one durable sandbox from the hosting preset's artifact, or the provider default.
    pub async fn create(
        &self,
        operation_id: OperationId,
        idempotency_key: &IdempotencyKey,
        artifact: Option<&ArtifactRef>,
        selected_volume: Option<&crate::conversation::VolumeRef>,
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
            artifact.map(|artifact| self.image(artifact)).transpose()?,
            network_policy_digest,
        );
        configure(&mut request);
        request.selected_volume_ref = selected_volume.map(Into::into);
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

    /// Enrolls the exact selected volume through the original Machine's hosted admission.
    pub async fn attach_volume(
        &self,
        operation_id: OperationId,
        idempotency_key: &IdempotencyKey,
        sandbox: &SandboxRef,
        volume: &crate::conversation::VolumeRef,
    ) -> Result<MachineObservation> {
        let machine = self.machine_id(sandbox)?;
        match self.provider.attach_volume(machine, volume.into(), machines_key(idempotency_key)?)
            .await.map_err(|error| map_error(error, Some(operation_id)))?
        {
            MutationOutcome::VolumeAttached(observation) if observation.id == machine => Ok(observation),
            _ => Err(Error::Storage("Machines substituted the volume attachment result; recovery: recover the original operation".into())),
        }
    }

    /// Reads the provider's durable sandbox observation.
    pub async fn inspect(&self, sandbox: &SandboxRef) -> Result<MachineObservation> {
        self.provider
            .inspect_machine(self.machine_id(sandbox)?)
            .await
            .map_err(|error| map_error(error, None))
    }

    /// Dispatches one command in the admitted sandbox. Consume the returned
    /// stream for output and its terminal exit; dropping it cancels the RPC.
    /// An interrupted command is never submitted again by this adapter.
    pub async fn exec(
        &self,
        sandbox: &SandboxRef,
        command: acyclic_machines::ExecCommand,
    ) -> Result<acyclic_machines::ExecStream> {
        self.provider
            .exec(self.machine_id(sandbox)?, command)
            .await
            .map_err(|error| map_error(error, None))
    }

    /// Reads one native guest-file chunk under the original absolute deadline.
    pub async fn read_file(
        &self,
        sandbox: &SandboxRef,
        access: acyclic_machines::FileAccess,
        limit: u32,
    ) -> Result<acyclic_machines::FileRead> {
        self.provider
            .read_file(self.machine_id(sandbox)?, access, limit)
            .await
            .map_err(|error| map_error(error, None))
    }

    /// Writes one native guest-file chunk without shell evaluation or retries.
    pub async fn write_file(
        &self,
        sandbox: &SandboxRef,
        access: acyclic_machines::FileAccess,
        bytes: Vec<u8>,
        truncate: bool,
    ) -> Result<acyclic_machines::FileWrite> {
        self.provider
            .write_file(self.machine_id(sandbox)?, access, bytes, truncate)
            .await
            .map_err(|error| map_error(error, None))
    }

    /// Suspends the original machine under the caller's stable mutation key.
    pub async fn suspend(
        &self,
        operation_id: OperationId,
        idempotency_key: &IdempotencyKey,
        sandbox: &SandboxRef,
    ) -> Result<()> {
        let machine = self.machine_id(sandbox)?;
        match self.provider.suspend(machine, machines_key(idempotency_key)?).await
            .map_err(|error| map_error(error, Some(operation_id)))?
        {
            MutationOutcome::Suspended(actual) if actual == machine => Ok(()),
            _ => Err(Error::Storage("Machines substituted the suspension result; recovery: inspect the original operation".into())),
        }
    }

    /// Wakes the original machine under the caller's stable mutation key.
    /// Exec on an idle-suspended provisioned Machine wakes it implicitly.
    pub async fn wake(
        &self,
        operation_id: OperationId,
        idempotency_key: &IdempotencyKey,
        sandbox: &SandboxRef,
    ) -> Result<()> {
        let machine = self.machine_id(sandbox)?;
        match self
            .provider
            .wake(machine, machines_key(idempotency_key)?)
            .await
            .map_err(|error| map_error(error, Some(operation_id)))?
        {
            MutationOutcome::Woken(actual) if actual == machine => Ok(()),
            _ => Err(Error::Storage(
                "Machines substituted the wake result; recovery: inspect the original operation"
                    .into(),
            )),
        }
    }

    /// Forks a live machine and returns the provider's fidelity and children.
    pub async fn fork_machine(
        &self,
        operation_id: OperationId,
        idempotency_key: &IdempotencyKey,
        sandbox: &SandboxRef,
        count: NonZeroU32,
    ) -> Result<(ForkFidelity, Vec<SandboxRef>)> {
        let machine = self.machine_id(sandbox)?;
        match self
            .provider
            .fork_machine(machine, count, machines_key(idempotency_key)?)
            .await
            .map_err(|error| map_error(error, Some(operation_id)))?
        {
            MutationOutcome::MachineForked {
                source,
                fidelity,
                children,
            } if source == machine && children.len() == count.get() as usize => Ok((
                fidelity,
                children
                    .into_iter()
                    .map(|child| {
                        SandboxRef::new(self.provider_ref.clone(), child.id.as_bytes().into(), None)
                    })
                    .collect::<Result<_>>()?,
            )),
            _ => Err(Error::Storage(
                "Machines substituted the live fork result; recovery: recover the original operation".into(),
            )),
        }
    }

    /// Deletes the original machine and retains uncertainty on a lost reply.
    pub async fn destroy(
        &self,
        operation_id: OperationId,
        idempotency_key: &IdempotencyKey,
        sandbox: &SandboxRef,
    ) -> Result<()> {
        let machine = self.machine_id(sandbox)?;
        match self.provider.destroy_machine(machine, machines_key(idempotency_key)?).await
            .map_err(|error| map_error(error, Some(operation_id)))?
        {
            MutationOutcome::MachineDestroyed(actual) if actual == machine => Ok(()),
            _ => Err(Error::Storage("Machines substituted the deletion result; recovery: inspect the original operation".into())),
        }
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
    pub(super) fn machine_id(&self, reference: &SandboxRef) -> Result<MachineId> {
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
