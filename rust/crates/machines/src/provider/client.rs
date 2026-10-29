use super::*;

/// Customer client bound to one provider.
#[derive(Clone)]
pub struct Machines {
    provider: Arc<dyn MachinesProvider>,
}
impl Machines {
    /// Binds one provider implementation.
    pub fn new(provider: Arc<dyn MachinesProvider>) -> Self {
        Self { provider }
    }
    /// Returns the provider's assurance boundary.
    pub fn assurance(&self) -> ProviderAssurance {
        self.provider.assurance()
    }

    pub async fn attach(&self, id: MachineId) -> Result<Machine, ProviderError> {
        self.provider.inspect_machine(id).await?;
        Ok(Machine::new(self.clone(), id))
    }
    pub async fn create(&self, request: CreateMachine) -> Result<Machine, ProviderError> {
        match self.provider.create(request).await? {
            MutationOutcome::Created(value) => Ok(Machine::new(self.clone(), value.id)),
            _ => Err(ProviderError::Rejected(
                "provider returned the wrong create outcome".into(),
            )),
        }
    }
}

/// Stable machine handle bound to its original provider.
#[derive(Clone)]
pub struct Machine {
    machines: Machines,
    id: MachineId,
}
impl Machine {
    /// Global identity of this handle in the caller's account.
    pub fn identity(
        &self,
        account: &[u8],
    ) -> Result<crate::addressing::ResourceIdentity, crate::addressing::IdentityError> {
        self.id.identity(account)
    }

    pub(crate) fn new(machines: Machines, id: MachineId) -> Self {
        Self { machines, id }
    }
    /// Returns the machine identity.
    pub fn id(&self) -> MachineId {
        self.id
    }

    pub async fn exec(&self, command: ExecCommand) -> Result<ExecStream, ProviderError> {
        self.machines.provider.exec(self.id, command).await
    }
    /// Enrolls a selected volume and settles its original operation watch.
    pub async fn attach_volume(
        &self,
        volume: wire::selection::VolumeRef,
        key: IdempotencyKey,
    ) -> Result<MutationOutcome, ProviderError> {
        self.machines
            .provider
            .attach_volume(self.id, volume, key)
            .await
    }
    /// Reads one native guest-file chunk under its original deadline.
    pub async fn read_file(
        &self,
        access: FileAccess,
        limit: u32,
    ) -> Result<FileRead, ProviderError> {
        self.machines
            .provider
            .read_file(self.id, access, limit)
            .await
    }
    /// Writes one native chunk; interruption never authorizes redispatch.
    pub async fn write_file(
        &self,
        access: FileAccess,
        data: Vec<u8>,
        truncate: bool,
    ) -> Result<FileWrite, ProviderError> {
        self.machines
            .provider
            .write_file(self.id, access, data, truncate)
            .await
    }
    pub async fn read_output(
        &self,
        offset: u64,
        limit: u32,
    ) -> Result<MachineOutput, ProviderError> {
        if !(1..=MAX_OUTPUT_READ_BYTES).contains(&limit) {
            return Err(ProviderError::Invalid("invalid console read limit".into()));
        }
        self.machines
            .provider
            .read_output(self.id, offset, limit)
            .await
    }
}
