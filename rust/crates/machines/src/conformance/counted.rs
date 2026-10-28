//! Transparent forwarding at the Machines provider boundary.
use crate::*;
use acyclic_stream::testing::{Counted, RequestClass};

#[async_trait::async_trait]
impl<P: MachinesProvider> MachinesProvider for Counted<P> {
    fn assurance(&self) -> ProviderAssurance {
        self.inner().assurance()
    }
    async fn read_file(
        &self,
        machine: MachineId,
        access: FileAccess,
        limit: u32,
    ) -> Result<FileRead, ProviderError> {
        self.record(RequestClass::Data);
        self.inner().read_file(machine, access, limit).await
    }
    async fn write_file(
        &self,
        machine: MachineId,
        access: FileAccess,
        data: Vec<u8>,
        truncate: bool,
    ) -> Result<FileWrite, ProviderError> {
        self.record(RequestClass::Data);
        self.inner()
            .write_file(machine, access, data, truncate)
            .await
    }
    async fn exec(
        &self,
        machine: MachineId,
        command: ExecCommand,
    ) -> Result<ExecStream, ProviderError> {
        self.record(RequestClass::Data);
        self.inner().exec(machine, command).await
    }
    async fn read_output(
        &self,
        machine: MachineId,
        offset: u64,
        limit: u32,
    ) -> Result<MachineOutput, ProviderError> {
        self.record(RequestClass::Data);
        self.inner().read_output(machine, offset, limit).await
    }
    async fn qualify_image(&self, image: Image) -> Result<ImageQualification, ProviderError> {
        self.record(RequestClass::Metadata);
        self.inner().qualify_image(image).await
    }
    async fn create(&self, request: CreateMachine) -> Result<MutationOutcome, ProviderError> {
        self.record(RequestClass::Metadata);
        self.inner().create(request).await
    }
    async fn inspect_machine(
        &self,
        machine: MachineId,
    ) -> Result<MachineObservation, ProviderError> {
        self.record(RequestClass::Metadata);
        self.inner().inspect_machine(machine).await
    }
    async fn list_machines(
        &self,
        after: Option<MachineId>,
        limit: u32,
    ) -> Result<MachinePage, ProviderError> {
        self.record(RequestClass::List);
        self.inner().list_machines(after, limit).await
    }
    async fn checkpoint(
        &self,
        machine: MachineId,
        key: IdempotencyKey,
    ) -> Result<MutationOutcome, ProviderError> {
        self.record(RequestClass::Metadata);
        self.inner().checkpoint(machine, key).await
    }
    async fn inspect_checkpoint(
        &self,
        checkpoint: CheckpointId,
    ) -> Result<CheckpointObservation, ProviderError> {
        self.record(RequestClass::Metadata);
        self.inner().inspect_checkpoint(checkpoint).await
    }
    async fn fork(
        &self,
        checkpoint: CheckpointId,
        count: NonZeroU32,
        key: IdempotencyKey,
    ) -> Result<MutationOutcome, ProviderError> {
        self.record(RequestClass::Metadata);
        self.inner().fork(checkpoint, count, key).await
    }
    async fn fork_machine(
        &self,
        machine: MachineId,
        count: NonZeroU32,
        key: IdempotencyKey,
    ) -> Result<MutationOutcome, ProviderError> {
        self.record(RequestClass::Metadata);
        self.inner().fork_machine(machine, count, key).await
    }
    async fn suspend(
        &self,
        machine: MachineId,
        key: IdempotencyKey,
    ) -> Result<MutationOutcome, ProviderError> {
        self.record(RequestClass::Metadata);
        self.inner().suspend(machine, key).await
    }
    async fn wake(
        &self,
        machine: MachineId,
        key: IdempotencyKey,
    ) -> Result<MutationOutcome, ProviderError> {
        self.record(RequestClass::Metadata);
        self.inner().wake(machine, key).await
    }
    async fn set_suspension_policy(
        &self,
        machine: MachineId,
        policy: SuspensionPolicy,
        key: IdempotencyKey,
    ) -> Result<MutationOutcome, ProviderError> {
        self.record(RequestClass::Metadata);
        self.inner()
            .set_suspension_policy(machine, policy, key)
            .await
    }
    async fn destroy_machine(
        &self,
        machine: MachineId,
        key: IdempotencyKey,
    ) -> Result<MutationOutcome, ProviderError> {
        self.record(RequestClass::Metadata);
        self.inner().destroy_machine(machine, key).await
    }
    async fn destroy_checkpoint(
        &self,
        checkpoint: CheckpointId,
        key: IdempotencyKey,
    ) -> Result<MutationOutcome, ProviderError> {
        self.record(RequestClass::Metadata);
        self.inner().destroy_checkpoint(checkpoint, key).await
    }
    async fn events(
        &self,
        machine: MachineId,
        after_sequence: Option<u64>,
        limit: u32,
    ) -> Result<EventPage, ProviderError> {
        self.record(RequestClass::Data);
        self.inner().events(machine, after_sequence, limit).await
    }
    async fn usage(
        &self,
        machine: MachineId,
        start_unix_ms: u64,
        end_unix_ms: u64,
    ) -> Result<UsageReceipt, ProviderError> {
        self.record(RequestClass::Metadata);
        self.inner()
            .usage(machine, start_unix_ms, end_unix_ms)
            .await
    }
    async fn recover(&self, key: IdempotencyKey) -> Result<MutationOutcome, ProviderError> {
        self.record(RequestClass::Metadata);
        self.inner().recover(key).await
    }
    async fn recover_operation(&self, key: IdempotencyKey) -> Result<OperationId, ProviderError> {
        self.record(RequestClass::Metadata);
        self.inner().recover_operation(key).await
    }
    async fn inspect_operation(
        &self,
        operation: OperationId,
    ) -> Result<OperationObservation, ProviderError> {
        self.record(RequestClass::Metadata);
        self.inner().inspect_operation(operation).await
    }
    async fn cancel(&self, operation: OperationId) -> Result<OperationObservation, ProviderError> {
        self.record(RequestClass::Metadata);
        self.inner().cancel(operation).await
    }
    async fn watch_operation(
        &self,
        operation: OperationId,
    ) -> Result<OperationStream, ProviderError> {
        self.record(RequestClass::Data);
        self.inner().watch_operation(operation).await
    }
}
