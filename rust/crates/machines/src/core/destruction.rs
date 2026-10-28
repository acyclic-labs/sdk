//! Destruction shares admission, native targeting and durable state publication.
use super::{NativeResource, OperationRegistry, RawMachines};
use crate::*;

impl OperationRegistry {
    pub async fn destroy_machine<D: RawMachines + ?Sized>(
        &self,
        driver: &D,
        machine: MachineId,
        key: IdempotencyKey,
        at: u64,
    ) -> Result<SettledMutation, ProviderError> {
        self.apply_with(driver, key, &MutationIntent::DestroyMachine(machine), async |operation| {
            let current = self.inspect_machine(driver, machine, at).await?;
            if current.state == MachineState::Destroyed {
                return Ok(MutationOutcome::MachineDestroyed(machine));
            }
            let native = self.native_binding(machine, driver.name()).await?;
            if current.contract.fork_fidelity() == Some(ForkFidelity::MemoryAndDisk) {
                let live = driver.raw_fork_children(machine, native.as_ref()).await?
                    .into_iter().filter(|state| *state != MachineState::Destroyed).count();
                if live != 0 {
                    return Err(ProviderError::Conflict(format!(
                        "machine has {live} live fork children; recovery: destroy its children before their source"
                    )));
                }
            }
            let handle = native.map_or_else(|| machine.to_string(), |native| native.handle);
            let deleted_at = self.delete_target(driver, operation, NativeResource::Machine(handle)).await?;
            self.mark_destroyed(machine, deleted_at.unwrap_or(at).max(current.changed_at_unix_ms)).await?;
            Ok(MutationOutcome::MachineDestroyed(machine))
        }).await
    }

    pub async fn destroy_checkpoint<D: RawMachines + ?Sized>(
        &self,
        driver: &D,
        checkpoint: CheckpointId,
        key: IdempotencyKey,
    ) -> Result<SettledMutation, ProviderError> {
        self.apply_with(
            driver,
            key,
            &MutationIntent::DestroyCheckpoint(checkpoint),
            async |operation| {
                // Retain the canonical immutable observation after native deletion.
                self.inspect_checkpoint(driver, checkpoint).await?;
                let native = self
                    .native_checkpoint_binding(checkpoint, driver.name())
                    .await?;
                let handle = native.map_or_else(|| checkpoint.to_string(), |native| native.handle);
                self.delete_target(driver, operation, NativeResource::Checkpoint(handle))
                    .await?;
                self.mark_checkpoint_destroyed(checkpoint).await?;
                Ok(MutationOutcome::CheckpointDestroyed(checkpoint))
            },
        )
        .await
    }

    async fn delete_target<D: RawMachines + ?Sized>(
        &self,
        driver: &D,
        operation: OperationId,
        resource: NativeResource,
    ) -> Result<Option<u64>, ProviderError> {
        self.bind_target(operation, &resource).await?;
        match driver.raw_destroy(&resource).await {
            Ok(at) => Ok(Some(at)),
            Err(ProviderError::NotFound(_)) => Ok(None),
            Err(error) => Err(error),
        }
    }
}
