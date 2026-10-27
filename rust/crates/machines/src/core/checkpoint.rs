//! Immutable checkpoint custody, observation and fork sequencing share one owner.
use super::{
    ForkSlot, NativeBinding, OperationRegistry, RawMachines, lifecycle::require_capability, settle,
};
use crate::*;

impl OperationRegistry {
    /// The shared owner records an accepted snapshot before any readiness read.
    /// Native conflicts and lost replies are refusals, never proof of adoption.
    pub async fn create_checkpoint<D: RawMachines + ?Sized>(
        &self,
        driver: &D,
        machine: MachineId,
        key: IdempotencyKey,
        interval: Duration,
        timeout: Duration,
        at: u64,
    ) -> Result<SettledMutation, ProviderError> {
        self.apply_with(driver, key, &MutationIntent::Checkpoint(machine), async |operation| {
            let source = self.inspect_machine(driver, machine, at).await?;
            require_capability(&source, Capability::LiveCheckpoint)?;
            if source.state != MachineState::Running {
                return Err(ProviderError::Conflict(
                    "checkpoint requires a running machine; recovery: wake the source before taking a checkpoint".into(),
                ));
            }
            if interval.is_zero() || timeout.is_zero() {
                return Err(ProviderError::Invalid(
                    "checkpoint readiness admission failed; recovery: configure positive poll_interval and ready_timeout".into(),
                ));
            }
            let native = self.native_binding(machine, driver.name()).await?;
            self.bind_target(operation, native.as_ref().map_or_else(|| machine.to_string(), |value| value.handle.clone()).as_str()).await?;
            let resource = super::NativeResource::Checkpoint(driver.raw_checkpoint(&source, native.as_ref(), key).await?);
            self.claim(driver, operation, &resource, false).await?;
            let checkpoint = settle(key, interval, timeout, || async {
                driver.raw_observe_checkpoint(resource.handle(), source.id, &source.contract).await
            }).await?;
            if checkpoint.source != machine || checkpoint.contract != source.contract || !checkpoint.forkable {
                return Err(ProviderError::Rejected(
                    "native checkpoint does not match its admitted source; recovery: cancel this operation and repair the raw checkpoint mapping".into(),
                ));
            }
            self.wait_machine(driver, machine, key, interval, timeout).await?;
            self.claim_checkpoint(driver, operation, checkpoint.clone(), NativeBinding {
                provider: driver.name().into(),
                handle: resource.handle().into(),
                endpoint: None,
            }).await?;
            let mut current = self.machine(machine).await?.ok_or_else(|| ProviderError::NotFound(machine.to_string()))?;
            current.last_checkpoint = Some(checkpoint.id);
            self.observe_machine(current.clone(), current.changed_at_unix_ms).await?;
            Ok(MutationOutcome::Checkpointed(checkpoint))
        }).await
    }

    /// Only an admitted immutable checkpoint carries its original contract. Never
    /// reconstruct that contract from its source machine's current mutable state.
    pub async fn inspect_checkpoint<D: RawMachines + ?Sized>(
        &self,
        driver: &D,
        checkpoint: CheckpointId,
    ) -> Result<CheckpointObservation, ProviderError> {
        let native = self
            .native_checkpoint_binding(checkpoint, driver.name())
            .await?;
        let known = self
            .checkpoint(checkpoint)
            .await?
            .ok_or_else(|| ProviderError::NotFound(checkpoint.to_string()))?;
        if !known.forkable {
            return Ok(known);
        }
        let handle = native
            .as_ref()
            .map_or_else(|| checkpoint.to_string(), |native| native.handle.clone());
        let Some(observed) = driver
            .raw_observe_checkpoint(&handle, known.source, &known.contract)
            .await?
        else {
            return Ok(CheckpointObservation {
                forkable: false,
                ..known
            });
        };
        if observed.id != known.id
            || observed.source != known.source
            || observed.contract != known.contract
        {
            return Err(ProviderError::Rejected(
                "native checkpoint contradicts its canonical immutable identity or contract; recovery: repair the raw snapshot mapping before retrying".into(),
            ));
        }
        // Creation time and contract come from the admitted immutable generation;
        // a later native read cannot rewrite that history (including timestamp
        // fallbacks for providers which omit creation time).
        Ok(CheckpointObservation {
            forkable: observed.forkable,
            ..known
        })
    }

    pub async fn fork_checkpoint<D: RawMachines + ?Sized>(
        &self,
        driver: &D,
        checkpoint: CheckpointId,
        count: NonZeroU32,
        key: IdempotencyKey,
        interval: Duration,
        timeout: Duration,
    ) -> Result<SettledMutation, ProviderError> {
        let count = count.get();
        if count > MAX_FORK_CHILDREN {
            return Err(ProviderError::Invalid("fork count exceeds 1024".into()));
        }
        self.apply_with(driver, key, &MutationIntent::Fork(checkpoint, count), async |operation| {
            if interval.is_zero() || timeout.is_zero() {
                return Err(ProviderError::Invalid(
                    "fork readiness admission failed; recovery: configure positive poll_interval and ready_timeout".into(),
                ));
            }
            let source = self.inspect_checkpoint(driver, checkpoint).await?;
            if !source.forkable {
                return Err(ProviderError::Conflict("checkpoint no longer accepts forks; recovery: take a new admitted checkpoint".into()));
            }
            let native = self.native_checkpoint_binding(checkpoint, driver.name()).await?;
            let mut contract = source.contract;
            contract.image = Image::Checkpoint(checkpoint);
            let mut ids = Vec::with_capacity(count as usize);
            let mut seen = BTreeSet::new();
            for index in 0..count {
                let created = driver.raw_fork(checkpoint, native.as_ref(), &contract, ForkSlot { index, count }, key).await?.checked(|child| {
                    if child.contract != contract || child.last_checkpoint != Some(checkpoint) || child.id == source.source || !seen.insert(child.id) {
                        return Err(ProviderError::Rejected(
                            "native fork child contradicts admission; recovery: inspect this operation's cleanup before creating a new fork".into(),
                        ));
                    }
                    Ok(())
                });
                let child = self.accept_created(driver, operation, created, key).await?;
                ids.push(child.id);
            }
            let mut children = Vec::with_capacity(ids.len());
            for id in ids {
                let child = self.wait_machine(driver, id, key, interval, timeout).await?;
                if child.state != MachineState::Running || child.contract != contract || child.last_checkpoint != Some(checkpoint) {
                    return Err(ProviderError::Failed);
                }
                children.push(child);
            }
            Ok(MutationOutcome::Forked(children))
        }).await
    }
}
