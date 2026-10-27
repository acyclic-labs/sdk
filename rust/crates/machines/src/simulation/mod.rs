use super::*;

use crate::core::{NativeBinding, NativeResource, NativeResources, OperationRegistry};

struct SimulatedMachine {
    observation: MachineObservation,
    fork_parent: Option<MachineId>,
}

struct MemoryState {
    now: u64,
    machines: BTreeMap<MachineId, SimulatedMachine>,
    checkpoints: BTreeMap<CheckpointId, CheckpointObservation>,
}

impl Default for MemoryState {
    fn default() -> Self {
        Self {
            now: 1,
            machines: BTreeMap::new(),
            checkpoints: BTreeMap::new(),
        }
    }
}

/// Deterministic bounded process-local state-machine provider.
#[derive(Clone)]
pub struct SimulatedMachines {
    state: Arc<Mutex<MemoryState>>,
    ops: OperationRegistry,
    capabilities: BTreeSet<Capability>,
}

impl Default for SimulatedMachines {
    /// Declares every capability.
    fn default() -> Self {
        Self::with_capabilities(BTreeSet::from([
            Capability::ElasticCpu,
            Capability::ElasticMemory,
            Capability::LiveCheckpoint,
            Capability::LiveFork,
            Capability::SuspendResume,
            Capability::LiveMovement,
            Capability::DiskFork,
        ]))
    }
}

impl SimulatedMachines {
    /// Simulates a provider that qualifies images with exactly `capabilities`, for example without [`Capability::LiveFork`] to exercise a caller's fallback path.
    pub fn with_capabilities(capabilities: BTreeSet<Capability>) -> Self {
        Self {
            state: Arc::default(),
            ops: OperationRegistry::simulated(),
            capabilities,
        }
    }
    fn revision() -> [u8; 32] {
        Sha256::digest(b"acyclic-machines-memory-v1").into()
    }
    fn machine(key: IdempotencyKey, index: u32) -> MachineId {
        MachineId::from_digest(derived_digest(b"machine", key, index))
    }
    fn checkpoint_id(key: IdempotencyKey) -> CheckpointId {
        CheckpointId::from_digest(derived_digest(b"checkpoint", key, 0))
    }
    async fn apply<F>(
        &self,
        key: IdempotencyKey,
        intent: MutationIntent,
        action: F,
    ) -> Result<MutationOutcome, ProviderError>
    where
        F: FnOnce(&mut MemoryState) -> Result<MutationOutcome, ProviderError>,
    {
        self.ops
            .apply_with(self, key, &intent, async |operation| {
                let mut state = self.state.lock().await;
                let outcome = action(&mut state)?;
                match &outcome {
                    MutationOutcome::Created(value) => self.claim(operation, value).await?,
                    MutationOutcome::Forked(children)
                    | MutationOutcome::MachineForked { children, .. } => {
                        for child in children {
                            self.claim(operation, child).await?;
                        }
                    }
                    MutationOutcome::Checkpointed(value) => {
                        self.ops
                            .bind_target(operation, &value.source.to_string())
                            .await?;
                        let handle = value.id.to_string();
                        self.ops
                            .claim(
                                self,
                                operation,
                                &NativeResource::Checkpoint(handle.clone()),
                                false,
                            )
                            .await?;
                        self.ops
                            .claim_checkpoint(
                                self,
                                operation,
                                value.clone(),
                                NativeBinding {
                                    provider: self.name().into(),
                                    handle,
                                    endpoint: None,
                                },
                            )
                            .await?;
                        let source = state
                            .machines
                            .get(&value.source)
                            .ok_or_else(|| ProviderError::NotFound(value.source.to_string()))?;
                        self.ops
                            .observe_machine(
                                source.observation.clone(),
                                source.observation.changed_at_unix_ms,
                            )
                            .await?;
                    }
                    MutationOutcome::CheckpointDestroyed(checkpoint) => {
                        self.ops
                            .bind_target(operation, &checkpoint.to_string())
                            .await?;
                        self.ops.mark_checkpoint_destroyed(*checkpoint).await?;
                    }
                    MutationOutcome::Suspended(machine)
                    | MutationOutcome::Woken(machine)
                    | MutationOutcome::SuspensionPolicySet(machine, _)
                    | MutationOutcome::MachineDestroyed(machine) => {
                        self.ops
                            .bind_target(operation, &machine.to_string())
                            .await?;
                        let value = state
                            .machines
                            .get(machine)
                            .ok_or_else(|| ProviderError::NotFound(machine.to_string()))?;
                        self.ops
                            .observe_machine(
                                value.observation.clone(),
                                value.observation.changed_at_unix_ms,
                            )
                            .await?;
                    }
                }
                Ok(outcome)
            })
            .await
            .map(|settled| settled.outcome)
    }

    async fn claim(
        &self,
        operation: OperationId,
        value: &MachineObservation,
    ) -> Result<(), ProviderError> {
        self.ops
            .claim_machine(
                self,
                operation,
                value.clone(),
                NativeBinding {
                    provider: self.name().into(),
                    handle: value.id.to_string(),
                    endpoint: None,
                },
                false,
            )
            .await
    }
}

fn derived_digest(domain: &[u8], key: IdempotencyKey, index: u32) -> [u8; 32] {
    let mut hash = Sha256::new();
    hash.update(domain);
    hash.update(key.as_bytes());
    hash.update(index.to_be_bytes());
    hash.finalize().into()
}

fn tick(state: &mut MemoryState) -> Result<u64, ProviderError> {
    state.now = state
        .now
        .checked_add(1)
        .ok_or_else(|| ProviderError::Rejected("simulation clock exhausted".into()))?;
    Ok(state.now)
}

fn transition(
    state: &mut MemoryState,
    machine: MachineId,
    required: MachineState,
    target: MachineState,
    outcome: MutationOutcome,
) -> Result<MutationOutcome, ProviderError> {
    let current = state
        .machines
        .get(&machine)
        .ok_or_else(|| ProviderError::NotFound(machine.to_string()))?
        .observation
        .state;
    if current == target {
        return Ok(outcome);
    }
    if current != required {
        return Err(ProviderError::Conflict(
            "machine cannot perform that transition".into(),
        ));
    }
    let now = tick(state)?;
    let value = state
        .machines
        .get_mut(&machine)
        .ok_or_else(|| ProviderError::NotFound(machine.to_string()))?;
    value.observation.state = target;
    value.observation.changed_at_unix_ms = now;
    Ok(outcome)
}

mod backend;
#[cfg(test)]
mod tests;

#[cfg_attr(target_arch = "wasm32", async_trait(?Send))]
#[cfg_attr(not(target_arch = "wasm32"), async_trait)]
impl NativeResources for SimulatedMachines {
    fn name(&self) -> &str {
        "simulation"
    }

    async fn raw_destroy(&self, resource: &NativeResource) -> Result<u64, ProviderError> {
        // A native-world mutation may still be publishing its accepted result.
        // Refuse while busy; the one canonical cleanup owner retains the debt.
        let mut state = self.state.try_lock().map_err(|_| ProviderError::Conflict(
            "simulated native world is busy; recovery: call cancel again with the original operation identity after its active mutation returns".into(),
        ))?;
        match resource {
            NativeResource::Machine(handle) => {
                let machine = MachineId::parse(handle).map_err(|_| ProviderError::Rejected("simulated native machine address is invalid; recovery: inspect the original canonical cleanup record".into()))?;
                let current = state
                    .machines
                    .get(&machine)
                    .ok_or_else(|| ProviderError::NotFound(handle.clone()))?;
                if current.observation.state != MachineState::Destroyed {
                    let now = tick(&mut state)?;
                    let current = state
                        .machines
                        .get_mut(&machine)
                        .ok_or_else(|| ProviderError::NotFound(handle.clone()))?;
                    current.observation.state = MachineState::Destroyed;
                    current.observation.changed_at_unix_ms = now;
                }
            }
            NativeResource::Checkpoint(handle) => {
                let checkpoint = CheckpointId::parse(handle).map_err(|_| ProviderError::Rejected("simulated native checkpoint address is invalid; recovery: inspect the original canonical cleanup record".into()))?;
                let current = state
                    .checkpoints
                    .get_mut(&checkpoint)
                    .ok_or_else(|| ProviderError::NotFound(handle.clone()))?;
                current.forkable = false;
            }
        }
        Ok(state.now)
    }
}
