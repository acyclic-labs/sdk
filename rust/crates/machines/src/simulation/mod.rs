use super::*;

#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
enum MemoryIntent {
    Create(CreateMachine),
    Checkpoint(MachineId),
    Fork(CheckpointId, u32),
    ForkMachine(MachineId, u32),
    Suspend(MachineId),
    Wake(MachineId),
    Policy(MachineId, SuspensionPolicy),
    DestroyMachine(MachineId),
    DestroyCheckpoint(CheckpointId),
}

#[derive(Clone)]
struct Replay {
    digest: [u8; 32],
    outcome: MutationOutcome,
}

struct MemoryState {
    now: u64,
    machines: BTreeMap<MachineId, MachineObservation>,
    checkpoints: BTreeMap<CheckpointId, CheckpointObservation>,
    events: BTreeMap<MachineId, Vec<MachineEvent>>,
    replays: BTreeMap<IdempotencyKey, Replay>,
}

impl Default for MemoryState {
    fn default() -> Self {
        Self {
            now: 1,
            machines: BTreeMap::new(),
            checkpoints: BTreeMap::new(),
            events: BTreeMap::new(),
            replays: BTreeMap::new(),
        }
    }
}

/// Deterministic bounded process-local state-machine provider.
#[derive(Clone)]
pub struct SimulatedMachines {
    state: Arc<Mutex<MemoryState>>,
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
            capabilities,
        }
    }
    fn revision() -> [u8; 32] {
        Sha256::digest(b"acyclic-machines-memory-v1").into()
    }
    fn operation(key: IdempotencyKey) -> OperationId {
        OperationId::from_digest(derived_digest(b"operation", key, 0))
    }
    fn machine(key: IdempotencyKey, index: u32) -> MachineId {
        MachineId::from_digest(derived_digest(b"machine", key, index))
    }
    fn checkpoint_id(key: IdempotencyKey) -> CheckpointId {
        CheckpointId::from_digest(derived_digest(b"checkpoint", key, 0))
    }
    fn contract(request: &CreateMachine, capabilities: BTreeSet<Capability>) -> MachineContract {
        MachineContract {
            image: request.image.clone(),
            capabilities,
            compatibility: request.compatibility.clone(),
            compatibility_revision: Self::revision(),
            suspension: request.suspension,
            expiration: request.expiration,
            network_policy_digest: request.network_policy_digest,
            budgets: request.budgets,
        }
    }

    async fn apply<F>(
        &self,
        key: IdempotencyKey,
        intent: MemoryIntent,
        action: F,
    ) -> Result<MutationOutcome, ProviderError>
    where
        F: FnOnce(&mut MemoryState) -> Result<MutationOutcome, ProviderError>,
    {
        let digest = intent_digest(&intent)?;
        let mut state = self.state.lock().await;
        if let Some(replay) = state.replays.get(&key) {
            return if replay.digest == digest {
                Ok(replay.outcome.clone())
            } else {
                Err(ProviderError::Conflict(
                    "idempotency key is bound to another intent".into(),
                ))
            };
        }
        if state.replays.len() >= 4_096 {
            return Err(ProviderError::Rejected(
                "simulation operation limit reached".into(),
            ));
        }
        let outcome = action(&mut state)?;
        state.replays.insert(
            key,
            Replay {
                digest,
                outcome: outcome.clone(),
            },
        );
        Ok(outcome)
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

fn event(
    state: &mut MemoryState,
    machine: MachineId,
    fact: EventFact,
    observed_at: u64,
) -> Result<(), ProviderError> {
    let values = state.events.entry(machine).or_default();
    if values.len() >= 4_096 {
        return Err(ProviderError::Rejected(
            "simulation event limit reached".into(),
        ));
    }
    let sequence = u64::try_from(values.len())
        .map_err(|_| ProviderError::Rejected("simulation event limit reached".into()))?
        + 1;
    values.push(MachineEvent {
        machine,
        sequence,
        observed_at_unix_ms: observed_at,
        fact,
    });
    Ok(())
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
    value.state = target;
    value.changed_at_unix_ms = now;
    event(state, machine, EventFact::State(target), now)?;
    Ok(outcome)
}

mod backend;
#[cfg(test)]
mod tests;
