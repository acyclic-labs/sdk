use super::*;

#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
enum MemoryIntent {
    Create(CreateMachine),
    Attach(Platform),
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

/// Deterministic process-local state-machine provider.
#[derive(Clone)]
pub struct SimulatedMachines {
    state: Arc<Mutex<MemoryState>>,
    capabilities: BTreeSet<Capability>,
}

/// Platform of every simulated provisioned Machine.
pub const SIMULATED_PLATFORM: Platform = Platform {
    os: OperatingSystem::Linux,
    arch: Architecture::X64,
    device_kind: DeviceKind::VirtualMachine,
    shell: Shell::Bash,
    path_style: PathStyle::Posix,
};

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

    /// Enrolls a simulated attached device, as its owner's runner would. Create
    /// never yields one; replaying key returns the original observation.
    pub async fn attach(
        &self,
        key: IdempotencyKey,
        platform: Platform,
    ) -> Result<MachineObservation, ProviderError> {
        let outcome = self
            .apply(key, MemoryIntent::Attach(platform), move |state| {
                let id = Self::machine(key, 0);
                let now = tick(state)?;
                let observation = MachineObservation {
                    id,
                    state: MachineState::Running,
                    contract: MachineContract {
                        image: Image::Custom(ImageDigest::new(Self::revision())?),
                        selected_volume_ref: None,
                        capabilities: BTreeSet::new(),
                        compatibility: CompatibilityPolicy::BestEffort,
                        compatibility_revision: Self::revision(),
                        suspension: SuspensionPolicy::Manual,
                        expiration: ExpirationPolicy::Never,
                        network_policy_digest: Self::revision(),
                        budgets: Budgets::default(),
                        platform,
                        lifecycle: LifecycleClass::Attached,
                    },
                    endpoints: vec![Endpoint {
                        name: "default".into(),
                        uri: format!("memory://{id}"),
                    }],
                    last_checkpoint: None,
                    created_at_unix_ms: now,
                    changed_at_unix_ms: now,
                };
                state.machines.insert(id, observation.clone());
                event(state, id, EventFact::State(MachineState::Running), now)?;
                Ok(MutationOutcome::Created(observation))
            })
            .await?;
        match outcome {
            MutationOutcome::Created(observation) => Ok(observation),
            _ => Err(ProviderError::Conflict(
                "attach key names another intent".into(),
            )),
        }
    }

    /// Simulates a device or guest connection opening or closing: Running
    /// becomes Offline, and Offline becomes Running again.
    pub async fn set_online(&self, machine: MachineId, online: bool) -> Result<(), ProviderError> {
        let (from, to) = if online {
            (MachineState::Offline, MachineState::Running)
        } else {
            (MachineState::Running, MachineState::Offline)
        };
        let mut state = self.state.lock().await;
        change_state(&mut state, machine, from, to)
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
    fn contract(
        request: &CreateMachine,
        image: Image,
        capabilities: BTreeSet<Capability>,
        platform: Platform,
    ) -> MachineContract {
        MachineContract {
            image,
            selected_volume_ref: None,
            capabilities,
            compatibility: request.compatibility.clone(),
            compatibility_revision: Self::revision(),
            suspension: request.suspension,
            expiration: request.expiration,
            network_policy_digest: request.network_policy_digest,
            budgets: request.budgets,
            platform,
            lifecycle: LifecycleClass::Provisioned,
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
    let value = state
        .machines
        .get(&machine)
        .ok_or_else(|| ProviderError::NotFound(machine.to_string()))?;
    let current = value.state;
    // Only the device can bring an Offline Machine back.
    if current == MachineState::Offline {
        return Err(ProviderError::Unavailable);
    }
    value.contract.provisioned()?;
    if current == target {
        return Ok(outcome);
    }
    change_state(state, machine, required, target).map(|()| outcome)
}

fn change_state(
    state: &mut MemoryState,
    machine: MachineId,
    required: MachineState,
    target: MachineState,
) -> Result<(), ProviderError> {
    let current = state
        .machines
        .get(&machine)
        .ok_or_else(|| ProviderError::NotFound(machine.to_string()))?
        .state;
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
    event(state, machine, EventFact::State(target), now)
}

mod backend;
#[cfg(test)]
mod tests;
