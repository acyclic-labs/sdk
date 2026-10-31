use super::*;

#[cfg_attr(target_arch = "wasm32", async_trait(?Send))]
#[cfg_attr(not(target_arch = "wasm32"), async_trait)]
impl MachinesProvider for SimulatedMachines {
    fn assurance(&self) -> ProviderAssurance {
        ProviderAssurance::ProcessLocalSimulation
    }
    async fn read_file(
        &self,
        _machine: MachineId,
        _access: FileAccess,
        _limit: u32,
    ) -> Result<FileRead, ProviderError> {
        acyclic_observability::async_operation!(
            "acyclic.machines.read_file",
            ProviderError::kind,
            acyclic_native_runtime::obs::OperationSpan::new,
            async {
                Err(ProviderError::Unsupported(
                    "the simulator has no guest filesystem".into(),
                ))
            }
        )
    }
    async fn write_file(
        &self,
        _machine: MachineId,
        _access: FileAccess,
        _data: Vec<u8>,
        _truncate: bool,
    ) -> Result<FileWrite, ProviderError> {
        acyclic_observability::async_operation!(
            "acyclic.machines.write_file",
            ProviderError::kind,
            acyclic_native_runtime::obs::OperationSpan::new,
            async {
                Err(ProviderError::Unsupported(
                    "the simulator has no guest filesystem".into(),
                ))
            }
        )
    }
    /// Admits like a physical provider, including the implicit wake of an
    /// idle-suspended Machine, then refuses: no command result is fabricated.
    async fn exec(
        &self,
        machine: MachineId,
        _command: ExecCommand,
    ) -> Result<ExecStream, ProviderError> {
        acyclic_observability::async_operation!(
            "acyclic.machines.exec",
            ProviderError::kind,
            acyclic_native_runtime::obs::OperationSpan::new,
            async {
                let mut state = self.state.lock().await;
                let value = state
                    .machines
                    .get(&machine)
                    .ok_or_else(|| ProviderError::NotFound(machine.to_string()))?;
                match (value.state, value.contract.wakes_on_demand()) {
                    (MachineState::Running, _) => {}
                    (MachineState::Suspended, true) => change_state(
                        &mut state,
                        machine,
                        MachineState::Suspended,
                        MachineState::Running,
                    )?,
                    (MachineState::Offline, _) => return Err(ProviderError::Unavailable),
                    _ => {
                        return Err(ProviderError::Conflict(
                            "machine is not running; recovery: wake it before executing".into(),
                        ));
                    }
                }
                Err(ProviderError::Unsupported(
                    "the simulator does not execute a guest operating system".into(),
                ))
            }
        )
    }
    async fn read_output(
        &self,
        _machine: MachineId,
        _offset: u64,
        _limit: u32,
    ) -> Result<MachineOutput, ProviderError> {
        acyclic_observability::async_operation!(
            "acyclic.machines.read_output",
            ProviderError::kind,
            acyclic_native_runtime::obs::OperationSpan::new,
            async {
                Err(ProviderError::Unsupported(
                    "the simulator has no guest console".into(),
                ))
            }
        )
    }
    async fn qualify_image(&self, image: Image) -> Result<ImageQualification, ProviderError> {
        acyclic_observability::async_operation!(
            "acyclic.machines.qualify_image",
            ProviderError::kind,
            acyclic_native_runtime::obs::OperationSpan::new,
            async {
                Ok(ImageQualification {
                    image,
                    capabilities: self.capabilities.clone(),
                    compatibility_revision: Self::revision(),
                })
            }
        )
    }
    async fn attach_volume(
        &self,
        _machine: MachineId,
        _volume: wire::selection::VolumeRef,
        _key: IdempotencyKey,
    ) -> Result<MutationOutcome, ProviderError> {
        acyclic_observability::async_operation!(
            "acyclic.machines.attach_volume",
            ProviderError::kind,
            acyclic_native_runtime::obs::OperationSpan::new,
            async {
                Err(ProviderError::Unsupported(
            "the simulator has no hosted guest admission; recovery: use a physical Machines provider".into(),
        ))
            }
        )
    }
    async fn create(&self, request: CreateMachine) -> Result<MutationOutcome, ProviderError> {
        acyclic_observability::async_operation!(
            "acyclic.machines.create",
            ProviderError::kind,
            acyclic_native_runtime::obs::OperationSpan::new,
            async {
                if request.selected_volume_ref.is_some() {
                    return Err(ProviderError::Unsupported(
                "the simulator has no hosted guest admission; recovery: use a physical Machines provider".into(),
            ));
                }
                let key = request.idempotency_key;
                let intent = MemoryIntent::Create(request.clone());
                let capabilities = self.capabilities.clone();
                let platform = SIMULATED_PLATFORM;
                self.apply(key, intent, move |state| {
            let image = request.image.clone().ok_or_else(|| ProviderError::Unsupported(
                "the simulator has no installed default image; recovery: supply an explicit immutable image".into(),
            ))?;
            if let CompatibilityPolicy::Require(required) = &request.compatibility
                && (required.is_empty() || !required.is_subset(&capabilities))
            {
                return Err(ProviderError::Unsupported(
                    "required image capabilities are unavailable".into(),
                ));
            }
            if state.machines.len() >= 1_024 {
                return Err(ProviderError::Rejected(
                    "simulation machine limit reached".into(),
                ));
            }
            let id = Self::machine(key, 0);
            let now = tick(state)?;
            let contract = Self::contract(&request, image, capabilities, platform);
            let observation = MachineObservation {
                id,
                state: MachineState::Running,
                contract,
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
        .await
            }
        )
    }
    async fn inspect_machine(
        &self,
        machine: MachineId,
    ) -> Result<MachineObservation, ProviderError> {
        acyclic_observability::async_operation!(
            "acyclic.machines.inspect_machine",
            ProviderError::kind,
            acyclic_native_runtime::obs::OperationSpan::new,
            async {
                self.state
                    .lock()
                    .await
                    .machines
                    .get(&machine)
                    .cloned()
                    .ok_or_else(|| ProviderError::NotFound(machine.to_string()))
            }
        )
    }
    async fn list_machines(
        &self,
        after: Option<MachineId>,
        limit: u32,
    ) -> Result<MachinePage, ProviderError> {
        acyclic_observability::async_operation!(
            "acyclic.machines.list_machines",
            ProviderError::kind,
            acyclic_native_runtime::obs::OperationSpan::new,
            async {
                if limit == 0 || limit > MAX_PAGE_SIZE {
                    return Err(ProviderError::Invalid(
                        "machine page limit must be 1..=256".into(),
                    ));
                }
                let state = self.state.lock().await;
                let mut values = state
                    .machines
                    .values()
                    .filter(|value| after.is_none_or(|cursor| value.id > cursor))
                    .take(
                        usize::try_from(limit)
                            .map_err(|_| ProviderError::Invalid("invalid page limit".into()))?
                            + 1,
                    )
                    .cloned()
                    .collect::<Vec<_>>();
                let next = if values.len()
                    > usize::try_from(limit)
                        .map_err(|_| ProviderError::Invalid("invalid page limit".into()))?
                {
                    values.pop();
                    values.last().map(|value| value.id)
                } else {
                    None
                };
                Ok(MachinePage {
                    machines: values,
                    next,
                })
            }
        )
    }
    async fn checkpoint(
        &self,
        machine: MachineId,
        key: IdempotencyKey,
    ) -> Result<MutationOutcome, ProviderError> {
        acyclic_observability::async_operation!(
            "acyclic.machines.checkpoint",
            ProviderError::kind,
            acyclic_native_runtime::obs::OperationSpan::new,
            async {
                self.apply(key, MemoryIntent::Checkpoint(machine), move |state| {
                    let source = state
                        .machines
                        .get(&machine)
                        .cloned()
                        .ok_or_else(|| ProviderError::NotFound(machine.to_string()))?;
                    source.contract.provisioned()?;
                    if !matches!(
                        source.state,
                        MachineState::Running | MachineState::Suspended
                    ) {
                        return Err(ProviderError::Conflict(
                            "machine cannot be checkpointed in its current state".into(),
                        ));
                    }
                    let id = Self::checkpoint_id(key);
                    let now = tick(state)?;
                    let value = CheckpointObservation {
                        id,
                        source: machine,
                        contract: source.contract,
                        forkable: true,
                        created_at_unix_ms: now,
                    };
                    state.checkpoints.insert(id, value.clone());
                    if let Some(machine_value) = state.machines.get_mut(&machine) {
                        machine_value.last_checkpoint = Some(id);
                        machine_value.changed_at_unix_ms = now;
                    }
                    Ok(MutationOutcome::Checkpointed(value))
                })
                .await
            }
        )
    }
    async fn inspect_checkpoint(
        &self,
        checkpoint: CheckpointId,
    ) -> Result<CheckpointObservation, ProviderError> {
        acyclic_observability::async_operation!(
            "acyclic.machines.inspect_checkpoint",
            ProviderError::kind,
            acyclic_native_runtime::obs::OperationSpan::new,
            async {
                self.state
                    .lock()
                    .await
                    .checkpoints
                    .get(&checkpoint)
                    .cloned()
                    .ok_or_else(|| ProviderError::NotFound(checkpoint.to_string()))
            }
        )
    }
    async fn fork(
        &self,
        checkpoint: CheckpointId,
        count: NonZeroU32,
        key: IdempotencyKey,
    ) -> Result<MutationOutcome, ProviderError> {
        acyclic_observability::async_operation!(
            "acyclic.machines.fork",
            ProviderError::kind,
            acyclic_native_runtime::obs::OperationSpan::new,
            async {
                let count_value = count.get();
                if count_value > MAX_FORK_CHILDREN {
                    return Err(ProviderError::Invalid("fork count exceeds 1024".into()));
                }
                self.apply(
                    key,
                    MemoryIntent::Fork(checkpoint, count_value),
                    move |state| {
                        let source = state
                            .checkpoints
                            .get(&checkpoint)
                            .cloned()
                            .ok_or_else(|| ProviderError::NotFound(checkpoint.to_string()))?;
                        if !source.forkable {
                            return Err(ProviderError::Conflict(
                                "checkpoint no longer accepts forks".into(),
                            ));
                        }
                        let add = usize::try_from(count_value)
                            .map_err(|_| ProviderError::Invalid("invalid fork count".into()))?;
                        if state.machines.len().saturating_add(add) > 1_024 {
                            return Err(ProviderError::Rejected(
                                "simulation machine limit reached".into(),
                            ));
                        }
                        let mut children = Vec::with_capacity(add);
                        for index in 0..count_value {
                            let id = Self::machine(key, index);
                            let now = tick(state)?;
                            let mut contract = source.contract.clone();
                            contract.image = Image::Checkpoint(checkpoint);
                            let value = MachineObservation {
                                id,
                                state: MachineState::Running,
                                contract,
                                endpoints: vec![Endpoint {
                                    name: "default".into(),
                                    uri: format!("memory://{id}"),
                                }],
                                last_checkpoint: Some(checkpoint),
                                created_at_unix_ms: now,
                                changed_at_unix_ms: now,
                            };
                            state.machines.insert(id, value.clone());
                            event(state, id, EventFact::State(MachineState::Running), now)?;
                            children.push(value);
                        }
                        Ok(MutationOutcome::Forked(children))
                    },
                )
                .await
            }
        )
    }
    async fn fork_machine(
        &self,
        machine: MachineId,
        count: NonZeroU32,
        key: IdempotencyKey,
    ) -> Result<MutationOutcome, ProviderError> {
        acyclic_observability::async_operation!(
            "acyclic.machines.fork_machine",
            ProviderError::kind,
            acyclic_native_runtime::obs::OperationSpan::new,
            async {
                let count_value = count.get();
                if count_value > MAX_FORK_CHILDREN {
                    return Err(ProviderError::Invalid("fork count exceeds 1024".into()));
                }
                self.apply(
                    key,
                    MemoryIntent::ForkMachine(machine, count_value),
                    move |state| {
                        let source = state
                            .machines
                            .get(&machine)
                            .cloned()
                            .ok_or_else(|| ProviderError::NotFound(machine.to_string()))?;
                        let fidelity = source.contract.fork_fidelity().ok_or_else(|| {
                            ProviderError::Unsupported(
                                "machine contract does not declare live fork".into(),
                            )
                        })?;
                        // A suspended Machine is a checkpoint: its forks start from
                        // it while it stays suspended.
                        if !matches!(
                            source.state,
                            MachineState::Running | MachineState::Suspended
                        ) {
                            return Err(ProviderError::Conflict(
                                "only a running or suspended machine can be forked".into(),
                            ));
                        }
                        let add = usize::try_from(count_value)
                            .map_err(|_| ProviderError::Invalid("invalid fork count".into()))?;
                        if state.machines.len().saturating_add(add) > 1_024 {
                            return Err(ProviderError::Rejected(
                                "simulation machine limit reached".into(),
                            ));
                        }
                        let mut children = Vec::with_capacity(add);
                        for index in 0..count_value {
                            let id = Self::machine(key, index);
                            let now = tick(state)?;
                            let value = MachineObservation {
                                id,
                                state: MachineState::Running,
                                contract: source.contract.clone(),
                                endpoints: vec![Endpoint {
                                    name: "default".into(),
                                    uri: format!("memory://{id}"),
                                }],
                                last_checkpoint: None,
                                created_at_unix_ms: now,
                                changed_at_unix_ms: now,
                            };
                            state.machines.insert(id, value.clone());
                            event(state, id, EventFact::State(MachineState::Running), now)?;
                            children.push(value);
                        }
                        Ok(MutationOutcome::MachineForked {
                            source: machine,
                            fidelity,
                            children,
                        })
                    },
                )
                .await
            }
        )
    }
    async fn suspend(
        &self,
        machine: MachineId,
        key: IdempotencyKey,
    ) -> Result<MutationOutcome, ProviderError> {
        acyclic_observability::async_operation!(
            "acyclic.machines.suspend",
            ProviderError::kind,
            acyclic_native_runtime::obs::OperationSpan::new,
            async {
                self.apply(key, MemoryIntent::Suspend(machine), move |state| {
                    transition(
                        state,
                        machine,
                        MachineState::Running,
                        MachineState::Suspended,
                        MutationOutcome::Suspended(machine),
                    )
                })
                .await
            }
        )
    }
    async fn wake(
        &self,
        machine: MachineId,
        key: IdempotencyKey,
    ) -> Result<MutationOutcome, ProviderError> {
        acyclic_observability::async_operation!(
            "acyclic.machines.wake",
            ProviderError::kind,
            acyclic_native_runtime::obs::OperationSpan::new,
            async {
                self.apply(key, MemoryIntent::Wake(machine), move |state| {
                    transition(
                        state,
                        machine,
                        MachineState::Suspended,
                        MachineState::Running,
                        MutationOutcome::Woken(machine),
                    )
                })
                .await
            }
        )
    }
    async fn set_suspension_policy(
        &self,
        machine: MachineId,
        policy: SuspensionPolicy,
        key: IdempotencyKey,
    ) -> Result<MutationOutcome, ProviderError> {
        acyclic_observability::async_operation!(
            "acyclic.machines.set_suspension_policy",
            ProviderError::kind,
            acyclic_native_runtime::obs::OperationSpan::new,
            async {
                self.apply(key, MemoryIntent::Policy(machine, policy), move |state| {
                    let value = state
                        .machines
                        .get(&machine)
                        .ok_or_else(|| ProviderError::NotFound(machine.to_string()))?;
                    value.contract.provisioned()?;
                    if value.state == MachineState::Destroyed {
                        return Err(ProviderError::Conflict(
                            "destroyed machine cannot change policy".into(),
                        ));
                    }
                    let now = tick(state)?;
                    let value = state
                        .machines
                        .get_mut(&machine)
                        .ok_or_else(|| ProviderError::NotFound(machine.to_string()))?;
                    value.contract.suspension = policy;
                    value.changed_at_unix_ms = now;
                    Ok(MutationOutcome::SuspensionPolicySet(machine, policy))
                })
                .await
            }
        )
    }
    async fn destroy_machine(
        &self,
        machine: MachineId,
        key: IdempotencyKey,
    ) -> Result<MutationOutcome, ProviderError> {
        acyclic_observability::async_operation!(
            "acyclic.machines.destroy_machine",
            ProviderError::kind,
            acyclic_native_runtime::obs::OperationSpan::new,
            async {
                self.apply(key, MemoryIntent::DestroyMachine(machine), move |state| {
            let current = state
                .machines
                .get(&machine)
                .ok_or_else(|| ProviderError::NotFound(machine.to_string()))?
                .state;
            if current == MachineState::Destroyed {
                return Ok(MutationOutcome::MachineDestroyed(machine));
            }
            let live_children = state.replays.values().any(|replay| {
                matches!(&replay.outcome, MutationOutcome::MachineForked { source, children, .. }
                    if *source == machine && children.iter().any(|child| state.machines.get(&child.id)
                        .is_some_and(|value| value.state != MachineState::Destroyed)))
            });
            if live_children {
                return Err(ProviderError::FailedPrecondition(Precondition::LiveForks));
            }
            let now = tick(state)?;
            let value = state
                .machines
                .get_mut(&machine)
                .ok_or_else(|| ProviderError::NotFound(machine.to_string()))?;
            value.state = MachineState::Destroyed;
            value.changed_at_unix_ms = now;
            event(
                state,
                machine,
                EventFact::State(MachineState::Destroyed),
                now,
            )?;
            Ok(MutationOutcome::MachineDestroyed(machine))
        })
        .await
            }
        )
    }
    async fn destroy_checkpoint(
        &self,
        checkpoint: CheckpointId,
        key: IdempotencyKey,
    ) -> Result<MutationOutcome, ProviderError> {
        acyclic_observability::async_operation!(
            "acyclic.machines.destroy_checkpoint",
            ProviderError::kind,
            acyclic_native_runtime::obs::OperationSpan::new,
            async {
                self.apply(
                    key,
                    MemoryIntent::DestroyCheckpoint(checkpoint),
                    move |state| {
                        let value = state
                            .checkpoints
                            .get_mut(&checkpoint)
                            .ok_or_else(|| ProviderError::NotFound(checkpoint.to_string()))?;
                        value.forkable = false;
                        Ok(MutationOutcome::CheckpointDestroyed(checkpoint))
                    },
                )
                .await
            }
        )
    }
    async fn events(
        &self,
        machine: MachineId,
        after_sequence: Option<u64>,
        limit: u32,
    ) -> Result<EventPage, ProviderError> {
        acyclic_observability::async_operation!(
            "acyclic.machines.events",
            ProviderError::kind,
            acyclic_native_runtime::obs::OperationSpan::new,
            async {
                if limit == 0 || limit > MAX_EVENT_PAGE_SIZE {
                    return Err(ProviderError::Invalid(
                        "event page limit must be 1..=1024".into(),
                    ));
                }
                let state = self.state.lock().await;
                if !state.machines.contains_key(&machine) {
                    return Err(ProviderError::NotFound(machine.to_string()));
                }
                let limit = usize::try_from(limit)
                    .map_err(|_| ProviderError::Invalid("invalid event limit".into()))?;
                let mut values = state
                    .events
                    .get(&machine)
                    .into_iter()
                    .flatten()
                    .filter(|value| after_sequence.is_none_or(|cursor| value.sequence > cursor))
                    .take(limit + 1)
                    .cloned()
                    .collect::<Vec<_>>();
                let next_sequence = if values.len() > limit {
                    values.pop();
                    values.last().map(|value| value.sequence)
                } else {
                    None
                };
                Ok(EventPage {
                    events: values,
                    next_sequence,
                })
            }
        )
    }
    async fn usage(
        &self,
        machine: MachineId,
        start_unix_ms: u64,
        end_unix_ms: u64,
    ) -> Result<UsageReceipt, ProviderError> {
        acyclic_observability::async_operation!(
            "acyclic.machines.usage",
            ProviderError::kind,
            acyclic_native_runtime::obs::OperationSpan::new,
            async {
                if start_unix_ms >= end_unix_ms {
                    return Err(ProviderError::Invalid(
                        "usage interval must be non-empty".into(),
                    ));
                }
                if !self.state.lock().await.machines.contains_key(&machine) {
                    return Err(ProviderError::NotFound(machine.to_string()));
                }
                Ok(UsageReceipt {
                    machine,
                    start_unix_ms,
                    end_unix_ms,
                    cpu_ns: 0,
                    resident_byte_seconds: 0,
                    durable_private_bytes: 0,
                    lineage_receipt_sha256: [0; 32],
                    egress_bytes: 0,
                    receipt: Vec::new(),
                })
            }
        )
    }
    async fn recover(&self, key: IdempotencyKey) -> Result<MutationOutcome, ProviderError> {
        acyclic_observability::async_operation!(
            "acyclic.machines.recover",
            ProviderError::kind,
            acyclic_native_runtime::obs::OperationSpan::new,
            async {
                self.state
                    .lock()
                    .await
                    .replays
                    .get(&key)
                    .map(|value| value.outcome.clone())
                    .ok_or_else(|| ProviderError::NotFound(key.to_string()))
            }
        )
    }
    async fn recover_operation(&self, key: IdempotencyKey) -> Result<OperationId, ProviderError> {
        acyclic_observability::async_operation!(
            "acyclic.machines.recover_operation",
            ProviderError::kind,
            acyclic_native_runtime::obs::OperationSpan::new,
            async {
                if self.state.lock().await.replays.contains_key(&key) {
                    Ok(Self::operation(key))
                } else {
                    Err(ProviderError::NotFound(key.to_string()))
                }
            }
        )
    }
    async fn inspect_operation(
        &self,
        operation: OperationId,
    ) -> Result<OperationObservation, ProviderError> {
        acyclic_observability::async_operation!(
            "acyclic.machines.inspect_operation",
            ProviderError::kind,
            acyclic_native_runtime::obs::OperationSpan::new,
            async {
                if self
                    .state
                    .lock()
                    .await
                    .replays
                    .keys()
                    .any(|key| Self::operation(*key) == operation)
                {
                    Ok(OperationObservation {
                        id: operation,
                        phase: OperationPhase::Succeeded,
                    })
                } else {
                    Err(ProviderError::NotFound(operation.to_string()))
                }
            }
        )
    }
    async fn cancel(&self, operation: OperationId) -> Result<OperationObservation, ProviderError> {
        acyclic_observability::async_operation!(
            "acyclic.machines.cancel",
            ProviderError::kind,
            acyclic_native_runtime::obs::OperationSpan::new,
            async { self.inspect_operation(operation).await }
        )
    }
    async fn watch_operation(
        &self,
        operation: OperationId,
    ) -> Result<OperationStream, ProviderError> {
        acyclic_observability::async_operation!(
            "acyclic.machines.watch_operation",
            ProviderError::kind,
            acyclic_native_runtime::obs::OperationSpan::new,
            async {
                let observation = self.inspect_operation(operation).await?;
                Ok(stream::once(async move { Ok(observation) }).boxed())
            }
        )
    }
}
