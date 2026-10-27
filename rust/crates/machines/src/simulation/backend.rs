use super::*;

#[cfg_attr(target_arch = "wasm32", async_trait(?Send))]
#[cfg_attr(not(target_arch = "wasm32"), async_trait)]
impl MachinesProvider for SimulatedMachines {
    fn assurance(&self) -> ProviderAssurance {
        ProviderAssurance::ProcessLocalSimulation
    }
    async fn exec(
        &self,
        _machine: MachineId,
        _command: ExecCommand,
    ) -> Result<ExecStream, ProviderError> {
        Err(ProviderError::Unsupported(
            "the simulator does not execute a guest operating system".into(),
        ))
    }
    async fn read_output(
        &self,
        _machine: MachineId,
        _offset: u64,
        _limit: u32,
    ) -> Result<MachineOutput, ProviderError> {
        Err(ProviderError::Unsupported(
            "the simulator has no guest console".into(),
        ))
    }
    async fn qualify_image(&self, image: Image) -> Result<ImageQualification, ProviderError> {
        Ok(ImageQualification {
            image,
            capabilities: self.capabilities.clone(),
            compatibility_revision: Self::revision(),
        })
    }
    async fn create(&self, request: CreateMachine) -> Result<MutationOutcome, ProviderError> {
        let key = request.idempotency_key;
        let intent = MutationIntent::Create(request.clone());
        let capabilities = self.capabilities.clone();
        self.apply(key, intent, move |state| {
            if let CompatibilityPolicy::Require(required) = &request.compatibility
                && (required.is_empty() || !required.is_subset(&capabilities))
            {
                return Err(ProviderError::Unsupported(
                    "required image capabilities are unavailable".into(),
                ));
            }
            self.ops.check_machine_capacity(state.machines.len(), 1)?;
            let id = Self::machine(key, 0);
            let now = tick(state)?;
            let contract = Self::contract(&request, capabilities);
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
            state.machines.insert(
                id,
                SimulatedMachine {
                    observation: observation.clone(),
                    fork_parent: None,
                },
            );
            Ok(MutationOutcome::Created(observation))
        })
        .await
    }
    async fn inspect_machine(
        &self,
        machine: MachineId,
    ) -> Result<MachineObservation, ProviderError> {
        self.ops
            .machine(machine)
            .await?
            .ok_or_else(|| ProviderError::NotFound(machine.to_string()))
    }
    async fn list_machines(
        &self,
        after: Option<MachineId>,
        limit: u32,
    ) -> Result<MachinePage, ProviderError> {
        self.ops.machine_page(after, limit).await
    }

    async fn checkpoint(
        &self,
        machine: MachineId,
        key: IdempotencyKey,
    ) -> Result<MutationOutcome, ProviderError> {
        self.apply(key, MutationIntent::Checkpoint(machine), move |state| {
            let source = state
                .machines
                .get(&machine)
                .map(|machine| machine.observation.clone())
                .ok_or_else(|| ProviderError::NotFound(machine.to_string()))?;
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
                machine_value.observation.last_checkpoint = Some(id);
                machine_value.observation.changed_at_unix_ms = now;
            }
            Ok(MutationOutcome::Checkpointed(value))
        })
        .await
    }
    async fn inspect_checkpoint(
        &self,
        checkpoint: CheckpointId,
    ) -> Result<CheckpointObservation, ProviderError> {
        self.ops
            .checkpoint(checkpoint)
            .await?
            .ok_or_else(|| ProviderError::NotFound(checkpoint.to_string()))
    }

    async fn fork(
        &self,
        checkpoint: CheckpointId,
        count: NonZeroU32,
        key: IdempotencyKey,
    ) -> Result<MutationOutcome, ProviderError> {
        let count_value = count.get();
        if count_value > MAX_FORK_CHILDREN {
            return Err(ProviderError::Invalid("fork count exceeds 1024".into()));
        }
        self.apply(
            key,
            MutationIntent::Fork(checkpoint, count_value),
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
                self.ops.check_machine_capacity(state.machines.len(), add)?;
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
                    state.machines.insert(
                        id,
                        SimulatedMachine {
                            observation: value.clone(),
                            fork_parent: None,
                        },
                    );
                    children.push(value);
                }
                Ok(MutationOutcome::Forked(children))
            },
        )
        .await
    }
    async fn fork_machine(
        &self,
        machine: MachineId,
        count: NonZeroU32,
        key: IdempotencyKey,
    ) -> Result<MutationOutcome, ProviderError> {
        let count_value = count.get();
        if count_value > MAX_FORK_CHILDREN {
            return Err(ProviderError::Invalid("fork count exceeds 1024".into()));
        }
        self.apply(
            key,
            MutationIntent::ForkMachine(machine, count_value),
            move |state| {
                let source = state
                    .machines
                    .get(&machine)
                    .map(|machine| machine.observation.clone())
                    .ok_or_else(|| ProviderError::NotFound(machine.to_string()))?;
                let fidelity = source.contract.fork_fidelity().ok_or_else(|| {
                    ProviderError::Unsupported("machine contract does not declare live fork".into())
                })?;
                if source.state != MachineState::Running {
                    return Err(ProviderError::Conflict(
                        "only a running machine can be forked".into(),
                    ));
                }
                let add = usize::try_from(count_value)
                    .map_err(|_| ProviderError::Invalid("invalid fork count".into()))?;
                self.ops.check_machine_capacity(state.machines.len(), add)?;
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
                    state.machines.insert(
                        id,
                        SimulatedMachine {
                            observation: value.clone(),
                            fork_parent: Some(machine),
                        },
                    );
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
    async fn suspend(
        &self,
        machine: MachineId,
        key: IdempotencyKey,
    ) -> Result<MutationOutcome, ProviderError> {
        self.apply(key, MutationIntent::Suspend(machine), move |state| {
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
    async fn wake(
        &self,
        machine: MachineId,
        key: IdempotencyKey,
    ) -> Result<MutationOutcome, ProviderError> {
        self.apply(key, MutationIntent::Wake(machine), move |state| {
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
    async fn set_suspension_policy(
        &self,
        machine: MachineId,
        policy: SuspensionPolicy,
        key: IdempotencyKey,
    ) -> Result<MutationOutcome, ProviderError> {
        self.apply(key, MutationIntent::Policy(machine, policy), move |state| {
            let value = state
                .machines
                .get(&machine)
                .ok_or_else(|| ProviderError::NotFound(machine.to_string()))?;
            if value.observation.state == MachineState::Destroyed {
                return Err(ProviderError::Conflict(
                    "destroyed machine cannot change policy".into(),
                ));
            }
            let now = tick(state)?;
            let value = state
                .machines
                .get_mut(&machine)
                .ok_or_else(|| ProviderError::NotFound(machine.to_string()))?;
            value.observation.contract.suspension = policy;
            value.observation.changed_at_unix_ms = now;
            Ok(MutationOutcome::SuspensionPolicySet(machine, policy))
        })
        .await
    }
    async fn destroy_machine(
        &self,
        machine: MachineId,
        key: IdempotencyKey,
    ) -> Result<MutationOutcome, ProviderError> {
        self.apply(key, MutationIntent::DestroyMachine(machine), move |state| {
            let current = state
                .machines
                .get(&machine)
                .ok_or_else(|| ProviderError::NotFound(machine.to_string()))?
                .observation
                .state;
            if current == MachineState::Destroyed {
                return Ok(MutationOutcome::MachineDestroyed(machine));
            }
            let live_children = state.machines.values().any(|child| {
                child.fork_parent == Some(machine)
                    && child.observation.state != MachineState::Destroyed
            });
            if live_children {
                return Err(ProviderError::Conflict(
                    "machine has live-fork children; destroy them first".into(),
                ));
            }
            let now = tick(state)?;
            let value = state
                .machines
                .get_mut(&machine)
                .ok_or_else(|| ProviderError::NotFound(machine.to_string()))?;
            value.observation.state = MachineState::Destroyed;
            value.observation.changed_at_unix_ms = now;
            Ok(MutationOutcome::MachineDestroyed(machine))
        })
        .await
    }
    async fn destroy_checkpoint(
        &self,
        checkpoint: CheckpointId,
        key: IdempotencyKey,
    ) -> Result<MutationOutcome, ProviderError> {
        self.apply(
            key,
            MutationIntent::DestroyCheckpoint(checkpoint),
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
    async fn events(
        &self,
        machine: MachineId,
        after_sequence: Option<u64>,
        limit: u32,
    ) -> Result<EventPage, ProviderError> {
        self.ops
            .projected_event_page(machine, after_sequence, limit)
            .await
    }

    async fn usage(
        &self,
        machine: MachineId,
        start_unix_ms: u64,
        end_unix_ms: u64,
    ) -> Result<UsageReceipt, ProviderError> {
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
            elastic_cpu_ns: 0,
            dedicated_cpu_ns: 0,
            private_resident_byte_seconds: 0,
            durable_private_bytes: 0,
            lineage_receipt_sha256: [0; 32],
            egress_bytes: 0,
            receipt: Vec::new(),
        })
    }
    async fn recover(&self, key: IdempotencyKey) -> Result<MutationOutcome, ProviderError> {
        Ok(self.ops.settled(key).await?.outcome)
    }
    async fn recover_operation(&self, key: IdempotencyKey) -> Result<OperationId, ProviderError> {
        self.ops
            .record_for_key(key)
            .await?
            .map(|record| record.id)
            .ok_or_else(|| ProviderError::NotFound(key.to_string()))
    }
    async fn inspect_operation(
        &self,
        operation: OperationId,
    ) -> Result<OperationObservation, ProviderError> {
        self.ops
            .inspect(operation)
            .await?
            .ok_or_else(|| ProviderError::NotFound(operation.to_string()))
    }
    async fn cancel(&self, operation: OperationId) -> Result<OperationObservation, ProviderError> {
        self.ops.cancel_with(self, operation).await
    }
    async fn watch_operation(
        &self,
        operation: OperationId,
    ) -> Result<OperationStream, ProviderError> {
        self.ops.watch(operation).await
    }
}
