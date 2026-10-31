use super::*;

macro_rules! machine_mutations {
    ($(($method:ident, $outcome:ident, $span:literal)),+; $($other:tt)*) => {
        #[cfg_attr(target_arch = "wasm32", async_trait(?Send))]
        #[cfg_attr(not(target_arch = "wasm32"), async_trait)]
        impl<E: RpcEndpoint> MachinesProvider for Provider<E> {
        $(
        async fn $method(&self, machine: MachineId, key: IdempotencyKey)
            -> Result<MutationOutcome, ProviderError> {
            obs::operation!($span, observation, async {
                let value = self.client().await?.$method(wire::MachineMutationRequest {
                    protocol: Some(protocol()),
                    idempotency_key: Some(encode_key(key)),
                    machine: Some(encode_machine(machine)),
                }).await.map_err(|error| { obs::failed_rpc(&observation, &error); mutation_error(key, &error) })?.into_inner();
                if decode_machine(value.machine.as_ref()).map_err(|_| ProviderError::Indeterminate(key))? != machine {
                    return Err(ProviderError::Indeterminate(key));
                }
                let operation = decode_operation(value.operation.as_ref()).map_err(|_| ProviderError::Indeterminate(key))?;
                settled_change(wait(self, key, operation).await?, MutationOutcome::$outcome(machine), key)
                    .map_err(|_| ProviderError::Indeterminate(key))
            })
        }
        )+
        $($other)*
        }
    };
}

machine_mutations! {
    (suspend, Suspended, "acyclic.machines.suspend"),
    (wake, Woken, "acyclic.machines.wake"),
    (destroy_machine, MachineDestroyed, "acyclic.machines.destroy_machine");
    fn assurance(&self) -> ProviderAssurance {
        ProviderAssurance::CustomerHosted
    }

    async fn read_file(
        &self,
        machine: MachineId,
        access: FileAccess,
        limit: u32,
    ) -> Result<FileRead, ProviderError> {
        obs::operation!("acyclic.machines.read_file", observation, async {
            self.guest_read_file(machine, access, limit, &observation)
                .await
        })
    }

    async fn write_file(
        &self,
        machine: MachineId,
        access: FileAccess,
        data: Vec<u8>,
        truncate: bool,
    ) -> Result<FileWrite, ProviderError> {
        obs::operation!("acyclic.machines.write_file", observation, async {
            self.guest_write_file(machine, access, data, truncate, &observation)
                .await
        })
    }

    async fn exec(
        &self,
        machine: MachineId,
        command: ExecCommand,
    ) -> Result<ExecStream, ProviderError> {
        obs::operation!("acyclic.machines.exec", observation, async {
            // Execution timeout belongs to the remote command. Only the caller's
            // separate wall expiry may stop receipt of its physical terminal.
            let deadline = command.expiry_unix_ms
                .map(|expiry| self.endpoint.absolute_deadline(expiry)).transpose()?;
            let mut client = transport::run(deadline.as_ref(), self.client()).await
                    .ok_or_else(|| ProviderError::Rejected("exec deadline expired during credential admission; recovery: obtain fresh authority before dispatch".into()))??;
            let (input, input_guard) = duplex::input(stream::iter(input::encode(machine, command)));
            let mut request = Request::new(input);
            if let Some(deadline) = &deadline {
                request.set_timeout(deadline.remaining());
            }
            let output = transport::run(deadline.as_ref(), client.exec(request))
                .await
                .ok_or(ProviderError::CommandIndeterminate(machine))?
                .map_err(|error| {
                    obs::failed_rpc(&observation, &error);
                    command_error(machine, &error)
                })?
                .into_inner();
            let output = stream::try_unfold(
                (output, false, deadline, input_guard),
                move |(mut output, mut exited, deadline, input_guard)| async move {
                    if exited {
                        return Ok(None);
                    }
                    let frame = transport::run(deadline.as_ref(), output.message())
                        .await
                        .ok_or(ProviderError::CommandIndeterminate(machine))?
                        .map_err(|error| command_stream_error(machine, exited, &error))?;
                    match frame {
                        None => Err(ProviderError::CommandIndeterminate(machine)),
                        Some(frame) => {
                            let frame = decode_exec_output(frame, machine, &mut exited)?;
                            Ok(Some((frame, (output, exited, deadline, input_guard))))
                        }
                    }
                },
            );
            Ok(Box::pin(output) as ExecStream)
        })
    }

    async fn read_output(
        &self,
        machine: MachineId,
        offset: u64,
        limit: u32,
    ) -> Result<MachineOutput, ProviderError> {
        obs::operation!("acyclic.machines.read_output", observation, async {
            if !(1..=MAX_OUTPUT_READ_BYTES).contains(&limit) {
                return Err(ProviderError::Invalid("invalid console read limit".into()));
            }
            let value = self
                .client()
                .await?
                .read_output(wire::ReadOutputRequest {
                    protocol: Some(protocol()),
                    machine: Some(encode_machine(machine)),
                    offset,
                    limit,
                })
                .await
                .map_err(|error| {
                    obs::failed_rpc(&observation, &error);
                    read_error(&error)
                })?
                .into_inner();
            decode_machine_output(value, machine, offset, limit)
        })
    }

    async fn qualify_image(&self, image: Image) -> Result<ImageQualification, ProviderError> {
        self.qualification(Some(image)).await
    }

    async fn create(&self, request: CreateMachine) -> Result<MutationOutcome, ProviderError> {
        obs::operation!("acyclic.machines.create", observation, async {
            let key = request.idempotency_key;
            let response = self
                .client()
                .await?
                .create(wire::CreateMachineRequest {
                    protocol: Some(protocol()),
                    idempotency_key: Some(encode_key(key)),
                    image: request.image.as_ref().map(encode_image).transpose()?,
                    selected_volume_ref: request.selected_volume_ref.clone(),
                    compatibility: Some(encode_compatibility(&request.compatibility)?),
                    suspension: Some(encode_suspension(request.suspension)?),
                    expiration: Some(encode_expiration(request.expiration)?),
                    network_policy_digest: request.network_policy_digest.to_vec(),
                    budgets: Some(wire::Budgets {
                        spend_micros: request.budgets.spend_micros,
                        concurrency: request.budgets.concurrency,
                    }),
                })
                .await
                .map_err(|error| {
                    obs::failed_rpc(&observation, &error);
                    mutation_error(key, &error)
                })?
                .into_inner();
            settle_machine(self, key, response, false, |_, contract| {
                request
                    .image
                    .as_ref()
                    .is_none_or(|image| image == &contract.image)
                    && request.selected_volume_ref == contract.selected_volume_ref
            })
            .await
            .map(MutationOutcome::Created)
        })
    }

    async fn attach_volume(
        &self,
        machine: MachineId,
        volume: wire::selection::VolumeRef,
        key: IdempotencyKey,
    ) -> Result<MutationOutcome, ProviderError> {
        obs::operation!("acyclic.machines.attach_volume", observation, async {
            let response = self
                .client()
                .await?
                .attach_volume(wire::AttachVolumeRequest {
                    protocol: Some(protocol()),
                    idempotency_key: Some(encode_key(key)),
                    machine: Some(encode_machine(machine)),
                    selected_volume_ref: Some(volume.clone()),
                })
                .await
                .map_err(|error| {
                    obs::failed_rpc(&observation, &error);
                    mutation_error(key, &error)
                })?
                .into_inner();
            settle_machine(self, key, response, true, |actual, contract| {
                actual == machine && contract.selected_volume_ref.as_ref() == Some(&volume)
            })
            .await
            .map(MutationOutcome::VolumeAttached)
        })
    }

    async fn inspect_machine(
        &self,
        machine: MachineId,
    ) -> Result<MachineObservation, ProviderError> {
        obs::operation!("acyclic.machines.inspect_machine", observation, async {
            self.client()
                .await?
                .inspect_machine(wire::InspectMachineRequest {
                    protocol: Some(protocol()),
                    machine: Some(encode_machine(machine)),
                })
                .await
                .map_err(|error| {
                    obs::failed_rpc(&observation, &error);
                    read_error(&error)
                })
                .and_then(|response| decode_machine_observation(response.into_inner(), machine))
        })
    }

    async fn list_machines(
        &self,
        after: Option<MachineId>,
        limit: u32,
    ) -> Result<MachinePage, ProviderError> {
        obs::operation!("acyclic.machines.list_machines", observation, async {
            if limit == 0 || limit > MAX_PAGE_SIZE {
                return Err(ProviderError::Invalid(
                    "machine page limit must be 1..=256".into(),
                ));
            }
            let page = self
                .client()
                .await?
                .list_machines(wire::ListMachinesRequest {
                    protocol: Some(protocol()),
                    after: after.map(encode_machine),
                    limit,
                })
                .await
                .map_err(|error| {
                    obs::failed_rpc(&observation, &error);
                    read_error(&error)
                })?
                .into_inner();
            let mut previous = after;
            let mut machines = Vec::with_capacity(page.machines.len());
            for value in page.machines {
                let expected = decode_machine(value.machine.as_ref())?;
                if previous.is_some_and(|cursor| expected <= cursor) {
                    return Err(ProviderError::Rejected(
                        "server returned a noncanonical machine page".into(),
                    ));
                }
                machines.push(decode_machine_observation(value, expected)?);
                previous = Some(expected);
            }
            let next = page
                .next
                .as_ref()
                .map(|value| decode_machine(Some(value)))
                .transpose()?;
            if machines.len()
                > usize::try_from(limit)
                    .map_err(|_| ProviderError::Invalid("invalid page limit".into()))?
                || next.is_some() && next != machines.last().map(|value| value.id)
            {
                return Err(ProviderError::Rejected(
                    "server returned a malformed machine cursor".into(),
                ));
            }
            Ok(MachinePage { machines, next })
        })
    }

    async fn checkpoint(
        &self,
        machine: MachineId,
        key: IdempotencyKey,
    ) -> Result<MutationOutcome, ProviderError> {
        obs::operation!("acyclic.machines.checkpoint", observation, async {
            let value = self
                .client()
                .await?
                .checkpoint(wire::CheckpointMachineRequest {
                    protocol: Some(protocol()),
                    idempotency_key: Some(encode_key(key)),
                    machine: Some(encode_machine(machine)),
                })
                .await
                .map_err(|error| {
                    obs::failed_rpc(&observation, &error);
                    mutation_error(key, &error)
                })?
                .into_inner();
            let operation = decode_operation(value.operation.as_ref())
                .map_err(|_| ProviderError::Indeterminate(key))?;
            if value.source != Some(encode_machine(machine)) {
                return Err(ProviderError::Indeterminate(key));
            }
            let outcome = wait(self, key, operation).await?;
            settled_checkpoint(value, outcome).map_err(|_| ProviderError::Indeterminate(key))
        })
    }

    async fn inspect_checkpoint(
        &self,
        checkpoint: CheckpointId,
    ) -> Result<CheckpointObservation, ProviderError> {
        obs::operation!("acyclic.machines.inspect_checkpoint", observation, async {
            self.client()
                .await?
                .inspect_checkpoint(wire::InspectCheckpointRequest {
                    protocol: Some(protocol()),
                    checkpoint: Some(encode_checkpoint(checkpoint)),
                })
                .await
                .map_err(|error| {
                    obs::failed_rpc(&observation, &error);
                    read_error(&error)
                })
                .and_then(|response| {
                    decode_checkpoint_observation(&response.into_inner(), checkpoint)
                })
        })
    }

    async fn fork(
        &self,
        checkpoint: CheckpointId,
        count: NonZeroU32,
        key: IdempotencyKey,
    ) -> Result<MutationOutcome, ProviderError> {
        obs::operation!("acyclic.machines.fork", observation, async {
            let value = self
                .client()
                .await?
                .fork(wire::ForkCheckpointRequest {
                    protocol: Some(protocol()),
                    idempotency_key: Some(encode_key(key)),
                    checkpoint: Some(encode_checkpoint(checkpoint)),
                    count: count.get(),
                })
                .await
                .map_err(|error| {
                    obs::failed_rpc(&observation, &error);
                    mutation_error(key, &error)
                })?
                .into_inner();
            if value.checkpoint != Some(encode_checkpoint(checkpoint))
                || value.children.len()
                    != usize::try_from(count.get())
                        .map_err(|_| ProviderError::Invalid("invalid fork count".into()))?
            {
                return Err(ProviderError::Indeterminate(key));
            }
            let operation = decode_operation(value.operation.as_ref())
                .map_err(|_| ProviderError::Indeterminate(key))?;
            let outcome = wait(self, key, operation).await?;
            settled_fork(value, outcome).map_err(|_| ProviderError::Indeterminate(key))
        })
    }

    async fn fork_machine(
        &self,
        machine: MachineId,
        count: NonZeroU32,
        key: IdempotencyKey,
    ) -> Result<MutationOutcome, ProviderError> {
        obs::operation!("acyclic.machines.fork_machine", observation, async {
            let value = self
                .client()
                .await?
                .fork_machine(wire::ForkMachineRequest {
                    protocol: Some(protocol()),
                    idempotency_key: Some(encode_key(key)),
                    machine: Some(encode_machine(machine)),
                    count: count.get(),
                })
                .await
                .map_err(|error| {
                    obs::failed_rpc(&observation, &error);
                    mutation_error(key, &error)
                })?
                .into_inner();
            let operation = decode_operation(value.operation.as_ref())
                .map_err(|_| ProviderError::Indeterminate(key))?;
            if value.source != Some(encode_machine(machine))
                || value.children.len()
                    != usize::try_from(count.get())
                        .map_err(|_| ProviderError::Invalid("invalid fork count".into()))?
            {
                return Err(ProviderError::Indeterminate(key));
            }
            let outcome = wait(self, key, operation).await?;
            settled_live_fork(value, outcome).map_err(|_| ProviderError::Indeterminate(key))
        })
    }

    async fn set_suspension_policy(
        &self,
        machine: MachineId,
        policy: SuspensionPolicy,
        key: IdempotencyKey,
    ) -> Result<MutationOutcome, ProviderError> {
        obs::operation!(
            "acyclic.machines.set_suspension_policy",
            observation,
            async {
                let value = self
                    .client()
                    .await?
                    .set_suspension_policy(wire::SetSuspensionPolicyRequest {
                        protocol: Some(protocol()),
                        idempotency_key: Some(encode_key(key)),
                        machine: Some(encode_machine(machine)),
                        policy: Some(encode_suspension(policy)?),
                    })
                    .await
                    .map_err(|error| {
                        obs::failed_rpc(&observation, &error);
                        mutation_error(key, &error)
                    })?
                    .into_inner();
                if decode_machine(value.machine.as_ref())
                    .map_err(|_| ProviderError::Indeterminate(key))?
                    != machine
                    || decode_suspension(value.policy.as_ref())
                        .map_err(|_| ProviderError::Indeterminate(key))?
                        != policy
                {
                    return Err(ProviderError::Indeterminate(key));
                }
                let operation = decode_operation(value.operation.as_ref())
                    .map_err(|_| ProviderError::Indeterminate(key))?;
                settled_change(
                    wait(self, key, operation).await?,
                    MutationOutcome::SuspensionPolicySet(machine, policy),
                    key,
                )
                .map_err(|_| ProviderError::Indeterminate(key))
            }
        )
    }

    async fn destroy_checkpoint(
        &self,
        checkpoint: CheckpointId,
        key: IdempotencyKey,
    ) -> Result<MutationOutcome, ProviderError> {
        obs::operation!("acyclic.machines.destroy_checkpoint", observation, async {
            let value = self
                .client()
                .await?
                .destroy_checkpoint(wire::CheckpointMutationRequest {
                    protocol: Some(protocol()),
                    idempotency_key: Some(encode_key(key)),
                    checkpoint: Some(encode_checkpoint(checkpoint)),
                })
                .await
                .map_err(|error| {
                    obs::failed_rpc(&observation, &error);
                    mutation_error(key, &error)
                })?
                .into_inner();
            if decode_checkpoint(value.checkpoint.as_ref())
                .map_err(|_| ProviderError::Indeterminate(key))?
                != checkpoint
            {
                return Err(ProviderError::Indeterminate(key));
            }
            let operation = decode_operation(value.operation.as_ref())
                .map_err(|_| ProviderError::Indeterminate(key))?;
            settled_change(
                wait(self, key, operation).await?,
                MutationOutcome::CheckpointDestroyed(checkpoint),
                key,
            )
            .map_err(|_| ProviderError::Indeterminate(key))
        })
    }

    async fn events(
        &self,
        machine: MachineId,
        after_sequence: Option<u64>,
        limit: u32,
    ) -> Result<EventPage, ProviderError> {
        obs::operation!("acyclic.machines.events", observation, async {
            if limit == 0 || limit > MAX_EVENT_PAGE_SIZE {
                return Err(ProviderError::Invalid(
                    "event page limit must be 1..=1024".into(),
                ));
            }
            let value = self
                .client()
                .await?
                .events(wire::EventsRequest {
                    protocol: Some(protocol()),
                    machine: Some(encode_machine(machine)),
                    after_sequence: after_sequence.unwrap_or(0),
                    limit,
                })
                .await
                .map_err(|error| {
                    obs::failed_rpc(&observation, &error);
                    read_error(&error)
                })?
                .into_inner();
            let mut previous = after_sequence.unwrap_or(0);
            let mut events = Vec::with_capacity(value.events.len());
            for item in value.events {
                let event = decode_event(&item, machine)?;
                if event.sequence <= previous {
                    return Err(ProviderError::Rejected(
                        "event sequence is not increasing".into(),
                    ));
                }
                previous = event.sequence;
                events.push(event);
            }
            let next_sequence = (value.next_sequence != 0).then_some(value.next_sequence);
            if events.len()
                > usize::try_from(limit)
                    .map_err(|_| ProviderError::Invalid("invalid event limit".into()))?
                || next_sequence.is_some()
                    && next_sequence != events.last().map(|event| event.sequence)
            {
                return Err(ProviderError::Rejected("event cursor is malformed".into()));
            }
            Ok(EventPage {
                events,
                next_sequence,
            })
        })
    }

    async fn usage(
        &self,
        machine: MachineId,
        start_unix_ms: u64,
        end_unix_ms: u64,
    ) -> Result<UsageReceipt, ProviderError> {
        obs::operation!("acyclic.machines.usage", observation, async {
            if start_unix_ms >= end_unix_ms {
                return Err(ProviderError::Invalid(
                    "usage interval must be non-empty".into(),
                ));
            }
            let value = self
                .client()
                .await?
                .usage(wire::UsageRequest {
                    protocol: Some(protocol()),
                    machine: Some(encode_machine(machine)),
                    start_unix_ms,
                    end_unix_ms,
                })
                .await
                .map_err(|error| {
                    obs::failed_rpc(&observation, &error);
                    read_error(&error)
                })?
                .into_inner();
            decode_usage_receipt(value, machine, start_unix_ms, end_unix_ms)
        })
    }

    async fn recover(&self, key: IdempotencyKey) -> Result<MutationOutcome, ProviderError> {
        obs::operation!("acyclic.machines.recover", observation, async {
            let (operation, value) = recovered_admission(self, key, &observation).await?;
            let outcome = wait(self, key, operation).await?;
            decode_recovered(value, outcome, key).map_err(|_| ProviderError::Indeterminate(key))
        })
    }

    async fn recover_operation(&self, key: IdempotencyKey) -> Result<OperationId, ProviderError> {
        obs::operation!("acyclic.machines.recover_operation", observation, async {
            recovered_admission(self, key, &observation)
                .await
                .map(|(operation, _)| operation)
        })
    }

    async fn inspect_operation(
        &self,
        operation: OperationId,
    ) -> Result<OperationObservation, ProviderError> {
        obs::operation!("acyclic.machines.inspect_operation", observation, async {
            self.client()
                .await?
                .inspect_operation(operation_request(operation))
                .await
                .map_err(|error| {
                    obs::failed_rpc(&observation, &error);
                    read_error(&error)
                })
                .and_then(|response| {
                    decode_operation_observation(&response.into_inner(), operation)
                })
        })
    }

    async fn cancel(&self, operation: OperationId) -> Result<OperationObservation, ProviderError> {
        obs::operation!("acyclic.machines.cancel", observation, async {
            self.client()
                .await?
                .cancel(operation_request(operation))
                .await
                .map_err(|error| {
                    obs::failed_rpc(&observation, &error);
                    read_error(&error)
                })
                .and_then(|response| {
                    decode_operation_observation(&response.into_inner(), operation)
                })
        })
    }

    async fn watch_operation(
        &self,
        operation: OperationId,
    ) -> Result<OperationStream, ProviderError> {
        obs::operation!("acyclic.machines.watch_operation", observation, async {
            let stream = self
                .client()
                .await?
                .watch_operation(operation_request(operation))
                .await
                .map_err(|error| {
                    obs::failed_rpc(&observation, &error);
                    read_error(&error)
                })?
                .into_inner();
            let stream = stream::unfold((stream, false), move |(mut stream, done)| async move {
                if done {
                    return None;
                }
                match stream.message().await {
                    Ok(Some(value)) => {
                        let value = decode_operation_observation(&value, operation);
                        let decode_failed = value.is_err();
                        let terminal = value
                            .as_ref()
                            .is_ok_and(|value| !matches!(value.phase, OperationPhase::Pending));
                        Some((value, (stream, terminal || decode_failed)))
                    }
                    Ok(None) | Err(_) => {
                        Some((Err(operation_watch_error(operation)), (stream, true)))
                    }
                }
            });
            Ok(Box::pin(stream) as OperationStream)
        })
    }
}
