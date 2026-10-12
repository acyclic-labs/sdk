use super::*;

macro_rules! machine_mutations {
    ($(($method:ident, $outcome:ident, $span:literal)),+; $($other:tt)*) => {
        #[async_trait]
        impl MachinesProvider for GrpcProvider {
        $(
        #[tracing::instrument(name = $span, skip_all,
            fields(outcome = obs::Empty, rpc.code = obs::Empty, error.kind = obs::Empty))]
        async fn $method(&self, machine: MachineId, key: IdempotencyKey)
            -> Result<MutationOutcome, ProviderError> {
            obs::finish(async {
                let value = self.client.clone().$method(wire::MachineMutationRequest {
                    protocol: Some(protocol()),
                    idempotency_key: Some(encode_key(key)),
                    machine: Some(encode_machine(machine)),
                }).await.map_err(|error| mutation_error(key, &error))?.into_inner();
                if decode_machine(value.machine.as_ref())? != machine {
                    return Err(ProviderError::Rejected("machine result was substituted".into()));
                }
                wait(self, key, decode_operation(value.operation.as_ref())?).await?;
                Ok(MutationOutcome::$outcome(machine))
            }.await)
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

    #[tracing::instrument(
        name = "acyclic.machines.qualify_image",
        skip_all,
        fields(outcome = obs::Empty, rpc.code = obs::Empty, error.kind = obs::Empty)
    )]
    async fn qualify_image(&self, image: Image) -> Result<ImageQualification, ProviderError> {
        obs::finish(
            async {
                let response = self.client.clone()
                    .qualify_image(wire::QualifyImageRequest {
                        protocol: Some(protocol()),
                        image: Some(encode_image(&image)?),
                    })
                    .await
                    .map_err(|error| read_error(&error))?
                    .into_inner();
                decode_qualification(response, &image)
            }
            .await,
        )
    }

    #[tracing::instrument(
        name = "acyclic.machines.create",
        skip_all,
        fields(outcome = obs::Empty, rpc.code = obs::Empty, error.kind = obs::Empty)
    )]
    async fn create(&self, request: CreateMachine) -> Result<MutationOutcome, ProviderError> {
        obs::finish(
            async {
                let key = request.idempotency_key;
                let response = self.client.clone()
                    .create(wire::CreateMachineRequest {
                        protocol: Some(protocol()),
                        idempotency_key: Some(encode_key(key)),
                        image: Some(encode_image(&request.image)?),
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
                    .map_err(|error| mutation_error(key, &error))?
                    .into_inner();
                let operation = decode_operation(response.operation.as_ref())?;
                let machine = decode_machine(response.machine.as_ref())?;
                let _contract = decode_contract(response.contract.as_ref())?;
                wait(self, key, operation).await?;
                self.inspect_machine(machine)
                    .await
                    .map(MutationOutcome::Created)
            }
            .await,
        )
    }

    #[tracing::instrument(
        name = "acyclic.machines.inspect_machine",
        skip_all,
        fields(outcome = obs::Empty, rpc.code = obs::Empty, error.kind = obs::Empty)
    )]
    async fn inspect_machine(
        &self,
        machine: MachineId,
    ) -> Result<MachineObservation, ProviderError> {
        obs::finish(
            async {
                self.client.clone()
                    .inspect_machine(wire::InspectMachineRequest {
                        protocol: Some(protocol()),
                        machine: Some(encode_machine(machine)),
                    })
                    .await
                    .map_err(|error| read_error(&error))
                    .and_then(|response| decode_machine_observation(response.into_inner(), machine))
            }
            .await,
        )
    }

    #[tracing::instrument(
        name = "acyclic.machines.list_machines",
        skip_all,
        fields(outcome = obs::Empty, rpc.code = obs::Empty, error.kind = obs::Empty)
    )]
    async fn list_machines(
        &self,
        after: Option<MachineId>,
        limit: u32,
    ) -> Result<MachinePage, ProviderError> {
        obs::finish(
            async {
                if limit == 0 || limit > MAX_PAGE_SIZE {
                    return Err(ProviderError::Invalid(
                        "machine page limit must be 1..=256".into(),
                    ));
                }
                let page = self.client.clone()
                    .list_machines(wire::ListMachinesRequest {
                        protocol: Some(protocol()),
                        after: after.map(encode_machine),
                        limit,
                    })
                    .await
                    .map_err(|error| read_error(&error))?
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
            }
            .await,
        )
    }

    #[tracing::instrument(
        name = "acyclic.machines.checkpoint",
        skip_all,
        fields(outcome = obs::Empty, rpc.code = obs::Empty, error.kind = obs::Empty)
    )]
    async fn checkpoint(
        &self,
        machine: MachineId,
        key: IdempotencyKey,
    ) -> Result<MutationOutcome, ProviderError> {
        obs::finish(
            async {
                let value = self.client.clone()
                    .checkpoint(wire::CheckpointMachineRequest {
                        protocol: Some(protocol()),
                        idempotency_key: Some(encode_key(key)),
                        machine: Some(encode_machine(machine)),
                    })
                    .await
                    .map_err(|error| mutation_error(key, &error))?
                    .into_inner();
                let operation = decode_operation(value.operation.as_ref())?;
                let checkpoint = decode_checkpoint(value.checkpoint.as_ref())?;
                if decode_machine(value.source.as_ref())? != machine {
                    return Err(ProviderError::Rejected(
                        "checkpoint source was substituted".into(),
                    ));
                }
                wait(self, key, operation).await?;
                self.inspect_checkpoint(checkpoint)
                    .await
                    .map(MutationOutcome::Checkpointed)
            }
            .await,
        )
    }

    #[tracing::instrument(
        name = "acyclic.machines.inspect_checkpoint",
        skip_all,
        fields(outcome = obs::Empty, rpc.code = obs::Empty, error.kind = obs::Empty)
    )]
    async fn inspect_checkpoint(
        &self,
        checkpoint: CheckpointId,
    ) -> Result<CheckpointObservation, ProviderError> {
        obs::finish(
            async {
                self.client.clone()
                    .inspect_checkpoint(wire::InspectCheckpointRequest {
                        protocol: Some(protocol()),
                        checkpoint: Some(encode_checkpoint(checkpoint)),
                    })
                    .await
                    .map_err(|error| read_error(&error))
                    .and_then(|response| {
                        decode_checkpoint_observation(&response.into_inner(), checkpoint)
                    })
            }
            .await,
        )
    }

    #[tracing::instrument(
        name = "acyclic.machines.fork",
        skip_all,
        fields(outcome = obs::Empty, rpc.code = obs::Empty, error.kind = obs::Empty)
    )]
    async fn fork(
        &self,
        checkpoint: CheckpointId,
        count: NonZeroU32,
        key: IdempotencyKey,
    ) -> Result<MutationOutcome, ProviderError> {
        obs::finish(
            async {
                if count.get() > MAX_FORK_CHILDREN {
                    return Err(ProviderError::Invalid("fork count exceeds 1024".into()));
                }
                let value = self.client.clone()
                    .fork(wire::ForkCheckpointRequest {
                        protocol: Some(protocol()),
                        idempotency_key: Some(encode_key(key)),
                        checkpoint: Some(encode_checkpoint(checkpoint)),
                        count: count.get(),
                    })
                    .await
                    .map_err(|error| mutation_error(key, &error))?
                    .into_inner();
                if decode_checkpoint(value.checkpoint.as_ref())? != checkpoint
                    || value.children.len()
                        != usize::try_from(count.get())
                            .map_err(|_| ProviderError::Invalid("invalid fork count".into()))?
                {
                    return Err(ProviderError::Rejected(
                        "fork result was substituted".into(),
                    ));
                }
                let operation = decode_operation(value.operation.as_ref())?;
                wait(self, key, operation).await?;
                Ok(MutationOutcome::Forked(
                    inspect_children(self, fork_children(&value.children, None)?).await?,
                ))
            }
            .await,
        )
    }

    #[tracing::instrument(
        name = "acyclic.machines.fork_machine",
        skip_all,
        fields(outcome = obs::Empty, rpc.code = obs::Empty, error.kind = obs::Empty)
    )]
    async fn fork_machine(
        &self,
        machine: MachineId,
        count: NonZeroU32,
        key: IdempotencyKey,
    ) -> Result<MutationOutcome, ProviderError> {
        obs::finish(
            async {
                if count.get() > MAX_FORK_CHILDREN {
                    return Err(ProviderError::Invalid("fork count exceeds 1024".into()));
                }
                let value = self.client.clone()
                    .fork_machine(wire::ForkMachineRequest {
                        protocol: Some(protocol()),
                        idempotency_key: Some(encode_key(key)),
                        machine: Some(encode_machine(machine)),
                        count: count.get(),
                    })
                    .await
                    .map_err(|error| mutation_error(key, &error))?
                    .into_inner();
                let (source, fidelity, children, operation) = decode_fork_machine_admission(&value)?;
                if source != machine
                    || children.len()
                        != usize::try_from(count.get())
                            .map_err(|_| ProviderError::Invalid("invalid fork count".into()))?
                {
                    return Err(ProviderError::Rejected(
                        "fork result was substituted".into(),
                    ));
                }
                wait(self, key, operation).await?;
                Ok(MutationOutcome::MachineForked {
                    source, fidelity, children: inspect_children(self, children).await?,
                })
            }
            .await,
        )
    }

    #[tracing::instrument(
        name = "acyclic.machines.set_suspension_policy",
        skip_all,
        fields(outcome = obs::Empty, rpc.code = obs::Empty, error.kind = obs::Empty)
    )]
    async fn set_suspension_policy(
        &self,
        machine: MachineId,
        policy: SuspensionPolicy,
        key: IdempotencyKey,
    ) -> Result<MutationOutcome, ProviderError> {
        obs::finish(
            async {
                let value = self.client.clone()
                    .set_suspension_policy(wire::SetSuspensionPolicyRequest {
                        protocol: Some(protocol()),
                        idempotency_key: Some(encode_key(key)),
                        machine: Some(encode_machine(machine)),
                        policy: Some(encode_suspension(policy)?),
                    })
                    .await
                    .map_err(|error| mutation_error(key, &error))?
                    .into_inner();
                if decode_machine(value.machine.as_ref())? != machine
                    || decode_suspension(value.policy.as_ref())? != policy
                {
                    return Err(ProviderError::Rejected(
                        "policy result was substituted".into(),
                    ));
                }
                wait(self, key, decode_operation(value.operation.as_ref())?)
                    .await?;
                Ok(MutationOutcome::SuspensionPolicySet(machine, policy))
            }
            .await,
        )
    }
    #[tracing::instrument(
        name = "acyclic.machines.destroy_checkpoint",
        skip_all,
        fields(outcome = obs::Empty, rpc.code = obs::Empty, error.kind = obs::Empty)
    )]
    async fn destroy_checkpoint(
        &self,
        checkpoint: CheckpointId,
        key: IdempotencyKey,
    ) -> Result<MutationOutcome, ProviderError> {
        obs::finish(
            async {
                let value = self.client.clone()
                    .destroy_checkpoint(wire::CheckpointMutationRequest {
                        protocol: Some(protocol()),
                        idempotency_key: Some(encode_key(key)),
                        checkpoint: Some(encode_checkpoint(checkpoint)),
                    })
                    .await
                    .map_err(|error| mutation_error(key, &error))?
                    .into_inner();
                if decode_checkpoint(value.checkpoint.as_ref())? != checkpoint {
                    return Err(ProviderError::Rejected(
                        "checkpoint result was substituted".into(),
                    ));
                }
                wait(self, key, decode_operation(value.operation.as_ref())?)
                    .await?;
                Ok(MutationOutcome::CheckpointDestroyed(checkpoint))
            }
            .await,
        )
    }
    #[tracing::instrument(
        name = "acyclic.machines.events",
        skip_all,
        fields(outcome = obs::Empty, rpc.code = obs::Empty, error.kind = obs::Empty)
    )]
    async fn events(
        &self,
        machine: MachineId,
        after_sequence: Option<u64>,
        limit: u32,
    ) -> Result<EventPage, ProviderError> {
        obs::finish(
            async {
                if limit == 0 || limit > MAX_EVENT_PAGE_SIZE {
                    return Err(ProviderError::Invalid(
                        "event page limit must be 1..=1024".into(),
                    ));
                }
                let value = self.client.clone()
                    .events(wire::EventsRequest {
                        protocol: Some(protocol()),
                        machine: Some(encode_machine(machine)),
                        after_sequence: after_sequence.unwrap_or(0),
                        limit,
                    })
                    .await
                    .map_err(|error| read_error(&error))?
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
            }
            .await,
        )
    }
    #[tracing::instrument(
        name = "acyclic.machines.usage",
        skip_all,
        fields(outcome = obs::Empty, rpc.code = obs::Empty, error.kind = obs::Empty)
    )]
    async fn usage(
        &self,
        machine: MachineId,
        start_unix_ms: u64,
        end_unix_ms: u64,
    ) -> Result<UsageReceipt, ProviderError> {
        obs::finish(
            async {
                if start_unix_ms >= end_unix_ms {
                    return Err(ProviderError::Invalid(
                        "usage interval must be non-empty".into(),
                    ));
                }
                let value = self.client.clone()
                    .usage(wire::UsageRequest {
                        protocol: Some(protocol()),
                        machine: Some(encode_machine(machine)),
                        start_unix_ms,
                        end_unix_ms,
                    })
                    .await
                    .map_err(|error| read_error(&error))?
                    .into_inner();
                decode_usage_receipt(value, machine, start_unix_ms, end_unix_ms)
            }
            .await,
        )
    }
    #[tracing::instrument(
        name = "acyclic.machines.recover",
        skip_all,
        fields(outcome = obs::Empty, rpc.code = obs::Empty, error.kind = obs::Empty)
    )]
    async fn recover(&self, key: IdempotencyKey) -> Result<MutationOutcome, ProviderError> {
        obs::finish(
            async {
                let (operation, value) = recovered_admission(self, key).await?;
                wait(self, key, operation).await?;
                decode_recovered(self, value).await
            }
            .await,
        )
    }
    #[tracing::instrument(
        name = "acyclic.machines.recover_operation",
        skip_all,
        fields(outcome = obs::Empty, rpc.code = obs::Empty, error.kind = obs::Empty)
    )]
    async fn recover_operation(&self, key: IdempotencyKey) -> Result<OperationId, ProviderError> {
        obs::finish(
            async {
                recovered_admission(self, key)
                    .await
                    .map(|(operation, _)| operation)
            }
            .await,
        )
    }
    #[tracing::instrument(
        name = "acyclic.machines.inspect_operation",
        skip_all,
        fields(outcome = obs::Empty, rpc.code = obs::Empty, error.kind = obs::Empty)
    )]
    async fn inspect_operation(
        &self,
        operation: OperationId,
    ) -> Result<OperationObservation, ProviderError> {
        obs::finish(
            async {
                self.client.clone()
                    .inspect_operation(operation_request(operation))
                    .await
                    .map_err(|error| read_error(&error))
                    .and_then(|response| {
                        decode_operation_observation(&response.into_inner(), operation)
                    })
            }
            .await,
        )
    }
    #[tracing::instrument(
        name = "acyclic.machines.cancel",
        skip_all,
        fields(outcome = obs::Empty, rpc.code = obs::Empty, error.kind = obs::Empty)
    )]
    async fn cancel(&self, operation: OperationId) -> Result<OperationObservation, ProviderError> {
        obs::finish(
            async {
                self.client.clone()
                    .cancel(operation_request(operation))
                    .await
                    .map_err(|error| read_error(&error))
                    .and_then(|response| {
                        decode_operation_observation(&response.into_inner(), operation)
                    })
            }
            .await,
        )
    }
    #[tracing::instrument(
        name = "acyclic.machines.watch_operation",
        skip_all,
        fields(outcome = obs::Empty, rpc.code = obs::Empty, error.kind = obs::Empty)
    )]
    async fn watch_operation(
        &self,
        operation: OperationId,
    ) -> Result<OperationStream, ProviderError> {
        obs::finish(
            async {
                let stream = self.client.clone()
                    .watch_operation(operation_request(operation))
                    .await
                    .map_err(|error| read_error(&error))?
                    .into_inner();
                let stream =
                    stream::unfold((stream, false), move |(mut stream, done)| async move {
                        if done {
                            return None;
                        }
                        match stream.message().await {
                            Ok(Some(value)) => {
                                let value = decode_operation_observation(&value, operation);
                                let decode_failed = value.is_err();
                                let terminal = value.as_ref().is_ok_and(|value| {
                                    !matches!(value.phase, OperationPhase::Pending)
                                });
                                Some((value, (stream, terminal || decode_failed)))
                            }
                            Ok(None) | Err(_) => {
                                Some((Err(operation_watch_error(operation)), (stream, true)))
                            }
                        }
                    });
                Ok(Box::pin(stream) as OperationStream)
            }
            .await,
        )
    }
}
