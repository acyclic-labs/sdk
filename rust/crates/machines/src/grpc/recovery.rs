use super::*;

pub(super) async fn recovered_admission(
    provider: &GrpcProvider,
    key: IdempotencyKey,
) -> Result<(OperationId, wire::RecoveredAdmission), ProviderError> {
    let value = provider
        .client
        .clone()
        .recover(wire::RecoverRequest {
            protocol: Some(protocol()),
            idempotency_key: Some(encode_key(key)),
        })
        .await
        .map_err(|error| recovery_error(key, &error))?
        .into_inner();
    let operation = validate_recovered_admission(&value)?;
    Ok((operation, value))
}

#[tracing::instrument(name = "acyclic.machines.wait", level = "debug", skip_all,
    fields(outcome = obs::Empty, rpc.code = obs::Empty, error.kind = obs::Empty))]
pub(super) async fn wait(
    provider: &GrpcProvider,
    key: IdempotencyKey,
    operation: OperationId,
) -> Result<(), ProviderError> {
    obs::finish(
        async {
            let mut unknown_observations = 0;
            loop {
                match provider
                    .client
                    .clone()
                    .watch_operation(operation_request(operation))
                    .await
                {
                    Ok(response) => {
                        let mut stream = response.into_inner();
                        loop {
                            match tokio::time::timeout(WATCH_RECONCILE_INTERVAL, stream.message())
                                .await
                            {
                                Ok(Ok(Some(value))) => {
                                    let phase =
                                        decode_operation_observation(&value, operation)?.phase;
                                    unknown_observations = 0;
                                    if let Some(outcome) = observed_outcome(key, phase) {
                                        return outcome;
                                    }
                                }
                                Ok(Err(error)) if !retryable_watch_error(&error) => {
                                    return Err(watch_error(key, &error));
                                }
                                // Inspect the admitted operation after interruption; never resubmit.
                                _ => break,
                            }
                        }
                    }
                    Err(error) if !retryable_watch_error(&error) => {
                        return Err(watch_error(key, &error));
                    }
                    Err(_) => {}
                }
                match provider
                    .client
                    .clone()
                    .inspect_operation(operation_request(operation))
                    .await
                {
                    Ok(response) => {
                        let phase =
                            decode_operation_observation(&response.into_inner(), operation)?.phase;
                        unknown_observations = 0;
                        if let Some(outcome) = observed_outcome(key, phase) {
                            return outcome;
                        }
                    }
                    Err(error)
                        if !retryable_observation_error(&error, &mut unknown_observations) =>
                    {
                        return Err(watch_error(key, &error));
                    }
                    Err(_) => {}
                }
                tokio::time::sleep(OBSERVATION_RETRY_DELAY).await;
            }
        }
        .await,
    )
}

pub(super) async fn decode_recovered(
    provider: &GrpcProvider,
    value: wire::RecoveredAdmission,
) -> Result<MutationOutcome, ProviderError> {
    use wire::recovered_admission::Result as ResultKind;
    validate_recovered_admission(&value)?;
    match value
        .result
        .ok_or_else(|| ProviderError::Rejected("recovery result is missing".into()))?
    {
        ResultKind::Create(value) => {
            MachinesProvider::inspect_machine(provider, decode_machine(value.machine.as_ref())?)
                .await
                .map(MutationOutcome::Created)
        }
        ResultKind::Checkpoint(value) => MachinesProvider::inspect_checkpoint(
            provider,
            decode_checkpoint(value.checkpoint.as_ref())?,
        )
        .await
        .map(MutationOutcome::Checkpointed),
        ResultKind::Fork(value) => Ok(MutationOutcome::Forked(
            inspect_children(provider, fork_children(&value.children, None)?).await?,
        )),
        ResultKind::ForkMachine(value) => {
            let (source, fidelity, children, _) = decode_fork_machine_admission(&value)?;
            Ok(MutationOutcome::MachineForked {
                source,
                fidelity,
                children: inspect_children(provider, children).await?,
            })
        }
        ResultKind::Suspend(value) => {
            decode_machine(value.machine.as_ref()).map(MutationOutcome::Suspended)
        }
        ResultKind::Wake(value) => {
            decode_machine(value.machine.as_ref()).map(MutationOutcome::Woken)
        }
        ResultKind::DestroyMachine(value) => {
            decode_machine(value.machine.as_ref()).map(MutationOutcome::MachineDestroyed)
        }
        ResultKind::SetSuspensionPolicy(value) => Ok(MutationOutcome::SuspensionPolicySet(
            decode_machine(value.machine.as_ref())?,
            decode_suspension(value.policy.as_ref())?,
        )),
        ResultKind::DestroyCheckpoint(value) => {
            decode_checkpoint(value.checkpoint.as_ref()).map(MutationOutcome::CheckpointDestroyed)
        }
    }
}

pub(super) async fn inspect_children(
    provider: &GrpcProvider,
    children: Vec<MachineId>,
) -> Result<Vec<MachineObservation>, ProviderError> {
    let mut result = Vec::with_capacity(children.len());
    for child in children {
        result.push(MachinesProvider::inspect_machine(provider, child).await?);
    }
    Ok(result)
}

pub(super) fn decode_fork_machine_admission(
    value: &wire::ForkMachineAdmission,
) -> Result<(MachineId, ForkFidelity, Vec<MachineId>, OperationId), ProviderError> {
    let source = decode_machine(value.source.as_ref())?;
    let operation = decode_operation(value.operation.as_ref())?;
    let contract = decode_contract(value.contract.as_ref())?;
    let fidelity = match wire::ForkFidelity::try_from(value.fidelity)
        .map_err(|_| ProviderError::Rejected("fork fidelity is invalid".into()))?
    {
        wire::ForkFidelity::MemoryAndDisk => ForkFidelity::MemoryAndDisk,
        wire::ForkFidelity::DiskOnly => ForkFidelity::DiskOnly,
        wire::ForkFidelity::Unspecified => {
            return Err(ProviderError::Rejected(
                "fork fidelity is unspecified".into(),
            ));
        }
    };
    if contract.fork_fidelity() != Some(fidelity) {
        return Err(ProviderError::Rejected(
            "fork fidelity contradicts the source contract".into(),
        ));
    }
    Ok((
        source,
        fidelity,
        fork_children(&value.children, Some(source))?,
        operation,
    ))
}

pub(super) fn fork_children(
    values: &[wire::MachineId],
    source: Option<MachineId>,
) -> Result<Vec<MachineId>, ProviderError> {
    if values.is_empty()
        || values.len()
            > usize::try_from(MAX_FORK_CHILDREN)
                .map_err(|_| ProviderError::Invalid("invalid fork bound".into()))?
    {
        return Err(ProviderError::Rejected(
            "fork child set is outside the public bound".into(),
        ));
    }
    let mut seen = BTreeSet::new();
    let mut children = Vec::with_capacity(values.len());
    for item in values {
        let child = decode_machine(Some(item))?;
        if Some(child) == source || !seen.insert(child) {
            return Err(ProviderError::Rejected(
                "fork returned duplicate children".into(),
            ));
        }
        children.push(child);
    }
    Ok(children)
}

pub(super) fn validate_recovered_admission(
    value: &wire::RecoveredAdmission,
) -> Result<OperationId, ProviderError> {
    use wire::recovered_admission::Result as ResultKind;
    let outer = decode_operation(value.operation.as_ref())?;
    let nested = match value
        .result
        .as_ref()
        .ok_or_else(|| ProviderError::Rejected("recovery result is missing".into()))?
    {
        ResultKind::Create(value) => value.operation.as_ref(),
        ResultKind::Checkpoint(value) => value.operation.as_ref(),
        ResultKind::Fork(value) => value.operation.as_ref(),
        ResultKind::ForkMachine(value) => value.operation.as_ref(),
        ResultKind::Suspend(value)
        | ResultKind::Wake(value)
        | ResultKind::DestroyMachine(value)
        | ResultKind::DestroyCheckpoint(value) => value.operation.as_ref(),
        ResultKind::SetSuspensionPolicy(value) => value.operation.as_ref(),
    };
    if decode_operation(nested)? == outer {
        Ok(outer)
    } else {
        Err(ProviderError::Rejected(
            "recovery operation identity was substituted".into(),
        ))
    }
}

pub(super) fn retryable_observation_error(
    value: &tonic::Status,
    unknown_observations: &mut u8,
) -> bool {
    match value.code() {
        tonic::Code::Unavailable | tonic::Code::DeadlineExceeded | tonic::Code::Cancelled => true,
        // Repeated ambiguous errors return the retained recovery key.
        tonic::Code::Unknown => {
            *unknown_observations = unknown_observations.saturating_add(1);
            *unknown_observations < MAX_CONSECUTIVE_UNKNOWN_INSPECTIONS
        }
        _ => false,
    }
}

pub(super) fn retryable_watch_error(value: &tonic::Status) -> bool {
    matches!(
        value.code(),
        tonic::Code::Unavailable
            | tonic::Code::DeadlineExceeded
            | tonic::Code::Cancelled
            | tonic::Code::Unknown
    )
}

pub(super) fn observed_outcome(
    key: IdempotencyKey,
    phase: OperationPhase,
) -> Option<Result<(), ProviderError>> {
    match phase {
        OperationPhase::Pending => None,
        OperationPhase::Succeeded => Some(Ok(())),
        OperationPhase::Cancelled => Some(Err(ProviderError::Cancelled)),
        OperationPhase::Failed => Some(Err(ProviderError::Failed)),
        OperationPhase::Indeterminate => Some(Err(ProviderError::Indeterminate(key))),
    }
}
