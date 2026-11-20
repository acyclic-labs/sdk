use super::*;

pub(super) async fn settle_machine<E: RpcEndpoint>(
    provider: &Provider<E>,
    key: IdempotencyKey,
    admission: wire::MachineAdmission,
    attached: bool,
    accepts: impl FnOnce(MachineId, &MachineContract) -> bool + acyclic_stream::ProviderTask,
) -> Result<MachineObservation, ProviderError> {
    let operation = decode_operation(admission.operation.as_ref())
        .map_err(|_| ProviderError::Indeterminate(key))?;
    let machine = decode_machine(admission.machine.as_ref())
        .map_err(|_| ProviderError::Indeterminate(key))?;
    let contract = decode_contract(admission.contract.as_ref())
        .map_err(|_| ProviderError::Indeterminate(key))?;
    if admission.provenance.is_some() || !accepts(machine, &contract) {
        return Err(ProviderError::Indeterminate(key));
    }
    let outcome = wait(provider, key, operation).await?;
    settled_machine(machine, &contract, outcome, attached)
        .map_err(|_| ProviderError::Indeterminate(key))
}
pub(super) async fn recovered_admission<E: RpcEndpoint>(
    provider: &Provider<E>,
    key: IdempotencyKey,
    observation: &obs::Span,
) -> Result<(OperationId, wire::RecoveredAdmission), ProviderError> {
    let value = provider
        .client()
        .await
        .map_err(|_| ProviderError::Indeterminate(key))?
        .recover(wire::RecoverRequest {
            protocol: Some(protocol()),
            idempotency_key: Some(encode_key(key)),
        })
        .await
        .map_err(|error| {
            obs::failed_rpc(observation, &error);
            recovery_error(key, error)
        })?
        .into_inner();
    let operation =
        validate_recovered_admission(&value).map_err(|_| ProviderError::Indeterminate(key))?;
    Ok((operation, value))
}

pub(super) async fn wait<E: RpcEndpoint>(
    provider: &Provider<E>,
    key: IdempotencyKey,
    operation: OperationId,
) -> Result<Option<wire::MutationOutcome>, ProviderError> {
    let (phase, outcome) = wait_terminal(provider, key, operation).await?;
    observed_outcome(key, phase).ok_or(ProviderError::Indeterminate(key))??;
    Ok(outcome)
}

pub(super) async fn wait_terminal<E: RpcEndpoint>(
    provider: &Provider<E>,
    key: IdempotencyKey,
    operation: OperationId,
) -> Result<(OperationPhase, Option<wire::MutationOutcome>), ProviderError> {
    obs::operation!(@tracing::Level::DEBUG, "acyclic.machines.wait", observation, async {
        let mut stream = provider
            .client()
            .await
            .map_err(|_| ProviderError::Indeterminate(key))?
            .watch_operation(operation_request(operation))
            .await
            .map_err(|error| {
                obs::failed_rpc(&observation, &error);
                watch_error(key, error)
            })?
            .into_inner();
        // The server owns completion. Quiet output does not authorize an
        // inspection, another subscription or another mutation dispatch.
        while let Some(value) = stream.message().await.map_err(|error| {
            obs::failed_rpc(&observation, &error);
            watch_error(key, error)
        })? {
            let phase = decode_operation_observation(&value, operation)
                .map_err(|_| ProviderError::Indeterminate(key))?
                .phase;
            if phase != OperationPhase::Pending {
                return Ok((phase, value.outcome));
            }
        }
        Err(ProviderError::Indeterminate(key))
    })
}

/// Checks a Fork or ForkMachine reply against its request, then settles it on
/// the shared operation's original terminal outcome.
pub(super) async fn settle_fork<E: RpcEndpoint>(
    provider: &Provider<E>,
    key: IdempotencyKey,
    source: ForkSource,
    count: NonZeroU32,
    reply: &wire::MachineAdmissions,
) -> Result<MutationOutcome, ProviderError> {
    let admitted = decode_fork_admissions(reply).map_err(|_| ProviderError::Indeterminate(key))?;
    if admitted.provenance.source != source || admitted.count != u64::from(count.get()) {
        return Err(ProviderError::Indeterminate(key));
    }
    let (phase, outcome) = wait_terminal(provider, key, admitted.operation).await?;
    fork_outcome(&admitted, phase, outcome, key)
}

pub(super) fn fork_outcome(
    admitted: &ForkAdmitted,
    phase: OperationPhase,
    outcome: Option<wire::MutationOutcome>,
    key: IdempotencyKey,
) -> Result<MutationOutcome, ProviderError> {
    match settled_fork(admitted, phase, outcome) {
        value @ (Ok(_) | Err(ProviderError::ForkFailed { .. })) => value,
        Err(_) => Err(ProviderError::Indeterminate(key)),
    }
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
