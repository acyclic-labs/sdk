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
    settled_machine(machine, contract, outcome, attached)
        .map_err(|_| ProviderError::Indeterminate(key))
}

/// Validate the original successful create or attachment terminal without a
/// provider call. The caller binds it to the admitted operation and completes
/// its authenticated transport before publishing the result.
pub fn settled_machine(
    machine: MachineId,
    contract: MachineContract,
    outcome: Option<wire::MutationOutcome>,
    attached: bool,
) -> Result<MachineObservation, ProviderError> {
    let result = outcome.and_then(|outcome| outcome.result);
    let value = match result {
        Some(wire::mutation_outcome::Result::Created(value)) if !attached => value,
        Some(wire::mutation_outcome::Result::VolumeAttached(value)) if attached => value,
        _ => {
            return Err(ProviderError::Rejected(
                "original settled machine result is missing".into(),
            ));
        }
    };
    let observation = decode_machine_observation(value, machine)?;
    if observation.contract != contract {
        return Err(ProviderError::Rejected(
            "settled machine contract was substituted".into(),
        ));
    }
    Ok(observation)
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

/// Decode the retained admission and its original successful terminal. The caller
/// binds the terminal to the admitted operation and completes its authenticated
/// transport. Malformed results never authorize redispatch.
pub fn decode_recovered(
    value: wire::RecoveredAdmission,
    outcome: Option<wire::MutationOutcome>,
    key: IdempotencyKey,
) -> Result<MutationOutcome, ProviderError> {
    use wire::recovered_admission::Result as ResultKind;
    validate_recovered_admission(&value)?;
    match value
        .result
        .ok_or_else(|| ProviderError::Rejected("recovery result is missing".into()))?
    {
        ResultKind::Create(value) | ResultKind::AttachVolume(value)
            if value.provenance.is_some() =>
        {
            Err(ProviderError::Rejected(
                "created machine admission carries fork provenance".into(),
            ))
        }
        ResultKind::Create(value) => settled_machine(
            decode_machine(value.machine.as_ref())?,
            decode_contract(value.contract.as_ref())?,
            outcome,
            false,
        )
        .map(MutationOutcome::Created),
        ResultKind::AttachVolume(value) => settled_machine(
            decode_machine(value.machine.as_ref())?,
            decode_contract(value.contract.as_ref())?,
            outcome,
            true,
        )
        .map(MutationOutcome::VolumeAttached),
        ResultKind::Checkpoint(value) => settled_checkpoint(value, outcome),
        ResultKind::Forked(value) => settled_fork(
            &decode_fork_admissions(&value)?,
            OperationPhase::Succeeded,
            outcome,
        ),
        ResultKind::Suspend(value) => settled_change(
            outcome,
            MutationOutcome::Suspended(decode_machine(value.machine.as_ref())?),
            key,
        ),
        ResultKind::Wake(value) => settled_change(
            outcome,
            MutationOutcome::Woken(decode_machine(value.machine.as_ref())?),
            key,
        ),
        ResultKind::DestroyMachine(value) => settled_change(
            outcome,
            MutationOutcome::MachineDestroyed(decode_machine(value.machine.as_ref())?),
            key,
        ),
        ResultKind::SetSuspensionPolicy(value) => settled_change(
            outcome,
            MutationOutcome::SuspensionPolicySet(
                decode_machine(value.machine.as_ref())?,
                decode_suspension(value.policy.as_ref())?,
            ),
            key,
        ),
        ResultKind::DestroyCheckpoint(value) => settled_change(
            outcome,
            MutationOutcome::CheckpointDestroyed(decode_checkpoint(value.checkpoint.as_ref())?),
            key,
        ),
    }
}

/// Match a lifecycle terminal to the original requested resource and policy.
/// The caller retains the operation, watch and authenticated transport association.
pub fn settled_change(
    outcome: Option<wire::MutationOutcome>,
    expected: MutationOutcome,
    key: IdempotencyKey,
) -> Result<MutationOutcome, ProviderError> {
    use wire::mutation_outcome::Result as Outcome;
    let actual = match outcome.and_then(|value| value.result) {
        Some(Outcome::Suspended(machine)) => {
            MutationOutcome::Suspended(decode_machine(Some(&machine))?)
        }
        Some(Outcome::Woken(machine)) => MutationOutcome::Woken(decode_machine(Some(&machine))?),
        Some(Outcome::MachineDestroyed(machine)) => {
            MutationOutcome::MachineDestroyed(decode_machine(Some(&machine))?)
        }
        Some(Outcome::SuspensionPolicySet(value)) => MutationOutcome::SuspensionPolicySet(
            decode_machine(value.machine.as_ref())?,
            decode_suspension(value.policy.as_ref())?,
        ),
        Some(Outcome::CheckpointDestroyed(checkpoint)) => {
            MutationOutcome::CheckpointDestroyed(decode_checkpoint(Some(&checkpoint))?)
        }
        _ => return Err(ProviderError::Indeterminate(key)),
    };
    if actual != expected {
        return Err(ProviderError::Indeterminate(key));
    }
    Ok(actual)
}

/// Match a checkpoint terminal to its original admission without a provider call.
/// The caller retains the operation and authenticated transport association.
pub fn settled_checkpoint(
    admission: wire::CheckpointAdmission,
    outcome: Option<wire::MutationOutcome>,
) -> Result<MutationOutcome, ProviderError> {
    let Some(wire::mutation_outcome::Result::Checkpointed(value)) =
        outcome.and_then(|value| value.result)
    else {
        return Err(ProviderError::Rejected(
            "original checkpoint result is missing".into(),
        ));
    };
    let result =
        decode_checkpoint_observation(&value, decode_checkpoint(admission.checkpoint.as_ref())?)?;
    if result.source != decode_machine(admission.source.as_ref())?
        || result.contract != decode_contract(admission.contract.as_ref())?
    {
        return Err(ProviderError::Rejected(
            "checkpoint result was substituted".into(),
        ));
    }
    Ok(MutationOutcome::Checkpointed(result))
}

/// One original operation admission, independent of aggregate child bytes.
pub struct ForkAdmitted {
    pub operation: OperationId,
    pub provenance: Provenance<ForkSource, ForkFidelity>,
    pub count: u64,
    pub contract: MachineContract,
}

/// Shared client/server admission check, including Edge's HTTP/JSON boundary.
/// The source contract is required and its declared fidelity must match exactly.
pub fn decode_fork_admissions(
    value: &wire::MachineAdmissions,
) -> Result<ForkAdmitted, ProviderError> {
    let operation = decode_operation(value.operation.as_ref())?;
    let provenance = decode_provenance(value.provenance.as_ref())?;
    let contract = decode_contract(value.contract.as_ref())?;
    check_fork_fidelity(&contract, provenance)?;
    if value.count == 0 {
        return Err(ProviderError::Rejected("fork admission extent or provenance is invalid; recovery: recover the original operation".into()));
    }
    Ok(ForkAdmitted {
        operation,
        provenance,
        count: value.count,
        contract,
    })
}

fn decode_provenance(
    value: Option<&wire::Provenance>,
) -> Result<Provenance<ForkSource, ForkFidelity>, ProviderError> {
    let value =
        value.ok_or_else(|| ProviderError::Rejected("fork provenance is missing".into()))?;
    let source = match value.source.as_ref() {
        Some(wire::provenance::Source::MachineId(machine)) => {
            ForkSource::Machine(decode_machine(Some(machine))?)
        }
        Some(wire::provenance::Source::CheckpointId(checkpoint)) => {
            ForkSource::Checkpoint(decode_checkpoint(Some(checkpoint))?)
        }
        None => {
            return Err(ProviderError::Rejected(
                "fork provenance source is missing".into(),
            ));
        }
    };
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
    Ok(Provenance {
        source,
        inherited: fidelity,
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

/// Settles a checked fork admission against its original terminal outcome.
/// The caller binds the terminal and phase to admitted.operation and completes
/// its authenticated transport. Failed/cancelled forks retain the immutable
/// partial result cut; no child read, current-resource lookup or wait occurs.
pub fn settled_fork(
    admitted: &ForkAdmitted,
    phase: OperationPhase,
    outcome: Option<wire::MutationOutcome>,
) -> Result<MutationOutcome, ProviderError> {
    let Some(wire::mutation_outcome::Result::Forked(value)) =
        outcome.and_then(|value| value.result)
    else {
        return Err(ProviderError::Rejected(
            "original fork result is missing".into(),
        ));
    };
    let results = decode_result_cut(value.results.as_ref())?;
    if decode_provenance(value.provenance.as_ref())? != admitted.provenance
        || results.operation != admitted.operation
        || results.count > admitted.count
        || phase == OperationPhase::Succeeded && results.count != admitted.count
    {
        return Err(ProviderError::Rejected(
            "fork settlement was substituted; recovery: recover the original operation".into(),
        ));
    }
    match phase {
        OperationPhase::Succeeded => {}
        OperationPhase::Failed | OperationPhase::Cancelled => {
            return Err(ProviderError::ForkFailed { phase, results });
        }
        _ => {
            return Err(ProviderError::Rejected(
                "fork has no terminal settlement".into(),
            ));
        }
    }
    Ok(MutationOutcome::Forked {
        provenance: admitted.provenance,
        results,
    })
}

/// Check that outer and nested recovery operation identities agree. This alone
/// confers no request authority; settle the original terminal with decode_recovered.
pub fn validate_recovered_admission(
    value: &wire::RecoveredAdmission,
) -> Result<OperationId, ProviderError> {
    use wire::recovered_admission::Result as ResultKind;
    let outer = decode_operation(value.operation.as_ref())?;
    let nested = match value
        .result
        .as_ref()
        .ok_or_else(|| ProviderError::Rejected("recovery result is missing".into()))?
    {
        ResultKind::Create(value) | ResultKind::AttachVolume(value) => value.operation.as_ref(),
        ResultKind::Checkpoint(value) => value.operation.as_ref(),
        ResultKind::Forked(value) => value.operation.as_ref(),
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
