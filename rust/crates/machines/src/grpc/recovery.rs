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

fn settled_machine(
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
        .map_err(|error| recovery_error(key, &error))?
        .into_inner();
    let operation =
        validate_recovered_admission(&value).map_err(|_| ProviderError::Indeterminate(key))?;
    Ok((operation, value))
}

#[cfg_attr(not(target_arch = "wasm32"), tracing::instrument(name = "acyclic.machines.wait", level = "debug", skip_all,
    fields(outcome = obs::Empty, rpc.code = obs::Empty, error.kind = obs::Empty)))]
pub(super) async fn wait<E: RpcEndpoint>(
    provider: &Provider<E>,
    key: IdempotencyKey,
    operation: OperationId,
) -> Result<Option<wire::MutationOutcome>, ProviderError> {
    obs::finish(
        async {
            let mut stream = provider
                .client()
                .await
                .map_err(|_| ProviderError::Indeterminate(key))?
                .watch_operation(operation_request(operation))
                .await
                .map_err(|error| watch_error(key, &error))?
                .into_inner();
            // The server owns completion. Quiet output does not authorize an
            // inspection, another subscription or another mutation dispatch.
            while let Some(value) = stream
                .message()
                .await
                .map_err(|error| watch_error(key, &error))?
            {
                let phase = decode_operation_observation(&value, operation)
                    .map_err(|_| ProviderError::Indeterminate(key))?
                    .phase;
                if let Some(outcome) = observed_outcome(key, phase) {
                    outcome?;
                    return Ok(value.outcome);
                }
            }
            Err(ProviderError::Indeterminate(key))
        }
        .await,
    )
}

pub(super) fn decode_recovered(
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
        ResultKind::Forked(value) => settled_fork(&decode_fork_admissions(&value)?, outcome),
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

pub(super) fn settled_change(
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

pub(super) fn settled_checkpoint(
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

/// One checked Fork or ForkMachine admission: every child is an ordinary
/// machine admission under the shared operation and one provenance.
pub(super) struct ForkAdmitted {
    pub(super) operation: OperationId,
    pub(super) provenance: Provenance,
    pub(super) children: Vec<(MachineId, MachineContract)>,
}

pub(super) fn decode_fork_admissions(
    value: &wire::MachineAdmissions,
) -> Result<ForkAdmitted, ProviderError> {
    let operation = decode_operation(value.operation.as_ref())?;
    let Some(first) = value.machines.first().filter(|_| {
        u32::try_from(value.machines.len()).is_ok_and(|count| count <= MAX_FORK_CHILDREN)
    }) else {
        return Err(ProviderError::Rejected(
            "fork child set is outside the public bound".into(),
        ));
    };
    let provenance = decode_provenance(first.provenance.as_ref())?;
    let mut seen = BTreeSet::new();
    let mut children = Vec::with_capacity(value.machines.len());
    for admission in &value.machines {
        if decode_operation(admission.operation.as_ref())? != operation
            || decode_provenance(admission.provenance.as_ref())? != provenance
        {
            return Err(ProviderError::Rejected(
                "fork children do not share one operation and provenance".into(),
            ));
        }
        let child = decode_machine(admission.machine.as_ref())?;
        let contract = decode_contract(admission.contract.as_ref())?;
        let inherited = match provenance.source {
            ForkSource::Machine(source) => {
                child != source && contract.fork_fidelity() == Some(provenance.fidelity)
            }
            ForkSource::Checkpoint(checkpoint) => {
                provenance.fidelity == ForkFidelity::MemoryAndDisk
                    && contract.image == Image::Checkpoint(checkpoint)
            }
        };
        if !inherited {
            return Err(ProviderError::Rejected(
                "fork child contradicts its provenance".into(),
            ));
        }
        if !seen.insert(child) {
            return Err(ProviderError::Rejected(
                "fork returned duplicate children".into(),
            ));
        }
        children.push((child, contract));
    }
    Ok(ForkAdmitted {
        operation,
        provenance,
        children,
    })
}

fn decode_provenance(value: Option<&wire::Provenance>) -> Result<Provenance, ProviderError> {
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
    Ok(Provenance { source, fidelity })
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
    if admitted.provenance.source != source
        || u32::try_from(admitted.children.len()).ok() != Some(count.get())
    {
        return Err(ProviderError::Indeterminate(key));
    }
    let outcome = wait(provider, key, admitted.operation).await?;
    settled_fork(&admitted, outcome).map_err(|_| ProviderError::Indeterminate(key))
}

/// Settles a checked fork admission against its original terminal outcome.
pub(super) fn settled_fork(
    admitted: &ForkAdmitted,
    outcome: Option<wire::MutationOutcome>,
) -> Result<MutationOutcome, ProviderError> {
    let Some(wire::mutation_outcome::Result::Forked(value)) =
        outcome.and_then(|value| value.result)
    else {
        return Err(ProviderError::Rejected(
            "original fork result is missing".into(),
        ));
    };
    if decode_provenance(value.provenance.as_ref())? != admitted.provenance
        || value.machines.len() != admitted.children.len()
    {
        return Err(ProviderError::Rejected(
            "fork result was substituted".into(),
        ));
    }
    let machines = value
        .machines
        .into_iter()
        .zip(&admitted.children)
        .map(|(value, (expected, contract))| {
            let value = decode_machine_observation(value, *expected)?;
            if value.contract != *contract {
                return Err(ProviderError::Rejected(
                    "fork child contract was substituted".into(),
                ));
            }
            Ok(value)
        })
        .collect::<Result<_, _>>()?;
    Ok(MutationOutcome::Forked {
        provenance: admitted.provenance,
        machines,
    })
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
