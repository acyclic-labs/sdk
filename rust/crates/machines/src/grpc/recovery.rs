use super::*;

pub(super) async fn recovered_admission(
    provider: &GrpcProvider,
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
    let operation = validate_recovered_admission(&value)?;
    Ok((operation, value))
}

impl GrpcProvider {
    /// Join the admitted operation once. Retry this observation with the same key
    /// after interruption; neither losing this reply nor dropping it cancels work.
    #[tracing::instrument(name = "acyclic.machines.join", skip_all,
        fields(outcome = obs::Empty, rpc.code = obs::Empty, error.kind = obs::Empty))]
    pub async fn join(
        &self,
        key: IdempotencyKey,
        operation: OperationId,
    ) -> Result<SettledMutation, ProviderError> {
        obs::finish(
            async {
                let value = self
                    .client()
                    .await
                    .map_err(|_| ProviderError::Indeterminate(key))?
                    .join(wire::JoinRequest {
                        protocol: Some(protocol()),
                        idempotency_key: Some(encode_key(key)),
                        operation: operation_request(operation).operation,
                    })
                    .await
                    .map_err(|error| watch_error(key, &error))?
                    .into_inner();
                decode_settlement(value, operation)?.result(key)
            }
            .await,
        )
    }
}

pub(super) async fn finish_admission(
    provider: &GrpcProvider,
    key: IdempotencyKey,
    admission: wire::RecoveredAdmission,
) -> Result<MutationOutcome, ProviderError> {
    let operation = validate_recovered_admission(&admission)?;
    // Validate the admitted child set before any join or per-child work.
    match &admission.result {
        Some(wire::recovered_admission::Result::Fork(value)) => {
            fork_children(&value.children, None)?;
        }
        Some(wire::recovered_admission::Result::ForkMachine(value)) => {
            decode_fork_machine_admission(value)?;
        }
        _ => {}
    }
    let settled = provider.join(key, operation).await?;
    validate_settled_admission(&admission, &settled.outcome)?;
    Ok(settled.outcome)
}

fn validate_settled_admission(
    admission: &wire::RecoveredAdmission,
    outcome: &MutationOutcome,
) -> Result<(), ProviderError> {
    use wire::recovered_admission::Result as Kind;
    let matches = match (admission.result.as_ref(), outcome) {
        (Some(Kind::Create(value)), MutationOutcome::Created(machine)) => {
            machine.id == decode_machine(value.machine.as_ref())?
                && machine.contract == decode_contract(value.contract.as_ref())?
        }
        (Some(Kind::Checkpoint(value)), MutationOutcome::Checkpointed(checkpoint)) => {
            checkpoint.id == decode_checkpoint(value.checkpoint.as_ref())?
                && checkpoint.source == decode_machine(value.source.as_ref())?
                && checkpoint.contract == decode_contract(value.contract.as_ref())?
        }
        (Some(Kind::Fork(value)), MutationOutcome::Forked(children)) => {
            let contract = decode_contract(value.contract.as_ref())?;
            children
                .iter()
                .map(|child| child.id)
                .eq(fork_children(&value.children, None)?)
                && children.iter().all(|child| child.contract == contract)
        }
        (
            Some(Kind::ForkMachine(value)),
            MutationOutcome::MachineForked {
                source,
                fidelity,
                children,
            },
        ) => {
            let (expected, expected_fidelity, ids, _) = decode_fork_machine_admission(value)?;
            let contract = decode_contract(value.contract.as_ref())?;
            *source == expected
                && *fidelity == expected_fidelity
                && children.iter().map(|child| child.id).eq(ids)
                && children.iter().all(|child| child.contract == contract)
        }
        (Some(Kind::Suspend(value)), MutationOutcome::Suspended(machine))
        | (Some(Kind::Wake(value)), MutationOutcome::Woken(machine))
        | (Some(Kind::DestroyMachine(value)), MutationOutcome::MachineDestroyed(machine)) => {
            *machine == decode_machine(value.machine.as_ref())?
        }
        (
            Some(Kind::DestroyCheckpoint(value)),
            MutationOutcome::CheckpointDestroyed(checkpoint),
        ) => *checkpoint == decode_checkpoint(value.checkpoint.as_ref())?,
        (
            Some(Kind::SetSuspensionPolicy(value)),
            MutationOutcome::SuspensionPolicySet(machine, policy),
        ) => {
            *machine == decode_machine(value.machine.as_ref())?
                && *policy == decode_suspension(value.policy.as_ref())?
        }
        _ => false,
    };
    if matches {
        Ok(())
    } else {
        Err(ProviderError::Rejected("settled result contradicts its admitted mutation; recovery: retain the original key and repair the server settlement binding".into()))
    }
}

pub(super) fn decode_fork_machine_admission(
    value: &wire::ForkMachineAdmission,
) -> Result<(MachineId, ForkFidelity, Vec<MachineId>, OperationId), ProviderError> {
    let source = decode_machine(value.source.as_ref())?;
    let operation = decode_operation(value.operation.as_ref())?;
    let contract = decode_contract(value.contract.as_ref())?;
    let fidelity = decode_fidelity(value.fidelity)?;
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
