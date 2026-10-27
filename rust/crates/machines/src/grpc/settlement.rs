//! Canonical Machines result encoding, shared by servers and clients.
use super::*;

/// Encode the original canonical publication; never inspect current resources here.
pub fn encode_settlement(
    value: &OperationSettlement,
) -> Result<wire::OperationSettlement, ProviderError> {
    let status = match (value.operation.phase, value.outcome.is_some()) {
        (OperationPhase::Succeeded, true) => wire::OperationStatus::Succeeded,
        (OperationPhase::Failed, false) => wire::OperationStatus::Failed,
        (OperationPhase::Cancelled, false) => wire::OperationStatus::Cancelled,
        _ => return Err(invalid_settlement()),
    };
    digest(value.settlement.as_bytes(), "settlement commit")?;
    Ok(wire::OperationSettlement {
        operation: Some(wire::OperationState {
            operation: operation_request(value.operation.id).operation,
            status: status as i32,
        }),
        settlement_commit_id: value.settlement.as_bytes().to_vec(),
        outcome: value.outcome.as_ref().map(encode_outcome).transpose()?,
    })
}

/// Validate the admitted operation identity and exact terminal result.
pub fn decode_settlement(
    value: wire::OperationSettlement,
    expected: OperationId,
) -> Result<OperationSettlement, ProviderError> {
    let operation = decode_operation_observation(
        value.operation.as_ref().ok_or_else(invalid_settlement)?,
        expected,
    )?;
    let settlement = acyclic_stream::CommitId::from_bytes(digest(
        &value.settlement_commit_id,
        "settlement commit",
    )?);
    match (operation.phase, value.outcome.is_some()) {
        (OperationPhase::Succeeded, true)
        | (OperationPhase::Failed | OperationPhase::Cancelled, false) => {}
        _ => return Err(invalid_settlement()),
    }
    Ok(OperationSettlement {
        operation,
        settlement,
        outcome: value.outcome.map(decode_outcome).transpose()?,
    })
}

fn invalid_settlement() -> ProviderError {
    ProviderError::Rejected("operation settlement is missing or contradictory; recovery: repair the canonical settlement encoding".into())
}

pub(super) fn encode_outcome(
    value: &MutationOutcome,
) -> Result<wire::MutationOutcome, ProviderError> {
    use wire::mutation_outcome::Result as Kind;
    let result = match value {
        MutationOutcome::Created(value) => Kind::Created(encode_observation(value)?),
        MutationOutcome::Checkpointed(value) => {
            Kind::Checkpointed(encode_checkpoint_observation(value)?)
        }
        MutationOutcome::Forked(values) => Kind::Forked(wire::ForkedMachines {
            machines: encode_children(values)?,
        }),
        MutationOutcome::MachineForked {
            source,
            fidelity,
            children,
        } => Kind::MachineForked(wire::ForkedLiveMachines {
            source: Some(encode_machine(*source)),
            fidelity: encode_fidelity(*fidelity),
            children: encode_children(children)?,
        }),
        MutationOutcome::Suspended(id) => Kind::Suspended(encode_machine(*id)),
        MutationOutcome::Woken(id) => Kind::Woken(encode_machine(*id)),
        MutationOutcome::MachineDestroyed(id) => Kind::MachineDestroyed(encode_machine(*id)),
        MutationOutcome::CheckpointDestroyed(id) => {
            Kind::CheckpointDestroyed(encode_checkpoint(*id))
        }
        MutationOutcome::SuspensionPolicySet(id, policy) => {
            Kind::SuspensionPolicySet(wire::PolicySet {
                machine: Some(encode_machine(*id)),
                policy: Some(encode_suspension(*policy)?),
            })
        }
    };
    Ok(wire::MutationOutcome {
        result: Some(result),
    })
}

pub(super) fn decode_outcome(
    value: wire::MutationOutcome,
) -> Result<MutationOutcome, ProviderError> {
    use wire::mutation_outcome::Result as Kind;
    Ok(match value.result.ok_or_else(invalid_settlement)? {
        Kind::Created(value) => {
            let id = decode_machine(value.machine.as_ref())?;
            MutationOutcome::Created(decode_machine_observation(value, id)?)
        }
        Kind::Checkpointed(value) => {
            let id = decode_checkpoint(value.checkpoint.as_ref())?;
            MutationOutcome::Checkpointed(decode_checkpoint_observation(&value, id)?)
        }
        Kind::Forked(value) => MutationOutcome::Forked(decode_children(value.machines, None)?),
        Kind::MachineForked(value) => {
            let source = decode_machine(value.source.as_ref())?;
            let fidelity = decode_fidelity(value.fidelity)?;
            let children = decode_children(value.children, Some(source))?;
            if children
                .iter()
                .any(|child| child.contract.fork_fidelity() != Some(fidelity))
            {
                return Err(ProviderError::Rejected(
                    "fork fidelity contradicts its child contracts".into(),
                ));
            }
            MutationOutcome::MachineForked {
                source,
                fidelity,
                children,
            }
        }
        Kind::Suspended(value) => MutationOutcome::Suspended(decode_machine(Some(&value))?),
        Kind::Woken(value) => MutationOutcome::Woken(decode_machine(Some(&value))?),
        Kind::MachineDestroyed(value) => {
            MutationOutcome::MachineDestroyed(decode_machine(Some(&value))?)
        }
        Kind::CheckpointDestroyed(value) => {
            MutationOutcome::CheckpointDestroyed(decode_checkpoint(Some(&value))?)
        }
        Kind::SuspensionPolicySet(value) => MutationOutcome::SuspensionPolicySet(
            decode_machine(value.machine.as_ref())?,
            decode_suspension(value.policy.as_ref())?,
        ),
    })
}

fn encode_children(
    values: &[MachineObservation],
) -> Result<Vec<wire::MachineState>, ProviderError> {
    values.iter().map(encode_observation).collect()
}
fn decode_children(
    values: Vec<wire::MachineState>,
    source: Option<MachineId>,
) -> Result<Vec<MachineObservation>, ProviderError> {
    let ids = values
        .iter()
        .map(|value| value.machine.clone().ok_or_else(invalid_settlement))
        .collect::<Result<Vec<_>, _>>()?;
    let ids = fork_children(&ids, source)?;
    values
        .into_iter()
        .zip(ids)
        .map(|(value, id)| decode_machine_observation(value, id))
        .collect()
}

pub(super) fn encode_fidelity(value: ForkFidelity) -> i32 {
    (match value {
        ForkFidelity::MemoryAndDisk => wire::ForkFidelity::MemoryAndDisk,
        ForkFidelity::DiskOnly => wire::ForkFidelity::DiskOnly,
    }) as i32
}
pub(super) fn decode_fidelity(value: i32) -> Result<ForkFidelity, ProviderError> {
    match wire::ForkFidelity::try_from(value) {
        Ok(wire::ForkFidelity::MemoryAndDisk) => Ok(ForkFidelity::MemoryAndDisk),
        Ok(wire::ForkFidelity::DiskOnly) => Ok(ForkFidelity::DiskOnly),
        _ => Err(ProviderError::Rejected(
            "fork fidelity is invalid or unspecified".into(),
        )),
    }
}
fn encode_contract(value: &MachineContract) -> Result<wire::MachineContract, ProviderError> {
    Ok(wire::MachineContract {
        image: Some(encode_image(&value.image)?),
        capabilities: value
            .capabilities
            .iter()
            .copied()
            .map(encode_capability)
            .collect(),
        compatibility: Some(encode_compatibility(&value.compatibility)?),
        compatibility_revision: value.compatibility_revision.to_vec(),
        suspension: Some(encode_suspension(value.suspension)?),
        expiration: Some(encode_expiration(value.expiration)?),
        network_policy_digest: value.network_policy_digest.to_vec(),
        budgets: Some(wire::Budgets {
            spend_micros: value.budgets.spend_micros,
            concurrency: value.budgets.concurrency,
        }),
    })
}
fn encode_observation(value: &MachineObservation) -> Result<wire::MachineState, ProviderError> {
    let status = match value.state {
        MachineState::Starting => wire::MachineStatus::Starting,
        MachineState::Running => wire::MachineStatus::Running,
        MachineState::Suspending => wire::MachineStatus::Suspending,
        MachineState::Suspended => wire::MachineStatus::Suspended,
        MachineState::Waking => wire::MachineStatus::Waking,
        MachineState::Destroying => wire::MachineStatus::Destroying,
        MachineState::Destroyed => wire::MachineStatus::Destroyed,
        MachineState::Failed => wire::MachineStatus::Failed,
        MachineState::Indeterminate => wire::MachineStatus::Indeterminate,
    };
    Ok(wire::MachineState {
        machine: Some(encode_machine(value.id)),
        status: status as i32,
        contract: Some(encode_contract(&value.contract)?),
        endpoints: value
            .endpoints
            .iter()
            .map(|value| wire::Endpoint {
                name: value.name.clone(),
                uri: value.uri.clone(),
            })
            .collect(),
        last_checkpoint: value.last_checkpoint.map(encode_checkpoint),
        created_at_unix_ms: value.created_at_unix_ms,
        changed_at_unix_ms: value.changed_at_unix_ms,
    })
}
fn encode_checkpoint_observation(
    value: &CheckpointObservation,
) -> Result<wire::CheckpointState, ProviderError> {
    Ok(wire::CheckpointState {
        checkpoint: Some(encode_checkpoint(value.id)),
        source: Some(encode_machine(value.source)),
        contract: Some(encode_contract(&value.contract)?),
        forkable: value.forkable,
        created_at_unix_ms: value.created_at_unix_ms,
    })
}
