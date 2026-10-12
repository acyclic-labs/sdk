use super::*;

pub(in super::super) fn decode_qualification(
    value: wire::ImageQualification,
    expected: &Image,
) -> Result<ImageQualification, ProviderError> {
    let image = decode_image(value.image.as_ref())?;
    if &image != expected {
        return Err(ProviderError::Rejected(
            "qualified image was substituted".into(),
        ));
    }
    let capabilities = value
        .capabilities
        .into_iter()
        .map(decode_capability)
        .collect::<Result<BTreeSet<_>, _>>()?;
    let compatibility_revision = digest(&value.compatibility_revision, "compatibility revision")?;
    Ok(ImageQualification {
        image,
        capabilities,
        compatibility_revision,
    })
}
pub(in super::super) fn decode_contract(
    value: Option<&wire::MachineContract>,
) -> Result<MachineContract, ProviderError> {
    let value =
        value.ok_or_else(|| ProviderError::Rejected("machine contract is missing".into()))?;
    let capabilities = value
        .capabilities
        .iter()
        .map(|item| decode_capability(*item))
        .collect::<Result<BTreeSet<_>, _>>()?;
    if capabilities.len() != value.capabilities.len() {
        return Err(ProviderError::Rejected(
            "capabilities contain duplicates".into(),
        ));
    }
    let budgets = value
        .budgets
        .as_ref()
        .ok_or_else(|| ProviderError::Rejected("budgets are missing".into()))?;
    Ok(MachineContract {
        image: decode_image(value.image.as_ref())?,
        compatibility: decode_compatibility(value.compatibility.as_ref(), &capabilities)?,
        capabilities,
        compatibility_revision: digest(&value.compatibility_revision, "compatibility revision")?,
        suspension: decode_suspension(value.suspension.as_ref())?,
        expiration: decode_expiration(value.expiration.as_ref())?,
        network_policy_digest: digest(&value.network_policy_digest, "network policy")?,
        budgets: Budgets {
            spend_micros: budgets.spend_micros,
            concurrency: budgets.concurrency,
        },
    })
}
pub(in super::super) fn digest(value: &[u8], name: &str) -> Result<[u8; 32], ProviderError> {
    let value: [u8; 32] = value
        .try_into()
        .map_err(|_| ProviderError::Rejected(format!("{name} digest width is invalid")))?;
    if value == [0; 32] {
        Err(ProviderError::Rejected(format!("{name} digest is zero")))
    } else {
        Ok(value)
    }
}
pub(in super::super) fn decode_machine_observation(
    value: wire::MachineState,
    expected: MachineId,
) -> Result<MachineObservation, ProviderError> {
    let id = decode_machine(value.machine.as_ref())?;
    if id != expected {
        return Err(ProviderError::Rejected(
            "machine identity was substituted".into(),
        ));
    }
    let state = match wire::MachineStatus::try_from(value.status)
        .map_err(|_| ProviderError::Rejected("machine state is invalid".into()))?
    {
        wire::MachineStatus::Starting => MachineState::Starting,
        wire::MachineStatus::Running => MachineState::Running,
        wire::MachineStatus::Suspending => MachineState::Suspending,
        wire::MachineStatus::Suspended => MachineState::Suspended,
        wire::MachineStatus::Waking => MachineState::Waking,
        wire::MachineStatus::Destroying => MachineState::Destroying,
        wire::MachineStatus::Destroyed => MachineState::Destroyed,
        wire::MachineStatus::Failed => MachineState::Failed,
        wire::MachineStatus::Indeterminate => MachineState::Indeterminate,
        wire::MachineStatus::Unspecified => {
            return Err(ProviderError::Rejected(
                "machine state is unspecified".into(),
            ));
        }
    };
    let mut names = BTreeSet::new();
    let mut endpoints = Vec::with_capacity(value.endpoints.len());
    for endpoint in value.endpoints {
        if endpoint.name.is_empty()
            || endpoint.uri.is_empty()
            || !names.insert(endpoint.name.clone())
        {
            return Err(ProviderError::Rejected(
                "machine endpoints are malformed".into(),
            ));
        }
        endpoints.push(Endpoint {
            name: endpoint.name,
            uri: endpoint.uri,
        });
    }
    if value.created_at_unix_ms == 0 || value.changed_at_unix_ms < value.created_at_unix_ms {
        return Err(ProviderError::Rejected(
            "machine timestamps are malformed".into(),
        ));
    }
    Ok(MachineObservation {
        id,
        state,
        contract: decode_contract(value.contract.as_ref())?,
        endpoints,
        last_checkpoint: value
            .last_checkpoint
            .as_ref()
            .map(|item| decode_checkpoint(Some(item)))
            .transpose()?,
        created_at_unix_ms: value.created_at_unix_ms,
        changed_at_unix_ms: value.changed_at_unix_ms,
    })
}
pub(in super::super) fn decode_checkpoint_observation(
    value: &wire::CheckpointState,
    expected: CheckpointId,
) -> Result<CheckpointObservation, ProviderError> {
    let id = decode_checkpoint(value.checkpoint.as_ref())?;
    if id != expected || value.created_at_unix_ms == 0 {
        return Err(ProviderError::Rejected(
            "checkpoint observation is malformed".into(),
        ));
    }
    Ok(CheckpointObservation {
        id,
        source: decode_machine(value.source.as_ref())?,
        contract: decode_contract(value.contract.as_ref())?,
        forkable: value.forkable,
        created_at_unix_ms: value.created_at_unix_ms,
    })
}
pub(in super::super) fn decode_event(
    value: &wire::MachineEvent,
    expected: MachineId,
) -> Result<MachineEvent, ProviderError> {
    let machine = decode_machine(value.machine.as_ref())?;
    if machine != expected || value.sequence == 0 || value.observed_at_unix_ms == 0 {
        return Err(ProviderError::Rejected(
            "event identity is malformed".into(),
        ));
    }
    let fact = match wire::EventKind::try_from(value.kind)
        .map_err(|_| ProviderError::Rejected("event kind is invalid".into()))?
    {
        wire::EventKind::State => EventFact::State(decode_machine_state(value.state)?),
        wire::EventKind::Pressure => EventFact::Pressure(
            match wire::PressureKind::try_from(value.pressure)
                .map_err(|_| ProviderError::Rejected("pressure kind is invalid".into()))?
            {
                wire::PressureKind::CustomerBudget => Pressure::CustomerBudget,
                wire::PressureKind::MachineLimit => Pressure::MachineLimit,
                wire::PressureKind::ServiceSaturation => Pressure::ServiceSaturation,
                wire::PressureKind::Unspecified => {
                    return Err(ProviderError::Rejected("pressure is unspecified".into()));
                }
            },
        ),
        wire::EventKind::Capacity => EventFact::CapacityChanged,
        wire::EventKind::Unspecified => {
            return Err(ProviderError::Rejected("event kind is unspecified".into()));
        }
    };
    Ok(MachineEvent {
        machine,
        sequence: value.sequence,
        observed_at_unix_ms: value.observed_at_unix_ms,
        fact,
    })
}
pub(in super::super) fn decode_machine_state(value: i32) -> Result<MachineState, ProviderError> {
    match wire::MachineStatus::try_from(value)
        .map_err(|_| ProviderError::Rejected("machine state is invalid".into()))?
    {
        wire::MachineStatus::Starting => Ok(MachineState::Starting),
        wire::MachineStatus::Running => Ok(MachineState::Running),
        wire::MachineStatus::Suspending => Ok(MachineState::Suspending),
        wire::MachineStatus::Suspended => Ok(MachineState::Suspended),
        wire::MachineStatus::Waking => Ok(MachineState::Waking),
        wire::MachineStatus::Destroying => Ok(MachineState::Destroying),
        wire::MachineStatus::Destroyed => Ok(MachineState::Destroyed),
        wire::MachineStatus::Failed => Ok(MachineState::Failed),
        wire::MachineStatus::Indeterminate => Ok(MachineState::Indeterminate),
        wire::MachineStatus::Unspecified => Err(ProviderError::Rejected(
            "machine state is unspecified".into(),
        )),
    }
}
pub(in super::super) fn decode_operation_observation(
    value: &wire::OperationState,
    expected: OperationId,
) -> Result<OperationObservation, ProviderError> {
    let id = decode_operation(value.operation.as_ref())?;
    if id != expected {
        return Err(ProviderError::Rejected(
            "operation identity was substituted".into(),
        ));
    }
    let phase = match wire::OperationStatus::try_from(value.status)
        .map_err(|_| ProviderError::Rejected("operation status is invalid".into()))?
    {
        wire::OperationStatus::Pending => OperationPhase::Pending,
        wire::OperationStatus::Succeeded => OperationPhase::Succeeded,
        wire::OperationStatus::Cancelled => OperationPhase::Cancelled,
        wire::OperationStatus::Indeterminate => OperationPhase::Indeterminate,
        wire::OperationStatus::Failed => OperationPhase::Failed,
        wire::OperationStatus::Unspecified => {
            return Err(ProviderError::Rejected(
                "operation status is unspecified".into(),
            ));
        }
    };
    Ok(OperationObservation { id, phase })
}
pub(in super::super) fn decode_usage_receipt(
    value: wire::UsageReceipt,
    machine: MachineId,
    start_unix_ms: u64,
    end_unix_ms: u64,
) -> Result<UsageReceipt, ProviderError> {
    let lineage_receipt_sha256: [u8; 32] = value
        .lineage_receipt_sha256
        .as_slice()
        .try_into()
        .map_err(|_| ProviderError::Rejected("lineage receipt identity is malformed".into()))?;
    if decode_machine(value.machine.as_ref())? != machine
        || value.start_unix_ms != start_unix_ms
        || value.end_unix_ms != end_unix_ms
        || value.receipt.is_empty()
        || lineage_receipt_sha256 == [0; 32]
    {
        return Err(ProviderError::Rejected("usage receipt is malformed".into()));
    }
    Ok(UsageReceipt {
        machine,
        start_unix_ms,
        end_unix_ms,
        elastic_cpu_ns: value.elastic_cpu_ns,
        dedicated_cpu_ns: value.dedicated_cpu_ns,
        private_resident_byte_seconds: value.private_resident_byte_seconds,
        durable_private_bytes: value.durable_private_bytes,
        lineage_receipt_sha256,
        egress_bytes: value.egress_bytes,
        receipt: value.receipt,
    })
}
