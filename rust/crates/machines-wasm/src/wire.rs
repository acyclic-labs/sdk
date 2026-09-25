//! Generated protobuf, not serde JSON, is the simulator's result boundary.

use acyclic_machines::{
    self as domain, Budgets, Capability, CheckpointId, CheckpointObservation, CompatibilityPolicy,
    EventFact, ExpirationPolicy, Image, ImageQualification, MachineContract, MachineId,
    MachineObservation, MutationOutcome, OperationId, OperationObservation, Performance, Pressure,
    ProviderError, SuspensionPolicy, UsageReceipt, wire,
};
use prost::Message as _;

fn millis(value: std::time::Duration) -> Result<u64, ProviderError> {
    u64::try_from(value.as_millis())
        .map_err(|_| ProviderError::Invalid("duration exceeds protocol range".into()))
}

fn machine_id(value: MachineId) -> wire::MachineId {
    wire::MachineId {
        value: value.as_bytes().to_vec(),
    }
}

fn checkpoint_id(value: CheckpointId) -> wire::CheckpointId {
    wire::CheckpointId {
        value: value.as_bytes().to_vec(),
    }
}

fn operation_id(value: OperationId) -> wire::OperationId {
    wire::OperationId {
        value: value.as_bytes().to_vec(),
    }
}

fn image(value: &Image) -> wire::Image {
    use wire::image::ImmutableReference;
    let (kind, immutable_reference) = match value {
        Image::ManagedOci(digest) => (
            wire::ImageKind::ManagedOci,
            ImmutableReference::ManagedDigest(digest.as_bytes().to_vec()),
        ),
        Image::Custom(digest) => (
            wire::ImageKind::Custom,
            ImmutableReference::CustomDigest(digest.as_bytes().to_vec()),
        ),
        Image::Checkpoint(checkpoint) => (
            wire::ImageKind::Checkpoint,
            ImmutableReference::Checkpoint(checkpoint_id(*checkpoint)),
        ),
    };
    wire::Image {
        kind: kind as i32,
        immutable_reference: Some(immutable_reference),
    }
}

fn capability(value: Capability) -> i32 {
    (match value {
        Capability::ElasticCpu => wire::Capability::ElasticCpu,
        Capability::ElasticMemory => wire::Capability::ElasticMemory,
        Capability::LiveCheckpoint => wire::Capability::LiveCheckpoint,
        Capability::LiveFork => wire::Capability::LiveFork,
        Capability::SuspendResume => wire::Capability::SuspendResume,
        Capability::LiveMovement => wire::Capability::LiveMovement,
    }) as i32
}

fn compatibility(value: &CompatibilityPolicy) -> wire::CompatibilityPolicy {
    match value {
        CompatibilityPolicy::BestEffort => wire::CompatibilityPolicy {
            mode: wire::CompatibilityMode::BestEffort as i32,
            required: Vec::new(),
        },
        CompatibilityPolicy::Require(required) => wire::CompatibilityPolicy {
            mode: wire::CompatibilityMode::Require as i32,
            required: required.iter().copied().map(capability).collect(),
        },
    }
}

fn performance(value: Performance) -> i32 {
    (match value {
        Performance::Elastic => wire::Performance::Elastic,
        Performance::Dedicated => wire::Performance::Dedicated,
    }) as i32
}

fn suspension(value: SuspensionPolicy) -> Result<wire::SuspensionPolicy, ProviderError> {
    use wire::suspension_policy::Policy;
    Ok(wire::SuspensionPolicy {
        policy: Some(match value {
            SuspensionPolicy::Manual => Policy::Manual(true),
            SuspensionPolicy::AfterIdle(duration) => Policy::AfterIdleMs(millis(duration)?),
        }),
    })
}

fn expiration(value: ExpirationPolicy) -> Result<wire::ExpirationPolicy, ProviderError> {
    let (kind, value_ms) = match value {
        ExpirationPolicy::Never => (wire::ExpirationKind::Never, 0),
        ExpirationPolicy::MaxAge(duration) => (wire::ExpirationKind::MaxAge, millis(duration)?),
        ExpirationPolicy::AtUnixMs(value) => (wire::ExpirationKind::At, value),
        ExpirationPolicy::Idle(duration) => (wire::ExpirationKind::Idle, millis(duration)?),
    };
    Ok(wire::ExpirationPolicy {
        kind: kind as i32,
        value_ms,
    })
}

fn contract(value: &MachineContract) -> Result<wire::MachineContract, ProviderError> {
    let Budgets {
        spend_micros,
        concurrency,
    } = value.budgets;
    Ok(wire::MachineContract {
        image: Some(image(&value.image)),
        capabilities: value.capabilities.iter().copied().map(capability).collect(),
        compatibility: Some(compatibility(&value.compatibility)),
        compatibility_revision: value.compatibility_revision.to_vec(),
        performance: performance(value.performance),
        suspension: Some(suspension(value.suspension)?),
        expiration: Some(expiration(value.expiration)?),
        network_policy_digest: value.network_policy_digest.to_vec(),
        budgets: Some(wire::Budgets {
            spend_micros,
            concurrency,
        }),
    })
}

fn machine_state(value: &MachineObservation) -> Result<wire::MachineState, ProviderError> {
    let status = match value.state {
        domain::MachineState::Starting => wire::MachineStatus::Starting,
        domain::MachineState::Running => wire::MachineStatus::Running,
        domain::MachineState::Suspending => wire::MachineStatus::Suspending,
        domain::MachineState::Suspended => wire::MachineStatus::Suspended,
        domain::MachineState::Waking => wire::MachineStatus::Waking,
        domain::MachineState::Destroying => wire::MachineStatus::Destroying,
        domain::MachineState::Destroyed => wire::MachineStatus::Destroyed,
        domain::MachineState::Failed => wire::MachineStatus::Failed,
        domain::MachineState::Indeterminate => wire::MachineStatus::Indeterminate,
    };
    Ok(wire::MachineState {
        machine: Some(machine_id(value.id)),
        status: status as i32,
        contract: Some(contract(&value.contract)?),
        endpoints: value
            .endpoints
            .iter()
            .map(|endpoint| wire::Endpoint {
                name: endpoint.name.clone(),
                uri: endpoint.uri.clone(),
            })
            .collect(),
        last_checkpoint: value.last_checkpoint.map(checkpoint_id),
        created_at_unix_ms: value.created_at_unix_ms,
        changed_at_unix_ms: value.changed_at_unix_ms,
    })
}

fn checkpoint_state(value: &CheckpointObservation) -> Result<wire::CheckpointState, ProviderError> {
    Ok(wire::CheckpointState {
        checkpoint: Some(checkpoint_id(value.id)),
        source: Some(machine_id(value.source)),
        contract: Some(contract(&value.contract)?),
        forkable: value.forkable,
        created_at_unix_ms: value.created_at_unix_ms,
    })
}

fn operation_state(value: OperationObservation) -> wire::OperationState {
    let status = match value.phase {
        domain::OperationPhase::Pending => wire::OperationStatus::Pending,
        domain::OperationPhase::Succeeded => wire::OperationStatus::Succeeded,
        domain::OperationPhase::Cancelled => wire::OperationStatus::Cancelled,
        domain::OperationPhase::Indeterminate => wire::OperationStatus::Indeterminate,
        domain::OperationPhase::Failed => wire::OperationStatus::Failed,
    };
    wire::OperationState {
        operation: Some(operation_id(value.id)),
        status: status as i32,
    }
}

fn event(value: &domain::MachineEvent) -> wire::MachineEvent {
    let (kind, state, pressure) = match value.fact {
        EventFact::State(state) => {
            let status = match state {
                domain::MachineState::Starting => wire::MachineStatus::Starting,
                domain::MachineState::Running => wire::MachineStatus::Running,
                domain::MachineState::Suspending => wire::MachineStatus::Suspending,
                domain::MachineState::Suspended => wire::MachineStatus::Suspended,
                domain::MachineState::Waking => wire::MachineStatus::Waking,
                domain::MachineState::Destroying => wire::MachineStatus::Destroying,
                domain::MachineState::Destroyed => wire::MachineStatus::Destroyed,
                domain::MachineState::Failed => wire::MachineStatus::Failed,
                domain::MachineState::Indeterminate => wire::MachineStatus::Indeterminate,
            };
            (
                wire::EventKind::State,
                status,
                wire::PressureKind::Unspecified,
            )
        }
        EventFact::Pressure(pressure) => {
            let kind = match pressure {
                Pressure::CustomerBudget => wire::PressureKind::CustomerBudget,
                Pressure::MachineLimit => wire::PressureKind::MachineLimit,
                Pressure::ServiceSaturation => wire::PressureKind::ServiceSaturation,
            };
            (
                wire::EventKind::Pressure,
                wire::MachineStatus::Unspecified,
                kind,
            )
        }
        EventFact::CapacityChanged => (
            wire::EventKind::Capacity,
            wire::MachineStatus::Unspecified,
            wire::PressureKind::Unspecified,
        ),
    };
    wire::MachineEvent {
        machine: Some(machine_id(value.machine)),
        sequence: value.sequence,
        observed_at_unix_ms: value.observed_at_unix_ms,
        kind: kind as i32,
        state: state as i32,
        pressure: pressure as i32,
    }
}

pub fn qualification(value: ImageQualification) -> Vec<u8> {
    wire::ImageQualification {
        image: Some(image(&value.image)),
        capabilities: value.capabilities.into_iter().map(capability).collect(),
        compatibility_revision: value.compatibility_revision.to_vec(),
    }
    .encode_to_vec()
}

pub fn machine(value: MachineObservation) -> Result<Vec<u8>, ProviderError> {
    Ok(machine_state(&value)?.encode_to_vec())
}

pub fn machines(value: domain::MachinePage) -> Result<Vec<u8>, ProviderError> {
    Ok(wire::MachinePage {
        machines: value
            .machines
            .iter()
            .map(machine_state)
            .collect::<Result<_, _>>()?,
        next: value.next.map(machine_id),
    }
    .encode_to_vec())
}

pub fn checkpoint(value: CheckpointObservation) -> Result<Vec<u8>, ProviderError> {
    Ok(checkpoint_state(&value)?.encode_to_vec())
}

pub fn mutation(value: MutationOutcome) -> Result<Vec<u8>, ProviderError> {
    use wire::mutation_outcome::Result as Outcome;
    let result = match value {
        MutationOutcome::Created(value) => Outcome::Created(machine_state(&value)?),
        MutationOutcome::Checkpointed(value) => Outcome::Checkpointed(checkpoint_state(&value)?),
        MutationOutcome::Forked(values) => Outcome::Forked(wire::ForkedMachines {
            machines: values.iter().map(machine_state).collect::<Result<_, _>>()?,
        }),
        MutationOutcome::Suspended(value) => Outcome::Suspended(machine_id(value)),
        MutationOutcome::Woken(value) => Outcome::Woken(machine_id(value)),
        MutationOutcome::SuspensionPolicySet(id, policy) => {
            Outcome::SuspensionPolicySet(wire::PolicySet {
                machine: Some(machine_id(id)),
                policy: Some(suspension(policy)?),
            })
        }
        MutationOutcome::MachineDestroyed(value) => Outcome::MachineDestroyed(machine_id(value)),
        MutationOutcome::CheckpointDestroyed(value) => {
            Outcome::CheckpointDestroyed(checkpoint_id(value))
        }
    };
    Ok(wire::MutationOutcome {
        result: Some(result),
    }
    .encode_to_vec())
}

pub fn events(value: domain::EventPage) -> Vec<u8> {
    wire::EventPage {
        events: value.events.iter().map(event).collect(),
        next_sequence: value.next_sequence.unwrap_or_default(),
    }
    .encode_to_vec()
}

#[allow(
    deprecated,
    reason = "v1 protobuf requires the zero-valued wire tombstone"
)]
pub fn usage(value: UsageReceipt) -> Vec<u8> {
    wire::UsageReceipt {
        machine: Some(machine_id(value.machine)),
        start_unix_ms: value.start_unix_ms,
        end_unix_ms: value.end_unix_ms,
        elastic_cpu_ns: value.elastic_cpu_ns,
        dedicated_cpu_ns: value.dedicated_cpu_ns,
        private_resident_byte_seconds: value.private_resident_byte_seconds,
        durable_private_bytes: value.durable_private_bytes,
        lineage_shared_bytes: 0,
        lineage_receipt_sha256: value.lineage_receipt_sha256.to_vec(),
        egress_bytes: value.egress_bytes,
        receipt: value.receipt,
    }
    .encode_to_vec()
}

pub fn operation(value: OperationObservation) -> Vec<u8> {
    operation_state(value).encode_to_vec()
}

pub fn operations(values: Vec<OperationObservation>) -> Vec<u8> {
    wire::OperationPage {
        operations: values.into_iter().map(operation_state).collect(),
    }
    .encode_to_vec()
}

pub fn recovered_operation(value: OperationId) -> Vec<u8> {
    operation_id(value).encode_to_vec()
}
