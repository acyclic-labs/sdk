use crate::{CheckpointId, CreateMachine, MachineId, SuspensionPolicy};
use serde::Serialize;

/// Canonical mutation intent used to bind idempotency keys across adapters.
#[derive(Clone, Debug, Serialize)]
pub enum MutationIntent {
    Create(CreateMachine),
    Checkpoint(MachineId),
    Fork(CheckpointId, u32),
    ForkMachine(MachineId, u32),
    Suspend(MachineId),
    Wake(MachineId),
    Policy(MachineId, SuspensionPolicy),
    DestroyMachine(MachineId),
    DestroyCheckpoint(CheckpointId),
}
