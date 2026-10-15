#![cfg_attr(
    test,
    allow(
        clippy::indexing_slicing,
        clippy::too_many_lines,
        reason = "adapter tests run ordered scenarios over fixtures they built"
    )
)]
#![doc = include_str!("../../docs/machines.md")]

use crate::{
    Error, IdempotencyKey, OperationId, Result,
    fork::{
        Capture, CapturedResource, ForkCaptureProvider, ForkRequest, ForkSeed, ForkSeedVerifier,
        ForkSelection, ResourceRevision,
    },
    resources::{ArtifactRef, CheckpointRef, ProviderRef, SandboxRef},
};
use acyclic_machines::{
    CheckpointId, CheckpointObservation, CreateMachine, IdempotencyKey as MachinesKey, Image,
    ImageQualification, MachineId, MachineObservation, MachinesProvider, MutationOutcome,
    OperationId as MachinesOperationId, OperationObservation, ProviderError,
};
use acyclic_stream::BoxProviderFuture as BoxFuture;
use std::{num::NonZeroU32, sync::Arc};

mod fork;
mod host;
mod identity;
pub use host::MachinesHost;
use identity::{machines_key, map_error, parse_uuid};
#[cfg(test)]
mod tests;
