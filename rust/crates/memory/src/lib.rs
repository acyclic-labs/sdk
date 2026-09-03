//! A complete in-memory provider profile.

use acyclic_filesystem::MemoryFilesystem;
use acyclic_inference::DeterministicInference;
use acyclic_machines::SimulatedMachines;
use acyclic_objects::MemoryObjects;
use acyclic_stream::MemoryStream;

/// All reference providers needed to run the local harness.
#[derive(Clone, Default)]
pub struct MemoryProfile {
    /// In-memory filesystem provider.
    pub filesystem: MemoryFilesystem,
    /// In-memory ordered stream provider.
    pub stream: MemoryStream,
    /// In-memory immutable objects provider.
    pub objects: MemoryObjects,
    /// Deterministic execution simulator.
    pub machines: SimulatedMachines,
    /// Deterministic model provider.
    pub inference: DeterministicInference,
}
