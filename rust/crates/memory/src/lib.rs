//! A complete in-memory provider profile.

use acyclic_inference::DeterministicInference;
use acyclic_machines::SimulatedMachines;
use acyclic_objects::MemoryObjects;
use acyclic_stream::MemoryStream;

/// All reference providers needed to run the local harness.
#[derive(Clone)]
pub struct MemoryProfile {
    /// In-memory filesystem provider.
    pub filesystem: acyclic_fs::Fs<acyclic_fs::MemoryAuthorityStore, acyclic_fs::MemoryObjectStore>,
    /// In-memory ordered stream provider.
    pub stream: MemoryStream,
    /// In-memory immutable objects provider.
    pub objects: MemoryObjects,
    /// Deterministic execution simulator.
    pub machines: SimulatedMachines,
    /// Deterministic model provider.
    pub inference: DeterministicInference,
}

impl MemoryProfile {
    /// Creates one profile from each family's deterministic memory provider.
    #[must_use]
    pub fn new() -> Self {
        let stream = MemoryStream::default();
        let objects = MemoryObjects::default();
        Self {
            filesystem: acyclic_fs::Fs::memory(),
            stream,
            objects,
            machines: SimulatedMachines::default(),
            inference: DeterministicInference::default(),
        }
    }
}

impl Default for MemoryProfile {
    fn default() -> Self {
        Self::new()
    }
}
