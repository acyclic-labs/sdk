//! A complete in-memory provider profile.

use acyclic_inference::DeterministicInference;
use acyclic_machines::SimulatedMachines;
use acyclic_objects::MemoryObjects;
use acyclic_stream::MemoryStream;
use std::sync::Arc;

/// All reference providers needed to run the local harness.
#[derive(Clone)]
pub struct MemoryProfile {
    /// In-memory filesystem provider.
    pub filesystem: acyclic_fs::MemoryFs,
    /// In-memory ordered stream provider.
    pub stream: MemoryStream,
    /// In-memory immutable objects provider.
    pub objects: MemoryObjects,
    /// Bucket used by the filesystem composition in `objects`.
    pub filesystem_bucket: acyclic_objects::wire::BucketRef,
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
        let (objects, filesystem_bucket) = MemoryObjects::with_default_bucket();
        let filesystem = acyclic_fs::Fs::from_memory_providers(
            Arc::new(stream.clone()),
            Arc::new(objects.clone()),
            filesystem_bucket.clone(),
        );
        Self {
            filesystem,
            stream,
            objects,
            filesystem_bucket,
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
