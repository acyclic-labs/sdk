//! A complete in-memory provider profile.

use acyclic_fs::{ProviderObjectStore, StreamAuthorityStore};
use acyclic_inference::DeterministicInference;
use acyclic_machines::SimulatedMachines;
use acyclic_objects::{MemoryObjects, ObjectsProvider};
use acyclic_stream::MemoryStream;
use std::sync::Arc;

/// All reference providers needed to run the local harness.
#[derive(Clone)]
pub struct MemoryProfile {
    /// In-memory filesystem provider.
    pub filesystem:
        acyclic_fs::Fs<StreamAuthorityStore<MemoryStream>, ProviderObjectStore<MemoryObjects>>,
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
    /// Creates one profile whose filesystem consumes these same public
    /// in-memory Stream and Objects providers.
    pub async fn new() -> Result<Self, acyclic_objects::ObjectsError> {
        let stream = MemoryStream::default();
        let objects = MemoryObjects::default();
        let bucket = objects
            .create_bucket(
                "filesystem".to_owned(),
                Some("filesystem-bucket-v1".to_owned()),
            )
            .await?
            .bucket
            .ok_or(acyclic_objects::ObjectsError::Invalid(
                "missing bucket identity",
            ))?;
        let filesystem = acyclic_fs::Fs::from_public_primitives(
            Arc::new(stream.clone()),
            Arc::new(objects.clone()),
            bucket,
            acyclic_fs::EmbeddedCapabilities::MEMORY,
        );
        Ok(Self {
            filesystem,
            stream,
            objects,
            machines: SimulatedMachines::default(),
            inference: DeterministicInference::default(),
        })
    }
}
