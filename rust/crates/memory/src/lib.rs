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

#[cfg(test)]
mod tests {
    use super::MemoryProfile;
    use acyclic_objects::{ObjectsProvider as _, ReadTarget};
    use acyclic_stream::{ChildrenRequest, StreamProvider as _};
    use bytes::Bytes;
    use futures::StreamExt as _;

    #[tokio::test]
    async fn filesystem_uses_the_profiles_exact_public_provider_instances()
    -> Result<(), Box<dyn std::error::Error>> {
        let profile = MemoryProfile::new();
        let workspace = profile.filesystem.create_workspace("shared-state").await?;
        workspace
            .write("/answer", Bytes::from_static(b"42"))
            .await?;

        assert_eq!(
            workspace.read("/answer", 2).await?,
            Bytes::from_static(b"42")
        );
        let mut stream_children = profile
            .stream
            .children(ChildrenRequest {
                parent: None,
                limit: 16,
            })
            .await?;
        assert!(stream_children.next().await.transpose()?.is_some());
        let objects = profile
            .objects
            .list(
                ReadTarget::Bucket(profile.filesystem_bucket),
                "fs/v1/".to_owned(),
                None,
                false,
                128,
                None,
            )
            .await?;
        assert!(!objects.entries.is_empty());
        Ok(())
    }
}
