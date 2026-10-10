//! Selected resource metadata from the existing authenticated allocation receipts.
use super::*;
use crate::{conversation::{VolumeClass, VolumeRef}, resources::GenerationRef};

/// Original immutable project/scratch snapshot proved by a retained allocation.
/// This value grants no filesystem access and is not a mutable-head observation.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
#[cfg_attr(feature = "wasm", derive(tsify::Tsify))]
pub struct TaskVolumeSnapshot {
    /// Exact provider/owner volume identity authenticated by the creation receipt.
    #[cfg_attr(feature = "wasm", tsify(type = "WasmVolumeRefWire"))]
    pub volume: VolumeRef,
    /// Original allocation generation; later source edits require a new selection.
    #[cfg_attr(feature = "wasm", tsify(type = "WasmResourceRefWire"))]
    pub generation: GenerationRef,
}

impl<P: StreamProvider + 'static> CoordinatorTaskHost<P> {
    /// Returns only selected source metadata from fully authenticated creation
    /// receipts. No source content handles or grants are exposed to the reader.
    pub async fn observe_volume_snapshots(&self, task: TaskId) -> Result<Vec<TaskVolumeSnapshot>> {
        let admission = self.read_retained_admission(OperationId::from_bytes(task.into_bytes())).await?;
        let scope = self.authenticated_scope(&admission).await?;
        // The existing sealed scope has already resolved the nearest actor
        // origin. Never repeat that lineage choice over raw provider flags.
        let Some(current_private) = scope.current_created_private_volume()? else { return Ok(Vec::new()); };
        let mut current_origin = None;
        let mut snapshots = Vec::new();
        for proof in &admission.resource_proofs {
            // authenticated_scope() verifies the original provenance chain.
            // Reverification obtains its exact immutable generation, not a head.
            let preparation = self.read_retained_admission(OperationId::from_bytes(proof.preparation.into_bytes())).await?;
            let request: crate::resource_preparation::ResourcePreparationRequest =
                serde_json::from_value(preparation.input).map_err(|error| Error::Invalid(error.to_string()))?;
            let provider = self.resource_creation.iter().find(|provider| provider.provider() == &proof.provider)
                .ok_or_else(|| Error::Unsupported("resource proof verifier is not bound".into()))?;
            let receipt = provider.verify(&request, proof).await?;
            if receipt.resource != proof.resource || receipt.evidence != proof.evidence {
                return Err(Error::Unauthorized("volume receipt differs from admitted proof".into()));
            }
            for content in receipt.content {
                let allocated = receipt.operations.iter().any(|operation| match operation {
                    crate::resource_preparation::CreatedResourceOperation::ReadVolume(volume)
                    | crate::resource_preparation::CreatedResourceOperation::WriteVolume(volume) => volume == &content.volume,
                    _ => false,
                });
                // Exact foreign-file readers do not describe another allocation
                // and must never acquire this allocation's generation or agent.
                if allocated && matches!(content.volume.class(), VolumeClass::Project | VolumeClass::AgentPrivate) {
                    let origin = receipt.origin.intended_child;
                    if content.default_writer && &content.volume == current_private {
                        if current_origin.is_some_and(|current| current != origin) {
                            return Err(Error::Unauthorized("current actor allocation origin is ambiguous".into()));
                        }
                        current_origin = Some(origin);
                    }
                    let snapshot = (origin, TaskVolumeSnapshot { volume: content.volume, generation: receipt.allocation_generation.clone() });
                    if !snapshots.contains(&snapshot) { snapshots.push(snapshot); }
                }
            }
        }
        let origin = current_origin.ok_or_else(|| Error::Unauthorized("current actor allocation receipt is absent".into()))?;
        Ok(snapshots.into_iter().filter_map(|(allocation, snapshot)| {
            (allocation == origin && (snapshot.volume.class() == VolumeClass::Project
                || &snapshot.volume == current_private)).then_some(snapshot)
        }).collect())
    }
}
