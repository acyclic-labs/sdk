use super::*;

/// Immutable authority for publication or native preparation.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub enum MaterializationIntent {
    /// Transition between authenticated generations.
    GenerationTransition {
        /// Source generation.
        from: GenerationId,
        /// Target generation.
        to: GenerationId,
    },
    /// Authenticated provider selection and request provenance.
    NativePreparation {
        /// Selected provider generations.
        selection: Vec<MaterializationPreparationSelection>,
        /// Original request and fences.
        original: Vec<u8>,
    },
}
/// One selected provider generation.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct MaterializationPreparationSelection {
    /// Provider identity.
    pub provider_identity: Vec<u8>,
    /// SDK volume.
    pub volume: crate::VolumeId,
    /// Selected generation.
    pub generation: GenerationId,
}
/// Original recovery directory allocations.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct MaterializationPreparationIdentity {
    /// Operation directory.
    pub operation: [u8; 16],
    /// Staged target.
    pub target: [u8; 16],
    /// Preimage backup.
    pub backup: [u8; 16],
}
/// Durable directory creation prefix.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct MaterializationPreparationProgress {
    /// Existing parent allocation.
    pub parent: [u8; 16],
    /// Acknowledged operation directory.
    pub operation: Option<[u8; 16]>,
    /// Acknowledged target directory.
    pub target: Option<[u8; 16]>,
    /// Acknowledged backup directory.
    pub backup: Option<[u8; 16]>,
    /// Creation awaiting acknowledgement.
    pub pending: Option<MaterializationPreparationAllocation>,
}
/// Directory at the next preparation boundary.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub enum MaterializationPreparationAllocation {
    /// Operation directory.
    Operation,
    /// Target directory.
    Target,
    /// Backup directory.
    Backup,
}
