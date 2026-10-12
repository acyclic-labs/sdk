use super::*;

/// Immutable authority of one materialization operation.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub enum MaterializationIntent {
    /// Publication between exact SDK generations.
    GenerationTransition {
        /// Represented generation.
        from: GenerationId,
        /// Published generation.
        to: GenerationId,
    },
    /// Preparation from authenticated provider selections.
    NativePreparation {
        /// Qualified provider generations.
        selection: Vec<MaterializationPreparationSelection>,
        /// Authenticated request and fence provenance.
        original: Vec<u8>,
    },
}

/// One qualified provider generation.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct MaterializationPreparationSelection {
    /// Provider identity.
    pub provider_identity: Vec<u8>,
    /// SDK volume.
    pub volume: crate::VolumeId,
    /// Selected generation.
    pub generation: GenerationId,
}

/// Original operation, staging, and backup allocations.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct MaterializationPreparationIdentity {
    /// Operation directory.
    pub operation: [u8; 16],
    /// Staged target.
    pub target: [u8; 16],
    /// Preimage backup.
    pub backup: [u8; 16],
}

/// Durable directory-creation prefix.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct MaterializationPreparationProgress {
    /// Held existing parent.
    pub parent: [u8; 16],
    /// Acknowledged operation directory.
    pub operation: Option<[u8; 16]>,
    /// Acknowledged staging directory.
    pub target: Option<[u8; 16]>,
    /// Acknowledged backup directory.
    pub backup: Option<[u8; 16]>,
    /// Admitted mkdir without a durable acknowledgement.
    pub pending: Option<MaterializationPreparationAllocation>,
}

/// Directory admitted at a durable boundary.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub enum MaterializationPreparationAllocation {
    /// Operation directory.
    Operation,
    /// Staged target.
    Target,
    /// Preimage backup.
    Backup,
}
