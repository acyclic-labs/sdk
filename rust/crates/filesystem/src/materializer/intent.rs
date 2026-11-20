use super::*;

/// Immutable authority for publication or native preparation.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub enum MaterializationIntent {
    /// Transition between authenticated generations.
    GenerationTransition {
        /// Source generation.
        from: Box<GenerationId>,
        /// Target generation.
        to: Box<GenerationId>,
    },
    /// Authenticated provider selection and request provenance.
    NativePreparation {
        /// Selected provider generations.
        selection: Vec<MaterializationPreparationSelection>,
        /// Original request and fences.
        original: Vec<u8>,
    },
}
impl MaterializationIntent {
    /// Heap bytes owned by a generation transition's two boxed ids.
    pub(crate) const TRANSITION_HEAP_BYTES: usize = 2 * std::mem::size_of::<GenerationId>();

    /// Transition from one authenticated generation to another.
    pub fn transition(from: GenerationId, to: GenerationId) -> Self {
        Self::GenerationTransition {
            from: Box::new(from),
            to: Box::new(to),
        }
    }

    /// The source and target of a generation transition.
    pub fn generation_transition(&self) -> Option<(GenerationId, GenerationId)> {
        match self {
            Self::GenerationTransition { from, to } => Some((**from, **to)),
            Self::NativePreparation { .. } => None,
        }
    }
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
    /// Creation awaiting acknowledgement.
    pub pending: bool,
}
