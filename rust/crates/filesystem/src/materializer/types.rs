use super::*;

/// One declarative pathwise checkout edit.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[non_exhaustive]
pub enum MaterializationEdit {
    /// Install a complete backend-defined entry image at one portable path.
    Install {
        /// Canonical relative path.
        path: String,
        /// Opaque entry image interpreted by the backend.
        image: Vec<u8>,
    },
    /// Remove one entry or subtree.
    Remove {
        /// Canonical relative path.
        path: String,
    },
    /// Rename one entry without flattening its metadata or link identity.
    Rename {
        /// Existing canonical relative path.
        from: String,
        /// Destination canonical relative path.
        to: String,
    },
    /// Change representable metadata without replacing a directory subtree.
    SetMetadata {
        /// Canonical relative path.
        path: String,
        /// Backend-defined declarative metadata image.
        image: Vec<u8>,
    },
}

/// Immutable materialization plan bound to exact SDK generations.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct MaterializationPlan {
    /// Stable retry and recovery identity.
    pub operation_id: OperationId,
    /// Exact generation transition or authenticated preparation.
    pub intent: MaterializationIntent,
    /// Ordered, nonoverlapping destructive edits and descendant metadata edits.
    pub edits: Vec<MaterializationEdit>,
}

/// Opaque complete preimage for one edit.
#[derive(Debug, Serialize, Deserialize)]
pub struct MaterializationPreimage {
    /// Backend-defined bytes sufficient to restore every representable fact.
    pub image: Vec<u8>,
    /// Live allocation of this payload.
    #[cfg(not(target_arch = "wasm32"))]
    #[serde(skip)]
    pub custody: Option<NativeAllocation>,
}

/// Exact physical state observed while resuming one journaled edit.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub enum MaterializationObservation {
    /// Backend delegates idempotency to its apply/restore implementations.
    Unverified,
    /// The edit has not yet been applied.
    Preimage,
    /// The edit was applied before its progress record became durable.
    Postimage,
    /// An operation-owned rename sequence stopped between its durable endpoints.
    Interrupted,
    /// Neither durable endpoint matches; an external writer intervened.
    Diverged,
}

/// Durable application phase.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub enum MaterializationPhase {
    /// Intent is durable; staging or preimage capture is incomplete.
    Preparing,
    /// Every preimage is durable and no edit has been applied.
    Prepared,
    /// Forward application is in progress.
    Applying,
    /// Every edit is durable in the target checkout.
    Applied,
    /// Reverse preimage restoration is in progress.
    RollingBack,
    /// The original checkout has been restored.
    RolledBack,
}

/// Complete versioned recovery journal.
#[derive(Debug, Serialize, Deserialize)]
pub struct MaterializationJournal {
    /// Serialization contract version.
    pub version: u32,
    /// Monotonic optimistic-concurrency revision.
    pub revision: u64,
    /// Immutable plan.
    pub plan: MaterializationPlan,
    /// Original physical root allocation.
    pub root_identity: Option<[u8; 16]>,
    /// Original operation, target and backup allocations.
    pub preparation_identity: Option<MaterializationPreparationIdentity>,
    /// Durable prefix of operation-directory creation.
    pub preparation_progress: Option<MaterializationPreparationProgress>,
    /// Actual work known at the last durable boundary.
    pub work: crate::WorkCounters,
    /// Preimages in exact edit order.
    pub preimages: Vec<MaterializationPreimage>,
    /// Current recovery phase.
    pub phase: MaterializationPhase,
    /// Number of forward edits known durable.
    pub applied: u32,
    /// Number of applied edits restored from the end.
    pub restored: u32,
    /// Live allocation of the journal payload.
    #[cfg(not(target_arch = "wasm32"))]
    #[serde(skip)]
    pub custody: Option<NativeAllocation>,
}

/// Durable optimistic-concurrency adapter for materialization journals.
pub trait MaterializationJournalStore: Send + Sync {
    /// Adapter error.
    type Error: std::error::Error + Send + Sync + 'static;

    /// Journal completion prepaid before a native mutation.
    #[cfg(not(target_arch = "wasm32"))]
    type ActionCompletion<'a>: Future<Output = Result<MaterializationJournal, Self::Error>> + Send
    where
        Self: 'a;

    /// Admits the durable acknowledgement before its physical action.
    #[cfg(not(target_arch = "wasm32"))]
    fn prepare_action<'a>(
        &'a self,
        journal: MaterializationJournal,
    ) -> impl Future<Output = Result<Self::ActionCompletion<'a>, Self::Error>> + Send;

    /// Loads one operation journal.
    fn load(
        &self,
        operation_id: OperationId,
    ) -> impl Future<Output = Result<Option<MaterializationJournal>, Self::Error>> + Send;

    /// Atomically replaces `expected_revision`; zero creates a journal.
    fn compare_and_swap(
        &self,
        operation_id: OperationId,
        expected_revision: u64,
        replacement: MaterializationJournal,
    ) -> impl Future<Output = Result<(bool, MaterializationJournal), Self::Error>> + Send;
}

/// Backend that can capture, apply, and restore every representable host fact.
pub trait MaterializationBackend: Send + Sync {
    /// Backend error.
    type Error: std::error::Error + Send + Sync + 'static;

    /// Uses the backend's existing finite context for journal work.
    #[cfg(not(target_arch = "wasm32"))]
    fn native_work<T>(
        &self,
        _operation: impl FnOnce(&NativeMaterializationWork) -> Result<T, NativeTreeMaterializationError>,
    ) -> Result<Option<T>, Self::Error> {
        Ok(None)
    }
    /// Original root allocation, when independently observable.
    fn allocation_identity(&self) -> Option<[u8; 16]> {
        None
    }
    /// Original staging and backup allocations.
    fn preparation_identity(&self) -> Option<MaterializationPreparationIdentity> {
        None
    }
    /// Observes the currently named root allocation.
    fn observe_allocation_identity(
        &self,
    ) -> impl Future<Output = Result<Option<[u8; 16]>, Self::Error>> + Send {
        async { Ok(self.allocation_identity()) }
    }
    /// Observes the currently named staging and backup allocations.
    fn observe_preparation_identity(
        &self,
    ) -> impl Future<Output = Result<Option<MaterializationPreparationIdentity>, Self::Error>> + Send
    {
        async { Ok(self.preparation_identity()) }
    }
    /// Finishes durability barriers after a lost acknowledgement.
    fn settle_observed(
        &self,
        _edit: &MaterializationEdit,
    ) -> impl Future<Output = Result<(), Self::Error>> + Send {
        async { Ok(()) }
    }

    /// Captures a complete preimage without mutating the checkout.
    fn capture(
        &self,
        edit: &MaterializationEdit,
    ) -> impl Future<Output = Result<MaterializationPreimage, Self::Error>> + Send;

    /// Classifies durable endpoints, unless exclusion is provided by the caller.
    fn observe(
        &self,
        _edit: &MaterializationEdit,
        _preimage: &MaterializationPreimage,
    ) -> impl Future<Output = Result<MaterializationObservation, Self::Error>> + Send {
        async { Ok(MaterializationObservation::Unverified) }
    }

    /// Applies one declarative edit idempotently.
    fn apply(
        &self,
        edit: &MaterializationEdit,
        preimage: &MaterializationPreimage,
    ) -> impl Future<Output = Result<(), Self::Error>> + Send;

    /// Restores the exact preimage for one edit idempotently.
    fn restore(
        &self,
        edit: &MaterializationEdit,
        preimage: &MaterializationPreimage,
    ) -> impl Future<Output = Result<(), Self::Error>> + Send;
}

/// Recovery decision for a nonterminal journal.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum MaterializationRecovery {
    /// Continue applying the unpublished checkout candidate.
    Complete,
    /// Restore every path changed before interruption.
    RollBack,
}

/// Materialization failure.
#[derive(Debug, Error, strum::IntoStaticStr)]
pub enum MaterializationError<S: std::error::Error + 'static, B: std::error::Error + 'static> {
    /// Durable journal access failed.
    #[error("materialization journal failed: {0}")]
    Store(S),
    /// Host checkout access failed.
    #[error("materialization backend failed: {0}")]
    Backend(B),
    /// A retry supplied different immutable plan input.
    #[error("materialization retry conflicts with its durable plan")]
    PlanConflict,
    /// Persisted progress is malformed or uses an unsupported version.
    #[error("materialization journal is incompatible")]
    IncompatibleJournal,
    /// Optimistic state contention exceeded the bounded retry policy.
    #[error("materialization journal remained contended")]
    Contended,
    /// Physical state no longer matches either durable endpoint.
    #[error("materialization paused because an external path mutation was observed")]
    ExternalMutation,
}

/// Immutable authority domain of a physical operation.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub enum MaterializationIntent {
    /// Publication between actual SDK generations.
    GenerationTransition {
        /// Represented source.
        from: GenerationId,
        /// Published target.
        to: GenerationId,
    },
    /// Preparation from qualified provider selections.
    NativePreparation {
        /// Actual provider, volume and generation selections.
        selection: Vec<MaterializationPreparationSelection>,
        /// Canonical authenticated request and fence provenance.
        original: Vec<u8>,
    },
}

/// One actual provider's selected generation.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct MaterializationPreparationSelection {
    /// Actual provider identity descriptor.
    pub provider_identity: Vec<u8>,
    /// Actual SDK volume.
    pub volume: crate::VolumeId,
    /// Selected generation.
    pub generation: GenerationId,
}

/// Allocations of the operation directory and its two children.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct MaterializationPreparationIdentity {
    /// Operation directory.
    pub operation: [u8; 16],
    /// Staged target.
    pub target: [u8; 16],
    /// Preimage backup.
    pub backup: [u8; 16],
}

/// Directory-creation progress persisted before proceeding to the next child.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct MaterializationPreparationProgress {
    /// Held existing parent.
    pub parent: [u8; 16],
    /// Recorded operation allocation.
    pub operation: Option<[u8; 16]>,
    /// Recorded target allocation.
    pub target: Option<[u8; 16]>,
    /// Recorded backup allocation.
    pub backup: Option<[u8; 16]>,
    /// Allocation whose mkdir was admitted, but not yet acknowledged.
    pub pending: Option<MaterializationPreparationAllocation>,
}

/// Directory being allocated at a durable boundary.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub enum MaterializationPreparationAllocation {
    /// Operation directory.
    Operation,
    /// Staged target.
    Target,
    /// Preimage backup.
    Backup,
}

impl PartialEq for MaterializationPreimage {
    fn eq(&self, other: &Self) -> bool {
        self.image == other.image
    }
}
impl Eq for MaterializationPreimage {}
impl PartialEq for MaterializationJournal {
    fn eq(&self, other: &Self) -> bool {
        self.version == other.version
            && self.revision == other.revision
            && self.plan == other.plan
            && self.root_identity == other.root_identity
            && self.preparation_identity == other.preparation_identity
            && self.preparation_progress == other.preparation_progress
            && self.work == other.work
            && self.preimages == other.preimages
            && self.phase == other.phase
            && self.applied == other.applied
            && self.restored == other.restored
    }
}
impl Eq for MaterializationJournal {}
impl MaterializationJournal {
    /// Checks wire version and progress for this operation.
    pub fn is_valid_for(&self, operation: OperationId) -> bool {
        valid_journal(self, operation)
    }
}
