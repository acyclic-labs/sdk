use super::*;

/// Data-only persistence/share representation.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Hypothesis<K, B, O, A, V> {
    /// Stable hypothesis identity.
    pub id: BranchId,
    /// Correspondence adapter pin.
    pub adapter: u128,
    /// Target normalized record identity.
    pub key: K,
    /// Original admitted operation identity, if any.
    pub operation: Option<O>,
    /// Exact authoritative basis pinned at prediction.
    pub basis: Arc<B>,
    /// Domain-typed assumptions.
    pub assumption: A,
    /// Dependencies must be live and older, so cycles cannot be introduced.
    pub dependencies: Vec<Dependency>,
    /// Deterministic, immutable predicted projection.
    pub predicted: Arc<V>,
    /// Logical expiry supplied by the embedding host.
    pub expires: u64,
    /// Prediction status remains separate from the operation.
    pub prediction: PredictionOutcome,
    /// Last trusted correlated operation status.
    pub outcome: OperationOutcome,
    /// Conservatively accounted retained bytes including kernel/index metadata.
    pub bytes: usize,
}

/// Arguments for a pure `begin` or dependent `fork`.
pub struct Begin<K, B, O, A, V> {
    /// Target demanded key.
    pub key: K,
    /// Exact pinned basis.
    pub basis: Arc<B>,
    /// Optional original operation identity.
    pub operation: Option<O>,
    /// Domain assumptions.
    pub assumption: A,
    /// Deterministic prediction.
    pub predicted: Arc<V>,
    /// Explicit dependencies; no speculative fanout is started.
    pub dependencies: Vec<Dependency>,
    /// Logical expiry.
    pub expires: u64,
}

/// Provenance of a selected immutable value.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum Provenance<B> {
    /// Exact authoritative basis.
    Authoritative(Arc<B>),
    /// A local hypothesis under an exact authoritative basis and adapter.
    Hypothesis {
        /// Stable local identity.
        id: BranchId,
        /// Exact authority/generation/revision/content basis.
        basis: Arc<B>,
        /// Pinned correspondence implementation and revision.
        adapter: u128,
    },
}

/// Stable snapshot of one record. Caller-held snapshots survive reconciliation.
pub struct View<B, V> {
    /// Shared immutable value.
    pub value: Arc<V>,
    /// Explicit authority/hypothesis provenance.
    pub provenance: Provenance<B>,
}
