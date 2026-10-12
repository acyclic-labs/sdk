use super::*;

/// Stable identity within an embedding application's durable namespace.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Ord, PartialOrd, Serialize, Deserialize)]
pub struct BranchId {
    /// Embedding application's session/replica identity.
    pub namespace: u128,
    /// Monotonic local identity, never recycled by disposal.
    pub sequence: u64,
}

/// Required provenance of one explicitly declared dependency.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub enum DependencyRequirement {
    /// A still-valid local prediction or its confirmed canonical counterpart.
    Prediction,
    /// Correspondence has already been established by trusted evidence.
    Confirmed,
}

/// A typed dependency edge. It never grants effect admission or authority.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct Dependency {
    /// Stable identity of an older hypothesis.
    pub branch: BranchId,
    /// Provenance required at creation; subsequent invalidation is transitive.
    pub requirement: DependencyRequirement,
}

impl Dependency {
    /// A pure dependent prediction may use a provisional parent projection.
    pub fn prediction(branch: BranchId) -> Self {
        Self {
            branch,
            requirement: DependencyRequirement::Prediction,
        }
    }
}
