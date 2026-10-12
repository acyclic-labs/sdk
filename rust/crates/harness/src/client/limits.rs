use super::*;

/// Hard bounds on active demand and each transition.
#[derive(Clone, Copy, Debug, Serialize, Deserialize)]
pub struct Limits {
    /// Resident authoritative records.
    pub records: usize,
    /// Resident hypotheses including terminal outcomes.
    pub branches: usize,
    /// Total resident hypothesis dependency edges.
    pub edges: usize,
    /// Total conservatively accounted resident bytes: adapter-declared deep allocations plus kernel/index metadata.
    pub bytes: usize,
    /// Maximum adapter steps plus branch/edge visits per mutation (map lookups additionally cost logarithmic time in bounded active demand).
    pub work: usize,
    /// Maximum logical retention ticks of a hypothesis.
    pub retention: u64,
    /// Maximum explicitly selected overlays in one view request.
    pub visible: usize,
}

/// Explicit failure; mutations are atomic on failure.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum Error {
    /// Finite capacity or per-transition work exhausted.
    Budget,
    /// Exact basis no longer available.
    StaleBasis,
    /// Incompatible adapter, evidence or assumptions.
    Conflict,
    /// Adapter cannot establish correspondence.
    Unsupported,
    /// Identity, demanded record or dependency absent.
    Missing,
    /// A competing visible hypothesis targets the same record.
    Ambiguous,
    /// Logical time regressed or expiry invalid.
    Time,
}
