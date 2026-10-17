/// Admission, actual reserve failures, and completion points of canonical owned-buffer readers.
pub enum ReadAllocationStep {
    /// Admit owned capacity before reservation.
    Allocate {
        /// Requested bytes.
        bytes: u64,
        /// Allocation operations.
        operations: u64,
    },
    /// Preserve the actual failed reserve cause.
    ReserveFailed(std::collections::TryReserveError),
    /// Reconcile owned physical capacity.
    Capacity {
        /// Requested bytes.
        admitted_bytes: u64,
        /// Physical bytes.
        actual_bytes: u64,
    },
    /// Check an aggregate copy bound without reserving individual effects.
    CheckCopy(u64),
    /// Admit a byte copy.
    BeforeCopy(u64),
    /// Record a completed byte copy.
    Copied(u64),
    /// Admit hashing.
    BeforeHash(u64),
    /// Record completed hashing.
    Hashed(u64),
    /// Admit one decoded item.
    BeforeItem,
    /// Record one completed decoded item.
    Item,
}
