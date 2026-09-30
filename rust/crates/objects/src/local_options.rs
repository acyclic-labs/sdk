//! Durable capacity, synchronization policy and private reclamation counters.
/// How journal frames and immutable bodies are made durable before they become observable.
///
/// A per-open policy, not part of the on-disk contract: a store written under one policy
/// reopens under any other.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub enum LocalDurability {
    /// Flushes file contents and requests durable publication. Windows uses write-through
    /// moves; its API does not expose a documented equivalent of POSIX directory `fsync`.
    /// Power-loss durability of same-volume Windows moves is unproven; storage hardware
    /// and remote filesystems may also provide weaker guarantees.
    #[default]
    FullFlush,
    /// Uses Apple's `F_BARRIERFSYNC`. Opening on a target without that exact primitive fails
    /// with an I/O error instead of substituting different durability semantics.
    Barrier,
}

/// Exact durable-local capacity contract. Reopening requires the same capacity limits;
/// `durability` is a per-open policy.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct LocalObjectsLimits {
    /// Largest body admitted by a single completed object.
    pub maximum_object_bytes: u64,
    /// Largest aggregate logical body/part footprint admitted by the provider.
    pub maximum_bytes: u64,
    /// Maximum complete mutation frames replayed at startup.
    pub maximum_journal_operations: u64,
    /// Maximum journal bytes including its durable header and frame checksums.
    pub maximum_journal_bytes: u64,
    /// Synchronization policy for journal frames and bodies. Not recorded in the durable
    /// header.
    pub durability: LocalDurability,
}

impl Default for LocalObjectsLimits {
    fn default() -> Self {
        Self {
            maximum_object_bytes: 64 * 1_024 * 1_024,
            maximum_bytes: 4 * 1_024 * 1_024 * 1_024,
            maximum_journal_operations: 1_000_000,
            maximum_journal_bytes: 1_024 * 1_024 * 1_024,
            durability: LocalDurability::FullFlush,
        }
    }
}

/// Exact bounded physical-reclamation result for a durable local Objects root.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct LocalObjectsGarbageCollection {
    /// Immutable body segments examined.
    pub segments_examined: u64,
    /// Segments unreachable from every retained body removed.
    pub segments_removed: u64,
    /// Crash-left temporary publication files removed.
    pub temporary_files_removed: u64,
    /// Journal bytes reclaimed by compaction, which moves live inline bodies into segments
    /// and drops the rest.
    pub journal_bytes_reclaimed: u64,
}
