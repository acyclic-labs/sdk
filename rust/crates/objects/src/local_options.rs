//! Durable capacity, synchronization policy and private reclamation counters.
/// How journal frames and immutable bodies are made durable before they become observable.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub enum LocalDurability {
    /// Flushes file contents and requests durable publication. Windows uses write-through
    #[default]
    FullFlush,
    /// Uses Apple's `F_BARRIERFSYNC`. Opening on a target without that exact primitive fails
    Barrier,
}

/// Exact durable-local capacity contract. Reopening requires the same capacity limits;
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct LocalObjectsLimits {
    /// Largest body admitted by a single completed object.
    pub maximum_object_bytes: Option<u64>,
    /// Largest aggregate logical body/part footprint admitted by the provider.
    pub maximum_bytes: Option<u64>,
    /// Maximum complete mutation frames replayed at startup.
    pub maximum_journal_operations: Option<u64>,
    /// Maximum journal bytes including its durable header and frame checksums.
    pub maximum_journal_bytes: Option<u64>,
    /// Synchronization policy for journal frames and bodies. Not recorded in the durable
    pub durability: LocalDurability,
}

impl Default for LocalObjectsLimits {
    fn default() -> Self {
        Self {
            maximum_object_bytes: None,
            maximum_bytes: None,
            maximum_journal_operations: None,
            maximum_journal_bytes: None,
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
    pub journal_bytes_reclaimed: u64,
}
