/// Caller capacities and journal compaction targets.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct LocalStreamLimits {
    /// Canonical in-memory semantic-state limits.
    pub memory: MemoryLimits,
    /// Journal command target that triggers snapshot compaction.
    pub compaction_operations: u64,
    /// Journal byte target that triggers snapshot compaction.
    pub compaction_bytes: u64,
    /// Journal synchronization policy. Not recorded in the durable header.
    pub durability: LocalDurability,
}

impl Default for LocalStreamLimits {
    fn default() -> Self {
        Self {
            memory: MemoryLimits::default(),
            compaction_operations: 1_000_000,
            compaction_bytes: 1024 * 1024 * 1024,
            durability: LocalDurability::FullFlush,
        }
    }
}
