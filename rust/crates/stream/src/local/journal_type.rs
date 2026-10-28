struct Journal {
    /// Holds a descriptor only while recently used; flushes that run
    /// without holding the journal lease it.
    file: acyclic_native_runtime::CachedFile,
    /// Which snapshot the journal follows: the one naming this epoch.
    epoch: u64,
    operations: u64,
    bytes: u64,
    limits: LocalStreamLimits,
    /// How many frames were ever written, across every epoch: the sequence of
    appended: u64,
    /// The latest frame written by a mutation that waits for the device.
    required: u64,
    /// Every frame through this sequence is ordered (possibly still cached).
    synced: u64,
    /// A device flush, including the recovered prefix, has covered this cut.
    flushed_sequence: Option<u64>,
    /// Whether a flush is under way; see [`LocalStream::await_publication`].
    syncing: bool,
    root: PathBuf,
    torn_tail: Option<u64>,
    /// Whether this run has written a frame whose revision a crash can lose:
    /// a deferred mutation, or any under barrier durability. The store's
    /// incarnation record is removed first, so the next open rotates it.
    volatile: bool,
    /// While a persistence cut is installed: the length of this epoch's
    /// journal known flushed. It lives beside `epoch`, and `restart` resets it.
    #[cfg(feature = "test-support")]
    flushed: u64,
    /// Held for as long as the provider is open, without a descriptor, and
    /// released only after the journal's files close. Each user-facing
    /// provider holds one of its handles.
    claim: acyclic_native_runtime::StoreClaim,
    _ownership_anchor: Option<OwnershipAnchor>,
}
