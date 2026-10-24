struct Journal {
    /// Held locked for as long as the provider is open; unlocked explicitly
    /// when dropped, even if a child inherited the descriptor.
    _lock: FileLock,
    /// Shared with flushes that run without holding the journal, so the
    /// journal costs one descriptor.
    file: Arc<File>,
    /// Which snapshot the journal follows: the one naming this epoch.
    epoch: u64,
    operations: u64,
    bytes: u64,
    limits: LocalStreamLimits,
    /// How many frames were ever written, across every epoch: the sequence of
    appended: u64,
    /// The latest frame written by a mutation that waits for the device.
    required: u64,
    /// Every frame through this sequence is durable.
    synced: u64,
    /// Whether a flush is under way; see [`LocalStream::await_durable`].
    syncing: bool,
    root: PathBuf,
    torn_tail: Option<u64>,
    /// End of the previous seal; raw writes following it form the next group.
    group_start: u64,
    /// The previous seal must be synchronized before another is written.
    sealed_sequence: u64,
    pending_seal: Option<SealCredit>,
    /// While a persistence cut is installed: the length of this epoch's
    /// journal known flushed. It lives beside `epoch`, and `restart` resets it.
    #[cfg(feature = "test-support")]
    flushed: u64,
    _ownership_anchor: Option<OwnershipAnchor>,
}
