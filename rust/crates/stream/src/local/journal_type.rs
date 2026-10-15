struct Journal {
    /// Held locked for as long as the provider is open.
    lock: File,
    file: File,
    /// Which snapshot the journal follows: the one naming this epoch.
    epoch: u64,
    operations: u64,
    bytes: u64,
    limits: LocalStreamLimits,
    /// A second handle on `file`, synchronized without holding the journal,
    sync: Arc<File>,
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
    _ownership_anchor: Option<OwnershipAnchor>,
}

impl Drop for Journal {
    fn drop(&mut self) {
        let _ = self.lock.unlock();
    }
}
