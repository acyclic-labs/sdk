/// Exclusive-process durable local provider backed by a snapshot and a checksummed command
#[derive(Clone)]
pub struct LocalStream {
    /// A user's handle on the store's claim; none for the provider's own
    /// detached work. Once every user's handle is gone the store is closing,
    /// and a reopen in this process waits for the journal to close instead
    /// of finding the store open. Declared first, so it drops before
    /// `inner`.
    handle: Option<acyclic_native_runtime::StoreHandle>,
    inner: Arc<LocalInner>,
    work: Option<Arc<crate::memory::Work>>,
}
