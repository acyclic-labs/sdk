pub(super) type BrowserAuthority =
    acyclic_fs::StreamAuthorityStore<dyn acyclic_stream::StreamProvider>;

/// `IndexedDB`, OPFS or memory objects behind the one erased object contract.
pub(super) type BrowserObjects = acyclic_fs::SharedObjectStore;
