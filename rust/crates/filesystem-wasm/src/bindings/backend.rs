pub(super) type BrowserAuthority = acyclic_fs::StreamAuthorityStore;

/// `IndexedDB`, OPFS or memory objects behind the one erased object contract.
pub(super) type BrowserObjects = acyclic_fs::SharedObjectStore;
