//! Synchronization used by task-volume publication and observation.
#[cfg(any(acyclic_loom, all(test, not(target_arch = "wasm32"))))]
pub(crate) mod parity;

#[cfg(acyclic_loom)]
pub use loom::sync::Mutex as SyncMutex;
#[cfg(not(acyclic_loom))]
pub use std::sync::Mutex as SyncMutex;
#[cfg(not(acyclic_loom))]
pub use tokio::sync::{Mutex, RwLock, watch};

#[cfg(acyclic_loom)]
#[path = "query_sync/locks.rs"]
mod locks;
#[cfg(acyclic_loom)]
pub use locks::{Mutex, RwLock};
#[cfg(acyclic_loom)]
#[path = "query_sync/watch.rs"]
pub mod watch;
