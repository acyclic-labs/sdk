//! Synchronization used by stream publication and observation.
#[cfg(any(loom, all(test, not(target_arch = "wasm32"))))]
pub(crate) mod parity;

#[cfg(loom)]
pub use loom::sync::Mutex as SyncMutex;
#[cfg(not(loom))]
pub use std::sync::Mutex as SyncMutex;
#[cfg(not(loom))]
pub use tokio::sync::{Mutex, RwLock, watch};

#[cfg(loom)]
#[path = "query_sync/locks.rs"]
mod locks;
#[cfg(loom)]
pub use locks::{Mutex, RwLock};
#[cfg(loom)]
#[path = "query_sync/watch.rs"]
pub mod watch;
