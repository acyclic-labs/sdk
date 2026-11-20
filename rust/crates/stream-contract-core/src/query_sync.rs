//! Synchronization used by stream publication and observation.
#[cfg(all(test, not(target_arch = "wasm32")))]
#[path = "query_sync/parity.rs"]
pub mod parity;

#[cfg(all(not(loom), not(target_arch = "wasm32")))]
pub use acyclic_native_runtime::InlineMutex as SyncMutex;
#[cfg(loom)]
pub use loom::sync::{Arc, Mutex as SyncMutex};
#[cfg(not(loom))]
pub use std::sync::Arc;
#[cfg(all(not(loom), target_arch = "wasm32"))]
pub use std::sync::Mutex as SyncMutex;
#[cfg(not(loom))]
pub use tokio::sync::watch;

#[path = "query_sync/budgeted.rs"]
mod budgeted;
#[cfg(loom)]
pub use budgeted::budget;
pub use budgeted::{Mutex, RwLock};
#[cfg(loom)]
#[path = "query_sync/locks.rs"]
mod locks;
#[cfg(loom)]
#[path = "query_sync/watch.rs"]
pub mod watch;
