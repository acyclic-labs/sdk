//! Synchronization selected only by the dedicated model command's cfg.
#[cfg(loom)]
#[doc(hidden)]
pub use acyclic_stream::query_sync::Mutex;
#[cfg(not(loom))]
#[doc(hidden)]
pub use tokio::sync::Mutex;

#[path = "sync/mutex.rs"]
mod mutex;
#[cfg(any(
    test,
    all(feature = "wasm", target_arch = "wasm32"),
    loom,
    not(target_arch = "wasm32")
))]
include!("sync/arc.rs");
#[doc(hidden)]
pub use mutex::Mutex as SyncMutex;
include!("sync/atomic.rs");

#[cfg(not(target_arch = "wasm32"))]
#[path = "sync/owned.rs"]
#[doc(hidden)]
pub mod owned;
