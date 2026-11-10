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
    all(
        not(target_arch = "wasm32"),
        any(
            feature = "filesystem-local",
            feature = "mcp-stdio",
            feature = "mcp-http"
        )
    )
))]
include!("sync/arc.rs");
#[doc(hidden)]
pub use mutex::Mutex as SyncMutex;
#[cfg(any(
    test,
    loom,
    feature = "filesystem",
    all(
        not(target_arch = "wasm32"),
        any(feature = "mcp-stdio", feature = "mcp-http")
    )
))]
include!("sync/atomic.rs");

#[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
#[path = "sync/owned.rs"]
#[doc(hidden)]
pub mod owned;
