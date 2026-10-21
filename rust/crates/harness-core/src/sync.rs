//! Synchronization selected only by the dedicated model command's cfg.
#[cfg(acyclic_loom)]
#[doc(hidden)]
pub use acyclic_stream::query_sync::Mutex;
#[cfg(not(acyclic_loom))]
#[doc(hidden)]
pub use tokio::sync::Mutex;

#[path = "sync/mutex.rs"]
mod mutex;
#[cfg(any(
    test,
    all(feature = "wasm", target_arch = "wasm32"),
    acyclic_harness_loom,
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
    acyclic_harness_loom,
    all(
        not(target_arch = "wasm32"),
        any(feature = "mcp-stdio", feature = "mcp-http")
    )
))]
include!("sync/atomic.rs");

#[cfg(all(feature = "filesystem-local", not(target_arch = "wasm32")))]
#[path = "sync/owned.rs"]
#[doc(hidden)]
pub mod owned;

#[cfg(test)]
#[test]
fn ordinary_build_uses_standard_synchronization() {
    assert!(!cfg!(acyclic_harness_loom));
    #[cfg(not(acyclic_harness_loom))]
    {
        let value: std::sync::Arc<atomic::AtomicUsize> = Arc::new(atomic::AtomicUsize::new(0));
        assert_eq!(value.load(atomic::Ordering::Relaxed), 0);
    }
}
