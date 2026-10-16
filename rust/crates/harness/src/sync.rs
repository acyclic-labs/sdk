//! Synchronization selected only by the dedicated model command's cfg.
#[cfg(acyclic_loom)]
pub(crate) use acyclic_stream::query_sync::Mutex;
#[cfg(not(acyclic_loom))]
pub(crate) use tokio::sync::Mutex;

#[path = "sync/primitives.rs"]
mod primitives;
pub(crate) use primitives::{Arc, Mutex as SyncMutex, atomic};

#[cfg(all(feature = "filesystem-local", not(target_arch = "wasm32")))]
#[path = "sync/owned.rs"]
pub(crate) mod owned;

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
