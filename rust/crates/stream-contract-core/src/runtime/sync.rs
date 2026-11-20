#[cfg(loom)]
use loom::sync;
#[cfg(not(loom))]
use std::sync;

#[cfg(not(loom))]
pub use crate::query_sync::SyncMutex as Mutex;
#[cfg(loom)]
pub use loom::sync::Mutex;

#[cfg(any(
    not(target_arch = "wasm32"),
    all(feature = "wasm", target_arch = "wasm32")
))]
pub use sync::Arc;
pub use sync::atomic::{AtomicU64, Ordering};
