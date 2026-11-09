#[cfg(loom)]
use loom::sync;
#[cfg(not(loom))]
use std::sync;

pub(crate) use crate::query_sync::SyncMutex as Mutex;

#[cfg(any(
    all(feature = "session", not(target_arch = "wasm32")),
    all(feature = "wasm", target_arch = "wasm32")
))]
pub(crate) use sync::Arc;
pub(crate) use sync::atomic::{AtomicU64, Ordering};
