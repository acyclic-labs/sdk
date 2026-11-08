#[cfg(loom)]
use loom::sync;
#[cfg(not(loom))]
use std::sync;

#[cfg(all(not(loom), not(target_arch = "wasm32")))]
pub(crate) use acyclic_native_runtime::InlineMutex as Mutex;
#[cfg(any(loom, target_arch = "wasm32"))]
pub(crate) use sync::Mutex;

#[cfg(any(
    all(feature = "session", not(target_arch = "wasm32")),
    all(feature = "wasm", target_arch = "wasm32")
))]
pub(crate) use sync::Arc;
pub(crate) use sync::atomic::{AtomicU64, Ordering};
