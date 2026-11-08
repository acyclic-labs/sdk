#[cfg(loom)]
use loom::sync;
#[cfg(not(loom))]
use std::sync;

#[cfg(any(
    all(feature = "session", not(target_arch = "wasm32")),
    all(feature = "wasm", target_arch = "wasm32")
))]
pub(crate) use sync::Arc;
pub(crate) use sync::{
    Mutex,
    atomic::{AtomicU64, Ordering},
};
