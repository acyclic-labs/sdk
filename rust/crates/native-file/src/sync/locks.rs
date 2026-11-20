//! Shared native and model lock types, without queue or reference-count state.
#[cfg(not(loom))]
#[path = "inline.rs"]
mod inline;
#[cfg(not(loom))]
pub use inline::{Condvar, Mutex, MutexGuard};
#[cfg(loom)]
pub use loom::sync::{Condvar, Mutex, MutexGuard};
