#[cfg(loom)]
#[doc(hidden)]
pub use loom::sync::atomic;
#[cfg(not(loom))]
pub(crate) use std::sync::atomic;
