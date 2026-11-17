#[cfg(loom)]
#[doc(hidden)]
pub use loom::sync::atomic;
#[cfg(not(loom))]
pub use std::sync::atomic;
