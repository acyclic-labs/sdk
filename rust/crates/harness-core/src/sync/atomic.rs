#[cfg(acyclic_harness_loom)]
#[doc(hidden)]
pub use loom::sync::atomic;
#[cfg(not(acyclic_harness_loom))]
pub(crate) use std::sync::atomic;
