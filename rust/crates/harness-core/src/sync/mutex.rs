#[cfg(acyclic_harness_loom)]
#[doc(hidden)]
pub use loom::sync::Mutex;
#[cfg(not(acyclic_harness_loom))]
#[doc(hidden)]
pub use std::sync::Mutex;
