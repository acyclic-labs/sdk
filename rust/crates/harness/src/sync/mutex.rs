#[cfg(acyclic_harness_loom)]
pub(crate) use loom::sync::Mutex;
#[cfg(not(acyclic_harness_loom))]
pub(crate) use std::sync::Mutex;
