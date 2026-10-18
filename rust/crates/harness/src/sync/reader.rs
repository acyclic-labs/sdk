#[cfg(acyclic_harness_loom)]
pub(crate) use loom::sync::{Condvar, MutexGuard};
#[cfg(not(acyclic_harness_loom))]
pub(crate) use std::sync::{Condvar, MutexGuard};
