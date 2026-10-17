#[cfg(acyclic_harness_loom)]
pub(crate) use loom::sync::Arc;
#[cfg(not(acyclic_harness_loom))]
pub(crate) use std::sync::Arc;
