//! Production and model synchronization share these exact imports.
#[cfg(acyclic_harness_loom)]
pub(crate) use loom::sync::{Arc, Mutex, atomic};
#[cfg(not(acyclic_harness_loom))]
pub(crate) use std::sync::{Arc, Mutex, atomic};
