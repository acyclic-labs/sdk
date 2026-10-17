//! Model builds instrument the same queue and completion locks as native runs.
#[cfg(acyclic_native_runtime_loom)]
pub(crate) use loom::sync::Mutex;
#[cfg(not(acyclic_native_runtime_loom))]
pub(crate) use std::sync::{Mutex, mpsc};

#[cfg(acyclic_native_runtime_loom)]
pub(crate) mod mpsc;
