//! Task-selected native views and their filesystem access decisions.
mod composition;
pub(crate) mod copied;
mod discovery;
pub(crate) mod namespace;
pub use acyclic_fs::access::MountMode;
pub use composition::{Mount, MountTable, NativeRoot, VolumeView, compose};
