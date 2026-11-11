//! Task-selected native views and their filesystem access decisions.
mod composition;
pub(crate) mod copied;
mod discovery;
pub use acyclic_fs::access::MountMode;
pub(crate) use composition::split_address;
pub use composition::{Mount, MountTable, NativeRoot, VolumeView, compose};
