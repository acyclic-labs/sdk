//! Task-selected native views and their filesystem access decisions.
mod composition;
mod discovery;
pub use acyclic_fs::access::MountMode;
pub use composition::{Mount, MountTable, VolumeView, compose};
