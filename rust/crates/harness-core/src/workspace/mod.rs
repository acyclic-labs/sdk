//! Task-selected native views and their filesystem access decisions.
mod composition;
#[cfg(feature = "machines")]
pub(crate) mod copied;
mod discovery;
pub use acyclic_fs::access::MountMode;
pub use composition::{Mount, MountTable, VolumeView, compose};
