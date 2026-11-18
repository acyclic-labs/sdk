//! Identity and path contracts shared by native filesystem adapters.
#![deny(unsafe_code)]

mod mount_path;
mod native_identity;

pub use mount_path::MountPath;
pub use native_identity::{NativeRootGeneration, NativeRootIdentity, NativeRootIncarnation};
