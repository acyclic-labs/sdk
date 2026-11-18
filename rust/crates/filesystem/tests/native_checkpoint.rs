//! Portable checkpoint codec qualification over the canonical native modules.

#[path = "../src/windows_usn_codec.rs"]
pub mod codec;
use acyclic_fs_native_contract::NativeRootIdentity;

#[path = "native_checkpoint/windows_usn_codec.rs"]
mod windows_usn_codec;
