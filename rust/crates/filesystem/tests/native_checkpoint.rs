//! Portable checkpoint codec qualification over the canonical native modules.

#[path = "../src/windows_usn_codec.rs"]
pub mod codec;
#[path = "../src/native_identity.rs"]
pub mod native_identity;
use native_identity::NativeRootIdentity;

#[path = "native_checkpoint/windows_usn_codec.rs"]
mod windows_usn_codec;
