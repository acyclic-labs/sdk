//! Shared Ed25519 capability contract for hosted and in-process runtimes.
#[cfg(any(test, feature = "test-support"))]
extern crate self as acyclic_auth;
#[cfg(feature = "test-support")]
#[doc(hidden)]
#[path = "../tests/common/signed.rs"]
pub mod testing;
pub mod authority;
pub mod contract;
pub mod credential;
pub mod crypto;
mod resource;
pub mod socket;
pub mod trust;
mod validation;
pub mod verify;

pub use resource::{
    FS_ACTIONS, decode_component, encode_component, objects_resource, valid_action, valid_resource,
};

/// Canonical shared auth descriptor, generated from the semantic declarations.
pub const FILE_DESCRIPTOR_SET: &[u8] =
    include_bytes!(concat!(env!("OUT_DIR"), "/acyclic-auth-v1.bin"));

/// Ed25519 block transcript prefix. Follow with the zero-based block index as
/// LE u64, format version as LE u32, previous revocation id (32 zero bytes at root), payload length as
/// LE u64, then exact canonical Block bytes. Root is signed by the certified
/// account leaf; each later block by the previous next_public_key.
pub const BLOCK_DOMAIN: &[u8] = b"acyclic.auth.v1/block\0";

/// Sealing transcript prefix, followed by the tip revocation id. Sign with
/// the tip private key, then delete it. Verifiers check this signature or
/// that the supplied 32-byte secret derives the tip's next_public_key.
pub const SEAL_DOMAIN: &[u8] = b"acyclic.auth.v1/seal\0";

/// Revocation ids are SHA-256(block transcript || 64-byte signature).
/// Revoke any ancestor to revoke every descendant. Key/epoch revocation and
/// full certificate ancestry are also mandatory at the trust boundary.
pub const REVOCATION_ID_BYTES: usize = 32;

/// Malformed or unsupported capability data.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, thiserror::Error)]
pub enum Error {
    /// A required value is absent or malformed.
    #[default]
    #[error("invalid capability")]
    Invalid,
    /// Explicit unsupported format version.
    #[error("unsupported capability version {0}")]
    UnsupportedVersion(u32),
    /// Unknown enum values fail closed.
    #[error("unknown capability enum {0}")]
    UnknownEnum(i32),
}

impl From<std::convert::Infallible> for Error {
    fn from(value: std::convert::Infallible) -> Self {
        match value {}
    }
}

/// Environment birth-certificate signature domain.
pub const BIRTH_DOMAIN: &[u8] = b"acyclic.auth.v1/birth\0";
/// Online key-certificate signature domain.
pub const KEY_DOMAIN: &[u8] = b"acyclic.auth.v1/key\0";
/// Followed keyring publication signature domain.
pub const KEYRING_DOMAIN: &[u8] = b"acyclic.auth.v1/keyring\0";
/// Supported capability format.
pub const VERSION: u32 = 1;
