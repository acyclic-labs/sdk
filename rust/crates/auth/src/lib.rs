//! Shared Ed25519 capability contract for hosted and in-process runtimes.
pub mod contract;
mod resource;
mod validation;

pub use resource::{FS_ACTIONS, valid_action, valid_resource};

/// Canonical shared auth descriptor, generated from the semantic declarations.
pub const FILE_DESCRIPTOR_SET: &[u8] = include_bytes!("generated/acyclic-auth-v1.bin");

/// Ed25519 block transcript prefix. Follow with the zero-based block index as
/// LE u64, previous revocation id (32 zero bytes at root), payload length as
/// LE u64, then exact canonical Block bytes. Root is signed by the certified
/// account leaf; each later block by the previous next_public_key.
pub const BLOCK_DOMAIN: &[u8] = b"acyclic.auth.v1/block\0";

/// Sealing transcript prefix, followed by the tip revocation id. Sign with
/// the tip private key, then delete it. Verifiers check this signature or
/// that the supplied 32-byte secret derives the tip's next_public_key.
pub const SEAL_DOMAIN: &[u8] = b"acyclic.auth.v1/seal\0";

/// Revocation ids are SHA-256(block transcript || 64-byte signature).
/// Revoke any ancestor to revoke every descendant. Key/epoch revocation and
/// account-bearer certification are also mandatory at the trust boundary.
pub const REVOCATION_ID_BYTES: usize = 32;

/// Malformed or unsupported capability data.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, thiserror::Error)]
pub enum Error {
    /// A required value is absent or malformed.
    #[default]
    #[error("invalid capability")]
    Invalid,
    /// Unknown enum values fail closed.
    #[error("unknown capability enum {0}")]
    UnknownEnum(i32),
}

impl From<std::convert::Infallible> for Error {
    fn from(value: std::convert::Infallible) -> Self {
        match value {}
    }
}
