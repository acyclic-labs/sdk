//! Portable codec for the durable Windows USN checkpoint.
use crate::NativeRootIdentity;
use thiserror::Error;

const CHECKPOINT_MAGIC: &[u8; 8] = b"ACYUSN\0\x01";
const CHECKPOINT_BYTES: usize = 40;

/// Durable conservative USN marker captured before a full authenticated scan.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct WindowsUsnCheckpoint {
    pub(super) root_identity: NativeRootIdentity,
    pub(super) journal_id: u64,
    pub(super) next_usn: u64,
}

impl WindowsUsnCheckpoint {
    /// Returns the root identity to which this marker is confined.
    #[must_use]
    pub const fn root_identity(self) -> NativeRootIdentity {
        self.root_identity
    }

    /// Returns the volume change-journal identity.
    #[must_use]
    pub const fn journal_id(self) -> u64 {
        self.journal_id
    }

    /// Returns the first USN not represented by the marker.
    #[must_use]
    pub const fn next_usn(self) -> u64 {
        self.next_usn
    }

    /// Encodes the stable versioned 40-byte checkpoint.
    #[must_use]
    pub fn to_bytes(self) -> [u8; CHECKPOINT_BYTES] {
        let mut bytes = [0_u8; CHECKPOINT_BYTES];
        bytes[..8].copy_from_slice(CHECKPOINT_MAGIC);
        bytes[8..24].copy_from_slice(&self.root_identity.to_bytes());
        bytes[24..32].copy_from_slice(&self.journal_id.to_le_bytes());
        bytes[32..40].copy_from_slice(&self.next_usn.to_le_bytes());
        bytes
    }

    /// Decodes one exact canonical checkpoint.
    ///
    /// # Errors
    ///
    /// Rejects a wrong length, version, or USN outside the Windows signed-USN
    /// domain.
    pub fn from_bytes(bytes: &[u8]) -> Result<Self, WindowsUsnError> {
        acyclic_auth::format::header(
            bytes,
            b"ACYUSN\0",
            b"\x01",
            || WindowsUsnError::InvalidCheckpoint,
            || WindowsUsnError::UnsupportedVersion,
        )?;
        if bytes.len() != CHECKPOINT_BYTES {
            return Err(WindowsUsnError::InvalidCheckpoint);
        }
        let mut root = [0_u8; 16];
        root.copy_from_slice(bytes.get(8..24).ok_or(WindowsUsnError::InvalidCheckpoint)?);
        let journal_id = decode_u64(
            bytes
                .get(24..32)
                .ok_or(WindowsUsnError::InvalidCheckpoint)?,
        );
        let next_usn = decode_u64(
            bytes
                .get(32..40)
                .ok_or(WindowsUsnError::InvalidCheckpoint)?,
        );
        if next_usn > i64::MAX as u64 {
            return Err(WindowsUsnError::InvalidCheckpoint);
        }
        Ok(Self {
            root_identity: NativeRootIdentity::from_bytes(root),
            journal_id,
            next_usn,
        })
    }
}

fn decode_u64(bytes: &[u8]) -> u64 {
    let mut value = [0_u8; 8];
    value.copy_from_slice(bytes);
    u64::from_le_bytes(value)
}

#[cfg(feature = "test-support")]
/// Capture a checkpoint through the production fixed-width encoder.
pub fn seed() -> [u8; CHECKPOINT_BYTES] {
    WindowsUsnCheckpoint {
        root_identity: NativeRootIdentity::from_bytes([7; 16]),
        journal_id: 19,
        next_usn: 23,
    }
    .to_bytes()
}

/// Windows USN checkpoint failures.
#[derive(Debug, Error)]
pub enum WindowsUsnError {
    /// The checkpoint's recognized domain carries an old or unknown version.
    #[error("unsupported Windows USN checkpoint version")]
    UnsupportedVersion,
    /// Checkpoint bytes are not the exact current canonical format.
    #[error("the Windows USN checkpoint is invalid")]
    InvalidCheckpoint,
    /// The native root identity cannot be proven.
    #[error("the Windows USN root identity is unavailable")]
    RootIdentityUnavailable,
    /// The journal returned impossible or truncated metadata.
    #[error("the Windows USN journal metadata is invalid")]
    InvalidJournal,
    /// Ordinary path or volume I/O failed.
    #[error("Windows USN I/O failed: {0}")]
    Io(String),
    /// The native journal query failed.
    #[error("Windows USN query failed: {0}")]
    Native(String),
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn checkpoint_encoding_is_canonical_versioned_and_bounded() {
        let checkpoint = WindowsUsnCheckpoint {
            root_identity: NativeRootIdentity::from_bytes([7; 16]),
            journal_id: 19,
            next_usn: 23,
        };
        let bytes = checkpoint.to_bytes();
        assert!(matches!(
            WindowsUsnCheckpoint::from_bytes(&bytes),
            Ok(decoded) if decoded == checkpoint
        ));
        assert!(matches!(
            WindowsUsnCheckpoint::from_bytes(&bytes[..39]),
            Err(WindowsUsnError::InvalidCheckpoint)
        ));
        for version in [0, 2, u8::MAX] {
            let mut wrong_version = bytes[..8].to_vec();
            wrong_version[7] = version;
            assert!(matches!(
                WindowsUsnCheckpoint::from_bytes(&wrong_version),
                Err(WindowsUsnError::UnsupportedVersion)
            ));
        }
        let mut impossible = bytes;
        impossible[32..40].copy_from_slice(&u64::MAX.to_le_bytes());
        assert!(matches!(
            WindowsUsnCheckpoint::from_bytes(&impossible),
            Err(WindowsUsnError::InvalidCheckpoint)
        ));
    }
}
