//! Conservative durable Windows USN restart continuity.
//!
//! The adapter proves only the strongest inexpensive case: the exact root,
//! volume journal, and volume-wide next USN are unchanged. Any journal advance
//! or ambiguity requires the canonical watcher baseline; it is never treated
//! as proof that the watched subtree was unchanged.

#![allow(unsafe_code)]

use crate::NativeRootIdentity;
use crate::capture_root_identity;
use std::ffi::OsString;
use std::fs::{File, OpenOptions};
use std::mem::size_of;
use std::os::windows::fs::OpenOptionsExt;
use std::os::windows::io::AsRawHandle;
use std::path::{Component, Path, Prefix};
use thiserror::Error;
use windows::Win32::Foundation::HANDLE;
use windows::Win32::Storage::FileSystem::{FILE_SHARE_DELETE, FILE_SHARE_READ, FILE_SHARE_WRITE};
use windows::Win32::System::IO::DeviceIoControl;
use windows::Win32::System::Ioctl::{FSCTL_QUERY_USN_JOURNAL, USN_JOURNAL_DATA_V0};

const CHECKPOINT_MAGIC: &[u8; 8] = b"ACYUSN\0\x01";
const CHECKPOINT_BYTES: usize = 40;

/// Durable conservative USN marker captured before a full authenticated scan.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct WindowsUsnCheckpoint {
    root_identity: NativeRootIdentity,
    journal_id: u64,
    next_usn: u64,
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
        if bytes.len() != CHECKPOINT_BYTES || &bytes[..8] != CHECKPOINT_MAGIC {
            return Err(WindowsUsnError::InvalidCheckpoint);
        }
        let mut root = [0_u8; 16];
        root.copy_from_slice(&bytes[8..24]);
        let journal_id = decode_u64(&bytes[24..32]);
        let next_usn = decode_u64(&bytes[32..40]);
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

/// Why a restart cannot skip the canonical authenticated baseline.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum WindowsUsnDiscontinuity {
    /// The watched directory identity changed.
    RootChanged,
    /// Windows replaced the volume journal.
    JournalChanged,
    /// The retained marker fell behind the journal's valid range.
    JournalTruncated,
    /// The marker is ahead of the current journal and cannot be authentic.
    MarkerAhead,
    /// At least one change occurred anywhere on the volume.
    VolumeAdvanced,
}

/// Conservative restart validation outcome.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum WindowsUsnContinuity {
    /// No volume change occurred after the checkpoint.
    Unchanged,
    /// The caller must perform the ordinary authenticated baseline handshake.
    BaselineRequired(WindowsUsnDiscontinuity),
}

/// Captures a marker before an authenticated baseline scan begins.
///
/// A consumer persists the marker only after the baseline and every queued
/// watcher change have been durably published. Capturing after publication is
/// unsafe because a change could enter the journal before its watcher callback
/// is observed.
///
/// # Errors
///
/// Rejects unsupported paths, unavailable journals, invalid journal values,
/// root identity failures, or native I/O errors.
pub fn capture_windows_usn_checkpoint(
    root: &Path,
) -> Result<WindowsUsnCheckpoint, WindowsUsnError> {
    let canonical = root
        .canonicalize()
        .map_err(|error| WindowsUsnError::Io(error.to_string()))?;
    let root_identity =
        capture_root_identity(&canonical).map_err(|_| WindowsUsnError::RootIdentityUnavailable)?;
    let journal = query_journal(&canonical)?;
    Ok(WindowsUsnCheckpoint {
        root_identity,
        journal_id: journal.journal_id,
        next_usn: journal.next_usn,
    })
}

/// Proves unchanged restart continuity or requires a baseline.
///
/// # Errors
///
/// Returns only environmental/native failures. Every semantic mismatch is a
/// successful `BaselineRequired` outcome so callers cannot confuse fallback
/// with transport failure.
pub fn validate_windows_usn_checkpoint(
    root: &Path,
    checkpoint: WindowsUsnCheckpoint,
) -> Result<WindowsUsnContinuity, WindowsUsnError> {
    let canonical = root
        .canonicalize()
        .map_err(|error| WindowsUsnError::Io(error.to_string()))?;
    let observed =
        capture_root_identity(&canonical).map_err(|_| WindowsUsnError::RootIdentityUnavailable)?;
    let journal = query_journal(&canonical)?;
    Ok(classify_continuity(observed, checkpoint, &journal))
}

fn classify_continuity(
    observed: NativeRootIdentity,
    checkpoint: WindowsUsnCheckpoint,
    journal: &JournalSnapshot,
) -> WindowsUsnContinuity {
    if observed != checkpoint.root_identity {
        return WindowsUsnContinuity::BaselineRequired(WindowsUsnDiscontinuity::RootChanged);
    }
    if journal.journal_id != checkpoint.journal_id {
        return WindowsUsnContinuity::BaselineRequired(WindowsUsnDiscontinuity::JournalChanged);
    }
    if checkpoint.next_usn < journal.lowest_valid_usn {
        return WindowsUsnContinuity::BaselineRequired(WindowsUsnDiscontinuity::JournalTruncated);
    }
    if checkpoint.next_usn > journal.next_usn {
        return WindowsUsnContinuity::BaselineRequired(WindowsUsnDiscontinuity::MarkerAhead);
    }
    if checkpoint.next_usn != journal.next_usn {
        return WindowsUsnContinuity::BaselineRequired(WindowsUsnDiscontinuity::VolumeAdvanced);
    }
    WindowsUsnContinuity::Unchanged
}

struct JournalSnapshot {
    journal_id: u64,
    lowest_valid_usn: u64,
    next_usn: u64,
}

fn query_journal(root: &Path) -> Result<JournalSnapshot, WindowsUsnError> {
    let volume = open_volume(root)?;
    let mut data = USN_JOURNAL_DATA_V0::default();
    let mut returned = 0_u32;
    let output_size = u32::try_from(size_of::<USN_JOURNAL_DATA_V0>())
        .map_err(|_| WindowsUsnError::InvalidJournal)?;
    let handle = HANDLE(volume.as_raw_handle().cast::<core::ffi::c_void>());
    // SAFETY: `volume` owns a valid volume handle for the duration of the call;
    // the output pointer references a correctly sized initialized structure;
    // no overlapped operation outlives these stack values.
    unsafe {
        DeviceIoControl(
            handle,
            FSCTL_QUERY_USN_JOURNAL,
            None,
            0,
            Some((&raw mut data).cast()),
            output_size,
            Some(&raw mut returned),
            None,
        )
    }
    .map_err(|error| WindowsUsnError::Native(error.to_string()))?;
    if returned < output_size || data.NextUsn < 0 || data.LowestValidUsn < 0 {
        return Err(WindowsUsnError::InvalidJournal);
    }
    Ok(JournalSnapshot {
        journal_id: data.UsnJournalID,
        lowest_valid_usn: u64::try_from(data.LowestValidUsn)
            .map_err(|_| WindowsUsnError::InvalidJournal)?,
        next_usn: u64::try_from(data.NextUsn).map_err(|_| WindowsUsnError::InvalidJournal)?,
    })
}

fn open_volume(root: &Path) -> Result<File, WindowsUsnError> {
    let drive = match root.components().next() {
        Some(Component::Prefix(prefix)) => match prefix.kind() {
            Prefix::Disk(letter) | Prefix::VerbatimDisk(letter) => letter,
            _ => return Err(WindowsUsnError::UnsupportedVolumePath),
        },
        _ => return Err(WindowsUsnError::UnsupportedVolumePath),
    };
    let mut path = OsString::from(r"\\.\");
    path.push(char::from(drive).to_string());
    path.push(":");
    let mut options = OpenOptions::new();
    options
        .read(true)
        .share_mode(FILE_SHARE_READ.0 | FILE_SHARE_WRITE.0 | FILE_SHARE_DELETE.0);
    options.open(path).map_err(|error| {
        if error.kind() == std::io::ErrorKind::PermissionDenied {
            WindowsUsnError::PermissionDenied
        } else {
            WindowsUsnError::Io(error.to_string())
        }
    })
}

fn decode_u64(bytes: &[u8]) -> u64 {
    let mut value = [0_u8; 8];
    value.copy_from_slice(bytes);
    u64::from_le_bytes(value)
}

/// Windows USN checkpoint failures.
#[derive(Debug, Error)]
pub enum WindowsUsnError {
    /// Checkpoint bytes are not the exact current canonical format.
    #[error("the Windows USN checkpoint is invalid")]
    InvalidCheckpoint,
    /// The root does not resolve to one local drive-letter volume.
    #[error("the Windows USN root is not on a supported local volume")]
    UnsupportedVolumePath,
    /// The native root identity cannot be proven.
    #[error("the Windows USN root identity is unavailable")]
    RootIdentityUnavailable,
    /// Querying the volume journal requires an elevated qualification host.
    #[error("the Windows USN volume journal requires administrator access")]
    PermissionDenied,
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
        let mut wrong_version = bytes;
        wrong_version[7] = 2;
        assert!(matches!(
            WindowsUsnCheckpoint::from_bytes(&wrong_version),
            Err(WindowsUsnError::InvalidCheckpoint)
        ));
        let mut impossible = bytes;
        impossible[32..40].copy_from_slice(&u64::MAX.to_le_bytes());
        assert!(matches!(
            WindowsUsnCheckpoint::from_bytes(&impossible),
            Err(WindowsUsnError::InvalidCheckpoint)
        ));
    }

    #[test]
    fn every_journal_transition_is_total_and_fail_closed() {
        let root = NativeRootIdentity::from_bytes([1; 16]);
        let checkpoint = WindowsUsnCheckpoint {
            root_identity: root,
            journal_id: 2,
            next_usn: 20,
        };
        let snapshot = |journal_id, lowest_valid_usn, next_usn| JournalSnapshot {
            journal_id,
            lowest_valid_usn,
            next_usn,
        };
        assert_eq!(
            classify_continuity(root, checkpoint, &snapshot(2, 10, 20)),
            WindowsUsnContinuity::Unchanged
        );
        assert_eq!(
            classify_continuity(
                NativeRootIdentity::from_bytes([3; 16]),
                checkpoint,
                &snapshot(2, 10, 20)
            ),
            WindowsUsnContinuity::BaselineRequired(WindowsUsnDiscontinuity::RootChanged)
        );
        assert_eq!(
            classify_continuity(root, checkpoint, &snapshot(3, 10, 20)),
            WindowsUsnContinuity::BaselineRequired(WindowsUsnDiscontinuity::JournalChanged)
        );
        assert_eq!(
            classify_continuity(root, checkpoint, &snapshot(2, 21, 21)),
            WindowsUsnContinuity::BaselineRequired(WindowsUsnDiscontinuity::JournalTruncated)
        );
        assert_eq!(
            classify_continuity(root, checkpoint, &snapshot(2, 10, 19)),
            WindowsUsnContinuity::BaselineRequired(WindowsUsnDiscontinuity::MarkerAhead)
        );
        assert_eq!(
            classify_continuity(root, checkpoint, &snapshot(2, 10, 21)),
            WindowsUsnContinuity::BaselineRequired(WindowsUsnDiscontinuity::VolumeAdvanced)
        );
    }

    #[test]
    #[ignore = "requires a live local Windows volume with a readable USN journal"]
    fn live_journal_proves_unchanged_restart_and_fences_a_write()
    -> Result<(), Box<dyn std::error::Error>> {
        let root = tempfile::tempdir()?;
        let checkpoint = capture_windows_usn_checkpoint(root.path())?;
        assert_eq!(
            validate_windows_usn_checkpoint(root.path(), checkpoint)?,
            WindowsUsnContinuity::Unchanged
        );
        let changed = root.path().join("changed");
        std::fs::write(&changed, b"changed")?;
        File::open(&changed)?.sync_all()?;
        assert_eq!(
            validate_windows_usn_checkpoint(root.path(), checkpoint)?,
            WindowsUsnContinuity::BaselineRequired(WindowsUsnDiscontinuity::VolumeAdvanced)
        );
        Ok(())
    }
}
