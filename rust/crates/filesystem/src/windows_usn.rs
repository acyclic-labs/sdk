//! Conservative durable Windows USN restart continuity.
//!
//! The adapter proves only the strongest inexpensive case: the exact root,
//! volume journal, and volume-wide next USN are unchanged. Any journal advance
//! or ambiguity requires the canonical watcher baseline; it is never treated
//! as proof that the watched subtree was unchanged. A same-volume checkpoint
//! write advances the journal itself, so it cannot use this fast path.

use crate::NativeRootIdentity;
use crate::native_host::HostRoot;
use std::path::Path;

use acyclic_fs_windows::journal::{JournalSnapshot, query_journal};
use acyclic_fs_windows::windows_usn_codec::{WindowsUsnCheckpoint, WindowsUsnError};

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
    let (root_identity, journal) = observe_root(root)?;
    WindowsUsnCheckpoint::new(root_identity, journal.journal_id, journal.next_usn)
}

/// Proves unchanged restart continuity or requires a baseline.
///
/// # Errors
///
/// Returns only environmental/native failures. Every semantic mismatch is a
/// successful `BaselineRequired` outcome so callers cannot confuse baseline
/// with transport failure.
pub fn validate_windows_usn_checkpoint(
    root: &Path,
    checkpoint: WindowsUsnCheckpoint,
) -> Result<WindowsUsnContinuity, WindowsUsnError> {
    let (observed, journal) = observe_root(root)?;
    Ok(classify_continuity(observed, checkpoint, &journal))
}

fn classify_continuity(
    observed: NativeRootIdentity,
    checkpoint: WindowsUsnCheckpoint,
    journal: &JournalSnapshot,
) -> WindowsUsnContinuity {
    if observed != checkpoint.root_identity() {
        return WindowsUsnContinuity::BaselineRequired(WindowsUsnDiscontinuity::RootChanged);
    }
    if journal.journal_id != checkpoint.journal_id() {
        return WindowsUsnContinuity::BaselineRequired(WindowsUsnDiscontinuity::JournalChanged);
    }
    if checkpoint.next_usn() < journal.lowest_valid_usn {
        return WindowsUsnContinuity::BaselineRequired(WindowsUsnDiscontinuity::JournalTruncated);
    }
    if checkpoint.next_usn() > journal.next_usn {
        return WindowsUsnContinuity::BaselineRequired(WindowsUsnDiscontinuity::MarkerAhead);
    }
    if checkpoint.next_usn() != journal.next_usn {
        return WindowsUsnContinuity::BaselineRequired(WindowsUsnDiscontinuity::VolumeAdvanced);
    }
    WindowsUsnContinuity::Unchanged
}

/// Reads the root's identity and its volume's journal through one held
/// handle on the root itself: the journal is exactly the root's volume's
/// (mounted-folder volumes included), and querying through a directory
/// handle needs no volume access, so no administrator rights.
fn observe_root(root: &Path) -> Result<(NativeRootIdentity, JournalSnapshot), WindowsUsnError> {
    let canonical = root
        .canonicalize()
        .map_err(|error| WindowsUsnError::Io(error.to_string()))?;
    let root = HostRoot::open(&canonical, &mut crate::native_host::ordinary_work())
        .map_err(crate::native_host::ordinary_error)
        .map_err(|_| WindowsUsnError::RootIdentityUnavailable)?;
    Ok((root.identity(), query_journal(root.directory_handle())?))
}

#[cfg(test)]
mod tests {
    use super::{
        JournalSnapshot, NativeRootIdentity, Path, WindowsUsnCheckpoint, WindowsUsnContinuity,
        WindowsUsnDiscontinuity, WindowsUsnError, capture_windows_usn_checkpoint,
        classify_continuity, validate_windows_usn_checkpoint,
    };
    use proptest::prelude::*;

    #[test]
    fn every_journal_transition_is_total_and_fail_closed() {
        let root = NativeRootIdentity::from_bytes([1; 16]);
        let checkpoint = WindowsUsnCheckpoint::new(root, 2, 20).unwrap();
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

    proptest! {
        #[test]
        fn unchanged_requires_the_same_retained_volume_boundary(
            marker in 0_u64..=i64::MAX as u64,
            observed in proptest::option::of(0_u64..=i64::MAX as u64),
            lowest_valid in 0_u64..=i64::MAX as u64,
            same_root in any::<bool>(),
            same_journal in any::<bool>(),
        ) {
            let root = NativeRootIdentity::from_bytes([1; 16]);
            let checkpoint = WindowsUsnCheckpoint::new(root, 2, marker).unwrap();
            let observed_root = if same_root {
                root
            } else {
                NativeRootIdentity::from_bytes([3; 16])
            };
            let next_usn = observed.unwrap_or(marker);
            let journal = JournalSnapshot {
                journal_id: if same_journal { 2 } else { 4 },
                lowest_valid_usn: lowest_valid,
                next_usn,
            };
            prop_assert_eq!(
                classify_continuity(observed_root, checkpoint, &journal)
                    == WindowsUsnContinuity::Unchanged,
                same_root && same_journal && lowest_valid <= marker && next_usn == marker,
            );
        }
    }

    #[test]
    fn live_journal_proves_unchanged_restart_and_fences_a_write()
    -> Result<(), Box<dyn std::error::Error>> {
        let directory = tempfile::tempdir()?;
        let root = directory.path().join("watched");
        std::fs::create_dir(&root)?;
        let flush_change = |path: &Path| -> std::io::Result<()> {
            std::fs::write(path, b"changed")?;
            // FlushFileBuffers requires write access to the flushed handle.
            std::fs::OpenOptions::new()
                .write(true)
                .open(path)?
                .sync_all()
        };

        let checkpoint = match capture_windows_usn_checkpoint(&root) {
            Ok(checkpoint) => checkpoint,
            // The host volume may have no active journal. The contract then
            // refuses to certify continuity, and the caller takes the
            // canonical baseline; prove that refusal, not a volume feature.
            Err(WindowsUsnError::JournalInactive) => return Ok(()),
            Err(error) => return Err(error.into()),
        };
        // This fixture owns the interleaving, not the volume: another subtree
        // can advance its journal between two reads even while this root is
        // untouched. The exact unchanged case is proved on immutable journal
        // observations by the classifier tests above.
        flush_change(&directory.path().join("outside-watched-root"))?;
        assert!(matches!(
            validate_windows_usn_checkpoint(&root, checkpoint)?,
            WindowsUsnContinuity::BaselineRequired(_)
        ));

        let checkpoint = capture_windows_usn_checkpoint(&root)?;
        flush_change(&root.join("changed"))?;
        // A concurrent journal reset/truncation also requires a baseline;
        // the live host cannot promise one particular discontinuity reason.
        assert!(matches!(
            validate_windows_usn_checkpoint(&root, checkpoint)?,
            WindowsUsnContinuity::BaselineRequired(_)
        ));
        Ok(())
    }
}
