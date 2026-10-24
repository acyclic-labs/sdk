//! Fixed-size journal evidence is part of the generation-bound checkpoint.
//! It never lives in a sidecar whose cursor could outlive its name delta.
use super::{NameCut, SearchError};
use serde::{Deserialize, Serialize};

/// A consumed native journal position bound to one logical source and root.
/// Only a producer that has accepted every preceding record may publish it.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(try_from = "StoredCheckpoint", into = "StoredCheckpoint")]
pub struct NameJournalCheckpoint(StoredCheckpoint);

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(tag = "kind", deny_unknown_fields)]
enum StoredCheckpoint {
    WindowsUsn {
        source_identity: [u8; 16],
        root_identity: [u8; 16],
        journal_id: u64,
        next_usn: u64,
    },
}

impl NameJournalCheckpoint {
    /// Records the first unapplied USN after accepted records. The logical
    /// source identity and held-root identity must both survive restart.
    ///
    /// # Errors
    /// Rejects a cursor outside the signed native USN domain.
    pub fn windows_usn(
        source_identity: [u8; 16],
        root_identity: [u8; 16],
        journal_id: u64,
        next_usn: u64,
    ) -> Result<Self, SearchError> {
        StoredCheckpoint::WindowsUsn {
            source_identity,
            root_identity,
            journal_id,
            next_usn,
        }
        .try_into()
    }

    /// The logical source to which this evidence belongs.
    pub fn source_identity(self) -> [u8; 16] {
        match self.0 {
            StoredCheckpoint::WindowsUsn {
                source_identity, ..
            } => source_identity,
        }
    }

    /// Returns the native root, journal identity, and first unapplied USN.
    pub fn windows_usn_position(self) -> ([u8; 16], u64, u64) {
        match self.0 {
            StoredCheckpoint::WindowsUsn {
                root_identity,
                journal_id,
                next_usn,
                ..
            } => (root_identity, journal_id, next_usn),
        }
    }

    pub(super) fn validate_cut(self, cut: &NameCut) -> Result<(), SearchError> {
        if matches!(cut, NameCut::Lazy(cut) if cut.source.identity == self.source_identity()) {
            Ok(())
        } else {
            Err(SearchError::Invalid(
                "journal checkpoint belongs to another source".into(),
            ))
        }
    }

    pub(super) fn follows(self, previous: Self) -> bool {
        match (self.0, previous.0) {
            (
                StoredCheckpoint::WindowsUsn {
                    source_identity,
                    root_identity,
                    journal_id,
                    next_usn,
                },
                StoredCheckpoint::WindowsUsn {
                    source_identity: old_source,
                    root_identity: old_root,
                    journal_id: old_journal,
                    next_usn: old_usn,
                },
            ) => {
                source_identity == old_source
                    && root_identity == old_root
                    && journal_id == old_journal
                    && next_usn >= old_usn
            }
        }
    }
}

impl TryFrom<StoredCheckpoint> for NameJournalCheckpoint {
    type Error = SearchError;
    fn try_from(value: StoredCheckpoint) -> Result<Self, Self::Error> {
        match value {
            StoredCheckpoint::WindowsUsn { next_usn, .. } if next_usn > i64::MAX as u64 => {
                Err(SearchError::Invalid("invalid journal USN".into()))
            }
            value => Ok(Self(value)),
        }
    }
}

impl From<NameJournalCheckpoint> for StoredCheckpoint {
    fn from(value: NameJournalCheckpoint) -> Self {
        value.0
    }
}
