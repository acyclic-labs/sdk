//! Canonical signed filesystem scope labels.

use serde::{Deserialize, Serialize};

/// Prefix of an owner-issued exact-file read capability.
pub const FILE_READ_PREFIX: &str = "file:read:";
/// Prefix of an owner-issued subtree read capability.
pub const DIRECTORY_READ_PREFIX: &str = "directory:read:";
/// Structured filesystem grants carried in harness signed scopes.
pub const SCOPE_PREFIX: &str = "filesystem:";

/// Version of the directory and exact-file read label format.
const CONTENT_READ_VERSION: &str = "v1:";

/// What a directory or exact-file read label grants.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ContentRead {
    /// Every path under one directory of one volume.
    Directory,
    /// One file version of one volume.
    File,
}

impl ContentRead {
    const fn prefix(self) -> &'static str {
        match self {
            Self::Directory => DIRECTORY_READ_PREFIX,
            Self::File => FILE_READ_PREFIX,
        }
    }
}

/// A directory or exact-file read label. The volume's identity digest is
/// part of the label, so authority over a set of volumes can narrow it
/// without knowing its directory or file.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ContentReadLabel {
    /// The granted content kind.
    pub kind: ContentRead,
    /// Identity digest of the volume, as in the volume operation labels.
    pub volume: [u8; 32],
    /// Digest of the granted directory or file within that volume.
    pub target: [u8; 32],
}

/// A label in the directory or exact-file read family that is not in the
/// current format. It grants nothing and is refused, never read as some
/// other volume's grant.
#[derive(Clone, Copy, Debug, Eq, PartialEq, thiserror::Error)]
#[error("content read label is not in the v1 format")]
pub struct UnversionedContentRead;

impl ContentReadLabel {
    /// The one spelling of this label.
    #[must_use]
    pub fn to_label(&self) -> String {
        let mut label = String::with_capacity(
            self.kind.prefix().len() + CONTENT_READ_VERSION.len() + 2 * 64 + 1,
        );
        label.push_str(self.kind.prefix());
        label.push_str(CONTENT_READ_VERSION);
        label.push_str(&hex::encode(self.volume));
        label.push(':');
        label.push_str(&hex::encode(self.target));
        label
    }

    /// Parse `label`: `None` outside both families, an error for a label in
    /// either family that is not exactly the current format.
    pub fn parse(label: &str) -> Result<Option<Self>, UnversionedContentRead> {
        let Some((kind, rest)) = [ContentRead::Directory, ContentRead::File]
            .into_iter()
            .find_map(|kind| Some((kind, label.strip_prefix(kind.prefix())?)))
        else {
            return Ok(None);
        };
        let digest = |text: &str| {
            let mut digest = [0; 32];
            hex::decode_to_slice(text, &mut digest).map_err(|_| UnversionedContentRead)?;
            // One spelling: uppercase hex is not the canonical label.
            if hex::encode(digest) != text {
                return Err(UnversionedContentRead);
            }
            Ok(digest)
        };
        let (volume, target) = rest
            .strip_prefix(CONTENT_READ_VERSION)
            .and_then(|rest| rest.split_once(':'))
            .ok_or(UnversionedContentRead)?;
        Ok(Some(Self {
            kind,
            volume: digest(volume)?,
            target: digest(target)?,
        }))
    }
}

/// Whether a signed-scope label carries filesystem authority.
#[must_use]
pub fn is_filesystem_capability(label: &str) -> bool {
    [
        SCOPE_PREFIX,
        FILE_READ_PREFIX,
        DIRECTORY_READ_PREFIX,
        VolumeOperation::Read.capability_prefix(),
        VolumeOperation::Write.capability_prefix(),
        VolumeOperation::Delete.capability_prefix(),
    ]
    .iter()
    .any(|prefix| label.starts_with(prefix))
}

/// Independently granted whole-volume content operations in harness signed scopes.
#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum VolumeOperation {
    /// Resolve exact file bytes.
    Read,
    /// Create a new file version.
    Write,
    /// Remove an existing file. Does not grant creation or replacement.
    Delete,
}

impl VolumeOperation {
    /// Canonical signed-scope vocabulary, shared by every adapter.
    pub const fn capability_prefix(self) -> &'static str {
        match self {
            Self::Read => "volume:read:",
            Self::Write => "volume:write:",
            Self::Delete => "volume:delete:",
        }
    }
}
