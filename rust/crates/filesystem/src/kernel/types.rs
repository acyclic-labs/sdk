//! Typed invariants shared by authenticated namespace algorithms.

use crate::foundation::{Digest, FileId};
use crate::model::FilesystemProfile;
use crate::storage::{ObjectId, ObjectKind};
use thiserror::Error;

/// Canonical host-name representation.
#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd)]
pub enum NameEncoding {
    /// Portable UTF-8 scalar sequence.
    Utf8,
    /// Raw POSIX bytes excluding NUL and `/`.
    PosixBytes,
    /// Little-endian UTF-16 code units excluding NUL, `/`, and `\`.
    WindowsUtf16Le,
}

impl NameEncoding {
    pub(crate) const fn tag(self) -> u8 {
        match self {
            Self::Utf8 => 1,
            Self::PosixBytes => 2,
            Self::WindowsUtf16Le => 3,
        }
    }

    pub(crate) const fn from_tag(tag: u8) -> Result<Self, TreePageError> {
        match tag {
            1 => Ok(Self::Utf8),
            2 => Ok(Self::PosixBytes),
            3 => Ok(Self::WindowsUtf16Le),
            value => Err(TreePageError::UnknownNameEncoding(value)),
        }
    }
}

/// One separator-free canonical logical name.
#[derive(Clone, Debug, Eq, Ord, PartialEq, PartialOrd)]
pub struct LogicalName {
    encoding: NameEncoding,
    bytes: Vec<u8>,
}

impl LogicalName {
    /// Validates a bounded name in its declared host representation.
    ///
    /// # Errors
    ///
    /// Rejects empty, oversized, malformed, NUL, or separator-bearing names.
    pub fn new(
        encoding: NameEncoding,
        bytes: Vec<u8>,
        maximum_bytes: u32,
    ) -> Result<Self, TreePageError> {
        if bytes.is_empty() {
            return Err(TreePageError::EmptyName);
        }
        if u32::try_from(bytes.len()).unwrap_or(u32::MAX) > maximum_bytes {
            return Err(TreePageError::NameTooLarge);
        }
        match encoding {
            NameEncoding::Utf8 => {
                let value = std::str::from_utf8(&bytes).map_err(|_| TreePageError::InvalidName)?;
                if value == "." || value == ".." || value.contains('/') || value.contains('\0') {
                    return Err(TreePageError::InvalidName);
                }
            }
            NameEncoding::PosixBytes => {
                if bytes.contains(&0) || bytes.contains(&b'/') || bytes == b"." || bytes == b".." {
                    return Err(TreePageError::InvalidName);
                }
            }
            NameEncoding::WindowsUtf16Le => {
                if !bytes.len().is_multiple_of(2) {
                    return Err(TreePageError::InvalidName);
                }
                let units = bytes
                    .chunks_exact(2)
                    .map(|pair| u16::from_le_bytes([pair[0], pair[1]]));
                if bytes == [b'.', 0]
                    || bytes == [b'.', 0, b'.', 0]
                    || units.clone().any(|unit| {
                        unit == 0 || unit == u16::from(b'/') || unit == u16::from(b'\\')
                    })
                    || char::decode_utf16(units).any(|decoded| decoded.is_err())
                {
                    return Err(TreePageError::InvalidName);
                }
            }
        }
        Ok(Self { encoding, bytes })
    }

    /// Returns the canonical representation kind.
    #[must_use]
    pub const fn encoding(&self) -> NameEncoding {
        self.encoding
    }

    /// Borrows canonical name bytes.
    #[must_use]
    pub fn as_bytes(&self) -> &[u8] {
        &self.bytes
    }

    /// Comparison key for `CaseSensitivity::ProfileFolded` lookup and
    /// collision detection.
    ///
    /// Only [`NameEncoding::Utf8`] names are folded (via full Unicode
    /// case-folding, `str::to_lowercase`); other encodings return their raw
    /// bytes unchanged, so `ProfileFolded` behaves like `Sensitive` for
    /// `PosixBytes`/`WindowsUtf16Le` names. This never changes the physical,
    /// byte-ordered on-disk representation — it is a comparison-only key used
    /// by callers that already hold the name, not a new canonical form.
    #[must_use]
    pub fn case_fold_key(&self) -> Vec<u8> {
        match self.encoding {
            NameEncoding::Utf8 => {
                // `LogicalName::new` already rejected non-UTF-8 bytes for
                // this encoding, so this is exact, not lossy.
                let Ok(value) = std::str::from_utf8(&self.bytes) else {
                    return self.bytes.clone();
                };
                value.to_lowercase().into_bytes()
            }
            NameEncoding::PosixBytes | NameEncoding::WindowsUtf16Le => self.bytes.clone(),
        }
    }
}

/// Complete namespace entry kind.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum FileKind {
    /// Ordinary byte file.
    Regular,
    /// Directory namespace.
    Directory,
    /// Symbolic link.
    SymbolicLink,
    /// FIFO.
    Fifo,
    /// Socket namespace entry.
    Socket,
    /// Character device.
    CharacterDevice,
    /// Block device.
    BlockDevice,
    /// Windows reparse point not represented as a symbolic link.
    ReparsePoint,
    /// Opaque boundary delegated to another mounted volume.
    MountBoundary,
}

impl FileKind {
    /// Whether this kind has an exact representation in one volume profile.
    #[must_use]
    pub const fn is_supported_by_profile(self, profile: FilesystemProfile) -> bool {
        match self {
            Self::Regular | Self::Directory | Self::SymbolicLink | Self::MountBoundary => true,
            Self::Fifo | Self::Socket | Self::CharacterDevice | Self::BlockDevice => {
                matches!(profile, FilesystemProfile::Posix)
            }
            Self::ReparsePoint => matches!(profile, FilesystemProfile::Windows),
        }
    }

    pub(crate) const fn tag(self) -> u8 {
        match self {
            Self::Regular => 1,
            Self::Directory => 2,
            Self::SymbolicLink => 3,
            Self::Fifo => 4,
            Self::Socket => 5,
            Self::CharacterDevice => 6,
            Self::BlockDevice => 7,
            Self::ReparsePoint => 8,
            Self::MountBoundary => 9,
        }
    }

    pub(crate) const fn from_tag(tag: u8) -> Result<Self, TreePageError> {
        match tag {
            1 => Ok(Self::Regular),
            2 => Ok(Self::Directory),
            3 => Ok(Self::SymbolicLink),
            4 => Ok(Self::Fifo),
            5 => Ok(Self::Socket),
            6 => Ok(Self::CharacterDevice),
            7 => Ok(Self::BlockDevice),
            8 => Ok(Self::ReparsePoint),
            9 => Ok(Self::MountBoundary),
            value => Err(TreePageError::UnknownFileKind(value)),
        }
    }
}

/// One name-to-file binding in a directory leaf page.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct TreeEntry {
    /// Canonical logical name.
    pub name: LogicalName,
    /// Path-independent file record identity; hard links reuse it.
    pub file_id: FileId,
    /// Namespace kind used for bounded routing and restoration.
    pub kind: FileKind,
}

/// One lower-bound-to-page binding in an internal page.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct TreeChild {
    /// Exact first name represented by the child subtree.
    pub first_name: LogicalName,
    /// Authenticated tree-page identity.
    pub page: ObjectId,
}

/// One immutable authenticated directory B+tree page.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum TreePage {
    /// Ordered file bindings.
    Leaf(Vec<TreeEntry>),
    /// Ordered child lower bounds.
    Internal(Vec<TreeChild>),
}

impl TreePage {
    /// Validates ordering, uniqueness, object kinds, and page bounds.
    ///
    /// # Errors
    ///
    /// Rejects malformed pages before hashing or publication.
    pub fn validate(&self, maximum_items: u32) -> Result<(), TreePageError> {
        let count = match self {
            Self::Leaf(entries) => {
                validate_order(entries.iter().map(|entry| &entry.name))?;
                entries.len()
            }
            Self::Internal(children) => {
                if children.is_empty() {
                    return Err(TreePageError::EmptyInternalPage);
                }
                validate_order(children.iter().map(|child| &child.first_name))?;
                if children
                    .iter()
                    .any(|child| child.page.kind != ObjectKind::TreePage)
                {
                    return Err(TreePageError::WrongChildKind);
                }
                children.len()
            }
        };
        if u32::try_from(count).unwrap_or(u32::MAX) > maximum_items {
            return Err(TreePageError::TooManyItems);
        }
        Ok(())
    }
}

fn validate_order<'a>(
    mut names: impl Iterator<Item = &'a LogicalName>,
) -> Result<(), TreePageError> {
    let Some(mut previous) = names.next() else {
        return Ok(());
    };
    for name in names {
        if previous >= name {
            return Err(TreePageError::NamesNotStrictlyOrdered);
        }
        previous = name;
    }
    Ok(())
}

/// Authenticated tree-page invariant failures.
#[derive(Clone, Copy, Debug, Eq, Error, PartialEq)]
pub enum TreePageError {
    /// Logical names cannot be empty.
    #[error("logical name is empty")]
    EmptyName,
    /// Name exceeds the configured encoded-byte bound.
    #[error("logical name exceeds its byte bound")]
    NameTooLarge,
    /// Name bytes are malformed or contain a forbidden separator/component.
    #[error("logical name is invalid for its declared encoding")]
    InvalidName,
    /// Decoder encountered an unknown name representation.
    #[error("unknown logical name encoding {0}")]
    UnknownNameEncoding(u8),
    /// Decoder encountered an unknown file kind.
    #[error("unknown file kind {0}")]
    UnknownFileKind(u8),
    /// Names must be canonical, strictly ordered, and unique.
    #[error("tree page names are not strictly ordered")]
    NamesNotStrictlyOrdered,
    /// Internal pages must contain at least one routed child.
    #[error("internal tree page is empty")]
    EmptyInternalPage,
    /// Internal child points at another object class.
    #[error("internal tree child is not a tree page")]
    WrongChildKind,
    /// Page exceeds its admitted item bound.
    #[error("tree page exceeds its item bound")]
    TooManyItems,
    /// Canonical bytes contain an invalid digest or field.
    #[error("invalid canonical tree page field: {0}")]
    InvalidCanonicalField(&'static str),
}

pub(crate) fn digest_object(kind: ObjectKind, digest: [u8; 32]) -> ObjectId {
    ObjectId {
        kind,
        digest: Digest::from_bytes(digest),
    }
}

/// Physical representation of one logical byte interval.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum ExtentKind {
    /// Unallocated bytes that read as zero.
    Hole,
    /// Physically allocated bytes whose canonical value is zero.
    AllocatedZero,
    /// Bytes stored in one immutable blob span.
    Content {
        /// Typed immutable blob.
        object: ObjectId,
        /// First byte used from the blob.
        object_offset: u64,
    },
}

/// One non-empty logical file interval.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct Extent {
    /// Inclusive logical byte offset.
    pub offset: u64,
    /// Positive logical byte length.
    pub length: u64,
    /// Physical representation.
    pub kind: ExtentKind,
}

/// One lower-bound-to-page entry in an extent B+tree.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ExtentChild {
    /// First logical byte represented by the child.
    pub first_offset: u64,
    /// Exclusive logical end represented by the child.
    pub end_offset: u64,
    /// Authenticated extent page.
    pub page: ObjectId,
}

/// One immutable authenticated extent B+tree page.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum ExtentPage {
    /// Ordered non-overlapping extents.
    Leaf(Vec<Extent>),
    /// Ordered child lower bounds.
    Internal(Vec<ExtentChild>),
}

impl ExtentPage {
    /// Validates bounds, ordering, non-overlap, and typed child/content identities.
    ///
    /// # Errors
    ///
    /// Rejects malformed extent pages before hashing or publication.
    pub fn validate(&self, maximum_items: u32) -> Result<(), ExtentPageError> {
        let count = match self {
            Self::Leaf(extents) => {
                let mut previous_end = None;
                for extent in extents {
                    if extent.length == 0 {
                        return Err(ExtentPageError::ZeroLength);
                    }
                    let end = extent
                        .offset
                        .checked_add(extent.length)
                        .ok_or(ExtentPageError::RangeOverflow)?;
                    if previous_end.is_some_and(|prior| extent.offset != prior) {
                        return Err(ExtentPageError::NonContiguous);
                    }
                    if let ExtentKind::Content {
                        object,
                        object_offset,
                    } = extent.kind
                    {
                        if object.kind != ObjectKind::Blob {
                            return Err(ExtentPageError::WrongContentKind);
                        }
                        object_offset
                            .checked_add(extent.length)
                            .ok_or(ExtentPageError::RangeOverflow)?;
                    }
                    previous_end = Some(end);
                }
                extents.len()
            }
            Self::Internal(children) => {
                if children.is_empty() {
                    return Err(ExtentPageError::EmptyInternalPage);
                }
                if children
                    .windows(2)
                    .any(|pair| pair[0].end_offset != pair[1].first_offset)
                {
                    return Err(ExtentPageError::NonContiguous);
                }
                if children
                    .iter()
                    .any(|child| child.first_offset >= child.end_offset)
                {
                    return Err(ExtentPageError::InvalidChildRange);
                }
                if children
                    .iter()
                    .any(|child| child.page.kind != ObjectKind::ExtentPage)
                {
                    return Err(ExtentPageError::WrongChildKind);
                }
                children.len()
            }
        };
        if u32::try_from(count).unwrap_or(u32::MAX) > maximum_items {
            return Err(ExtentPageError::TooManyItems);
        }
        Ok(())
    }
}

/// Authenticated extent-page invariant failures.
#[derive(Clone, Copy, Debug, Eq, Error, PartialEq)]
pub enum ExtentPageError {
    /// Extents must represent at least one logical byte.
    #[error("extent length is zero")]
    ZeroLength,
    /// Logical or content-offset arithmetic overflowed.
    #[error("extent range overflowed")]
    RangeOverflow,
    /// Extents/children overlap, have a gap, or are out of order.
    #[error("extent page ranges are not contiguous")]
    NonContiguous,
    /// Content extent references another object class.
    #[error("extent content is not a blob object")]
    WrongContentKind,
    /// Internal page has no routed children.
    #[error("internal extent page is empty")]
    EmptyInternalPage,
    /// Internal child has an empty or reversed range.
    #[error("extent child range is empty or reversed")]
    InvalidChildRange,
    /// Internal child references another object class.
    #[error("extent child is not an extent page")]
    WrongChildKind,
    /// Page exceeds the admitted item bound.
    #[error("extent page exceeds its item bound")]
    TooManyItems,
    /// Decoder encountered an unknown physical representation.
    #[error("unknown extent kind {0}")]
    UnknownKind(u8),
}

#[cfg(test)]
#[path = "tests/types.rs"]
mod tests;
