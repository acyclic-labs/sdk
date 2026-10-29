use super::*;

/// Complete namespace entry kind.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[cfg_attr(
    all(feature = "typescript", target_arch = "wasm32"),
    derive(serde::Serialize, tsify::Tsify)
)]
#[cfg_attr(
    all(feature = "typescript", target_arch = "wasm32"),
    serde(rename_all = "kebab-case")
)]
#[cfg_attr(feature = "napi-types", napi_derive::napi(string_enum = "kebab-case"))]
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

/// One immutable authenticated directory B+tree page.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum TreePage {
    /// Ordered file bindings.
    Leaf(Vec<TreeEntry>),
    /// Ordered child lower bounds.
    Internal(Vec<Child<LogicalName>>),
}

impl TreePage {
    /// Validates ordering, uniqueness, object kinds, and page bounds.
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
                validate_order(children.iter().map(|child| &child.first))?;
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

#[cfg(test)]
pub(crate) fn digest_object(kind: ObjectKind, digest: [u8; 32]) -> ObjectId {
    ObjectId {
        storage: None,
        kind,
        digest: Digest::from_bytes(digest),
    }
}
