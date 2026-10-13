use super::*;

/// One within-volume zero-copy logical range clone request.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct FileCloneRequest {
    /// Existing source regular file.
    pub source: NamespacePath,
    /// Inclusive source byte offset.
    pub source_offset: u64,
    /// Existing destination regular file.
    pub destination: NamespacePath,
    /// Inclusive destination byte offset.
    pub destination_offset: u64,
    /// Positive logical byte count.
    pub length: u64,
}

/// One regular-file content change. `F` addresses files: an exact
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum ContentChange<F> {
    /// Replaces one range with caller bytes, preserving sparse untouched
    Write {
        /// Inclusive logical offset.
        offset: u64,
        /// Replacement bytes.
        bytes: acyclic_stream::OwnedBytes,
    },
    /// Changes logical length while preserving sparse semantics.
    Resize {
        /// New logical byte length.
        logical_bytes: u64,
    },
    /// Replaces one range with a hole or physically allocated zeros.
    ZeroRange {
        /// Replaced logical range.
        range: ByteRange,
        /// Preserve physical allocation instead of punching a hole.
        allocated: bool,
        /// Whether the replacement may extend logical file length.
        extend: bool,
    },
    /// Allocates sparse holes while preserving all existing content.
    Preallocate {
        /// Allocated logical range.
        range: ByteRange,
        /// Preserve logical file length.
        keep_size: bool,
    },
    /// Clones one range of another regular file by immutable extent
    CloneFrom {
        /// Existing source regular file.
        source: F,
        /// Inclusive source byte offset.
        source_offset: u64,
        /// Inclusive byte offset in the changed file.
        offset: u64,
        /// Positive logical byte count.
        length: u64,
    },
}

/// One independent change that [`Checkout::apply_group`] may apply together
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum GroupedChange {
    /// Creates one regular file with the caller's fresh stable identity and
    CreateFile {
        /// New namespace path.
        path: NamespacePath,
        /// Exact cross-profile metadata.
        metadata: Box<FileMetadata>,
        /// Fresh stable identity the caller chose, as [`FileId::new`] mints.
        file_id: FileId,
        /// Complete initial bytes.
        bytes: acyclic_stream::OwnedBytes,
    },
    /// Changes the content of one regular file by stable identity.
    Content {
        /// Existing regular-file identity.
        file_id: FileId,
        /// The content change.
        change: ContentChange<FileId>,
        /// Whether the change also stamps the file's content-change times.
        times: ContentTimes,
    },
}

/// What one successful [`GroupedChange`] produced.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum GroupedOutcome {
    /// The identity of the created file.
    Created(FileId),
    /// The content change applied.
    Changed,
}

/// Whether a content change also records its time in the changed file.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ContentTimes {
    /// Leaves the file's metadata unchanged.
    Preserve,
    /// Sets every represented modification and status-change time to this
    Stamp(i64),
}

pub(crate) enum ContentAddress<'a> {
    Path(&'a NamespacePath),
    File(FileId),
}

pub(crate) trait ContentFile: Sized {
    fn address(&self) -> ContentAddress<'_>;
}

impl ContentFile for NamespacePath {
    fn address(&self) -> ContentAddress<'_> {
        ContentAddress::Path(self)
    }
}
impl ContentFile for FileId {
    fn address(&self) -> ContentAddress<'_> {
        ContentAddress::File(*self)
    }
}

pub(super) fn content_input_path_bytes<F: ContentFile>(
    input: &(F, ContentChange<F>),
) -> Result<u64, FsError> {
    let owned = |file: &F| match file.address() {
        ContentAddress::Path(path) => path.retained_bytes(),
        ContentAddress::File(_) => 0,
    };
    let source = match &input.1 {
        ContentChange::CloneFrom { source, .. } => owned(source),
        _ => 0,
    };
    owned(&input.0)
        .checked_add(source)
        .ok_or_else(|| WorkError::Overflow.into())
}
