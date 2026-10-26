use super::{DependencyRegion, ExtentSeekTarget, FileId, LogicalName};
use std::cmp::Ordering;

// File identity is the first component so a changed record can query all of
// its captured preconditions with one BTreeMap range, without scanning reads.
#[derive(Eq, Ord, PartialEq, PartialOrd)]
enum Detail<'a> {
    Record,
    Metadata,
    Length,
    Content(u64, u64),
    Seek(u64, &'a ExtentSeekTarget),
    Name(&'a LogicalName),
    Range(&'a Option<LogicalName>, u32),
}

impl DependencyRegion {
    pub(crate) const fn file_id(&self) -> FileId {
        match self {
            Self::FileRecord(id) | Self::Metadata(id) | Self::FileLength(id) => *id,
            Self::ContentRange { file_id, .. } | Self::SparseSeek { file_id, .. } => *file_id,
            Self::DirectoryName { directory_id, .. }
            | Self::DirectoryRange { directory_id, .. } => *directory_id,
        }
    }

    fn detail(&self) -> Detail<'_> {
        match self {
            Self::FileRecord(_) => Detail::Record,
            Self::Metadata(_) => Detail::Metadata,
            Self::FileLength(_) => Detail::Length,
            Self::ContentRange { offset, length, .. } => Detail::Content(*offset, *length),
            Self::SparseSeek { offset, target, .. } => Detail::Seek(*offset, target),
            Self::DirectoryName { name, .. } => Detail::Name(name),
            Self::DirectoryRange {
                after,
                maximum_entries,
                ..
            } => Detail::Range(after, *maximum_entries),
        }
    }
}

impl Ord for DependencyRegion {
    fn cmp(&self, other: &Self) -> Ordering {
        self.file_id()
            .cmp(&other.file_id())
            .then_with(|| self.detail().cmp(&other.detail()))
    }
}

impl PartialOrd for DependencyRegion {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}
