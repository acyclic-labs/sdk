use super::*;

/// What a mounted checkout holds for a listed directory.
pub(super) struct MountedDirectory {
    /// Whether the checkout's directory stands in for the source's.
    pub(super) replaces_source: bool,
    /// Whether the checkout holds the directory at all.
    pub(super) authored: bool,
}

pub(super) fn lazy_store_read_work() -> WorkCounters {
    WorkCounters {
        object_probes: 1,
        backend_read_operations: 1,
        items_examined: 1,
        ..WorkCounters::default()
    }
}

pub(super) fn lazy_store_write_work() -> WorkCounters {
    WorkCounters {
        backend_write_operations: 1,
        items_examined: 1,
        ..WorkCounters::default()
    }
}

impl From<crate::WorkError> for LazyWorkspaceError {
    fn from(error: crate::WorkError) -> Self {
        Self::Demand(DemandError::Work(error))
    }
}

pub(super) fn source_kind(kind: crate::kernel::FileKind) -> SourceNodeKind {
    match kind {
        crate::kernel::FileKind::Directory => SourceNodeKind::Directory,
        crate::kernel::FileKind::Regular => SourceNodeKind::RegularFile,
        crate::kernel::FileKind::SymbolicLink => SourceNodeKind::SymbolicLink,
        crate::kernel::FileKind::Fifo => SourceNodeKind::Fifo,
        crate::kernel::FileKind::Socket => SourceNodeKind::Socket,
        crate::kernel::FileKind::CharacterDevice => SourceNodeKind::CharacterDevice,
        crate::kernel::FileKind::BlockDevice => SourceNodeKind::BlockDevice,
        crate::kernel::FileKind::ReparsePoint | crate::kernel::FileKind::MountBoundary => {
            SourceNodeKind::Unsupported
        }
    }
}

pub(super) fn record_logical_bytes(record: FileRecord) -> Option<u64> {
    match record.payload {
        FilePayload::InlineRegular(bytes) => u64::try_from(bytes.as_bytes().len()).ok(),
        FilePayload::Regular { logical_bytes, .. }
        | FilePayload::SymbolicLink {
            target_bytes: logical_bytes,
            ..
        }
        | FilePayload::ReparsePoint {
            payload_bytes: logical_bytes,
            ..
        } => Some(logical_bytes),
        FilePayload::Directory { .. } | FilePayload::Empty | FilePayload::Device { .. } => None,
    }
}

pub(super) fn source_file_metadata(source: crate::demand::SourceMetadata) -> FileMetadata {
    let time = |value: Option<i128>| {
        value
            .and_then(|value| i64::try_from(value).ok())
            .map_or(MetadataField::Unavailable, MetadataField::Value)
    };
    FileMetadata {
        posix_mode: metadata_u32(source.posix_mode),
        posix_uid: metadata_u32(source.posix_uid),
        posix_gid: metadata_u32(source.posix_gid),
        posix_flags: metadata_u64(source.posix_flags),
        windows_attributes: metadata_u32(source.windows_attributes),
        created_ns: time(source.created_ns),
        modified_ns: time(source.modified_ns),
        accessed_ns: time(source.accessed_ns),
        changed_ns: time(source.changed_ns),
        ..FileMetadata::default()
    }
}

pub(super) fn metadata_u32(value: Option<u32>) -> MetadataField<u32> {
    value.map_or(MetadataField::Unavailable, MetadataField::Value)
}

pub(super) fn metadata_u64(value: Option<u64>) -> MetadataField<u64> {
    value.map_or(MetadataField::Unavailable, MetadataField::Value)
}

pub(super) fn path_priority(path: &str) -> u64 {
    let digest = blake3::hash(path.as_bytes());
    let mut bytes = [0_u8; 8];
    if let Some(prefix) = digest.as_bytes().get(..8) {
        bytes.copy_from_slice(prefix);
    }
    u64::from_be_bytes(bytes)
}

pub(super) fn shadow_priority(file_id: FileId) -> u64 {
    let digest = blake3::hash(&file_id.into_bytes());
    let mut bytes = [0_u8; 8];
    bytes.copy_from_slice(&digest.as_bytes()[..8]);
    u64::from_le_bytes(bytes)
}

pub(super) fn parent_path(path: &str) -> Option<&str> {
    let index = path.rfind('/')?;
    if index == 0 {
        Some("/")
    } else {
        path.get(..index)
    }
}

/// Joins `name`, decoded by its declared encoding, onto a portable parent path.
pub(super) fn exactify_path_order(left: &str, right: &str) -> std::cmp::Ordering {
    left.matches('/')
        .count()
        .cmp(&right.matches('/').count())
        .then_with(|| left.cmp(right))
}

pub(crate) fn retained_directory_page_bytes<T>(
    entries: &Vec<T>,
    mut names: impl Iterator<Item = usize>,
) -> Result<u64, LazyWorkspaceError> {
    let slots = entries
        .capacity()
        .checked_mul(std::mem::size_of::<T>())
        .ok_or(crate::WorkError::Overflow)?;
    let names = names
        .try_fold(0_usize, |total, bytes| total.checked_add(bytes))
        .ok_or(crate::WorkError::Overflow)?;
    Ok(slots.checked_add(names).ok_or(crate::WorkError::Overflow)? as u64)
}

pub(super) fn exactify_batch_key(
    snapshot_id: LazySnapshotId,
    root: &str,
    index: u64,
) -> IdempotencyKey {
    let mut hasher = blake3::Hasher::new();
    hasher.update(b"acyclic-fs-lazy-exactify-batch-v1\0");
    hasher.update(&snapshot_id.into_bytes());
    hasher.update(root.as_bytes());
    hasher.update(&index.to_le_bytes());
    let mut bytes = [0; 16];
    bytes.copy_from_slice(&hasher.finalize().as_bytes()[..16]);
    IdempotencyKey::from_bytes(bytes)
}

pub(super) fn accept_receipt<T, E: Into<LazyWorkspaceError>>(
    result: crate::MeasuredResult<OperationReceipt<T>, E>,
    accounting: &mut crate::kernel::KernelAccounting<'_>,
) -> Result<T, LazyWorkspaceError> {
    let (result, spent) = match result {
        Ok(receipt) => (Ok(receipt.value), receipt.work),
        Err(failure) => (Err(failure.error.into()), *failure.work),
    };
    let (allocations, work, budget, _) = accounting
        .standalone_parts()
        .ok_or(LazyWorkspaceError::Concurrent)?;
    *work = work.with_backend(spent, allocations.live_bytes())?;
    work.verify(budget)?;
    if result.is_ok() {
        accounting.check()?;
    }
    result
}
