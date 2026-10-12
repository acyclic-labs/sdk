use super::*;


/// What a mounted checkout holds for a listed directory.
pub(super) struct MountedDirectory {
    /// Whether the checkout's directory stands in for the source's.
    pub(super) replaces_source: bool,
    /// Whether the checkout holds the directory at all.
    pub(super) authored: bool,
}

pub(super) fn store_error(error: impl std::fmt::Display) -> LazyWorkspaceError {
    LazyWorkspaceError::Store(error.to_string())
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

pub(super) fn measured_lazy_store_read(
    budget: WorkBudget,
    cancellation: &CancellationToken,
) -> Result<(), LazyWorkspaceError> {
    cancellation
        .check()
        .map_err(|_| LazyWorkspaceError::Cancelled)?;
    lazy_store_read_work()
        .verify(budget)
        .map_err(|error| LazyWorkspaceError::Work(error.to_string()))
}

pub(super) fn measured_lazy_store_write(
    budget: WorkBudget,
    cancellation: &CancellationToken,
) -> Result<(), LazyWorkspaceError> {
    cancellation
        .check()
        .map_err(|_| LazyWorkspaceError::Cancelled)?;
    lazy_store_write_work()
        .verify(budget)
        .map_err(|error| LazyWorkspaceError::Work(error.to_string()))
}

impl From<crate::WorkError> for LazyWorkspaceError {
    fn from(error: crate::WorkError) -> Self {
        Self::Work(error.to_string())
    }
}

pub(super) fn counter_overflow() -> LazyWorkspaceError {
    LazyWorkspaceError::Work("counter overflow".to_owned())
}

pub(super) fn workspace_error(error: WorkspaceError) -> LazyWorkspaceError {
    match error {
        WorkspaceError::StaleIdentity => LazyWorkspaceError::StaleIdentity,
        WorkspaceError::Cancelled(_) => LazyWorkspaceError::Cancelled,
        WorkspaceError::Work(error) => LazyWorkspaceError::Work(error.to_string()),
        error => LazyWorkspaceError::Workspace(error.to_string()),
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

pub(super) fn authored_entry(entry: WorkspaceDirectoryEntry) -> LazyDirectoryEntry {
    LazyDirectoryEntry {
        name: entry.name,
        kind: source_kind(entry.kind),
        authored: true,
        source: None,
        unobserved: false,
    }
}

/// Joins `name`, decoded by its declared encoding, onto a portable parent path.
pub(crate) fn logical_child_path(parent: &str, name: &LogicalName) -> Option<String> {
    let component = name.unicode_text()?;
    Some(if parent == "/" {
        format!("/{component}")
    } else {
        format!("{parent}/{component}")
    })
}

pub(super) fn exactify_path_order(left: &str, right: &str) -> std::cmp::Ordering {
    left.matches('/')
        .count()
        .cmp(&right.matches('/').count())
        .then_with(|| left.cmp(right))
}

pub(super) fn account_work(
    current: WorkCounters,
    additional: WorkCounters,
    budget: WorkBudget,
) -> Result<WorkCounters, LazyWorkspaceError> {
    let combined = current.checked_add(additional)?;
    combined.verify(budget)?;
    Ok(combined)
}

pub(super) fn account_nested_with_live_memory(
    current: WorkCounters,
    nested: WorkCounters,
    live_bytes: u64,
    budget: WorkBudget,
) -> Result<WorkCounters, LazyWorkspaceError> {
    let combined = current.with_backend(nested, live_bytes)?;
    combined.verify(budget)?;
    Ok(combined)
}

pub(super) fn queue_exactify_mutation(
    pending: &mut Vec<AuthoredMutation>,
    mutation: AuthoredMutation,
    pending_bytes: &mut u64,
    retained_bytes: &mut u64,
    work: &mut WorkCounters,
    budget: WorkBudget,
) -> Result<(), LazyWorkspaceError> {
    let (AuthoredMutation::CreateFileFromContent { path, .. }
    | AuthoredMutation::CreateDirectory { path, .. }) = &mutation
    else {
        return Err(LazyWorkspaceError::UnsupportedNode);
    };
    let name_bytes = path.components().iter().try_fold(0_u64, |sum, name| {
        sum.checked_add(u64::try_from(name.retained_bytes()).ok()?)
    });
    let path_bytes = u64::try_from(path.components().len())
        .ok()
        .and_then(|count| {
            count.checked_mul(u64::try_from(std::mem::size_of::<LogicalName>()).ok()?)
        })
        .and_then(|bytes| bytes.checked_add(name_bytes?))
        .ok_or_else(counter_overflow)?;
    let item_bytes =
        u64::try_from(std::mem::size_of::<AuthoredMutation>()).map_err(|_| counter_overflow())?;
    let growth = u64::from(pending.len() == pending.capacity())
        .checked_mul(item_bytes)
        .and_then(|bytes| bytes.checked_add(path_bytes))
        .ok_or_else(counter_overflow)?;
    let prospective = retained_bytes
        .checked_add(growth)
        .ok_or_else(counter_overflow)?;
    work.peak_allocation_bytes = work.peak_allocation_bytes.max(prospective);
    work.verify(budget)?;
    let old_capacity = pending.capacity();
    pending
        .try_reserve_exact(1)
        .map_err(|_| LazyWorkspaceError::Work("exactify mutation allocation failed".to_owned()))?;
    let new_items = pending.capacity().saturating_sub(old_capacity);
    let actual_growth = u64::try_from(new_items)
        .ok()
        .and_then(|items| items.checked_mul(item_bytes))
        .and_then(|bytes| bytes.checked_add(path_bytes))
        .ok_or_else(counter_overflow)?;
    *pending_bytes = pending_bytes
        .checked_add(actual_growth)
        .ok_or_else(counter_overflow)?;
    *retained_bytes = retained_bytes
        .checked_add(actual_growth)
        .ok_or_else(counter_overflow)?;
    work.peak_allocation_bytes = work.peak_allocation_bytes.max(*retained_bytes);
    work.verify(budget)?;
    pending.push(mutation);
    Ok(())
}

pub(super) fn reserve_exactify_path_slot(
    paths: &mut Vec<String>,
    retained_bytes: &mut u64,
    work: &mut WorkCounters,
    budget: WorkBudget,
) -> Result<(), LazyWorkspaceError> {
    if paths.len() < paths.capacity() {
        return Ok(());
    }
    let item_bytes =
        u64::try_from(std::mem::size_of::<String>()).map_err(|_| counter_overflow())?;
    let old_bytes = u64::try_from(paths.capacity())
        .ok()
        .and_then(|count| count.checked_mul(item_bytes))
        .ok_or_else(counter_overflow)?;
    let target = paths.len().checked_add(1).ok_or_else(counter_overflow)?;
    let requested_bytes = u64::try_from(target)
        .ok()
        .and_then(|count| count.checked_mul(item_bytes))
        .ok_or_else(counter_overflow)?;
    let copied = u64::try_from(paths.len())
        .ok()
        .and_then(|count| count.checked_mul(item_bytes))
        .ok_or_else(counter_overflow)?;
    let mut attempted = work.checked_add(WorkCounters {
        bytes_copied: copied,
        allocation_operations: 1,
        ..WorkCounters::default()
    })?;
    attempted.peak_allocation_bytes = attempted.peak_allocation_bytes.max(
        retained_bytes
            .checked_add(requested_bytes)
            .ok_or_else(counter_overflow)?,
    );
    attempted.verify(budget)?;
    paths
        .try_reserve_exact(1)
        .map_err(|_| LazyWorkspaceError::Work("exactify path allocation failed".to_owned()))?;
    let new_bytes = u64::try_from(paths.capacity())
        .ok()
        .and_then(|count| count.checked_mul(item_bytes))
        .ok_or_else(counter_overflow)?;
    *retained_bytes = retained_bytes
        .checked_sub(old_bytes)
        .and_then(|value| value.checked_add(new_bytes))
        .ok_or_else(counter_overflow)?;
    attempted.peak_allocation_bytes = attempted.peak_allocation_bytes.max(*retained_bytes);
    attempted.verify(budget)?;
    *work = attempted;
    Ok(())
}

pub(super) fn account_transient_string(
    work: WorkCounters,
    value: &String,
    live_bytes: u64,
    budget: WorkBudget,
) -> Result<WorkCounters, LazyWorkspaceError> {
    let bytes = u64::try_from(value.capacity()).map_err(|_| counter_overflow())?;
    let copied = u64::try_from(value.len()).map_err(|_| counter_overflow())?;
    let mut delta = WorkCounters {
        bytes_copied: copied,
        allocation_operations: u64::from(bytes != 0),
        ..WorkCounters::default()
    };
    delta.peak_allocation_bytes = live_bytes.checked_add(bytes).ok_or_else(counter_overflow)?;
    account_work(work, delta, budget)
}

pub(super) fn retained_directory_page_bytes(
    capacity: usize,
    entry_bytes: usize,
    mut names: impl Iterator<Item = usize>,
) -> Result<u64, LazyWorkspaceError> {
    let slots = capacity
        .checked_mul(entry_bytes)
        .ok_or_else(counter_overflow)?;
    let names = names
        .try_fold(0_usize, |total, bytes| total.checked_add(bytes))
        .ok_or_else(counter_overflow)?;
    u64::try_from(slots.checked_add(names).ok_or_else(counter_overflow)?)
        .map_err(|_| counter_overflow())
}

pub(super) fn reserve_lazy_directory_page(
    entries: &mut Vec<LazyDirectoryEntry>,
    count: usize,
    temporary_bytes: u64,
    work: &mut WorkCounters,
    budget: WorkBudget,
) -> Result<u64, LazyWorkspaceError> {
    let requested = count
        .checked_mul(std::mem::size_of::<LazyDirectoryEntry>())
        .and_then(|bytes| u64::try_from(bytes).ok())
        .ok_or_else(counter_overflow)?;
    let mut attempted = work.checked_add(WorkCounters {
        allocation_operations: u64::from(count != 0),
        ..WorkCounters::default()
    })?;
    attempted.peak_allocation_bytes = attempted.peak_allocation_bytes.max(
        temporary_bytes
            .checked_add(requested)
            .ok_or_else(counter_overflow)?,
    );
    attempted.verify(budget)?;
    entries
        .try_reserve_exact(count)
        .map_err(|_| LazyWorkspaceError::Work("directory page allocation failed".to_owned()))?;
    let allocated = entries
        .capacity()
        .checked_mul(std::mem::size_of::<LazyDirectoryEntry>())
        .and_then(|bytes| u64::try_from(bytes).ok())
        .ok_or_else(counter_overflow)?;
    attempted.peak_allocation_bytes = attempted.peak_allocation_bytes.max(
        temporary_bytes
            .checked_add(allocated)
            .ok_or_else(counter_overflow)?,
    );
    attempted.verify(budget)?;
    *work = attempted;
    temporary_bytes
        .checked_add(allocated)
        .ok_or_else(counter_overflow)
}

pub(super) fn exactify_path_vector_bytes(paths: &Vec<String>) -> Result<u64, LazyWorkspaceError> {
    u64::try_from(paths.capacity())
        .ok()
        .and_then(|count| count.checked_mul(u64::try_from(std::mem::size_of::<String>()).ok()?))
        .ok_or_else(counter_overflow)
}

pub(super) fn retain_exactify_string(
    value: &String,
    retained_bytes: &mut u64,
    work: &mut WorkCounters,
    budget: WorkBudget,
) -> Result<(), LazyWorkspaceError> {
    let capacity = u64::try_from(value.capacity()).map_err(|_| counter_overflow())?;
    let mut delta = WorkCounters {
        bytes_copied: u64::try_from(value.len()).map_err(|_| counter_overflow())?,
        allocation_operations: u64::from(capacity != 0),
        ..WorkCounters::default()
    };
    *retained_bytes = retained_bytes
        .checked_add(capacity)
        .ok_or_else(counter_overflow)?;
    delta.peak_allocation_bytes = *retained_bytes;
    *work = account_work(*work, delta, budget)?;
    Ok(())
}

pub(super) fn remaining_work(
    current: WorkCounters,
    budget: WorkBudget,
) -> Result<WorkBudget, LazyWorkspaceError> {
    current
        .remaining(budget)
        .map_err(|error| LazyWorkspaceError::Work(error.to_string()))
}

pub(super) fn exactify_error(stage: &str, path: &str, error: LazyWorkspaceError) -> LazyWorkspaceError {
    match error {
        LazyWorkspaceError::Cancelled | LazyWorkspaceError::Demand(DemandError::Cancelled) => {
            LazyWorkspaceError::Cancelled
        }
        error @ LazyWorkspaceError::Work(_) => error,
        error => {
            LazyWorkspaceError::Workspace(format!("exactify could not {stage} {path}: {error}"))
        }
    }
}

pub(super) fn exactify_batch_key(snapshot_id: LazySnapshotId, root: &str, index: u64) -> IdempotencyKey {
    let mut hasher = blake3::Hasher::new();
    hasher.update(b"acyclic-fs-lazy-exactify-batch-v1\0");
    hasher.update(&snapshot_id.into_bytes());
    hasher.update(root.as_bytes());
    hasher.update(&index.to_le_bytes());
    let mut bytes = [0; 16];
    bytes.copy_from_slice(&hasher.finalize().as_bytes()[..16]);
    IdempotencyKey::from_bytes(bytes)
}

