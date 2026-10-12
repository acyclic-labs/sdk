use super::*;

pub(super) fn copy_path(
    path: &[Vec<u8>],
    extra: Option<&[u8]>,
    work: &crate::NativeMaterializationWork,
) -> Result<(MountPath, crate::NativeAllocation), MountSourceError> {
    use adapter::CaptureInputScratch;
    let mut scratch = adapter::NativeCaptureScratchAllocation::new(work)?;
    let count = path
        .len()
        .checked_add(usize::from(extra.is_some()))
        .ok_or(MountSourceError::Work(crate::WorkError::Overflow))?;
    let mut components = scratch.vec(count)?;
    for component in path.iter().map(Vec::as_slice).chain(extra) {
        scratch.items(1)?;
        let copied = crate::WorkCounters {
            bytes_copied: component.len() as u64,
            ..crate::WorkCounters::UNCHARGED
        };
        scratch.admit(copied)?;
        let mut bytes = scratch.vec(component.len())?;
        bytes.extend_from_slice(component);
        scratch.record(copied)?;
        components.push(bytes);
    }
    Ok((MountPath { components }, scratch.retain()))
}

pub(super) async fn file_id<A, O, D, S>(
    lazy: &LazyWorkspace<A, O, D, S>,
    path: &str,
    lookup: &LazyLookup,
    original: Option<&crate::NativeMaterializationWork>,
) -> Result<FileId, MountSourceError>
where
    A: AsyncAuthorityStore + Send + Sync,
    O: AsyncObjectStore + Send + Sync,
    D: DemandSource + 'static,
    S: LazyWorkspaceStore,
{
    let cancellation = CancellationToken::new();
    let mut work = match original {
        Some(original) => {
            crate::kernel::KernelAccounting::native(original, original.cancellation())
        }
        None => crate::kernel::KernelAccounting::standalone(WorkBudget::UNBOUNDED, &cancellation),
    };
    lazy.stable_file_id_for_lookup(path, lookup, &mut work)
        .await
        .map_err(lazy_error)
}

pub(super) fn mount_lookup(lookup: LazyLookup, file_id: FileId) -> MountLookup {
    match lookup {
        LazyLookup::Authored { stat, .. } => MountLookup {
            node: MountNode {
                file_id,
                kind: file_kind(stat.kind),
                logical_bytes: stat.logical_bytes.unwrap_or(0),
                link_count: stat.link_count,
                device: None,
            },
            metadata: workspace_metadata(stat.metadata),
        },
        LazyLookup::Shadow {
            record,
            metadata,
            source_link_count,
        } => MountLookup {
            node: MountNode {
                file_id,
                kind: file_kind(record.kind),
                logical_bytes: record_logical_bytes(record.payload),
                link_count: source_link_count,
                device: match record.payload {
                    FilePayload::Device { major, minor } => Some((major, minor)),
                    _ => None,
                },
            },
            metadata: workspace_metadata(metadata),
        },
        LazyLookup::Source { node, .. } => MountLookup {
            node: MountNode {
                file_id,
                kind: source_kind(node.kind),
                logical_bytes: node.logical_bytes.unwrap_or(0),
                link_count: node.link_count.unwrap_or(1),
                device: node.device,
            },
            metadata: source_metadata(node.metadata),
        },
    }
}

pub(super) const fn source_kind(kind: SourceNodeKind) -> MountNodeKind {
    match kind {
        SourceNodeKind::RegularFile => MountNodeKind::Regular,
        SourceNodeKind::Directory => MountNodeKind::Directory,
        SourceNodeKind::SymbolicLink => MountNodeKind::SymbolicLink,
        SourceNodeKind::Fifo => MountNodeKind::Fifo,
        SourceNodeKind::Socket => MountNodeKind::Socket,
        SourceNodeKind::CharacterDevice => MountNodeKind::CharacterDevice,
        SourceNodeKind::BlockDevice => MountNodeKind::BlockDevice,
        SourceNodeKind::Unsupported => MountNodeKind::Unsupported,
    }
}

pub(super) fn record_logical_bytes(payload: FilePayload) -> u64 {
    match payload {
        FilePayload::InlineRegular(bytes) => {
            u64::try_from(bytes.as_bytes().len()).unwrap_or(u64::MAX)
        }
        FilePayload::Regular { logical_bytes, .. }
        | FilePayload::SymbolicLink {
            target_bytes: logical_bytes,
            ..
        }
        | FilePayload::ReparsePoint {
            payload_bytes: logical_bytes,
            ..
        } => logical_bytes,
        FilePayload::Directory { .. } | FilePayload::Empty | FilePayload::Device { .. } => 0,
    }
}

pub(super) const fn file_kind(kind: FileKind) -> MountNodeKind {
    match kind {
        FileKind::Regular => MountNodeKind::Regular,
        FileKind::Directory => MountNodeKind::Directory,
        FileKind::SymbolicLink => MountNodeKind::SymbolicLink,
        FileKind::Fifo => MountNodeKind::Fifo,
        FileKind::Socket => MountNodeKind::Socket,
        FileKind::CharacterDevice => MountNodeKind::CharacterDevice,
        FileKind::BlockDevice => MountNodeKind::BlockDevice,
        FileKind::ReparsePoint | FileKind::MountBoundary => MountNodeKind::Unsupported,
    }
}

pub(super) fn workspace_metadata(metadata: WorkspaceMetadata) -> FileMetadata {
    FileMetadata {
        posix_mode: field(metadata.posix_mode),
        posix_uid: field(metadata.posix_uid),
        posix_gid: field(metadata.posix_gid),
        posix_flags: field(metadata.posix_flags),
        windows_attributes: field(metadata.windows_attributes),
        created_ns: field(metadata.created_ns),
        modified_ns: field(metadata.modified_ns),
        accessed_ns: field(metadata.accessed_ns),
        changed_ns: field(metadata.changed_ns),
        ..FileMetadata::default()
    }
}

pub(super) fn source_metadata(metadata: crate::demand::SourceMetadata) -> FileMetadata {
    FileMetadata {
        posix_mode: field(metadata.posix_mode),
        posix_uid: field(metadata.posix_uid),
        posix_gid: field(metadata.posix_gid),
        posix_flags: field(metadata.posix_flags),
        windows_attributes: field(metadata.windows_attributes),
        created_ns: field(
            metadata
                .created_ns
                .and_then(|value| i64::try_from(value).ok()),
        ),
        modified_ns: field(
            metadata
                .modified_ns
                .and_then(|value| i64::try_from(value).ok()),
        ),
        accessed_ns: field(
            metadata
                .accessed_ns
                .and_then(|value| i64::try_from(value).ok()),
        ),
        changed_ns: field(
            metadata
                .changed_ns
                .and_then(|value| i64::try_from(value).ok()),
        ),
        ..FileMetadata::default()
    }
}

pub(super) fn field<T>(value: Option<T>) -> MetadataField<T> {
    value.map_or(MetadataField::Unavailable, MetadataField::Value)
}

#[allow(
    clippy::needless_pass_by_value,
    reason = "used as a map_err adapter, which hands over the error by value"
)]
pub(super) fn lazy_error(error: LazyWorkspaceError) -> MountSourceError {
    let error = match error {
        LazyWorkspaceError::SpentWork(failure) => return lazy_error(failure.error),
        LazyWorkspaceError::OriginalSnapshotUnavailable => {
            return MountSourceError::OriginalSnapshotUnavailable;
        }
        LazyWorkspaceError::SelectedPublication(outcome) => {
            return MountSourceError::SelectedPublication(outcome);
        }
        LazyWorkspaceError::Demand(DemandError::Mount(error)) => return error,
        LazyWorkspaceError::Demand(DemandError::Io(error)) => {
            return MountSourceError::Io(error, None);
        }
        LazyWorkspaceError::NativeWork(error) => return MountSourceError::NativeWork(error),
        LazyWorkspaceError::NativeRecordIo(error, allocation) => {
            return MountSourceError::Io(error, allocation);
        }
        LazyWorkspaceError::NativeRecordJson(error, allocation) => {
            return MountSourceError::NativeRecordJson(error, allocation);
        }
        LazyWorkspaceError::NativeRecordLockTimeout(path, allocation) => {
            return MountSourceError::NativeRecordLockTimeout(path, allocation);
        }
        LazyWorkspaceError::NativeRecordOwnershipConflict(path, allocation) => {
            return MountSourceError::NativeRecordOwnershipConflict(path, allocation);
        }
        LazyWorkspaceError::NativeRecordMaterializationInProgress => {
            return MountSourceError::Indeterminate;
        }
        LazyWorkspaceError::NativeRecordIntegrity => {
            return MountSourceError::NativeText {
                kind: crate::native_mount::NativeSourceTextKind::Engine,
                text: "native record integrity verification failed",
            };
        }
        LazyWorkspaceError::NativeRecordContextTransaction(message, allocation) => {
            return MountSourceError::NativeRecordContextTransaction(message, allocation);
        }
        LazyWorkspaceError::NativeRecordCorrupt(path, allocation) => {
            return MountSourceError::NativeRecordCorrupt(path, allocation);
        }
        LazyWorkspaceError::NativeLedgerMismatch => {
            return MountSourceError::NativeWork(
                crate::materializer::NativeTreeMaterializationError::InvalidLayout,
            );
        }
        LazyWorkspaceError::NativeUnsupported => return MountSourceError::NativeUnsupported,
        LazyWorkspaceError::NativeConstruction { cause, .. } => {
            return MountSourceError::NativeWork(cause);
        }
        LazyWorkspaceError::Filesystem(error) => {
            return mount_engine_error(error);
        }
        LazyWorkspaceError::NamespacePath(crate::kernel::NamespacePathError::Work(error)) => {
            return MountSourceError::Work(error);
        }
        LazyWorkspaceError::NamespacePath(crate::kernel::NamespacePathError::Cancelled) => {
            return MountSourceError::Cancelled;
        }
        LazyWorkspaceError::NamespacePath(error) => return MountSourceError::NamespacePath(error),
        LazyWorkspaceError::WorkspaceCause(error) => {
            return MountSourceError::WorkspaceCause(error);
        }
        LazyWorkspaceError::WorkspacePrefixFailure { source, accounting } => {
            return MountSourceError::WorkspacePrefixFailure { source, accounting };
        }
        LazyWorkspaceError::NativeWorkspaceCause(cause) => {
            return MountSourceError::NativeWorkspaceCause(cause);
        }
        LazyWorkspaceError::Workspace(message)
        | LazyWorkspaceError::Store(message)
        | LazyWorkspaceError::Work(message) => return MountSourceError::Engine(message),
        error => error,
    };
    match error.semantic_error() {
        LazyWorkspaceError::NotFound => MountSourceError::NotFound,
        LazyWorkspaceError::Cancelled | LazyWorkspaceError::Demand(DemandError::Cancelled) => {
            MountSourceError::Cancelled
        }
        LazyWorkspaceError::Demand(DemandError::Work(error)) => MountSourceError::Work(*error),
        LazyWorkspaceError::NotRegularFile | LazyWorkspaceError::InvalidPageBound => {
            MountSourceError::Invalid(error.to_string())
        }
        LazyWorkspaceError::UnsupportedNode => MountSourceError::Unsupported(error.to_string()),
        // A held source file that changed since it opened is a stale handle.
        LazyWorkspaceError::StaleSource
        | LazyWorkspaceError::StaleCursor
        | LazyWorkspaceError::StaleIdentity
        | LazyWorkspaceError::Concurrent
        | LazyWorkspaceError::Demand(DemandError::StaleVersion) => MountSourceError::Stale,
        _ => MountSourceError::Engine(error.to_string()),
    }
}
