use super::*;

pub(super) fn copy_path(
    path: &[Vec<u8>],
    extra: Option<&[u8]>,
    work: &crate::NativeMaterializationWork,
) -> Result<(MountPath, crate::NativeAllocation), MountSourceError> {
    let mut scratch_carrier = adapter::NativeDemandCarrier::Native(work);
    let mut scratch = adapter::InputBuffers::new(&mut scratch_carrier)?;
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
    Ok((
        MountPath { components },
        scratch
            .retain()
            .ok_or(MountSourceError::NativeUnsupported)?,
    ))
}

pub(super) async fn file_id<A, O, D, S>(
    lazy: &LazyWorkspace<A, O, D, S>,
    path: &str,
    lookup: &LazyLookup,
    work: &crate::NativeMaterializationWork,
) -> Result<FileId, MountSourceError>
where
    A: AsyncAuthorityStore + Send + Sync,
    O: AsyncObjectStore + Send + Sync,
    D: DemandSource + 'static,
    S: LazyWorkspaceStore,
{
    lazy.stable_file_id_for_lookup(
        path,
        lookup,
        &mut crate::kernel::KernelAccounting::native(work, work.cancellation()),
    )
    .await
    .map_err(lazy_error)
}

pub(super) fn mount_lookup(lookup: LazyLookup, file_id: FileId) -> MountLookup {
    let stat = lookup.stat();
    let metadata = workspace_metadata(lookup.metadata());
    let (link_count, device) = match lookup {
        LazyLookup::Authored { stat, .. } => (stat.link_count, None),
        LazyLookup::Shadow {
            record,
            source_link_count,
            ..
        } => (
            source_link_count,
            match record.payload {
                FilePayload::Device { major, minor } => Some((major, minor)),
                _ => None,
            },
        ),
        LazyLookup::Source { node, .. } => (node.link_count.unwrap_or(1), node.device),
    };
    MountLookup {
        node: MountNode {
            file_id,
            kind: source_kind(stat.kind),
            logical_bytes: stat.logical_bytes.unwrap_or(0),
            link_count,
            device,
        },
        metadata,
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

pub(super) fn field<T>(value: Option<T>) -> MetadataField<T> {
    value.map_or(MetadataField::Unavailable, MetadataField::Value)
}

pub(super) fn lazy_error(error: LazyWorkspaceError) -> MountSourceError {
    match error {
        LazyWorkspaceError::OriginalSnapshotUnavailable => {
            MountSourceError::OriginalSnapshotUnavailable
        }
        LazyWorkspaceError::Demand(DemandError::Mount(error)) => error,
        LazyWorkspaceError::Demand(DemandError::Io(error)) => MountSourceError::Io(error, None),
        LazyWorkspaceError::NativeRecordIo(error, allocation) => {
            MountSourceError::Io(error, allocation)
        }
        LazyWorkspaceError::NativeWork(error) => {
            MountSourceError::NativeWork(error)
        }
        LazyWorkspaceError::NativeRecordJson(error, allocation) => {
            MountSourceError::NativeRecordJson(error, allocation)
        }
        LazyWorkspaceError::NativeRecordLockTimeout(path, allocation) => {
            MountSourceError::NativeRecordLockTimeout(path, allocation)
        }
        LazyWorkspaceError::NativeRecordOwnershipConflict(path, allocation) => {
            MountSourceError::NativeRecordOwnershipConflict(path, allocation)
        }
        LazyWorkspaceError::NativeRecordMaterializationInProgress => {
            MountSourceError::Indeterminate
        }
        LazyWorkspaceError::NativeRecordIntegrity => {
            MountSourceError::Engine("native record integrity verification failed".into())
        }
        LazyWorkspaceError::NativeRecordContextTransaction(message, allocation) => {
            MountSourceError::NativeRecordContextTransaction(message, allocation)
        }
        LazyWorkspaceError::NativeRecordCorrupt(path, allocation) => {
            MountSourceError::NativeRecordCorrupt(path, allocation)
        }
        LazyWorkspaceError::NativeLedgerMismatch => {
            MountSourceError::NativeWork(crate::NativeTreeMaterializationError::InvalidLayout)
        }
        LazyWorkspaceError::Filesystem(error) => mount_engine_error(error),
        LazyWorkspaceError::NamespacePath(crate::kernel::NamespacePathError::Work(error)) => {
            MountSourceError::Work(error)
        }
        LazyWorkspaceError::NamespacePath(crate::kernel::NamespacePathError::Cancelled) => {
            MountSourceError::Cancelled
        }
        LazyWorkspaceError::NamespacePath(error) => MountSourceError::NamespacePath(error),
        LazyWorkspaceError::WorkspaceCause(error) => MountSourceError::WorkspaceCause(error),
        LazyWorkspaceError::Store(message) => MountSourceError::Engine(message.into()),
        error => match error.semantic_error() {
            LazyWorkspaceError::NotFound => MountSourceError::NotFound,
            LazyWorkspaceError::Cancelled | LazyWorkspaceError::Demand(DemandError::Cancelled) => {
                MountSourceError::Cancelled
            }
            LazyWorkspaceError::Demand(DemandError::Work(error)) => MountSourceError::Work(*error),
            LazyWorkspaceError::NotRegularFile | LazyWorkspaceError::InvalidPageBound => {
                MountSourceError::Invalid(error.to_string().into())
            }
            LazyWorkspaceError::UnsupportedNode => {
                MountSourceError::Unsupported(error.to_string().into())
            }
            LazyWorkspaceError::StaleSource
            | LazyWorkspaceError::StaleCursor
            | LazyWorkspaceError::StaleIdentity
            | LazyWorkspaceError::Concurrent
            | LazyWorkspaceError::Demand(DemandError::StaleVersion) => MountSourceError::Stale,
            _ => MountSourceError::Engine(error.to_string().into()),
        },
    }
}
