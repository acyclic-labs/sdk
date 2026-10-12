use super::*;

/// Creates the canonical provider-owned reference for a named workspace.
pub fn workspace_ref(provider: ProviderRef, name: &str) -> Result<WorkspaceRef> {
    acyclic_fs::WorkspaceName::new(name, acyclic_fs::WorkBudget::UNBOUNDED, &acyclic_fs::CancellationToken::new()).map(|receipt| receipt.value).map_err(|failure| failure.error).map_err(|error| Error::Invalid(error.to_string()))?;
    WorkspaceRef::new(provider, name.as_bytes().into(), None)
}

pub(in crate::filesystem) fn filesystem_key(key: &IdempotencyKey) -> FilesystemKey {
    let digest = blake3::hash(key.as_str().as_bytes());
    let mut bytes = [0; 16];
    bytes.copy_from_slice(&digest.as_bytes()[..16]);
    if bytes == [0; 16] {
        bytes[0] = 1;
    }
    FilesystemKey::from_bytes(bytes)
}

pub(in crate::filesystem) fn project_join_key(operation: crate::OperationId, io: Option<&PreparationIo>) -> Result<FilesystemKey> {
    use std::io::Write as _;
    if let Some(io) = io { io.charge(acyclic_fs::WorkCounters { items_examined: 1, ..Default::default() })?; }
    let mut buffer = [0_u8; 64];
    let mut output = buffer.as_mut_slice();
    write!(&mut output, "project-join:{operation}").map_err(|_| fork_preparer_io::failure(io,
        crate::wire::ErrorCode::Invalid, "project join retry key exceeds its fixed encoding"))?;
    let length = 64 - output.len();
    if let Some(io) = io { io.charge(acyclic_fs::WorkCounters { bytes_hashed: length as u64, ..Default::default() })?; }
    let digest = blake3::hash(&buffer[..length]);
    let mut bytes = [0_u8; 16];
    bytes.copy_from_slice(&digest.as_bytes()[..16]);
    if bytes == [0; 16] { bytes[0] = 1; }
    Ok(FilesystemKey::from_bytes(bytes))
}

pub(in crate::filesystem) fn content_metadata_path_io(path: &str, absolute: bool, io: Option<&PreparationIo>) -> Result<String> {
    content_identity_path_io(path, absolute, ".system/harness-file-metadata/", io)
}

pub(in crate::filesystem) fn content_identity_path_io(path: &str, absolute: bool, prefix: &str, io: Option<&PreparationIo>) -> Result<String> {
    const SUFFIX: &str = ".json";
    let capacity = usize::from(absolute) + prefix.len() + 64 + SUFFIX.len();
    let hash_work = acyclic_fs::WorkCounters { bytes_hashed: path.len() as u64, ..Default::default() };
    if let Some(io) = io { io.preflight(hash_work)?; }
    let hash = blake3::hash(path.as_bytes());
    if let Some(io) = io {
        io.charge(hash_work)?;
        io.retain_owned(capacity as u64)?;
        io.preflight(acyclic_fs::WorkCounters { bytes_copied: capacity as u64, ..Default::default() })?;
        io.allocation_attempt(capacity as u64)?;
    }
    let mut encoded = String::with_capacity(capacity);
    if absolute { encoded.push('/'); }
    encoded.push_str(prefix);
    encoded.push_str(&hash.to_hex());
    encoded.push_str(SUFFIX);
    if let Some(io) = io {
        if encoded.capacity() > capacity { io.retain_owned((encoded.capacity() - capacity) as u64)?; }
        io.charge(acyclic_fs::WorkCounters { bytes_copied: capacity as u64, ..Default::default() })?;
    }
    Ok(encoded)
}

pub(crate) fn map_error(error: WorkspaceError) -> Error {
    match error {
        WorkspaceError::NotFound => Error::NotFound("workspace path".into()),
        WorkspaceError::RetentionConflict
        | WorkspaceError::StaleGeneration
        | WorkspaceError::StaleSource
        | WorkspaceError::StaleIdentity => Error::Conflict(error.to_string()),
        WorkspaceError::Name(_)
        | WorkspaceError::Path(_)
        | WorkspaceError::NamespacePath(_)
        | WorkspaceError::ReadLimitExceeded
        | WorkspaceError::NotRegularFile
        | WorkspaceError::NotDirectory
        | WorkspaceError::ForeignGeneration
        | WorkspaceError::IncompatibleWorkspace
        | WorkspaceError::NoCommonAncestor
        | WorkspaceError::LineageLimit
        | WorkspaceError::JoinLimit
        | WorkspaceError::StaleTarget
        | WorkspaceError::InvalidMergeResolution
        | WorkspaceError::ChangeSetContinuity
        | WorkspaceError::ChangedPathLimit
        | WorkspaceError::EmptyContentSet
        | WorkspaceError::NotFork
        | WorkspaceError::ContentLengthOverflow
        | WorkspaceError::Work(_)
        | WorkspaceError::WorkMerge { .. } => Error::Invalid(error.to_string()),
        WorkspaceError::Cancelled(error) => Error::Storage(error.to_string()),
        WorkspaceError::Filesystem(error) => Error::Storage(error.to_string()),
        WorkspaceError::Engine(value) => Error::Storage(value),
    }
}
