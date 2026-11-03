use super::*;

/// Creates the canonical provider-owned reference for a named workspace.
pub fn workspace_ref(provider: ProviderRef, name: &str) -> Result<WorkspaceRef> {
    acyclic_fs::WorkspaceName::new(
        name,
        &acyclic_fs::WorkBudget::UNBOUNDED,
        &acyclic_fs::CancellationToken::new(),
    )
    .map(|receipt| receipt.value)
    .map_err(|failure| failure.error)
    .map_err(|error| Error::Invalid(error.to_string()))?;
    WorkspaceRef::new(provider, name.as_bytes().into(), None)
}

/// Maps a Harness retry key to its stable Filesystem operation identity.
pub fn filesystem_key(key: &IdempotencyKey) -> FilesystemKey {
    let digest = blake3::hash(key.as_str().as_bytes());
    let mut bytes = [0; 16];
    bytes.copy_from_slice(&digest.as_bytes()[..16]);
    if bytes == [0; 16] {
        bytes[0] = 1;
    }
    FilesystemKey::from_bytes(bytes)
}

pub(in crate::filesystem) fn content_metadata_path_io(
    path: &str,
    absolute: bool,
    io: Option<&PreparationIo>,
) -> Result<String> {
    content_identity_path_io(path, absolute, ".system/harness-file-metadata/", io)
}

/// Internal publications already have a path identity. Their canonical metadata
/// is also their recovery receipt; only caller-selected upload keys need an index.
pub(in crate::filesystem) fn content_receipt_path_io(
    identity: &str,
    internal: bool,
    absolute: bool,
    io: Option<&PreparationIo>,
) -> Result<String> {
    if internal {
        content_metadata_path_io(identity, absolute, io)
    } else {
        content_identity_path_io(identity, absolute, ".system/harness-uploads/", io)
    }
}

fn content_identity_path_io(
    path: &str,
    absolute: bool,
    prefix: &str,
    io: Option<&PreparationIo>,
) -> Result<String> {
    const SUFFIX: &str = ".json";
    let capacity = usize::from(absolute) + prefix.len() + 64 + SUFFIX.len();
    let hash_work = acyclic_fs::WorkCounters {
        bytes_hashed: path.len() as u64,
        ..Default::default()
    };
    if let Some(io) = io {
        io.preflight(hash_work)?;
    }
    let hash = blake3::hash(path.as_bytes());
    if let Some(io) = io {
        io.charge(hash_work)?;
        io.retain_owned(capacity as u64)?;
        io.preflight(acyclic_fs::WorkCounters {
            bytes_copied: capacity as u64,
            ..Default::default()
        })?;
        io.allocation_attempt(capacity as u64)?;
    }
    let mut encoded = String::with_capacity(capacity);
    if absolute {
        encoded.push('/');
    }
    encoded.push_str(prefix);
    encoded.push_str(&hash.to_hex());
    encoded.push_str(SUFFIX);
    if let Some(io) = io {
        if encoded.capacity() > capacity {
            io.retain_owned((encoded.capacity() - capacity) as u64)?;
        }
        io.charge(acyclic_fs::WorkCounters {
            bytes_copied: capacity as u64,
            ..Default::default()
        })?;
    }
    Ok(encoded)
}

pub(crate) fn map_error(error: WorkspaceError) -> Error {
    use crate::wire::ErrorCode;
    match Error::workspace_error_code(&error) {
        ErrorCode::Unauthorized => Error::Unauthorized(error.to_string()),
        ErrorCode::Unsupported => {
            Error::FilesystemAccessConfiguration("unauthenticated principal facts")
        }
        ErrorCode::NotFound => Error::NotFound("workspace path".into()),
        ErrorCode::Conflict => Error::Conflict(error.to_string()),
        ErrorCode::Invalid => Error::Invalid(error.to_string()),
        ErrorCode::Cancelled => Error::Cancelled,
        _ => match error {
            WorkspaceError::Engine(value) => Error::Storage(value),
            WorkspaceError::Filesystem(error) => Error::Storage(error.to_string()),
            error => Error::Storage(error.to_string()),
        },
    }
}
