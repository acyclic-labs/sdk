use super::*;

pub(super) fn native_entry_fingerprint(path: &Path) -> Result<Option<[u8; 32]>, std::io::Error> {
    native_entry_fingerprint_scoped(path, true, None)
}

pub(super) fn native_entry_fingerprint_for_edit(
    path: &Path,
    edit: &MaterializationEdit,
) -> Result<Option<[u8; 32]>, std::io::Error> {
    native_entry_fingerprint_scoped(
        path,
        !matches!(edit, MaterializationEdit::SetMetadata { .. }),
        None,
    )
}

pub(super) fn native_entry_fingerprint_scoped(
    path: &Path,
    include_contents: bool,
    override_metadata: Option<&crate::WorkspaceMetadata>,
) -> Result<Option<[u8; 32]>, std::io::Error> {
    let metadata = match std::fs::symlink_metadata(path) {
        Ok(metadata) => metadata,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(None),
        Err(error) => return Err(error),
    };
    let mut hasher = blake3::Hasher::new();
    if !include_contents {
        hash_native_metadata_edit(&metadata, override_metadata, &mut hasher);
        #[cfg(windows)]
        {
            // MetadataExt does not expose a stable Windows file identity.
            // Hold an opened binding while obtaining the identity so a path
            // replacement cannot masquerade as the same metadata-only edit.
            let identity = if metadata.is_dir() {
                let directory =
                    cap_std::fs::Dir::open_ambient_dir(path, cap_std::ambient_authority())?;
                Some(crate::NativeRootIdentity::from_metadata(
                    &directory.dir_metadata()?,
                )?)
            } else if metadata.is_file() {
                let file = cap_std::fs::File::from_std(std::fs::File::open(path)?);
                Some(crate::NativeRootIdentity::from_metadata(&file.metadata()?)?)
            } else {
                None
            };
            if let Some(identity) = identity {
                hasher.update(&identity.to_bytes());
            }
        }
        return Ok(Some(*hasher.finalize().as_bytes()));
    }
    hash_native_entry(
        path,
        &metadata,
        &mut hasher,
        override_metadata,
        include_contents,
    )?;
    Ok(Some(*hasher.finalize().as_bytes()))
}

pub(super) fn hash_native_metadata_edit(
    metadata: &std::fs::Metadata,
    desired: Option<&crate::WorkspaceMetadata>,
    hasher: &mut blake3::Hasher,
) {
    let file_type = metadata.file_type();
    hasher.update(if file_type.is_symlink() {
        b"link"
    } else if file_type.is_dir() {
        b"directory"
    } else if file_type.is_file() {
        b"file"
    } else {
        b"special"
    });
    #[cfg(unix)]
    {
        use std::os::unix::fs::MetadataExt as _;
        for value in [metadata.dev(), metadata.ino()] {
            hasher.update(&value.to_le_bytes());
        }
        hasher.update(
            &u64::from(
                desired
                    .and_then(|value| value.posix_mode)
                    .unwrap_or_else(|| metadata.mode()),
            )
            .to_le_bytes(),
        );
    }
    #[cfg(windows)]
    {
        use std::os::windows::fs::MetadataExt as _;
        let attributes = desired
            .and_then(|value| value.windows_attributes)
            .unwrap_or_else(|| {
                let attributes = metadata.file_attributes();
                match desired {
                    Some(value) if metadata_readonly(value) => attributes | 1,
                    Some(_) => attributes & !1,
                    None => attributes,
                }
            });
        hasher.update(&attributes.to_le_bytes());
    }
    #[cfg(not(any(unix, windows)))]
    hasher.update(&[u8::from(desired.map_or_else(
        || metadata.permissions().readonly(),
        |value| metadata_readonly(value),
    ))]);
}

pub(super) fn hash_native_entry(
    path: &Path,
    metadata: &std::fs::Metadata,
    hasher: &mut blake3::Hasher,
    override_metadata: Option<&crate::WorkspaceMetadata>,
    include_contents: bool,
) -> Result<(), std::io::Error> {
    use std::io::Read as _;

    let file_type = metadata.file_type();
    hasher.update(if file_type.is_symlink() {
        b"link"
    } else if file_type.is_dir() {
        b"directory"
    } else if file_type.is_file() {
        b"file"
    } else {
        b"special"
    });
    if include_contents {
        hasher.update(&metadata.len().to_le_bytes());
    }
    hasher.update(&[u8::from(override_metadata.map_or_else(
        || metadata.permissions().readonly(),
        |value| metadata_readonly(value),
    ))]);
    hash_native_metadata(metadata, override_metadata, include_contents, hasher);

    if !include_contents {
        return Ok(());
    }
    if file_type.is_symlink() {
        hash_native_os_str(std::fs::read_link(path)?.as_os_str(), hasher);
    } else if file_type.is_dir() {
        let mut entries = std::fs::read_dir(path)?.collect::<Result<Vec<_>, _>>()?;
        entries.sort_by_key(|entry| entry.file_name());
        for entry in entries {
            hash_native_os_str(&entry.file_name(), hasher);
            let metadata = std::fs::symlink_metadata(entry.path())?;
            hash_native_entry(&entry.path(), &metadata, hasher, None, true)?;
        }
    } else if file_type.is_file() {
        let mut file = std::fs::File::open(path)?;
        let mut buffer = vec![0; 1024 * 1024];
        loop {
            let read = file.read(&mut buffer)?;
            if read == 0 {
                break;
            }
            let bytes = buffer
                .get(..read)
                .ok_or_else(|| std::io::Error::other("invalid read length"))?;
            hasher.update(bytes);
        }
    }
    Ok(())
}

#[cfg(all(unix, not(target_arch = "wasm32")))]
pub(super) fn hash_native_metadata(
    metadata: &std::fs::Metadata,
    desired: Option<&crate::WorkspaceMetadata>,
    include_contents: bool,
    hasher: &mut blake3::Hasher,
) {
    use std::os::unix::fs::MetadataExt as _;
    for value in [
        metadata.dev(),
        metadata.ino(),
        u64::from(
            desired
                .and_then(|value| value.posix_mode)
                .unwrap_or_else(|| metadata.mode()),
        ),
    ] {
        hasher.update(&value.to_le_bytes());
    }
    if include_contents {
        for value in [
            metadata.nlink(),
            u64::from(metadata.uid()),
            u64::from(metadata.gid()),
            metadata.rdev(),
        ] {
            hasher.update(&value.to_le_bytes());
        }
        hasher.update(&metadata.mtime().to_le_bytes());
        hasher.update(&metadata.mtime_nsec().to_le_bytes());
    }
}

#[cfg(all(windows, not(target_arch = "wasm32")))]
pub(super) fn hash_native_metadata(
    metadata: &std::fs::Metadata,
    desired: Option<&crate::WorkspaceMetadata>,
    include_contents: bool,
    hasher: &mut blake3::Hasher,
) {
    use std::os::windows::fs::MetadataExt as _;
    let attributes = desired.map_or_else(
        || metadata.file_attributes(),
        |value| {
            if let Some(attributes) = value.windows_attributes {
                attributes
            } else if metadata_readonly(value) {
                metadata.file_attributes() | 1
            } else {
                metadata.file_attributes() & !1
            }
        },
    );
    hasher.update(&u64::from(attributes).to_le_bytes());
    if include_contents {
        for value in [metadata.last_write_time(), metadata.file_size()] {
            hasher.update(&value.to_le_bytes());
        }
    }
}

#[cfg(all(not(unix), not(windows), not(target_arch = "wasm32")))]
pub(super) fn hash_native_metadata(
    _metadata: &std::fs::Metadata,
    _desired: Option<&crate::WorkspaceMetadata>,
    _include_contents: bool,
    _hasher: &mut blake3::Hasher,
) {
}

pub(super) fn native_metadata(metadata: &std::fs::Metadata) -> crate::WorkspaceMetadata {
    let mut value = crate::WorkspaceMetadata::from_engine(crate::kernel::FileMetadata::default());
    #[cfg(unix)]
    {
        use std::os::unix::fs::MetadataExt as _;
        value.posix_mode = Some(metadata.mode());
    }
    #[cfg(windows)]
    {
        use std::os::windows::fs::MetadataExt as _;
        value.windows_attributes = Some(metadata.file_attributes());
    }
    value
}
fn metadata_readonly(metadata: &crate::WorkspaceMetadata) -> bool {
    metadata
        .windows_attributes
        .is_some_and(|value| value & 1 != 0)
        || metadata.posix_mode.is_some_and(|value| value & 0o222 == 0)
}

pub(super) fn apply_native_metadata(
    path: &Path,
    desired: &crate::WorkspaceMetadata,
) -> Result<(), NativeTreeMaterializationError> {
    #[cfg(windows)]
    if let Some(attributes) = desired.windows_attributes {
        acyclic_native_runtime::set_file_attributes(path, attributes)?;
        return Ok(());
    }
    let mut permissions = std::fs::symlink_metadata(path)?.permissions();
    #[cfg(unix)]
    if let Some(mode) = desired.posix_mode {
        use std::os::unix::fs::PermissionsExt as _;
        permissions.set_mode(mode);
    }
    #[cfg(not(unix))]
    permissions.set_readonly(metadata_readonly(desired));
    std::fs::set_permissions(path, permissions)?;
    Ok(())
}

#[cfg(all(unix, not(target_arch = "wasm32")))]
pub(super) fn hash_native_os_str(value: &std::ffi::OsStr, hasher: &mut blake3::Hasher) {
    use std::os::unix::ffi::OsStrExt as _;
    hasher.update(value.as_bytes());
}

#[cfg(all(windows, not(target_arch = "wasm32")))]
pub(super) fn hash_native_os_str(value: &std::ffi::OsStr, hasher: &mut blake3::Hasher) {
    use std::os::windows::ffi::OsStrExt as _;
    for code_unit in value.encode_wide() {
        hasher.update(&code_unit.to_le_bytes());
    }
}

#[cfg(all(not(unix), not(windows), not(target_arch = "wasm32")))]
pub(super) fn hash_native_os_str(value: &std::ffi::OsStr, hasher: &mut blake3::Hasher) {
    hasher.update(value.to_string_lossy().as_bytes());
}
