//! Stable identities for already-open native filesystem roots.

/// Stable native identity of one held filesystem root.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct NativeRootIdentity {
    pub(crate) device: u64,
    pub(crate) object: u64,
}

impl NativeRootIdentity {
    /// Derives identity from an already-open root handle.
    ///
    /// # Errors
    ///
    /// Returns an I/O error when this platform cannot report a stable root
    /// identity.
    pub fn from_file(file: &std::fs::File) -> std::io::Result<Self> {
        native_root_identity(file)
    }

    pub(crate) fn from_metadata(metadata: &cap_std::fs::Metadata) -> std::io::Result<Self> {
        native_metadata_identity(metadata)
    }

    /// Identifies the real directory `path` currently names, without
    /// following its final component or opening it for reading. It accepts
    /// exactly the directories a held root may be opened from, so comparing
    /// the result with a held root's identity revalidates that root cheaply.
    pub(crate) fn of_root_path(path: &std::path::Path) -> std::io::Result<Self> {
        native_root_path_identity(path)
    }

    /// Returns the canonical platform-neutral 16-byte identity encoding.
    #[must_use]
    pub fn to_bytes(self) -> [u8; 16] {
        let mut bytes = [0_u8; 16];
        bytes[..8].copy_from_slice(&self.device.to_le_bytes());
        bytes[8..].copy_from_slice(&self.object.to_le_bytes());
        bytes
    }

    /// Decodes the exact canonical identity representation.
    #[must_use]
    pub fn from_bytes(bytes: [u8; 16]) -> Self {
        let mut device = [0_u8; 8];
        device.copy_from_slice(&bytes[..8]);
        let mut object = [0_u8; 8];
        object.copy_from_slice(&bytes[8..]);
        Self {
            device: u64::from_le_bytes(device),
            object: u64::from_le_bytes(object),
        }
    }
}

#[cfg(unix)]
fn native_metadata_identity(
    metadata: &cap_std::fs::Metadata,
) -> std::io::Result<NativeRootIdentity> {
    use cap_std::fs::MetadataExt;
    Ok(NativeRootIdentity {
        device: metadata.dev(),
        object: metadata.ino(),
    })
}

#[cfg(windows)]
fn native_metadata_identity(
    metadata: &cap_std::fs::Metadata,
) -> std::io::Result<NativeRootIdentity> {
    use cap_primitives::fs::_WindowsByHandle;
    let device = metadata.volume_serial_number().ok_or_else(|| {
        std::io::Error::new(
            std::io::ErrorKind::Unsupported,
            "host volume identity is unavailable",
        )
    })?;
    let object = metadata.file_index().ok_or_else(|| {
        std::io::Error::new(
            std::io::ErrorKind::Unsupported,
            "host file identity is unavailable",
        )
    })?;
    Ok(NativeRootIdentity {
        device: u64::from(device),
        object,
    })
}

#[cfg(not(any(unix, windows)))]
fn native_metadata_identity(_: &cap_std::fs::Metadata) -> std::io::Result<NativeRootIdentity> {
    Err(std::io::Error::new(
        std::io::ErrorKind::Unsupported,
        "stable host file identity is unavailable on this platform",
    ))
}

#[cfg(unix)]
fn native_root_identity(file: &std::fs::File) -> std::io::Result<NativeRootIdentity> {
    use std::os::unix::fs::MetadataExt;
    let metadata = file.metadata()?;
    Ok(NativeRootIdentity {
        device: metadata.dev(),
        object: metadata.ino(),
    })
}

#[cfg(windows)]
fn native_root_identity(file: &std::fs::File) -> std::io::Result<NativeRootIdentity> {
    native_metadata_identity(&cap_std::fs::Metadata::from_file(file)?)
}

#[cfg(any(unix, windows))]
fn not_a_real_directory() -> std::io::Error {
    std::io::Error::new(
        std::io::ErrorKind::InvalidInput,
        "host root is not a real directory",
    )
}

#[cfg(unix)]
fn native_root_path_identity(path: &std::path::Path) -> std::io::Result<NativeRootIdentity> {
    use std::os::unix::fs::MetadataExt;
    let metadata = std::fs::symlink_metadata(path)?;
    if !metadata.is_dir() {
        return Err(not_a_real_directory());
    }
    Ok(NativeRootIdentity {
        device: metadata.dev(),
        object: metadata.ino(),
    })
}

#[cfg(windows)]
fn native_root_path_identity(path: &std::path::Path) -> std::io::Result<NativeRootIdentity> {
    use cap_std::fs::MetadataExt as _;
    use std::os::windows::fs::OpenOptionsExt;
    use windows::Win32::Storage::FileSystem::{
        FILE_ATTRIBUTE_REPARSE_POINT, FILE_FLAG_BACKUP_SEMANTICS, FILE_FLAG_OPEN_REPARSE_POINT,
        FILE_READ_ATTRIBUTES, FILE_SHARE_DELETE, FILE_SHARE_READ, FILE_SHARE_WRITE,
    };
    let file = std::fs::OpenOptions::new()
        .access_mode(FILE_READ_ATTRIBUTES.0)
        .share_mode(FILE_SHARE_READ.0 | FILE_SHARE_WRITE.0 | FILE_SHARE_DELETE.0)
        .custom_flags(FILE_FLAG_BACKUP_SEMANTICS.0 | FILE_FLAG_OPEN_REPARSE_POINT.0)
        .open(path)?;
    let metadata = cap_std::fs::Metadata::from_file(&file)?;
    if !metadata.is_dir() || metadata.file_attributes() & FILE_ATTRIBUTE_REPARSE_POINT.0 != 0 {
        return Err(not_a_real_directory());
    }
    native_metadata_identity(&metadata)
}

#[cfg(not(any(unix, windows)))]
fn native_root_path_identity(_: &std::path::Path) -> std::io::Result<NativeRootIdentity> {
    Err(std::io::Error::new(
        std::io::ErrorKind::Unsupported,
        "stable root identity is unavailable on this platform",
    ))
}

#[cfg(not(any(unix, windows)))]
fn native_root_identity(_: &std::fs::File) -> std::io::Result<NativeRootIdentity> {
    Err(std::io::Error::new(
        std::io::ErrorKind::Unsupported,
        "stable root identity is unavailable on this platform",
    ))
}
