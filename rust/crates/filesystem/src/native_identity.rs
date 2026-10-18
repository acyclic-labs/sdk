//! Stable identities for already-open native filesystem roots.

/// Stable native identity of one held filesystem root.
#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub struct NativeRootIdentity {
    pub(crate) device: u64,
    pub(crate) object: u64,
}

/// How stably a held root's object id names it. None of these tells one
/// incarnation of a root from a later directory at a recycled id; callers
/// that persist state per incarnation pair it with a token published in the
/// root (`HeldRoot::read_or_publish`).
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum NativeRootGeneration {
    /// A digest of the root's kernel file handle, which carries the inode
    /// generation and survives remounts (Linux).
    FileHandle([u8; 32]),
    /// The object id is stable for the root's lifetime and across remounts
    /// (an NTFS file index, an APFS or HFS+ catalog id), though the
    /// filesystem may reissue it after the root is deleted.
    StableObjectId,
    /// The filesystem may renumber the root's object id across remounts
    /// (no exportable file handle, or no stable file index).
    Unstable,
}

/// A held root's identity and generation, from stable generation data only:
/// never a mutable timestamp, and never a query error folded into a value.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct NativeRootIncarnation {
    /// The root's device and object id.
    pub identity: NativeRootIdentity,
    /// What makes that object id one incarnation.
    pub generation: NativeRootGeneration,
}

impl NativeRootIdentity {
    /// Derives identity from an already-open root handle.
    pub fn from_file(file: &std::fs::File) -> std::io::Result<Self> {
        native_root_identity(file)
    }

    pub(crate) fn from_metadata(metadata: &cap_std::fs::Metadata) -> std::io::Result<Self> {
        native_metadata_identity(metadata)
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

#[cfg(not(any(unix, windows)))]
fn native_root_identity(_: &std::fs::File) -> std::io::Result<NativeRootIdentity> {
    Err(std::io::Error::new(
        std::io::ErrorKind::Unsupported,
        "stable root identity is unavailable on this platform",
    ))
}
