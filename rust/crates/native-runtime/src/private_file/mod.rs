//! Private file custody shared by every native host. Permissions are established
//! at creation; existing files are opened without following aliases and checked
//! through their held descriptor before reads, writes, or truncation.
use std::{
    fs::{self, File},
    io,
    path::Path,
};

#[cfg(windows)]
#[allow(
    unsafe_code,
    reason = "Windows private-file security uses held handles and protected current-user DACLs"
)]
mod windows;
#[cfg(windows)]
pub(crate) use windows::user_data_directory;

/// Create a new owner-only regular file, without replacing an existing entry.
pub fn create(path: &Path) -> io::Result<File> {
    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt as _;
        fs::OpenOptions::new()
            .read(true)
            .write(true)
            .create_new(true)
            .mode(0o600)
            .custom_flags(libc::O_NOFOLLOW | libc::O_NONBLOCK)
            .open(path)
    }
    #[cfg(windows)]
    {
        windows::create(path).map_err(windows::io_error)
    }
}

/// Verify owner permissions on the held file, including alias restrictions.
pub fn is_private(file: &File) -> io::Result<bool> {
    let metadata = file.metadata()?;
    if !metadata.is_file() {
        return Ok(false);
    }
    #[cfg(unix)]
    {
        use std::os::unix::fs::MetadataExt as _;
        Ok(metadata.mode() & 0o7777 == 0o600
            && metadata.nlink() == 1
            && metadata.uid() == effective_user())
    }
    #[cfg(windows)]
    {
        windows::is_private(file).map_err(windows::io_error)
    }
}

#[cfg(unix)]
#[allow(
    unsafe_code,
    reason = "geteuid reads the current process identity and has no pointer preconditions"
)]
pub(crate) fn effective_user() -> u32 {
    // SAFETY: geteuid takes no arguments and returns the process effective uid.
    unsafe { libc::geteuid() }
}

/// Open an existing private file; reject links, nonregular files, foreign owners,
/// and public permissions before the caller can read or mutate its contents.
pub fn open(path: &Path, writable: bool) -> io::Result<File> {
    let before = fs::symlink_metadata(path)?;
    if !before.is_file() || before.file_type().is_symlink() {
        return Err(io::ErrorKind::PermissionDenied.into());
    }
    let mut options = fs::OpenOptions::new();
    options.read(true).write(writable);
    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt as _;
        options.custom_flags(libc::O_NOFOLLOW | libc::O_NONBLOCK);
    }
    #[cfg(windows)]
    {
        use std::os::windows::fs::OpenOptionsExt as _;
        options.custom_flags(windows_sys::Win32::Storage::FileSystem::FILE_FLAG_OPEN_REPARSE_POINT);
    }
    let file = options.open(path)?;
    if !is_private(&file)? {
        return Err(io::ErrorKind::PermissionDenied.into());
    }
    #[cfg(unix)]
    {
        use std::os::unix::fs::MetadataExt as _;
        let held = file.metadata()?;
        if (before.dev(), before.ino()) != (held.dev(), held.ino()) {
            return Err(io::ErrorKind::PermissionDenied.into());
        }
    }
    Ok(file)
}

/// Open the existing private inode or atomically create it. Never truncate an
/// existing entry before validating its held descriptor.
pub fn open_or_create(path: &Path) -> io::Result<File> {
    match create(path) {
        Ok(file) => Ok(file),
        Err(error) if error.kind() == io::ErrorKind::AlreadyExists => open(path, true),
        Err(error) => Err(error),
    }
}

/// Reject an existing nonprivate entry while permitting a missing destination.
pub fn validate_destination(path: &Path) -> io::Result<()> {
    match open(path, false) {
        Ok(_) => Ok(()),
        Err(error) if error.kind() == io::ErrorKind::NotFound => Ok(()),
        Err(error) => Err(error),
    }
}

/// Retire a private unpublished sidecar after its publisher namespace has been
/// exclusively acquired by the caller (for example, the credential store lock).
/// A public, aliased, or nonregular entry is never removed as recovery debris.
pub fn discard_unpublished(path: &Path) -> io::Result<()> {
    let file = match open(path, false) {
        Ok(file) => file,
        Err(error) if error.kind() == io::ErrorKind::NotFound => return Ok(()),
        Err(error) => return Err(error),
    };
    drop(file);
    fs::remove_file(path)?;
    crate::sync_parent(
        path.parent().ok_or(io::ErrorKind::InvalidInput)?,
        crate::Durability::Full,
    )
}

/// Durable stages observable by callers that retain publication evidence.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum PublicationStage {
    /// Exclusive staging ownership exists before payload writes.
    Created,
    /// The complete payload has reached durable file storage.
    Synced,
    /// The atomic rename installed the complete record. A subsequent directory
    /// sync or acknowledgement can still fail; published credentials must survive.
    Installed,
    /// The complete private destination has been durably published.
    Published,
}

/// Publish one private file. Staging creation is exclusive, so failed attempts
/// cannot truncate or remove another publisher's temporary file. Every success
/// includes file durability and the native runtime's durable namespace rename.
pub fn publish(
    path: &Path,
    temporary: &Path,
    mode: crate::RenameMode,
    write: impl FnOnce(&mut File) -> io::Result<()>,
    mut observe: impl FnMut(PublicationStage) -> io::Result<()>,
) -> io::Result<()> {
    let mut staged = StagedFile::create_at(temporary.to_path_buf())?;
    observe(PublicationStage::Created)?;
    write(staged.as_file_mut())?;
    staged.publish_observed(path, mode, &mut observe).map(drop)
}

/// Publish an immutable original credential or report the existing winner.
/// A competing complete record is never overwritten or regenerated.
pub fn publish_new(
    path: &Path,
    temporary: &Path,
    write: impl FnOnce(&mut File) -> io::Result<()>,
    observe: impl FnMut(PublicationStage) -> io::Result<()>,
) -> io::Result<bool> {
    match publish(
        path,
        temporary,
        crate::RenameMode::NoReplace,
        write,
        observe,
    ) {
        Ok(()) => Ok(true),
        Err(error) if error.kind() == io::ErrorKind::AlreadyExists => {
            // A staging-name collision is not evidence of a published winner.
            // Authenticate the original destination before reporting one.
            open(path, false).map(|_| false)
        }
        Err(error) => Err(error),
    }
}

/// A staging pathname and its exclusive descriptor have one cleanup owner.
/// Only this object can publish its held file. Cleanup ends when the rename
/// commits, so dropping the old owner can never remove a reused staging name.
pub struct StagedFile {
    file: File,
    path: std::path::PathBuf,
    cleanup: bool,
}

/// The original descriptor retained after successful durable publication.
///
/// This witnesses this publisher's completion, not current namespace identity
/// or immutable contents. Another writer may replace the name or modify the
/// inode. Source freshness still requires the original source observation.
pub struct PublishedFile(StagedFile);

impl PublishedFile {
    /// Borrow the same descriptor used for staging and durable publication.
    /// No reopen, descriptor duplication or payload read is performed.
    pub fn as_file(&self) -> &File {
        self.0.as_file()
    }
}

impl StagedFile {
    fn create_at(path: std::path::PathBuf) -> io::Result<Self> {
        Ok(Self {
            file: create(&path)?,
            path,
            cleanup: true,
        })
    }

    /// Create a new private staging file on the destination filesystem.
    pub fn create(parent: &Path, prefix: &str) -> io::Result<Self> {
        let mut random = [0; 16];
        crate::owner_record::fill_random(&mut random).map_err(io::Error::other)?;
        let suffix = u128::from_le_bytes(random);
        Self::create_at(parent.join(format!("{prefix}{suffix:032x}.next")))
    }

    /// Borrow the private descriptor without transferring cleanup ownership.
    pub fn as_file(&self) -> &File {
        &self.file
    }

    /// Mutably borrow the private descriptor for an admitted serializer.
    pub fn as_file_mut(&mut self) -> &mut File {
        &mut self.file
    }

    /// Durably publish the complete held file through the common implementation.
    pub fn publish(self, path: &Path, mode: crate::RenameMode) -> io::Result<()> {
        self.publish_retained(path, mode).map(drop)
    }

    /// Durably publish while retaining the same original descriptor. A failed
    /// durability barrier or acknowledgement never produces a witness, even
    /// when the rename installed the file. Such a failure does not prove that
    /// publication had no effect.
    pub fn publish_retained(
        self,
        path: &Path,
        mode: crate::RenameMode,
    ) -> io::Result<PublishedFile> {
        self.publish_observed(path, mode, &mut |_| Ok(()))
    }

    fn publish_observed(
        mut self,
        path: &Path,
        mode: crate::RenameMode,
        observe: &mut impl FnMut(PublicationStage) -> io::Result<()>,
    ) -> io::Result<PublishedFile> {
        if path == self.path {
            return Err(io::ErrorKind::InvalidInput.into());
        }
        crate::flush_file_all(&self.file)?;
        observe(PublicationStage::Synced)?;
        let mut installed_error = None;
        crate::namespace::durable_rename_observed(&self.path, path, mode, None, || {
            self.cleanup = false;
            installed_error = observe(PublicationStage::Installed).err();
        })?;
        if let Some(error) = installed_error {
            return Err(error);
        }
        observe(PublicationStage::Published)?;
        Ok(PublishedFile(self))
    }
}

impl Drop for StagedFile {
    fn drop(&mut self) {
        if self.cleanup {
            let _ = fs::remove_file(&self.path);
        }
    }
}

/// Create one private directory without adopting an existing entry.
pub fn create_directory(path: &Path) -> io::Result<()> {
    #[cfg(unix)]
    {
        use std::os::unix::fs::DirBuilderExt as _;
        fs::DirBuilder::new().mode(0o700).create(path)
    }
    #[cfg(windows)]
    {
        windows::create_directory(path).map_err(windows::io_error)
    }
}

/// Create missing directory components privately. Existing parents are host
/// configuration and are never chmod'ed or given ownership by this operation.
pub fn create_directory_all(path: &Path) -> io::Result<()> {
    match create_directory(path) {
        Ok(()) => Ok(()),
        Err(error) if error.kind() == io::ErrorKind::AlreadyExists && path.is_dir() => Ok(()),
        Err(error) if error.kind() == io::ErrorKind::NotFound => {
            let parent = path
                .parent()
                .filter(|parent| !parent.as_os_str().is_empty())
                .ok_or_else(|| io::Error::from(io::ErrorKind::InvalidInput))?;
            create_directory_all(parent)?;
            match create_directory(path) {
                Ok(()) => Ok(()),
                Err(error) if error.kind() == io::ErrorKind::AlreadyExists && path.is_dir() => {
                    Ok(())
                }
                Err(error) => Err(error),
            }
        }
        Err(error) => Err(error),
    }
}

/// Admit an existing directory only when its identity and permissions are private.
pub fn validate_directory(path: &Path) -> io::Result<()> {
    let metadata = fs::symlink_metadata(path)?;
    if !metadata.is_dir() || metadata.file_type().is_symlink() {
        return Err(io::ErrorKind::PermissionDenied.into());
    }
    #[cfg(unix)]
    {
        use std::os::unix::fs::MetadataExt as _;
        if metadata.uid() != effective_user() || metadata.mode() & 0o777 != 0o700 {
            return Err(io::ErrorKind::PermissionDenied.into());
        }
    }
    #[cfg(windows)]
    {
        use std::os::windows::fs::OpenOptionsExt as _;
        let file = fs::OpenOptions::new()
            .read(true)
            .custom_flags(
                windows_sys::Win32::Storage::FileSystem::FILE_FLAG_BACKUP_SEMANTICS
                    | windows_sys::Win32::Storage::FileSystem::FILE_FLAG_OPEN_REPARSE_POINT,
            )
            .open(path)?;
        if !windows::is_private(&file).map_err(windows::io_error)? {
            return Err(io::ErrorKind::PermissionDenied.into());
        }
    }
    Ok(())
}

/// Read a complete private record under an optional caller byte policy. Metered hosts use
/// `read_with` to admit the allocation to their own ledger before allocating it.
pub fn read(
    path: &Path,
    maximum: impl Into<Option<u64>>,
) -> io::Result<crate::owner_record::SecretBytes> {
    use std::io::Read as _;
    read_with(path, maximum, |file, length| {
        let mut bytes = crate::owner_record::SecretBytes(Vec::new());
        bytes
            .0
            .try_reserve_exact(usize::try_from(length + 1).map_err(io::Error::other)?)
            .map_err(io::Error::other)?;
        file.read_to_end(&mut bytes.0)?;
        Ok(bytes)
    })
}

/// Private-record read under an optional caller byte policy. The callback admits storage before
/// allocation, allowing native hosts to retain their original invocation ledger.
pub fn read_with<T, E: From<io::Error>>(
    path: &Path,
    maximum: impl Into<Option<u64>>,
    read: impl FnOnce(&mut std::io::Take<File>, u64) -> Result<T, E>,
) -> Result<T, E> {
    let file = open(path, false)?;
    read_held_with(file, maximum.into(), read)
}

fn read_held_with<T, E: From<io::Error>>(
    file: File,
    maximum: Option<u64>,
    read: impl FnOnce(&mut std::io::Take<File>, u64) -> Result<T, E>,
) -> Result<T, E> {
    use std::io::Read as _;
    let length = file.metadata()?.len();
    if maximum.is_some_and(|maximum| length > maximum) {
        return Err(io::Error::from(io::ErrorKind::InvalidData).into());
    }
    let mut bounded = file.take(
        length
            .checked_add(1)
            .ok_or_else(|| io::Error::from(io::ErrorKind::InvalidData))?,
    );
    let value = read(&mut bounded, length)?;
    if bounded.limit() != 1 || bounded.get_ref().metadata()?.len() != length {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "private record changed during read",
        )
        .into());
    }
    Ok(value)
}

#[cfg(test)]
mod tests;

/// Return the process owner's Windows security identifier through the shared
/// held-token implementation. Invalid process handles return an OS error.
#[cfg(windows)]
pub fn process_user_id(process: *mut std::ffi::c_void) -> io::Result<Vec<u8>> {
    windows::process_user_id(process).map_err(windows::io_error)
}

/// Keeps the same protected owner-only descriptor live for a native object creation.
#[cfg(windows)]
pub(crate) fn with_private_security<T>(
    action: impl FnOnce(*mut std::ffi::c_void) -> io::Result<T>,
) -> io::Result<T> {
    windows::with_security(|attributes| action(std::ptr::from_ref(attributes).cast_mut().cast()))
        .map_err(windows::io_error)
}

/// Check the held file or pipe object's owner-only security descriptor.
#[cfg(windows)]
pub(crate) fn is_private_security(handle: *mut std::ffi::c_void) -> io::Result<bool> {
    windows::is_private_security(handle).map_err(windows::io_error)
}
