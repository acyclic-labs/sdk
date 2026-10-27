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
fn effective_user() -> u32 {
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

/// One stable lock inode in the selected custody store's own namespace.
/// Production uses OS account data; fixture children borrow their parent's
/// isolated store directory. Never unlink a live store's lock inodes.
pub(crate) fn credential_lock(identity: &str) -> io::Result<File> {
    #[cfg(any(test, feature = "test-support"))]
    let shared = crate::signing_key::fixture::lock_directory()?;
    #[cfg(any(test, feature = "test-support"))]
    let root = shared.clone().unwrap_or_else(|| {
        static ROOT: std::sync::OnceLock<std::path::PathBuf> = std::sync::OnceLock::new();
        ROOT.get_or_init(std::env::temp_dir).clone()
    });
    #[cfg(all(unix, not(any(test, feature = "test-support"))))]
    let root = unix_user_data_directory().map_err(|error| io::Error::new(error.kind(),
        format!("credential lock namespace lookup failed: {error}; recovery: configure a persistent absolute home outside /tmp and /var/tmp for the effective OS account")))?;
    #[cfg(unix)]
    let user = effective_user().to_string();
    #[cfg(all(windows, not(any(test, feature = "test-support"))))]
    let root = windows::user_data_directory().map_err(windows::io_error)?;
    #[cfg(windows)]
    let user = "user";
    #[cfg(not(any(test, feature = "test-support")))]
    let root = credential_directory(&root)?;
    // A fixture credential exists in this process only. Its lock namespace
    // follows that same custody boundary, without changing the locking API.
    #[cfg(any(test, feature = "test-support"))]
    let identity = if shared.is_some() {
        identity.to_owned()
    } else {
        format!("fixture-{}-{identity}", std::process::id())
    };
    open_or_create(&root.join(format!("acyclic-custody-{user}-{identity}.lock")))
}

// Resolve the account's home through the OS account database, never HOME,
// TMPDIR or XDG overrides. PrivateTmp services and interactive processes must
// reach the same persistent inode when they share the user's credential store.
#[cfg(all(unix, any(test, not(feature = "test-support"))))]
#[allow(
    unsafe_code,
    reason = "getpwuid_r resolves the effective account using caller-owned output buffers"
)]
fn unix_user_data_directory() -> io::Result<std::path::PathBuf> {
    use std::{ffi::CStr, os::unix::ffi::OsStrExt as _};
    let uid = effective_user();
    let mut buffer = vec![0; 1024];
    loop {
        let mut account = std::mem::MaybeUninit::<libc::passwd>::uninit();
        let mut found = std::ptr::null_mut();
        // SAFETY: account and found are writable, buffer spans its supplied
        // length, and all returned pointers are consumed before buffer changes.
        let status = unsafe {
            libc::getpwuid_r(
                uid,
                account.as_mut_ptr(),
                buffer.as_mut_ptr(),
                buffer.len(),
                &mut found,
            )
        };
        if status == libc::ERANGE {
            let length = buffer
                .len()
                .checked_mul(2)
                .ok_or(io::ErrorKind::OutOfMemory)?;
            buffer
                .try_reserve_exact(length - buffer.len())
                .map_err(io::Error::other)?;
            buffer.resize(length, 0);
            continue;
        }
        if status != 0 {
            return Err(io::Error::from_raw_os_error(status));
        }
        if found.is_null() {
            return Err(io::Error::new(
                io::ErrorKind::NotFound,
                "credential lock account lookup failed; recovery: configure a persistent home for the effective OS account",
            ));
        }
        // SAFETY: successful getpwuid_r with a nonnull result initialized
        // account; pw_dir is its NUL-terminated string in the live buffer.
        let account = unsafe { account.assume_init() };
        if account.pw_dir.is_null() {
            return Err(io::ErrorKind::InvalidData.into());
        }
        let home = unsafe { CStr::from_ptr(account.pw_dir) };
        let directory = fs::canonicalize(Path::new(std::ffi::OsStr::from_bytes(home.to_bytes())))?;
        let home = persistent_user_directory(&directory)?;
        #[cfg(target_vendor = "apple")]
        return Ok(home.join("Library/Application Support"));
        #[cfg(not(target_vendor = "apple"))]
        return Ok(home.join(".local/share"));
    }
}

// Admission must follow every appended component and alias, including the
// service directory. A persistent account home alone does not establish that
// its data directory is outside a service-private temporary mount.
#[cfg(any(test, not(feature = "test-support")))]
fn credential_directory(data: &Path) -> io::Result<std::path::PathBuf> {
    let root = data.join("acyclic.authority.signing-seed.v1");
    let mut create = fs::DirBuilder::new();
    create.recursive(true);
    #[cfg(unix)]
    {
        use std::os::unix::fs::DirBuilderExt as _;
        create.mode(0o700);
    }
    create.create(&root)?;
    let resolved = fs::canonicalize(&root)?;
    persistent_user_directory(&resolved).map_err(|error| io::Error::new(error.kind(),
        format!("credential lock final namespace admission failed for {}: {error}; recovery: configure the OS account data directory on persistent storage outside temporary namespaces", resolved.display())))?;
    Ok(resolved)
}

#[cfg(any(test, not(feature = "test-support")))]
fn persistent_user_directory(path: &Path) -> io::Result<&Path> {
    #[cfg(unix)]
    let temporary = ["/tmp", "/var/tmp", "/private/tmp", "/private/var/tmp"]
        .iter()
        .any(|temporary| path.starts_with(temporary));
    // Windows temporary folders conventionally occur at either the volume,
    // Windows or account-data level. Compare complete components, including
    // canonicalized reparse-point targets, without consulting TEMP/TMP.
    #[cfg(windows)]
    let temporary = path.components().any(|part| {
        let component = part.as_os_str().as_encoded_bytes();
        component.eq_ignore_ascii_case(b"Temp") || component.eq_ignore_ascii_case(b"Tmp")
    });
    if !path.is_absolute() || temporary {
        return Err(io::ErrorKind::InvalidInput.into());
    }
    Ok(path)
}

/// Reject an existing nonprivate entry while permitting a missing destination.
pub fn validate_destination(path: &Path) -> io::Result<()> {
    validate_destination_with(path, |path| open(path, false))
}

/// Admit a missing destination or an existing trusted legacy owner record.
/// Windows tightens its descriptor under exclusive ownership before returning;
/// shared ACLs, aliases and live writers remain refused. Other OSes stay strict.
/// No payload is read or replaced, so original credentials and lock identities
/// survive migration through the same guard as [`read_for_upgrade_with`].
pub fn validate_destination_for_upgrade(path: &Path) -> io::Result<()> {
    validate_destination_with(path, open_for_upgrade)
}

fn validate_destination_with(
    path: &Path,
    open: impl FnOnce(&Path) -> io::Result<File>,
) -> io::Result<()> {
    match open(path) {
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
    let mut staged = StagedFile::create_at(temporary)?;
    observe(PublicationStage::Created)?;
    write(staged.as_file_mut())?;
    staged.publish_observed(path, mode, &mut observe)
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

impl StagedFile {
    fn create_at(path: &Path) -> io::Result<Self> {
        Ok(Self {
            file: create(path)?,
            path: path.to_path_buf(),
            cleanup: true,
        })
    }

    /// Create a new private staging file on the destination filesystem.
    pub fn create(parent: &Path, prefix: &str) -> io::Result<Self> {
        let mut random = [0; 16];
        crate::owner_record::fill_random(&mut random).map_err(io::Error::other)?;
        let suffix = u128::from_le_bytes(random);
        Self::create_at(&parent.join(format!("{prefix}{suffix:032x}.next")))
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
        self.publish_observed(path, mode, &mut |_| Ok(()))
    }

    fn publish_observed(
        mut self,
        path: &Path,
        mode: crate::RenameMode,
        observe: &mut impl FnMut(PublicationStage) -> io::Result<()>,
    ) -> io::Result<()> {
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
        observe(PublicationStage::Published)
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

/// Read a bounded complete private record into secret storage. Metered hosts use
/// `read_with` to admit the allocation to their own ledger before allocating it.
pub fn read(path: &Path, maximum: u64) -> io::Result<crate::owner_record::SecretBytes> {
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

/// Bounded private-record read. The allocator callback admits storage before
/// allocation, allowing native hosts to retain their original invocation ledger.
pub fn read_with<T, E: From<io::Error>>(
    path: &Path,
    maximum: u64,
    read: impl FnOnce(&mut std::io::Take<File>, u64) -> Result<T, E>,
) -> Result<T, E> {
    let file = open(path, false)?;
    read_held_with(path, file, maximum, read)
}

/// Read an existing owner for a format upgrade. On Windows, a legacy owner
/// descriptor is tightened before any read. Protected records and other OSes
/// take the strict path. Tightening requires an exclusive, unaliased regular
/// file owned by the current user, whose nonempty ACL grants access only to that
/// user, SYSTEM or local administrators. Other ACLs and foreign ownership are
/// refused. This does not generate credentials.
pub fn read_for_upgrade_with<T, E: From<io::Error>>(
    path: &Path,
    maximum: u64,
    read: impl FnOnce(&mut std::io::Take<File>, u64) -> Result<T, E>,
) -> Result<T, E> {
    read_held_with(path, open_for_upgrade(path)?, maximum, read)
}

fn open_for_upgrade(path: &Path) -> io::Result<File> {
    #[cfg(windows)]
    let file = match open(path, false) {
        Ok(file) => file,
        Err(error) if error.kind() == io::ErrorKind::PermissionDenied => {
            windows::open_for_upgrade(path).map_err(windows::io_error)?
        }
        Err(error) => return Err(error),
    };
    #[cfg(not(windows))]
    let file = open(path, false)?;
    Ok(file)
}

fn read_held_with<T, E: From<io::Error>>(
    path: &Path,
    file: File,
    maximum: u64,
    read: impl FnOnce(&mut std::io::Take<File>, u64) -> Result<T, E>,
) -> Result<T, E> {
    use std::io::Read as _;
    let length = file.metadata()?.len();
    if length > maximum {
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
    crate::sync_parent(
        path.parent()
            .filter(|parent| !parent.as_os_str().is_empty())
            .unwrap_or_else(|| Path::new(".")),
        crate::Durability::Full,
    )?;
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
