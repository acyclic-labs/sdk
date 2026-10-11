//! Held-parent entry operations for the canonical native materializer.
#![allow(unsafe_code, reason = "isolates descriptor-relative native rename and metadata calls")]

use crate::native_host::HostRoot;
#[cfg(windows)]
use cap_fs_ext::OpenOptionsFollowExt as _;
use cap_std::fs::{Dir, Metadata, OpenOptions};
use std::ffi::OsStr;
use std::io;
use std::path::Path;

/// Both the parent and its final name stay bound across observation and mutation.
pub(super) struct Entry<'a> {
    pub(super) parent: Dir,
    pub(super) name: &'a OsStr,
    pinned: Option<std::fs::File>,
}

impl<'a> Entry<'a> {
    pub(super) fn open(root: &HostRoot, path: &'a Path) -> io::Result<Self> {
        let name = path.file_name().ok_or_else(|| io::Error::new(io::ErrorKind::InvalidInput, "entry has no leaf"))?;
        let parent = root.open_dir_held(path.parent().unwrap_or_else(|| Path::new("")))?;
        let pinned = pin_entry(&parent, name)?;
        Ok(Self { parent, name, pinned })
    }

    pub(super) fn metadata(&self) -> io::Result<Option<Metadata>> {
        match self.parent.symlink_metadata(self.name) {
            Ok(metadata) => {
                if let Some(pinned) = &self.pinned
                    && crate::NativeRootIdentity::from_file(pinned)? != crate::NativeRootIdentity::from_metadata(&metadata)?
                {
                    return Err(io::Error::new(io::ErrorKind::InvalidData, "entry binding changed"));
                }
                Ok(Some(metadata))
            }
            Err(error) if error.kind() == io::ErrorKind::NotFound => Ok(None),
            Err(error) => Err(error),
        }
    }

    pub(super) fn is_present(&self) -> io::Result<bool> {
        match self.parent.symlink_metadata(self.name) {
            Ok(_) => Ok(true),
            Err(error) if error.kind() == io::ErrorKind::NotFound => Ok(false),
            Err(error) => Err(error),
        }
    }

    pub(super) fn repin(&self) -> io::Result<Self> {
        let parent = self.parent.try_clone()?;
        let pinned = pin_entry(&parent, self.name)?;
        Ok(Self { parent, name: self.name, pinned })
    }

    pub(super) fn rename_to(&self, destination: &Self) -> io::Result<()> {
        rename_no_replace(self, destination)?;
        sync_directory(&destination.parent)?;
        sync_directory(&self.parent)
    }

    pub(super) fn set_metadata(&self, readonly: bool, posix_mode: Option<u32>, windows_attributes: Option<u32>) -> io::Result<()> {
        #[cfg(unix)]
        {
            use cap_fs_ext::OpenOptionsFollowExt as _;
            use cap_std::fs::OpenOptionsExt as _;
            use std::os::unix::fs::PermissionsExt as _;
            let mut options = OpenOptions::new();
            options.read(true).follow(cap_primitives::fs::FollowSymlinks::No).custom_flags(libc::O_NONBLOCK);
            let file = self.parent.open_with(self.name, &options)?.into_std();
            self.verify_file(&file)?;
            let mut permissions = file.metadata()?.permissions();
            if let Some(mode) = posix_mode { permissions.set_mode(mode); } else { permissions.set_readonly(readonly); }
            file.set_permissions(permissions)?;
            acyclic_native_runtime::sync_file(&file, acyclic_native_runtime::Durability::Full)?;
            let _ = windows_attributes;
        }
        #[cfg(windows)]
        {
            use cap_std::fs::OpenOptionsExt as _;
            use std::mem::size_of;
            use std::os::windows::io::{AsHandle as _, AsRawHandle as _};
            use windows::Wdk::Storage::FileSystem::{FILE_BASIC_INFORMATION, FileBasicInformation, NtSetInformationFile};
            use windows::Win32::Foundation::HANDLE;
            use windows::Win32::Storage::FileSystem::{FILE_FLAG_BACKUP_SEMANTICS, FILE_FLAG_OPEN_REPARSE_POINT, FILE_READ_ATTRIBUTES, FILE_WRITE_ATTRIBUTES, SYNCHRONIZE};
            use windows::Win32::System::IO::IO_STATUS_BLOCK;
            let mut options = OpenOptions::new();
            options.access_mode((FILE_READ_ATTRIBUTES | FILE_WRITE_ATTRIBUTES | SYNCHRONIZE).0)
                .custom_flags(FILE_FLAG_BACKUP_SEMANTICS.0 | FILE_FLAG_OPEN_REPARSE_POINT.0);
            let file = self.parent.open_with(self.name, &options)?.into_std();
            self.verify_file(&file)?;
            use std::os::windows::fs::MetadataExt as _;
            let current = file.metadata()?.file_attributes();
            let attributes = windows_attributes.unwrap_or(if readonly { current | 1 } else { current & !1 });
            let information = FILE_BASIC_INFORMATION { FileAttributes: if attributes == 0 { 0x80 } else { attributes }, ..Default::default() };
            let mut status_block = IO_STATUS_BLOCK::default();
            // SAFETY: the held handle and complete information/status structures outlive the call.
            let status = unsafe { NtSetInformationFile(HANDLE(file.as_handle().as_raw_handle()), &raw mut status_block, (&raw const information).cast(), u32::try_from(size_of::<FILE_BASIC_INFORMATION>()).map_err(|_| io::Error::other("metadata information overflow"))?, FileBasicInformation) };
            if !status.is_ok() { return Err(crate::native_host::status_error(status)); }
            sync_directory(&self.parent)?;
            let _ = posix_mode;
        }
        #[cfg(not(any(unix, windows)))]
        {
            let mut options = OpenOptions::new();
            use cap_fs_ext::OpenOptionsFollowExt as _;
            options.read(true).follow(cap_primitives::fs::FollowSymlinks::No);
            let file = self.parent.open_with(self.name, &options)?.into_std();
            self.verify_file(&file)?;
            let mut permissions = file.metadata()?.permissions();
            permissions.set_readonly(readonly);
            file.set_permissions(permissions)?;
            acyclic_native_runtime::sync_file(&file, acyclic_native_runtime::Durability::Full)?;
            let _ = (posix_mode, windows_attributes);
        }
        Ok(())
    }

    fn verify_file(&self, file: &std::fs::File) -> io::Result<()> {
        let pinned = self.pinned.as_ref().ok_or_else(|| io::Error::new(io::ErrorKind::NotFound, "entry binding is absent"))?;
        if crate::NativeRootIdentity::from_file(pinned)? != crate::NativeRootIdentity::from_file(file)? {
            return Err(io::Error::new(io::ErrorKind::InvalidData, "entry binding changed"));
        }
        Ok(())
    }
}

pub(super) fn sync_directory(directory: &Dir) -> io::Result<()> {
    // Open relative to the held directory, never by its formerly ambient path.
    // Windows requires write access for a directory flush; Unix read access is enough.
    let mut options = OpenOptions::new();
    options.read(true);
    #[cfg(windows)]
    {
        use cap_std::fs::OpenOptionsExt as _;
        use windows::Win32::Storage::FileSystem::FILE_FLAG_BACKUP_SEMANTICS;
        options.write(true).custom_flags(FILE_FLAG_BACKUP_SEMANTICS.0);
    }
    let directory = directory.open_with(Path::new("."), &options)?.into_std();
    acyclic_native_runtime::sync_file(&directory, acyclic_native_runtime::Durability::Full)
}

#[cfg(target_os = "linux")]
fn rename_no_replace(source: &Entry, destination: &Entry) -> io::Result<()> {
    rustix::fs::renameat_with(
        &source.parent,
        source.name,
        &destination.parent,
        destination.name,
        rustix::fs::RenameFlags::NOREPLACE,
    ).map_err(Into::into)
}

#[cfg(target_os = "macos")]
fn rename_no_replace(source: &Entry, destination: &Entry) -> io::Result<()> {
    use std::os::fd::AsRawFd as _;
    use std::os::unix::ffi::OsStrExt as _;
    let source_name = std::ffi::CString::new(source.name.as_bytes()).map_err(|_| io::Error::new(io::ErrorKind::InvalidInput, "source name contains NUL"))?;
    let destination_name = std::ffi::CString::new(destination.name.as_bytes()).map_err(|_| io::Error::new(io::ErrorKind::InvalidInput, "destination name contains NUL"))?;
    // SAFETY: both directory descriptors and NUL-terminated leaf names remain live.
    let status = unsafe { libc::renameatx_np(source.parent.as_raw_fd(), source_name.as_ptr(), destination.parent.as_raw_fd(), destination_name.as_ptr(), libc::RENAME_EXCL) };
    if status == 0 { Ok(()) } else { Err(io::Error::last_os_error()) }
}

#[cfg(windows)]
fn rename_no_replace(source: &Entry, destination: &Entry) -> io::Result<()> {
    use cap_std::fs::OpenOptionsExt as _;
    use windows::Win32::Storage::FileSystem::{DELETE, FILE_FLAG_BACKUP_SEMANTICS, FILE_FLAG_OPEN_REPARSE_POINT, FILE_READ_ATTRIBUTES, FILE_SHARE_DELETE, FILE_SHARE_READ, FILE_SHARE_WRITE, SYNCHRONIZE};

    let mut options = OpenOptions::new();
    options.access_mode((DELETE | FILE_READ_ATTRIBUTES | SYNCHRONIZE).0)
        .share_mode((FILE_SHARE_READ | FILE_SHARE_WRITE | FILE_SHARE_DELETE).0)
        .custom_flags(FILE_FLAG_BACKUP_SEMANTICS.0 | FILE_FLAG_OPEN_REPARSE_POINT.0)
        .follow(cap_primitives::fs::FollowSymlinks::No);
    let source_file = source.parent.open_with(source.name, &options)?.into_std();
    if let Some(pinned) = &source.pinned
        && crate::NativeRootIdentity::from_file(pinned)? != crate::NativeRootIdentity::from_file(&source_file)?
    {
        return Err(io::Error::new(io::ErrorKind::InvalidData, "rename source binding changed"));
    }
    crate::native_host::rename_windows_entry(&source_file, &destination.parent, destination.name)
}


#[cfg(not(any(target_os = "linux", target_os = "macos", windows)))]
fn rename_no_replace(_source: &Entry, _destination: &Entry) -> io::Result<()> {
    Err(io::Error::new(io::ErrorKind::Unsupported, "this target has no native held-parent no-replace rename"))
}

fn pin_entry(parent: &Dir, name: &OsStr) -> io::Result<Option<std::fs::File>> {
    match pin_entry_present(parent, name) {
        Ok(file) => Ok(Some(file)),
        Err(error) if error.kind() == io::ErrorKind::NotFound => Ok(None),
        Err(error) => Err(error),
    }
}

#[cfg(target_os = "linux")]
fn pin_entry_present(parent: &Dir, name: &OsStr) -> io::Result<std::fs::File> {
    rustix::fs::openat(parent, name, rustix::fs::OFlags::PATH | rustix::fs::OFlags::NOFOLLOW | rustix::fs::OFlags::CLOEXEC, rustix::fs::Mode::empty())
        .map(Into::into).map_err(Into::into)
}

#[cfg(target_os = "macos")]
fn pin_entry_present(parent: &Dir, name: &OsStr) -> io::Result<std::fs::File> {
    use cap_std::fs::OpenOptionsExt as _;
    let mut options = OpenOptions::new();
    let metadata = parent.symlink_metadata(name)?;
    options.read(true).custom_flags(if metadata.is_symlink() { libc::O_SYMLINK } else { libc::O_NOFOLLOW } | libc::O_NONBLOCK);
    parent.open_with(name, &options).map(cap_std::fs::File::into_std)
}

#[cfg(windows)]
fn pin_entry_present(parent: &Dir, name: &OsStr) -> io::Result<std::fs::File> {
    use cap_std::fs::OpenOptionsExt as _;
    use windows::Win32::Storage::FileSystem::{FILE_FLAG_BACKUP_SEMANTICS, FILE_FLAG_OPEN_REPARSE_POINT, FILE_READ_ATTRIBUTES, FILE_SHARE_DELETE, FILE_SHARE_READ, FILE_SHARE_WRITE, SYNCHRONIZE};
    let mut options = OpenOptions::new();
    options.access_mode((FILE_READ_ATTRIBUTES | SYNCHRONIZE).0)
        .share_mode((FILE_SHARE_READ | FILE_SHARE_WRITE | FILE_SHARE_DELETE).0)
        .custom_flags(FILE_FLAG_BACKUP_SEMANTICS.0 | FILE_FLAG_OPEN_REPARSE_POINT.0)
        .follow(cap_primitives::fs::FollowSymlinks::No);
    parent.open_with(name, &options).map(cap_std::fs::File::into_std)
}

#[cfg(not(any(target_os = "linux", target_os = "macos", windows)))]
fn pin_entry_present(parent: &Dir, name: &OsStr) -> io::Result<std::fs::File> {
    parent.open(name).map(cap_std::fs::File::into_std)
}
