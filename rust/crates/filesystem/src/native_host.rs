//! Capability-rooted native filesystem access shared by capture and materialization.
#![allow(missing_docs, unsafe_code)]

use cap_fs_ext::DirExt as _;
use cap_std::fs::{Dir, Metadata, OpenOptions, Permissions, ReadDir};
use std::ffi::{OsStr, OsString};
use std::fs::File;
use std::io;
use std::path::Path;
#[cfg(windows)]
#[cfg(any(feature = "native-mount", test))]
use windows::Win32::Storage::FileSystem::FILE_BASIC_INFO;

#[cfg(target_os = "linux")]
#[derive(Debug, thiserror::Error)]
#[cfg(any(feature = "native-mount", test))]
pub(crate) enum LinuxMetadataError {
    #[error("Linux native metadata cannot represent {0}")]
    Unsupported(&'static str),
    #[error(transparent)]
    Io(#[from] io::Error),
}

#[cfg(target_os = "linux")]
#[cfg(any(feature = "native-mount", test))]
pub(crate) struct LinuxMetadataTarget {
    inode: std::os::fd::OwnedFd,
}

#[cfg(windows)]
#[cfg(any(feature = "native-mount", test))]
#[derive(Debug, thiserror::Error)]
pub(crate) enum WindowsMetadataError {
    #[error("Windows native metadata cannot represent {0}")]
    Unsupported(&'static str),
    #[error(transparent)]
    Io(#[from] io::Error),
}

/// A no-follow leaf handle pinned before metadata work is deferred.
#[cfg(windows)]
#[cfg(any(feature = "native-mount", test))]
pub(crate) struct WindowsMetadataTarget {
    file: cap_std::fs::File,
}

#[cfg(target_os = "macos")]
#[derive(Debug, thiserror::Error)]
#[cfg(any(feature = "native-mount", test))]
pub(crate) enum MacMetadataError {
    #[error("macOS native metadata cannot represent {0}")]
    Unsupported(&'static str),
    #[error(transparent)]
    Io(#[from] io::Error),
}

/// An exact, no-follow target pinned before metadata work is deferred.
#[cfg(target_os = "macos")]
#[cfg(any(feature = "native-mount", test))]
pub(crate) enum MacMetadataTarget {
    Held(File),
    Noop {
        parent: Dir,
        leaf: std::path::PathBuf,
        device: u64,
        inode: u64,
    },
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct HostDataRange {
    pub offset: u64,
    pub length: u64,
}

/// One object's kind, size, times, and identity, read without following its
/// name's final link.
#[cfg(not(windows))]
pub type HostStat = Metadata;

/// One object's kind, size, times, attributes, and identity, read without
/// following its name's final link. Every plain object's facts come from one
/// `FileStatInformation` query of its file record, by name or by handle, so
/// a name and a handle on it always agree. An NTFS directory index is never
/// a source: it may describe an older state of a file written through
/// another link or through a handle still open.
#[cfg(windows)]
#[derive(Clone, Copy, Debug)]
pub struct HostStat {
    file_type: cap_std::fs::FileType,
    len: u64,
    attributes: u32,
    creation_time: u64,
    last_access_time: u64,
    last_write_time: u64,
    volume_serial_number: Option<u32>,
    file_index: Option<u64>,
}

#[cfg(windows)]
impl HostStat {
    pub fn from_metadata(metadata: &Metadata) -> Self {
        use cap_primitives::fs::_WindowsByHandle;
        use cap_std::fs::MetadataExt;
        Self {
            file_type: metadata.file_type(),
            len: metadata.len(),
            attributes: MetadataExt::file_attributes(metadata),
            creation_time: metadata.creation_time(),
            last_access_time: metadata.last_access_time(),
            last_write_time: metadata.last_write_time(),
            volume_serial_number: _WindowsByHandle::volume_serial_number(metadata),
            file_index: _WindowsByHandle::file_index(metadata),
        }
    }

    /// The facts one `FileStatInformation` query reports for an object
    /// with no reparse point, on the volume `volume_serial_number` names.
    fn from_stat_information(
        information: &windows::Wdk::Storage::FileSystem::FILE_STAT_INFORMATION,
        volume_serial_number: Option<u32>,
    ) -> io::Result<Self> {
        use windows::Win32::Storage::FileSystem::FILE_ATTRIBUTE_DIRECTORY;

        let unsigned =
            |value: i64| u64::try_from(value).map_err(|_| io::Error::other("negative stat field"));
        Ok(Self {
            file_type: if information.FileAttributes & FILE_ATTRIBUTE_DIRECTORY.0 != 0 {
                cap_std::fs::FileType::dir()
            } else {
                cap_std::fs::FileType::file()
            },
            len: unsigned(information.EndOfFile)?,
            attributes: information.FileAttributes,
            creation_time: unsigned(information.CreationTime)?,
            last_access_time: unsigned(information.LastAccessTime)?,
            last_write_time: unsigned(information.LastWriteTime)?,
            volume_serial_number,
            file_index: Some(unsigned(information.FileId)?),
        })
    }

    #[must_use]
    pub const fn file_type(&self) -> cap_std::fs::FileType {
        self.file_type
    }

    #[must_use]
    pub fn is_dir(&self) -> bool {
        self.file_type.is_dir()
    }

    #[must_use]
    #[allow(
        clippy::len_without_is_empty,
        reason = "mirrors Metadata::len: the byte length of the named object"
    )]
    pub const fn len(&self) -> u64 {
        self.len
    }

    #[must_use]
    pub const fn file_attributes(&self) -> u32 {
        self.attributes
    }

    #[must_use]
    pub const fn creation_time(&self) -> u64 {
        self.creation_time
    }

    #[must_use]
    pub const fn last_write_time(&self) -> u64 {
        self.last_write_time
    }

    #[must_use]
    pub const fn volume_serial_number(&self) -> Option<u32> {
        self.volume_serial_number
    }

    #[must_use]
    pub const fn file_index(&self) -> Option<u64> {
        self.file_index
    }

    pub fn created(&self) -> io::Result<cap_std::time::SystemTime> {
        Ok(windows_time(self.creation_time))
    }

    pub fn modified(&self) -> io::Result<cap_std::time::SystemTime> {
        Ok(windows_time(self.last_write_time))
    }

    pub fn accessed(&self) -> io::Result<cap_std::time::SystemTime> {
        Ok(windows_time(self.last_access_time))
    }
}

/// `path` as the UTF-16 name a kernel call resolves relative to a held
/// directory, with its byte length; `None` for the directory itself or any
/// component that is not a plain name.
#[cfg(windows)]
fn relative_kernel_name(path: &Path) -> Option<(Vec<u16>, u16)> {
    use std::os::windows::ffi::OsStrExt as _;

    let mut name = Vec::new();
    for component in path.components() {
        let std::path::Component::Normal(component) = component else {
            return None;
        };
        if !name.is_empty() {
            name.push(u16::from(b'\\'));
        }
        name.extend(component.encode_wide());
    }
    let length = u16::try_from(name.len() * 2)
        .ok()
        .filter(|length| *length > 0)?;
    Some((name, length))
}

/// The instant one `FILETIME` names, as the standard library reads it.
#[cfg(windows)]
fn windows_time(ticks: u64) -> cap_std::time::SystemTime {
    const UNIX_EPOCH_TICKS: u64 = 116_444_736_000_000_000;
    let since = |ticks: u64| std::time::Duration::from_nanos(ticks.saturating_mul(100));
    cap_std::time::SystemTime::from_std(if ticks >= UNIX_EPOCH_TICKS {
        std::time::UNIX_EPOCH + since(ticks - UNIX_EPOCH_TICKS)
    } else {
        std::time::UNIX_EPOCH - since(UNIX_EPOCH_TICKS - ticks)
    })
}

/// One enumerated name with its facts. `stat` is `None` when enumeration
/// cannot report the facts exactly as [`HostRoot::stat`] would, and the name
/// must be stat'ed on its own.
pub struct HostListedEntry {
    pub name: OsString,
    pub stat: Option<HostStat>,
}

/// The names of one held directory, each with its facts, read relative to
/// the directory itself so no entry costs a path walk.
#[cfg(not(windows))]
pub struct HostStatReader(ReadDir);

#[cfg(not(windows))]
impl Iterator for HostStatReader {
    type Item = io::Result<HostListedEntry>;

    fn next(&mut self) -> Option<Self::Item> {
        let entry = match self.0.next()? {
            Ok(entry) => entry,
            Err(error) => return Some(Err(error)),
        };
        // One no-follow stat relative to the held directory descriptor.
        let stat = match entry.metadata() {
            Ok(stat) => Some(stat),
            Err(error) if error.kind() == io::ErrorKind::NotFound => None,
            Err(error) => return Some(Err(error)),
        };
        Some(Ok(HostListedEntry {
            name: entry.file_name(),
            stat,
        }))
    }
}

/// The names of one held directory, read one buffer of index records per
/// kernel call, each stat'ed by name relative to the held directory exactly
/// as a lookup stats it.
#[cfg(windows)]
pub struct HostStatReader {
    directory: Dir,
    volume_serial_number: Option<u32>,
    /// `u64` storage keeps every entry record 8-byte aligned.
    buffer: Vec<u64>,
    /// Byte offset of the next unread record in `buffer`, if any.
    next: Option<usize>,
    exhausted: bool,
}

#[cfg(windows)]
impl HostStatReader {
    const BUFFER_BYTES: usize = 64 * 1024;

    /// Reads the next buffer of records; `false` once none remain.
    #[allow(unsafe_code)]
    fn refill(&mut self) -> io::Result<bool> {
        use std::os::windows::io::{AsHandle as _, AsRawHandle as _};
        use windows::Win32::Foundation::{ERROR_NO_MORE_FILES, HANDLE};
        use windows::Win32::Storage::FileSystem::{
            FileIdExtdDirectoryInfo, GetFileInformationByHandleEx,
        };

        if self.exhausted {
            return Ok(false);
        }
        let bytes = u32::try_from(self.buffer.len() * std::mem::size_of::<u64>())
            .map_err(|_| io::Error::other("enumeration buffer size"))?;
        // SAFETY: the held directory handle and the owned buffer outlive this
        // synchronous call, and `bytes` is exactly the buffer's length.
        let result = unsafe {
            GetFileInformationByHandleEx(
                HANDLE(self.directory.as_handle().as_raw_handle()),
                FileIdExtdDirectoryInfo,
                self.buffer.as_mut_ptr().cast(),
                bytes,
            )
        };
        match result {
            Ok(()) => {
                self.next = Some(0);
                Ok(true)
            }
            Err(error)
                if error.code() == windows::core::HRESULT::from_win32(ERROR_NO_MORE_FILES.0) =>
            {
                self.exhausted = true;
                Ok(false)
            }
            Err(error) => Err(io::Error::from_raw_os_error(error.code().0 & 0xffff)),
        }
    }

    /// Decodes the record at `offset` and advances past it.
    #[allow(unsafe_code)]
    fn take(&mut self, offset: usize) -> io::Result<Option<HostListedEntry>> {
        use std::mem::{offset_of, size_of};
        use std::os::windows::ffi::OsStringExt as _;
        use windows::Win32::Storage::FileSystem::FILE_ID_EXTD_DIR_INFO;

        let total = self.buffer.len() * size_of::<u64>();
        let malformed = || io::Error::new(io::ErrorKind::InvalidData, "malformed directory record");
        let name_offset = offset_of!(FILE_ID_EXTD_DIR_INFO, FileName);
        if !offset.is_multiple_of(std::mem::align_of::<FILE_ID_EXTD_DIR_INFO>())
            || offset
                .checked_add(name_offset)
                .is_none_or(|end| end > total)
        {
            return Err(malformed());
        }
        // SAFETY: the record's fixed part lies within the buffer and is
        // aligned, as just checked; the kernel initialized it.
        let record = unsafe {
            &*self
                .buffer
                .as_ptr()
                .cast::<u8>()
                .add(offset)
                .cast::<FILE_ID_EXTD_DIR_INFO>()
        };
        let name_bytes = usize::try_from(record.FileNameLength).map_err(|_| malformed())?;
        let name_start = offset + name_offset;
        if !name_bytes.is_multiple_of(2)
            || name_start
                .checked_add(name_bytes)
                .is_none_or(|end| end > total)
        {
            return Err(malformed());
        }
        let step = usize::try_from(record.NextEntryOffset).map_err(|_| malformed())?;
        self.next = (step != 0).then_some(offset + step);
        // SAFETY: the name's UTF-16 units lie within the buffer, as checked,
        // and a record's name is 2-byte aligned after its aligned fixed part.
        let name = unsafe {
            std::slice::from_raw_parts(
                self.buffer
                    .as_ptr()
                    .cast::<u8>()
                    .add(name_start)
                    .cast::<u16>(),
                name_bytes / 2,
            )
        };
        let dot = u16::from(b'.');
        if name == [dot] || name == [dot, dot] {
            return Ok(None);
        }
        let name = OsString::from_wide(name);
        // The index names the entry; only the file record states its facts.
        let stat = match stat_at(&self.directory, Path::new(&name), self.volume_serial_number) {
            Ok(stat) => stat,
            Err(error) if error.kind() == io::ErrorKind::NotFound => None,
            Err(error) => return Err(error),
        };
        Ok(Some(HostListedEntry { name, stat }))
    }
}

#[cfg(windows)]
impl Iterator for HostStatReader {
    type Item = io::Result<HostListedEntry>;

    fn next(&mut self) -> Option<Self::Item> {
        loop {
            let Some(offset) = self.next.take() else {
                match self.refill() {
                    Ok(true) => continue,
                    Ok(false) => return None,
                    Err(error) => {
                        self.exhausted = true;
                        return Some(Err(error));
                    }
                }
            };
            match self.take(offset) {
                Ok(Some(entry)) => return Some(Ok(entry)),
                Ok(None) => {}
                Err(error) => {
                    self.exhausted = true;
                    return Some(Err(error));
                }
            }
        }
    }
}

/// One `FileStatInformation` query naming `path` relative to `directory`,
/// which no reparse point may redirect; its object's own reparse tag is
/// reported, not resolved. `None` when the path names the directory itself
/// or redirection was refused, which only a held walk may resolve.
#[cfg(windows)]
#[allow(unsafe_code)]
fn stat_information_at(
    directory: &Dir,
    path: &Path,
) -> io::Result<Option<windows::Wdk::Storage::FileSystem::FILE_STAT_INFORMATION>> {
    use std::os::windows::io::{AsHandle as _, AsRawHandle as _};
    use windows::Wdk::Foundation::OBJECT_ATTRIBUTES;
    use windows::Wdk::Storage::FileSystem::{
        FILE_STAT_INFORMATION, FileStatInformation, NtQueryInformationByName,
    };
    use windows::Win32::Foundation::{
        HANDLE, NTSTATUS, OBJ_CASE_INSENSITIVE, OBJ_DONT_REPARSE, RtlNtStatusToDosError,
        UNICODE_STRING,
    };
    use windows::Win32::System::IO::IO_STATUS_BLOCK;
    const REPARSE_POINT_ENCOUNTERED: NTSTATUS = NTSTATUS(0xC000_050B_u32.cast_signed());

    let Some((mut name, length)) = relative_kernel_name(path) else {
        return Ok(None);
    };
    let name = UNICODE_STRING {
        Length: length,
        MaximumLength: length,
        Buffer: windows::core::PWSTR(name.as_mut_ptr()),
    };
    let attributes = OBJECT_ATTRIBUTES {
        Length: u32::try_from(std::mem::size_of::<OBJECT_ATTRIBUTES>())
            .map_err(|_| io::Error::other("object attributes size"))?,
        RootDirectory: HANDLE(directory.as_handle().as_raw_handle()),
        ObjectName: &raw const name,
        // Win32 names are case-insensitive unless the directory says
        // otherwise; no reparse point may redirect the query.
        Attributes: OBJ_CASE_INSENSITIVE | OBJ_DONT_REPARSE,
        ..OBJECT_ATTRIBUTES::default()
    };
    let mut status_block = IO_STATUS_BLOCK::default();
    let mut information = FILE_STAT_INFORMATION::default();
    // SAFETY: every pointer names a live, correctly sized value for this
    // synchronous query, and the held directory handle outlives it.
    let status = unsafe {
        NtQueryInformationByName(
            &raw const attributes,
            &raw mut status_block,
            (&raw mut information).cast(),
            u32::try_from(std::mem::size_of::<FILE_STAT_INFORMATION>())
                .map_err(|_| io::Error::other("stat information size"))?,
            FileStatInformation,
        )
    };
    if status == REPARSE_POINT_ENCOUNTERED {
        return Ok(None);
    }
    if status.is_err() {
        // SAFETY: a pure status-code translation.
        let code = unsafe { RtlNtStatusToDosError(status) };
        return Err(io::Error::from_raw_os_error(
            i32::try_from(code).map_err(|_| io::Error::other("unmapped stat status"))?,
        ));
    }
    Ok(Some(information))
}

/// Stats `path` relative to `directory` with one [`stat_information_at`]
/// query, on the volume `volume_serial_number` names. `None` when the path
/// crosses or ends in a reparse point, which only a held walk may resolve.
#[cfg(windows)]
fn stat_at(
    directory: &Dir,
    path: &Path,
    volume_serial_number: Option<u32>,
) -> io::Result<Option<HostStat>> {
    match stat_information_at(directory, path)? {
        Some(information) if information.ReparseTag == 0 => {
            HostStat::from_stat_information(&information, volume_serial_number).map(Some)
        }
        _ => Ok(None),
    }
}

/// A held directory capability whose relative operations cannot escape through
/// path traversal or an intermediate symbolic link/reparse point.
pub struct HostRoot {
    directory: Dir,
    identity: crate::NativeRootIdentity,
}

pub(crate) struct HostDirectoryEntry {
    pub(crate) name: OsString,
    pub(crate) is_dir: bool,
}

pub(crate) enum HostReadDir<'a> {
    #[cfg(target_os = "linux")]
    Linux {
        entries: rustix::fs::Dir,
        held: &'a Dir,
    },
    #[cfg(not(target_os = "linux"))]
    Native(ReadDir, std::marker::PhantomData<&'a Dir>),
}

impl Iterator for HostReadDir<'_> {
    type Item = io::Result<HostDirectoryEntry>;

    fn next(&mut self) -> Option<Self::Item> {
        match self {
            #[cfg(target_os = "linux")]
            Self::Linux { entries, held } => loop {
                let entry = match entries.next()? {
                    Ok(entry) => entry,
                    Err(error) => return Some(Err(error.into())),
                };
                use std::os::unix::ffi::OsStrExt as _;
                let name = OsStr::from_bytes(entry.file_name().to_bytes());
                if name == "." || name == ".." {
                    continue;
                }
                let is_dir = match entry.file_type() {
                    rustix::fs::FileType::Directory => true,
                    rustix::fs::FileType::Unknown => match held.symlink_metadata(name) {
                        Ok(metadata) => metadata.is_dir(),
                        Err(error) => return Some(Err(error)),
                    },
                    _ => false,
                };
                return Some(Ok(HostDirectoryEntry {
                    name: name.to_os_string(),
                    is_dir,
                }));
            },
            #[cfg(not(target_os = "linux"))]
            Self::Native(entries, _) => entries.next().map(|entry| {
                let entry = entry?;
                let file_type = entry.file_type()?;
                Ok(HostDirectoryEntry {
                    name: entry.file_name(),
                    is_dir: file_type.is_dir() && !file_type.is_symlink(),
                })
            }),
        }
    }
}

/// A held directory capability for race-free leaf operations.
pub struct HostDirectory {
    directory: Dir,
}

#[cfg(unix)]
fn held_parent_leaf(root: &Dir, path: &Path) -> io::Result<(Dir, std::ffi::CString)> {
    use std::os::unix::ffi::OsStrExt as _;

    if path.as_os_str().is_empty() {
        return Ok((
            root.try_clone()?,
            std::ffi::CString::new(".").map_err(io::Error::other)?,
        ));
    }
    let mut parent = root.try_clone()?;
    let mut components = path.components().peekable();
    while let Some(component) = components.next() {
        let std::path::Component::Normal(name) = component else {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "invalid metadata path",
            ));
        };
        if components.peek().is_none() {
            let leaf = std::ffi::CString::new(name.as_bytes())
                .map_err(|_| io::Error::new(io::ErrorKind::InvalidInput, "path contains NUL"))?;
            return Ok((parent, leaf));
        }
        parent = parent.open_dir_nofollow(name)?;
    }
    Err(io::Error::new(
        io::ErrorKind::InvalidInput,
        "metadata path has no leaf",
    ))
}

#[cfg(target_os = "linux")]
#[allow(unsafe_code)]
#[cfg(any(feature = "native-mount", test))]
impl LinuxMetadataTarget {
    fn open(root: &Dir, path: &Path) -> io::Result<Self> {
        use std::os::fd::{AsRawFd as _, FromRawFd as _};

        let (parent, leaf) = held_parent_leaf(root, path)?;
        // SAFETY: the held parent and leaf are live; O_PATH avoids opening a
        // FIFO or device payload and O_NOFOLLOW binds the final inode itself.
        let fd = unsafe {
            libc::openat(
                parent.as_raw_fd(),
                leaf.as_ptr(),
                libc::O_PATH | libc::O_NOFOLLOW | libc::O_CLOEXEC,
            )
        };
        if fd < 0 {
            return Err(io::Error::last_os_error());
        }
        // SAFETY: successful openat returned one exclusively owned descriptor.
        Ok(Self {
            inode: unsafe { std::os::fd::OwnedFd::from_raw_fd(fd) },
        })
    }

    fn observed(&self) -> io::Result<libc::stat> {
        use std::os::fd::AsRawFd as _;

        let mut stat = std::mem::MaybeUninit::<libc::stat>::uninit();
        // SAFETY: the held inode is live and fstat initializes stat on success.
        if unsafe { libc::fstat(self.inode.as_raw_fd(), stat.as_mut_ptr()) } != 0 {
            return Err(io::Error::last_os_error());
        }
        // SAFETY: fstat succeeded and initialized the complete structure.
        Ok(unsafe { stat.assume_init() })
    }

    pub(crate) fn apply(
        &self,
        metadata: crate::kernel::FileMetadata,
    ) -> Result<(), LinuxMetadataError> {
        use crate::kernel::MetadataField;
        use std::os::fd::AsRawFd as _;

        let mut observed = self.observed()?;
        validate_linux_metadata(observed, metadata)?;
        let requested_uid = match metadata.posix_uid {
            MetadataField::Value(uid) if uid != observed.st_uid => uid,
            _ => libc::uid_t::MAX,
        };
        let requested_gid = match metadata.posix_gid {
            MetadataField::Value(gid) if gid != observed.st_gid => gid,
            _ => libc::gid_t::MAX,
        };
        if requested_uid != libc::uid_t::MAX || requested_gid != libc::gid_t::MAX {
            // SAFETY: AT_EMPTY_PATH acts on the pinned inode, including a final symlink.
            if unsafe {
                libc::fchownat(
                    self.inode.as_raw_fd(),
                    c"".as_ptr(),
                    requested_uid,
                    requested_gid,
                    libc::AT_EMPTY_PATH,
                )
            } != 0
            {
                return Err(io::Error::last_os_error().into());
            }
            observed = self.observed()?;
        }
        if let MetadataField::Value(mode) = metadata.posix_mode {
            let desired = mode & 0o7777;
            if desired != observed.st_mode & 0o7777 {
                // SAFETY: fchmodat2 with AT_EMPTY_PATH acts only on the held inode.
                if unsafe {
                    libc::syscall(
                        LINUX_FCHMODAT2_SYSCALL,
                        self.inode.as_raw_fd(),
                        c"".as_ptr(),
                        desired,
                        libc::AT_EMPTY_PATH,
                    )
                } != 0
                {
                    return Err(io::Error::last_os_error().into());
                }
                observed = self.observed()?;
            }
        }
        if matches!(metadata.modified_ns, MetadataField::Value(_))
            || matches!(metadata.accessed_ns, MetadataField::Value(_))
        {
            let times = [
                linux_time_spec(metadata.accessed_ns)?,
                linux_time_spec(metadata.modified_ns)?,
            ];
            // SAFETY: AT_EMPTY_PATH targets the held inode and the timespecs
            // remain live until the syscall returns.
            if unsafe {
                libc::utimensat(
                    self.inode.as_raw_fd(),
                    c"".as_ptr(),
                    times.as_ptr(),
                    libc::AT_EMPTY_PATH | libc::AT_SYMLINK_NOFOLLOW,
                )
            } != 0
            {
                return Err(io::Error::last_os_error().into());
            }
            observed = self.observed()?;
        }
        verify_linux_metadata(observed, metadata)
    }
}

#[cfg(target_os = "linux")]
#[cfg(target_arch = "aarch64")]
// libc does not expose SYS_fchmodat2 on aarch64 yet; asm-generic/unistd.h does.
#[cfg(any(feature = "native-mount", test))]
const LINUX_FCHMODAT2_SYSCALL: libc::c_long = 452;

#[cfg(target_os = "linux")]
#[cfg(not(target_arch = "aarch64"))]
#[cfg(any(feature = "native-mount", test))]
const LINUX_FCHMODAT2_SYSCALL: libc::c_long = libc::SYS_fchmodat2;

#[cfg(target_os = "linux")]
#[cfg(any(feature = "native-mount", test))]
fn validate_linux_metadata(
    observed: libc::stat,
    metadata: crate::kernel::FileMetadata,
) -> Result<(), LinuxMetadataError> {
    use crate::kernel::MetadataField;

    for (present, field) in [
        (
            matches!(metadata.windows_attributes, MetadataField::Value(_)),
            "windows_attributes",
        ),
        (
            matches!(metadata.posix_flags, MetadataField::Value(_)),
            "posix_flags",
        ),
        (
            matches!(metadata.named_attributes, MetadataField::Value(_)),
            "named_attributes",
        ),
        (matches!(metadata.acl, MetadataField::Value(_)), "acl"),
        (
            matches!(metadata.security_descriptor, MetadataField::Value(_)),
            "security_descriptor",
        ),
        (
            matches!(metadata.created_ns, MetadataField::Value(_)),
            "created_ns",
        ),
        (
            matches!(metadata.changed_ns, MetadataField::Value(_)),
            "changed_ns",
        ),
    ] {
        if present {
            return Err(LinuxMetadataError::Unsupported(field));
        }
    }
    if let MetadataField::Value(mode) = metadata.posix_mode {
        let kind = mode & libc::S_IFMT;
        if kind != 0 && kind != observed.st_mode & libc::S_IFMT {
            return Err(LinuxMetadataError::Unsupported("posix_mode file type"));
        }
        if mode & 0o7777 != observed.st_mode & 0o7777 {
            if observed.st_mode & libc::S_IFMT == libc::S_IFLNK {
                return Err(LinuxMetadataError::Unsupported("posix_mode symlink"));
            }
            linux_require_fchmodat2()?;
        }
    }
    Ok(())
}

#[cfg(target_os = "linux")]
#[allow(unsafe_code)]
#[cfg(any(feature = "native-mount", test))]
fn linux_require_fchmodat2() -> Result<(), LinuxMetadataError> {
    // An invalid descriptor makes the capability probe non-mutating. A kernel
    // with fchmodat2 and AT_EMPTY_PATH support returns EBADF; an older kernel
    // returns ENOSYS or EINVAL before any ownership change is attempted.
    let result = unsafe {
        libc::syscall(
            LINUX_FCHMODAT2_SYSCALL,
            -1,
            c"".as_ptr(),
            0,
            libc::AT_EMPTY_PATH,
        )
    };
    if result == 0 {
        return Err(LinuxMetadataError::Unsupported("fchmodat2 probe"));
    }
    match io::Error::last_os_error().raw_os_error() {
        Some(libc::EBADF) => Ok(()),
        Some(libc::ENOSYS | libc::EINVAL) => {
            Err(LinuxMetadataError::Unsupported("fchmodat2 AT_EMPTY_PATH"))
        }
        _ => Err(io::Error::last_os_error().into()),
    }
}

#[cfg(target_os = "linux")]
#[cfg(any(feature = "native-mount", test))]
fn verify_linux_metadata(
    observed: libc::stat,
    metadata: crate::kernel::FileMetadata,
) -> Result<(), LinuxMetadataError> {
    use crate::kernel::MetadataField;

    if let MetadataField::Value(uid) = metadata.posix_uid
        && observed.st_uid != uid
    {
        return Err(LinuxMetadataError::Unsupported("posix_uid"));
    }
    if let MetadataField::Value(gid) = metadata.posix_gid
        && observed.st_gid != gid
    {
        return Err(LinuxMetadataError::Unsupported("posix_gid"));
    }
    if let MetadataField::Value(mode) = metadata.posix_mode
        && observed.st_mode & 0o7777 != mode & 0o7777
    {
        return Err(LinuxMetadataError::Unsupported("posix_mode"));
    }
    if let MetadataField::Value(ns) = metadata.accessed_ns
        && linux_observed_ns(observed.st_atime, observed.st_atime_nsec)? != ns
    {
        return Err(LinuxMetadataError::Unsupported("accessed_ns"));
    }
    if let MetadataField::Value(ns) = metadata.modified_ns
        && linux_observed_ns(observed.st_mtime, observed.st_mtime_nsec)? != ns
    {
        return Err(LinuxMetadataError::Unsupported("modified_ns"));
    }
    Ok(())
}

#[cfg(target_os = "linux")]
#[cfg(any(feature = "native-mount", test))]
fn linux_time_spec(
    field: crate::kernel::MetadataField<i64>,
) -> Result<libc::timespec, LinuxMetadataError> {
    let crate::kernel::MetadataField::Value(ns) = field else {
        return Ok(libc::timespec {
            tv_sec: 0,
            tv_nsec: libc::UTIME_OMIT,
        });
    };
    Ok(libc::timespec {
        tv_sec: libc::time_t::try_from(ns.div_euclid(1_000_000_000))
            .map_err(|_| LinuxMetadataError::Unsupported("timestamp range"))?,
        tv_nsec: libc::c_long::try_from(ns.rem_euclid(1_000_000_000))
            .map_err(|_| LinuxMetadataError::Unsupported("timestamp range"))?,
    })
}

#[cfg(target_os = "linux")]
#[cfg(any(feature = "native-mount", test))]
fn linux_observed_ns(seconds: i64, nanos: i64) -> Result<i64, LinuxMetadataError> {
    seconds
        .checked_mul(1_000_000_000)
        .and_then(|whole| whole.checked_add(nanos))
        .ok_or(LinuxMetadataError::Unsupported("timestamp range"))
}

impl HostRoot {
    /// Opens an existing real directory without following the final path.
    pub fn open(path: &Path) -> io::Result<Self> {
        let file = open_root_directory(path)?;
        let metadata = file.metadata()?;
        if !metadata.is_dir() || root_is_reparse_point(&metadata) {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "host root is not a real directory",
            ));
        }
        let identity = crate::NativeRootIdentity::from_file(&file)?;
        Ok(Self {
            directory: Dir::from_std_file(file),
            identity,
        })
    }

    #[must_use]
    pub const fn identity(&self) -> crate::NativeRootIdentity {
        self.identity
    }

    /// Whether this root's filesystem is local. A network or user-space
    /// filesystem can stall a request indefinitely, so only local host I/O
    /// may block a native callback thread that must answer within its
    /// timeout. A filesystem that cannot be classified counts as remote.
    pub(crate) fn is_local(&self) -> bool {
        filesystem_is_local(&self.directory)
    }

    /// The held root directory's handle, for volume queries that must be
    /// bound to exactly this root.
    #[cfg(windows)]
    pub(crate) fn directory_handle(&self) -> std::os::windows::io::BorrowedHandle<'_> {
        use std::os::windows::io::AsHandle as _;
        self.directory.as_handle()
    }

    pub fn is_empty(&self) -> io::Result<bool> {
        Ok(self.directory.entries()?.next().is_none())
    }

    pub fn read_dir(&self, path: &Path) -> io::Result<ReadDir> {
        if path.as_os_str().is_empty() {
            self.directory.entries()
        } else {
            self.directory.read_dir(path)
        }
    }

    /// Enumerates the directory at `path`, reached without following any
    /// link, with each entry's facts as [`Self::stat`] reports them, or with
    /// `None` where only a separate stat of that name can report them.
    pub fn read_dir_stats(&self, path: &Path) -> io::Result<HostStatReader> {
        #[cfg(windows)]
        {
            // An enumeration's position belongs to the open file object, which
            // a duplicated handle shares, so each listing opens its own.
            let directory = if path.as_os_str().is_empty() {
                self.directory.open_dir(Path::new("."))?
            } else {
                self.open_dir_held(path)?
            };
            Ok(HostStatReader {
                directory,
                // A path without reparse points never leaves the root's volume.
                volume_serial_number: u32::try_from(self.identity.device).ok(),
                buffer: vec![0; HostStatReader::BUFFER_BYTES / std::mem::size_of::<u64>()],
                next: None,
                exhausted: false,
            })
        }
        #[cfg(not(windows))]
        {
            self.open_dir_held(path)?.entries().map(HostStatReader)
        }
    }

    /// Enumerates through held directory capabilities without following any
    /// intermediate symlink or reparse point.
    pub fn open_dir_held(&self, path: &Path) -> io::Result<Dir> {
        let mut current = self.directory.try_clone()?;
        for component in path.components() {
            let std::path::Component::Normal(name) = component else {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidInput,
                    "invalid directory path",
                ));
            };
            #[cfg(target_os = "linux")]
            {
                use cap_std::fs::OpenOptionsExt as _;

                let mut options = OpenOptions::new();
                options
                    .read(true)
                    .custom_flags(libc::O_DIRECTORY | libc::O_NOATIME);
                options._cap_fs_ext_follow(cap_primitives::fs::FollowSymlinks::No);
                current = match current.open_with(name, &options) {
                    Ok(file) => Dir::from_std_file(file.into_std()),
                    Err(error) if error.kind() == io::ErrorKind::PermissionDenied => {
                        current.open_dir_nofollow(name)?
                    }
                    Err(error) => return Err(error),
                };
            }
            #[cfg(not(target_os = "linux"))]
            {
                current = current.open_dir_nofollow(name)?;
            }
        }
        Ok(current)
    }

    pub(crate) fn scan_held_dir(directory: &Dir) -> io::Result<HostReadDir<'_>> {
        #[cfg(target_os = "linux")]
        {
            let entries = rustix::fs::Dir::read_from(directory)?;
            Ok(HostReadDir::Linux {
                entries,
                held: directory,
            })
        }
        #[cfg(not(target_os = "linux"))]
        {
            Ok(HostReadDir::Native(
                directory.entries()?,
                std::marker::PhantomData,
            ))
        }
    }

    pub fn symlink_metadata(&self, path: &Path) -> io::Result<Metadata> {
        if path.as_os_str().is_empty() {
            self.directory.dir_metadata()
        } else {
            self.directory.symlink_metadata(path)
        }
    }

    /// Stats `path` without following its final link, as
    /// [`Self::symlink_metadata`] does.
    #[cfg(not(windows))]
    pub fn stat(&self, path: &Path) -> io::Result<HostStat> {
        self.symlink_metadata(path)
    }

    /// Stats `path` without following its final link, as
    /// [`Self::symlink_metadata`] does. A path free of reparse points is
    /// answered by one kernel query against the held root, without opening
    /// a handle per component; any reparse point on it takes the held walk.
    #[cfg(windows)]
    pub fn stat(&self, path: &Path) -> io::Result<HostStat> {
        match self.stat_by_name(path)? {
            Some(stat) => Ok(stat),
            None => self
                .symlink_metadata(path)
                .map(|metadata| HostStat::from_metadata(&metadata)),
        }
    }

    /// One `FileStatInformation` query naming `path` relative to the held
    /// root. `None` when the path names the root itself or crosses or ends
    /// in any reparse point, which only the held walk may resolve.
    #[cfg(windows)]
    fn stat_by_name(&self, path: &Path) -> io::Result<Option<HostStat>> {
        stat_at(&self.directory, path, self.volume_serial_number())
    }

    /// One `FileStatInformation` query naming `path` relative to the held
    /// root, which no reparse point may redirect; its object's own reparse
    /// tag is reported, not resolved. `None` when the path names the root
    /// itself or redirection was refused, which only the held walk may
    /// resolve. A name the query answers lies on the root's volume.
    #[cfg(all(windows, feature = "native-mount"))]
    pub(crate) fn stat_information_by_name(
        &self,
        path: &Path,
    ) -> io::Result<Option<windows::Wdk::Storage::FileSystem::FILE_STAT_INFORMATION>> {
        stat_information_at(&self.directory, path)
    }

    /// The volume every name resolved without a reparse point lies on.
    #[cfg(windows)]
    fn volume_serial_number(&self) -> Option<u32> {
        u32::try_from(self.identity.device).ok()
    }

    /// Stats one file this root opened, exactly as [`Self::stat`] stats its
    /// name: the same query reports the same facts for the same object.
    #[cfg(not(windows))]
    pub fn stat_file(&self, file: &File) -> io::Result<HostStat> {
        Metadata::from_file(file)
    }

    /// Stats one file this root opened, exactly as [`Self::stat`] stats its
    /// name: one `FileStatInformation` query on the handle, or, for a
    /// reparse point, the handle facts the held walk also reads.
    #[cfg(windows)]
    #[allow(unsafe_code)]
    pub fn stat_file(&self, file: &File) -> io::Result<HostStat> {
        use std::os::windows::io::{AsHandle as _, AsRawHandle as _};
        use windows::Wdk::Storage::FileSystem::{
            FILE_STAT_INFORMATION, FileStatInformation, NtQueryInformationFile,
        };
        use windows::Win32::Foundation::{HANDLE, RtlNtStatusToDosError};
        use windows::Win32::System::IO::IO_STATUS_BLOCK;

        let mut status_block = IO_STATUS_BLOCK::default();
        let mut information = FILE_STAT_INFORMATION::default();
        // SAFETY: the handle, status block, and correctly sized information
        // buffer all outlive this synchronous query.
        let status = unsafe {
            NtQueryInformationFile(
                HANDLE(file.as_handle().as_raw_handle()),
                &raw mut status_block,
                (&raw mut information).cast(),
                u32::try_from(std::mem::size_of::<FILE_STAT_INFORMATION>())
                    .map_err(|_| io::Error::other("stat information size"))?,
                FileStatInformation,
            )
        };
        if status.is_err() {
            // SAFETY: a pure status-code translation.
            let code = unsafe { RtlNtStatusToDosError(status) };
            return Err(io::Error::from_raw_os_error(
                i32::try_from(code).map_err(|_| io::Error::other("unmapped stat status"))?,
            ));
        }
        if information.ReparseTag != 0 {
            return Metadata::from_file(file).map(|metadata| HostStat::from_metadata(&metadata));
        }
        // A file opened without traversing a reparse point lies on the
        // root's volume.
        HostStat::from_stat_information(&information, self.volume_serial_number())
    }

    /// Reads leaf metadata while refusing every intermediate link or reparse point.
    pub fn symlink_metadata_held(&self, path: &Path) -> io::Result<Metadata> {
        if path.as_os_str().is_empty() {
            return self.directory.dir_metadata();
        }
        let mut current = self.directory.try_clone()?;
        let mut components = path.components().peekable();
        while let Some(component) = components.next() {
            let std::path::Component::Normal(name) = component else {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidInput,
                    "invalid metadata path",
                ));
            };
            if components.peek().is_none() {
                return current.symlink_metadata(name);
            }
            current = current.open_dir_nofollow(name)?;
        }
        Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "metadata path has no leaf",
        ))
    }

    pub fn open_file(&self, path: &Path) -> io::Result<cap_std::fs::File> {
        #[cfg(windows)]
        if let Some(file) = self.open_file_by_name(path)? {
            return Ok(file);
        }
        let mut options = OpenOptions::new();
        options
            .read(true)
            ._cap_fs_ext_follow(cap_primitives::fs::FollowSymlinks::No);
        #[cfg(target_os = "linux")]
        {
            use cap_std::fs::OpenOptionsExt as _;

            options.custom_flags(libc::O_NOATIME);
            match self.directory.open_with(path, &options) {
                Ok(file) => return Ok(file),
                Err(error) if error.kind() == io::ErrorKind::PermissionDenied => {
                    options.custom_flags(0);
                }
                Err(error) => return Err(error),
            }
        }
        self.directory.open_with(path, &options)
    }

    /// Opens `path` for reading with one `NtCreateFile` relative to the held
    /// root, which no reparse point may redirect; a final reparse point is
    /// opened as itself, as the held walk opens it. `None` when the path
    /// names the root itself or redirection was refused, which only the held
    /// walk may resolve.
    #[cfg(windows)]
    #[allow(unsafe_code)]
    fn open_file_by_name(&self, path: &Path) -> io::Result<Option<cap_std::fs::File>> {
        use std::os::windows::io::{AsHandle as _, AsRawHandle as _, FromRawHandle as _};
        use windows::Wdk::Foundation::OBJECT_ATTRIBUTES;
        use windows::Wdk::Storage::FileSystem::{
            FILE_NON_DIRECTORY_FILE, FILE_OPEN, FILE_OPEN_REPARSE_POINT,
            FILE_SYNCHRONOUS_IO_NONALERT, NtCreateFile,
        };
        use windows::Win32::Foundation::{
            HANDLE, NTSTATUS, OBJ_CASE_INSENSITIVE, OBJ_DONT_REPARSE, RtlNtStatusToDosError,
            UNICODE_STRING,
        };
        use windows::Win32::Storage::FileSystem::{
            FILE_ATTRIBUTE_NORMAL, FILE_GENERIC_READ, FILE_SHARE_DELETE, FILE_SHARE_READ,
            FILE_SHARE_WRITE,
        };
        use windows::Win32::System::IO::IO_STATUS_BLOCK;
        const REPARSE_POINT_ENCOUNTERED: NTSTATUS = NTSTATUS(0xC000_050B_u32.cast_signed());

        let Some((mut name, length)) = relative_kernel_name(path) else {
            return Ok(None);
        };
        let name = UNICODE_STRING {
            Length: length,
            MaximumLength: length,
            Buffer: windows::core::PWSTR(name.as_mut_ptr()),
        };
        let attributes = OBJECT_ATTRIBUTES {
            Length: u32::try_from(std::mem::size_of::<OBJECT_ATTRIBUTES>())
                .map_err(|_| io::Error::other("object attributes size"))?,
            RootDirectory: HANDLE(self.directory.as_handle().as_raw_handle()),
            ObjectName: &raw const name,
            Attributes: OBJ_CASE_INSENSITIVE | OBJ_DONT_REPARSE,
            ..OBJECT_ATTRIBUTES::default()
        };
        let mut status_block = IO_STATUS_BLOCK::default();
        let mut handle = HANDLE::default();
        // SAFETY: every pointer names a live, correctly sized value for this
        // synchronous call, and the held root handle outlives it.
        let status = unsafe {
            NtCreateFile(
                &raw mut handle,
                FILE_GENERIC_READ,
                &raw const attributes,
                &raw mut status_block,
                None,
                FILE_ATTRIBUTE_NORMAL,
                FILE_SHARE_READ | FILE_SHARE_WRITE | FILE_SHARE_DELETE,
                FILE_OPEN,
                FILE_NON_DIRECTORY_FILE | FILE_SYNCHRONOUS_IO_NONALERT | FILE_OPEN_REPARSE_POINT,
                None,
                0,
            )
        };
        if status == REPARSE_POINT_ENCOUNTERED {
            return Ok(None);
        }
        if status.is_err() {
            // SAFETY: a pure status-code translation.
            let code = unsafe { RtlNtStatusToDosError(status) };
            return Err(io::Error::from_raw_os_error(
                i32::try_from(code).map_err(|_| io::Error::other("unmapped open status"))?,
            ));
        }
        // SAFETY: the call succeeded, so `handle` is a new handle this
        // function exclusively owns.
        let file = unsafe { File::from_raw_handle(handle.0) };
        Ok(Some(cap_std::fs::File::from_std(file)))
    }

    pub fn create_file(&self, path: &Path) -> io::Result<File> {
        let mut options = OpenOptions::new();
        options.write(true).create_new(true);
        self.directory
            .open_with(path, &options)
            .map(cap_std::fs::File::into_std)
    }

    /// Creates a capability-rooted Windows file ready for native overlapped I/O.
    /// The returned handle has not been associated with a completion port.
    #[cfg(windows)]
    pub fn create_overlapped_file(&self, path: &Path) -> io::Result<File> {
        use cap_std::fs::OpenOptionsExt as _;
        use windows::Win32::Storage::FileSystem::FILE_FLAG_OVERLAPPED;

        let mut options = OpenOptions::new();
        options
            .write(true)
            .create_new(true)
            .custom_flags(FILE_FLAG_OVERLAPPED.0);
        self.directory
            .open_with(path, &options)
            .map(cap_std::fs::File::into_std)
    }

    pub fn create_dir(&self, path: &Path) -> io::Result<()> {
        self.directory.create_dir(path)
    }

    /// Opens or creates each real directory component without following links.
    pub fn create_dir_all_held(&self, path: &Path) -> io::Result<HostDirectory> {
        let mut current = self.directory.try_clone()?;
        for component in path.components() {
            let std::path::Component::Normal(name) = component else {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidInput,
                    "invalid directory path",
                ));
            };
            match current.open_dir_nofollow(name) {
                Ok(next) => current = next,
                Err(error) if error.kind() == io::ErrorKind::NotFound => {
                    current.create_dir(name)?;
                    current = current.open_dir_nofollow(name)?;
                }
                Err(error) => return Err(error),
            }
        }
        Ok(HostDirectory { directory: current })
    }

    pub fn hard_link(&self, source: &Path, destination: &Path) -> io::Result<()> {
        self.directory
            .hard_link(source, &self.directory, destination)
    }

    /// Creates an APFS copy-on-write clone when the hosting volume supports it.
    /// The source and destination remain rooted in held directory capabilities.
    #[cfg(target_os = "macos")]
    #[allow(unsafe_code)]
    pub fn clone_file(&self, source: &Path, destination: &Path) -> io::Result<bool> {
        self.clone_file_from(self, source, destination)
    }

    /// Clones one regular file from a held source root into this held destination.
    /// This does not seal a multi-file source view; the caller must do that.
    #[cfg(target_os = "macos")]
    #[allow(unsafe_code)]
    pub fn clone_file_from(
        &self,
        source_root: &HostRoot,
        source: &Path,
        destination: &Path,
    ) -> io::Result<bool> {
        use std::os::fd::AsRawFd;
        use std::os::unix::ffi::OsStrExt;

        let source = match source_root.open_file(source) {
            Ok(source) => source,
            Err(error) if error.kind() == io::ErrorKind::PermissionDenied => return Ok(false),
            Err(error) => return Err(error),
        };
        let parent_path = destination.parent().unwrap_or_else(|| Path::new(""));
        let parent = if parent_path.as_os_str().is_empty() {
            self.directory.try_clone()?
        } else {
            self.directory.open_dir(parent_path)?
        };
        let name = destination.file_name().ok_or_else(|| {
            io::Error::new(io::ErrorKind::InvalidInput, "clone destination has no name")
        })?;
        let name = std::ffi::CString::new(name.as_bytes())
            .map_err(|_| io::Error::new(io::ErrorKind::InvalidInput, "clone name contains NUL"))?;
        // SAFETY: the source and destination-parent descriptors remain live,
        // and the target is one NUL-terminated leaf below the held directory.
        if unsafe { libc::fclonefileat(source.as_raw_fd(), parent.as_raw_fd(), name.as_ptr(), 0) }
            == 0
        {
            return Ok(true);
        }
        let error = io::Error::last_os_error();
        if matches!(
            error.raw_os_error(),
            Some(libc::ENOTSUP | libc::ENOSYS | libc::EXDEV)
        ) {
            Ok(false)
        } else {
            Err(error)
        }
    }

    /// Creates a same-volume Windows copy-on-write clone when the filesystem
    /// accepts the complete file as one block-clone range. An unsupported or
    /// unaligned request leaves no destination so callers can write normally.
    #[cfg(windows)]
    pub fn clone_file(&self, source: &Path, destination: &Path) -> io::Result<bool> {
        self.clone_file_from(self, source, destination)
    }

    /// Clones a regular file from a held source root into this root when the
    /// destination filesystem supports block cloning. Neither root is sealed
    /// here; callers must keep the source immutable for the operation.
    #[cfg(windows)]
    pub fn clone_file_from(
        &self,
        source_root: &HostRoot,
        source: &Path,
        destination: &Path,
    ) -> io::Result<bool> {
        self.clone_file_from_identity(source_root, source, destination, None)
    }

    #[cfg(windows)]
    fn clone_file_from_identity(
        &self,
        source_root: &HostRoot,
        source: &Path,
        destination: &Path,
        expected: Option<crate::NativeRootIdentity>,
    ) -> io::Result<bool> {
        let source = open_windows_regular_source(&source_root.directory, source, false)?;
        if expected.is_some_and(|identity| {
            crate::NativeRootIdentity::from_file(&source).ok() != Some(identity)
        }) {
            return Err(io::Error::other("copy source identity changed"));
        }
        let length = source.metadata()?.len();
        let mut source = cap_std::fs::File::from_std(source);
        if length < 4 * 1024 || i64::try_from(length).is_err() {
            return Ok(false);
        }
        // ReFS volumes use either 4-KiB or 64-KiB clusters. Try the common
        // smaller unit first for maximum sharing, then retry at 64 KiB when
        // the volume requires it. A failed attempt is always removed.
        for alignment in [4 * 1024, 64 * 1024] {
            let mut target = create_windows_copy_target(&self.directory, destination, false)?;
            let cloned = clone_windows_file(&mut source, &mut target, length, alignment).is_ok();
            drop(target);
            if cloned {
                return Ok(true);
            }
            self.directory.remove_file(destination)?;
        }
        Ok(false)
    }

    /// Copies one pinned regular source into a new file using `ReFS` block
    /// cloning when available, then owned-buffer overlapped I/O otherwise.
    /// The caller must keep the source immutable; the copy fails rather than
    /// replace an existing destination. This byte primitive does not capture
    /// SDK lineage or preserve multi-file hard-link topology.
    ///
    /// The destination name only ever holds the complete copy: bytes are
    /// written under a private staging name beside it and renamed into place
    /// once complete, so a failed or cancelled copy leaves the destination
    /// exactly as absent as it was.
    #[cfg(windows)]
    pub async fn copy_file_from(
        &self,
        source_root: &HostRoot,
        source: &Path,
        destination: &Path,
    ) -> io::Result<()> {
        let source_root = HostRoot {
            directory: source_root.directory.try_clone()?,
            identity: source_root.identity,
        };
        let (parent, name) = open_windows_parent(&self.directory, destination)?;
        let name = name.to_os_string();
        let staging_root = HostRoot {
            directory: parent,
            identity: self.identity,
        };
        let staged = staging_name();
        let source = source.to_path_buf();
        let (sender, receiver) = tokio::sync::oneshot::channel();
        // The detached owner drains admitted kernel I/O before discarding the
        // staged bytes even when its awaiting caller is cancelled.
        tokio::spawn(async move {
            let mut created = false;
            let mut result = copy_windows_file_worker(
                &source_root,
                &source,
                &staging_root,
                &staged,
                &sender,
                &mut created,
            )
            .await;
            if result.is_ok() {
                result = if sender.is_closed() {
                    Err(io::Error::new(io::ErrorKind::Interrupted, "copy cancelled"))
                } else {
                    let parent = staging_root.directory.try_clone();
                    let staged = staged.clone();
                    match parent {
                        Ok(parent) => acyclic_native_runtime::run_blocking_io(move || {
                            publish_windows_file(&parent, &staged, &name)
                        })
                        .await
                        .and_then(|published| published),
                        Err(error) => Err(error),
                    }
                };
            }
            if created && result.is_err() {
                let root = staging_root.directory.try_clone();
                let path = staged.clone();
                if let Ok(root) = root {
                    let _ =
                        acyclic_native_runtime::run_blocking_io(move || root.remove_file(&path))
                            .await;
                }
            }
            let _ = sender.send(result);
        });
        receiver
            .await
            .map_err(|_| io::Error::other("native copy owner stopped before completion"))?
    }

    pub fn read_link(&self, path: &Path) -> io::Result<std::path::PathBuf> {
        self.directory.read_link_contents(path)
    }

    pub fn set_permissions(&self, path: &Path, permissions: Permissions) -> io::Result<()> {
        self.directory.set_permissions(
            if path.as_os_str().is_empty() {
                Path::new(".")
            } else {
                path
            },
            permissions,
        )
    }

    /// Pins one ordinary macOS inode before an offloaded metadata mutation.
    #[cfg(target_os = "macos")]
    #[cfg(any(feature = "native-mount", test))]
    pub(crate) fn open_macos_metadata_target(
        &self,
        path: &Path,
    ) -> Result<MacMetadataTarget, MacMetadataError> {
        open_macos_metadata_target(&self.directory, path)
    }

    /// Binds metadata restoration to the current inode before deferred I/O.
    /// A later rename or path replacement cannot redirect the mutation.
    #[cfg(target_os = "linux")]
    #[cfg(any(feature = "native-mount", test))]
    pub(crate) fn open_linux_metadata_target(
        &self,
        path: &Path,
    ) -> Result<LinuxMetadataTarget, LinuxMetadataError> {
        LinuxMetadataTarget::open(&self.directory, path).map_err(Into::into)
    }

    /// Pins the exact Windows leaf before metadata restoration is deferred.
    #[cfg(windows)]
    #[cfg(any(feature = "native-mount", test))]
    pub(crate) fn open_windows_metadata_target(
        &self,
        path: &Path,
    ) -> Result<WindowsMetadataTarget, WindowsMetadataError> {
        WindowsMetadataTarget::open(&self.directory, path).map_err(Into::into)
    }

    #[cfg(unix)]
    pub fn symlink(&self, target: &OsStr, destination: &Path) -> io::Result<()> {
        self.directory.symlink_contents(target, destination)
    }

    #[cfg(windows)]
    pub fn symlink_file(&self, target: &Path, destination: &Path) -> io::Result<()> {
        self.directory.symlink_file(target, destination)
    }

    /// Applies a permission mask without opening the target node.
    ///
    /// Special files must never be chmodded through an open file
    /// description: opening a FIFO blocks until a peer appears.
    #[cfg(unix)]
    #[allow(unsafe_code)]
    pub fn set_permissions_without_open(&self, path: &Path, mode: u32) -> io::Result<()> {
        use std::os::fd::AsRawFd;
        let (parent, destination) = held_parent_leaf(&self.directory, path)?;
        let mask = libc::mode_t::try_from(mode & 0o7777)
            .map_err(|_| io::Error::new(io::ErrorKind::InvalidInput, "mode exceeds mode_t"))?;
        // SAFETY: the held parent and NUL-terminated leaf remain live. No
        // intermediate or final symlink may redirect the chmod outside it.
        let result = unsafe {
            libc::fchmodat(
                parent.as_raw_fd(),
                destination.as_ptr(),
                mask,
                libc::AT_SYMLINK_NOFOLLOW,
            )
        };
        if result == 0 {
            Ok(())
        } else {
            Err(io::Error::last_os_error())
        }
    }

    #[cfg(unix)]
    #[allow(unsafe_code)]
    #[cfg(any(feature = "native-mount", test))]
    pub(crate) fn create_fifo_held(&self, path: &Path, mode: u32) -> io::Result<()> {
        use std::os::fd::AsRawFd as _;

        let (parent, leaf) = held_parent_leaf(&self.directory, path)?;
        let mask = libc::mode_t::try_from(mode & 0o7777)
            .map_err(|_| io::Error::new(io::ErrorKind::InvalidInput, "mode exceeds mode_t"))?;
        // SAFETY: the held parent and leaf remain live for the syscall.
        if unsafe { libc::mkfifoat(parent.as_raw_fd(), leaf.as_ptr(), mask) } == 0 {
            Ok(())
        } else {
            Err(io::Error::last_os_error())
        }
    }

    #[cfg(unix)]
    #[allow(unsafe_code)]
    #[allow(
        clippy::useless_conversion,
        reason = "libc file-type constants differ in width between Linux and macOS"
    )]
    #[cfg(any(feature = "native-mount", all(test, target_os = "macos")))]
    pub(crate) fn create_device_held(
        &self,
        path: &Path,
        mode: u32,
        device: libc::dev_t,
    ) -> io::Result<()> {
        use std::os::fd::AsRawFd as _;

        let kind = mode & u32::from(libc::S_IFMT);
        if kind != u32::from(libc::S_IFCHR) && kind != u32::from(libc::S_IFBLK) {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "not a device mode",
            ));
        }
        let (parent, leaf) = held_parent_leaf(&self.directory, path)?;
        let mask = libc::mode_t::try_from(mode)
            .map_err(|_| io::Error::new(io::ErrorKind::InvalidInput, "mode exceeds mode_t"))?;
        // SAFETY: the held parent and leaf remain live for the syscall.
        if unsafe { libc::mknodat(parent.as_raw_fd(), leaf.as_ptr(), mask, device) } == 0 {
            Ok(())
        } else {
            Err(io::Error::last_os_error())
        }
    }

    #[cfg(unix)]
    pub fn bind_unix_socket(&self, destination: &Path) -> io::Result<()> {
        use std::os::unix::ffi::OsStrExt as _;
        if destination.as_os_str().is_empty() {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "socket path has no file name",
            ));
        }
        let (parent, leaf) = held_parent_leaf(&self.directory, destination)?;
        bind_unix_socket_in(&parent, std::ffi::OsStr::from_bytes(leaf.as_bytes()))
    }
}

#[cfg(target_os = "macos")]
#[cfg(any(feature = "native-mount", test))]
fn open_macos_metadata_target(
    root: &Dir,
    path: &Path,
) -> Result<MacMetadataTarget, MacMetadataError> {
    use cap_std::fs::{FileTypeExt as _, MetadataExt as _, OpenOptionsExt as _};
    use std::os::unix::fs::FileTypeExt as _;
    use std::os::unix::fs::MetadataExt as _;

    let mut parent = root.try_clone()?;
    let mut leaf = Path::new(".");
    let mut components = path.components().peekable();
    while let Some(component) = components.next() {
        let std::path::Component::Normal(name) = component else {
            return Err(
                io::Error::new(io::ErrorKind::InvalidInput, "invalid metadata path").into(),
            );
        };
        if components.peek().is_some() {
            parent = parent.open_dir_nofollow(name)?;
        } else {
            leaf = Path::new(name);
        }
    }
    let before = parent.symlink_metadata(leaf)?;
    let fifo = before.file_type().is_fifo();
    if !before.is_file() && !before.is_dir() && !fifo {
        // No descriptor can be safely opened for a socket or similar node.
        // The target remains admissible only for metadata requiring no write.
        return Ok(MacMetadataTarget::Noop {
            parent,
            leaf: leaf.to_path_buf(),
            device: before.dev(),
            inode: before.ino(),
        });
    }
    let mut options = OpenOptions::new();
    options
        .read(true)
        .custom_flags(libc::O_NOFOLLOW | libc::O_NONBLOCK | if fifo { libc::O_EVTONLY } else { 0 });
    let file = parent.open_with(leaf, &options)?.into_std();
    let after = file.metadata()?;
    if before.dev() != after.dev() || before.ino() != after.ino() {
        return Err(io::Error::other("metadata target changed during admission").into());
    }
    if !after.is_file() && !after.is_dir() && !after.file_type().is_fifo() {
        return Err(MacMetadataError::Unsupported("non-ordinary file kind"));
    }
    Ok(MacMetadataTarget::Held(file))
}

#[cfg(target_os = "macos")]
#[allow(unsafe_code)]
#[cfg(any(feature = "native-mount", test))]
impl MacMetadataTarget {
    /// Applies representable metadata to the held inode and reads it back.
    /// Canonical ctime requires a durable native-view baseline first.
    pub(crate) fn apply(
        self,
        metadata: crate::kernel::FileMetadata,
    ) -> Result<(), MacMetadataError> {
        use crate::kernel::MetadataField;
        use std::os::fd::AsRawFd as _;

        validate_macos_metadata_fields(metadata)?;
        let file = match self {
            Self::Held(file) => file,
            Self::Noop {
                parent,
                leaf,
                device,
                inode,
            } => {
                use cap_std::fs::MetadataExt as _;
                if metadata != crate::kernel::FileMetadata::default() {
                    return Err(MacMetadataError::Unsupported("non-ordinary file kind"));
                }
                let observed = parent.symlink_metadata(&leaf)?;
                if observed.dev() != device || observed.ino() != inode {
                    return Err(io::Error::other("metadata target changed during no-op").into());
                }
                return Ok(());
            }
        };
        let fd = file.as_raw_fd();
        let current = macos_fstat(fd)?;
        if let MetadataField::Value(mode) = metadata.posix_mode
            && mode & u32::from(libc::S_IFMT) != 0
            && mode & u32::from(libc::S_IFMT) != u32::from(current.st_mode & libc::S_IFMT)
        {
            return Err(MacMetadataError::Unsupported("posix_mode file kind"));
        }
        let uid = match metadata.posix_uid {
            MetadataField::Value(value) => value,
            MetadataField::Unavailable => current.st_uid,
        };
        let gid = match metadata.posix_gid {
            MetadataField::Value(value) => value,
            MetadataField::Unavailable => current.st_gid,
        };
        if uid != current.st_uid || gid != current.st_gid {
            // SAFETY: fd pins the admitted inode; no later path lookup occurs.
            if unsafe { libc::fchown(fd, uid, gid) } != 0 {
                return Err(io::Error::last_os_error().into());
            }
        }
        if let MetadataField::Value(mode) = metadata.posix_mode {
            // Ownership changes may clear setuid/setgid, so mode is applied last.
            let mask = libc::mode_t::try_from(mode & 0o7777)
                .map_err(|_| MacMetadataError::Unsupported("posix_mode"))?;
            if unsafe { libc::fchmod(fd, mask) } != 0 {
                return Err(io::Error::last_os_error().into());
            }
        }
        apply_macos_timestamp_metadata(fd, metadata)?;
        let observed = macos_fstat(fd)?;
        if macos_mismatch(metadata.posix_uid, observed.st_uid)
            || macos_mismatch(metadata.posix_gid, observed.st_gid)
            || matches!(metadata.posix_mode, MetadataField::Value(value) if u32::from(observed.st_mode) & 0o7777 != value & 0o7777)
            || matches!(metadata.posix_flags, MetadataField::Value(0) if observed.st_flags != 0)
            || matches!(metadata.created_ns, MetadataField::Value(value) if macos_nanos(observed.st_birthtime, observed.st_birthtime_nsec) != Some(value))
            || matches!(metadata.accessed_ns, MetadataField::Value(value) if macos_nanos(observed.st_atime, observed.st_atime_nsec) != Some(value))
            || matches!(metadata.modified_ns, MetadataField::Value(value) if macos_nanos(observed.st_mtime, observed.st_mtime_nsec) != Some(value))
        {
            return Err(MacMetadataError::Unsupported("metadata readback mismatch"));
        }
        Ok(())
    }
}

#[cfg(target_os = "macos")]
#[cfg(any(feature = "native-mount", test))]
fn validate_macos_metadata_fields(
    metadata: crate::kernel::FileMetadata,
) -> Result<(), MacMetadataError> {
    use crate::kernel::MetadataField;

    for (present, field) in [
        (
            matches!(metadata.posix_flags, MetadataField::Value(value) if value != 0),
            "posix_flags",
        ),
        (
            matches!(metadata.windows_attributes, MetadataField::Value(_)),
            "windows_attributes",
        ),
        (
            matches!(metadata.named_attributes, MetadataField::Value(_)),
            "named_attributes",
        ),
        (matches!(metadata.acl, MetadataField::Value(_)), "acl"),
        (
            matches!(metadata.security_descriptor, MetadataField::Value(_)),
            "security_descriptor",
        ),
        (
            matches!(metadata.changed_ns, MetadataField::Value(_)),
            "changed_ns without native-view baseline",
        ),
    ] {
        if present {
            return Err(MacMetadataError::Unsupported(field));
        }
    }
    Ok(())
}

#[cfg(target_os = "macos")]
#[allow(unsafe_code)]
#[cfg(any(feature = "native-mount", test))]
fn apply_macos_timestamp_metadata(
    fd: libc::c_int,
    metadata: crate::kernel::FileMetadata,
) -> Result<(), MacMetadataError> {
    use crate::kernel::MetadataField;

    if let MetadataField::Value(value) = metadata.created_ns {
        let mut attributes = libc::attrlist {
            bitmapcount: libc::ATTR_BIT_MAP_COUNT,
            reserved: 0,
            commonattr: libc::ATTR_CMN_CRTIME,
            volattr: 0,
            dirattr: 0,
            fileattr: 0,
            forkattr: 0,
        };
        let mut created = macos_timespec(value);
        // SAFETY: both buffers remain live for the synchronous system call.
        if unsafe {
            libc::fsetattrlist(
                fd,
                (&raw mut attributes).cast(),
                (&raw mut created).cast(),
                std::mem::size_of::<libc::timespec>(),
                0,
            )
        } != 0
        {
            return Err(io::Error::last_os_error().into());
        }
    }
    if matches!(metadata.accessed_ns, MetadataField::Value(_))
        || matches!(metadata.modified_ns, MetadataField::Value(_))
    {
        let omitted = libc::timespec {
            tv_sec: 0,
            tv_nsec: libc::UTIME_OMIT,
        };
        let times = [
            match metadata.accessed_ns {
                MetadataField::Value(value) => macos_timespec(value),
                MetadataField::Unavailable => omitted,
            },
            match metadata.modified_ns {
                MetadataField::Value(value) => macos_timespec(value),
                MetadataField::Unavailable => omitted,
            },
        ];
        // SAFETY: fd pins the inode and times is a live two-element array.
        if unsafe { libc::futimens(fd, times.as_ptr()) } != 0 {
            return Err(io::Error::last_os_error().into());
        }
    }
    Ok(())
}

#[cfg(target_os = "macos")]
#[cfg(any(feature = "native-mount", test))]
fn macos_mismatch<T: Copy + Eq>(field: crate::kernel::MetadataField<T>, observed: T) -> bool {
    matches!(field, crate::kernel::MetadataField::Value(expected) if expected != observed)
}

#[cfg(target_os = "macos")]
#[cfg(any(feature = "native-mount", test))]
fn macos_timespec(value: i64) -> libc::timespec {
    libc::timespec {
        tv_sec: value.div_euclid(1_000_000_000),
        tv_nsec: value.rem_euclid(1_000_000_000),
    }
}

#[cfg(target_os = "macos")]
#[cfg(any(feature = "native-mount", test))]
fn macos_nanos(seconds: libc::time_t, nanoseconds: libc::c_long) -> Option<i64> {
    let nanos = i128::from(seconds) * 1_000_000_000 + i128::from(nanoseconds);
    i64::try_from(nanos).ok()
}

#[cfg(target_os = "macos")]
#[allow(unsafe_code)]
#[cfg(any(feature = "native-mount", test))]
fn macos_fstat(fd: std::os::fd::RawFd) -> io::Result<libc::stat> {
    let mut observed = std::mem::MaybeUninit::<libc::stat>::uninit();
    // SAFETY: fstat initializes the whole result on success.
    if unsafe { libc::fstat(fd, observed.as_mut_ptr()) } != 0 {
        return Err(io::Error::last_os_error());
    }
    Ok(unsafe { observed.assume_init() })
}

#[cfg(windows)]
#[cfg(any(feature = "native-mount", test))]
impl WindowsMetadataTarget {
    fn open(directory: &Dir, path: &Path) -> io::Result<Self> {
        use cap_std::fs::OpenOptionsExt as _;
        use windows::Win32::Storage::FileSystem::{
            FILE_FLAG_BACKUP_SEMANTICS, FILE_FLAG_OPEN_REPARSE_POINT, FILE_SHARE_DELETE,
            FILE_SHARE_READ, FILE_SHARE_WRITE,
        };

        const FILE_READ_ATTRIBUTES: u32 = 0x80;
        const FILE_WRITE_ATTRIBUTES: u32 = 0x100;
        let mut options = OpenOptions::new();
        options
            .access_mode(FILE_READ_ATTRIBUTES | FILE_WRITE_ATTRIBUTES)
            .share_mode(FILE_SHARE_READ.0 | FILE_SHARE_WRITE.0 | FILE_SHARE_DELETE.0)
            .custom_flags(FILE_FLAG_BACKUP_SEMANTICS.0 | FILE_FLAG_OPEN_REPARSE_POINT.0)
            ._cap_fs_ext_follow(cap_primitives::fs::FollowSymlinks::No);
        let file = open_windows_metadata_file(directory, path, &options)?;
        Ok(Self { file })
    }

    /// Applies representable metadata and verifies it through the pinned leaf.
    /// Fields which this provider cannot reproduce fail closed.
    pub(crate) fn apply(
        self,
        metadata: crate::kernel::FileMetadata,
    ) -> Result<(), WindowsMetadataError> {
        reject_unsupported_windows_metadata(metadata)?;
        if metadata == crate::kernel::FileMetadata::default() {
            return Ok(());
        }
        let current = query_windows_basic_info(&self.file)?;
        let desired = desired_windows_basic_info(metadata, current)?;
        set_windows_basic_info(&self.file, desired)?;
        verify_windows_basic_info(metadata, desired, query_windows_basic_info(&self.file)?)
    }
}

#[cfg(windows)]
#[cfg(any(feature = "native-mount", test))]
fn reject_unsupported_windows_metadata(
    metadata: crate::kernel::FileMetadata,
) -> Result<(), WindowsMetadataError> {
    use crate::kernel::MetadataField;
    for (field, present) in [
        (
            "posix mode",
            !matches!(metadata.posix_mode, MetadataField::Unavailable),
        ),
        (
            "POSIX owner",
            !matches!(metadata.posix_uid, MetadataField::Unavailable),
        ),
        (
            "POSIX group",
            !matches!(metadata.posix_gid, MetadataField::Unavailable),
        ),
        (
            "POSIX flags",
            !matches!(metadata.posix_flags, MetadataField::Unavailable),
        ),
        (
            "named attributes",
            !matches!(metadata.named_attributes, MetadataField::Unavailable),
        ),
        ("ACL", !matches!(metadata.acl, MetadataField::Unavailable)),
        (
            "security descriptor",
            !matches!(metadata.security_descriptor, MetadataField::Unavailable),
        ),
    ] {
        if present {
            return Err(WindowsMetadataError::Unsupported(field));
        }
    }
    Ok(())
}

#[cfg(windows)]
#[cfg(any(feature = "native-mount", test))]
fn desired_windows_basic_info(
    metadata: crate::kernel::FileMetadata,
    current: FILE_BASIC_INFO,
) -> Result<FILE_BASIC_INFO, WindowsMetadataError> {
    use crate::kernel::MetadataField;
    const SETTABLE_ATTRIBUTES: u32 = 0x0000_0001 // readonly
        | 0x0000_0002 // hidden
        | 0x0000_0004 // system
        | 0x0000_0020 // archive
        | 0x0000_0080 // normal
        | 0x0000_0100 // temporary
        | 0x0000_2000; // not content indexed
    const STRUCTURAL_ATTRIBUTES: u32 = 0x0000_0010 // directory
        | 0x0000_0040 // device
        | 0x0000_0200 // sparse
        | 0x0000_0400 // reparse point
        | 0x0000_0800 // compressed
        | 0x0000_1000 // offline
        | 0x0000_4000 // encrypted
        | 0x0000_8000 // integrity stream
        | 0x0001_0000 // virtual
        | 0x0002_0000 // no scrub data
        | 0x0004_0000 // EA / recall on open
        | 0x0008_0000 // pinned
        | 0x0010_0000 // unpinned
        | 0x0040_0000; // recall on data access
    const KNOWN_ATTRIBUTES: u32 = SETTABLE_ATTRIBUTES | STRUCTURAL_ATTRIBUTES;
    let attributes = match metadata.windows_attributes {
        MetadataField::Unavailable => current.FileAttributes,
        MetadataField::Value(attributes) => {
            if attributes == 0
                || attributes & !KNOWN_ATTRIBUTES != 0
                || attributes & 0x80 != 0 && attributes != 0x80
                || attributes & STRUCTURAL_ATTRIBUTES
                    != current.FileAttributes & STRUCTURAL_ATTRIBUTES
            {
                return Err(WindowsMetadataError::Unsupported("Windows attributes"));
            }
            attributes
        }
    };
    Ok(FILE_BASIC_INFO {
        CreationTime: metadata_time(metadata.created_ns, current.CreationTime)?,
        LastAccessTime: metadata_time(metadata.accessed_ns, current.LastAccessTime)?,
        LastWriteTime: metadata_time(metadata.modified_ns, current.LastWriteTime)?,
        ChangeTime: metadata_time(metadata.changed_ns, current.ChangeTime)?,
        FileAttributes: attributes,
    })
}

#[cfg(windows)]
#[cfg(any(feature = "native-mount", test))]
fn verify_windows_basic_info(
    metadata: crate::kernel::FileMetadata,
    desired: FILE_BASIC_INFO,
    observed: FILE_BASIC_INFO,
) -> Result<(), WindowsMetadataError> {
    use crate::kernel::MetadataField;
    for (field, expected, actual, required) in [
        (
            "creation time",
            desired.CreationTime,
            observed.CreationTime,
            !matches!(metadata.created_ns, MetadataField::Unavailable),
        ),
        (
            "access time",
            desired.LastAccessTime,
            observed.LastAccessTime,
            !matches!(metadata.accessed_ns, MetadataField::Unavailable),
        ),
        (
            "modification time",
            desired.LastWriteTime,
            observed.LastWriteTime,
            !matches!(metadata.modified_ns, MetadataField::Unavailable),
        ),
        (
            "change time",
            desired.ChangeTime,
            observed.ChangeTime,
            !matches!(metadata.changed_ns, MetadataField::Unavailable),
        ),
    ] {
        if required && expected != actual {
            return Err(WindowsMetadataError::Unsupported(field));
        }
    }
    if matches!(metadata.windows_attributes, MetadataField::Value(_))
        && observed.FileAttributes != desired.FileAttributes
    {
        return Err(WindowsMetadataError::Unsupported("Windows attributes"));
    }
    Ok(())
}

#[cfg(windows)]
#[cfg(any(feature = "native-mount", test))]
#[allow(unsafe_code)]
fn query_windows_basic_info(file: &cap_std::fs::File) -> io::Result<FILE_BASIC_INFO> {
    use std::mem::size_of;
    use std::os::windows::io::AsRawHandle as _;
    use windows::Win32::Foundation::HANDLE;
    use windows::Win32::Storage::FileSystem::{FileBasicInfo, GetFileInformationByHandleEx};

    let mut information = FILE_BASIC_INFO::default();
    // SAFETY: the capability-opened handle remains live and `information` is
    // one correctly sized writable output for this synchronous query.
    unsafe {
        GetFileInformationByHandleEx(
            HANDLE(file.as_raw_handle()),
            FileBasicInfo,
            (&raw mut information).cast(),
            u32::try_from(size_of::<FILE_BASIC_INFO>())
                .map_err(|_| io::Error::other("FILE_BASIC_INFO size overflow"))?,
        )
        .map_err(io::Error::other)?;
    }
    Ok(information)
}

#[cfg(windows)]
#[cfg(any(feature = "native-mount", test))]
#[allow(unsafe_code)]
fn set_windows_basic_info(
    file: &cap_std::fs::File,
    information: FILE_BASIC_INFO,
) -> io::Result<()> {
    use std::mem::size_of;
    use std::os::windows::io::AsRawHandle as _;
    use windows::Win32::Foundation::HANDLE;
    use windows::Win32::Storage::FileSystem::{FileBasicInfo, SetFileInformationByHandle};

    // SAFETY: the held handle remains live and `information` is one complete,
    // correctly sized input for this synchronous update.
    unsafe {
        SetFileInformationByHandle(
            HANDLE(file.as_raw_handle()),
            FileBasicInfo,
            (&raw const information).cast(),
            u32::try_from(size_of::<FILE_BASIC_INFO>())
                .map_err(|_| io::Error::other("FILE_BASIC_INFO size overflow"))?,
        )
        .map_err(io::Error::other)
    }
}

#[cfg(windows)]
fn open_windows_metadata_file(
    root: &Dir,
    path: &Path,
    options: &OpenOptions,
) -> io::Result<cap_std::fs::File> {
    if path.as_os_str().is_empty() {
        return root.open_with(Path::new("."), options);
    }
    let (parent, name) = open_windows_parent(root, path)?;
    parent.open_with(Path::new(name), options)
}

/// Opens the directory holding `path`'s leaf without following any
/// intermediate link, and returns it with that leaf name.
#[cfg(windows)]
fn open_windows_parent<'a>(root: &Dir, path: &'a Path) -> io::Result<(Dir, &'a OsStr)> {
    let mut parent = root.try_clone()?;
    let mut components = path.components().peekable();
    while let Some(component) = components.next() {
        let std::path::Component::Normal(name) = component else {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "invalid metadata path",
            ));
        };
        if components.peek().is_none() {
            return Ok((parent, name));
        }
        parent = parent.open_dir_nofollow(name)?;
    }
    Err(io::Error::new(
        io::ErrorKind::InvalidInput,
        "metadata path has no leaf",
    ))
}

/// Gives a completed file its final name within one directory, failing
/// rather than replacing an existing entry. The name changes atomically, so
/// no reader of `name` can observe the file before it is complete, and the
/// target is named relative to the held directory, never re-resolved by path.
#[cfg(windows)]
#[allow(unsafe_code)]
fn publish_windows_file(parent: &Dir, staged: &Path, name: &OsStr) -> io::Result<()> {
    use cap_std::fs::OpenOptionsExt as _;
    use std::mem::{offset_of, size_of};
    use std::os::windows::ffi::OsStrExt as _;
    use std::os::windows::io::{AsHandle as _, AsRawHandle as _};
    use windows::Wdk::Storage::FileSystem::{
        FILE_RENAME_INFORMATION, FileRenameInformation, NtSetInformationFile,
    };
    use windows::Win32::Foundation::{HANDLE, RtlNtStatusToDosError};
    use windows::Win32::Storage::FileSystem::{DELETE, FILE_FLAG_OPEN_REPARSE_POINT};
    use windows::Win32::System::IO::IO_STATUS_BLOCK;

    let mut options = OpenOptions::new();
    options
        .access_mode(DELETE.0)
        .custom_flags(FILE_FLAG_OPEN_REPARSE_POINT.0);
    let staged = parent.open_with(staged, &options)?;
    let name = name.encode_wide().collect::<Vec<_>>();
    let overflow = || io::Error::other("rename information overflow");
    let name_bytes = name
        .len()
        .checked_mul(size_of::<u16>())
        .ok_or_else(overflow)?;
    let name_offset = offset_of!(FILE_RENAME_INFORMATION, FileName);
    let total = name_offset
        .checked_add(name_bytes)
        .ok_or_else(overflow)?
        .max(size_of::<FILE_RENAME_INFORMATION>());
    // u64 storage satisfies FILE_RENAME_INFORMATION's alignment.
    let mut storage = vec![0_u64; total.div_ceil(size_of::<u64>())];
    let information = storage.as_mut_ptr().cast::<FILE_RENAME_INFORMATION>();
    // SAFETY: `storage` spans `total` bytes, aligned for the structure, with
    // `name_bytes` after the name offset; nothing else aliases it.
    unsafe {
        (*information).Anonymous.ReplaceIfExists = false;
        (*information).RootDirectory = HANDLE(parent.as_handle().as_raw_handle());
        (*information).FileNameLength = u32::try_from(name_bytes).map_err(|_| overflow())?;
        std::ptr::copy_nonoverlapping(
            name.as_ptr(),
            information.cast::<u8>().add(name_offset).cast::<u16>(),
            name.len(),
        );
    }
    let mut status_block = IO_STATUS_BLOCK::default();
    // SAFETY: both handles, the status block, and the initialized information
    // buffer outlive this synchronous call; the length is exactly the buffer's.
    let status = unsafe {
        NtSetInformationFile(
            HANDLE(staged.as_handle().as_raw_handle()),
            &raw mut status_block,
            information.cast(),
            u32::try_from(total).map_err(|_| overflow())?,
            FileRenameInformation,
        )
    };
    if status.is_ok() {
        return Ok(());
    }
    // SAFETY: a pure status-code translation.
    let code = unsafe { RtlNtStatusToDosError(status) };
    Err(io::Error::from_raw_os_error(
        i32::try_from(code).map_err(|_| io::Error::other("unmapped rename status"))?,
    ))
}

#[cfg(windows)]
fn open_windows_regular_source(root: &Dir, path: &Path, overlapped: bool) -> io::Result<File> {
    use cap_std::fs::OpenOptionsExt as _;
    use windows::Win32::Storage::FileSystem::{FILE_FLAG_OPEN_REPARSE_POINT, FILE_FLAG_OVERLAPPED};

    let mut options = OpenOptions::new();
    options.read(true).custom_flags(
        FILE_FLAG_OPEN_REPARSE_POINT.0
            | if overlapped {
                FILE_FLAG_OVERLAPPED.0
            } else {
                0
            },
    );
    let file = open_windows_metadata_file(root, path, &options)?.into_std();
    let metadata = file.metadata()?;
    if !metadata.is_file() || root_is_reparse_point(&metadata) {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "copy source must be a real regular file",
        ));
    }
    Ok(file)
}

#[cfg(windows)]
fn create_windows_copy_target(root: &Dir, path: &Path, overlapped: bool) -> io::Result<File> {
    use cap_std::fs::OpenOptionsExt as _;
    use windows::Win32::Storage::FileSystem::FILE_FLAG_OVERLAPPED;

    let mut options = OpenOptions::new();
    options.write(true).create_new(true);
    if overlapped {
        options.custom_flags(FILE_FLAG_OVERLAPPED.0);
    }
    open_windows_metadata_file(root, path, &options).map(cap_std::fs::File::into_std)
}

/// One private name no other writer uses, for bytes not yet published.
#[cfg(windows)]
fn staging_name() -> std::path::PathBuf {
    std::path::PathBuf::from(format!("{STAGING_PREFIX}{}", uuid::Uuid::new_v4().simple()))
}

#[cfg(windows)]
const STAGING_PREFIX: &str = ".acyclic-copy-";

#[cfg(windows)]
async fn copy_windows_file_worker(
    source_root: &HostRoot,
    source_path: &Path,
    destination_root: &HostRoot,
    destination_path: &Path,
    receiver: &tokio::sync::oneshot::Sender<io::Result<()>>,
    created: &mut bool,
) -> io::Result<()> {
    use acyclic_native_runtime::{NativeFile, OwnedRead, OwnedWrite};

    const CHUNK: usize = 1024 * 1024;
    let source_directory = source_root.directory.try_clone()?;
    let source_path_owned = source_path.to_path_buf();
    let (source, identity, length) = acyclic_native_runtime::run_blocking_io(move || {
        let source = open_windows_regular_source(&source_directory, &source_path_owned, true)?;
        let identity = crate::NativeRootIdentity::from_file(&source)?;
        let length = source.metadata()?.len();
        Ok::<_, io::Error>((source, identity, length))
    })
    .await??;
    if receiver.is_closed() {
        return Err(io::Error::new(io::ErrorKind::Interrupted, "copy cancelled"));
    }
    let clone_source = HostRoot {
        directory: source_root.directory.try_clone()?,
        identity: source_root.identity,
    };
    let clone_destination = HostRoot {
        directory: destination_root.directory.try_clone()?,
        identity: destination_root.identity,
    };
    let clone_source_path = source_path.to_path_buf();
    let clone_destination_path = destination_path.to_path_buf();
    if acyclic_native_runtime::run_blocking_io(move || {
        clone_destination.clone_file_from_identity(
            &clone_source,
            &clone_source_path,
            &clone_destination_path,
            Some(identity),
        )
    })
    .await??
    {
        *created = true;
        return Ok(());
    }
    if receiver.is_closed() {
        return Err(io::Error::new(io::ErrorKind::Interrupted, "copy cancelled"));
    }
    let destination_directory = destination_root.directory.try_clone()?;
    let destination_path_owned = destination_path.to_path_buf();
    let destination = acyclic_native_runtime::run_blocking_io(move || {
        create_windows_copy_target(&destination_directory, &destination_path_owned, true)
    })
    .await??;
    *created = true;
    // SAFETY: both handles were opened with FILE_FLAG_OVERLAPPED and are
    // transferred once; no independent I/O is performed after this point.
    #[allow(unsafe_code)]
    let source = unsafe { NativeFile::from_overlapped_file_unchecked(source)? };
    #[allow(unsafe_code)]
    let destination = unsafe { NativeFile::from_overlapped_file_unchecked(destination)? };
    let mut offset = 0_u64;
    while offset < length {
        if receiver.is_closed() {
            return Err(io::Error::new(io::ErrorKind::Interrupted, "copy cancelled"));
        }
        let requested = usize::try_from((length - offset).min(CHUNK as u64))
            .map_err(|_| io::Error::other("copy chunk overflow"))?;
        let mut result = source
            .read_batch_async(vec![OwnedRead {
                offset,
                length: requested,
            }])
            .await?;
        let bytes = result
            .pop()
            .ok_or_else(|| io::Error::other("copy read missing"))?;
        if bytes.is_empty() {
            return Err(io::Error::new(
                io::ErrorKind::UnexpectedEof,
                "copy source became shorter",
            ));
        }
        let copied = bytes.len() as u64;
        destination
            .write_all_batch_async(vec![OwnedWrite { offset, bytes }])
            .await?;
        offset += copied;
    }
    Ok(())
}

#[cfg(windows)]
#[cfg(any(feature = "native-mount", test))]
fn metadata_time(
    field: crate::kernel::MetadataField<i64>,
    current: i64,
) -> Result<i64, WindowsMetadataError> {
    const WINDOWS_EPOCH_TICKS: i64 = 116_444_736_000_000_000;
    let crate::kernel::MetadataField::Value(nanoseconds) = field else {
        return Ok(current);
    };
    if nanoseconds % 100 != 0 {
        return Err(WindowsMetadataError::Unsupported("sub-100ns timestamp"));
    }
    nanoseconds
        .checked_div(100)
        .and_then(|ticks| ticks.checked_add(WINDOWS_EPOCH_TICKS))
        .ok_or(WindowsMetadataError::Unsupported("timestamp range"))
}

impl HostDirectory {
    pub fn symlink_metadata(&self, name: &Path) -> io::Result<Metadata> {
        self.directory.symlink_metadata(name)
    }

    pub fn rename_to(
        &self,
        name: &Path,
        destination: &Self,
        destination_name: &Path,
    ) -> io::Result<()> {
        self.directory
            .rename(name, &destination.directory, destination_name)
    }

    /// Creates one child directory and retains a capability for it.
    pub fn create_dir_held(&self, name: &Path) -> io::Result<Self> {
        self.directory.create_dir(name)?;
        Ok(Self {
            directory: self.directory.open_dir_nofollow(name)?,
        })
    }

    /// Removes one empty child directory.
    pub fn remove_dir(&self, name: &Path) -> io::Result<()> {
        self.directory.remove_dir(name)
    }

    /// Releases the held directory handle before callers remove its subtree.
    pub fn close(self) {
        drop(self);
    }

    pub fn remove(&self, name: &Path) -> io::Result<()> {
        let metadata = match self.directory.symlink_metadata(name) {
            Ok(metadata) => metadata,
            Err(error) if error.kind() == io::ErrorKind::NotFound => return Ok(()),
            Err(error) => return Err(error),
        };
        if metadata.is_dir() && !metadata.file_type().is_symlink() {
            self.directory.remove_dir_all(name)
        } else {
            self.directory.remove_file(name)
        }
    }
}

/// Binds a Unix socket named `name` inside the held directory, race-free
/// against concurrent renames of that directory.
#[cfg(target_os = "linux")]
fn bind_unix_socket_in(parent: &Dir, name: &OsStr) -> io::Result<()> {
    use std::os::fd::AsRawFd;

    let mut held_path = std::path::PathBuf::from(format!("/proc/self/fd/{}", parent.as_raw_fd()));
    held_path.push(name);
    std::os::unix::net::UnixListener::bind(held_path).map(|_| ())
}

/// Binds a Unix socket named `name` inside the held directory.
///
/// Darwin has no `/proc/self/fd` and its `/dev/fd/N` entries cannot be
/// traversed with a child component, so the held descriptor is resolved to a
/// path with `F_GETPATH` and the bind goes through that path. The window
/// between resolution and bind is closed by re-checking afterwards that the
/// bound node is the one visible through the held descriptor; on mismatch the
/// stray node is removed and the bind reports a race.
#[cfg(target_os = "macos")]
#[allow(unsafe_code)]
fn bind_unix_socket_in(parent: &Dir, name: &OsStr) -> io::Result<()> {
    use cap_std::fs::MetadataExt;
    use std::os::fd::AsRawFd;
    use std::os::unix::ffi::OsStringExt;
    use std::os::unix::fs::MetadataExt as HostMetadataExt;

    let mut resolved = [0_u8; libc::PATH_MAX as usize];
    // SAFETY: F_GETPATH writes a NUL-terminated path of at most PATH_MAX
    // bytes into the provided buffer for a live descriptor.
    if unsafe {
        libc::fcntl(
            parent.as_raw_fd(),
            libc::F_GETPATH,
            resolved.as_mut_ptr().cast::<libc::c_char>(),
        )
    } != 0
    {
        return Err(io::Error::last_os_error());
    }
    let terminator = resolved
        .iter()
        .position(|byte| *byte == 0)
        .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidData, "unterminated F_GETPATH"))?;
    #[allow(
        clippy::indexing_slicing,
        reason = "terminator is the position() of a byte within resolved, so terminator < resolved.len() always"
    )]
    let mut held_path = std::path::PathBuf::from(std::ffi::OsString::from_vec(
        resolved[..terminator].to_vec(),
    ));
    held_path.push(name);
    let sun_path_capacity =
        size_of::<libc::sockaddr_un>() - std::mem::offset_of!(libc::sockaddr_un, sun_path);
    if held_path.as_os_str().len() >= sun_path_capacity {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "socket path exceeds the host sun_path capacity",
        ));
    }
    std::os::unix::net::UnixListener::bind(&held_path)?;
    let through_descriptor = parent.symlink_metadata(name)?;
    let through_path = std::fs::symlink_metadata(&held_path)?;
    if through_descriptor.dev() != HostMetadataExt::dev(&through_path)
        || through_descriptor.ino() != HostMetadataExt::ino(&through_path)
    {
        let _ = std::fs::remove_file(&held_path);
        return Err(io::Error::other(
            "socket parent directory moved during bind",
        ));
    }
    Ok(())
}

/// Network, clustered, and user-space filesystems, by `statfs(2)` magic.
#[cfg(target_os = "linux")]
const REMOTE_FILESYSTEMS: [u64; 12] = [
    0x6969,      // NFS
    0x517b,      // SMB
    0xff53_4d42, // CIFS
    0xfe53_4d42, // SMB2
    0x5346_414f, // AFS
    0x00c3_6400, // Ceph
    0x6573_5546, // FUSE
    0x0102_1997, // 9P
    0x564c,      // NCP
    0x7375_7245, // Coda
    0x0bd0_0bd0, // Lustre
    0x4750_4653, // GPFS
];

#[cfg(target_os = "linux")]
#[allow(unsafe_code)]
fn filesystem_is_local(directory: &Dir) -> bool {
    use std::os::fd::AsRawFd as _;
    let mut stats = std::mem::MaybeUninit::<libc::statfs>::uninit();
    // SAFETY: `fstatfs` fills the provided out-struct for a live descriptor.
    if unsafe { libc::fstatfs(directory.as_raw_fd(), stats.as_mut_ptr()) } != 0 {
        return false;
    }
    // SAFETY: `fstatfs` succeeded and initialized the struct.
    let kind = unsafe { stats.assume_init() }.f_type;
    // libc models `f_type` as signed for glibc and unsigned for musl.
    u64::try_from(kind).is_ok_and(|kind| !REMOTE_FILESYSTEMS.contains(&kind))
}

#[cfg(target_os = "macos")]
#[allow(unsafe_code)]
fn filesystem_is_local(directory: &Dir) -> bool {
    use std::os::fd::AsRawFd as _;
    let mut stats = std::mem::MaybeUninit::<libc::statfs>::uninit();
    // SAFETY: `fstatfs` fills the provided out-struct for a live descriptor.
    if unsafe { libc::fstatfs(directory.as_raw_fd(), stats.as_mut_ptr()) } != 0 {
        return false;
    }
    // SAFETY: `fstatfs` succeeded and initialized the struct.
    let flags = unsafe { stats.assume_init() }.f_flags;
    u32::try_from(libc::MNT_LOCAL).is_ok_and(|local| flags & local != 0)
}

#[cfg(windows)]
#[allow(unsafe_code)]
fn filesystem_is_local(directory: &Dir) -> bool {
    use std::os::windows::io::AsRawHandle as _;
    use windows::Wdk::Storage::FileSystem::{
        FileFsDeviceInformation, NtQueryVolumeInformationFile,
    };
    use windows::Win32::Foundation::HANDLE;
    use windows::Win32::System::IO::IO_STATUS_BLOCK;
    /// The characteristic of a volume on a network redirector.
    const FILE_REMOTE_DEVICE: u32 = 0x10;
    // `FILE_FS_DEVICE_INFORMATION`: the device type, then characteristics.
    let mut device = [0_u32; 2];
    let mut status = IO_STATUS_BLOCK::default();
    // SAFETY: the handle is live for the call, and the buffer is exactly one
    // `FILE_FS_DEVICE_INFORMATION` of the length passed.
    let queried = unsafe {
        NtQueryVolumeInformationFile(
            HANDLE(directory.as_raw_handle()),
            &raw mut status,
            device.as_mut_ptr().cast(),
            8,
            FileFsDeviceInformation,
        )
    };
    let [_, characteristics] = device;
    queried.is_ok() && characteristics & FILE_REMOTE_DEVICE == 0
}

#[cfg(not(any(target_os = "linux", target_os = "macos", windows)))]
fn filesystem_is_local(_directory: &Dir) -> bool {
    false
}

/// Deallocates an already-zero range of a host file.
///
/// APFS materializes the zero tail created by `ftruncate` as soon as any byte
/// of the file is written, so holes must be punched explicitly after the data
/// spans are in place. The range is shrunk inward to filesystem-block
/// alignment. Unsupported or failed deallocation is reported rather than
/// silently materializing a dense file.
#[cfg(target_os = "macos")]
#[allow(unsafe_code)]
pub fn punch_hole(file: &impl std::os::fd::AsRawFd, offset: u64, length: u64) -> io::Result<()> {
    let fd = file.as_raw_fd();
    let mut stats = std::mem::MaybeUninit::<libc::statfs>::uninit();
    // SAFETY: `fstatfs` fills the provided out-struct for a live descriptor.
    if unsafe { libc::fstatfs(fd, stats.as_mut_ptr()) } != 0 {
        return Err(io::Error::last_os_error());
    }
    // SAFETY: `fstatfs` succeeded and initialized the struct.
    let block = u64::from(unsafe { stats.assume_init() }.f_bsize);
    if block == 0 {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "host filesystem reports a zero allocation unit",
        ));
    }
    let Some(start) = offset
        .checked_add(block - 1)
        .map(|edge| edge / block * block)
    else {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "hole start overflow",
        ));
    };
    let Some(end) = offset.checked_add(length).map(|edge| edge / block * block) else {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "hole end overflow",
        ));
    };
    if end <= start {
        return Ok(());
    }
    let (Ok(fp_offset), Ok(fp_length)) = (i64::try_from(start), i64::try_from(end - start)) else {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "hole range exceeds host limits",
        ));
    };
    let arg = libc::fpunchhole_t {
        fp_flags: 0,
        reserved: 0,
        fp_offset,
        fp_length,
    };
    // SAFETY: the argument struct outlives this value-only fcntl call.
    if unsafe { libc::fcntl(fd, libc::F_PUNCHHOLE, &arg) } == 0 {
        Ok(())
    } else {
        Err(io::Error::last_os_error())
    }
}

#[cfg(windows)]
#[allow(unsafe_code)]
fn clone_windows_file(
    source: &mut cap_std::fs::File,
    target: &mut File,
    length: u64,
    alignment: u64,
) -> io::Result<()> {
    use std::io::{Read, Seek, SeekFrom, Write};
    use std::mem::size_of;
    use std::os::windows::io::AsRawHandle;
    use windows::Win32::Foundation::HANDLE;
    use windows::Win32::System::IO::DeviceIoControl;
    use windows::Win32::System::Ioctl::{
        DUPLICATE_EXTENTS_DATA, FSCTL_DUPLICATE_EXTENTS_TO_FILE, FSCTL_SET_SPARSE,
    };

    const CLONE_CHUNK: u64 = 1024 * 1024 * 1024;
    let clone_length = length / alignment * alignment;
    if clone_length == 0 {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "file is smaller than the clone alignment",
        ));
    }
    // The materializer creates sparse source files on Windows. The target
    // must also be sparse for the clone FSCTL to preserve holes.
    unsafe {
        DeviceIoControl(
            HANDLE(target.as_raw_handle()),
            FSCTL_SET_SPARSE,
            None,
            0,
            None,
            0,
            None,
            None,
        )?;
    }
    target.set_len(length)?;
    let chunk_step = usize::try_from(CLONE_CHUNK)
        .map_err(|_| io::Error::other("clone chunk exceeds addressable size"))?;
    for offset in (0..clone_length).step_by(chunk_step) {
        let duplicate = DUPLICATE_EXTENTS_DATA {
            FileHandle: HANDLE(source.as_raw_handle()),
            SourceFileOffset: i64::try_from(offset)
                .map_err(|_| io::Error::other("clone offset overflow"))?,
            TargetFileOffset: i64::try_from(offset)
                .map_err(|_| io::Error::other("clone offset overflow"))?,
            ByteCount: i64::try_from((clone_length - offset).min(CLONE_CHUNK))
                .map_err(|_| io::Error::other("clone length overflow"))?,
        };
        // SAFETY: both handles remain live for the synchronous request; the
        // input pointer names one complete value.
        unsafe {
            DeviceIoControl(
                HANDLE(target.as_raw_handle()),
                FSCTL_DUPLICATE_EXTENTS_TO_FILE,
                Some((&raw const duplicate).cast()),
                u32::try_from(size_of::<DUPLICATE_EXTENTS_DATA>())
                    .map_err(|_| io::Error::other("clone control size overflow"))?,
                None,
                0,
                None,
                None,
            )?;
        }
    }
    if clone_length < length {
        // A sparse tail must stay sparse: writing a zero-filled hole would
        // allocate it. Query only the bounded tail, not the whole source.
        let ranges = query_allocated_data_ranges(source, clone_length, length - clone_length, 64)?;
        let mut buffer = [0_u8; 64 * 1024];
        for range in ranges {
            let size = usize::try_from(range.length)
                .map_err(|_| io::Error::other("clone tail range overflow"))?;
            source.seek(SeekFrom::Start(range.offset))?;
            target.seek(SeekFrom::Start(range.offset))?;
            source.read_exact(buffer.get_mut(..size).ok_or_else(|| {
                io::Error::new(io::ErrorKind::InvalidData, "clone tail exceeds buffer")
            })?)?;
            target.write_all(buffer.get(..size).ok_or_else(|| {
                io::Error::new(io::ErrorKind::InvalidData, "clone tail exceeds buffer")
            })?)?;
        }
    }
    // Durability belongs to the enclosing SDK operation, not each copied file.
    Ok(())
}

pub fn allocated_data_ranges(
    file: &cap_std::fs::File,
    logical_bytes: u64,
    maximum_ranges: u32,
) -> io::Result<Vec<HostDataRange>> {
    allocated_data_ranges_platform(file, logical_bytes, maximum_ranges)
}

#[cfg(unix)]
#[allow(unsafe_code)]
fn allocated_data_ranges_platform(
    file: &cap_std::fs::File,
    logical_bytes: u64,
    maximum_ranges: u32,
) -> io::Result<Vec<HostDataRange>> {
    use std::os::fd::AsRawFd;

    let mut ranges = Vec::new();
    let mut offset = 0_u64;
    while offset < logical_bytes {
        let offset_i64 = i64::try_from(offset)
            .map_err(|_| io::Error::new(io::ErrorKind::InvalidInput, "file offset exceeds i64"))?;
        // SAFETY: the descriptor is borrowed from a live regular file and
        // `lseek` only updates that descriptor's current offset.
        let data = unsafe { libc::lseek(file.as_raw_fd(), offset_i64, libc::SEEK_DATA) };
        if data < 0 {
            let error = io::Error::last_os_error();
            if error.raw_os_error() == Some(libc::ENXIO) {
                break;
            }
            return Err(error);
        }
        // SAFETY: same live descriptor and bounded non-negative offset.
        let hole = unsafe { libc::lseek(file.as_raw_fd(), data, libc::SEEK_HOLE) };
        if hole < 0 {
            return Err(io::Error::last_os_error());
        }
        let data = u64::try_from(data)
            .map_err(|_| io::Error::new(io::ErrorKind::InvalidData, "negative data offset"))?;
        let hole = u64::try_from(hole)
            .map_err(|_| io::Error::new(io::ErrorKind::InvalidData, "negative hole offset"))?
            .min(logical_bytes);
        if hole <= data {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "host sparse-range query made no progress",
            ));
        }
        if ranges.len() >= usize::try_from(maximum_ranges).unwrap_or(usize::MAX) {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "host file exceeds admitted sparse range count",
            ));
        }
        ranges.push(HostDataRange {
            offset: data,
            length: hole - data,
        });
        offset = hole;
    }
    Ok(ranges)
}

#[cfg(windows)]
#[allow(unsafe_code)]
fn allocated_data_ranges_platform(
    file: &cap_std::fs::File,
    logical_bytes: u64,
    maximum_ranges: u32,
) -> io::Result<Vec<HostDataRange>> {
    query_allocated_data_ranges(file, 0, logical_bytes, maximum_ranges)
}

#[cfg(windows)]
#[allow(unsafe_code)]
#[allow(
    clippy::too_many_lines,
    reason = "one bounded native range query and validation"
)]
fn query_allocated_data_ranges(
    file: &cap_std::fs::File,
    offset: u64,
    length: u64,
    maximum_ranges: u32,
) -> io::Result<Vec<HostDataRange>> {
    use std::mem::size_of;
    use std::os::windows::io::AsRawHandle;
    use windows::Win32::Foundation::{ERROR_INSUFFICIENT_BUFFER, ERROR_MORE_DATA, HANDLE};
    use windows::Win32::System::IO::DeviceIoControl;
    use windows::Win32::System::Ioctl::{
        FILE_ALLOCATED_RANGE_BUFFER, FSCTL_QUERY_ALLOCATED_RANGES,
    };

    if length == 0 {
        return Ok(Vec::new());
    }
    let end = offset
        .checked_add(length)
        .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidInput, "range end overflow"))?;
    let max_capacity = usize::try_from(u64::from(maximum_ranges) + 1)
        .unwrap_or(usize::MAX)
        .min(u32::MAX as usize / size_of::<FILE_ALLOCATED_RANGE_BUFFER>());
    let mut output = vec![FILE_ALLOCATED_RANGE_BUFFER::default(); max_capacity.min(8)];
    let query = FILE_ALLOCATED_RANGE_BUFFER {
        FileOffset: i64::try_from(offset)
            .map_err(|_| io::Error::new(io::ErrorKind::InvalidInput, "range offset exceeds i64"))?,
        Length: i64::try_from(length)
            .map_err(|_| io::Error::new(io::ErrorKind::InvalidInput, "file length exceeds i64"))?,
    };
    let input_bytes = u32::try_from(size_of::<FILE_ALLOCATED_RANGE_BUFFER>())
        .map_err(|_| io::Error::new(io::ErrorKind::InvalidInput, "input size overflow"))?;
    let mut returned: u32;
    loop {
        let output_bytes =
            u32::try_from(output.len() * size_of::<FILE_ALLOCATED_RANGE_BUFFER>())
                .map_err(|_| io::Error::new(io::ErrorKind::InvalidInput, "output size overflow"))?;
        returned = 0;
        // SAFETY: every pointer addresses a live, correctly sized value/buffer
        // for the synchronous call; the borrowed file remains open throughout.
        let result = unsafe {
            DeviceIoControl(
                HANDLE(file.as_raw_handle()),
                FSCTL_QUERY_ALLOCATED_RANGES,
                Some(std::ptr::from_ref(&query).cast()),
                input_bytes,
                Some(output.as_mut_ptr().cast()),
                output_bytes,
                Some(&raw mut returned),
                None,
            )
        };
        match result {
            Ok(()) => break,
            Err(error)
                if error.code() == windows::core::HRESULT::from_win32(ERROR_MORE_DATA.0)
                    || error.code()
                        == windows::core::HRESULT::from_win32(ERROR_INSUFFICIENT_BUFFER.0) =>
            {
                if output.len() == max_capacity {
                    return Err(io::Error::new(
                        io::ErrorKind::InvalidData,
                        "host file exceeds admitted sparse range count",
                    ));
                }
                output.resize(
                    output.len().saturating_mul(2).min(max_capacity),
                    FILE_ALLOCATED_RANGE_BUFFER::default(),
                );
            }
            Err(error) => return Err(io::Error::other(error.to_string())),
        }
    }
    let count = usize::try_from(returned)
        .ok()
        .filter(|bytes| bytes % size_of::<FILE_ALLOCATED_RANGE_BUFFER>() == 0)
        .map(|bytes| bytes / size_of::<FILE_ALLOCATED_RANGE_BUFFER>())
        .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidData, "invalid range response"))?;
    if count > usize::try_from(maximum_ranges).unwrap_or(usize::MAX) {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "host file exceeds admitted sparse range count",
        ));
    }
    output
        .into_iter()
        .take(count)
        .filter_map(|range| {
            let start = match u64::try_from(range.FileOffset) {
                Ok(start) => start.max(offset),
                Err(_) => {
                    return Some(Err(io::Error::new(
                        io::ErrorKind::InvalidData,
                        "negative range offset",
                    )));
                }
            };
            let Ok(size) = u64::try_from(range.Length) else {
                return Some(Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    "negative range length",
                )));
            };
            let range_end = match u64::try_from(range.FileOffset)
                .ok()
                .and_then(|base| base.checked_add(size))
            {
                Some(range_end) => range_end.min(end),
                None => {
                    return Some(Err(io::Error::new(
                        io::ErrorKind::InvalidData,
                        "range end overflow",
                    )));
                }
            };
            (start < range_end).then_some(Ok(HostDataRange {
                offset: start,
                length: range_end - start,
            }))
        })
        .collect()
}

#[cfg(not(any(unix, windows)))]
fn allocated_data_ranges_platform(
    _: &cap_std::fs::File,
    _: u64,
    _: u32,
) -> io::Result<Vec<HostDataRange>> {
    Err(io::Error::new(
        io::ErrorKind::Unsupported,
        "host sparse range discovery is unavailable",
    ))
}

#[cfg(unix)]
fn open_root_directory(path: &Path) -> io::Result<File> {
    use std::os::unix::fs::OpenOptionsExt;
    let mut options = std::fs::OpenOptions::new();
    options
        .read(true)
        .custom_flags(libc::O_DIRECTORY | libc::O_NOFOLLOW);
    #[cfg(target_os = "linux")]
    {
        options.custom_flags(libc::O_DIRECTORY | libc::O_NOFOLLOW | libc::O_NOATIME);
        match options.open(path) {
            Ok(directory) => return Ok(directory),
            Err(error) if error.kind() == io::ErrorKind::PermissionDenied => {
                options.custom_flags(libc::O_DIRECTORY | libc::O_NOFOLLOW);
            }
            Err(error) => return Err(error),
        }
    }
    options.open(path)
}

#[cfg(windows)]
fn open_root_directory(path: &Path) -> io::Result<File> {
    use std::os::windows::fs::OpenOptionsExt;
    use windows::Win32::Storage::FileSystem::{
        FILE_FLAG_BACKUP_SEMANTICS, FILE_FLAG_OPEN_REPARSE_POINT, FILE_SHARE_DELETE,
        FILE_SHARE_READ, FILE_SHARE_WRITE,
    };
    let mut options = std::fs::OpenOptions::new();
    options
        .read(true)
        .share_mode(FILE_SHARE_READ.0 | FILE_SHARE_WRITE.0 | FILE_SHARE_DELETE.0)
        .custom_flags(FILE_FLAG_BACKUP_SEMANTICS.0 | FILE_FLAG_OPEN_REPARSE_POINT.0);
    options.open(path)
}

#[cfg(not(any(unix, windows)))]
fn open_root_directory(path: &Path) -> io::Result<File> {
    std::fs::File::open(path)
}

#[cfg(windows)]
fn root_is_reparse_point(metadata: &std::fs::Metadata) -> bool {
    use std::os::windows::fs::MetadataExt;
    use windows::Win32::Storage::FileSystem::FILE_ATTRIBUTE_REPARSE_POINT;
    metadata.file_attributes() & FILE_ATTRIBUTE_REPARSE_POINT.0 != 0
}

#[cfg(not(windows))]
fn root_is_reparse_point(_: &std::fs::Metadata) -> bool {
    false
}

#[cfg(all(test, target_os = "macos"))]
mod macos_metadata_tests {
    use super::{HostRoot, MacMetadataError};
    use crate::kernel::{FileMetadata, MetadataField};
    use std::os::unix::fs::MetadataExt as _;
    use std::path::Path;
    use std::time::UNIX_EPOCH;

    #[test]
    fn clone_from_a_separate_held_source_is_cow_and_cannot_escape()
    -> Result<(), Box<dyn std::error::Error>> {
        let temporary = tempfile::tempdir()?;
        let source_path = temporary.path().join("source");
        let view_path = temporary.path().join("view");
        std::fs::create_dir(&source_path)?;
        std::fs::create_dir(&view_path)?;
        std::fs::write(source_path.join("file"), b"original")?;
        let source = HostRoot::open(&source_path)?;
        let view = HostRoot::open(&view_path)?;
        if !view.clone_file_from(&source, Path::new("file"), Path::new("clone"))? {
            return Ok(()); // This volume does not support APFS cloning.
        }
        std::fs::write(source_path.join("file"), b"changed")?;
        assert_eq!(std::fs::read(view_path.join("clone"))?, b"original");
        assert!(!matches!(
            view.clone_file_from(&source, Path::new("../view/clone"), Path::new("escape")),
            Ok(true)
        ));
        assert!(!view_path.join("escape").exists());
        std::os::unix::fs::symlink(&view_path, source_path.join("pivot"))?;
        assert!(!matches!(
            view.clone_file_from(&source, Path::new("pivot/clone"), Path::new("escape")),
            Ok(true)
        ));
        assert!(!view_path.join("escape").exists());
        Ok(())
    }

    #[test]
    fn held_metadata_restores_fields_without_retargeting_a_replaced_path()
    -> Result<(), Box<dyn std::error::Error>> {
        let temporary = tempfile::tempdir()?;
        let root_path = temporary.path().join("root");
        std::fs::create_dir(&root_path)?;
        let original = root_path.join("file");
        std::fs::write(&original, b"old")?;
        let owner = std::fs::metadata(&original)?;
        let root = HostRoot::open(&root_path)?;
        let target = root.open_macos_metadata_target(Path::new("file"))?;
        let moved = root_path.join("moved");
        std::fs::rename(&original, &moved)?;
        std::fs::write(&original, b"replacement")?;

        let metadata = FileMetadata {
            posix_mode: MetadataField::Value(0o4751),
            posix_uid: MetadataField::Value(owner.uid()),
            posix_gid: MetadataField::Value(owner.gid()),
            created_ns: MetadataField::Value(1_234_567_890_123_000_000),
            modified_ns: MetadataField::Value(1_234_567_891_234_000_000),
            accessed_ns: MetadataField::Value(1_234_567_892_345_000_000),
            ..FileMetadata::default()
        };
        target.apply(metadata)?;
        let observed = std::fs::metadata(&moved)?;
        assert_eq!(observed.mode() & 0o7777, 0o4751);
        assert_eq!(observed.uid(), owner.uid());
        assert_eq!(observed.gid(), owner.gid());
        for (actual, expected) in [
            (observed.created()?, 1_234_567_890_123_000_000_u128),
            (observed.modified()?, 1_234_567_891_234_000_000_u128),
            (observed.accessed()?, 1_234_567_892_345_000_000_u128),
        ] {
            assert_eq!(actual.duration_since(UNIX_EPOCH)?.as_nanos(), expected);
        }
        assert_eq!(std::fs::read(&original)?, b"replacement");
        assert_ne!(std::fs::metadata(&original)?.mode() & 0o7777, 0o4751);
        assert!(
            root.open_macos_metadata_target(Path::new("../outside"))
                .is_err()
        );
        let outside = temporary.path().join("outside");
        std::fs::write(&outside, b"untouched")?;
        std::os::unix::fs::symlink(temporary.path(), root_path.join("pivot"))?;
        assert!(
            root.open_macos_metadata_target(Path::new("pivot/outside"))
                .is_err()
        );
        assert_eq!(std::fs::read(outside)?, b"untouched");
        root.open_macos_metadata_target(Path::new("file"))?
            .apply(FileMetadata {
                posix_flags: MetadataField::Value(0),
                ..FileMetadata::default()
            })?;
        assert!(matches!(
            root.open_macos_metadata_target(Path::new("file"))?
                .apply(FileMetadata {
                    posix_flags: MetadataField::Value(1),
                    ..FileMetadata::default()
                }),
            Err(MacMetadataError::Unsupported("posix_flags"))
        ));
        assert!(matches!(
            root.open_macos_metadata_target(Path::new("file"))?
                .apply(FileMetadata {
                    changed_ns: MetadataField::Value(1),
                    ..FileMetadata::default()
                }),
            Err(MacMetadataError::Unsupported(
                "changed_ns without native-view baseline"
            ))
        ));
        Ok(())
    }

    #[test]
    fn fifo_metadata_uses_a_nonblocking_held_handle_and_socket_metadata_fails_closed()
    -> Result<(), Box<dyn std::error::Error>> {
        use std::os::unix::ffi::OsStrExt as _;

        let temporary = tempfile::tempdir()?;
        let pipe = temporary.path().join("pipe");
        let name = std::ffi::CString::new(pipe.as_os_str().as_bytes())?;
        assert_eq!(unsafe { libc::mkfifo(name.as_ptr(), 0o600) }, 0);
        let socket = temporary.path().join("socket");
        let _listener = std::os::unix::net::UnixListener::bind(&socket)?;
        let root = HostRoot::open(temporary.path())?;
        root.open_macos_metadata_target(Path::new("pipe"))?
            .apply(FileMetadata {
                posix_mode: MetadataField::Value(0o640),
                created_ns: MetadataField::Value(1_234_567_890_123_000_000),
                modified_ns: MetadataField::Value(1_234_567_891_234_000_000),
                accessed_ns: MetadataField::Value(1_234_567_892_345_000_000),
                ..FileMetadata::default()
            })?;
        let observed = std::fs::symlink_metadata(&pipe)?;
        assert_eq!(observed.mode() & 0o7777, 0o640);
        for (actual, expected) in [
            (observed.created()?, 1_234_567_890_123_000_000_u128),
            (observed.modified()?, 1_234_567_891_234_000_000_u128),
            (observed.accessed()?, 1_234_567_892_345_000_000_u128),
        ] {
            assert_eq!(actual.duration_since(UNIX_EPOCH)?.as_nanos(), expected);
        }
        root.open_macos_metadata_target(Path::new("socket"))?
            .apply(FileMetadata::default())?;
        assert!(matches!(
            root.open_macos_metadata_target(Path::new("socket"))?
                .apply(FileMetadata {
                    posix_mode: MetadataField::Value(0o640),
                    ..FileMetadata::default()
                }),
            Err(MacMetadataError::Unsupported("non-ordinary file kind"))
        ));
        let old_socket = temporary.path().join("old-socket");
        let pending = root.open_macos_metadata_target(Path::new("socket"))?;
        std::fs::rename(&socket, old_socket)?;
        let _replacement = std::os::unix::net::UnixListener::bind(&socket)?;
        assert!(matches!(
            pending.apply(FileMetadata::default()),
            Err(MacMetadataError::Io(_))
        ));
        Ok(())
    }

    #[tokio::test(flavor = "current_thread")]
    async fn held_metadata_offload_does_not_stall_the_service_executor()
    -> Result<(), Box<dyn std::error::Error>> {
        use std::sync::mpsc;
        use std::time::Duration;

        let temporary = tempfile::tempdir()?;
        std::fs::write(temporary.path().join("file"), b"data")?;
        let root = HostRoot::open(temporary.path())?;
        let target = root.open_macos_metadata_target(Path::new("file"))?;
        let (started_tx, started_rx) = mpsc::channel();
        let (release_tx, release_rx) = mpsc::channel();
        let worker = tokio::spawn(async move {
            acyclic_native_runtime::run_blocking_io(move || {
                assert!(started_tx.send(()).is_ok(), "test receiver remains live");
                let executor_progressed = release_rx.recv_timeout(Duration::from_secs(2)).is_ok();
                target.apply(FileMetadata::default())?;
                Ok::<_, MacMetadataError>(executor_progressed)
            })
            .await
        });
        tokio::task::spawn_blocking(move || started_rx.recv_timeout(Duration::from_secs(2)))
            .await??;
        release_tx.send(())?;
        assert!(worker.await???);
        Ok(())
    }

    #[test]
    fn special_node_creation_stays_inside_the_held_root() -> Result<(), Box<dyn std::error::Error>>
    {
        use std::os::unix::fs::FileTypeExt as _;

        let temporary = tempfile::tempdir()?;
        let root_path = temporary.path().join("root");
        let outside = temporary.path().join("outside");
        std::fs::create_dir(&root_path)?;
        std::fs::create_dir(&outside)?;
        std::os::unix::fs::symlink(&outside, root_path.join("pivot"))?;
        let root = HostRoot::open(&root_path)?;
        assert!(
            root.create_fifo_held(Path::new("pivot/pipe"), 0o600)
                .is_err()
        );
        assert!(root.bind_unix_socket(Path::new("pivot/socket")).is_err());
        assert!(
            root.create_device_held(
                Path::new("pivot/device"),
                u32::from(libc::S_IFCHR) | 0o600,
                0,
            )
            .is_err()
        );
        assert!(std::fs::read_dir(&outside)?.next().is_none());

        root.create_fifo_held(Path::new("pipe"), 0o600)?;
        root.bind_unix_socket(Path::new("socket"))?;
        assert!(
            std::fs::symlink_metadata(root_path.join("pipe"))?
                .file_type()
                .is_fifo()
        );
        assert!(
            std::fs::symlink_metadata(root_path.join("socket"))?
                .file_type()
                .is_socket()
        );
        Ok(())
    }
}

#[cfg(all(test, unix))]
mod tests {
    use super::HostRoot;
    use std::io::Read;
    use std::path::Path;

    /// Only a local root may serve host I/O inline on a native callback
    /// thread; every other stays bounded by the callback's timeout.
    #[test]
    fn a_local_root_is_classified_local() -> std::io::Result<()> {
        let temporary = tempfile::tempdir()?;
        assert!(HostRoot::open(temporary.path())?.is_local());
        #[cfg(target_os = "linux")]
        assert!(super::REMOTE_FILESYSTEMS.contains(&0x6969));
        Ok(())
    }

    #[cfg(target_os = "linux")]
    #[test]
    fn capability_reads_do_not_change_owned_access_times() -> std::io::Result<()> {
        use std::os::unix::fs::MetadataExt as _;
        use std::time::{Duration, UNIX_EPOCH};

        let temporary = tempfile::tempdir()?;
        let root_path = temporary.path().to_path_buf();
        let directory = temporary.path().join("directory");
        std::fs::create_dir(&directory)?;
        let file = directory.join("file");
        std::fs::write(&file, b"body")?;
        let old = UNIX_EPOCH + Duration::from_secs(1_700_000_000);
        for path in [&file, &directory, &root_path] {
            std::fs::File::open(path)?.set_times(std::fs::FileTimes::new().set_accessed(old))?;
        }

        let root = HostRoot::open(temporary.path())?;
        let mut body = String::new();
        root.open_file(Path::new("directory/file"))?
            .read_to_string(&mut body)?;
        assert_eq!(body, "body");
        let held = root.open_dir_held(Path::new("directory"))?;
        assert_eq!(HostRoot::scan_held_dir(&held)?.count(), 1);
        let root_directory = root.open_dir_held(Path::new(""))?;
        assert_eq!(HostRoot::scan_held_dir(&root_directory)?.count(), 1);
        for path in [&file, &directory, &root_path] {
            assert_eq!(std::fs::metadata(path)?.atime(), 1_700_000_000);
        }
        Ok(())
    }

    #[test]
    fn held_root_rejects_intermediate_symlink_escape_for_reads_and_writes() -> std::io::Result<()> {
        use std::os::unix::fs::symlink;

        let temporary = tempfile::tempdir()?;
        let root_path = temporary.path().join("root");
        let outside_path = temporary.path().join("outside");
        std::fs::create_dir(&root_path)?;
        std::fs::create_dir(&outside_path)?;
        std::fs::write(outside_path.join("secret"), b"outside")?;
        symlink(&outside_path, root_path.join("pivot"))?;

        let root = HostRoot::open(&root_path)?;
        assert!(root.open_file(Path::new("pivot/secret")).is_err());
        assert!(
            root.symlink_metadata_held(Path::new("pivot/secret"))
                .is_err()
        );
        assert!(root.create_file(Path::new("pivot/created")).is_err());
        assert!(!outside_path.join("created").exists());

        let mut secret = String::new();
        std::fs::File::open(outside_path.join("secret"))?.read_to_string(&mut secret)?;
        assert_eq!(secret, "outside");
        Ok(())
    }

    #[test]
    fn held_special_metadata_rejects_intermediate_symlink_escape() -> std::io::Result<()> {
        use std::os::unix::fs::{FileTypeExt as _, PermissionsExt as _, symlink};

        let temporary = tempfile::tempdir()?;
        let root_path = temporary.path().join("root");
        let outside_path = temporary.path().join("outside");
        std::fs::create_dir(&root_path)?;
        std::fs::create_dir(&outside_path)?;
        let target = outside_path.join("target");
        std::fs::write(&target, b"outside")?;
        std::fs::set_permissions(&target, std::fs::Permissions::from_mode(0o644))?;
        symlink(&outside_path, root_path.join("pivot"))?;

        let root = HostRoot::open(&root_path)?;
        assert!(
            root.set_permissions_without_open(Path::new("pivot/target"), 0o600)
                .is_err()
        );
        assert!(
            root.create_fifo_held(Path::new("pivot/created"), 0o600)
                .is_err()
        );
        assert!(!outside_path.join("created").exists());
        root.create_fifo_held(Path::new("fifo"), 0o600)?;
        assert!(
            std::fs::symlink_metadata(root_path.join("fifo"))?
                .file_type()
                .is_fifo()
        );
        assert_eq!(
            std::fs::metadata(&target)?.permissions().mode() & 0o777,
            0o644
        );
        assert_eq!(std::fs::read(target)?, b"outside");
        Ok(())
    }

    #[cfg(target_os = "linux")]
    #[test]
    fn held_linux_metadata_targets_inode_across_rename_and_replacement()
    -> Result<(), Box<dyn std::error::Error>> {
        use crate::kernel::{FileMetadata, MetadataField};
        use std::os::unix::fs::{MetadataExt as _, PermissionsExt as _};

        let temporary = tempfile::tempdir()?;
        let root_path = temporary.path().join("root");
        std::fs::create_dir(&root_path)?;
        let original = root_path.join("original");
        let moved = root_path.join("moved");
        std::fs::write(&original, b"old")?;
        let root = HostRoot::open(&root_path)?;
        let target = root.open_linux_metadata_target(Path::new("original"))?;
        std::fs::rename(&original, &moved)?;
        std::fs::write(&original, b"new")?;
        std::fs::set_permissions(&original, std::fs::Permissions::from_mode(0o644))?;
        let owner = std::fs::metadata(&moved)?;
        let original_mode = owner.permissions().mode() & 0o7777;
        let atime_ns = 1_700_000_000_123_456_789_i64;
        let mtime_ns = 1_700_000_001_987_654_321_i64;
        let representable = FileMetadata {
            posix_mode: MetadataField::Value(0o600),
            posix_uid: MetadataField::Value(owner.uid()),
            posix_gid: MetadataField::Value(owner.gid()),
            accessed_ns: MetadataField::Value(atime_ns),
            modified_ns: MetadataField::Value(mtime_ns),
            ..FileMetadata::default()
        };
        assert!(matches!(
            target.apply(FileMetadata {
                created_ns: MetadataField::Value(12),
                ..representable
            }),
            Err(super::LinuxMetadataError::Unsupported("created_ns"))
        ));
        assert_eq!(
            std::fs::metadata(&moved)?.permissions().mode() & 0o7777,
            original_mode
        );
        target.apply(representable)?;
        let old = std::fs::metadata(&moved)?;
        assert_eq!(old.permissions().mode() & 0o777, 0o600);
        assert_eq!(old.atime() * 1_000_000_000 + old.atime_nsec(), atime_ns);
        assert_eq!(old.mtime() * 1_000_000_000 + old.mtime_nsec(), mtime_ns);
        assert_eq!(std::fs::read(&moved)?, b"old");
        assert_eq!(std::fs::read(&original)?, b"new");
        assert_eq!(
            std::fs::metadata(&original)?.permissions().mode() & 0o777,
            0o644
        );
        Ok(())
    }

    #[cfg(target_os = "linux")]
    #[test]
    fn held_linux_symlink_metadata_does_not_mutate_target() -> Result<(), Box<dyn std::error::Error>>
    {
        use crate::kernel::{FileMetadata, MetadataField};
        use std::os::unix::fs::{MetadataExt as _, symlink};

        let temporary = tempfile::tempdir()?;
        let root_path = temporary.path().join("root");
        std::fs::create_dir(&root_path)?;
        let outside = temporary.path().join("outside");
        std::fs::write(&outside, b"outside")?;
        let link = root_path.join("link");
        symlink(&outside, &link)?;
        let outside_before = std::fs::metadata(&outside)?;
        let root = HostRoot::open(&root_path)?;
        let target = root.open_linux_metadata_target(Path::new("link"))?;
        let desired = 1_700_000_000_456_789_123_i64;
        target.apply(FileMetadata {
            modified_ns: MetadataField::Value(desired),
            ..FileMetadata::default()
        })?;
        let link_after = std::fs::symlink_metadata(&link)?;
        let outside_after = std::fs::metadata(&outside)?;
        assert_eq!(
            link_after.mtime() * 1_000_000_000 + link_after.mtime_nsec(),
            desired
        );
        assert_eq!(outside_after.mtime(), outside_before.mtime());
        assert_eq!(outside_after.mtime_nsec(), outside_before.mtime_nsec());
        assert_eq!(std::fs::read(outside)?, b"outside");
        Ok(())
    }

    #[test]
    fn root_symbolic_link_is_never_admitted() -> std::io::Result<()> {
        use std::os::unix::fs::symlink;

        let temporary = tempfile::tempdir()?;
        let real = temporary.path().join("real");
        let link = temporary.path().join("link");
        std::fs::create_dir(&real)?;
        symlink(&real, &link)?;
        assert!(HostRoot::open(&link).is_err());
        Ok(())
    }

    #[test]
    fn bound_unix_socket_lands_inside_the_held_root() -> std::io::Result<()> {
        use std::os::unix::fs::FileTypeExt;

        let temporary = tempfile::tempdir()?;
        std::fs::create_dir(temporary.path().join("nested"))?;
        let root = HostRoot::open(temporary.path())?;
        root.bind_unix_socket(Path::new("root.sock"))?;
        root.bind_unix_socket(Path::new("nested/child.sock"))?;
        for bound in ["root.sock", "nested/child.sock"] {
            let file_type = std::fs::symlink_metadata(temporary.path().join(bound))?.file_type();
            assert!(file_type.is_socket(), "{bound} is not a socket");
        }
        Ok(())
    }

    #[cfg(target_os = "macos")]
    #[test]
    fn bound_unix_socket_rejects_paths_beyond_sun_path() -> std::io::Result<()> {
        let temporary = tempfile::tempdir()?;
        let root = HostRoot::open(temporary.path())?;
        let name = format!("{}.sock", "n".repeat(128));
        match root.bind_unix_socket(Path::new(&name)) {
            Err(error) => assert_eq!(error.kind(), std::io::ErrorKind::InvalidInput),
            Ok(()) => return Err(std::io::Error::other("over-long socket path was admitted")),
        }
        Ok(())
    }
}

#[cfg(all(test, windows))]
mod windows_clone_tests {
    use super::{HostRoot, WindowsMetadataError, allocated_data_ranges};
    use acyclic_native_runtime::{Durability, NativeFile, OwnedWrite};
    use bytes::Bytes;
    use std::io::{Read, Seek, SeekFrom, Write};
    use std::os::windows::io::AsRawHandle;
    use std::path::Path;
    use windows::Win32::Foundation::HANDLE;
    use windows::Win32::System::IO::DeviceIoControl;
    use windows::Win32::System::Ioctl::FSCTL_SET_SPARSE;

    #[tokio::test]
    async fn held_async_copy_preserves_source_hardlinks_and_rejects_escape() -> std::io::Result<()>
    {
        let temporary = tempfile::tempdir()?;
        let source_path = temporary.path().join("source");
        let destination_path = temporary.path().join("destination");
        let outside_path = temporary.path().join("outside");
        std::fs::create_dir(&source_path)?;
        std::fs::create_dir(&destination_path)?;
        std::fs::create_dir(&outside_path)?;
        std::fs::write(source_path.join("file"), b"source contents")?;
        std::fs::hard_link(source_path.join("file"), source_path.join("alias"))?;
        std::fs::write(outside_path.join("untouched"), b"outside")?;
        let source = HostRoot::open(&source_path)?;
        let destination = HostRoot::open(&destination_path)?;
        destination
            .copy_file_from(&source, Path::new("alias"), Path::new("copy"))
            .await?;
        assert_eq!(
            std::fs::read(destination_path.join("copy"))?,
            b"source contents"
        );
        std::fs::write(destination_path.join("copy"), b"changed")?;
        assert_eq!(std::fs::read(source_path.join("file"))?, b"source contents");
        assert_eq!(
            std::fs::read(source_path.join("alias"))?,
            b"source contents"
        );

        assert!(
            destination
                .copy_file_from(
                    &source,
                    Path::new("../outside/untouched"),
                    Path::new("escape")
                )
                .await
                .is_err()
        );
        match std::os::windows::fs::symlink_dir(&outside_path, source_path.join("pivot")) {
            Ok(()) => assert!(
                destination
                    .copy_file_from(&source, Path::new("pivot/untouched"), Path::new("escape"))
                    .await
                    .is_err()
            ),
            Err(error) if error.kind() == std::io::ErrorKind::PermissionDenied => {}
            Err(error) => return Err(error),
        }
        assert!(!destination_path.join("escape").exists());
        assert_eq!(std::fs::read(outside_path.join("untouched"))?, b"outside");
        Ok(())
    }

    #[tokio::test]
    async fn cancelled_async_copy_leaves_no_partial_output() -> std::io::Result<()> {
        use std::time::Duration;

        let temporary = tempfile::tempdir()?;
        let source_path = temporary.path().join("source");
        let destination_path = temporary.path().join("destination");
        std::fs::create_dir(&source_path)?;
        std::fs::create_dir(&destination_path)?;
        let source_file = std::fs::File::create(source_path.join("large"))?;
        let length = 128 * 1024 * 1024;
        source_file.set_len(length)?;
        let source = HostRoot::open(&source_path)?;
        let destination = HostRoot::open(&destination_path)?;
        let task = tokio::spawn(async move {
            destination
                .copy_file_from(&source, Path::new("large"), Path::new("copy"))
                .await
        });
        let copy_path = destination_path.join("copy");
        let staged = || -> std::io::Result<bool> {
            for entry in std::fs::read_dir(&destination_path)? {
                if entry?
                    .file_name()
                    .to_string_lossy()
                    .starts_with(super::STAGING_PREFIX)
                {
                    return Ok(true);
                }
            }
            Ok(false)
        };
        // Every observation of the destination name, before and after the
        // cancellation, finds it absent or complete.
        let absent_or_complete = || match std::fs::metadata(&copy_path) {
            Ok(metadata) => assert_eq!(metadata.len(), length, "partial destination"),
            // Windows briefly denies metadata access while the copier's
            // handle on the just-renamed file closes; that observes nothing.
            Err(error) => assert!(
                matches!(
                    error.kind(),
                    std::io::ErrorKind::NotFound | std::io::ErrorKind::PermissionDenied
                ),
                "{error}"
            ),
        };
        while !staged()? && !task.is_finished() {
            absent_or_complete();
            tokio::task::yield_now().await;
        }
        task.abort();
        let _ = task.await;
        // The detached owner always finishes: it publishes the complete copy
        // or removes the staged bytes, and then no staged name remains.
        while staged()? {
            absent_or_complete();
            tokio::time::sleep(Duration::from_millis(1)).await;
        }
        absent_or_complete();
        Ok(())
    }

    #[test]
    fn by_name_and_listed_stats_report_exactly_what_a_handle_query_does() -> std::io::Result<()> {
        use super::HostStat;
        use std::io::Write as _;

        let temporary = tempfile::tempdir()?;
        std::fs::create_dir(temporary.path().join("directory"))?;
        std::fs::write(temporary.path().join("directory").join("file"), b"payload")?;
        std::fs::hard_link(
            temporary.path().join("directory").join("file"),
            temporary.path().join("alias"),
        )?;
        // Growing the file through one link leaves the other link's index
        // record stale, and an open writer's record is stale until it closes.
        std::fs::OpenOptions::new()
            .append(true)
            .open(temporary.path().join("directory").join("file"))?
            .write_all(b" grown through one link")?;
        let mut writer = std::fs::File::create(temporary.path().join("open"))?;
        writer.write_all(b"written, never closed")?;
        let root = HostRoot::open(temporary.path())?;
        let same = |fast: &HostStat, held: &HostStat| {
            assert_eq!(fast.file_type(), held.file_type());
            assert_eq!(fast.len(), held.len());
            assert_eq!(fast.file_attributes(), held.file_attributes());
            assert_eq!(fast.creation_time(), held.creation_time());
            assert_eq!(fast.last_write_time(), held.last_write_time());
            assert_eq!(fast.volume_serial_number(), held.volume_serial_number());
            assert_eq!(fast.file_index(), held.file_index());
            assert_eq!(fast.created()?, held.created()?);
            assert_eq!(fast.modified()?, held.modified()?);
            assert_eq!(fast.accessed()?, held.accessed()?);
            Ok::<(), std::io::Error>(())
        };
        for path in [
            Path::new("directory"),
            Path::new("directory/file"),
            Path::new("DIRECTORY/FILE"),
        ] {
            let fast = root
                .stat_by_name(path)?
                .ok_or_else(|| std::io::Error::other("a plain path is answered by name"))?;
            let held = HostStat::from_metadata(&root.symlink_metadata(path)?);
            same(&fast, &held)?;
        }
        // Every enumerated name carries exactly the facts its own stat
        // reports, except a reparse point, which only its own stat resolves.
        for directory in [Path::new(""), Path::new("directory")] {
            let mut names = Vec::new();
            for entry in root.read_dir_stats(directory)? {
                let entry = entry?;
                let path = directory.join(&entry.name);
                let listed = entry.stat.ok_or_else(|| {
                    std::io::Error::other("a plain entry is listed with its facts")
                })?;
                same(&listed, &root.stat(&path)?)?;
                if listed.file_type().is_file() {
                    same(
                        &listed,
                        &root.stat_file(&root.open_file(&path)?.into_std())?,
                    )?;
                }
                names.push(entry.name);
            }
            names.sort();
            let expected: &[&str] = if directory.as_os_str().is_empty() {
                &["alias", "directory", "open"]
            } else {
                &["file"]
            };
            assert_eq!(names, expected);
        }
        assert_eq!(
            root.stat(Path::new("directory/missing"))
                .map_err(|error| error.kind())
                .err(),
            Some(std::io::ErrorKind::NotFound)
        );
        // Only the held walk may resolve a reparse point, final or not.
        assert!(
            root.stat_by_name(Path::new(""))
                .map(|stat| stat.is_none())?
        );
        match std::os::windows::fs::symlink_dir(
            temporary.path().join("directory"),
            temporary.path().join("link"),
        ) {
            Ok(()) => {
                assert!(root.stat_by_name(Path::new("link"))?.is_none());
                assert!(root.stat_by_name(Path::new("link/file"))?.is_none());
                assert!(root.stat(Path::new("link"))?.file_type().is_symlink());
                let listed = root
                    .read_dir_stats(Path::new(""))?
                    .find(|entry| entry.as_ref().is_ok_and(|entry| entry.name == "link"))
                    .ok_or_else(|| std::io::Error::other("link is listed"))??;
                assert!(
                    listed.stat.is_none(),
                    "a reparse point is stat'ed on its own"
                );
            }
            Err(error) if error.kind() == std::io::ErrorKind::PermissionDenied => {}
            Err(error) => return Err(error),
        }
        Ok(())
    }

    #[test]
    fn a_file_opened_by_name_is_the_one_the_held_walk_opens() -> std::io::Result<()> {
        use std::io::Read as _;

        let temporary = tempfile::tempdir()?;
        std::fs::create_dir(temporary.path().join("directory"))?;
        std::fs::write(temporary.path().join("directory").join("file"), b"payload")?;
        let root = HostRoot::open(temporary.path())?;
        for path in [Path::new("directory/file"), Path::new("DIRECTORY/FILE")] {
            let mut opened = root
                .open_file_by_name(path)?
                .ok_or_else(|| std::io::Error::other("a plain path is opened by name"))?;
            let held = root.symlink_metadata(path)?;
            let metadata = opened.metadata()?;
            assert_eq!(
                cap_primitives::fs::_WindowsByHandle::file_index(&metadata),
                cap_primitives::fs::_WindowsByHandle::file_index(&held)
            );
            let mut read = Vec::new();
            opened.read_to_end(&mut read)?;
            assert_eq!(read, b"payload");
        }
        assert!(root.open_file_by_name(Path::new(""))?.is_none());
        assert!(root.open_file_by_name(Path::new("directory")).is_err());
        assert_eq!(
            root.open_file(Path::new("directory/missing"))
                .map_err(|error| error.kind())
                .err(),
            Some(std::io::ErrorKind::NotFound)
        );
        // Only the held walk may resolve a reparse point on the way.
        match std::os::windows::fs::symlink_dir(
            temporary.path().join("directory"),
            temporary.path().join("link"),
        ) {
            Ok(()) => assert!(root.open_file_by_name(Path::new("link/file"))?.is_none()),
            Err(error) if error.kind() == std::io::ErrorKind::PermissionDenied => {}
            Err(error) => return Err(error),
        }
        Ok(())
    }

    #[tokio::test]
    async fn copy_never_replaces_an_existing_destination() -> std::io::Result<()> {
        let temporary = tempfile::tempdir()?;
        std::fs::write(temporary.path().join("source"), b"new")?;
        std::fs::write(temporary.path().join("existing"), b"old")?;
        let root = HostRoot::open(temporary.path())?;
        let copied = root
            .copy_file_from(&root, Path::new("source"), Path::new("existing"))
            .await;
        assert_eq!(
            copied.map_err(|error| error.kind()),
            Err(std::io::ErrorKind::AlreadyExists),
            "an existing destination is never replaced"
        );
        assert_eq!(std::fs::read(temporary.path().join("existing"))?, b"old");
        let mut names = std::fs::read_dir(temporary.path())?
            .map(|entry| entry.map(|entry| entry.file_name()))
            .collect::<std::io::Result<Vec<_>>>()?;
        names.sort();
        assert_eq!(
            names,
            ["existing", "source"],
            "a staged copy was left behind"
        );
        Ok(())
    }

    #[test]
    fn held_windows_metadata_applies_and_verifies_file_and_directory_fields()
    -> Result<(), Box<dyn std::error::Error>> {
        use crate::kernel::{FileMetadata, MetadataField};
        use std::os::windows::fs::MetadataExt as _;

        const FIXED_NS: i64 = 1_700_000_000_000_000_000;
        const ARCHIVE: u32 = 0x20;
        const DIRECTORY: u32 = 0x10;
        const HIDDEN: u32 = 0x2;
        let temporary = tempfile::tempdir()?;
        std::fs::write(temporary.path().join("file"), b"payload")?;
        std::fs::create_dir(temporary.path().join("directory"))?;
        let root = HostRoot::open(temporary.path())?;
        for (path, attributes) in [
            (Path::new("file"), ARCHIVE | HIDDEN),
            (Path::new("directory"), DIRECTORY | HIDDEN),
        ] {
            root.open_windows_metadata_target(path)?
                .apply(FileMetadata {
                    windows_attributes: MetadataField::Value(attributes),
                    created_ns: MetadataField::Value(FIXED_NS),
                    modified_ns: MetadataField::Value(FIXED_NS + 100),
                    accessed_ns: MetadataField::Value(FIXED_NS + 200),
                    changed_ns: MetadataField::Value(FIXED_NS + 300),
                    ..FileMetadata::default()
                })?;
            assert_eq!(
                std::fs::symlink_metadata(temporary.path().join(path))?.file_attributes(),
                attributes
            );
        }
        let file = temporary.path().join("file");
        let mut permissions = std::fs::metadata(&file)?.permissions();
        permissions.set_readonly(true);
        std::fs::set_permissions(&file, permissions.clone())?;
        root.open_windows_metadata_target(Path::new("file"))?
            .apply(FileMetadata::default())?;
        root.open_windows_metadata_target(Path::new("file"))?
            .apply(FileMetadata {
                windows_attributes: MetadataField::Value(ARCHIVE),
                ..FileMetadata::default()
            })?;
        Ok(())
    }

    #[test]
    fn held_windows_metadata_rejects_unrepresentable_fields_and_path_escape() -> std::io::Result<()>
    {
        use crate::kernel::{FileMetadata, MetadataField};

        const REPARSE_POINT: u32 = 0x400;
        let temporary = tempfile::tempdir()?;
        let root_path = temporary.path().join("root");
        std::fs::create_dir(&root_path)?;
        std::fs::write(root_path.join("file"), b"inside")?;
        std::fs::write(temporary.path().join("outside"), b"outside")?;
        let root = HostRoot::open(&root_path)?;
        assert!(matches!(
            root.open_windows_metadata_target(Path::new("file"))
                .map_err(std::io::Error::other)?
                .apply(FileMetadata {
                    windows_attributes: MetadataField::Value(REPARSE_POINT),
                    ..FileMetadata::default()
                }),
            Err(WindowsMetadataError::Unsupported("Windows attributes"))
        ));
        assert!(matches!(
            root.open_windows_metadata_target(Path::new("file"))
                .map_err(std::io::Error::other)?
                .apply(FileMetadata {
                    named_attributes: MetadataField::Value(crate::ObjectId {
                        kind: crate::ObjectKind::AttributePage,
                        digest: crate::Digest::from_bytes([7; 32]),
                    }),
                    ..FileMetadata::default()
                }),
            Err(WindowsMetadataError::Unsupported("named attributes"))
        ));
        assert!(
            root.open_windows_metadata_target(Path::new("../outside"))
                .is_err()
        );
        assert_eq!(std::fs::read(temporary.path().join("outside"))?, b"outside");
        Ok(())
    }

    #[test]
    fn held_windows_metadata_rejects_intermediate_reparse_escape() -> std::io::Result<()> {
        use std::os::windows::fs::MetadataExt as _;

        let temporary = tempfile::tempdir()?;
        let root_path = temporary.path().join("root");
        let outside = temporary.path().join("outside");
        std::fs::create_dir(&root_path)?;
        std::fs::create_dir(&outside)?;
        std::fs::write(outside.join("target"), b"outside")?;
        match std::os::windows::fs::symlink_dir(&outside, root_path.join("pivot")) {
            Ok(()) => {}
            Err(error) if error.kind() == std::io::ErrorKind::PermissionDenied => return Ok(()),
            Err(error) => return Err(error),
        }
        let before = std::fs::metadata(outside.join("target"))?.file_attributes();
        let root = HostRoot::open(&root_path)?;
        assert!(
            root.open_windows_metadata_target(Path::new("pivot/target"))
                .is_err()
        );
        assert_eq!(
            std::fs::metadata(outside.join("target"))?.file_attributes(),
            before
        );
        assert_eq!(std::fs::read(outside.join("target"))?, b"outside");
        Ok(())
    }

    #[test]
    fn held_windows_metadata_updates_final_reparse_point_without_touching_target()
    -> Result<(), Box<dyn std::error::Error>> {
        use crate::kernel::{FileMetadata, MetadataField};
        use std::os::windows::fs::MetadataExt as _;

        const FIXED_NS: i64 = 1_700_000_000_000_000_000;
        const HIDDEN: u32 = 0x2;
        let temporary = tempfile::tempdir()?;
        let root_path = temporary.path().join("root");
        std::fs::create_dir(&root_path)?;
        let target = root_path.join("target");
        let link = root_path.join("link");
        std::fs::write(&target, b"target")?;
        match std::os::windows::fs::symlink_file(&target, &link) {
            Ok(()) => {}
            Err(error) if error.kind() == std::io::ErrorKind::PermissionDenied => return Ok(()),
            Err(error) => return Err(error.into()),
        }
        let target_attributes = std::fs::symlink_metadata(&target)?.file_attributes();
        let link_attributes = std::fs::symlink_metadata(&link)?.file_attributes();
        let root = HostRoot::open(&root_path)?;
        root.open_windows_metadata_target(Path::new("link"))?
            .apply(FileMetadata {
                windows_attributes: MetadataField::Value(link_attributes | HIDDEN),
                created_ns: MetadataField::Value(FIXED_NS),
                modified_ns: MetadataField::Value(FIXED_NS + 100),
                accessed_ns: MetadataField::Value(FIXED_NS + 200),
                changed_ns: MetadataField::Value(FIXED_NS + 300),
                ..FileMetadata::default()
            })?;
        assert_eq!(
            std::fs::symlink_metadata(&link)?.file_attributes(),
            link_attributes | HIDDEN
        );
        assert_eq!(
            std::fs::symlink_metadata(&target)?.file_attributes(),
            target_attributes
        );
        assert_eq!(std::fs::read(&target)?, b"target");
        Ok(())
    }

    #[test]
    fn held_windows_metadata_targets_leaf_across_rename_and_replacement()
    -> Result<(), Box<dyn std::error::Error>> {
        use crate::kernel::{FileMetadata, MetadataField};
        use std::os::windows::fs::MetadataExt as _;

        const HIDDEN: u32 = 0x2;
        const MODIFIED_NS: i64 = 1_700_000_000_000_000_000;
        let temporary = tempfile::tempdir()?;
        let original = temporary.path().join("original");
        let moved = temporary.path().join("moved");
        std::fs::write(&original, b"old")?;
        let root = HostRoot::open(temporary.path())?;
        let target = root.open_windows_metadata_target(Path::new("original"))?;
        std::fs::rename(&original, &moved)?;
        std::fs::write(&original, b"new")?;
        let replacement_metadata = std::fs::metadata(&original)?;
        let replacement_attributes = replacement_metadata.file_attributes();
        let replacement_modified = replacement_metadata.last_write_time();
        let moved_attributes = std::fs::metadata(&moved)?.file_attributes();
        target.apply(FileMetadata {
            windows_attributes: MetadataField::Value(moved_attributes | HIDDEN),
            modified_ns: MetadataField::Value(MODIFIED_NS),
            ..FileMetadata::default()
        })?;
        assert_eq!(
            std::fs::metadata(&moved)?.file_attributes(),
            moved_attributes | HIDDEN
        );
        assert_eq!(
            std::fs::metadata(&original)?.file_attributes(),
            replacement_attributes
        );
        const WINDOWS_EPOCH_TICKS: u64 = 116_444_736_000_000_000;
        assert_eq!(
            std::fs::metadata(&moved)?.last_write_time(),
            WINDOWS_EPOCH_TICKS + u64::try_from(MODIFIED_NS)? / 100
        );
        assert_eq!(
            std::fs::metadata(&original)?.last_write_time(),
            replacement_modified
        );
        assert_eq!(std::fs::read(&moved)?, b"old");
        assert_eq!(std::fs::read(&original)?, b"new");
        Ok(())
    }

    #[tokio::test]
    async fn capability_opened_overlapped_file_runs_native_io_without_reopen() -> std::io::Result<()>
    {
        let directory = tempfile::tempdir()?;
        let root = HostRoot::open(directory.path())?;
        let path = Path::new("overlapped.bin");
        let file = root.create_overlapped_file(path)?;
        // SAFETY: HostRoot used FILE_FLAG_OVERLAPPED and this new handle is
        // transferred directly, with no other I/O or completion-port owner.
        #[allow(unsafe_code)]
        let native = unsafe { NativeFile::from_overlapped_file_unchecked(file)? };
        native.set_len_async(8).await?;
        native
            .write_all_batch_async(vec![OwnedWrite {
                offset: 2,
                bytes: Bytes::from_static(b"abc"),
            }])
            .await?;
        native.sync_async(Durability::Full).await?;
        assert_eq!(
            std::fs::read(directory.path().join(path))?,
            b"\0\0abc\0\0\0"
        );
        Ok(())
    }

    #[test]
    fn held_metadata_rejects_intermediate_reparse_escape() -> std::io::Result<()> {
        let temporary = tempfile::tempdir()?;
        let root_path = temporary.path().join("root");
        let outside = temporary.path().join("outside");
        std::fs::create_dir(&root_path)?;
        std::fs::create_dir(&outside)?;
        std::fs::write(outside.join("secret"), b"outside")?;
        match std::os::windows::fs::symlink_dir(&outside, root_path.join("pivot")) {
            Ok(()) => {}
            Err(error) if error.kind() == std::io::ErrorKind::PermissionDenied => return Ok(()),
            Err(error) => return Err(error),
        }
        let root = HostRoot::open(&root_path)?;
        assert!(
            root.symlink_metadata_held(Path::new("pivot/secret"))
                .is_err()
        );
        Ok(())
    }

    #[cfg(feature = "native-mount")]
    #[test]
    fn block_clone_preserves_cow_or_leaves_no_destination() -> std::io::Result<()> {
        let parent = std::env::var_os("ACYCLIC_TEST_BLOCK_CLONE_ROOT");
        let require_clone = parent.is_some();
        let directory = if let Some(parent) = parent {
            tempfile::Builder::new()
                .prefix("acyclic-clone-")
                .tempdir_in(parent)?
        } else {
            tempfile::tempdir()?
        };
        let root = HostRoot::open(directory.path())?;
        let source_path = Path::new("source.bin");
        let target_path = Path::new("target.bin");
        let mut source = root.create_file(source_path)?;
        // SAFETY: this synchronous control takes no buffers and the source
        // handle remains live for the entire call.
        unsafe {
            DeviceIoControl(
                HANDLE(source.as_raw_handle()),
                FSCTL_SET_SPARSE,
                None,
                0,
                None,
                0,
                None,
                None,
            )
            .map_err(std::io::Error::other)?;
        }
        source.set_len(1024 * 1024)?;
        source.write_all(&[0xA5; 4096])?;
        source.seek(SeekFrom::Start(1024 * 1024 - 4096))?;
        source.write_all(&[0xA5; 4096])?;
        source.sync_all()?;
        let cloned = root.clone_file(source_path, target_path)?;
        let capabilities = crate::probe_native_storage_capabilities(directory.path())
            .map_err(std::io::Error::other)?;
        if capabilities.block_cloning || require_clone {
            assert!(cloned, "advertised block cloning rejected an aligned file");
        }
        if cloned {
            let mut target = root.open_file(target_path)?;
            let mut byte = [0];
            target.read_exact(&mut byte)?;
            assert_eq!(byte, [0xA5]);
            target.seek(SeekFrom::Start(512 * 1024))?;
            target.read_exact(&mut byte)?;
            assert_eq!(byte, [0]);
            target.seek(SeekFrom::Start(1024 * 1024 - 1))?;
            target.read_exact(&mut byte)?;
            assert_eq!(byte, [0xA5]);
            source.seek(SeekFrom::Start(0))?;
            source.write_all(&[0x5A])?;
            source.sync_all()?;
            target.seek(SeekFrom::Start(0))?;
            target.read_exact(&mut byte)?;
            assert_eq!(byte, [0xA5]);
        } else {
            assert!(root.symlink_metadata(target_path).is_err());
        }
        Ok(())
    }

    #[test]
    fn block_clone_accepts_unaligned_file_lengths() -> std::io::Result<()> {
        let Some(parent) = std::env::var_os("ACYCLIC_TEST_BLOCK_CLONE_ROOT") else {
            return Ok(());
        };
        let directory = tempfile::Builder::new()
            .prefix("acyclic-unaligned-clone-")
            .tempdir_in(parent)?;
        let root = HostRoot::open(directory.path())?;
        let source_path = Path::new("source.bin");
        let target_path = Path::new("target.bin");
        let mut source = root.create_file(source_path)?;
        // SAFETY: no buffers are passed and the source handle stays live.
        unsafe {
            DeviceIoControl(
                HANDLE(source.as_raw_handle()),
                FSCTL_SET_SPARSE,
                None,
                0,
                None,
                0,
                None,
                None,
            )
            .map_err(std::io::Error::other)?;
        }
        source.set_len(1024 * 1024 + 17)?;
        source.write_all(&[0xA5; 4096])?;
        source.seek(SeekFrom::Start(1024 * 1024))?;
        source.write_all(&[0x5A; 17])?;
        source.sync_all()?;
        let cloned = root.clone_file(source_path, target_path)?;
        assert!(
            cloned,
            "advertised block cloning rejected an unaligned file"
        );
        let mut target = root.open_file(target_path)?;
        let ranges = allocated_data_ranges(&target, 1024 * 1024 + 17, 3)?;
        assert_eq!(ranges.len(), 2, "clone must preserve the sparse middle");
        target.seek(SeekFrom::Start(1024 * 1024))?;
        let mut tail = [0; 17];
        target.read_exact(&mut tail)?;
        assert_eq!(tail, [0x5A; 17]);
        source.seek(SeekFrom::Start(1024 * 1024))?;
        source.write_all(&[0x11; 17])?;
        source.sync_all()?;
        target.seek(SeekFrom::Start(1024 * 1024))?;
        target.read_exact(&mut tail)?;
        assert_eq!(tail, [0x5A; 17]);
        Ok(())
    }

    #[test]
    fn block_clone_spans_multiple_native_requests() -> std::io::Result<()> {
        let Some(parent) = std::env::var_os("ACYCLIC_TEST_BLOCK_CLONE_ROOT") else {
            return Ok(());
        };
        let directory = tempfile::Builder::new()
            .prefix("acyclic-large-clone-")
            .tempdir_in(parent)?;
        let root = HostRoot::open(directory.path())?;
        let source_path = Path::new("source.bin");
        let target_path = Path::new("target.bin");
        let mut source = root.create_file(source_path)?;
        // SAFETY: no buffers are passed and the source handle stays live.
        unsafe {
            DeviceIoControl(
                HANDLE(source.as_raw_handle()),
                FSCTL_SET_SPARSE,
                None,
                0,
                None,
                0,
                None,
                None,
            )
            .map_err(std::io::Error::other)?;
        }
        let second_chunk = 1024_u64 * 1024 * 1024;
        let length = second_chunk + 64 * 1024 + 17;
        source.set_len(length)?;
        source.write_all(&[0xA5; 4096])?;
        source.seek(SeekFrom::Start(second_chunk))?;
        source.write_all(&[0x5A; 4096])?;
        source.seek(SeekFrom::Start(length - 17))?;
        source.write_all(&[0x11; 17])?;
        source.sync_all()?;
        assert!(root.clone_file(source_path, target_path)?);
        let mut target = root.open_file(target_path)?;
        target.seek(SeekFrom::Start(second_chunk))?;
        let mut byte = [0];
        target.read_exact(&mut byte)?;
        assert_eq!(byte, [0x5A]);
        target.seek(SeekFrom::Start(length - 1))?;
        target.read_exact(&mut byte)?;
        assert_eq!(byte, [0x11]);
        let ranges = allocated_data_ranges(&target, length, 4)?;
        assert_eq!(ranges.len(), 3, "multi-request clone must preserve holes");
        Ok(())
    }

    #[test]
    fn sparse_range_query_grows_only_when_ranges_require_it() -> std::io::Result<()> {
        let directory = tempfile::tempdir()?;
        let root = HostRoot::open(directory.path())?;
        let path = Path::new("ranges.bin");
        let mut file = root.create_file(path)?;
        // SAFETY: the control has no buffers and the file handle remains live.
        unsafe {
            DeviceIoControl(
                HANDLE(file.as_raw_handle()),
                FSCTL_SET_SPARSE,
                None,
                0,
                None,
                0,
                None,
                None,
            )
            .map_err(std::io::Error::other)?;
        }
        file.set_len(20 * 1024 * 1024)?;
        for index in 0..10_u64 {
            file.seek(SeekFrom::Start(index * 2 * 1024 * 1024))?;
            file.write_all(&[0xA5; 4096])?;
        }
        file.sync_all()?;
        let source = root.open_file(path)?;
        assert!(allocated_data_ranges(&source, 20 * 1024 * 1024, 9).is_err());
        let ranges = allocated_data_ranges(&source, 20 * 1024 * 1024, 10)?;
        assert_eq!(ranges.len(), 10);
        assert_eq!(ranges[0].offset, 0);
        assert_eq!(ranges[9].offset, 18 * 1024 * 1024);
        Ok(())
    }
}
