//! Capability-rooted native filesystem access shared by capture and materialization.
#![allow(missing_docs, unsafe_code)]

use cap_fs_ext::DirExt as _;
use cap_std::fs::{Dir, Metadata, OpenOptions, Permissions, ReadDir};
#[cfg(unix)]
use std::ffi::OsStr;
use std::fs::File;
use std::io;
use std::path::Path;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct HostDataRange {
    pub offset: u64,
    pub length: u64,
}

/// A held directory capability whose relative operations cannot escape through
/// path traversal or an intermediate symbolic link/reparse point.
pub struct HostRoot {
    directory: Dir,
    identity: crate::NativeRootIdentity,
}

/// A held directory capability for race-free leaf operations.
pub struct HostDirectory {
    directory: Dir,
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

    pub fn symlink_metadata(&self, path: &Path) -> io::Result<Metadata> {
        if path.as_os_str().is_empty() {
            self.directory.dir_metadata()
        } else {
            self.directory.symlink_metadata(path)
        }
    }

    pub fn open_file(&self, path: &Path) -> io::Result<cap_std::fs::File> {
        let mut options = OpenOptions::new();
        options
            .read(true)
            ._cap_fs_ext_follow(cap_primitives::fs::FollowSymlinks::No);
        self.directory.open_with(path, &options)
    }

    pub fn create_file(&self, path: &Path) -> io::Result<File> {
        let mut options = OpenOptions::new();
        options.write(true).create_new(true);
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
        use std::os::fd::AsRawFd;
        use std::os::unix::ffi::OsStrExt;

        let source = match self.open_file(source) {
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
        if matches!(error.raw_os_error(), Some(libc::ENOTSUP | libc::ENOSYS)) {
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
        let mut source = self.open_file(source)?;
        let length = source.metadata()?.len();
        if length < 4 * 1024 || i64::try_from(length).is_err() {
            return Ok(false);
        }
        // ReFS volumes use either 4-KiB or 64-KiB clusters. Try the common
        // smaller unit first for maximum sharing, then retry at 64 KiB when
        // the volume requires it. A failed attempt is always removed.
        for alignment in [4 * 1024, 64 * 1024] {
            let mut target = self.create_file(destination)?;
            let cloned = clone_windows_file(&mut source, &mut target, length, alignment).is_ok();
            drop(target);
            if cloned {
                return Ok(true);
            }
            self.directory.remove_file(destination)?;
        }
        Ok(false)
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
        use std::os::unix::ffi::OsStrExt;
        let relative = if path.as_os_str().is_empty() {
            Path::new(".")
        } else {
            path
        };
        let destination = std::ffi::CString::new(relative.as_os_str().as_bytes())
            .map_err(|_| io::Error::new(io::ErrorKind::InvalidInput, "path contains NUL"))?;
        let mask = libc::mode_t::try_from(mode & 0o7777)
            .map_err(|_| io::Error::new(io::ErrorKind::InvalidInput, "mode exceeds mode_t"))?;
        // SAFETY: `destination` is a live NUL-terminated byte string and the
        // mode contains only a conventional permission mask.
        let result =
            unsafe { libc::fchmodat(self.raw_directory_fd(), destination.as_ptr(), mask, 0) };
        if result == 0 {
            Ok(())
        } else {
            Err(io::Error::last_os_error())
        }
    }

    #[cfg(unix)]
    #[must_use]
    pub fn raw_directory_fd(&self) -> std::os::fd::RawFd {
        use std::os::fd::AsRawFd;
        self.directory.as_raw_fd()
    }

    #[cfg(unix)]
    pub fn bind_unix_socket(&self, destination: &Path) -> io::Result<()> {
        let name = destination.file_name().ok_or_else(|| {
            io::Error::new(io::ErrorKind::InvalidInput, "socket path has no file name")
        })?;
        let parent_path = destination.parent().unwrap_or_else(|| Path::new(""));
        let parent = if parent_path.as_os_str().is_empty() {
            self.directory.try_clone()?
        } else {
            self.directory.open_dir(parent_path)?
        };
        bind_unix_socket_in(&parent, name)
    }
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
    target.sync_all()
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

#[cfg(all(test, unix))]
mod tests {
    use super::HostRoot;
    use std::io::Read;
    use std::path::Path;

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
        assert!(root.create_file(Path::new("pivot/created")).is_err());
        assert!(!outside_path.join("created").exists());

        let mut secret = String::new();
        std::fs::File::open(outside_path.join("secret"))?.read_to_string(&mut secret)?;
        assert_eq!(secret, "outside");
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
    use super::{HostRoot, allocated_data_ranges};
    use std::io::{Read, Seek, SeekFrom, Write};
    use std::os::windows::io::AsRawHandle;
    use std::path::Path;
    use windows::Win32::Foundation::HANDLE;
    use windows::Win32::System::IO::DeviceIoControl;
    use windows::Win32::System::Ioctl::FSCTL_SET_SPARSE;

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
