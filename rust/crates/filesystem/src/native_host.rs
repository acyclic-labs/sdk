//! Capability-rooted native filesystem access shared by capture and materialization.
#![allow(missing_docs, unsafe_code)]

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

    pub fn hard_link(&self, source: &Path, destination: &Path) -> io::Result<()> {
        self.directory
            .hard_link(source, &self.directory, destination)
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

/// Best-effort deallocation of an already-zero range of a host file.
///
/// APFS materializes the zero tail created by `ftruncate` as soon as any byte
/// of the file is written, so holes must be punched explicitly after the data
/// spans are in place. The range is shrunk inward to filesystem-block
/// alignment, and a punch the host filesystem refuses is not an error: the
/// zeros are already durable, only allocation efficiency is lost.
#[cfg(target_os = "macos")]
#[allow(unsafe_code)]
pub fn punch_hole(file: &impl std::os::fd::AsRawFd, offset: u64, length: u64) {
    let fd = file.as_raw_fd();
    let mut stats = std::mem::MaybeUninit::<libc::statfs>::uninit();
    // SAFETY: `fstatfs` fills the provided out-struct for a live descriptor.
    if unsafe { libc::fstatfs(fd, stats.as_mut_ptr()) } != 0 {
        return;
    }
    // SAFETY: `fstatfs` succeeded and initialized the struct.
    let block = u64::from(unsafe { stats.assume_init() }.f_bsize);
    if block == 0 {
        return;
    }
    let Some(start) = offset
        .checked_add(block - 1)
        .map(|edge| edge / block * block)
    else {
        return;
    };
    let Some(end) = offset.checked_add(length).map(|edge| edge / block * block) else {
        return;
    };
    if end <= start {
        return;
    }
    let (Ok(fp_offset), Ok(fp_length)) = (i64::try_from(start), i64::try_from(end - start)) else {
        return;
    };
    let arg = libc::fpunchhole_t {
        fp_flags: 0,
        reserved: 0,
        fp_offset,
        fp_length,
    };
    // SAFETY: the argument struct outlives this value-only fcntl call.
    let _ = unsafe { libc::fcntl(fd, libc::F_PUNCHHOLE, &arg) };
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
    use std::mem::size_of;
    use std::os::windows::io::AsRawHandle;
    use windows::Win32::Foundation::HANDLE;
    use windows::Win32::System::IO::DeviceIoControl;
    use windows::Win32::System::Ioctl::{
        FILE_ALLOCATED_RANGE_BUFFER, FSCTL_QUERY_ALLOCATED_RANGES,
    };

    if logical_bytes == 0 {
        return Ok(Vec::new());
    }
    let capacity = maximum_ranges
        .checked_add(1)
        .and_then(|count| usize::try_from(count).ok())
        .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidInput, "range bound overflow"))?;
    let mut output = vec![FILE_ALLOCATED_RANGE_BUFFER::default(); capacity];
    let query = FILE_ALLOCATED_RANGE_BUFFER {
        FileOffset: 0,
        Length: i64::try_from(logical_bytes)
            .map_err(|_| io::Error::new(io::ErrorKind::InvalidInput, "file length exceeds i64"))?,
    };
    let input_bytes = u32::try_from(size_of::<FILE_ALLOCATED_RANGE_BUFFER>())
        .map_err(|_| io::Error::new(io::ErrorKind::InvalidInput, "input size overflow"))?;
    let output_bytes = u32::try_from(
        output
            .len()
            .checked_mul(size_of::<FILE_ALLOCATED_RANGE_BUFFER>())
            .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidInput, "output size overflow"))?,
    )
    .map_err(|_| io::Error::new(io::ErrorKind::InvalidInput, "output size overflow"))?;
    let mut returned = 0_u32;
    // SAFETY: every pointer addresses a live, correctly sized value/buffer for
    // the synchronous call; the borrowed file remains open throughout.
    unsafe {
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
        .map_err(|error| io::Error::other(error.to_string()))?;
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
        .map(|range| {
            let offset = u64::try_from(range.FileOffset)
                .map_err(|_| io::Error::new(io::ErrorKind::InvalidData, "negative range offset"))?;
            let length = u64::try_from(range.Length)
                .map_err(|_| io::Error::new(io::ErrorKind::InvalidData, "negative range length"))?;
            Ok(HostDataRange { offset, length })
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
        FILE_FLAG_BACKUP_SEMANTICS, FILE_FLAG_OPEN_REPARSE_POINT, FILE_SHARE_READ, FILE_SHARE_WRITE,
    };
    let mut options = std::fs::OpenOptions::new();
    options
        .read(true)
        .share_mode(FILE_SHARE_READ.0 | FILE_SHARE_WRITE.0)
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
