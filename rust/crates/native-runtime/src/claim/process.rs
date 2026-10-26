//! This process's one process lock, and what a record proves about the
//! process it names.

use super::protocol::Liveness;
use super::record::{Process, RECORD_LIMIT, Record};
use crate::{FileLock, FileLockMode};
use std::fs::{File, OpenOptions};
use std::io;
use std::path::{Path, PathBuf};
use std::sync::OnceLock;

static OWNER: OnceLock<Option<ProcessOwner>> = OnceLock::new();

/// The lock this process holds for its whole life. A claim record names it, so
/// the record lapses when the process dies.
#[derive(Debug)]
pub(super) struct ProcessOwner {
    pub(super) process: Process,
    _lock: FileLock,
}

impl ProcessOwner {
    /// This process's owner, created once. `None` when no process lock could be
    /// established, in which case each claim holds its own file lock instead.
    pub(super) fn current() -> Option<&'static Self> {
        OWNER.get_or_init(|| Self::create().ok()).as_ref()
    }

    /// This process's owner if one was already established. Never creates
    /// one: a check of another store's claim must not lock, write or sweep.
    pub(super) fn existing() -> Option<&'static Self> {
        OWNER.get().and_then(Option::as_ref)
    }

    fn create() -> io::Result<Self> {
        let directory = owner_directory()?;
        sweep(&directory);
        let pid = std::process::id();
        let nanos = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map_err(io::Error::other)?
            .as_nanos();
        let path = directory.join(format!("{pid}-{nanos}.lock"));
        let mut options = OpenOptions::new();
        options.create_new(true).read(true).write(true);
        #[cfg(windows)]
        {
            // No FILE_SHARE_DELETE: the name can be neither removed nor
            // renamed while this process lives, so its absence proves death.
            use std::os::windows::fs::OpenOptionsExt as _;
            options.share_mode(0x1 | 0x2);
        }
        let lock = FileLock::try_acquire(options.open(&path)?, FileLockMode::Exclusive)?;
        // Others prove this process dead by taking this lock, so it must
        // exclude them: a filesystem whose locks always succeed cannot hold it.
        match FileLock::try_acquire(File::open(&path)?, FileLockMode::Shared) {
            Err(error) if error.kind() == io::ErrorKind::WouldBlock => {}
            _ => return Err(io::Error::from(io::ErrorKind::Unsupported)),
        }
        let process = Process {
            pid,
            start: start_identity(pid).unwrap_or_default(),
            lock: path_bytes(&path)?,
            lock_identity: identity_bytes(&lock)?,
        };
        // Every record names this process with a fixed-width token, so one
        // check covers them all: a record too long to read back whole is
        // never written, and each claim holds its own file lock instead.
        let longest = Record {
            process: process.clone(),
            token: u64::MAX,
        };
        if longest.encode().len() > RECORD_LIMIT {
            return Err(io::Error::from(io::ErrorKind::InvalidFilename));
        }
        Ok(Self {
            process,
            _lock: lock,
        })
    }
}

/// The most probes one [`liveness`] makes: the process lock, then the boot
/// and the process's start on Linux, or the process and its status on macOS.
pub(super) const PROBES: u64 = 3;

/// What can be proven about the process a record names. Only a positive proof
/// of death is `Dead`; every probe that fails to answer is `Unknown`. Counts
/// each probe it makes in `probes`.
pub(super) fn liveness(process: &Process, probes: &mut u64, probe: super::LockProbe) -> Liveness {
    let Some(path) = bytes_path(&process.lock) else {
        return Liveness::Unknown;
    };
    *probes += 1;
    match File::open(&path) {
        Ok(lock) => match identity_bytes(&lock) {
            Ok(identity) if identity == process.lock_identity => {
                match probe(lock) {
                    // Its owner holds it exclusively for life.
                    Ok(_) => Liveness::Dead,
                    Err(error) if error.kind() == io::ErrorKind::WouldBlock => Liveness::Alive,
                    Err(_) => Liveness::Unknown,
                }
            }
            // Another file now has that name: the named one is gone.
            Ok(_) => without_lock(process, probes),
            Err(_) => Liveness::Unknown,
        },
        Err(error) if error.kind() == io::ErrorKind::NotFound => without_lock(process, probes),
        Err(_) => Liveness::Unknown,
    }
}

/// For a process lock that is gone: whether the process that held it lives.
/// Something other than its owner can remove it on Unix, such as a
/// temporary-file cleaner, so a missing lock alone proves nothing there.
fn without_lock(process: &Process, probes: &mut u64) -> Liveness {
    #[cfg(target_os = "linux")]
    {
        if process.start.is_empty() {
            return Liveness::Unknown;
        }
        // The boot, then the process.
        *probes += 2;
        match start_identity(process.pid) {
            Ok(start) if start == process.start => Liveness::Alive,
            // Another boot, or another process with a reused pid.
            Ok(_) => Liveness::Dead,
            Err(error) if error.kind() == io::ErrorKind::NotFound => Liveness::Dead,
            Err(_) => Liveness::Unknown,
        }
    }
    #[cfg(target_vendor = "apple")]
    {
        let Some(pid) = i32::try_from(process.pid)
            .ok()
            .and_then(rustix::process::Pid::from_raw)
        else {
            return Liveness::Unknown;
        };
        *probes += 1;
        match rustix::process::test_kill_process(pid) {
            Ok(()) | Err(rustix::io::Errno::PERM) => {
                *probes += 1;
                // A zombie awaiting its parent's wait holds no files. A reused
                // pid reads as alive: a refusal, never two owners.
                if zombie(pid) {
                    Liveness::Dead
                } else {
                    Liveness::Alive
                }
            }
            Err(rustix::io::Errno::SRCH) => Liveness::Dead,
            Err(_) => Liveness::Unknown,
        }
    }
    // The owner's lock denies removal and renaming while it is open, and a
    // sweep removes it only while holding its lock.
    #[cfg(windows)]
    {
        let _ = (process, probes);
        Liveness::Dead
    }
    #[cfg(not(any(target_os = "linux", target_vendor = "apple", windows)))]
    {
        let _ = (process, probes);
        Liveness::Unknown
    }
}

fn path_bytes(path: &Path) -> io::Result<Vec<u8>> {
    #[cfg(unix)]
    {
        use std::os::unix::ffi::OsStrExt as _;
        Ok(path.as_os_str().as_bytes().to_vec())
    }
    #[cfg(windows)]
    {
        use std::os::windows::ffi::OsStrExt as _;
        Ok(path
            .as_os_str()
            .encode_wide()
            .flat_map(u16::to_le_bytes)
            .collect())
    }
    #[cfg(not(any(unix, windows)))]
    {
        let _ = path;
        Err(io::Error::from(io::ErrorKind::Unsupported))
    }
}

pub(super) fn bytes_path(bytes: &[u8]) -> Option<PathBuf> {
    #[cfg(unix)]
    {
        use std::os::unix::ffi::OsStrExt as _;
        Some(PathBuf::from(std::ffi::OsStr::from_bytes(bytes)))
    }
    #[cfg(windows)]
    {
        use std::os::windows::ffi::OsStringExt as _;
        let (units, []) = bytes.as_chunks::<2>() else {
            return None;
        };
        let wide = units.iter().map(|unit| u16::from_le_bytes(*unit));
        Some(PathBuf::from(std::ffi::OsString::from_wide(
            &wide.collect::<Vec<_>>(),
        )))
    }
    #[cfg(not(any(unix, windows)))]
    {
        let _ = bytes;
        None
    }
}

/// The identity of a process lock, so a record never names a different file
/// that later took its path.
fn identity_bytes(file: &File) -> io::Result<Vec<u8>> {
    #[cfg(any(target_os = "linux", target_vendor = "apple"))]
    {
        let (device, inode) = crate::file_identity(file)?;
        Ok([device.to_le_bytes(), inode.to_le_bytes()].concat())
    }
    #[cfg(windows)]
    {
        let (volume, id) = crate::file_identity(file)?;
        Ok([&volume.to_le_bytes()[..], &id[..]].concat())
    }
    #[cfg(not(any(target_os = "linux", target_vendor = "apple", windows)))]
    {
        let _ = file;
        Err(io::Error::from(io::ErrorKind::Unsupported))
    }
}

/// Where this user's process locks live: private to the user, and outside any
/// store so that every process finds the lock a record names.
fn owner_directory() -> io::Result<PathBuf> {
    #[cfg(target_os = "linux")]
    let base = std::env::var_os("XDG_RUNTIME_DIR")
        .map(PathBuf::from)
        .filter(|path| path.is_absolute())
        .unwrap_or_else(std::env::temp_dir);
    #[cfg(not(target_os = "linux"))]
    let base = std::env::temp_dir();
    #[cfg(unix)]
    let directory = base.join(format!("acyclic-owners-{}", user_id()?));
    #[cfg(not(unix))]
    let directory = base.join("acyclic-owners");
    let mut builder = std::fs::DirBuilder::new();
    builder.recursive(true);
    #[cfg(unix)]
    {
        use std::os::unix::fs::{DirBuilderExt as _, MetadataExt as _};
        builder.mode(0o700);
        builder.create(&directory)?;
        // A shared temporary directory lets another user create this name
        // first; only a private directory of this user's is trusted.
        let metadata = std::fs::symlink_metadata(&directory)?;
        if !metadata.is_dir() || metadata.uid() != user_id()? || metadata.mode() & 0o077 != 0 {
            return Err(io::Error::from(io::ErrorKind::PermissionDenied));
        }
    }
    #[cfg(not(unix))]
    builder.create(&directory)?;
    Ok(directory)
}

#[cfg(unix)]
fn user_id() -> io::Result<u32> {
    #[cfg(target_os = "linux")]
    {
        use std::os::unix::fs::MetadataExt as _;
        Ok(std::fs::metadata("/proc/self")?.uid())
    }
    #[cfg(target_vendor = "apple")]
    return Ok(rustix::process::getuid().as_raw());
    #[cfg(not(any(target_os = "linux", target_vendor = "apple")))]
    Err(io::Error::from(io::ErrorKind::Unsupported))
}

/// Removes the process locks of processes that have died. A lock is removed
/// only while held here, so a removed lock always belonged to a dead process.
/// The hold is shared, so a probe of the same dead owner meanwhile still
/// proves its death instead of finding the lock held.
fn sweep(directory: &Path) {
    let Ok(entries) = std::fs::read_dir(directory) else {
        return;
    };
    for entry in entries.flatten() {
        let path = entry.path();
        if path.extension().is_none_or(|extension| extension != "lock") {
            continue;
        }
        if let Ok(lock) = File::open(&path)
            && let Ok(_held) = FileLock::try_acquire(lock, FileLockMode::Shared)
        {
            let _ = std::fs::remove_file(&path);
        }
    }
}

/// A Linux process's identity across pid reuse: the boot and its start tick.
/// `NotFound` only when the boot is known and the process is not, or has
/// exited: a zombie awaiting its parent's wait holds no files, so it owns
/// nothing. Reads into fixed buffers, so a probe allocates only the identity
/// it returns.
#[cfg(target_os = "linux")]
fn start_identity(pid: u32) -> io::Result<Vec<u8>> {
    use std::io::Write as _;
    use std::os::unix::ffi::OsStrExt as _;
    let malformed = || io::Error::from(io::ErrorKind::InvalidData);
    // Read first, so a missing /proc is never mistaken for a missing process.
    let mut boot = [0; 64];
    let boot =
        read_bounded(Path::new("/proc/sys/kernel/random/boot_id"), &mut boot).map_err(|error| {
            match error.kind() {
                io::ErrorKind::NotFound => io::Error::from(io::ErrorKind::Unsupported),
                _ => error,
            }
        })?;
    let mut name = [0; 32];
    let mut cursor = io::Cursor::new(&mut name[..]);
    write!(cursor, "/proc/{pid}/stat")?;
    let length = usize::try_from(cursor.position()).map_err(|_| malformed())?;
    let name = name.get(..length).ok_or_else(malformed)?;
    let mut stat = [0; 4096];
    let stat = read_bounded(Path::new(std::ffi::OsStr::from_bytes(name)), &mut stat)?;
    let stat = std::str::from_utf8(stat).map_err(|_| malformed())?;
    // Fields are counted after the parenthesized command name, which may
    // itself contain spaces.
    let (_, fields) = stat.rsplit_once(')').ok_or_else(malformed)?;
    let mut fields = fields.split_whitespace();
    // Field 3, the state, and field 20, the threads not yet released.
    let state = fields.next().ok_or_else(malformed)?;
    let threads = fields.nth(16).ok_or_else(malformed)?;
    // Exited, or a zombie whose threads have all exited. A leader that exited
    // before its other threads also reads as a zombie, but they live on.
    if state == "X" || state == "x" || (state == "Z" && threads == "1") {
        return Err(io::Error::from(io::ErrorKind::NotFound));
    }
    // Field 22, the start tick.
    let start = fields.nth(1).ok_or_else(malformed)?;
    let boot = std::str::from_utf8(boot).map_err(|_| malformed())?.trim();
    let mut identity = Vec::with_capacity(boot.len() + 1 + start.len());
    identity.extend_from_slice(boot.as_bytes());
    identity.push(b':');
    identity.extend_from_slice(start.as_bytes());
    Ok(identity)
}

/// Whether process `pid` has exited and awaits its parent's wait. `false`
/// whenever its status cannot be read.
#[cfg(target_vendor = "apple")]
#[allow(unsafe_code, reason = "FFI: proc_pidinfo, which rustix does not wrap")]
fn zombie(pid: rustix::process::Pid) -> bool {
    /// Asks for zombies too: without it the kernel looks up only live
    /// processes, and a zombie fails with ESRCH as if its status were unknown.
    const FIND_ZOMBIES: u64 = 1;
    let Ok(size) = libc::c_int::try_from(std::mem::size_of::<libc::proc_bsdinfo>()) else {
        return false;
    };
    let mut info = std::mem::MaybeUninit::<libc::proc_bsdinfo>::zeroed();
    // SAFETY: proc_pidinfo writes at most `size` bytes, the size of `info`.
    let written = unsafe {
        libc::proc_pidinfo(
            pid.as_raw_nonzero().get(),
            libc::PROC_PIDTBSDINFO,
            FIND_ZOMBIES,
            info.as_mut_ptr().cast(),
            size,
        )
    };
    if written != size {
        return false;
    }
    // SAFETY: zeroed, then wholly written: every field is an integer or an
    // integer array, valid for any bytes.
    let info = unsafe { info.assume_init() };
    info.pbi_status == libc::SZOMB
}

/// The whole of the file at `path`, which must fit `buffer`.
#[cfg(target_os = "linux")]
fn read_bounded<'a>(path: &Path, buffer: &'a mut [u8]) -> io::Result<&'a [u8]> {
    use std::io::Read as _;
    let mut file = File::open(path)?;
    let mut filled = 0;
    loop {
        let Some(rest) = buffer.get_mut(filled..) else {
            return Err(io::Error::from(io::ErrorKind::InvalidData));
        };
        if rest.is_empty() {
            return Err(io::Error::from(io::ErrorKind::InvalidData));
        }
        match file.read(rest) {
            Ok(0) => break,
            Ok(read) => filled += read,
            Err(error) if error.kind() == io::ErrorKind::Interrupted => {}
            Err(error) => return Err(error),
        }
    }
    buffer
        .get(..filled)
        .ok_or_else(|| io::Error::from(io::ErrorKind::InvalidData))
}

#[cfg(not(target_os = "linux"))]
fn start_identity(_: u32) -> io::Result<Vec<u8>> {
    Err(io::Error::from(io::ErrorKind::Unsupported))
}
