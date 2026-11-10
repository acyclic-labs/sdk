//! This process's one process lock, and what a record proves about the
//! process it names.

use super::protocol::Liveness;
use super::record::Process;
use crate::meter::read_operation;
use crate::{FileLock, FileLockMode};
use crate::{NativeReadMeter, ReadAllocation};
use std::fs::{File, OpenOptions};
use std::io;
use std::path::{Path, PathBuf};
use std::sync::Arc;
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
            _ => return Err(io::Error::from(io::ErrorKind::Unsupported).into()),
        }
        let process = Process {
            pid,
            start: start_identity(pid, None)
                .map(|identity| identity.bytes)
                .unwrap_or_default(),
            lock: path_bytes(&path)?,
            lock_identity: identity_bytes(&lock, None)
                .map_err(ProbeError::into_io)?
                .bytes,
        };
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
pub(super) fn liveness(
    process: &Process<&[u8]>,
    probes: &mut u64,
    probe: super::LockProbe,
    meter: Option<&Arc<dyn NativeReadMeter>>,
) -> io::Result<Liveness> {
    *probes += 1;
    match crate::namespace::open_read_encoded(process.lock, meter)? {
        Ok(lock) => match identity_bytes(&lock, meter) {
            Ok(identity) if identity.bytes.as_slice() == process.lock_identity => {
                Ok(match read_operation(meter, || probe(lock))? {
                    Ok(_) => Liveness::Dead,
                    Err(error) if error.kind() == io::ErrorKind::WouldBlock => Liveness::Alive,
                    Err(_) => Liveness::Unknown,
                })
            }
            Ok(_) => without_lock(process, probes, meter),
            Err(ProbeError::Io(_)) => Ok(Liveness::Unknown),
            Err(ProbeError::Work(error)) => Err(error),
        },
        Err(error) if error.kind() == io::ErrorKind::NotFound => {
            without_lock(process, probes, meter)
        }
        Err(_) => Ok(Liveness::Unknown),
    }
}

/// A missing lock proves death only through the platform's original identity
/// and lifecycle checks. Admission failure never becomes an unknown OS probe.
fn without_lock(
    process: &Process<&[u8]>,
    probes: &mut u64,
    meter: Option<&Arc<dyn NativeReadMeter>>,
) -> io::Result<Liveness> {
    #[cfg(target_os = "linux")]
    {
        if process.start.is_empty() {
            return Ok(Liveness::Unknown);
        }
        *probes += 2;
        match start_identity(process.pid, meter) {
            Ok(start) if start.bytes.as_slice() == process.start => Ok(Liveness::Alive),
            Ok(_) => Ok(Liveness::Dead),
            Err(ProbeError::Io(error)) if error.kind() == io::ErrorKind::NotFound => {
                Ok(Liveness::Dead)
            }
            Err(ProbeError::Io(_)) => Ok(Liveness::Unknown),
            Err(ProbeError::Work(error)) => Err(error),
        }
    }
    #[cfg(target_vendor = "apple")]
    {
        let Some(pid) = i32::try_from(process.pid)
            .ok()
            .and_then(rustix::process::Pid::from_raw)
        else {
            return Ok(Liveness::Unknown);
        };
        *probes += 1;
        let present = match read_operation(meter, || {
            rustix::process::test_kill_process(pid).map_err(io::Error::from)
        })? {
            Ok(()) => true,
            Err(error) if error.raw_os_error() == Some(rustix::io::Errno::PERM.raw_os_error()) => {
                true
            }
            Err(error) if error.raw_os_error() == Some(rustix::io::Errno::SRCH.raw_os_error()) => {
                return Ok(Liveness::Dead);
            }
            Err(_) => false,
        };
        if !present {
            return Ok(Liveness::Unknown);
        }
        *probes += 1;
        Ok(if read_operation(meter, || Ok(zombie(pid)))?? {
            Liveness::Dead
        } else {
            Liveness::Alive
        })
    }
    #[cfg(windows)]
    {
        // The lock's owner denied removal/renaming for its whole lifetime.
        let _ = (process, probes, meter);
        Ok(Liveness::Dead)
    }
    #[cfg(not(any(target_os = "linux", target_vendor = "apple", windows)))]
    {
        let _ = (process, probes, meter);
        Ok(Liveness::Unknown)
    }
}

/// Separates a native observation from refusal on the original work. Inline
/// variants preserve the exact error without allocating an error wrapper.
enum ProbeError {
    Io(io::Error),
    Work(io::Error),
}
impl From<io::Error> for ProbeError {
    fn from(error: io::Error) -> Self {
        Self::Io(error)
    }
}
impl ProbeError {
    fn into_io(self) -> io::Error {
        match self {
            Self::Io(error) | Self::Work(error) => error,
        }
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
        Err(io::Error::from(io::ErrorKind::Unsupported).into())
    }
}

#[cfg(test)]
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

struct Identity {
    bytes: Vec<u8>,
    _allocation: Option<ReadAllocation>,
}

impl Identity {
    fn copy(parts: &[&[u8]], meter: Option<&Arc<dyn NativeReadMeter>>) -> io::Result<Self> {
        let size = parts.iter().try_fold(0_usize, |size, part| {
            size.checked_add(part.len())
                .ok_or(io::ErrorKind::InvalidInput)
        })?;
        let mut value = Self {
            bytes: Vec::new(),
            _allocation: meter
                .map(|meter| ReadAllocation::new(meter, size))
                .transpose()?,
        };
        value
            .bytes
            .try_reserve_exact(size)
            .map_err(|error| match meter {
                Some(meter) => meter.allocation_failed(error),
                None => io::ErrorKind::OutOfMemory.into(),
            })?;
        if let Some(allocation) = &mut value._allocation {
            allocation.reconcile_capacity(value.bytes.capacity())?;
        }
        if let Some(meter) = meter {
            meter.admit_codec(0, size as u64, 0)?;
        }
        for part in parts {
            value.bytes.extend_from_slice(part);
        }
        if let Some(meter) = meter {
            meter.record_codec(0, size as u64, 0)?;
        }
        Ok(value)
    }
}

/// The identity of a process lock, so a record never names a different file
/// that later took its path.
fn identity_bytes(
    file: &File,
    meter: Option<&Arc<dyn NativeReadMeter>>,
) -> Result<Identity, ProbeError> {
    #[cfg(any(target_os = "linux", target_vendor = "apple"))]
    {
        let (device, inode) =
            read_operation(meter, || crate::file_identity(file)).map_err(ProbeError::Work)??;
        Identity::copy(&[&device.to_le_bytes(), &inode.to_le_bytes()], meter)
            .map_err(ProbeError::Work)
    }
    #[cfg(windows)]
    {
        let (volume, id) =
            read_operation(meter, || crate::file_identity(file)).map_err(ProbeError::Work)??;
        Identity::copy(&[&volume.to_le_bytes(), &id], meter).map_err(ProbeError::Work)
    }
    #[cfg(not(any(target_os = "linux", target_vendor = "apple", windows)))]
    {
        let _ = (file, meter);
        Err(io::Error::from(io::ErrorKind::Unsupported).into())
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
    Err(io::Error::from(io::ErrorKind::Unsupported).into())
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
fn start_identity(
    pid: u32,
    meter: Option<&Arc<dyn NativeReadMeter>>,
) -> Result<Identity, ProbeError> {
    use std::io::Write as _;
    use std::os::unix::ffi::OsStrExt as _;
    let malformed = || io::Error::from(io::ErrorKind::InvalidData);
    // Read first, so a missing /proc is never mistaken for a missing process.
    let mut boot = [0; 64];
    let boot = read_bounded(
        Path::new("/proc/sys/kernel/random/boot_id"),
        &mut boot,
        meter,
    )
    .map_err(|error| match error {
        ProbeError::Io(error) if error.kind() == io::ErrorKind::NotFound => {
            ProbeError::Io(io::ErrorKind::Unsupported.into())
        }
        error => error,
    })?;
    let mut name = [0; 32];
    let mut cursor = io::Cursor::new(&mut name[..]);
    write!(cursor, "/proc/{pid}/stat")?;
    let length = usize::try_from(cursor.position()).map_err(|_| malformed())?;
    let name = name.get(..length).ok_or_else(malformed)?;
    let mut stat = [0; 4096];
    let stat = read_bounded(
        Path::new(std::ffi::OsStr::from_bytes(name)),
        &mut stat,
        meter,
    )?;
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
        return Err(io::Error::from(io::ErrorKind::NotFound).into());
    }
    // Field 22, the start tick.
    let start = fields.nth(1).ok_or_else(malformed)?;
    let boot = std::str::from_utf8(boot).map_err(|_| malformed())?.trim();
    Identity::copy(&[boot.as_bytes(), b":", start.as_bytes()], meter).map_err(ProbeError::Work)
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
fn read_bounded<'a>(
    path: &Path,
    buffer: &'a mut [u8],
    meter: Option<&Arc<dyn NativeReadMeter>>,
) -> Result<&'a [u8], ProbeError> {
    use std::io::Read as _;
    use std::os::unix::ffi::OsStrExt as _;
    let mut file = crate::namespace::open_read_encoded(path.as_os_str().as_bytes(), meter)
        .map_err(ProbeError::Work)??;
    let mut filled = 0;
    loop {
        let Some(rest) = buffer.get_mut(filled..) else {
            return Err(io::Error::from(io::ErrorKind::InvalidData).into());
        };
        if rest.is_empty() {
            return Err(io::Error::from(io::ErrorKind::InvalidData).into());
        }
        let mut bytes = meter
            .map(|meter| {
                crate::NativeByteReservation::new(
                    meter.clone(),
                    crate::NativeOperationKind::Read,
                    rest.len(),
                )
            })
            .transpose()
            .map_err(ProbeError::Work)?;
        let read = read_operation(meter, || file.read(rest)).map_err(ProbeError::Work)?;
        if let Some(bytes) = &mut bytes {
            bytes
                .record(read.as_ref().copied().unwrap_or(0))
                .map_err(ProbeError::Work)?;
        }
        match read {
            Ok(0) => break,
            Ok(read) => filled += read,
            Err(error) => return Err(error.into()),
        }
    }
    buffer
        .get(..filled)
        .ok_or_else(|| io::Error::from(io::ErrorKind::InvalidData).into())
}

#[cfg(not(target_os = "linux"))]
fn start_identity(_: u32, _: Option<&Arc<dyn NativeReadMeter>>) -> Result<Identity, ProbeError> {
    Err(io::Error::from(io::ErrorKind::Unsupported).into())
}
