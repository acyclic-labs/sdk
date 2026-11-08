//! What this process holds from the OS, read from the OS itself on every
//! platform, so per-agent cost is measured, not assumed, wherever it runs.
use std::io;
use std::path::{Path, PathBuf};

/// Point-in-time process-wide OS resource readings.
///
/// Fields are read separately and can reflect different instants. Resource
/// lifetimes between reads require accounting at their admission and release
/// events to establish a high-water mark.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct ProcessResources {
    /// Memory the process holds: resident bytes on Linux and macOS, private
    /// committed bytes on Windows.
    pub memory_bytes: u64,
    /// Threads of this process.
    pub threads: u64,
    /// Open descriptors on Unix; open kernel handles of every kind on
    /// Windows.
    pub descriptors: u64,
    /// Kernel watches beyond their descriptors: inotify watches on Linux.
    /// macOS `kqueue` and `FSEvents` and Windows directory watches each hold a
    /// descriptor or handle, which `descriptors` already counts.
    pub watches: u64,
    /// CPU time used by the process, user and system, in hundredths of a
    /// second.
    pub cpu_centiseconds: u64,
}

/// One open descriptor or handle of this process.
#[derive(Clone, Debug, Eq, Ord, PartialEq, PartialOrd)]
pub enum Descriptor {
    /// A file or directory, by its path.
    File(PathBuf),
    /// Anything else, by its kind: a socket, pipe, event, thread or port.
    Other(String),
}

/// Reads this process's resources from one platform's OS.
trait Sampler {
    /// Reads every counter but `watches` on demand.
    fn sample() -> io::Result<ProcessResources>;
    /// Watches beyond their descriptors.
    fn watches() -> io::Result<u64>;
    /// This process's open descriptors or handles.
    fn descriptors() -> io::Result<Vec<Descriptor>>;
    /// Every mount point this process sees, in the form `canonicalize`
    /// returns.
    fn mount_points() -> io::Result<Vec<PathBuf>>;
}

#[cfg(target_os = "linux")]
type Native = linux::Procfs;
#[cfg(target_vendor = "apple")]
type Native = apple::Libproc;
#[cfg(windows)]
type Native = windows::NtProcess;

/// Reads every counter but `watches` on demand.
pub fn sample_process_resources() -> io::Result<ProcessResources> {
    Native::sample()
}

/// Reads every counter on demand, including `watches`.
pub fn process_resources() -> io::Result<ProcessResources> {
    Ok(ProcessResources {
        watches: Native::watches()?,
        ..Native::sample()?
    })
}

/// How many of this process's descriptors or handles are open on the file at
/// `path`.
pub fn descriptors_naming(path: &Path) -> io::Result<usize> {
    let path = Descriptor::File(std::fs::canonicalize(path)?);
    Ok(Native::descriptors()?
        .into_iter()
        .filter(|descriptor| *descriptor == path)
        .count())
}

/// Every descriptor or handle this process holds.
pub fn descriptors() -> io::Result<Vec<Descriptor>> {
    Native::descriptors()
}

/// How many filesystems are mounted at `root` or beneath it.
///
/// The mount table is shared: a Linux mount namespace with every process in
/// it, and the whole system on macOS and Windows. Its total changes whenever
/// any other process mounts, so only the mounts beneath a root its owner
/// serves are that owner's cost.
pub fn mounts_beneath(root: &Path) -> io::Result<u64> {
    let root = std::fs::canonicalize(root)?;
    Ok(Native::mount_points()?
        .iter()
        .filter(|point| point.starts_with(&root))
        .count() as u64)
}

#[cfg(target_os = "linux")]
mod linux {
    use super::{Descriptor, ProcessResources, Sampler};
    use std::ffi::OsString;
    use std::io;
    use std::os::unix::ffi::OsStringExt as _;
    use std::path::PathBuf;

    /// A mountinfo path field, whose space, tab, newline and backslash bytes
    /// are written as three octal digits after a backslash.
    pub(super) fn unescape(field: &[u8]) -> PathBuf {
        let mut path = Vec::with_capacity(field.len());
        let mut rest = field;
        loop {
            rest = match rest {
                [
                    b'\\',
                    high @ b'0'..=b'3',
                    middle @ b'0'..=b'7',
                    low @ b'0'..=b'7',
                    after @ ..,
                ] => {
                    path.push(((high - b'0') << 6) | ((middle - b'0') << 3) | (low - b'0'));
                    after
                }
                [byte, after @ ..] => {
                    path.push(*byte);
                    after
                }
                [] => break,
            };
        }
        PathBuf::from(OsString::from_vec(path))
    }

    pub(super) struct Procfs;

    impl Sampler for Procfs {
        fn sample() -> io::Result<ProcessResources> {
            let status = std::fs::read_to_string("/proc/self/status")?;
            let field = |name: &str| {
                status
                    .lines()
                    .find_map(|line| line.strip_prefix(name))
                    .and_then(|rest| rest.split_whitespace().next()?.parse::<u64>().ok())
                    .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidData, name.to_owned()))
            };
            let stat = std::fs::read_to_string("/proc/self/stat")?;
            // utime and stime, in clock ticks of 1/100 s.
            let cpu = stat.rsplit(')').next().map_or(0, |rest| {
                rest.split_whitespace()
                    .skip(11)
                    .take(2)
                    .filter_map(|tick| tick.parse::<u64>().ok())
                    .sum()
            });
            Ok(ProcessResources {
                memory_bytes: field("VmRSS:")? * 1024,
                threads: field("Threads:")?,
                descriptors: std::fs::read_dir("/proc/self/fd")?.count() as u64,
                watches: 0,
                cpu_centiseconds: cpu,
            })
        }

        fn watches() -> io::Result<u64> {
            Ok(std::fs::read_dir("/proc/self/fdinfo")?
                .filter_map(|entry| std::fs::read_to_string(entry.ok()?.path()).ok())
                .map(|info| {
                    info.lines()
                        .filter(|line| line.starts_with("inotify"))
                        .count() as u64
                })
                .sum())
        }

        fn descriptors() -> io::Result<Vec<Descriptor>> {
            let mut descriptors = Vec::new();
            for entry in std::fs::read_dir("/proc/self/fd")? {
                // A descriptor closed since the listing has no target.
                let Ok(target) = std::fs::read_link(entry?.path()) else {
                    continue;
                };
                descriptors.push(if target.is_absolute() {
                    Descriptor::File(target)
                } else {
                    Descriptor::Other(target.display().to_string())
                });
            }
            Ok(descriptors)
        }

        fn mount_points() -> io::Result<Vec<PathBuf>> {
            // The fifth field of each mountinfo row is its mount point.
            Ok(std::fs::read("/proc/self/mountinfo")?
                .split(|byte| *byte == b'\n')
                .filter_map(|row| row.split(|byte| *byte == b' ').nth(4))
                .map(unescape)
                .collect())
        }
    }
}

#[cfg(target_vendor = "apple")]
#[allow(
    unsafe_code,
    reason = "FFI: proc_pidinfo, fcntl(F_GETPATH), getfsstat and getrusage"
)]
mod apple {
    use super::{Descriptor, ProcessResources, Sampler};
    use std::ffi::CStr;
    use std::io;
    use std::os::unix::ffi::OsStrExt as _;
    use std::path::PathBuf;

    pub(super) struct Libproc;

    /// `MAXPATHLEN` in `<sys/param.h>`: the most `F_GETPATH` writes.
    const MAXPATHLEN: usize = 1024;

    fn pid() -> io::Result<libc::c_int> {
        libc::c_int::try_from(std::process::id())
            .map_err(|_| io::Error::from(io::ErrorKind::InvalidData))
    }

    fn size_of<T>() -> io::Result<libc::c_int> {
        libc::c_int::try_from(std::mem::size_of::<T>())
            .map_err(|_| io::Error::from(io::ErrorKind::InvalidInput))
    }

    /// This process's open descriptors.
    fn descriptors() -> io::Result<Vec<libc::proc_fdinfo>> {
        let pid = pid()?;
        let entry = size_of::<libc::proc_fdinfo>()?;
        // SAFETY: a null buffer of size 0 only asks for the size needed.
        let needed =
            unsafe { libc::proc_pidinfo(pid, libc::PROC_PIDLISTFDS, 0, std::ptr::null_mut(), 0) };
        if needed < 0 {
            return Err(io::Error::last_os_error());
        }
        // Room for descriptors opened since, so the list is never cut short.
        let capacity = usize::try_from(needed / entry).unwrap_or(0) + 64;
        let mut entries = Vec::<libc::proc_fdinfo>::with_capacity(capacity);
        let bytes = libc::c_int::try_from(capacity * std::mem::size_of::<libc::proc_fdinfo>())
            .map_err(|_| io::Error::from(io::ErrorKind::InvalidInput))?;
        // SAFETY: proc_pidinfo writes at most `bytes` bytes, the capacity.
        let written = unsafe {
            libc::proc_pidinfo(
                pid,
                libc::PROC_PIDLISTFDS,
                0,
                entries.as_mut_ptr().cast(),
                bytes,
            )
        };
        if written < 0 {
            return Err(io::Error::last_os_error());
        }
        let count = usize::try_from(written / entry).unwrap_or(0).min(capacity);
        // SAFETY: the kernel initialized `count` whole entries, all plain
        // integers.
        unsafe { entries.set_len(count) };
        Ok(entries)
    }

    impl Sampler for Libproc {
        fn sample() -> io::Result<ProcessResources> {
            let pid = pid()?;
            let size = size_of::<libc::proc_taskinfo>()?;
            let mut task = std::mem::MaybeUninit::<libc::proc_taskinfo>::zeroed();
            // SAFETY: proc_pidinfo writes at most `size` bytes, the size of
            // `task`.
            let written = unsafe {
                libc::proc_pidinfo(
                    pid,
                    libc::PROC_PIDTASKINFO,
                    0,
                    task.as_mut_ptr().cast(),
                    size,
                )
            };
            if written != size {
                return Err(io::Error::last_os_error());
            }
            // SAFETY: zeroed, then wholly written; every field is an integer.
            let task = unsafe { task.assume_init() };
            let mut usage = std::mem::MaybeUninit::<libc::rusage>::zeroed();
            // SAFETY: getrusage writes one rusage into `usage`.
            if unsafe { libc::getrusage(libc::RUSAGE_SELF, usage.as_mut_ptr()) } != 0 {
                return Err(io::Error::last_os_error());
            }
            // SAFETY: zeroed, then written; every field is an integer.
            let usage = unsafe { usage.assume_init() };
            let centiseconds = |time: libc::timeval| {
                u64::try_from(time.tv_sec).unwrap_or(0) * 100
                    + u64::try_from(time.tv_usec).unwrap_or(0) / 10_000
            };
            Ok(ProcessResources {
                memory_bytes: task.pti_resident_size,
                threads: u64::try_from(task.pti_threadnum).unwrap_or(0),
                descriptors: descriptors()?.len() as u64,
                watches: 0,
                cpu_centiseconds: centiseconds(usage.ru_utime) + centiseconds(usage.ru_stime),
            })
        }

        fn watches() -> io::Result<u64> {
            Ok(0)
        }

        fn descriptors() -> io::Result<Vec<Descriptor>> {
            let mut named = Vec::new();
            for descriptor in descriptors()? {
                if u32::try_from(libc::PROX_FDTYPE_VNODE).ok() != Some(descriptor.proc_fdtype) {
                    named.push(Descriptor::Other(format!(
                        "fdtype {}",
                        descriptor.proc_fdtype
                    )));
                    continue;
                }
                let mut buffer: [libc::c_char; MAXPATHLEN] = [0; MAXPATHLEN];
                // SAFETY: F_GETPATH writes a terminated path of at most
                // MAXPATHLEN bytes into `buffer`.
                let got = unsafe {
                    libc::fcntl(descriptor.proc_fd, libc::F_GETPATH, buffer.as_mut_ptr())
                };
                if got == -1 {
                    // Closed since the list was read.
                    continue;
                }
                // SAFETY: F_GETPATH terminated the path within `buffer`.
                let path = unsafe { CStr::from_ptr(buffer.as_ptr()) };
                named.push(Descriptor::File(PathBuf::from(
                    std::ffi::OsStr::from_bytes(path.to_bytes()),
                )));
            }
            Ok(named)
        }

        fn mount_points() -> io::Result<Vec<PathBuf>> {
            // SAFETY: a null buffer only asks for the number of mounts.
            let mounted = unsafe { libc::getfsstat(std::ptr::null_mut(), 0, libc::MNT_NOWAIT) };
            // Room for mounts added since, so the table is never cut short.
            let capacity = usize::try_from(mounted).map_err(|_| io::Error::last_os_error())? + 16;
            let mut table = Vec::<libc::statfs>::with_capacity(capacity);
            let bytes = libc::c_int::try_from(capacity * std::mem::size_of::<libc::statfs>())
                .map_err(|_| io::Error::from(io::ErrorKind::InvalidInput))?;
            // SAFETY: getfsstat writes at most `bytes` bytes, the capacity.
            let written = unsafe { libc::getfsstat(table.as_mut_ptr(), bytes, libc::MNT_NOWAIT) };
            let count = usize::try_from(written).map_err(|_| io::Error::last_os_error())?;
            // SAFETY: the kernel initialized `count` whole entries.
            unsafe { table.set_len(count.min(capacity)) };
            Ok(table
                .iter()
                .map(|entry| {
                    // SAFETY: the kernel terminates `f_mntonname` within it.
                    let point = unsafe { CStr::from_ptr(entry.f_mntonname.as_ptr()) };
                    PathBuf::from(std::ffi::OsStr::from_bytes(point.to_bytes()))
                })
                .collect())
        }
    }
}

#[cfg(windows)]
#[allow(
    unsafe_code,
    reason = "FFI: process counters, the handle snapshot, handles' final paths and volume mount points"
)]
mod windows {
    use super::{Descriptor, ProcessResources, Sampler};
    use std::io;
    use std::os::windows::ffi::OsStringExt as _;
    use std::path::PathBuf;
    use windows_sys::Wdk::Foundation::{NtQueryObject, ObjectTypeInformation};
    use windows_sys::Wdk::System::Threading::{
        NtQueryInformationProcess, ProcessHandleInformation,
    };
    use windows_sys::Win32::Foundation::{
        CloseHandle, ERROR_MORE_DATA, ERROR_NO_MORE_FILES, FILETIME, HANDLE, INVALID_HANDLE_VALUE,
        STATUS_INFO_LENGTH_MISMATCH,
    };
    use windows_sys::Win32::Storage::FileSystem::{
        FILE_TYPE_DISK, FindFirstVolumeW, FindNextVolumeW, FindVolumeClose, GetFileType,
        GetFinalPathNameByHandleW, GetVolumePathNamesForVolumeNameW, VOLUME_NAME_DOS,
    };
    use windows_sys::Win32::System::Diagnostics::ToolHelp::{
        CreateToolhelp32Snapshot, TH32CS_SNAPTHREAD, THREADENTRY32, Thread32First, Thread32Next,
    };
    use windows_sys::Win32::System::ProcessStatus::{
        GetProcessMemoryInfo, PROCESS_MEMORY_COUNTERS, PROCESS_MEMORY_COUNTERS_EX,
    };
    use windows_sys::Win32::System::Threading::{
        GetCurrentProcess, GetCurrentProcessId, GetProcessHandleCount, GetProcessTimes,
    };

    pub(super) struct NtProcess;

    /// `PROCESS_HANDLE_SNAPSHOT_INFORMATION`'s header, before its entries.
    #[repr(C)]
    struct SnapshotHeader {
        count: usize,
        _reserved: usize,
    }

    /// `PROCESS_HANDLE_TABLE_ENTRY_INFO`.
    #[repr(C)]
    #[derive(Clone, Copy)]
    struct SnapshotEntry {
        handle: HANDLE,
        _handle_count: usize,
        _pointer_count: usize,
        _granted_access: u32,
        _object_type_index: u32,
        _handle_attributes: u32,
        _reserved: u32,
    }

    /// Every handle this process holds.
    fn handles() -> io::Result<Vec<HANDLE>> {
        // Whole words keep the entries aligned.
        let mut buffer = vec![0_u64; 8 * 1024];
        loop {
            let length = u32::try_from(buffer.len() * 8)
                .map_err(|_| io::Error::from(io::ErrorKind::OutOfMemory))?;
            let mut needed = 0_u32;
            // SAFETY: the kernel writes at most `length` bytes into `buffer`.
            let status = unsafe {
                NtQueryInformationProcess(
                    GetCurrentProcess(),
                    ProcessHandleInformation,
                    buffer.as_mut_ptr().cast(),
                    length,
                    &mut needed,
                )
            };
            if status == STATUS_INFO_LENGTH_MISMATCH {
                let words = (needed as usize).div_ceil(8).max(buffer.len() * 2);
                buffer = vec![0_u64; words];
                continue;
            }
            if status < 0 {
                return Err(io::Error::other(format!(
                    "handle snapshot failed: {status:#x}"
                )));
            }
            let header_bytes = std::mem::size_of::<SnapshotHeader>();
            let entry_bytes = std::mem::size_of::<SnapshotEntry>();
            // SAFETY: the buffer is word-aligned and begins with the header.
            let count = unsafe { buffer.as_ptr().cast::<SnapshotHeader>().read() }.count;
            let fits = (buffer.len() * 8).saturating_sub(header_bytes) / entry_bytes;
            let base = buffer.as_ptr().cast::<u8>();
            return Ok((0..count.min(fits))
                .map(|index| {
                    // SAFETY: entry `index` lies wholly within the buffer, at
                    // an offset aligned for it.
                    unsafe {
                        base.add(header_bytes + index * entry_bytes)
                            .cast::<SnapshotEntry>()
                            .read()
                    }
                    .handle
                })
                .collect());
        }
    }

    /// The final path of the file `handle` opens, if it opens a disk file.
    fn final_path(handle: HANDLE) -> Option<PathBuf> {
        // SAFETY: GetFileType only queries the handle; it answers unknown for
        // handles that are not files and never blocks on pipes.
        if unsafe { GetFileType(handle) } != FILE_TYPE_DISK {
            return None;
        }
        let mut buffer = vec![0_u16; 1024];
        loop {
            let capacity = u32::try_from(buffer.len()).ok()?;
            // SAFETY: writes at most `capacity` UTF-16 units into `buffer`.
            let written = unsafe {
                GetFinalPathNameByHandleW(handle, buffer.as_mut_ptr(), capacity, VOLUME_NAME_DOS)
            };
            let written = written as usize;
            if written == 0 {
                return None;
            }
            if written < buffer.len() {
                buffer.truncate(written);
                return Some(PathBuf::from(std::ffi::OsString::from_wide(&buffer)));
            }
            buffer = vec![0; written + 1];
        }
    }

    /// The object type of `handle`, such as `Event`, `Thread` or
    /// `IoCompletion`.
    fn kind(handle: HANDLE) -> String {
        // PUBLIC_OBJECT_TYPE_INFORMATION: a UNICODE_STRING naming the type,
        // whose characters follow it in the same buffer. Whole words keep it
        // aligned.
        let mut buffer = [0_u64; 128];
        let length = u32::try_from(std::mem::size_of_val(&buffer)).unwrap_or(0);
        // SAFETY: the kernel writes at most `length` bytes into `buffer`.
        let status = unsafe {
            NtQueryObject(
                handle,
                ObjectTypeInformation,
                buffer.as_mut_ptr().cast(),
                length,
                std::ptr::null_mut(),
            )
        };
        if status < 0 {
            return format!("unknown {status:#x}");
        }
        // UNICODE_STRING: a u16 byte length, a u16 capacity, then a pointer.
        let bytes = usize::from(u16::try_from(buffer[0] & 0xffff).unwrap_or(0));
        let start = usize::try_from(buffer[1]).unwrap_or(0);
        let base = buffer.as_ptr() as usize;
        // SAFETY: plain integers, viewed as the UTF-16 units they hold.
        let units = unsafe {
            std::slice::from_raw_parts(buffer.as_ptr().cast::<u16>(), size_of_val(&buffer) / 2)
        };
        // The name must lie within `buffer`.
        start
            .checked_sub(base)
            .map(|offset| offset / 2)
            .and_then(|offset| units.get(offset..offset + bytes / 2))
            .map_or_else(|| "unknown".to_owned(), String::from_utf16_lossy)
    }

    fn threads() -> io::Result<u64> {
        // SAFETY: a snapshot of every thread, closed below.
        let snapshot = unsafe { CreateToolhelp32Snapshot(TH32CS_SNAPTHREAD, 0) };
        if snapshot == INVALID_HANDLE_VALUE {
            return Err(io::Error::last_os_error());
        }
        // SAFETY: plain data; dwSize names its size, as Thread32First needs.
        let mut entry: THREADENTRY32 = unsafe { std::mem::zeroed() };
        entry.dwSize = u32::try_from(std::mem::size_of::<THREADENTRY32>()).unwrap_or(u32::MAX);
        // SAFETY: GetCurrentProcessId has no preconditions.
        let process = unsafe { GetCurrentProcessId() };
        let mut threads = 0;
        // SAFETY: each call writes one entry into `entry` from the open
        // snapshot.
        let mut more = unsafe { Thread32First(snapshot, &mut entry) } != 0;
        while more {
            if entry.th32OwnerProcessID == process {
                threads += 1;
            }
            // SAFETY: as above.
            more = unsafe { Thread32Next(snapshot, &mut entry) } != 0;
        }
        // SAFETY: the snapshot is open and closed once.
        unsafe { CloseHandle(snapshot) };
        Ok(threads)
    }

    impl Sampler for NtProcess {
        fn sample() -> io::Result<ProcessResources> {
            // SAFETY: the pseudo-handle for this process needs no closing.
            let process = unsafe { GetCurrentProcess() };
            let mut handles = 0_u32;
            // SAFETY: writes the count into `handles`.
            if unsafe { GetProcessHandleCount(process, &mut handles) } == 0 {
                return Err(io::Error::last_os_error());
            }
            // SAFETY: plain data, valid when zeroed.
            let mut memory: PROCESS_MEMORY_COUNTERS_EX = unsafe { std::mem::zeroed() };
            memory.cb = u32::try_from(std::mem::size_of::<PROCESS_MEMORY_COUNTERS_EX>())
                .unwrap_or(u32::MAX);
            // SAFETY: `cb` tells the call it may write the extended counters.
            let read = unsafe {
                GetProcessMemoryInfo(
                    process,
                    (&raw mut memory).cast::<PROCESS_MEMORY_COUNTERS>(),
                    memory.cb,
                )
            };
            if read == 0 {
                return Err(io::Error::last_os_error());
            }
            let zero = FILETIME {
                dwLowDateTime: 0,
                dwHighDateTime: 0,
            };
            let (mut created, mut exited, mut kernel, mut user) = (zero, zero, zero, zero);
            // SAFETY: writes the four times.
            if unsafe {
                GetProcessTimes(process, &mut created, &mut exited, &mut kernel, &mut user)
            } == 0
            {
                return Err(io::Error::last_os_error());
            }
            // 100 ns units; 100,000 of them are a hundredth of a second.
            let units = |time: FILETIME| {
                ((u64::from(time.dwHighDateTime) << 32) | u64::from(time.dwLowDateTime)) / 100_000
            };
            Ok(ProcessResources {
                memory_bytes: memory.PrivateUsage as u64,
                threads: threads()?,
                descriptors: u64::from(handles),
                watches: 0,
                cpu_centiseconds: units(kernel) + units(user),
            })
        }

        fn watches() -> io::Result<u64> {
            Ok(0)
        }

        fn descriptors() -> io::Result<Vec<Descriptor>> {
            Ok(handles()?
                .into_iter()
                .map(|handle| {
                    final_path(handle)
                        .map_or_else(|| Descriptor::Other(kind(handle)), Descriptor::File)
                })
                .collect())
        }

        fn mount_points() -> io::Result<Vec<PathBuf>> {
            // A volume GUID path, `\\?\Volume{GUID}\`, is 49 characters.
            let mut volume = [0u16; 64];
            let length = u32::try_from(volume.len()).unwrap_or(u32::MAX);
            // SAFETY: writes a terminated name of at most `length` units.
            let search = unsafe { FindFirstVolumeW(volume.as_mut_ptr(), length) };
            if search == INVALID_HANDLE_VALUE {
                return Err(io::Error::last_os_error());
            }
            let mut points = Vec::new();
            let listed = loop {
                if let Err(error) = volume_paths(&volume, &mut points) {
                    break Err(error);
                }
                // SAFETY: `search` is open; writes at most `length` units.
                if unsafe { FindNextVolumeW(search, volume.as_mut_ptr(), length) } == 0 {
                    let error = io::Error::last_os_error();
                    break if error.raw_os_error() == i32::try_from(ERROR_NO_MORE_FILES).ok() {
                        Ok(points)
                    } else {
                        Err(error)
                    };
                }
            };
            // SAFETY: closes the search opened above, once.
            unsafe { FindVolumeClose(search) };
            listed
        }
    }

    /// Appends the paths `volume` is mounted at, in the verbatim form
    /// `canonicalize` returns.
    fn volume_paths(volume: &[u16], points: &mut Vec<PathBuf>) -> io::Result<()> {
        let mut needed = 0u32;
        // SAFETY: an empty buffer only asks for the length needed.
        if unsafe {
            GetVolumePathNamesForVolumeNameW(volume.as_ptr(), std::ptr::null_mut(), 0, &mut needed)
        } != 0
        {
            return Ok(());
        }
        let error = io::Error::last_os_error();
        if error.raw_os_error() != i32::try_from(ERROR_MORE_DATA).ok() {
            return Err(error);
        }
        let units =
            usize::try_from(needed).map_err(|_| io::Error::from(io::ErrorKind::InvalidData))?;
        let mut names = vec![0u16; units];
        // SAFETY: writes at most `needed` units into `names`.
        if unsafe {
            GetVolumePathNamesForVolumeNameW(
                volume.as_ptr(),
                names.as_mut_ptr(),
                needed,
                &mut needed,
            )
        } == 0
        {
            return Err(io::Error::last_os_error());
        }
        points.extend(
            names
                .split(|unit| *unit == 0)
                .filter(|name| !name.is_empty())
                .map(|name| {
                    let mut point = std::ffi::OsString::from(r"\\?\");
                    point.push(std::ffi::OsString::from_wide(name));
                    PathBuf::from(point)
                }),
        );
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Each platform's counts follow real opens and closes, so a sampler that
    /// answered a constant would fail here.
    #[test]
    fn the_sampler_counts_the_descriptors_this_process_opens() -> io::Result<()> {
        let directory = tempfile::tempdir()?;
        let path = directory.path().join("counted");
        std::fs::write(&path, b"counted")?;
        assert_eq!(descriptors_naming(&path)?, 0);
        let files = (0..64)
            .map(|_| std::fs::File::open(&path))
            .collect::<io::Result<Vec<_>>>()?;
        let open = sample_process_resources()?;
        assert_eq!(descriptors_naming(&path)?, files.len());
        // Other tests open and close descriptors concurrently, so no
        // difference between two samples is certain; the 64 held here are
        // open throughout the sample, so its total counts at least them.
        assert!(open.descriptors >= files.len() as u64, "{open:?}");
        drop(files);
        assert_eq!(descriptors_naming(&path)?, 0);
        assert!(open.memory_bytes > 0 && open.threads > 0, "{open:?}");
        Ok(())
    }

    /// The filesystem root is mounted on every platform, and a fresh
    /// directory beneath it has no mount: only mounts beneath the root count.
    #[test]
    fn only_mounts_beneath_the_root_count() -> io::Result<()> {
        let directory = tempfile::tempdir()?;
        let top = directory
            .path()
            .ancestors()
            .last()
            .unwrap_or(directory.path());
        assert!(mounts_beneath(top)? > 0, "{top:?}");
        assert_eq!(mounts_beneath(directory.path())?, 0);
        Ok(())
    }

    #[cfg(target_os = "linux")]
    #[test]
    fn mountinfo_paths_decode_their_octal_escapes() {
        assert_eq!(
            linux::unescape(br"/a\040b\134c\011"),
            Path::new("/a b\\c\t")
        );
        // A backslash that starts no escape is kept as written.
        assert_eq!(linux::unescape(br"/a\9b\04"), Path::new(r"/a\9b\04"));
    }
}
