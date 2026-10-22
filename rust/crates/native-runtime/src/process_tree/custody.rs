//! Durable custody of owned process trees across the death of their owner.
//!
//! Every custodied tree is tied to the host process that spawned it and is
//! recorded in the owner's directory before any of its code runs. A reopened
//! owner calls [`ProcessCustody::fence_stale`] before admitting work: it
//! returns only once every tree a dead owner left is dead, so nothing such a
//! tree started can act after the new owner records its interruption.
//!
//! - Unix: the direct child is a warden forked before exec. It creates and
//!   locks its record, watches the owner (Linux: pidfd; macOS: kqueue
//!   `NOTE_EXIT`) and forks the program. When the owner or the program exits
//!   it kills every remaining descendant (Linux: as child subreaper; macOS:
//!   its process group), unlinks the record and exits as the program did.
//!   A fence takes each record's lock, which the kernel releases only when
//!   the warden exits, so the identity of the waited-for tree never depends
//!   on a PID. A record still linked after its lock is free belongs to a
//!   warden killed outright; its recorded process group is killed and awaited.
//! - Windows: each tree is a named Job with `KILL_ON_JOB_CLOSE`. A fence
//!   opens each recorded Job by name, terminates it and awaits every member.

use std::fs;
#[cfg(unix)]
use std::fs::File;
use std::io;
use std::path::{Path, PathBuf};

/// One owner's directory of live process-tree records.
pub struct ProcessCustody {
    path: PathBuf,
    #[cfg(unix)]
    directory: File,
    #[cfg(windows)]
    sequence: std::sync::atomic::AtomicU64,
}

/// One tree's record, removed by its owner once the tree is gone.
pub(super) struct Record {
    #[cfg(unix)]
    directory: File,
    #[cfg(unix)]
    pid: libc::pid_t,
    #[cfg(windows)]
    path: PathBuf,
}

impl ProcessCustody {
    /// Opens or creates `path` as the custody directory of this owner. Only
    /// one live owner may use a directory; it must fence before spawning.
    pub fn open(path: &Path) -> io::Result<Self> {
        fs::create_dir_all(path)?;
        Ok(Self {
            path: path.to_path_buf(),
            #[cfg(unix)]
            directory: File::open(path)?,
            #[cfg(windows)]
            sequence: std::sync::atomic::AtomicU64::new(0),
        })
    }

    /// Kills every tree a previous owner of this directory left and returns
    /// once each is dead. Returns how many records it fenced.
    pub fn fence_stale(&self) -> io::Result<usize> {
        let mut fenced = 0;
        for entry in fs::read_dir(&self.path)? {
            let entry = entry?;
            if fence(&entry.path(), &entry.file_name())? {
                fenced += 1;
            }
        }
        Ok(fenced)
    }
}

impl Record {
    /// Removes the record of a tree its owner has observed to be gone.
    pub(super) fn remove(&self) -> io::Result<()> {
        #[cfg(unix)]
        {
            let mut storage = [0; 24];
            let name = super::warden::record_name(self.pid, &mut storage)
                .ok_or(io::ErrorKind::InvalidInput)?;
            // SAFETY: a live directory descriptor and a NUL-terminated name.
            let removed = unsafe {
                libc::unlinkat(
                    std::os::fd::AsRawFd::as_raw_fd(&self.directory),
                    name.as_ptr(),
                    0,
                )
            };
            if removed == 0 {
                return Ok(());
            }
            let error = io::Error::last_os_error();
            if error.kind() == io::ErrorKind::NotFound {
                Ok(())
            } else {
                Err(error)
            }
        }
        #[cfg(windows)]
        match fs::remove_file(&self.path) {
            Err(error) if error.kind() != io::ErrorKind::NotFound => Err(error),
            _ => Ok(()),
        }
    }
}

#[cfg(unix)]
mod unix {
    use super::{File, Path, ProcessCustody, Record, fs, io};
    use std::io::Read as _;
    use std::os::fd::AsRawFd as _;
    use std::os::unix::fs::MetadataExt as _;
    use std::os::unix::process::CommandExt as _;
    use std::process::Command;

    impl ProcessCustody {
        /// Makes `command`'s direct child the warden of its tree.
        pub(in crate::process_tree) fn arm(&self, command: &mut Command) -> io::Result<File> {
            let directory = self.directory.try_clone()?;
            let descriptor = self.directory.as_raw_fd();
            let owner = libc::pid_t::try_from(std::process::id())
                .map_err(|_| io::ErrorKind::InvalidInput)?;
            // SAFETY: the warden makes only async-signal-safe calls, and the
            // custody directory stays open while the borrowing spawn runs.
            unsafe {
                command.pre_exec(move || super::super::warden::enter(descriptor, owner));
            }
            Ok(directory)
        }
    }

    impl Record {
        pub(in crate::process_tree) const fn new(directory: File, pid: libc::pid_t) -> Self {
            Self { directory, pid }
        }
    }

    /// Fences one record. False for a name that is not a record.
    pub(super) fn fence(path: &Path, name: &std::ffi::OsStr) -> io::Result<bool> {
        let Some(pid) = name
            .to_str()
            .and_then(|name| name.parse::<libc::pid_t>().ok())
            .filter(|pid| *pid > 0)
        else {
            return Ok(false);
        };
        let mut record = match File::open(path) {
            Ok(record) => record,
            Err(error) if error.kind() == io::ErrorKind::NotFound => return Ok(true),
            Err(error) => return Err(error),
        };
        // Blocks until the warden, the lock's only other holder, has exited.
        record.lock()?;
        if record.metadata()?.nlink() == 0 {
            // The warden drained its tree and unlinked the record.
            return Ok(true);
        }
        let mut recorded = Vec::new();
        record.read_to_end(&mut recorded)?;
        let mut boot = [0; super::super::warden::BOOT_BYTES];
        let length = super::super::warden::boot_identity(&mut boot)?;
        // A record from another boot, or one whose warden died before it
        // forked the program, names no live tree.
        if boot.get(..length) == Some(recorded.as_slice()) {
            fence_group(pid)?;
        }
        match fs::remove_file(path) {
            Err(error) if error.kind() != io::ErrorKind::NotFound => Err(error),
            _ => Ok(true),
        }
    }

    /// Kills and awaits a group whose warden was killed outright. A PID that
    /// names a live process is not this tree's warden (its lock is free), and
    /// while any member of the group lives its ID cannot be reused, so a live
    /// process with that PID means the group is already empty.
    fn fence_group(group: libc::pid_t) -> io::Result<()> {
        // SAFETY: signal 0 only probes for existence.
        if unsafe { libc::kill(group, 0) } == 0
            || io::Error::last_os_error().raw_os_error() == Some(libc::EPERM)
        {
            return Ok(());
        }
        loop {
            // SAFETY: a negative PID names exactly this process group.
            if unsafe { libc::kill(-group, libc::SIGKILL) } != 0 {
                let error = io::Error::last_os_error();
                return if error.raw_os_error() == Some(libc::ESRCH) {
                    Ok(())
                } else {
                    Err(error)
                };
            }
            if !await_members(group)? {
                return Ok(());
            }
        }
    }

    /// Waits for the live members of `group` to exit. False when none lived.
    #[cfg(target_os = "linux")]
    fn await_members(group: libc::pid_t) -> io::Result<bool> {
        let mut watches = Vec::new();
        for entry in fs::read_dir("/proc")? {
            let entry = entry?;
            let Some(pid) = entry
                .file_name()
                .to_str()
                .and_then(|name| name.parse::<libc::pid_t>().ok())
            else {
                continue;
            };
            let Ok(stat) = fs::read_to_string(entry.path().join("stat")) else {
                continue;
            };
            // Fields after the command name: state, ppid, pgrp.
            let mut fields = stat
                .rsplit_once(')')
                .map(|(_, rest)| rest.split_whitespace())
                .into_iter()
                .flatten();
            let state = fields.next();
            let member = fields
                .nth(1)
                .and_then(|pgrp| pgrp.parse::<libc::pid_t>().ok());
            if member != Some(group) || matches!(state, Some("Z" | "X")) {
                continue;
            }
            // SAFETY: pidfd_open takes a PID and zero flags.
            let fd = unsafe { libc::syscall(libc::SYS_pidfd_open, pid, 0) };
            if let Ok(fd) = libc::c_int::try_from(fd)
                && fd >= 0
            {
                // SAFETY: the descriptor pidfd_open just returned.
                watches.push(unsafe {
                    <std::os::fd::OwnedFd as std::os::fd::FromRawFd>::from_raw_fd(fd)
                });
            }
        }
        if watches.is_empty() {
            return Ok(false);
        }
        for watch in &watches {
            let mut ready = libc::pollfd {
                fd: watch.as_raw_fd(),
                events: libc::POLLIN,
                revents: 0,
            };
            // SAFETY: one initialized pollfd for an owned descriptor.
            while unsafe { libc::poll(&raw mut ready, 1, -1) } < 0 {
                let error = io::Error::last_os_error();
                if error.kind() != io::ErrorKind::Interrupted {
                    return Err(error);
                }
            }
        }
        Ok(true)
    }

    #[cfg(target_vendor = "apple")]
    fn await_members(group: libc::pid_t) -> io::Result<bool> {
        use super::super::warden::{group_members, next_exit, watch_exit};
        let mut members = [0; 256];
        let count = group_members(group, &mut members)?;
        // SAFETY: kqueue has no preconditions.
        let queue = unsafe { libc::kqueue() };
        if queue < 0 {
            return Err(io::Error::last_os_error());
        }
        // SAFETY: the queue just created.
        let queue = unsafe { <std::os::fd::OwnedFd as std::os::fd::FromRawFd>::from_raw_fd(queue) };
        let mut live = 0_usize;
        for member in members.iter().take(count).copied() {
            if member > 0 && watch_exit(queue.as_raw_fd(), member)? {
                live += 1;
            }
        }
        let any = live > 0;
        while live > 0 {
            next_exit(queue.as_raw_fd())?;
            live -= 1;
        }
        Ok(any)
    }
}
#[cfg(unix)]
use unix::fence;

#[cfg(windows)]
mod windows {
    use super::{Path, ProcessCustody, Record, fs, io};
    use std::sync::atomic::Ordering;
    use windows_sys::Win32::Foundation::{CloseHandle, ERROR_FILE_NOT_FOUND, HANDLE, WAIT_FAILED};
    use windows_sys::Win32::System::JobObjects::{
        JOBOBJECT_BASIC_PROCESS_ID_LIST, JobObjectBasicProcessIdList, OpenJobObjectW,
        QueryInformationJobObject, TerminateJobObject,
    };
    use windows_sys::Win32::System::Threading::{
        OpenProcess, PROCESS_SYNCHRONIZE, WaitForMultipleObjects,
    };

    const PREFIX: &str = "acyclic-process-tree-";
    // JOB_OBJECT_QUERY | JOB_OBJECT_TERMINATE.
    const JOB_ACCESS: u32 = 0x0004 | 0x0008;

    /// The Job name a record names, NUL-terminated.
    pub(in crate::process_tree) fn job_name(record: &str) -> Vec<u16> {
        format!("Local\\{record}")
            .encode_utf16()
            .chain(std::iter::once(0))
            .collect()
    }

    impl ProcessCustody {
        /// Records a new tree before its Job exists; returns the record and
        /// the Job name the spawn must create.
        pub(in crate::process_tree) fn record(&self) -> io::Result<(Record, Vec<u16>)> {
            let sequence = self.sequence.fetch_add(1, Ordering::Relaxed);
            let nanos = std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .map_err(io::Error::other)?
                .as_nanos();
            let name = format!("{PREFIX}{}-{nanos}-{sequence}", std::process::id());
            let path = self.path.join(&name);
            fs::OpenOptions::new()
                .write(true)
                .create_new(true)
                .open(&path)?;
            Ok((Record { path }, job_name(&name)))
        }
    }

    pub(super) fn fence(path: &Path, name: &std::ffi::OsStr) -> io::Result<bool> {
        let Some(name) = name.to_str().filter(|name| name.starts_with(PREFIX)) else {
            return Ok(false);
        };
        let wide = job_name(name);
        // SAFETY: a NUL-terminated name; the handle is owned on success.
        let job = unsafe { OpenJobObjectW(JOB_ACCESS, 0, wide.as_ptr()) };
        if job.is_null() {
            let error = io::Error::last_os_error();
            if error.raw_os_error() != Some(ERROR_FILE_NOT_FOUND.cast_signed()) {
                return Err(error);
            }
        } else {
            let fenced = terminate_and_await(job);
            // SAFETY: the Job handle opened above.
            unsafe { CloseHandle(job) };
            fenced?;
        }
        match fs::remove_file(path) {
            Err(error) if error.kind() != io::ErrorKind::NotFound => Err(error),
            _ => Ok(true),
        }
    }

    fn terminate_and_await(job: HANDLE) -> io::Result<()> {
        // SAFETY: an owned Job handle with terminate access.
        if unsafe { TerminateJobObject(job, 1) } == 0 {
            return Err(io::Error::last_os_error());
        }
        loop {
            let members = members(job)?;
            if members.is_empty() {
                return Ok(());
            }
            let handles: Vec<HANDLE> = members
                .iter()
                // SAFETY: opening a synchronize-only handle to a listed PID.
                .map(|pid| unsafe { OpenProcess(PROCESS_SYNCHRONIZE, 0, *pid) })
                .filter(|handle| !handle.is_null())
                .collect();
            let waited = handles.chunks(64).try_for_each(|chunk| {
                let count = u32::try_from(chunk.len()).map_err(|_| io::ErrorKind::InvalidInput)?;
                // SAFETY: up to 64 owned process handles; waits for all.
                if unsafe { WaitForMultipleObjects(count, chunk.as_ptr(), 1, u32::MAX) }
                    == WAIT_FAILED
                {
                    Err(io::Error::last_os_error())
                } else {
                    Ok(())
                }
            });
            for handle in handles {
                // SAFETY: each handle opened above.
                unsafe { CloseHandle(handle) };
            }
            waited?;
        }
    }

    /// Lists the process IDs still in `job`.
    fn members(job: HANDLE) -> io::Result<Vec<u32>> {
        const CAPACITY: usize = 1024;
        let header = std::mem::offset_of!(JOBOBJECT_BASIC_PROCESS_ID_LIST, ProcessIdList)
            / std::mem::size_of::<usize>();
        let mut storage = vec![0_usize; header + CAPACITY];
        let bytes = u32::try_from(storage.len() * std::mem::size_of::<usize>())
            .map_err(|_| io::ErrorKind::InvalidInput)?;
        // SAFETY: writable, suitably aligned storage of the stated size.
        if unsafe {
            QueryInformationJobObject(
                job,
                JobObjectBasicProcessIdList,
                storage.as_mut_ptr().cast(),
                bytes,
                std::ptr::null_mut(),
            )
        } == 0
        {
            let error = io::Error::last_os_error();
            // ERROR_MORE_DATA still fills the listed prefix.
            if error.raw_os_error() != Some(234) {
                return Err(error);
            }
        }
        // SAFETY: the storage begins with the initialized fixed header.
        let list = unsafe { &*storage.as_ptr().cast::<JOBOBJECT_BASIC_PROCESS_ID_LIST>() };
        let listed = usize::try_from(list.NumberOfProcessIdsInList)
            .map_err(|_| io::ErrorKind::InvalidData)?
            .min(CAPACITY);
        Ok(storage
            .iter()
            .skip(header)
            .take(listed)
            .filter_map(|pid| u32::try_from(*pid).ok())
            .collect())
    }
}
#[cfg(windows)]
use windows::fence;
