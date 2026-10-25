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

use std::collections::BTreeMap;
use std::fs;
#[cfg(unix)]
use std::fs::File;
use std::io;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex, Weak};
use std::time::{Duration, Instant};

/// Serialized admission to a durable process-custody directory.
///
/// The native owner keeps this lease through physical retirement. If that
/// owner dies, its successor takes the same OS lock and fences the retained
/// records before it can spawn. No caller-process mutex participates.
pub struct ProcessCustodyLease {
    custody: ProcessCustody,
    _lock: crate::FileLock,
    waited: Duration,
}

impl ProcessCustodyLease {
    /// Locks `root/owner.lock`, then fences `root/trees` before admission.
    ///
    /// # Errors
    ///
    /// Returns a filesystem, lock or physical-retirement error without
    /// admitting a command. The root must have one stable shared identity.
    pub fn acquire(root: &Path) -> io::Result<Self> {
        fs::create_dir_all(root)?;
        let file = fs::OpenOptions::new()
            .create(true)
            .truncate(false)
            .read(true)
            .write(true)
            .open(root.join("owner.lock"))?;
        let start = Instant::now();
        let lock = crate::FileLock::acquire(file, crate::FileLockMode::Exclusive)?;
        let waited = start.elapsed();
        // Lock acquisition must precede recovery: fencing first could kill a
        // different invocation that still legitimately holds this lease.
        let custody = ProcessCustody::open(&root.join("trees"))?;
        custody.fence_stale()?;
        Ok(Self {
            custody,
            _lock: lock,
            waited,
        })
    }

    /// The already-fenced custody used for every admitted child.
    #[must_use]
    pub fn custody(&self) -> &ProcessCustody {
        &self.custody
    }

    /// Time blocked acquiring the lease, excluding physical recovery work.
    #[must_use]
    pub const fn waited(&self) -> Duration {
        self.waited
    }
}

/// One owner's directory of live process-tree records.
pub struct ProcessCustody {
    path: PathBuf,
    #[cfg(unix)]
    directory: File,
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
        })
    }

    /// This process's custody of `path`. Owners in one process share the
    /// custody of a directory: the first fences it, and later ones reuse it
    /// while it lives, never waiting on a live owner's running trees. Opens
    /// of different directories never wait on each other.
    pub fn acquire(path: &Path) -> io::Result<Arc<Self>> {
        fs::create_dir_all(path)?;
        let key = fs::canonicalize(path)?;
        let slot = {
            let mut live = LIVE.lock().map_err(|_| poisoned())?;
            Arc::clone(live.entry(key.clone()).or_default())
        };
        let mut slot = slot.lock().map_err(|_| poisoned())?;
        if let Some(custody) = slot.upgrade() {
            return Ok(custody);
        }
        let custody = Arc::new(Self::open(&key)?);
        custody.fence_stale()?;
        *slot = Arc::downgrade(&custody);
        Ok(custody)
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

/// Each directory's live custody in this process, behind its own lock so a
/// fence of one directory never delays another.
type Slot = Arc<Mutex<Weak<ProcessCustody>>>;
static LIVE: Mutex<BTreeMap<PathBuf, Slot>> = Mutex::new(BTreeMap::new());

fn poisoned() -> io::Error {
    io::Error::other("process custody registry poisoned")
}

impl Drop for ProcessCustody {
    fn drop(&mut self) {
        // Forget this directory unless an owner is acquiring it right now:
        // acquirers clone the slot under the registry lock held here.
        if let Ok(mut live) = LIVE.lock()
            && let Some(slot) = live.get(&self.path)
            && Arc::strong_count(slot) == 1
            && slot.try_lock().is_ok_and(|slot| slot.strong_count() == 0)
        {
            live.remove(&self.path);
        }
    }
}

impl Record {
    /// Keep recovery authority until the original ledger and physical removal
    /// both accept retirement. A failed cleanup must leave an owned record.
    pub(super) fn retire(
        record: &mut Option<Self>,
        meter: Option<&dyn crate::NativeReadMeter>,
    ) -> io::Result<()> {
        if let Some(retained) = record {
            super::process_control_attempt(meter, crate::NativeOperationKind::Write, || {
                retained.remove()
            })?;
            record.take();
        }
        Ok(())
    }

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
        {
            windows::await_record(self)?;
            match fs::remove_file(&self.path) {
                Err(error) if error.kind() != io::ErrorKind::NotFound => Err(error),
                _ => Ok(()),
            }
        }
    }
}

#[cfg(test)]
#[path = "custody_retirement_tests.rs"]
mod retirement_tests;

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
        let record = match File::open(path) {
            Ok(record) => record,
            Err(error) if error.kind() == io::ErrorKind::NotFound => return Ok(true),
            Err(error) => return Err(error),
        };
        // Blocks until the warden, the lock's only other holder, has exited.
        let record = crate::FileLock::acquire(record, crate::FileLockMode::Exclusive)?;
        if record.metadata()?.nlink() == 0 {
            // The warden drained its tree and unlinked the record.
            return Ok(true);
        }
        let mut recorded = Vec::new();
        (&*record).read_to_end(&mut recorded)?;
        let mut boot = [0; super::super::warden::BOOT_BYTES];
        super::super::warden::boot_identity(&mut boot)?;
        // A record from another boot, or one whose warden died before it
        // recorded its start (and so before it forked the program), names no
        // live tree.
        if let Some(start) = recorded
            .strip_prefix(boot.as_slice())
            .filter(|start| !start.is_empty())
        {
            fence_group(pid, start)?;
        }
        match fs::remove_file(path) {
            Err(error) if error.kind() != io::ErrorKind::NotFound => Err(error),
            _ => Ok(true),
        }
    }

    /// Kills and awaits the group of a warden that was killed outright and
    /// recorded `start` as its own start identity.
    ///
    /// A process with the warden's PID and that identity is the warden itself,
    /// a zombie whose orphaned tree may still run. One with another identity
    /// reused the PID, which the kernel allows only once no process uses it as
    /// a process group ID: the warden's group is already empty, and a group
    /// with that ID now is someone else's. With no such process at all, the
    /// group ID stays reserved while any member of the warden's group lives.
    fn fence_group(group: libc::pid_t, start: &[u8]) -> io::Result<()> {
        let mut current = [0; super::super::warden::START_BYTES];
        use crate::process_identity::{IncarnationState, ProcessIncarnation, ProcessObservation};
        let owner = ProcessIncarnation::new(
            u32::try_from(group).map_err(|_| io::ErrorKind::InvalidData)?,
            start,
        )
        .ok_or(io::ErrorKind::InvalidData)?;
        let observation = match super::super::warden::start_identity(group, &mut current) {
            Ok(length) => ProcessObservation::Present {
                started: current.get(..length).ok_or(io::ErrorKind::InvalidData)?,
                exited: false,
            },
            Err(error) if error.kind() == io::ErrorKind::NotFound => ProcessObservation::Absent,
            Err(error) => return Err(error),
        };
        match owner.observe(observation) {
            IncarnationState::Replaced => return Ok(()),
            IncarnationState::Unknown => return Err(io::ErrorKind::InvalidData.into()),
            // Unlike a Windows process handle, a POSIX group can outlive its
            // absent or zombie leader. Retain the group until every member exits.
            IncarnationState::Running | IncarnationState::Exited | IncarnationState::Absent => {}
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
            if !linux_group_member(&entry.path().join("stat"), group)? {
                continue;
            }
            if let Some(watch) = linux_pin_member(group, pid)? {
                watches.push(watch);
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

    #[cfg(target_os = "linux")]
    fn linux_group_member(path: &Path, group: libc::pid_t) -> io::Result<bool> {
        let stat = match fs::read_to_string(path) {
            Ok(stat) => stat,
            Err(error)
                if error.kind() == io::ErrorKind::NotFound
                    || error.raw_os_error() == Some(libc::ESRCH) =>
            {
                return Ok(false);
            }
            Err(error) => return Err(error),
        };
        // Fields after the command name: state, ppid, pgrp. Invalid identity
        // data is an observation failure, never evidence of nonmembership.
        let (_, rest) = stat.rsplit_once(')').ok_or(io::ErrorKind::InvalidData)?;
        let mut fields = rest.split_whitespace();
        let state = fields.next().ok_or(io::ErrorKind::InvalidData)?;
        let member = fields
            .nth(1)
            .ok_or(io::ErrorKind::InvalidData)?
            .parse::<libc::pid_t>()
            .map_err(|_| io::ErrorKind::InvalidData)?;
        Ok(member == group && !matches!(state, "Z" | "X"))
    }

    #[cfg(target_os = "linux")]
    fn linux_pin_member(
        group: libc::pid_t,
        pid: libc::pid_t,
    ) -> io::Result<Option<std::os::fd::OwnedFd>> {
        // SAFETY: pidfd_open takes a PID and zero flags.
        let fd = unsafe { libc::syscall(libc::SYS_pidfd_open, pid, 0) };
        if fd < 0 {
            let error = io::Error::last_os_error();
            return if error.raw_os_error() == Some(libc::ESRCH) {
                Ok(None)
            } else {
                Err(error)
            };
        }
        let fd = libc::c_int::try_from(fd).map_err(io::Error::other)?;
        // SAFETY: pidfd_open transferred this descriptor, retained through the
        // second observation. A reused foreign PID can never be a wait target.
        let watch = unsafe { <std::os::fd::OwnedFd as std::os::fd::FromRawFd>::from_raw_fd(fd) };
        if linux_group_member(
            &Path::new("/proc").join(pid.to_string()).join("stat"),
            group,
        )? {
            Ok(Some(watch))
        } else {
            Ok(None)
        }
    }

    #[cfg(all(test, target_os = "linux"))]
    mod member_tests {
        use super::*;

        #[test]
        fn a_pidfd_failure_cannot_prove_a_member_absent() {
            let result = linux_pin_member(-1, 0);
            eprintln!("LINUX_FENCE_OPEN_FAILURE result={result:?}");
            let error = result.expect_err("stale fencing must reject a failed pidfd open");
            assert_eq!(error.raw_os_error(), Some(libc::EINVAL));
        }

        #[test]
        fn a_reused_pid_outside_the_group_is_not_a_wait_target() -> io::Result<()> {
            let pid = libc::pid_t::try_from(std::process::id()).map_err(io::Error::other)?;
            let pinned = linux_pin_member(-1, pid)?;
            eprintln!(
                "LINUX_FENCE_FOREIGN_PID retained={}",
                usize::from(pinned.is_some())
            );
            assert!(
                pinned.is_none(),
                "a listed PID can name a live process outside the fenced group"
            );
            Ok(())
        }

        #[test]
        fn unreadable_member_identity_cannot_prove_nonmembership() -> io::Result<()> {
            let file = tempfile::NamedTempFile::new()?;
            fs::write(file.path(), [0xff])?;
            let result = linux_group_member(file.path(), -1);
            eprintln!("LINUX_FENCE_STAT_FAILURE result={result:?}");
            let error =
                result.expect_err("a failed member identity read must not prove nonmembership");
            assert_eq!(error.kind(), io::ErrorKind::InvalidData);
            Ok(())
        }
    }

    #[cfg(target_vendor = "apple")]
    fn await_members(group: libc::pid_t) -> io::Result<bool> {
        // Stale-owner fencing uses the same complete, creation-fenced group
        // enumeration as live termination. A fixed PID prefix cannot prove
        // an entire group empty, and failed observations never acknowledge it.
        if super::super::platform::group_has_no_live_members(group, None)? {
            Ok(false)
        } else {
            Err(io::ErrorKind::TimedOut.into())
        }
    }
}
#[cfg(unix)]
use unix::fence;

#[cfg(windows)]
mod windows {
    use super::{Path, ProcessCustody, Record, fs, io};
    use std::sync::atomic::{AtomicU64, Ordering};
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
            // One sequence for the whole process: Job names share one
            // namespace across every custody directory this process owns.
            static SEQUENCE: AtomicU64 = AtomicU64::new(0);
            let sequence = SEQUENCE.fetch_add(1, Ordering::Relaxed);
            // This process's PID and creation time name it among every
            // process, past or present, that may have shared the directory.
            let name = format!(
                "{PREFIX}{}-{}-{sequence}",
                std::process::id(),
                crate::process_identity::current_process_started()?
            );
            let path = self.path.join(&name);
            fs::OpenOptions::new()
                .write(true)
                .create_new(true)
                .open(&path)?;
            Ok((Record { path }, job_name(&name)))
        }
    }

    /// Separates a record's tree from the custodian it names.
    const CUSTODIAN: &str = "-custodian-";

    pub(super) fn await_record(record: &Record) -> io::Result<()> {
        let name = record
            .path
            .file_name()
            .and_then(|name| name.to_str())
            .ok_or(io::ErrorKind::InvalidData)?;
        if let Some((_, identity)) = name.split_once(CUSTODIAN) {
            super::super::custodian::await_custodian(identity)?;
        }
        Ok(())
    }

    impl Record {
        /// Names the tree's custodian in the record, before any member
        /// starts. The rename is atomic: a fence sees the record with the
        /// custodian or without it, and without it no member ever started.
        pub(in crate::process_tree) fn name_custodian(&mut self, identity: &str) -> io::Result<()> {
            let mut named = self.path.clone().into_os_string();
            named.push(CUSTODIAN);
            named.push(identity);
            fs::rename(&self.path, &named)?;
            self.path = named.into();
            Ok(())
        }
    }

    pub(super) fn fence(path: &Path, name: &std::ffi::OsStr) -> io::Result<bool> {
        let Some(name) = name.to_str().filter(|name| name.starts_with(PREFIX)) else {
            return Ok(false);
        };
        let (name, custodian) = name
            .split_once(CUSTODIAN)
            .map_or((name, None), |(name, custodian)| (name, Some(custodian)));
        let wide = job_name(name);
        // SAFETY: a NUL-terminated name; the handle is owned on success.
        let job = unsafe { OpenJobObjectW(JOB_ACCESS, 0, wide.as_ptr()) };
        if job.is_null() {
            let error = io::Error::last_os_error();
            if error.raw_os_error() != Some(ERROR_FILE_NOT_FOUND.cast_signed()) {
                return Err(error);
            }
        } else {
            // A member leaves the Job's list before its process has fully
            // exited; the custodian exits only once each has, so its exit
            // ends the fence.
            let fenced = terminate_and_await(job);
            // SAFETY: the Job handle opened above.
            unsafe { CloseHandle(job) };
            fenced?;
        }
        // A missing Job proves that the custodian closed it after member
        // retirement, but its own process and retained pipe handles can still
        // be live. The recorded identity is the same barrier in either case.
        if let Some(custodian) = custodian {
            super::super::custodian::await_custodian(custodian)?;
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
    pub(in crate::process_tree) fn members(job: HANDLE) -> io::Result<Vec<u32>> {
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
#[cfg(windows)]
pub(super) use windows::members;
