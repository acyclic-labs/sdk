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
        match super::super::warden::start_identity(group, &mut current) {
            Ok(length) if current.get(..length) != Some(start) => return Ok(()),
            // Another user's process is never this owner's warden.
            Err(error) if error.raw_os_error() == Some(libc::EPERM) => return Ok(()),
            Err(error) if error.kind() != io::ErrorKind::NotFound => return Err(error),
            _ => {}
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
    use std::os::windows::io::{AsRawHandle as _, OwnedHandle};
    use std::sync::atomic::{AtomicU64, Ordering};
    use windows_sys::Win32::Foundation::{
        CloseHandle, ERROR_FILE_NOT_FOUND, FILETIME, HANDLE, WAIT_FAILED,
    };
    use windows_sys::Win32::System::JobObjects::{
        JOBOBJECT_BASIC_PROCESS_ID_LIST, JobObjectBasicProcessIdList, OpenJobObjectW,
        QueryInformationJobObject, TerminateJobObject,
    };
    use windows_sys::Win32::System::Threading::{
        GetCurrentProcess, GetProcessTimes, WaitForMultipleObjects,
    };

    const PREFIX: &str = "acyclic-process-tree-";
    // JOB_OBJECT_QUERY | JOB_OBJECT_TERMINATE.
    const JOB_ACCESS: u32 = 0x0004 | 0x0008;

    /// This process's creation time, in 100 ns intervals since 1601.
    fn creation_time() -> io::Result<u64> {
        let mut times = [FILETIME {
            dwLowDateTime: 0,
            dwHighDateTime: 0,
        }; 4];
        let [creation, exit, kernel, user] = &mut times;
        // SAFETY: the current-process pseudo-handle and four writable times.
        if unsafe { GetProcessTimes(GetCurrentProcess(), creation, exit, kernel, user) } == 0 {
            return Err(io::Error::last_os_error());
        }
        Ok(u64::from(creation.dwHighDateTime) << 32 | u64::from(creation.dwLowDateTime))
    }

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
                creation_time()?
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
        // Open completion before the Job: while the daemon still holds a
        // live Job, its completion witness cannot disappear between opens.
        let completion = custodian
            .map(|_| super::super::custodian::Completion::open(&wide))
            .transpose()?
            .flatten();
        // SAFETY: a NUL-terminated name; the handle is owned on success.
        let job = unsafe { OpenJobObjectW(JOB_ACCESS, 0, wide.as_ptr()) };
        if job.is_null() {
            let error = io::Error::last_os_error();
            if error.raw_os_error() != Some(ERROR_FILE_NOT_FOUND.cast_signed()) {
                return Err(error);
            }
        } else {
            // Job removal precedes full kernel exit. Its independent event
            // is published only once the daemon's pinned members have exited.
            let fenced = terminate_and_await(job).and_then(|()| {
                custodian.map_or(Ok(()), |identity| {
                    super::super::custodian::await_completion(completion.as_ref(), identity)
                })
            });
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
            let handles = pin_members(job, &members)?;
            handles.chunks(64).try_for_each(|chunk| {
                let count = u32::try_from(chunk.len()).map_err(|_| io::ErrorKind::InvalidInput)?;
                let mut raw = [std::ptr::null_mut(); 64];
                for (slot, handle) in raw.iter_mut().zip(chunk) {
                    *slot = handle.as_raw_handle();
                }
                // SAFETY: up to 64 owned process handles; waits for all.
                if unsafe { WaitForMultipleObjects(count, raw.as_ptr(), 1, u32::MAX) }
                    == WAIT_FAILED
                {
                    Err(io::Error::last_os_error())
                } else {
                    Ok(())
                }
            })?;
        }
    }

    fn pin_members(job: HANDLE, members: &[u32]) -> io::Result<Vec<OwnedHandle>> {
        let mut handles = Vec::new();
        for pid in members {
            if let Some(process) = super::super::custodian::member(job, *pid)? {
                handles.push(process);
            }
        }
        Ok(handles)
    }

    /// Lists one complete kernel page of the process IDs still in `job`.
    pub(in crate::process_tree) fn members(job: HANDLE) -> io::Result<Vec<u32>> {
        use super::super::job_members::{Page, collect_members};
        let header = std::mem::offset_of!(JOBOBJECT_BASIC_PROCESS_ID_LIST, ProcessIdList)
            / size_of::<usize>();
        if header != super::super::job_members::HEADER {
            return Err(io::ErrorKind::InvalidData.into());
        }
        collect_members(|storage| {
            let bytes = storage
                .len()
                .checked_mul(size_of::<usize>())
                .and_then(|bytes| u32::try_from(bytes).ok())
                .ok_or(io::ErrorKind::InvalidInput)?;
            // SAFETY: writable, suitably aligned storage, including the fixed
            // header and at least one process slot, of exactly the stated size.
            let queried = unsafe {
                QueryInformationJobObject(
                    job,
                    JobObjectBasicProcessIdList,
                    storage.as_mut_ptr().cast(),
                    bytes,
                    std::ptr::null_mut(),
                )
            };
            let complete = if queried == 0 {
                let error = io::Error::last_os_error();
                if error.raw_os_error() != Some(234) {
                    return Err(error);
                }
                false
            } else {
                true
            };
            // SAFETY: success and ERROR_MORE_DATA both initialize the fixed
            // header. No process slot is consumed before its count is checked.
            let list = unsafe { &*storage.as_ptr().cast::<JOBOBJECT_BASIC_PROCESS_ID_LIST>() };
            Ok(Page {
                assigned: usize::try_from(list.NumberOfAssignedProcesses)
                    .map_err(|_| io::ErrorKind::InvalidData)?,
                listed: usize::try_from(list.NumberOfProcessIdsInList)
                    .map_err(|_| io::ErrorKind::InvalidData)?,
                complete,
            })
        })
    }

    #[cfg(test)]
    mod member_tests {
        use super::*;
        #[test]
        fn job_member_page_child() -> io::Result<()> {
            use std::io::{Read as _, Write as _};
            use std::os::windows::process::CommandExt as _;
            use std::process::{Command, Stdio};
            let Some(mode) = std::env::var_os("ACYCLIC_JOB_MEMBER_PAGE_CHILD") else {
                return Ok(());
            };
            let mut descendant = None;
            if mode == "parent" {
                let child = Command::new(std::env::current_exe()?)
                    .args([
                        "--exact",
                        "process_tree::custody::windows::member_tests::job_member_page_child",
                        "--nocapture",
                    ])
                    .env("ACYCLIC_JOB_MEMBER_PAGE_CHILD", "leaf")
                    .stdin(Stdio::piped())
                    .stdout(Stdio::null())
                    .stderr(Stdio::null())
                    .creation_flags(windows_sys::Win32::System::Threading::CREATE_NO_WINDOW)
                    .spawn()?;
                println!("JOB_PAGE_CHILD={}", child.id());
                std::io::stdout().flush()?;
                descendant = Some(child);
            }
            let mut byte = [0];
            std::io::stdin().read_exact(&mut byte)?;
            if let Some(mut child) = descendant {
                child.kill()?;
                child.wait()?;
            }
            Ok(())
        }

        #[test]
        fn the_native_job_query_grows_to_include_a_live_descendant() -> io::Result<()> {
            use std::io::BufRead as _;
            use std::process::{Command, Stdio};
            let directory = tempfile::tempdir()?;
            let custody = super::super::ProcessCustody::open(directory.path())?;
            let mut command = Command::new(std::env::current_exe()?);
            command
                .args([
                    "--exact",
                    "process_tree::custody::windows::member_tests::job_member_page_child",
                    "--nocapture",
                ])
                .env("ACYCLIC_JOB_MEMBER_PAGE_CHILD", "parent")
                .stdin(Stdio::piped())
                .stdout(Stdio::piped())
                .stderr(Stdio::null());
            let mut tree = crate::ProcessTree::spawn_in_custody(&mut command, &custody)?;
            let mut output =
                std::io::BufReader::new(tree.take_stdout().ok_or(io::ErrorKind::BrokenPipe)?);
            let mut line = String::new();
            let child_pid = loop {
                line.clear();
                if output.read_line(&mut line)? == 0 {
                    return Err(io::ErrorKind::UnexpectedEof.into());
                }
                if let Some((_, pid)) = line.trim().rsplit_once("JOB_PAGE_CHILD=") {
                    break pid.parse::<u32>().map_err(io::Error::other)?;
                }
            };
            let root_pid = tree.child.as_ref().ok_or(io::ErrorKind::NotFound)?.id();
            let record = tree.custody.as_ref().ok_or(io::ErrorKind::NotFound)?;
            let name = record
                .path
                .file_name()
                .and_then(|name| name.to_str())
                .ok_or(io::ErrorKind::InvalidData)?;
            let name = name.split_once(CUSTODIAN).map_or(name, |(name, _)| name);
            let wide = job_name(name);
            // SAFETY: a NUL-terminated name of this live tree's owned Job.
            let job = unsafe { OpenJobObjectW(JOB_ACCESS, 0, wide.as_ptr()) };
            if job.is_null() {
                return Err(io::Error::last_os_error());
            }
            // SAFETY: OpenJobObjectW transferred this handle.
            let job = unsafe { OwnedHandle::from_raw_handle(job) };
            let mut observed = members(job.as_raw_handle())?;
            observed.sort_unstable();
            // Windows can create an additional console host in the Job. Its
            // independent accounting count is the completeness oracle; known
            // fixture PIDs prove that collection includes the live descendant.
            use windows_sys::Win32::System::JobObjects::{
                JOBOBJECT_BASIC_ACCOUNTING_INFORMATION, JobObjectBasicAccountingInformation,
            };
            // SAFETY: every bit pattern of the native accounting structure is valid.
            let mut accounting: JOBOBJECT_BASIC_ACCOUNTING_INFORMATION =
                unsafe { std::mem::zeroed() };
            // SAFETY: the retained live Job and complete writable native structure.
            if unsafe {
                QueryInformationJobObject(
                    job.as_raw_handle(),
                    JobObjectBasicAccountingInformation,
                    (&raw mut accounting).cast(),
                    u32::try_from(size_of::<JOBOBJECT_BASIC_ACCOUNTING_INFORMATION>())
                        .map_err(io::Error::other)?,
                    std::ptr::null_mut(),
                )
            } == 0
            {
                return Err(io::Error::last_os_error());
            }
            eprintln!(
                "JOB_MEMBER_NATIVE root={root_pid} descendant={child_pid} active={} observed={observed:?}",
                accounting.ActiveProcesses
            );
            assert!(observed.contains(&root_pid) && observed.contains(&child_pid));
            assert_eq!(
                observed.len(),
                usize::try_from(accounting.ActiveProcesses).map_err(io::Error::other)?,
                "the complete page must include every active Job member"
            );
            tree.terminate(None)?;
            Ok(())
        }

        use std::os::windows::io::FromRawHandle as _;
        use windows_sys::Win32::Foundation::INVALID_HANDLE_VALUE;
        use windows_sys::Win32::System::JobObjects::CreateJobObjectW;

        #[test]
        fn stale_fencing_cannot_discard_a_membership_query_failure() {
            let pinned = pin_members(INVALID_HANDLE_VALUE, &[std::process::id()]);
            eprintln!("FENCE_MEMBERSHIP_FAILURE result={pinned:?}");
            assert!(
                pinned.is_err(),
                "stale fencing must reject a failed Job membership query"
            );
        }

        #[test]
        fn stale_fencing_does_not_pin_an_unrelated_process_at_a_listed_pid() -> io::Result<()> {
            // SAFETY: creates an empty unnamed Job, owned on success.
            let job = unsafe { CreateJobObjectW(std::ptr::null(), std::ptr::null()) };
            if job.is_null() {
                return Err(io::Error::last_os_error());
            }
            // SAFETY: CreateJobObjectW transferred this handle.
            let job = unsafe { OwnedHandle::from_raw_handle(job) };
            let pinned = pin_members(job.as_raw_handle(), &[std::process::id()])?;
            eprintln!("FENCE_FOREIGN_PID retained={}", pinned.len());
            assert!(
                pinned.is_empty(),
                "a listed PID can name an unrelated process after exit"
            );
            Ok(())
        }
    }
}
#[cfg(windows)]
use windows::fence;
#[cfg(windows)]
pub(super) use windows::members;
