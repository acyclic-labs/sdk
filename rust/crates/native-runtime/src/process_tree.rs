#![allow(unsafe_code)]

use std::io;
use std::process::{Child, ChildStderr, ChildStdin, ChildStdout, Command, ExitStatus, Output};

/// One child and descendants that remain in its operating-system containment.
///
/// Windows uses a Job object. Unix uses a process group, which cannot contain
/// a descendant that deliberately creates a new process group or session.
pub struct ProcessTree {
    child: Option<Child>,
    guard: platform::Guard,
}

impl ProcessTree {
    pub(crate) fn spawn(command: &mut Command) -> io::Result<Self> {
        let (child, guard) = platform::spawn(command)?;
        Ok(Self {
            child: Some(child),
            guard,
        })
    }

    /// Adopts an already running process into an owned termination boundary.
    ///
    /// Hosts that must expose a platform-native owner while retaining their
    /// own stdio handles (for example Node's ChildProcess) use this narrow
    /// hand-off. The returned guard is the authority for later cleanup; the
    /// caller never has to recover ownership from a PID after the hand-off.
    pub fn adopt(pid: u32) -> io::Result<Self> {
        if pid == 0 {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "process id must be nonzero",
            ));
        }
        Ok(Self {
            child: None,
            guard: platform::adopt(pid)?,
        })
    }

    /// Returns the direct child identity while the tree retains its launch
    /// handle. Native callers use this value only as an observation; cleanup
    /// remains authorized by the retained process-group or Job guard.
    pub fn id(&self) -> Option<u32> {
        self.child.as_ref().map(Child::id)
    }

    /// Polls the direct child without releasing ownership of its descendants.
    pub fn try_wait(&mut self) -> io::Result<Option<ExitStatus>> {
        self.child
            .as_mut()
            .ok_or_else(|| io::Error::other("process output was already collected"))?
            .try_wait()
    }

    /// Waits for the direct child while retaining ownership of its descendants.
    pub fn wait(&mut self) -> io::Result<ExitStatus> {
        self.child
            .as_mut()
            .ok_or_else(|| io::Error::other("process output was already collected"))?
            .wait()
    }

    /// Collects the direct child's output while retaining the descendant guard.
    pub fn wait_with_output(&mut self) -> io::Result<Output> {
        self.child
            .take()
            .ok_or_else(|| io::Error::other("process output was already collected"))?
            .wait_with_output()
    }

    /// Takes the direct child's stdout pipe while retaining descendant
    /// ownership for later termination.
    pub fn take_stdout(&mut self) -> Option<ChildStdout> {
        self.child.as_mut().and_then(|child| child.stdout.take())
    }

    /// Takes the direct child's stderr pipe while retaining descendant
    /// ownership for later termination.
    pub fn take_stderr(&mut self) -> Option<ChildStderr> {
        self.child.as_mut().and_then(|child| child.stderr.take())
    }

    /// Takes the direct child's stdin while retaining native tree ownership.
    pub fn take_stdin(&mut self) -> Option<ChildStdin> {
        self.child.as_mut().and_then(|child| child.stdin.take())
    }

    /// Terminates every process in the tree and reaps the direct child.
    pub fn terminate(&mut self) -> io::Result<()> {
        self.terminate_descendants()?;
        if let Some(child) = self.child.as_mut() {
            child.wait()?;
            self.child.take();
        }
        if !self.termination_complete()? {
            return Err(io::Error::new(
                io::ErrorKind::TimedOut,
                "process tree termination was not observed",
            ));
        }
        Ok(())
    }

    /// Signals termination to the entire tree while retaining the direct child
    /// so its captured output can still be collected.
    pub fn terminate_descendants(&mut self) -> io::Result<()> {
        self.guard.terminate()
    }

    /// Returns whether the platform-owned process boundary has disappeared.
    /// A successful termination request alone is not proof of completion.
    pub fn termination_complete(&mut self) -> io::Result<bool> {
        self.guard.termination_complete()
    }
}

impl Drop for ProcessTree {
    fn drop(&mut self) {
        let _ = self.terminate();
    }
}

#[cfg(unix)]
mod platform {
    use std::io;
    use std::os::unix::process::CommandExt as _;
    use std::process::{Child, Command};

    pub(super) struct Guard {
        process_group: libc::pid_t,
        active: bool,
    }

    pub(super) fn spawn(command: &mut Command) -> io::Result<(Child, Guard)> {
        command.process_group(0);
        let child = command.spawn()?;
        let process_group = libc::pid_t::try_from(child.id())
            .map_err(|_| io::Error::other("child process id does not fit pid_t"))?;
        Ok((
            child,
            Guard {
                process_group,
                active: true,
            },
        ))
    }

    pub(super) fn adopt(pid: u32) -> io::Result<Guard> {
        let process_group = libc::pid_t::try_from(pid)
            .map_err(|_| io::Error::other("process id does not fit pid_t"))?;
        // The host launcher requests a detached root. Require the observed
        // process-group identity before retaining the group as our authority;
        // a bare PID is never enough to authorize later cleanup.
        // SAFETY: querying the process group does not mutate process state.
        let observed_group = unsafe { libc::getpgid(process_group) };
        if observed_group != process_group {
            return Err(io::Error::new(
                io::ErrorKind::PermissionDenied,
                "process is not the leader of its detached process group",
            ));
        }
        // SAFETY: a zero signal is an existence probe only.
        if unsafe { libc::kill(process_group, 0) } != 0 {
            return Err(io::Error::last_os_error());
        }
        Ok(Guard {
            process_group,
            active: true,
        })
    }

    impl Guard {
        pub(super) fn terminate(&mut self) -> io::Result<()> {
            if !self.active {
                return Ok(());
            }
            // SAFETY: a negative, nonzero pid addresses exactly this owned
            // process group; SIGKILL requires no shared memory or signal data.
            if unsafe { libc::kill(-self.process_group, libc::SIGKILL) } == 0 {
                return Ok(());
            }
            let error = io::Error::last_os_error();
            if error.raw_os_error() == Some(libc::ESRCH) {
                self.active = false;
                Ok(())
            } else {
                Err(error)
            }
        }

        pub(super) fn termination_complete(&mut self) -> io::Result<bool> {
            if !self.active {
                return Ok(true);
            }
            // SAFETY: a negative, nonzero pid addresses exactly this process
            // group. No signal is sent by the existence probe.
            if unsafe { libc::kill(-self.process_group, 0) } == 0 {
                return Ok(false);
            }
            let error = io::Error::last_os_error();
            if error.raw_os_error() == Some(libc::ESRCH) {
                self.active = false;
                Ok(true)
            } else if error.raw_os_error() == Some(libc::EPERM) {
                Ok(false)
            } else {
                Err(error)
            }
        }
    }
}

#[cfg(test)]
#[allow(clippy::expect_used)]
mod tests {
    use super::ProcessTree;
    use std::fs;
    #[cfg(unix)]
    use std::os::unix::process::CommandExt as _;
    use std::process::{Command, Stdio};
    use std::thread;
    use std::time::{Duration, Instant};

    const MODE: &str = "ACYCLIC_PROCESS_TREE_TEST_MODE";
    const ROOT: &str = "ACYCLIC_PROCESS_TREE_TEST_ROOT";

    #[test]
    fn process_tree_helper() {
        let Ok(mode) = std::env::var(MODE) else {
            return;
        };
        let root = std::path::PathBuf::from(std::env::var_os(ROOT).expect("helper root"));
        if mode == "grandchild" {
            fs::write(root.join("grandchild-ready"), b"ready").expect("grandchild ready");
            thread::sleep(Duration::from_millis(750));
            fs::write(root.join("escaped"), b"descendant survived").expect("escaped marker");
            return;
        }
        if mode == "orphan" {
            let mut grandchild = Command::new(std::env::current_exe().expect("test executable"));
            grandchild
                .args(["--exact", "process_tree::tests::process_tree_helper"])
                .env(MODE, "grandchild")
                .env(ROOT, &root)
                .stdin(Stdio::null())
                .stdout(Stdio::null())
                .stderr(Stdio::null());
            let _ = grandchild.spawn().expect("spawn orphan descendant");
            let deadline = Instant::now() + Duration::from_secs(5);
            while !root.join("grandchild-ready").exists() && Instant::now() < deadline {
                thread::sleep(Duration::from_millis(10));
            }
            assert!(root.join("grandchild-ready").exists());
            fs::write(root.join("orphan-ready"), b"ready").expect("orphan ready");
            return;
        }
        assert_eq!(mode, "child");
        let mut grandchild = Command::new(std::env::current_exe().expect("test executable"));
        grandchild
            .args(["--exact", "process_tree::tests::process_tree_helper"])
            .env(MODE, "grandchild")
            .env(ROOT, &root)
            .stdin(Stdio::null())
            .stdout(Stdio::null())
            .stderr(Stdio::null());
        let mut grandchild = grandchild.spawn().expect("spawn grandchild");
        let deadline = Instant::now() + Duration::from_secs(5);
        while !root.join("grandchild-ready").exists() && Instant::now() < deadline {
            thread::sleep(Duration::from_millis(10));
        }
        assert!(root.join("grandchild-ready").exists());
        fs::write(root.join("tree-ready"), b"ready").expect("tree ready");
        let _ = grandchild.wait();
    }

    #[test]
    fn termination_contains_descendants() {
        let temporary = tempfile::tempdir().expect("temporary process-tree directory");
        let mut command = Command::new(std::env::current_exe().expect("test executable"));
        command
            .args(["--exact", "process_tree::tests::process_tree_helper"])
            .env(MODE, "child")
            .env(ROOT, temporary.path())
            .stdin(Stdio::null())
            .stdout(Stdio::null())
            .stderr(Stdio::null());
        let mut tree = ProcessTree::spawn(&mut command).expect("spawn process tree");
        let deadline = Instant::now() + Duration::from_secs(5);
        while !temporary.path().join("tree-ready").exists() && Instant::now() < deadline {
            thread::sleep(Duration::from_millis(10));
        }
        assert!(temporary.path().join("tree-ready").exists());
        tree.terminate().expect("terminate process tree");
        thread::sleep(Duration::from_secs(1));
        assert!(!temporary.path().join("escaped").exists());
    }

    #[test]
    fn termination_contains_descendants_after_root_exit() {
        let temporary = tempfile::tempdir().expect("temporary process-tree directory");
        let mut command = Command::new(std::env::current_exe().expect("test executable"));
        command
            .args(["--exact", "process_tree::tests::process_tree_helper"])
            .env(MODE, "orphan")
            .env(ROOT, temporary.path())
            .stdin(Stdio::null())
            .stdout(Stdio::null())
            .stderr(Stdio::null());
        let mut tree = ProcessTree::spawn(&mut command).expect("spawn root-exits-first tree");
        let deadline = Instant::now() + Duration::from_secs(5);
        while !temporary.path().join("orphan-ready").exists() && Instant::now() < deadline {
            thread::sleep(Duration::from_millis(10));
        }
        assert!(temporary.path().join("orphan-ready").exists());
        while tree.try_wait().expect("observe root exit").is_none() && Instant::now() < deadline {
            thread::sleep(Duration::from_millis(10));
        }
        assert!(tree.try_wait().expect("observe root exit").is_some());
        tree.terminate().expect("terminate after root exit");
        thread::sleep(Duration::from_secs(1));
        assert!(!temporary.path().join("escaped").exists());
    }

    #[cfg(unix)]
    #[test]
    fn adoption_contains_descendants_without_recovering_a_pid() {
        let temporary = tempfile::tempdir().expect("temporary process-tree directory");
        let mut command = Command::new(std::env::current_exe().expect("test executable"));
        command
            .args(["--exact", "process_tree::tests::process_tree_helper"])
            .env(MODE, "child")
            .env(ROOT, temporary.path())
            .stdin(Stdio::null())
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .process_group(0);
        let mut child = command.spawn().expect("spawn adopted process tree");
        let deadline = Instant::now() + Duration::from_secs(5);
        while !temporary.path().join("tree-ready").exists() && Instant::now() < deadline {
            thread::sleep(Duration::from_millis(10));
        }
        assert!(temporary.path().join("tree-ready").exists());
        let mut owner = ProcessTree::adopt(child.id()).expect("adopt process tree");
        owner.terminate().expect("terminate adopted process tree");
        let _ = child.wait();
        thread::sleep(Duration::from_secs(1));
        assert!(!temporary.path().join("escaped").exists());
    }

    #[cfg(windows)]
    #[test]
    fn adoption_contains_descendants_without_recovering_a_pid() {
        let temporary = tempfile::tempdir().expect("temporary process-tree directory");
        let mut command = Command::new(std::env::current_exe().expect("test executable"));
        command
            .args(["--exact", "process_tree::tests::process_tree_helper"])
            .env(MODE, "child")
            .env(ROOT, temporary.path())
            .stdin(Stdio::null())
            .stdout(Stdio::null())
            .stderr(Stdio::null());
        let mut child = command.spawn().expect("spawn adopted process tree");
        let deadline = Instant::now() + Duration::from_secs(5);
        while !temporary.path().join("tree-ready").exists() && Instant::now() < deadline {
            thread::sleep(Duration::from_millis(10));
        }
        assert!(temporary.path().join("tree-ready").exists());
        let mut owner = ProcessTree::adopt(child.id()).expect("adopt process tree");
        owner.terminate().expect("terminate adopted process tree");
        let _ = child.wait();
        thread::sleep(Duration::from_secs(1));
        assert!(!temporary.path().join("escaped").exists());
    }
}

#[cfg(windows)]
mod platform {
    use std::io;
    use std::mem::{size_of, zeroed};
    use std::os::windows::io::AsRawHandle as _;
    use std::os::windows::process::CommandExt as _;
    use std::process::{Child, Command};
    use windows_sys::Win32::Foundation::{CloseHandle, HANDLE, INVALID_HANDLE_VALUE};
    use windows_sys::Win32::System::Diagnostics::ToolHelp::{
        CreateToolhelp32Snapshot, Process32First, Process32Next, Thread32First, Thread32Next,
        PROCESSENTRY32, TH32CS_SNAPPROCESS, TH32CS_SNAPTHREAD, THREADENTRY32,
    };
    use windows_sys::Win32::System::JobObjects::{
        AssignProcessToJobObject, CreateJobObjectW, JobObjectBasicAccountingInformation,
        JobObjectExtendedLimitInformation, QueryInformationJobObject, SetInformationJobObject,
        TerminateJobObject, JOBOBJECT_BASIC_ACCOUNTING_INFORMATION,
        JOBOBJECT_EXTENDED_LIMIT_INFORMATION, JOB_OBJECT_LIMIT_KILL_ON_JOB_CLOSE,
    };
    use windows_sys::Win32::System::Threading::{
        OpenProcess, OpenThread, ResumeThread, CREATE_NEW_PROCESS_GROUP, CREATE_SUSPENDED,
        PROCESS_QUERY_LIMITED_INFORMATION, PROCESS_SET_QUOTA, PROCESS_TERMINATE,
        THREAD_SUSPEND_RESUME,
    };

    pub(super) struct Guard {
        job: HANDLE,
        active: bool,
    }

    pub(super) fn spawn(command: &mut Command) -> io::Result<(Child, Guard)> {
        let mut guard = Guard::new()?;
        command.creation_flags(CREATE_SUSPENDED | CREATE_NEW_PROCESS_GROUP);
        let mut child = command.spawn()?;
        // SAFETY: the Job and Child each own live handles for the duration of
        // this call. The suspended child cannot create descendants before it
        // has been assigned to the kill-on-close Job.
        if unsafe { AssignProcessToJobObject(guard.job, child.as_raw_handle().cast()) } == 0 {
            let error = io::Error::last_os_error();
            let _ = guard.terminate();
            let _ = child.kill();
            let _ = child.wait();
            return Err(error);
        }
        if let Err(error) = resume_process(child.id()) {
            let _ = guard.terminate();
            let _ = child.wait();
            return Err(error);
        }
        Ok((child, guard))
    }

    pub(super) fn adopt(pid: u32) -> io::Result<Guard> {
        let guard = Guard::new()?;
        // Keep the process handle only for the assignment operation. The Job
        // handle retained by `Guard` is the durable authority and cannot be
        // redirected to a later process that reuses this PID.
        let process = unsafe {
            OpenProcess(
                PROCESS_QUERY_LIMITED_INFORMATION | PROCESS_SET_QUOTA | PROCESS_TERMINATE,
                0,
                pid,
            )
        };
        if process.is_null() {
            let error = io::Error::last_os_error();
            drop(guard);
            return Err(error);
        }
        let assigned = unsafe { AssignProcessToJobObject(guard.job, process) } != 0;
        let error = if assigned {
            assign_descendants(guard.job, pid).err()
        } else {
            Some(io::Error::last_os_error())
        };
        unsafe { CloseHandle(process) };
        if let Some(error) = error {
            drop(guard);
            return Err(error);
        }
        Ok(guard)
    }

    fn assign_descendants(job: HANDLE, root_pid: u32) -> io::Result<()> {
        let mut owned = vec![root_pid];
        loop {
            let snapshot = unsafe { CreateToolhelp32Snapshot(TH32CS_SNAPPROCESS, 0) };
            if snapshot == INVALID_HANDLE_VALUE {
                return Err(io::Error::last_os_error());
            }
            let mut entry: PROCESSENTRY32 = unsafe { zeroed() };
            entry.dwSize = u32::try_from(size_of::<PROCESSENTRY32>())
                .map_err(|_| io::Error::other("process entry structure is too large"))?;
            let mut found = unsafe { Process32First(snapshot, &raw mut entry) } != 0;
            let mut added = 0;
            while found {
                if owned.contains(&entry.th32ParentProcessID)
                    && !owned.contains(&entry.th32ProcessID)
                {
                    let process = unsafe {
                        OpenProcess(
                            PROCESS_QUERY_LIMITED_INFORMATION
                                | PROCESS_SET_QUOTA
                                | PROCESS_TERMINATE,
                            0,
                            entry.th32ProcessID,
                        )
                    };
                    if process.is_null() {
                        let error = io::Error::last_os_error();
                        unsafe { CloseHandle(snapshot) };
                        return Err(error);
                    }
                    let assigned = unsafe { AssignProcessToJobObject(job, process) } != 0;
                    let error = if assigned {
                        None
                    } else {
                        Some(io::Error::last_os_error())
                    };
                    unsafe { CloseHandle(process) };
                    if let Some(error) = error {
                        unsafe { CloseHandle(snapshot) };
                        return Err(error);
                    }
                    owned.push(entry.th32ProcessID);
                    added += 1;
                }
                found = unsafe { Process32Next(snapshot, &raw mut entry) } != 0;
            }
            unsafe { CloseHandle(snapshot) };
            if added == 0 {
                return Ok(());
            }
        }
    }

    impl Guard {
        fn new() -> io::Result<Self> {
            // SAFETY: null security attributes and name request an unnamed Job
            // with default access controls.
            let job = unsafe { CreateJobObjectW(std::ptr::null(), std::ptr::null()) };
            if job.is_null() {
                return Err(io::Error::last_os_error());
            }
            // SAFETY: the structure is plain Win32 data and zero is its
            // documented initial state before setting the requested limit.
            let mut limits: JOBOBJECT_EXTENDED_LIMIT_INFORMATION = unsafe { zeroed() };
            limits.BasicLimitInformation.LimitFlags = JOB_OBJECT_LIMIT_KILL_ON_JOB_CLOSE;
            // SAFETY: `limits` is initialized and its exact size is provided.
            let configured = unsafe {
                SetInformationJobObject(
                    job,
                    JobObjectExtendedLimitInformation,
                    (&raw const limits).cast(),
                    u32::try_from(size_of::<JOBOBJECT_EXTENDED_LIMIT_INFORMATION>())
                        .map_err(|_| io::Error::other("Job limit structure is too large"))?,
                )
            };
            if configured == 0 {
                let error = io::Error::last_os_error();
                // SAFETY: `job` is an owned live handle.
                unsafe { CloseHandle(job) };
                return Err(error);
            }
            Ok(Self { job, active: true })
        }

        pub(super) fn terminate(&mut self) -> io::Result<()> {
            if !self.active {
                return Ok(());
            }
            // SAFETY: `job` remains owned until Drop closes it.
            if unsafe { TerminateJobObject(self.job, 1) } == 0 {
                Err(io::Error::last_os_error())
            } else {
                Ok(())
            }
        }

        pub(super) fn termination_complete(&mut self) -> io::Result<bool> {
            if !self.active {
                return Ok(true);
            }
            let mut accounting: JOBOBJECT_BASIC_ACCOUNTING_INFORMATION = unsafe { zeroed() };
            let mut returned = 0;
            let queried = unsafe {
                QueryInformationJobObject(
                    self.job,
                    JobObjectBasicAccountingInformation,
                    (&mut accounting as *mut JOBOBJECT_BASIC_ACCOUNTING_INFORMATION).cast(),
                    u32::try_from(size_of::<JOBOBJECT_BASIC_ACCOUNTING_INFORMATION>())
                        .map_err(|_| io::Error::other("Job accounting structure is too large"))?,
                    &mut returned,
                )
            };
            if queried == 0 {
                return Err(io::Error::last_os_error());
            }
            if accounting.ActiveProcesses == 0 {
                self.active = false;
                Ok(true)
            } else {
                Ok(false)
            }
        }
    }

    impl Drop for Guard {
        fn drop(&mut self) {
            let _ = self.terminate();
            // SAFETY: `job` is owned by this guard and closed exactly once.
            unsafe { CloseHandle(self.job) };
        }
    }

    fn resume_process(process_id: u32) -> io::Result<()> {
        // SAFETY: flags request a system-owned snapshot handle.
        let snapshot = unsafe { CreateToolhelp32Snapshot(TH32CS_SNAPTHREAD, 0) };
        if snapshot == INVALID_HANDLE_VALUE {
            return Err(io::Error::last_os_error());
        }
        // SAFETY: zero is the documented initialization; dwSize is set below.
        let mut entry: THREADENTRY32 = unsafe { zeroed() };
        entry.dwSize = u32::try_from(size_of::<THREADENTRY32>())
            .map_err(|_| io::Error::other("thread entry structure is too large"))?;
        // SAFETY: snapshot and entry are live for the iteration.
        let mut found = unsafe { Thread32First(snapshot, &raw mut entry) } != 0;
        let mut result = Err(io::Error::new(
            io::ErrorKind::NotFound,
            "suspended child thread was not found",
        ));
        while found {
            if entry.th32OwnerProcessID == process_id {
                // SAFETY: the thread id came from the live system snapshot.
                let thread = unsafe { OpenThread(THREAD_SUSPEND_RESUME, 0, entry.th32ThreadID) };
                if thread.is_null() {
                    result = Err(io::Error::last_os_error());
                } else {
                    // SAFETY: `thread` is a live handle with resume access.
                    result = if unsafe { ResumeThread(thread) } == u32::MAX {
                        Err(io::Error::last_os_error())
                    } else {
                        Ok(())
                    };
                    // SAFETY: this function owns the thread handle.
                    unsafe { CloseHandle(thread) };
                }
                break;
            }
            // SAFETY: snapshot and entry remain live for the next iteration.
            found = unsafe { Thread32Next(snapshot, &raw mut entry) } != 0;
        }
        // SAFETY: this function owns the snapshot handle.
        unsafe { CloseHandle(snapshot) };
        result
    }
}
