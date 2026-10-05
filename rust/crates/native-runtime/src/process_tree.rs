#![allow(unsafe_code)]

use std::io;
use std::process::{Child, ChildStderr, ChildStdout, Command, ExitStatus, Output};

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

    /// Terminates every process in the tree and reaps the direct child.
    pub fn terminate(&mut self) -> io::Result<()> {
        // Always reap the direct child even when the platform tree signal
        // fails. Returning early here would drop `std::process::Child`
        // without waiting; Rust does not terminate a child from `Child`'s
        // destructor, so a failed tree signal could leak a process. Keep the
        // tree error for the caller (the descendant outcome is then unknown),
        // but still make a best-effort direct-child kill and wait.
        let tree_error = self.terminate_descendants().err();
        let child_error = if let Some(mut child) = self.child.take() {
            if tree_error.is_some() {
                let _ = child.kill();
            }
            let mut result = child.wait();
            if result.is_err() {
                let _ = child.kill();
                result = child.wait();
            }
            result.err()
        } else {
            None
        };
        match (tree_error, child_error) {
            (Some(error), _) => Err(error),
            (None, Some(error)) => Err(error),
            (None, None) => Ok(()),
        }
    }

    /// Signals termination to the entire tree while retaining the direct child
    /// so its captured output can still be collected.
    pub fn terminate_descendants(&mut self) -> io::Result<()> {
        self.guard.terminate()
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

    impl Guard {
        pub(super) fn terminate(&mut self) -> io::Result<()> {
            if !self.active {
                return Ok(());
            }
            // SAFETY: a negative, nonzero pid addresses exactly this owned
            // process group; SIGKILL requires no shared memory or signal data.
            if unsafe { libc::kill(-self.process_group, libc::SIGKILL) } == 0 {
                self.active = false;
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
    }
}

#[cfg(test)]
#[allow(clippy::expect_used)]
mod tests {
    use super::ProcessTree;
    use std::fs;
    use std::path::Path;
    use std::process::{Command, Stdio};
    use std::thread;
    use std::time::{Duration, Instant};

    const MODE: &str = "ACYCLIC_PROCESS_TREE_TEST_MODE";
    const ROOT: &str = "ACYCLIC_PROCESS_TREE_TEST_ROOT";

    fn helper_command(mode: &str, root: &Path) -> Command {
        let mut command = Command::new(std::env::current_exe().expect("test executable"));
        command
            .args(["--exact", "process_tree::tests::process_tree_helper"])
            .env(MODE, mode)
            .env(ROOT, root)
            .stdin(Stdio::null())
            .stdout(Stdio::null())
            .stderr(Stdio::null());
        #[cfg(windows)]
        {
            use std::os::windows::process::CommandExt;
            command.creation_flags(0x0800_0000); // CREATE_NO_WINDOW
        }
        command
    }

    #[test]
    fn process_tree_helper() {
        let Ok(mode) = std::env::var(MODE) else {
            return;
        };
        let root = std::path::PathBuf::from(std::env::var_os(ROOT).expect("helper root"));
        if mode == "grandchild" {
            fs::write(root.join("grandchild-pid"), std::process::id().to_string())
                .expect("grandchild pid");
            fs::write(root.join("grandchild-ready"), b"ready").expect("grandchild ready");
            thread::sleep(Duration::from_millis(750));
            fs::write(root.join("escaped"), b"descendant survived").expect("escaped marker");
            return;
        }
        if mode == "parent-exits" {
            let mut grandchild = helper_command("grandchild", &root);
            let _grandchild = grandchild.spawn().expect("spawn grandchild");
            let deadline = Instant::now() + Duration::from_secs(5);
            while !root.join("grandchild-ready").exists() && Instant::now() < deadline {
                thread::sleep(Duration::from_millis(10));
            }
            assert!(root.join("grandchild-ready").exists());
            fs::write(root.join("tree-ready"), b"ready").expect("tree ready");
            return;
        }
        assert_eq!(mode, "child");
        let mut grandchild = helper_command("grandchild", &root);
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
        let mut command = helper_command("child", temporary.path());
        let mut tree = ProcessTree::spawn(&mut command).expect("spawn process tree");
        let deadline = Instant::now() + Duration::from_secs(5);
        while !temporary.path().join("tree-ready").exists() && Instant::now() < deadline {
            thread::sleep(Duration::from_millis(10));
        }
        assert!(temporary.path().join("tree-ready").exists());
        let grandchild_pid = read_pid(temporary.path());
        assert!(
            process_is_alive(grandchild_pid),
            "grandchild exited before termination"
        );
        tree.terminate().expect("terminate process tree");
        wait_for_process_exit(grandchild_pid);
        assert!(!temporary.path().join("escaped").exists());
    }

    #[test]
    fn drop_contains_descendants_after_direct_child_exit() {
        let temporary = tempfile::tempdir().expect("temporary process-tree directory");
        let mut command = helper_command("parent-exits", temporary.path());
        let mut tree = ProcessTree::spawn(&mut command).expect("spawn process tree");
        let deadline = Instant::now() + Duration::from_secs(5);
        while !temporary.path().join("tree-ready").exists() && Instant::now() < deadline {
            thread::sleep(Duration::from_millis(10));
        }
        assert!(temporary.path().join("tree-ready").exists());
        let grandchild_pid = read_pid(temporary.path());
        assert!(
            process_is_alive(grandchild_pid),
            "grandchild exited before parent"
        );
        let deadline = Instant::now() + Duration::from_secs(5);
        let mut exited = false;
        while Instant::now() < deadline {
            if tree.try_wait().expect("poll process tree").is_some() {
                exited = true;
                break;
            }
            thread::sleep(Duration::from_millis(10));
        }
        assert!(exited, "direct child did not exit before the cleanup check");
        drop(tree);
        wait_for_process_exit(grandchild_pid);
        assert!(!temporary.path().join("escaped").exists());
    }

    fn read_pid(root: &Path) -> u32 {
        fs::read_to_string(root.join("grandchild-pid"))
            .expect("grandchild pid marker")
            .trim()
            .parse()
            .expect("valid grandchild pid")
    }

    fn wait_for_process_exit(pid: u32) {
        let deadline = Instant::now() + Duration::from_secs(5);
        while process_is_alive(pid) && Instant::now() < deadline {
            thread::sleep(Duration::from_millis(10));
        }
        assert!(
            !process_is_alive(pid),
            "owned grandchild {pid} remained alive"
        );
    }

    #[cfg(unix)]
    fn process_is_alive(pid: u32) -> bool {
        let pid = match libc::pid_t::try_from(pid) {
            Ok(pid) => pid,
            Err(_) => return false,
        };
        // SAFETY: signal 0 performs an existence check without changing the
        // target process. The PID came from the fixture's own child.
        let result = unsafe { libc::kill(pid, 0) };
        result == 0 || std::io::Error::last_os_error().raw_os_error() == Some(libc::EPERM)
    }

    #[cfg(windows)]
    fn process_is_alive(pid: u32) -> bool {
        use windows_sys::Win32::Foundation::{CloseHandle, STILL_ACTIVE};
        use windows_sys::Win32::System::Threading::{
            GetExitCodeProcess, OpenProcess, PROCESS_QUERY_LIMITED_INFORMATION,
        };

        // SAFETY: the fixture owns this PID and requests query-only access.
        let handle = unsafe { OpenProcess(PROCESS_QUERY_LIMITED_INFORMATION, 0, pid) };
        if handle.is_null() {
            return false;
        }
        let mut exit_code = 0;
        // SAFETY: handle is valid until the close below and exit_code is an
        // initialized out-parameter of the documented size.
        let queried = unsafe { GetExitCodeProcess(handle, &mut exit_code) } != 0;
        // SAFETY: handle was returned by OpenProcess and is closed exactly once.
        unsafe { CloseHandle(handle) };
        queried && exit_code == STILL_ACTIVE
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
        CreateToolhelp32Snapshot, TH32CS_SNAPTHREAD, THREADENTRY32, Thread32First, Thread32Next,
    };
    use windows_sys::Win32::System::JobObjects::{
        AssignProcessToJobObject, CreateJobObjectW, JOB_OBJECT_LIMIT_KILL_ON_JOB_CLOSE,
        JOBOBJECT_EXTENDED_LIMIT_INFORMATION, JobObjectExtendedLimitInformation,
        SetInformationJobObject, TerminateJobObject,
    };
    use windows_sys::Win32::System::Threading::{
        CREATE_NEW_PROCESS_GROUP, CREATE_NO_WINDOW, CREATE_SUSPENDED, OpenThread, ResumeThread,
        THREAD_SUSPEND_RESUME,
    };

    pub(super) struct Guard {
        job: HANDLE,
        active: bool,
    }

    pub(super) fn spawn(command: &mut Command) -> io::Result<(Child, Guard)> {
        let mut guard = Guard::new()?;
        // Keep the process tree hidden as well as contained. Command flags
        // replace the caller's existing flags, so include CREATE_NO_WINDOW
        // here instead of relying on the root command to set it.
        command.creation_flags(CREATE_SUSPENDED | CREATE_NEW_PROCESS_GROUP | CREATE_NO_WINDOW);
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
                self.active = false;
                Ok(())
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
