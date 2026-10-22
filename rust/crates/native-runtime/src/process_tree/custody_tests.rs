//! Custody ties a tree to its owner: once a reopened owner's fence returns,
//! no process of a tree whose owner was killed exists, and an ordinary tree
//! without custody demonstrably survives the same kill.
#![cfg(any(target_os = "linux", windows))]

use super::{ProcessCustody, ProcessTree};
use std::fs;
use std::path::{Path, PathBuf};
use std::process::{Command, Stdio};
use std::thread;
use std::time::Duration;

const MODE: &str = "ACYCLIC_CUSTODY_TEST_MODE";
const ROOT: &str = "ACYCLIC_CUSTODY_TEST_ROOT";

/// A long-lived program whose PID the owner publishes. On Unix it is a
/// background grandchild, so only whole-tree custody reaches it.
fn program(root: &Path) -> Command {
    #[cfg(unix)]
    {
        let pid = root.join("pid");
        let mut command = crate::shell_command(
            &format!(
                "sleep 3600 & echo $! > '{0}.tmp'; mv '{0}.tmp' '{0}'; wait",
                pid.display()
            ),
            Some("sh"),
        );
        command
            .stdin(Stdio::null())
            .stdout(Stdio::null())
            .stderr(Stdio::null());
        command
    }
    #[cfg(windows)]
    {
        let _ = root;
        let mut command = Command::new("ping");
        command
            .args(["-n", "3600", "127.0.0.1"])
            .stdin(Stdio::null())
            .stdout(Stdio::null())
            .stderr(Stdio::null());
        command
    }
}

/// The owner: spawns the program, publishes its PID, and lives until killed.
#[test]
fn custody_owner_helper() {
    let Ok(mode) = std::env::var(MODE) else {
        return;
    };
    let root = PathBuf::from(std::env::var_os(ROOT).expect("owner root"));
    let mut command = program(&root);
    let tree = if mode == "custodied" {
        let custody = ProcessCustody::open(&root.join("processes")).expect("custody");
        Some(ProcessTree::spawn_in_custody(&mut command, &custody).expect("custodied spawn"))
    } else {
        let child = command.spawn().expect("uncontained spawn");
        #[cfg(windows)]
        fs::write(root.join("pid"), child.id().to_string()).expect("pid");
        drop(child);
        None
    };
    #[cfg(windows)]
    if let Some(tree) = &tree {
        let pid = tree
            .child
            .as_ref()
            .map(std::process::Child::id)
            .expect("child id");
        fs::write(root.join("pid.tmp"), pid.to_string()).expect("pid");
        fs::rename(root.join("pid.tmp"), root.join("pid")).expect("pid");
    }
    fs::write(root.join("owner-ready"), b"ready").expect("ready");
    let _tree = tree;
    loop {
        thread::park();
    }
}

fn owner(mode: &str, root: &Path) -> std::process::Child {
    Command::new(std::env::current_exe().expect("test executable"))
        .args([
            "--exact",
            "process_tree::custody_tests::custody_owner_helper",
            "--nocapture",
        ])
        .env(MODE, mode)
        .env(ROOT, root)
        .stdin(Stdio::null())
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .spawn()
        .expect("owner")
}

/// Waits for the owner's readiness, then reads the program's PID.
fn published(root: &Path, owner: &mut std::process::Child) -> u32 {
    while !(root.join("owner-ready").exists() && root.join("pid").exists()) {
        assert!(
            owner.try_wait().expect("owner").is_none(),
            "owner exited early"
        );
        thread::sleep(Duration::from_millis(5));
    }
    fs::read_to_string(root.join("pid"))
        .expect("pid")
        .trim()
        .parse()
        .expect("numeric pid")
}

/// An exit watch on one process, independent of its parentage.
struct Watch {
    #[cfg(unix)]
    fd: std::os::fd::OwnedFd,
    #[cfg(windows)]
    handle: windows_sys::Win32::Foundation::HANDLE,
    #[cfg(unix)]
    pid: u32,
}

impl Watch {
    /// None when no such process exists any more.
    #[cfg(unix)]
    fn new(pid: u32) -> Option<Self> {
        use std::os::fd::FromRawFd as _;
        // SAFETY: pidfd_open takes a PID and zero flags.
        let fd = unsafe { libc::syscall(libc::SYS_pidfd_open, pid, 0) };
        let fd = libc::c_int::try_from(fd).expect("pidfd");
        if fd < 0 {
            let error = std::io::Error::last_os_error();
            assert_eq!(
                error.raw_os_error(),
                Some(libc::ESRCH),
                "pidfd_open: {error}"
            );
            return None;
        }
        // SAFETY: the descriptor pidfd_open just returned.
        let fd = unsafe { std::os::fd::OwnedFd::from_raw_fd(fd) };
        Some(Self { fd, pid })
    }

    #[cfg(windows)]
    fn new(pid: u32) -> Option<Self> {
        use windows_sys::Win32::System::Threading::{
            OpenProcess, PROCESS_SYNCHRONIZE, PROCESS_TERMINATE,
        };
        // SAFETY: opening a handle to a published PID; owned below.
        let handle = unsafe { OpenProcess(PROCESS_SYNCHRONIZE | PROCESS_TERMINATE, 0, pid) };
        if handle.is_null() {
            let error = std::io::Error::last_os_error();
            // ERROR_INVALID_PARAMETER: no process has this PID.
            assert_eq!(error.raw_os_error(), Some(87), "OpenProcess: {error}");
            return None;
        }
        Some(Self { handle })
    }

    /// Whether the process has exited, without waiting.
    fn exited(&self) -> bool {
        #[cfg(unix)]
        {
            use std::os::fd::AsRawFd as _;
            let mut ready = libc::pollfd {
                fd: self.fd.as_raw_fd(),
                events: libc::POLLIN,
                revents: 0,
            };
            // SAFETY: one initialized pollfd; zero timeout.
            let polled = unsafe { libc::poll(&raw mut ready, 1, 0) };
            assert!(polled >= 0, "poll: {}", std::io::Error::last_os_error());
            polled == 1
        }
        #[cfg(windows)]
        {
            use windows_sys::Win32::Foundation::WAIT_OBJECT_0;
            use windows_sys::Win32::System::Threading::WaitForSingleObject;
            // SAFETY: an owned process handle; zero timeout.
            unsafe { WaitForSingleObject(self.handle, 0) == WAIT_OBJECT_0 }
        }
    }

    /// Blocks until the process has exited.
    #[cfg(target_os = "linux")]
    fn wait(&self) {
        use std::os::fd::AsRawFd as _;
        let mut ready = libc::pollfd {
            fd: self.fd.as_raw_fd(),
            events: libc::POLLIN,
            revents: 0,
        };
        // SAFETY: one initialized pollfd; no timeout.
        let polled = unsafe { libc::poll(&raw mut ready, 1, -1) };
        assert_eq!(polled, 1, "poll: {}", std::io::Error::last_os_error());
    }

    fn kill(&self) {
        #[cfg(unix)]
        // SAFETY: the published, still-watched process.
        unsafe {
            libc::kill(libc::pid_t::try_from(self.pid).expect("pid"), libc::SIGKILL);
        }
        #[cfg(windows)]
        // SAFETY: an owned handle with terminate access.
        unsafe {
            windows_sys::Win32::System::Threading::TerminateProcess(self.handle, 1);
        }
    }
}

#[cfg(windows)]
impl Drop for Watch {
    fn drop(&mut self) {
        // SAFETY: the handle opened in `new`.
        unsafe { windows_sys::Win32::Foundation::CloseHandle(self.handle) };
    }
}

fn killed_owner_leaves(mode: &str) -> (tempfile::TempDir, Watch) {
    let root = tempfile::tempdir().expect("root");
    let mut owner = owner(mode, root.path());
    let pid = published(root.path(), &mut owner);
    let watch = Watch::new(pid).expect("the published program is running");
    owner.kill().expect("kill owner");
    owner.wait().expect("reap owner");
    (root, watch)
}

#[test]
fn reopened_owner_fence_returns_only_after_the_killed_owners_tree_is_dead() {
    let (root, watch) = killed_owner_leaves("custodied");
    let processes = root.path().join("processes");
    ProcessCustody::open(&processes)
        .expect("reopen custody")
        .fence_stale()
        .expect("fence");
    assert!(watch.exited(), "a killed owner's tree outlived the fence");
    assert_eq!(fs::read_dir(&processes).expect("records").count(), 0);
}

/// Negative control: the same program outside custody survives its owner.
#[test]
fn an_uncustodied_tree_outlives_its_killed_owner() {
    let (_root, watch) = killed_owner_leaves("uncontained");
    let survived = !watch.exited();
    watch.kill();
    assert!(survived, "the oracle cannot observe a surviving tree");
}

#[cfg(target_os = "linux")]
#[test]
fn custodied_tree_reports_the_programs_outcome_and_leaves_no_descendant() {
    use std::os::unix::process::ExitStatusExt as _;
    let root = tempfile::tempdir().expect("root");
    let custody = ProcessCustody::open(&root.path().join("processes")).expect("custody");
    let pid = root.path().join("pid");
    for (script, code, signal) in [
        ("exit 7", Some(7), None),
        ("kill -TERM $$", None, Some(libc::SIGTERM)),
    ] {
        let mut command = crate::shell_command(
            &format!("sleep 3600 & echo $! > '{}'; {script}", pid.display()),
            Some("sh"),
        );
        command
            .stdin(Stdio::null())
            .stdout(Stdio::null())
            .stderr(Stdio::null());
        let mut tree = ProcessTree::spawn_in_custody(&mut command, &custody).expect("spawn");
        let status = tree.wait(Duration::from_secs(30)).expect("wait");
        let descendant = Watch::new(
            fs::read_to_string(&pid)
                .expect("pid")
                .trim()
                .parse()
                .expect("numeric pid"),
        );
        // The warden drains the tree before it exits, so the reaped status
        // already orders the descendant's death.
        assert!(
            descendant.is_none_or(|watch| watch.exited()),
            "a descendant outlived its program"
        );
        tree.terminate(None).expect("terminate");
        assert_eq!(status.code(), code);
        assert_eq!(status.signal(), signal);
        assert_eq!(
            fs::read_dir(root.path().join("processes"))
                .expect("records")
                .count(),
            0
        );
    }
}

fn tree_pid(tree: &ProcessTree) -> u32 {
    #[cfg(unix)]
    {
        tree.owned_child_id().expect("child id")
    }
    #[cfg(windows)]
    {
        tree.child
            .as_ref()
            .map(std::process::Child::id)
            .expect("child id")
    }
}

/// A second owner in the same process shares the live custody: it neither
/// waits on nor kills the first owner's running tree.
#[test]
fn a_second_owner_in_one_process_shares_the_live_custody() {
    let root = tempfile::tempdir().expect("root");
    let processes = root.path().join("processes");
    let first = ProcessCustody::acquire(&processes).expect("first owner");
    #[cfg(unix)]
    let mut command = crate::shell_command("sleep 30", Some("sh"));
    #[cfg(windows)]
    let mut command = {
        let mut command = Command::new("ping");
        command.args(["-n", "30", "127.0.0.1"]);
        command
    };
    command
        .stdin(Stdio::null())
        .stdout(Stdio::null())
        .stderr(Stdio::null());
    let mut tree = ProcessTree::spawn_in_custody(&mut command, &first).expect("spawn");
    let watch = Watch::new(tree_pid(&tree)).expect("running tree");
    let second = ProcessCustody::acquire(&processes).expect("second owner");
    // A fence would have returned only once the tree was dead.
    let running = !watch.exited();
    tree.terminate(None).expect("terminate");
    assert!(std::sync::Arc::ptr_eq(&first, &second));
    assert!(running, "a second owner fenced a live owner's running tree");
}

/// A warden killed outright while its owner lives stays an unreaped zombie
/// that keeps its PID and group while its orphaned tree still runs; the
/// fence recognizes it by its recorded start and kills that tree.
#[cfg(target_os = "linux")]
#[test]
fn fence_kills_the_tree_of_a_zombie_warden() {
    let root = tempfile::tempdir().expect("root");
    let processes = root.path().join("processes");
    let custody = ProcessCustody::open(&processes).expect("custody");
    let pid = root.path().join("pid");
    let mut command = crate::shell_command(
        &format!(
            "sleep 3600 & echo $! > '{0}.tmp'; mv '{0}.tmp' '{0}'; wait",
            pid.display()
        ),
        Some("sh"),
    );
    command
        .stdin(Stdio::null())
        .stdout(Stdio::null())
        .stderr(Stdio::null());
    let mut tree = ProcessTree::spawn_in_custody(&mut command, &custody).expect("spawn");
    while !pid.exists() {
        thread::sleep(Duration::from_millis(5));
    }
    let descendant = Watch::new(
        fs::read_to_string(&pid)
            .expect("pid")
            .trim()
            .parse()
            .expect("numeric pid"),
    )
    .expect("running descendant");
    let warden = libc::pid_t::try_from(tree_pid(&tree)).expect("warden pid");
    let warden_watch = Watch::new(tree_pid(&tree)).expect("running warden");
    // SAFETY: this test's own unreaped child.
    assert_eq!(unsafe { libc::kill(warden, libc::SIGKILL) }, 0);
    warden_watch.wait();
    let survived = !descendant.exited();
    ProcessCustody::open(&processes)
        .expect("reopen custody")
        .fence_stale()
        .expect("fence");
    let fenced = descendant.exited();
    tree.terminate(None).expect("terminate");
    assert!(survived, "the zombie warden's tree must outlive the warden");
    assert!(fenced, "the fence left a zombie warden's tree running");
}

/// A program whose descendant leaves the tree's process group and session,
/// publishing its own PID.
#[cfg(target_os = "linux")]
fn escaping_program(pid: &Path) -> Command {
    let mut command = crate::shell_command(
        &format!(
            "setsid sh -c 'echo $$ > \"{0}.tmp\"; mv \"{0}.tmp\" \"{0}\"; exec sleep 3600' & wait",
            pid.display()
        ),
        Some("sh"),
    );
    command
        .stdin(Stdio::null())
        .stdout(Stdio::null())
        .stderr(Stdio::null());
    command
}

/// Starts `command`, waits for its escaped descendant, and cancels the tree.
/// Returns the descendant's watch once cancellation has returned.
#[cfg(target_os = "linux")]
fn cancel_after_escape(mut tree: ProcessTree, pid: &Path) -> Watch {
    while !pid.exists() {
        assert!(
            tree.try_wait(None).expect("try_wait").is_none(),
            "the tree exited before its descendant escaped"
        );
        thread::sleep(Duration::from_millis(5));
    }
    let escaped = Watch::new(
        fs::read_to_string(pid)
            .expect("pid")
            .trim()
            .parse()
            .expect("numeric pid"),
    )
    .expect("running descendant");
    tree.terminate(None).expect("terminate");
    escaped
}

/// Cancelling a custodied tree kills its group, never its warden, which
/// then drains every descendant, including one in another session, before
/// the owner observes the tree gone.
#[cfg(target_os = "linux")]
#[test]
fn cancelled_custodied_tree_leaves_no_descendant_outside_its_group() {
    let root = tempfile::tempdir().expect("root");
    let processes = root.path().join("processes");
    let custody = ProcessCustody::open(&processes).expect("custody");
    let pid = root.path().join("pid");
    let tree = ProcessTree::spawn_in_custody(&mut escaping_program(&pid), &custody).expect("spawn");
    let escaped = cancel_after_escape(tree, &pid);
    let survived = !escaped.exited();
    escaped.kill();
    assert!(!survived, "a cancelled tree's escaped descendant survived");
    assert_eq!(fs::read_dir(&processes).expect("records").count(), 0);
}

/// Negative control: the same cancellation without custody leaves the
/// escaped descendant running.
#[cfg(target_os = "linux")]
#[test]
fn a_cancelled_uncustodied_tree_leaves_its_escaped_descendant() {
    let root = tempfile::tempdir().expect("root");
    let pid = root.path().join("pid");
    let tree = ProcessTree::spawn(&mut escaping_program(&pid)).expect("spawn");
    let escaped = cancel_after_escape(tree, &pid);
    let survived = !escaped.exited();
    escaped.kill();
    assert!(survived, "the oracle cannot observe an escaped descendant");
}

const ANCHOR_MODE: &str = "ACYCLIC_CUSTODY_ANCHOR_MODE";

/// A stand-in warden: holds a record lock until exit, forks a child, and
/// publishes the child's PID. "anchor" forks the warden's group anchor;
/// "paused" forks a child that keeps every inherited descriptor and only
/// sleeps.
#[cfg(target_os = "linux")]
#[test]
fn anchor_parent_helper() {
    let Ok(mode) = std::env::var(ANCHOR_MODE) else {
        return;
    };
    let root = PathBuf::from(std::env::var_os(ROOT).expect("helper root"));
    let record = fs::File::create(root.join("record")).expect("record");
    crate::FileLock::try_hold_until_exit(record, crate::FileLockMode::Exclusive)
        .expect("record lock");
    let child = if mode == "anchor" {
        super::warden::spawn_anchor().expect("anchor")
    } else {
        // SAFETY: the child only sleeps, which is async-signal-safe.
        match unsafe { libc::fork() } {
            0 => loop {
                // SAFETY: sleeping until a signal.
                unsafe { libc::pause() };
            },
            pid => pid,
        }
    };
    assert!(child > 0, "fork: {}", std::io::Error::last_os_error());
    fs::write(root.join("child.tmp"), child.to_string()).expect("child pid");
    fs::rename(root.join("child.tmp"), root.join("child")).expect("child pid");
    loop {
        thread::park();
    }
}

/// Kills a stand-in warden that forked a child in `mode`, then reports
/// whether the record lock was free once the child was gone or, for a
/// child that outlives the warden, while it still lives.
#[cfg(target_os = "linux")]
fn record_free_after_killed_parent(mode: &str) -> bool {
    let root = tempfile::tempdir().expect("root");
    let mut parent = Command::new(std::env::current_exe().expect("test executable"))
        .args([
            "--exact",
            "process_tree::custody_tests::anchor_parent_helper",
            "--nocapture",
        ])
        .env(ANCHOR_MODE, mode)
        .env(ROOT, root.path())
        .stdin(Stdio::null())
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .spawn()
        .expect("helper");
    let child = published_child(root.path(), &mut parent);
    let watch = Watch::new(child).expect("running child");
    parent.kill().expect("kill helper");
    parent.wait().expect("reap helper");
    // An anchor exits with its parent; a paused child never does, so only
    // the anchor is awaited, and only a failure meets the bound.
    let bound = if mode == "anchor" { 10_000 } else { 0 };
    let mut ready = libc::pollfd {
        fd: std::os::fd::AsRawFd::as_raw_fd(&watch.fd),
        events: libc::POLLIN,
        revents: 0,
    };
    // SAFETY: one initialized pollfd.
    let exited = unsafe { libc::poll(&raw mut ready, 1, bound) } == 1;
    let record = fs::File::open(root.path().join("record")).expect("record");
    let free = match crate::FileLock::try_acquire(record, crate::FileLockMode::Exclusive) {
        Ok(_lock) => true,
        Err(error) if error.kind() == std::io::ErrorKind::WouldBlock => false,
        Err(error) => panic!("probe: {error}"),
    };
    watch.kill();
    assert_eq!(exited, free, "the lock outlived its last holder");
    free
}

#[cfg(target_os = "linux")]
fn published_child(root: &Path, parent: &mut std::process::Child) -> u32 {
    while !root.join("child").exists() {
        assert!(
            parent.try_wait().expect("helper").is_none(),
            "helper exited early"
        );
        thread::sleep(Duration::from_millis(5));
    }
    fs::read_to_string(root.join("child"))
        .expect("child pid")
        .trim()
        .parse()
        .expect("numeric pid")
}

/// A warden killed while its group anchor lives leaves no holder of its
/// record's lock: the anchor holds none of its descriptors and exits with it.
#[cfg(target_os = "linux")]
#[test]
fn an_anchor_never_outlives_or_holds_for_its_killed_warden() {
    assert!(
        record_free_after_killed_parent("anchor"),
        "a killed warden's anchor kept its record locked"
    );
}

/// Negative control: a child that keeps the inherited descriptors and only
/// sleeps holds the record locked after its parent is killed.
#[cfg(target_os = "linux")]
#[test]
fn a_paused_child_keeps_its_killed_parents_record_locked() {
    assert!(
        !record_free_after_killed_parent("paused"),
        "the oracle cannot observe a lock kept by a surviving child"
    );
}
