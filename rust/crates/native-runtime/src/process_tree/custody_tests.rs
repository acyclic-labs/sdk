//! Custody ties a tree to its owner: once a reopened owner's fence returns,
//! no process of a tree whose owner was killed exists, and an ordinary tree
//! without custody demonstrably survives the same kill.
#![cfg(any(target_os = "linux", windows))]

use super::{ProcessCustody, ProcessCustodyLease, ProcessTree};
use std::fs;
use std::path::{Path, PathBuf};
use std::process::{Command, Stdio};
use std::thread;

const MODE: &str = "ACYCLIC_CUSTODY_TEST_MODE";
const ROOT: &str = "ACYCLIC_CUSTODY_TEST_ROOT";

/// A shell program that publishes its descendant's PID at the path given as
/// its first argument, then waits; paths are arguments, never script text.
#[cfg(unix)]
const PUBLISHING_PROGRAM: &str = r#"sleep 3600 >/dev/null & child=$!; echo "$child" > "$1.tmp"; mv "$1.tmp" "$1"; echo ACYCLIC_CHILD_PID="$child"; wait"#;

// Exercise the same resolved shell as native host tools, with a real
// grandchild. A directly spawned ping cannot detect packaged-shell breakaway.
#[cfg(windows)]
const PUBLISHING_PROGRAM: &str = r#"
$child = Start-Process -FilePath $env:ACYCLIC_CUSTODY_TEST_EXECUTABLE -ArgumentList @('--exact', 'process_tree::custody_tests::custody_owner_helper', '--nocapture') -NoNewWindow -PassThru
$child.WaitForExit()
exit $child.ExitCode
"#;

/// A long-lived grandchild whose PID is published independently of the shell,
/// so only whole-tree custody reaches it on every platform.
fn program(root: &Path) -> Command {
    #[cfg(unix)]
    {
        let pid = root.join("pid");
        let mut command =
            crate::shell_command(PUBLISHING_PROGRAM, Some("sh")).expect("resolve fixture shell");
        command
            .arg("sh")
            .arg(pid)
            .stdin(Stdio::null())
            .stdout(Stdio::piped())
            .stderr(Stdio::null());
        command
    }
    #[cfg(windows)]
    {
        let _ = root;
        let mut command = crate::shell_command(PUBLISHING_PROGRAM, None)
            .expect("resolve the native host's fixture shell");
        command
            .env(MODE, "member")
            .env(
                "ACYCLIC_CUSTODY_TEST_EXECUTABLE",
                std::env::current_exe().expect("test executable"),
            )
            .stdin(Stdio::null())
            .stdout(Stdio::piped())
            .stderr(Stdio::null());
        command
    }
}

/// A live tree whose original stdin owner controls EOF; no timer or probe runs.
fn gated_command() -> Command {
    let mut command = Command::new(std::env::current_exe().expect("test executable"));
    command
        .args([
            "--exact",
            "process_tree::capture_events_tests::event_capture_child",
            "--nocapture",
        ])
        .env("ACYCLIC_TEST_EVENT_CAPTURE", "quiet")
        .stdin(Stdio::piped())
        .stdout(Stdio::null())
        .stderr(Stdio::null());
    command
}

/// The owner: spawns the program, publishes its PID, and lives until killed.
#[test]
fn custody_owner_helper() {
    let Ok(mode) = std::env::var(MODE) else {
        return;
    };
    let root = PathBuf::from(std::env::var_os(ROOT).expect("owner root"));
    if mode == "exit-before-publish" {
        return;
    }
    #[cfg(windows)]
    if mode == "member" {
        fs::write(root.join("pid.tmp"), std::process::id().to_string()).expect("pid");
        fs::rename(root.join("pid.tmp"), root.join("pid")).expect("publish grandchild pid");
        publish_pid(std::process::id());
        loop {
            thread::park();
        }
    }
    #[cfg(windows)]
    if mode == "churn" {
        return churn(&root);
    }
    let mut command = program(&root);
    let lease = (mode == "leased")
        .then(|| ProcessCustodyLease::acquire(&root.join("fleet")).expect("native lease"));
    let mut uncontained = None;
    let mut tree = if let Some(lease) = &lease {
        Some(ProcessTree::spawn_in_custody(&mut command, lease.custody()).expect("leased spawn"))
    } else if mode == "custodied" {
        let custody = ProcessCustody::open(&root.join("processes")).expect("custody");
        Some(ProcessTree::spawn_in_custody(&mut command, &custody).expect("custodied spawn"))
    } else {
        uncontained = Some(command.spawn().expect("uncontained spawn"));
        None
    };
    let output = tree
        .as_mut()
        .and_then(ProcessTree::take_stdout)
        .or_else(|| uncontained.as_mut()?.stdout.take())
        .expect("original program publication pipe");
    let pid = read_published_pid(output).expect("original program PID publication");
    publish_pid(pid);
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
        .stdout(Stdio::piped())
        .stderr(Stdio::null())
        .spawn()
        .expect("owner")
}

/// Publication follows both original spawn admission and descendant creation.
fn published(owner: &mut std::process::Child) -> u32 {
    read_published_pid(owner.stdout.take().expect("owner publication pipe"))
        .expect("original owner's descendant publication")
}

fn publish_pid(pid: u32) {
    use std::io::Write as _;
    println!("\nACYCLIC_CHILD_PID={pid}");
    std::io::stdout().flush().expect("publish original PID");
}

fn read_published_pid(output: impl std::io::Read) -> std::io::Result<u32> {
    use std::io::{BufRead as _, Error, ErrorKind};
    for line in std::io::BufReader::new(output).lines() {
        let line = line?;
        if let Some(pid) = line.strip_prefix("ACYCLIC_CHILD_PID=") {
            return pid
                .parse()
                .map_err(|error| Error::new(ErrorKind::InvalidData, error));
        }
    }
    Err(Error::new(
        ErrorKind::UnexpectedEof,
        "original child PID producer exited before publication",
    ))
}

/// An exit watch on one process, independent of its parentage.
pub(super) struct Watch {
    #[cfg(unix)]
    fd: std::os::fd::OwnedFd,
    #[cfg(windows)]
    handle: windows_sys::Win32::Foundation::HANDLE,
}

impl Watch {
    /// None when no such process exists any more.
    #[cfg(unix)]
    pub(super) fn new(pid: u32) -> Option<Self> {
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
        Some(Self { fd })
    }

    #[cfg(windows)]
    pub(super) fn new(pid: u32) -> Option<Self> {
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
    pub(super) fn exited(&self) -> bool {
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

    /// Blocks on the retained process handle's terminal event.
    #[cfg(windows)]
    fn wait(&self) {
        use windows_sys::Win32::Foundation::WAIT_OBJECT_0;
        use windows_sys::Win32::System::Threading::{INFINITE, WaitForSingleObject};
        // SAFETY: the original process handle remains owned by this watch.
        let status = unsafe { WaitForSingleObject(self.handle, INFINITE) };
        assert_eq!(
            status,
            WAIT_OBJECT_0,
            "process wait: {}",
            std::io::Error::last_os_error()
        );
    }

    fn kill(&self) {
        #[cfg(unix)]
        {
            use std::os::fd::AsRawFd as _;
            // SAFETY: the retained pidfd names the original process even after
            // exit/reaping; no numeric PID can be reused by an unrelated child.
            let killed = unsafe {
                libc::syscall(
                    libc::SYS_pidfd_send_signal,
                    self.fd.as_raw_fd(),
                    libc::SIGKILL,
                    std::ptr::null::<libc::siginfo_t>(),
                    0_u32,
                )
            };
            if killed != 0 {
                let error = std::io::Error::last_os_error();
                assert_eq!(
                    error.raw_os_error(),
                    Some(libc::ESRCH),
                    "pidfd kill: {error}"
                );
            }
        }
        #[cfg(windows)]
        {
            // SAFETY: the retained original handle has terminate access.
            let killed =
                unsafe { windows_sys::Win32::System::Threading::TerminateProcess(self.handle, 1) };
            if killed == 0 {
                let error = std::io::Error::last_os_error();
                assert!(self.exited(), "terminate watched process: {error}");
            }
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
    let pid = published(&mut owner);
    let watch = Watch::new(pid).expect("the published program is running");
    owner.kill().expect("kill owner");
    owner.wait().expect("reap owner");
    (root, watch)
}

#[test]
fn reopened_owner_fence_returns_only_after_the_killed_owners_tree_is_dead() {
    let root = tempfile::tempdir().expect("unpublished owner root");
    let mut unpublished = owner("exit-before-publish", root.path());
    let error = read_published_pid(unpublished.stdout.take().expect("publication pipe"))
        .expect_err("missing owner published a PID");
    assert_eq!(error.kind(), std::io::ErrorKind::UnexpectedEof);
    assert!(
        unpublished
            .wait()
            .expect("reap unpublished owner")
            .success()
    );
    let (root, watch) = killed_owner_leaves("custodied");
    let processes = root.path().join("processes");
    ProcessCustody::open(&processes)
        .expect("reopen custody")
        .fence_stale()
        .expect("fence");
    let exited = watch.exited();
    if !exited {
        watch.kill();
    }
    assert!(exited, "a killed owner's tree outlived the fence");
    assert_eq!(fs::read_dir(&processes).expect("records").count(), 0);
}

#[test]
fn leased_successor_fences_the_killed_native_owner_before_admission() {
    let root = tempfile::tempdir().expect("root");
    let mut owner = owner("leased", root.path());
    let watch = Watch::new(published(&mut owner)).expect("running program");
    #[cfg(windows)]
    let records = root.path().join("fleet/trees");
    #[cfg(windows)]
    let custodian = Watch::new(custodian_of(&records)).expect("running custodian");
    owner.kill().expect("kill native owner");
    owner.wait().expect("reap native owner");
    let successor = ProcessCustodyLease::acquire(&root.path().join("fleet"))
        .expect("serialized recovery admission");
    let exited = watch.exited();
    if !exited {
        watch.kill();
    }
    assert!(exited, "successor admitted before physical retirement");
    #[cfg(windows)]
    assert!(
        custodian.exited(),
        "successor admitted before pipe custody retired"
    );
    assert_eq!(successor.custody().fence_stale().expect("empty fence"), 0);
}

/// The PID of the custodian the only record in `processes` names.
#[cfg(windows)]
fn custodian_of(processes: &Path) -> u32 {
    let mut records = fs::read_dir(processes).expect("records");
    let record = records
        .next()
        .expect("a record")
        .expect("record")
        .file_name();
    assert!(records.next().is_none(), "one record");
    record
        .to_str()
        .and_then(|name| name.split_once("-custodian-"))
        .and_then(|(_, custodian)| custodian.split_once('-'))
        .and_then(|(pid, _)| pid.parse().ok())
        .expect("the record names its custodian")
}

/// How many handles process `pid` holds.
#[cfg(windows)]
fn handle_count(pid: u32) -> u32 {
    use windows_sys::Win32::Foundation::CloseHandle;
    use windows_sys::Win32::System::Threading::{
        GetProcessHandleCount, OpenProcess, PROCESS_QUERY_LIMITED_INFORMATION,
    };
    // SAFETY: opening a query handle to a running PID; closed below.
    let process = unsafe { OpenProcess(PROCESS_QUERY_LIMITED_INFORMATION, 0, pid) };
    assert!(!process.is_null(), "{}", std::io::Error::last_os_error());
    let mut count = 0;
    // SAFETY: the handle opened above and a writable count.
    let counted = unsafe { GetProcessHandleCount(process, &raw mut count) };
    // SAFETY: the handle opened above.
    unsafe { CloseHandle(process) };
    assert_ne!(counted, 0, "{}", std::io::Error::last_os_error());
    count
}

/// How many members a churning tree starts and ends.
#[cfg(windows)]
const CHURNED: u32 = 100;

/// A tree member that starts and ends [`CHURNED`] members one at a time,
/// marks that it has, and then lives until killed. Each is this executable
/// started as a custodian with no owner to read, which exits before `main`.
#[cfg(windows)]
fn churn(_root: &Path) {
    use std::io::{Read as _, Write as _};
    println!("\nACYCLIC_CHURN_READY");
    std::io::stdout().flush().expect("publish churn admission");
    let mut start = [0];
    std::io::stdin()
        .read_exact(&mut start)
        .expect("original churn caller");
    for _ in 0..CHURNED {
        let status = Command::new(std::env::current_exe().expect("test executable"))
            .env("ACYCLIC_PROCESS_CUSTODIAN", "1")
            .stdin(Stdio::null())
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .status()
            .expect("member");
        assert!(!status.success(), "an ownerless custodian ran");
    }
    println!("ACYCLIC_CHURN_DONE");
    std::io::stdout()
        .flush()
        .expect("publish completed members");
    loop {
        thread::park();
    }
}

#[cfg(windows)]
fn churn_event(output: &mut impl std::io::BufRead, expected: &str) -> std::io::Result<()> {
    for line in output.lines() {
        if line? == expected {
            return Ok(());
        }
    }
    Err(std::io::Error::new(
        std::io::ErrorKind::UnexpectedEof,
        "original member producer exited before publication",
    ))
}

/// A custodian holds state for live members only: a tree that starts and
/// ends many short members leaves its custodian's handles bounded.
#[cfg(windows)]
#[test]
fn a_custodians_handles_track_live_members_not_every_member() {
    let root = tempfile::tempdir().expect("root");
    let processes = root.path().join("processes");
    let custody = ProcessCustody::acquire(&processes).expect("custody");
    let mut command = Command::new(std::env::current_exe().expect("test executable"));
    command
        .args([
            "--exact",
            "process_tree::custody_tests::custody_owner_helper",
            "--nocapture",
        ])
        .env(MODE, "churn")
        .env(ROOT, root.path())
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::null());
    let mut tree = ProcessTree::spawn_in_custody(&mut command, &custody).expect("spawn");
    let custodian = custodian_of(&processes);
    let mut output = std::io::BufReader::new(tree.take_stdout().expect("churn publication pipe"));
    churn_event(&mut output, "ACYCLIC_CHURN_READY").expect("original member admission");
    let before = handle_count(custodian);
    {
        use std::io::Write as _;
        tree.take_stdin()
            .expect("churn start pipe")
            .write_all(&[1])
            .expect("start original members");
    }
    churn_event(&mut output, "ACYCLIC_CHURN_DONE").expect("original member completion");
    let after = handle_count(custodian);
    eprintln!("custodian handles: {before} before, {after} after {CHURNED} members");
    tree.terminate(None).expect("terminate");
    assert!(
        after < before + CHURNED / 10,
        "{CHURNED} exited members left the custodian holding {after} handles, from {before}"
    );
}

/// A killed custodian leaves its tree running under its live owner, and the
/// owner's handle kills the tree when the owner dies; the reopened fence
/// then clears the custodian's record.
#[cfg(windows)]
#[test]
fn a_killed_custodians_tree_dies_with_its_owner() {
    let root = tempfile::tempdir().expect("root");
    let processes = root.path().join("processes");
    let mut owner = owner("custodied", root.path());
    let watch = Watch::new(published(&mut owner)).expect("running program");
    let custodian = Watch::new(custodian_of(&processes)).expect("the custodian is running");
    custodian.kill();
    custodian.wait();
    let survived = !watch.exited();
    owner.kill().expect("kill owner");
    owner.wait().expect("reap owner");
    watch.wait();
    assert!(survived, "the custodian's death killed a live owner's tree");
    ProcessCustody::open(&processes)
        .expect("reopen custody")
        .fence_stale()
        .expect("fence");
    assert_eq!(fs::read_dir(&processes).expect("records").count(), 0);
}

/// Owners of two directories in one process name distinct Jobs, so ending
/// one tree never ends the other.
#[cfg(windows)]
#[test]
fn trees_of_two_custody_directories_in_one_process_never_share_a_job() {
    let root = tempfile::tempdir().expect("root");
    let spawn = |directory: &str| {
        let custody = ProcessCustody::acquire(&root.path().join(directory)).expect("custody");
        let mut command = gated_command();
        let tree = ProcessTree::spawn_in_custody(&mut command, &custody).expect("spawn");
        (custody, tree)
    };
    let (_first_custody, mut first) = spawn("first");
    let (_second_custody, mut second) = spawn("second");
    let survivor = Watch::new(tree_pid(&second)).expect("running second tree");
    first.terminate(None).expect("terminate first");
    let survived = !survivor.exited();
    second.terminate(None).expect("terminate second");
    assert!(survived, "ending one directory's tree ended another's");
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
            &format!(r#"sleep 3600 & echo $! > "$1"; {script}"#),
            Some("sh"),
        )
        .expect("resolve fixture shell");
        command
            .arg("sh")
            .arg(&pid)
            .stdin(Stdio::null())
            .stdout(Stdio::null())
            .stderr(Stdio::null());
        let mut tree = ProcessTree::spawn_in_custody(&mut command, &custody).expect("spawn");
        let status = tree.wait_exit(None).expect("wait");
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

/// An idle custody holds no descriptor, so an idle owner costs none; a
/// running tree's record holds the directory only while the tree runs.
#[cfg(all(target_os = "linux", feature = "test-support"))]
#[test]
fn an_idle_custody_holds_no_descriptor() -> std::io::Result<()> {
    use crate::testing::descriptors_naming;
    let root = tempfile::tempdir()?;
    let processes = root.path().join("processes");
    let custody = ProcessCustody::acquire(&processes)?;
    assert_eq!(descriptors_naming(&processes)?, 0);
    let mut command = gated_command();
    let mut tree = ProcessTree::spawn_in_custody(&mut command, &custody)?;
    // Negative control: the count sees the descriptor a running tree holds.
    assert_eq!(descriptors_naming(&processes)?, 1);
    tree.terminate(None)?;
    drop(tree);
    assert_eq!(descriptors_naming(&processes)?, 0);
    Ok(())
}

/// A custody never records a tree in a directory put in place of the one it
/// fenced: that directory's records were never fenced.
#[cfg(unix)]
#[test]
fn a_replaced_custody_directory_refuses_to_record() -> std::io::Result<()> {
    let root = tempfile::tempdir()?;
    let processes = root.path().join("processes");
    let custody = ProcessCustody::open(&processes)?;
    let mut command = crate::shell_command("exit 0", Some("sh"))?;
    command
        .stdin(Stdio::null())
        .stdout(Stdio::null())
        .stderr(Stdio::null());
    // Control: the fenced directory records the tree.
    let mut tree = ProcessTree::spawn_in_custody(&mut command, &custody)?;
    tree.wait_exit(None)?;
    tree.terminate(None)?;
    fs::rename(&processes, root.path().join("fenced"))?;
    fs::create_dir(&processes)?;
    let mut command = crate::shell_command("exit 0", Some("sh"))?;
    let refused = ProcessTree::spawn_in_custody(&mut command, &custody)
        .err()
        .ok_or_else(|| std::io::Error::other("a replaced directory recorded a tree"))?;
    assert!(refused.to_string().contains("replaced"), "{refused}");
    assert_eq!(fs::read_dir(&processes)?.count(), 0);
    Ok(())
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
    let mut command = gated_command();
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
    let mut command =
        crate::shell_command(PUBLISHING_PROGRAM, Some("sh")).expect("resolve fixture shell");
    command.arg("sh").arg(&pid);
    command
        .stdin(Stdio::null())
        .stdout(Stdio::piped())
        .stderr(Stdio::null());
    let mut tree = ProcessTree::spawn_in_custody(&mut command, &custody).expect("spawn");
    let descendant = Watch::new(
        read_published_pid(tree.take_stdout().expect("descendant publication pipe"))
            .expect("original descendant publication"),
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
        r#"setsid sh -c 'echo $$ > "$1.tmp"; mv "$1.tmp" "$1"; echo ACYCLIC_CHILD_PID=$$; exec sleep 3600 >/dev/null' sh "$1" & wait"#,
        Some("sh"),
    )
    .expect("resolve fixture shell");
    command.arg("sh").arg(pid);
    command
        .stdin(Stdio::null())
        .stdout(Stdio::piped())
        .stderr(Stdio::null());
    command
}

/// Starts `command`, waits for its escaped descendant, and cancels the tree.
/// Returns the descendant's watch once cancellation has returned.
#[cfg(target_os = "linux")]
fn cancel_after_escape(mut tree: ProcessTree) -> Watch {
    let escaped = Watch::new(
        read_published_pid(tree.take_stdout().expect("escape publication pipe"))
            .expect("original escaped descendant publication"),
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
    let escaped = cancel_after_escape(tree);
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
    let escaped = cancel_after_escape(tree);
    let survived = !escaped.exited();
    escaped.kill();
    assert!(survived, "the oracle cannot observe an escaped descendant");
}

#[cfg(target_os = "linux")]
const ANCHOR_MODE: &str = "ACYCLIC_CUSTODY_ANCHOR_MODE";

/// A stand-in warden: holds a record lock until exit, forks a child, and
/// publishes the child's PID. "anchor" forks the warden's group anchor;
/// "paused" forks a child that retains the record descriptor and waits for
/// a signal.
#[cfg(target_os = "linux")]
#[test]
fn anchor_parent_helper() {
    let Ok(mode) = std::env::var(ANCHOR_MODE) else {
        return;
    };
    if mode == "exit-before-publish" {
        return;
    }
    let root = PathBuf::from(std::env::var_os(ROOT).expect("helper root"));
    let record = fs::File::create(root.join("record")).expect("record");
    crate::FileLock::try_hold_until_exit(record, crate::FileLockMode::Exclusive)
        .expect("record lock");
    let child = if mode == "anchor" {
        super::warden::spawn_anchor().expect("anchor")
    } else {
        // SAFETY: the child only closes stdout and waits for signals;
        // both operations are async-signal-safe.
        match unsafe { libc::fork() } {
            0 => {
                // This negative control retains the record, but only its
                // original parent may hold the PID-publication pipe open.
                // SAFETY: close and _exit are async-signal-safe after fork.
                if unsafe { libc::close(libc::STDOUT_FILENO) } != 0 {
                    unsafe { libc::_exit(125) };
                }
                loop {
                    // SAFETY: waiting until a signal is async-signal-safe.
                    unsafe { libc::pause() };
                }
            }
            pid => pid,
        }
    };
    assert!(child > 0, "fork: {}", std::io::Error::last_os_error());
    println!("\nACYCLIC_CHILD_PID={child}");
    loop {
        thread::park();
    }
}

#[cfg(target_os = "linux")]
fn anchor_parent(mode: &str, root: &Path) -> std::process::Child {
    Command::new(std::env::current_exe().expect("test executable"))
        .args([
            "--exact",
            "process_tree::custody_tests::anchor_parent_helper",
            "--nocapture",
        ])
        .env(ANCHOR_MODE, mode)
        .env(ROOT, root)
        .stdin(Stdio::null())
        .stdout(Stdio::piped())
        .stderr(Stdio::null())
        .spawn()
        .expect("helper")
}

/// Kills a stand-in warden after its original PID publication and checks
/// the actual record lock once physical exit has settled.
#[cfg(target_os = "linux")]
fn record_free_after_killed_parent(mode: &str) -> bool {
    let root = tempfile::tempdir().expect("root");
    let mut parent = anchor_parent(mode, root.path());
    let child = published_child(&mut parent).expect("original child PID publication");
    let watch = Watch::new(child).expect("running child");
    parent.kill().expect("kill helper");
    parent.wait().expect("reap helper");
    // Reaping the original parent is the settle cut. Its anchor must end;
    // the paused negative control still owns its inherited record lock.
    let exited = if mode == "anchor" {
        watch.wait();
        true
    } else {
        watch.exited()
    };
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
fn published_child(parent: &mut std::process::Child) -> std::io::Result<u32> {
    let output = parent.stdout.take().ok_or_else(|| {
        std::io::Error::new(
            std::io::ErrorKind::BrokenPipe,
            "child PID publication pipe is missing",
        )
    })?;
    read_published_pid(output)
}

/// A warden killed while its group anchor lives leaves no holder of its
/// record's lock: the anchor holds none of its descriptors and exits with it.
#[cfg(target_os = "linux")]
#[test]
fn an_anchor_never_outlives_or_holds_for_its_killed_warden() {
    let root = tempfile::tempdir().expect("root");
    let mut parent = anchor_parent("exit-before-publish", root.path());
    let error = published_child(&mut parent).expect_err("missing producer published a PID");
    assert_eq!(error.kind(), std::io::ErrorKind::UnexpectedEof);
    assert!(parent.wait().expect("reap unpublished parent").success());
    assert!(
        record_free_after_killed_parent("anchor"),
        "a killed warden's anchor kept its record locked"
    );
}

/// Negative control: a child that retains the record descriptor and waits
/// for a signal holds the record locked after its parent is killed.
#[cfg(target_os = "linux")]
#[test]
fn a_paused_child_keeps_its_killed_parents_record_locked() {
    assert!(
        !record_free_after_killed_parent("paused"),
        "the oracle cannot observe a lock kept by a surviving child"
    );
}
