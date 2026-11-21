//! A tree never outlives the process that started it, however that owner
//! exits, and a service started inside a tree stays in the tree. The same
//! program started without a tree demonstrably survives the same kill.
#![cfg(unix)]

use super::{ProcessCommand, ProcessStdio, ProcessTree};
use std::io::{BufRead as _, BufReader};
use std::process::{Command, Stdio};
use std::thread;

const MODE: &str = "ACYCLIC_OWNER_EXIT_TEST_MODE";
const HELPER: &str = "process_tree::owner_exit_tests::owner_exit_helper";

#[test]
fn owner_exit_helper() {
    let Ok(mode) = std::env::var(MODE) else {
        return;
    };
    if mode == "member" {
        println!(
            "\nACYCLIC_MEMBER={} {} {}",
            std::process::id(),
            super::within_warded_tree(),
            start_service()
        );
        loop {
            thread::park();
        }
    }
    let exe = std::env::current_exe().expect("test executable");
    let args = ["--exact", HELPER, "--nocapture"];
    let mut tree = None;
    let mut plain = None;
    let output: Box<dyn std::io::Read> = if mode == "tree-owner" {
        let mut command = ProcessCommand::new(&exe);
        command
            .args(args)
            .env(MODE, "member")
            .stdin(ProcessStdio::null())
            .stdout(ProcessStdio::piped())
            .stderr(ProcessStdio::null());
        let spawned = tree.insert(ProcessTree::spawn(&mut command).expect("spawn tree"));
        Box::new(spawned.take_stdout().expect("member output"))
    } else {
        use std::os::unix::process::CommandExt as _;
        // Its own group, as a terminal job's leader, whatever runs this test.
        let spawned = plain.insert(
            Command::new(&exe)
                .process_group(0)
                .args(args)
                .env(MODE, "member")
                .stdin(Stdio::null())
                .stdout(Stdio::piped())
                .stderr(Stdio::null())
                .spawn()
                .expect("spawn plain child"),
        );
        Box::new(spawned.stdout.take().expect("member output"))
    };
    println!("\n{}", member_line(output));
    loop {
        thread::park();
    }
}

/// Starts a real service the way a member's tools do and returns its PID.
/// The service records its PID, then signals readiness and stays running.
fn start_service() -> libc::pid_t {
    let record =
        std::env::temp_dir().join(format!("acyclic-owner-exit-service-{}", std::process::id()));
    let readiness = crate::service::spawn_service_process(
        std::path::Path::new("/bin/sh"),
        &[
            "-c".into(),
            // $0 is the record path; the ready argument follows as $1.
            r#"echo $$ > "$0"; echo; exec sleep 1000000"#.into(),
            record.clone().into(),
        ],
    )
    .expect("spawn service");
    assert_eq!(
        readiness.wait().expect("service readiness"),
        crate::service::ServiceStartOutcome::Ready
    );
    let pid = std::fs::read_to_string(&record).expect("service PID record");
    std::fs::remove_file(&record).expect("remove service PID record");
    pid.trim().parse().expect("numeric service PID")
}

/// The member's publication line, forwarded unchanged by its owner.
fn member_line(output: impl std::io::Read) -> String {
    BufReader::new(output)
        .lines()
        .map(|line| line.expect("publication"))
        .find(|line| line.starts_with("ACYCLIC_MEMBER="))
        .expect("member published before exit")
}

/// Starts an owner, kills it with SIGKILL once its member and the member's
/// service run, and returns watches on both and whether the member saw
/// itself in a tree.
fn kill_owner(mode: &str) -> (Watch, Watch, bool) {
    let mut owner = Command::new(std::env::current_exe().expect("test executable"))
        .args(["--exact", HELPER, "--nocapture"])
        .env(MODE, mode)
        .stdin(Stdio::null())
        .stdout(Stdio::piped())
        .stderr(Stdio::null())
        .spawn()
        .expect("owner");
    let line = member_line(owner.stdout.take().expect("owner output"));
    let [member, warded, service] = line
        .trim_start_matches("ACYCLIC_MEMBER=")
        .split(' ')
        .collect::<Vec<_>>()[..]
    else {
        panic!("malformed publication {line}");
    };
    let watch = |pid: &str| Watch::new(pid.parse().expect("numeric PID")).expect("running process");
    let (member, warded, service) = (watch(member), warded == "true", watch(service));
    owner.kill().expect("kill owner");
    owner.wait().expect("reap owner");
    (member, service, warded)
}

#[test]
fn a_tree_and_its_services_die_with_a_killed_owner() {
    let (member, service, warded) = kill_owner("tree-owner");
    assert!(warded, "a tree member did not see its tree");
    // Each returns only once that process is gone: its warden saw the owner
    // exit and drained the whole tree, the member's service included.
    member.wait();
    service.wait();
}

/// Negative control: the same member leading its own group outside a tree
/// survives the same kill, and so does the service it started detached.
#[test]
fn a_plain_child_outlives_its_killed_owner() {
    let (member, service, warded) = kill_owner("plain-owner");
    let survived = (!member.exited(), !service.exited());
    member.kill();
    service.kill();
    assert_eq!(
        survived,
        (true, true),
        "the oracle cannot observe a surviving member and service"
    );
    assert!(!warded, "a plain child claimed tree membership");
}

const ACTION_MARKER: &str = "ACYCLIC_EXIT_ACTION_MARKER";
const ACTION_HELPER: &str = "process_tree::owner_exit_tests::exit_action_owner";

fn marker_action(path: &std::path::Path) -> super::ProcessExitAction {
    let mut command = ProcessCommand::new("/usr/bin/touch");
    command
        .arg(path)
        .stdin(ProcessStdio::null())
        .stdout(ProcessStdio::null())
        .stderr(ProcessStdio::null());
    let meter: std::sync::Arc<dyn crate::NativeReadMeter> = std::sync::Arc::new(
        crate::test_fixtures::ReadBudgetMeter::new(u64::MAX, u64::MAX),
    );
    super::ProcessExitAction::spawn(&command, &meter).expect("admit marker action")
}

#[test]
fn exit_action_owner() {
    let Some(marker) = std::env::var_os(ACTION_MARKER) else {
        return;
    };
    let action = marker_action(std::path::Path::new(&marker));
    println!(
        "\nACYCLIC_EXIT_ACTION={} {}",
        std::process::id(),
        action.id()
    );
    loop {
        thread::park();
    }
}

#[test]
fn exit_action_completes_after_owner_sigkill() {
    let temp = tempfile::tempdir().expect("marker directory");
    let marker = temp.path().join("completed");
    let mut command = ProcessCommand::new(std::env::current_exe().expect("test executable"));
    command
        .args(["--exact", ACTION_HELPER, "--nocapture"])
        .env(ACTION_MARKER, &marker)
        .stdin(ProcessStdio::null())
        .stdout(ProcessStdio::piped())
        .stderr(ProcessStdio::null());
    let mut tree = ProcessTree::spawn(&mut command).expect("start action owner in custody");
    let stdout = tree.take_stdout().expect("owner output");
    let publication = BufReader::new(stdout)
        .lines()
        .map(|line| line.expect("read owner output"))
        .find_map(|line| line.strip_prefix("ACYCLIC_EXIT_ACTION=").map(str::to_owned))
        .expect("owner published admitted action");
    let (owner, action) = publication.split_once(' ').expect("owner and action PIDs");
    let owner = Watch::new(owner.parse().expect("owner PID")).expect("owner exit watch");
    let action = Watch::new(action.parse().expect("action PID")).expect("action exit watch");
    owner.kill();
    action.wait();
    assert!(marker.is_file());
}

#[test]
fn exit_action_cancel_waits_without_running_command() {
    let temp = tempfile::tempdir().expect("marker directory");
    let marker = temp.path().join("cancelled");
    let mut action = marker_action(&marker);
    action.cancel().expect("cancel and await action");
    assert!(!marker.exists());
}

/// An exit watch on one process, independent of its parentage.
struct Watch {
    fd: std::os::fd::OwnedFd,
    pid: libc::pid_t,
}

impl Watch {
    fn new(pid: libc::pid_t) -> Option<Self> {
        use std::os::fd::FromRawFd as _;
        #[cfg(target_os = "linux")]
        // SAFETY: pidfd_open takes a PID and zero flags.
        let fd = unsafe { libc::syscall(libc::SYS_pidfd_open, pid, 0) };
        #[cfg(not(target_os = "linux"))]
        // SAFETY: kqueue has no preconditions.
        let fd = libc::c_long::from(unsafe { libc::kqueue() });
        let fd = libc::c_int::try_from(fd).expect("descriptor");
        assert!(fd >= 0, "watch: {}", std::io::Error::last_os_error());
        let watch = Self {
            // SAFETY: the descriptor the call above just returned.
            fd: unsafe { std::os::fd::OwnedFd::from_raw_fd(fd) },
            pid,
        };
        #[cfg(not(target_os = "linux"))]
        {
            use std::os::fd::AsRawFd as _;
            let change = libc::kevent {
                ident: usize::try_from(pid).expect("pid"),
                filter: libc::EVFILT_PROC,
                flags: libc::EV_ADD,
                fflags: libc::NOTE_EXIT,
                data: 0,
                udata: std::ptr::null_mut(),
            };
            // SAFETY: one initialized change and no event output.
            let added = unsafe {
                libc::kevent(
                    watch.fd.as_raw_fd(),
                    &raw const change,
                    1,
                    std::ptr::null_mut(),
                    0,
                    std::ptr::null(),
                )
            };
            if added != 0 {
                let error = std::io::Error::last_os_error();
                assert_eq!(error.raw_os_error(), Some(libc::ESRCH), "kevent: {error}");
                return None;
            }
        }
        Some(watch)
    }

    /// Whether the process has exited, waiting at most `timeout`.
    fn ready(&self, timeout: Option<&libc::timespec>) -> bool {
        use std::os::fd::AsRawFd as _;
        #[cfg(target_os = "linux")]
        {
            let mut ready = libc::pollfd {
                fd: self.fd.as_raw_fd(),
                events: libc::POLLIN,
                revents: 0,
            };
            let timeout = timeout.map_or(-1, |_| 0);
            // SAFETY: one initialized pollfd.
            let polled = unsafe { libc::poll(&raw mut ready, 1, timeout) };
            assert!(polled >= 0, "poll: {}", std::io::Error::last_os_error());
            polled == 1
        }
        #[cfg(not(target_os = "linux"))]
        {
            // SAFETY: plain fields; all-zero is a valid output value.
            let mut event: libc::kevent = unsafe { std::mem::zeroed() };
            // SAFETY: no changes and one writable event.
            let ready = unsafe {
                libc::kevent(
                    self.fd.as_raw_fd(),
                    std::ptr::null(),
                    0,
                    &raw mut event,
                    1,
                    timeout.map_or(std::ptr::null(), std::ptr::from_ref),
                )
            };
            assert!(ready >= 0, "kevent: {}", std::io::Error::last_os_error());
            ready == 1
        }
    }

    /// Whether the process has exited, without waiting.
    fn exited(&self) -> bool {
        self.ready(Some(&libc::timespec {
            tv_sec: 0,
            tv_nsec: 0,
        }))
    }

    /// Blocks until the process has exited.
    fn wait(&self) {
        assert!(self.ready(None), "the watch ended before the exit");
    }

    fn kill(&self) {
        // SAFETY: the watched process has not been seen to exit, so its
        // reparented PID still names it until its new parent reaps it.
        unsafe { libc::kill(self.pid, libc::SIGKILL) };
        self.wait();
    }
}
