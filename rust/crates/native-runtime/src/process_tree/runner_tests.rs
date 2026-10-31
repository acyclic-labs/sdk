//! The runner reproduces each custodied command exactly and holds no tree:
//! its wardens are the owner's own children, and killing it ends no tree.

use super::super::{ProcessCommand, ProcessCustody, ProcessStdio, ProcessTree};
use super::{make_image, retire_runner, runner_pid};
use std::fs;
use std::io::Read as _;
use std::process::{Command, Stdio};

const MODE: &str = "ACYCLIC_RUNNER_TEST_MODE";

fn custody() -> (tempfile::TempDir, ProcessCustody) {
    let root = tempfile::tempdir().expect("root");
    let custody = ProcessCustody::open(root.path()).expect("custody");
    (root, custody)
}

fn records(root: &tempfile::TempDir) -> usize {
    fs::read_dir(root.path()).expect("records").count()
}

/// Runs `command` in custody and returns its standard output.
fn output(command: &mut ProcessCommand, custody: &ProcessCustody) -> String {
    command
        .stdin(ProcessStdio::null())
        .stdout(ProcessStdio::piped())
        .stderr(ProcessStdio::null());
    let mut tree = ProcessTree::spawn_in_custody(command, custody).expect("spawn");
    // Control: the runner, not an owner fork, started this tree.
    assert!(runner_pid().is_some(), "no runner started the tree");
    let mut output = String::new();
    tree.take_stdout()
        .expect("stdout")
        .read_to_string(&mut output)
        .expect("read");
    tree.terminate(None).expect("terminate");
    output
}

#[test]
fn a_custodied_program_receives_exactly_its_environment_and_directory() {
    let (root, custody) = custody();
    let mut cleared = ProcessCommand::new("/usr/bin/env");
    cleared.env_clear().env("ACYCLIC_ONLY", "1");
    assert_eq!(output(&mut cleared, &custody), "ACYCLIC_ONLY=1\n");
    // Control: without clearing, the owner's environment reaches it too.
    let mut inherited = ProcessCommand::new("/usr/bin/env");
    inherited.env("ACYCLIC_ONLY", "1");
    let inherited = output(&mut inherited, &custody);
    assert!(inherited.lines().any(|line| line == "ACYCLIC_ONLY=1"));
    assert!(
        inherited.lines().count() > 1,
        "the owner's environment is missing"
    );
    let mut directory = ProcessCommand::new("/bin/pwd");
    directory.arg("-P").current_dir(root.path());
    let canonical = fs::canonicalize(root.path()).expect("canonical root");
    assert_eq!(
        output(&mut directory, &custody).trim_end(),
        canonical.to_str().expect("utf-8 root")
    );
    assert_eq!(records(&root), 0);
}

#[test]
fn a_runner_clones_each_warden_as_the_owners_child() {
    let (root, custody) = custody();
    let mut command = ProcessCommand::new("/bin/sleep");
    command
        .arg("30")
        .stdin(ProcessStdio::null())
        .stdout(ProcessStdio::null())
        .stderr(ProcessStdio::null());
    let mut tree = ProcessTree::spawn_in_custody(&mut command, &custody).expect("spawn");
    let warden = tree.owned_child_id().expect("warden");
    let stat = fs::read_to_string(format!("/proc/{warden}/stat")).expect("warden stat");
    let (_, fields) = stat.rsplit_once(')').expect("stat fields");
    let field = |index: usize| -> u32 {
        fields
            .split_whitespace()
            .nth(index)
            .and_then(|value| value.parse().ok())
            .expect("stat field")
    };
    let (parent, group) = (field(1), field(2));
    let runner = runner_pid().expect("runner");
    tree.terminate(None).expect("terminate");
    assert_eq!(
        parent,
        std::process::id(),
        "the warden is not the owner's child"
    );
    assert_ne!(parent, runner);
    // The warden left its tree's group, which its PID names, for the runner's.
    assert_eq!(group, runner, "the warden is not in the runner's group");
    assert_eq!(records(&root), 0);
}

#[test]
fn a_missing_program_fails_its_spawn_and_leaves_no_record() {
    let (root, custody) = custody();
    let mut command = ProcessCommand::new("/nonexistent/acyclic-program");
    command
        .stdin(ProcessStdio::null())
        .stdout(ProcessStdio::null())
        .stderr(ProcessStdio::null());
    let error = ProcessTree::spawn_in_custody(&mut command, &custody)
        .err()
        .expect("a missing program fails the spawn");
    assert_eq!(error.kind(), std::io::ErrorKind::NotFound);
    assert_eq!(records(&root), 0);
}

#[test]
fn a_custodied_program_receives_its_arguments_and_arg0_exactly() {
    let (root, custody) = custody();
    let mut command = ProcessCommand::new("/bin/sh");
    command.args(["-c", "printf '%s|' \"$0\" \"$@\"", "zero", "a b", ""]);
    assert_eq!(output(&mut command, &custody), "zero|a b||");
    // Control: a set arg0 replaces the program name the shell reports.
    let mut renamed = ProcessCommand::new("/bin/sh");
    renamed
        .arg0("acyclic-shell")
        .args(["-c", "printf '%s' \"$0\""]);
    assert_eq!(output(&mut renamed, &custody), "acyclic-shell");
    assert_eq!(records(&root), 0);
}

/// Starts a runner for each of many spawns while other threads allocate
/// without pause: a runner forked from the owner without exec could copy
/// the allocator mid-update and hang or fault.
fn hammered_owner() {
    use std::sync::Arc;
    use std::sync::atomic::{AtomicBool, Ordering};
    let stop = Arc::new(AtomicBool::new(false));
    let hammers: Vec<_> = (0..4)
        .map(|_| {
            let stop = Arc::clone(&stop);
            std::thread::spawn(move || {
                let mut held = Vec::new();
                while !stop.load(Ordering::Relaxed) {
                    held.push(vec![0_u8; 4096]);
                    if held.len() > 64 {
                        held.clear();
                    }
                }
            })
        })
        .collect();
    let (root, custody) = custody();
    for _ in 0..200 {
        let mut command = ProcessCommand::new("/bin/true");
        command
            .stdin(ProcessStdio::null())
            .stdout(ProcessStdio::null())
            .stderr(ProcessStdio::null());
        let mut tree = ProcessTree::spawn_in_custody(&mut command, &custody).expect("spawn");
        let status = tree.wait(std::time::Duration::from_secs(30)).expect("wait");
        tree.terminate(None).expect("terminate");
        assert!(status.success());
        // The next spawn starts a new runner mid-allocation.
        retire_runner();
    }
    stop.store(true, Ordering::Relaxed);
    for hammer in hammers {
        hammer.join().expect("hammer");
    }
    assert_eq!(records(&root), 0);
}

/// Execs the runner image at `image`, with no control socket: it exits at
/// once. Returns the spawn's error.
fn exec_image(image: &std::os::fd::OwnedFd) -> Option<std::io::Error> {
    use std::os::fd::AsRawFd as _;
    match Command::new(format!("/proc/self/fd/{}", image.as_raw_fd()))
        .stdin(Stdio::null())
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .spawn()
    {
        Ok(mut runner) => {
            runner.wait().expect("runner exit");
            None
        }
        Err(error) => Some(error),
    }
}

/// Makes and execs many runner images while another thread forks and execs
/// without pause: a fork that copied an image's writable descriptor would
/// make its exec fail with ETXTBSY until the fork's own exec.
#[test]
fn a_runner_image_made_while_another_thread_forks_always_execs() {
    use std::sync::Arc;
    use std::sync::atomic::{AtomicBool, Ordering};
    // Control: on this kernel, does a writable description fail the exec?
    let image = make_image().expect("image");
    let writable = std::fs::OpenOptions::new().write(true).open(format!(
        "/proc/self/fd/{}",
        std::os::fd::AsRawFd::as_raw_fd(&image)
    ));
    let busy = writable
        .as_ref()
        .ok()
        .and_then(|_| exec_image(&image))
        .and_then(|error| error.raw_os_error());
    eprintln!(
        "writable-image exec: {:?}; sealed reopen for writing: {:?}",
        busy,
        writable.err()
    );
    let stop = Arc::new(AtomicBool::new(false));
    let forker = {
        let stop = Arc::clone(&stop);
        std::thread::spawn(move || {
            let program = c"/bin/true";
            let argv = [program.as_ptr(), std::ptr::null()];
            let mut forks = 0_u64;
            while !stop.load(Ordering::Relaxed) {
                // SAFETY: the child makes only async-signal-safe calls.
                let pid = unsafe { libc::fork() };
                if pid == 0 {
                    // SAFETY: NUL-terminated program and argument vector.
                    unsafe {
                        libc::execv(program.as_ptr(), argv.as_ptr());
                        libc::_exit(127);
                    }
                }
                assert!(pid > 0, "fork");
                // SAFETY: this thread's own unreaped child.
                unsafe { libc::waitpid(pid, std::ptr::null_mut(), 0) };
                forks += 1;
            }
            forks
        })
    };
    let failures: Vec<_> = (0..500)
        .filter_map(|_| exec_image(&make_image().expect("image")))
        .collect();
    stop.store(true, Ordering::Relaxed);
    let forks = forker.join().expect("forker");
    eprintln!("forks during the images: {forks}");
    assert!(failures.is_empty(), "image execs failed: {failures:?}");
}

#[test]
fn a_runner_started_while_threads_allocate_serves_every_spawn() {
    owner("hammer");
}

/// A fresh owner for one mode. "kill" kills this owner's runner while a tree
/// runs: the tree keeps running and stays terminable, and the next spawn
/// starts another runner. "hammer" runs `hammered_owner`.
#[test]
fn runner_owner_helper() {
    match std::env::var(MODE).as_deref() {
        Ok("hammer") => hammered_owner(),
        Ok("security") => security_owner(),
        Ok(_) => killed_runner_owner(),
        Err(_) => {}
    }
}

fn killed_runner_owner() {
    let (root, custody) = custody();
    let sleeper = || {
        let mut command = ProcessCommand::new("/bin/sleep");
        command
            .arg("30")
            .stdin(ProcessStdio::null())
            .stdout(ProcessStdio::null())
            .stderr(ProcessStdio::null());
        command
    };
    let mut running = ProcessTree::spawn_in_custody(&mut sleeper(), &custody).expect("spawn");
    let first = runner_pid().expect("runner");
    let pid = libc::pid_t::try_from(first).expect("runner pid");
    // SAFETY: this owner's own unreaped child.
    assert_eq!(unsafe { libc::kill(pid, libc::SIGKILL) }, 0);
    // SAFETY: zeroed output storage; waits for the runner's exit, unreaped.
    let mut info: libc::siginfo_t = unsafe { std::mem::zeroed() };
    // SAFETY: this owner's own child and writable output storage.
    let waited = unsafe {
        libc::waitid(
            libc::P_PID,
            first,
            &raw mut info,
            libc::WEXITED | libc::WNOWAIT,
        )
    };
    assert_eq!(waited, 0);
    assert!(
        running.try_wait(None).expect("observe").is_none(),
        "killing the runner ended a tree"
    );
    assert_eq!(records(&root), 1);
    let mut next = ProcessTree::spawn_in_custody(&mut sleeper(), &custody).expect("next spawn");
    assert_ne!(runner_pid(), Some(first), "the dead runner served a spawn");
    running.terminate(None).expect("terminate");
    next.terminate(None).expect("terminate next");
    assert_eq!(records(&root), 0);
}

#[test]
fn a_killed_runner_ends_no_tree_and_the_next_spawn_starts_another() {
    owner("kill");
}

/// Runs `runner_owner_helper` in `mode` as a fresh owner, with no runner yet.
fn owner(mode: &str) {
    let status = Command::new(std::env::current_exe().expect("test executable"))
        .args([
            "--exact",
            "process_tree::runner::tests::runner_owner_helper",
            "--nocapture",
        ])
        .env(MODE, mode)
        .stdin(Stdio::null())
        .status()
        .expect("owner");
    assert!(status.success(), "the runner owner failed: {status}");
}

/// Exercises irreversible credential changes only in an isolated real owner.
fn security_owner() {
    fn status(custody: &ProcessCustody) -> String {
        let mut command = ProcessCommand::new("/bin/cat");
        command.arg("/proc/self/status");
        output(&mut command, custody)
    }
    fn field<'a>(status: &'a str, key: &str) -> &'a str {
        status
            .lines()
            .find(|line| line.starts_with(key))
            .expect(key)
    }
    let (root, first_custody) = custody();
    let before = status(&first_custody);
    let first_runner = runner_pid().expect("initial runner");
    // A different launching thread changes its own mask after runner creation.
    std::thread::scope(|scope| {
        scope
            .spawn(|| {
                // SAFETY: valid thread-local signal sets and no old-set output.
                unsafe {
                    let mut set: libc::sigset_t = std::mem::zeroed();
                    assert_eq!(libc::sigemptyset(&raw mut set), 0);
                    assert_eq!(libc::sigaddset(&raw mut set, libc::SIGUSR1), 0);
                    assert_eq!(
                        libc::pthread_sigmask(
                            libc::SIG_BLOCK,
                            &raw const set,
                            std::ptr::null_mut()
                        ),
                        0
                    );
                }
                let direct = Command::new("/bin/cat")
                    .arg("/proc/self/status")
                    .output()
                    .expect("direct child");
                let direct = String::from_utf8(direct.stdout).expect("status");
                let observed = status(&first_custody);
                assert_ne!(field(&direct, "SigBlk:"), field(&before, "SigBlk:"));
                assert_eq!(field(&observed, "SigBlk:"), field(&direct, "SigBlk:"));
                assert_eq!(
                    runner_pid(),
                    Some(first_runner),
                    "a signal mask needs no replacement runner"
                );
            })
            .join()
            .expect("launching thread");
    });
    let original = fs::read_to_string("/proc/thread-self/status").expect("owner status");
    if field(&original, "NoNewPrivs:").ends_with('0') {
        // SAFETY: irreversible restriction confined to this helper process.
        assert_eq!(
            unsafe { libc::prctl(libc::PR_SET_NO_NEW_PRIVS, 1, 0, 0, 0) },
            0
        );
        let observed = status(&first_custody);
        assert!(field(&observed, "NoNewPrivs:").ends_with('1'));
        assert_ne!(
            runner_pid(),
            Some(first_runner),
            "old runner retained unrestricted credentials"
        );
        eprintln!("actual NoNewPrivs transition qualified");
    }
    drop(first_custody);
    drop(root);
    // Where the original owner has privilege, reproduce the reviewed root ->
    // unprivileged transition, including supplementary groups and saved IDs.
    // SAFETY: reads this isolated owner's effective user.
    if unsafe { libc::geteuid() } == 0 {
        let privileged_runner = runner_pid().expect("privileged runner");
        // SAFETY: permanent privilege drop confined to this helper process.
        unsafe {
            assert_eq!(libc::setgroups(0, std::ptr::null()), 0);
            assert_eq!(libc::setresgid(65534, 65534, 65534), 0);
            assert_eq!(libc::setresuid(65534, 65534, 65534), 0);
        }
        let (_root, current_custody) = custody();
        let observed = status(&current_custody);
        let current = fs::read_to_string("/proc/thread-self/status").expect("current credentials");
        for key in ["Uid:", "Gid:", "Groups:"] {
            assert_eq!(field(&observed, key), field(&current, key));
        }
        assert_ne!(runner_pid(), Some(privileged_runner));
        eprintln!("actual root credential and supplementary-group drop qualified");
    }
    retire_runner();
}

#[test]
fn a_runner_preserves_current_security_and_each_launching_threads_signal_mask() {
    owner("security");
}

#[test]
fn a_custodied_program_reads_null_input_to_its_end() {
    let (_root, custody) = custody();
    let mut command = ProcessCommand::new("/bin/sh");
    command.args(["-c", "cat; echo status=$?"]);
    assert_eq!(output(&mut command, &custody), "status=0\n");
}

/// What `script` prints when the owner forks it directly, as std does.
fn forked(script: &str) -> String {
    let output = Command::new("/bin/sh")
        .args(["-c", script])
        .stdin(Stdio::null())
        .output()
        .expect("forked");
    String::from_utf8(output.stdout).expect("utf-8")
}

#[test]
fn a_custodied_program_inherits_the_owners_current_mask_and_limits() {
    let (_root, custody) = custody();
    let script = "umask; ulimit -c";
    let custodied = |custody: &ProcessCustody| {
        let mut command = ProcessCommand::new("/bin/sh");
        command.args(["-c", script]);
        output(&mut command, custody)
    };
    // The runner exists before the owner changes anything.
    let before = custodied(&custody);
    assert_eq!(before, forked(script));
    let mut limit = libc::rlimit {
        rlim_cur: 0,
        rlim_max: 0,
    };
    // SAFETY: writable limit storage for this process.
    let read = unsafe { libc::getrlimit(libc::RLIMIT_CORE, &raw mut limit) };
    assert_eq!(read, 0);
    let changed = libc::rlimit {
        rlim_cur: if limit.rlim_cur == 0 {
            limit.rlim_max.min(1 << 20)
        } else {
            0
        },
        rlim_max: limit.rlim_max,
    };
    // SAFETY: sets this process's mask and a harmless limit; both restored.
    let mask = unsafe {
        let mask = libc::umask(0o022);
        libc::umask(mask ^ 0o001);
        assert_eq!(libc::setrlimit(libc::RLIMIT_CORE, &raw const changed), 0);
        mask
    };
    let expected = forked(script);
    let after = custodied(&custody);
    // SAFETY: restores what this test changed.
    unsafe {
        libc::umask(mask);
        libc::setrlimit(libc::RLIMIT_CORE, &raw const limit);
    }
    // Control: the change is visible to a direct fork.
    assert_ne!(expected, before, "the owner's change is not observable");
    assert_eq!(after, expected);
}
