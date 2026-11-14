//! Real IPC objects come from shmget/shmat in this test, never fabricated
//! identifiers or captured text. The same compiled child is the negative
//! control, under the same UID and segment permissions, without confinement.
//! The same launches check self-identity queries and named-process refusal.

#![allow(unsafe_code, reason = "real IPC and process-group conformance FFI")]

use super::*;
use crate::{ProcessCommand, ProcessStdio, ProcessTree};

#[test]
fn anonymous_socketpair_is_local_ipc() {
    local_operation("socketpair");
}

#[test]
fn process_group_changes_remain_in_custody() {
    local_operation("setpgid");
    cancel_after_group_change("hold-setpgid");
}

#[test]
fn session_changes_remain_in_custody() {
    local_operation("setsid");
    cancel_after_group_change("hold-setsid");
}

#[test]
fn moved_groups_deliver_graceful_shutdown_to_children() {
    use std::io::BufRead;
    crate::initialize_native_io_services().unwrap();
    let executable = std::env::current_exe().unwrap();
    for operation in ["grace-setpgid", "grace-setsid"] {
        for custodied in [false, true] {
            let root = tempfile::tempdir().unwrap();
            let grant = ExecutionGrant::workspace_default(
                root.path(),
                ExecutionAccess::WorkspaceWrite,
                [],
                [executable.clone()],
            )
            .unwrap();
            let custody = crate::ProcessCustody::open(&root.path().join("custody")).unwrap();
            let mut command = ProcessCommand::new(&executable);
            command
                .args([
                    "--exact",
                    "execution::tests::ipc::local_probe",
                    "--nocapture",
                ])
                .current_dir(root.path())
                .env_clear()
                .env("ACYCLIC_LOCAL_OPERATION", operation)
                .stdin(ProcessStdio::null())
                .stdout(ProcessStdio::piped())
                .stderr(ProcessStdio::piped());
            let mut tree =
                ProcessTree::spawn_granted(command, &grant, custodied.then_some(&custody)).unwrap();
            let mut stdout = std::io::BufReader::new(tree.take_stdout().unwrap());
            assert!(
                (&mut stdout)
                    .lines()
                    .any(|line| line.unwrap() == "GRACE_READY")
            );
            // This API's caller-supplied grace is not the oracle: forced cleanup
            // cannot produce the child's signal acknowledgement or exit zero.
            let status = tree
                .terminate_after(std::time::Duration::from_secs(5))
                .unwrap();
            assert!(
                status.success(),
                "{operation} custody={custodied}: {status}"
            );
            assert_eq!(
                std::fs::read(root.path().join("graceful-child")).unwrap(),
                b"SIGTERM"
            );
        }
    }
}

#[test]
fn graceful_child_probe() {
    if std::env::var_os("ACYCLIC_GRACEFUL_CHILD").is_none() {
        return;
    }
    // The parent blocked SIGTERM before exec, so every libtest thread inherits
    // that mask. Only this synchronous wait can consume the group's signal.
    // SAFETY: initialized signal set, no handler, and writable sigwait output.
    unsafe {
        let mut set: libc::sigset_t = std::mem::zeroed();
        assert_eq!(libc::sigemptyset(&raw mut set), 0);
        assert_eq!(libc::sigaddset(&raw mut set, libc::SIGTERM), 0);
        assert_ne!(libc::signal(libc::SIGTERM, libc::SIG_DFL), libc::SIG_ERR);
        println!("GRACE_READY");
        use std::io::Write;
        std::io::stdout().flush().unwrap();
        let mut received = 0;
        assert_eq!(libc::sigwait(&raw const set, &raw mut received), 0);
        assert_eq!(received, libc::SIGTERM);
    }
    std::fs::write("graceful-child", b"SIGTERM").unwrap();
}

fn cancel_after_group_change(operation: &str) {
    use std::io::BufRead;
    let root = tempfile::tempdir().unwrap();
    let executable = std::env::current_exe().unwrap();
    let grant = ExecutionGrant::workspace_default(
        root.path(),
        ExecutionAccess::WorkspaceWrite,
        [],
        [executable.clone()],
    )
    .unwrap();
    let custody = crate::ProcessCustody::open(&root.path().join("custody")).unwrap();
    for custody in [None, Some(&custody)] {
        let mut command = ProcessCommand::new(&executable);
        command
            .args([
                "--exact",
                "execution::tests::ipc::local_probe",
                "--nocapture",
            ])
            .current_dir(root.path())
            .env_clear()
            .env("ACYCLIC_LOCAL_OPERATION", operation)
            .stdin(ProcessStdio::piped())
            .stdout(ProcessStdio::piped())
            .stderr(ProcessStdio::piped());
        let mut tree = ProcessTree::spawn_granted(command, &grant, custody).unwrap();
        // Keep the input writer alive across termination: EOF cannot masquerade
        // as cancellation of the guest's blocking read in its new group.
        let _input = tree.take_stdin().unwrap();
        let stdout = std::io::BufReader::new(tree.take_stdout().unwrap());
        assert!(stdout.lines().any(|line| line.unwrap() == "GROUP_READY"));
        let lifetime =
            crate::FileLock::new(std::fs::File::open(root.path().join("lifetime")).unwrap());
        assert_eq!(
            lifetime
                .try_lock(crate::FileLockMode::Exclusive)
                .unwrap_err()
                .kind(),
            io::ErrorKind::WouldBlock
        );
        tree.terminate(None).unwrap();
        assert!(tree.is_reaped());
        lifetime.try_lock(crate::FileLockMode::Exclusive).unwrap();
    }
}

#[test]
fn host_socket_connection_is_denied_without_killing_guest() {
    local_operation("connect");
}

fn local_operation(operation: &str) {
    crate::initialize_native_io_services().unwrap();
    let root = tempfile::tempdir().unwrap();
    let socket = root.path().join("host.sock");
    let listener = std::os::unix::net::UnixListener::bind(&socket).unwrap();
    listener.set_nonblocking(true).unwrap();
    let executable = std::env::current_exe().unwrap();
    let custody = crate::ProcessCustody::open(&root.path().join("custody")).unwrap();
    let command = |confined| {
        let mut command = ProcessCommand::new(&executable);
        command
            .args([
                "--exact",
                "execution::tests::ipc::local_probe",
                "--nocapture",
            ])
            .current_dir(root.path())
            .env_clear()
            .env("ACYCLIC_LOCAL_OPERATION", operation)
            .env("ACYCLIC_LOCAL_CONFINED", confined)
            .env("ACYCLIC_LOCAL_SOCKET", &socket)
            .stdin(ProcessStdio::null())
            .stdout(ProcessStdio::piped())
            .stderr(ProcessStdio::piped());
        command
    };
    // A real listening host endpoint succeeds for the identical unrestricted
    // executable. Missing paths and unavailable services cannot fake this test.
    if operation == "connect" {
        complete(ProcessTree::spawn(&mut command("no")).unwrap());
        listener.accept().unwrap();
    }
    for network in [
        ExecutionNetwork::Off,
        ExecutionNetwork::On,
        ExecutionNetwork::TcpPorts(BTreeSet::from([443])),
    ] {
        let mut read = super::system_read_roots();
        read.push(executable.clone());
        let grant =
            ExecutionGrant::new(root.path(), ExecutionAccess::ReadOnly, read, network).unwrap();
        for custody in [None, Some(&custody)] {
            complete(ProcessTree::spawn_granted(command("yes"), &grant, custody).unwrap());
            assert_eq!(
                listener.accept().unwrap_err().kind(),
                io::ErrorKind::WouldBlock
            );
        }
    }
}

#[test]
fn local_probe() {
    let Ok(operation) = std::env::var("ACYCLIC_LOCAL_OPERATION") else {
        return;
    };
    let selected = operation
        .strip_prefix("hold-")
        .or_else(|| operation.strip_prefix("grace-"))
        .unwrap_or(&operation);
    match selected {
        "socketpair" => {
            use std::io::{Read, Write};
            let (mut send, mut receive) = std::os::unix::net::UnixStream::pair().unwrap();
            send.write_all(b"local").unwrap();
            let mut bytes = [0; 5];
            receive.read_exact(&mut bytes).unwrap();
            assert_eq!(&bytes, b"local");
            // Datagram pairs can retarget a host service with sendto/sendmsg.
            assert_eq!(
                std::os::unix::net::UnixDatagram::pair()
                    .unwrap_err()
                    .raw_os_error(),
                Some(libc::EPERM)
            );
        }
        "setpgid" => {
            // SAFETY: zero selects the calling process and its own new group.
            assert_eq!(
                // SAFETY: zero selects the calling process and its own group, with no pointers.
                unsafe { libc::setpgid(0, 0) },
                0,
                "{}",
                io::Error::last_os_error()
            );
            // SAFETY: both calls only query the calling process's identity.
            assert_eq!(unsafe { libc::getpgid(0) }, unsafe { libc::getpid() });
        }
        "setsid" => {
            // SAFETY: creates a session containing only this process.
            let session = unsafe { libc::setsid() };
            assert!(session > 0, "{}", io::Error::last_os_error());
            // SAFETY: zero queries this process's session.
            assert_eq!(unsafe { libc::getsid(0) }, session);
        }
        "connect" => {
            let socket = std::env::var_os("ACYCLIC_LOCAL_SOCKET").unwrap();
            let result = std::os::unix::net::UnixStream::connect(socket);
            if std::env::var("ACYCLIC_LOCAL_CONFINED").unwrap() == "yes" {
                assert_eq!(result.unwrap_err().raw_os_error(), Some(libc::EPERM));
            } else {
                drop(result.unwrap());
            }
        }
        _ => panic!("unknown local operation: {operation}"),
    }
    if operation.starts_with("hold-") {
        use std::io::{Read, Write};
        let _lifetime = crate::FileLock::acquire(
            std::fs::File::create("lifetime").unwrap(),
            crate::FileLockMode::Exclusive,
        )
        .unwrap();
        println!("GROUP_READY");
        std::io::stdout().flush().unwrap();
        std::io::stdin().read_exact(&mut [0]).unwrap();
        panic!("held guest received unexpected input");
    }
    if operation.starts_with("grace-") {
        // This root deliberately outlives SIGTERM until its child acknowledges
        // that signal. Signalling only the root cannot make this test pass.
        // SAFETY: isolated fixture process; block in the spawning thread so
        // all threads in the child's exec inherit the synchronous wait mask.
        unsafe {
            assert_ne!(libc::signal(libc::SIGTERM, libc::SIG_IGN), libc::SIG_ERR);
            let mut set: libc::sigset_t = std::mem::zeroed();
            assert_eq!(libc::sigemptyset(&raw mut set), 0);
            assert_eq!(libc::sigaddset(&raw mut set, libc::SIGTERM), 0);
            assert_eq!(
                libc::pthread_sigmask(libc::SIG_BLOCK, &raw const set, std::ptr::null_mut()),
                0
            );
        }
        let status = std::process::Command::new(std::env::current_exe().unwrap())
            .args([
                "--exact",
                "execution::tests::ipc::graceful_child_probe",
                "--nocapture",
            ])
            .env("ACYCLIC_GRACEFUL_CHILD", "1")
            .status()
            .unwrap();
        assert!(status.success(), "graceful child: {status}");
    }
}

struct Segment {
    id: libc::c_int,
    address: *mut libc::c_void,
}

impl Drop for Segment {
    fn drop(&mut self) {
        if self.id >= 0 {
            // SAFETY: this test owns the segment identifier; IPC_RMID has no
            // output buffer and also handles an attachment failure.
            unsafe { libc::shmctl(self.id, libc::IPC_RMID, core::ptr::null_mut()) };
        }
        if self.address.addr() != usize::MAX {
            // SAFETY: the successful attachment remains owned by this guard.
            unsafe { libc::shmdt(self.address) };
        }
    }
}

#[test]
fn workspace_and_network_grants_cannot_attach_host_shared_memory() {
    // SAFETY: a fresh private segment, owned by this UID, with one data byte.
    let id = unsafe { libc::shmget(libc::IPC_PRIVATE, 1, libc::IPC_CREAT | 0o600) };
    assert!(id >= 0, "shmget: {}", io::Error::last_os_error());
    let mut segment = Segment {
        id,
        // SAFETY: asks the kernel to select the attachment address.
        address: unsafe { libc::shmat(id, core::ptr::null(), 0) },
    };
    assert_ne!(segment.address.addr(), usize::MAX, "shmat failed");
    assert!(!segment.address.is_null());
    // Linux permits reattachment to a marked segment while our attachment
    // holds it alive. Mark now so host loss cannot leave a persistent object.
    assert_eq!(
        // SAFETY: this test owns this live segment; no output buffer is used.
        unsafe { libc::shmctl(id, libc::IPC_RMID, core::ptr::null_mut()) },
        0
    );
    segment.id = -1;
    // SAFETY: the attached segment has at least one live byte.
    unsafe { segment.address.cast::<u8>().write_volatile(0) };
    let root = tempfile::tempdir().unwrap();
    let executable = std::env::current_exe().unwrap();
    let mut read = super::system_read_roots();
    read.push(executable.clone());
    let grant = ExecutionGrant::new(
        root.path(),
        ExecutionAccess::WorkspaceWrite,
        read,
        ExecutionNetwork::On,
    )
    .unwrap();
    let command = |allowed| {
        let mut command = ProcessCommand::new(&executable);
        command
            .args(["--exact", "execution::tests::ipc::probe", "--nocapture"])
            .current_dir(root.path())
            .env_clear()
            .env("ACYCLIC_BOUNDARY_IPC_ID", id.to_string())
            .env("ACYCLIC_BOUNDARY_IPC_ALLOWED", allowed)
            .stdin(ProcessStdio::null())
            .stdout(ProcessStdio::piped())
            .stderr(ProcessStdio::piped());
        command
    };
    let custody = crate::ProcessCustody::open(&root.path().join("custody")).unwrap();
    let mut admitted = 0;
    let mut refused = 0;
    for custody in [None, Some(&custody)] {
        let tree = match ProcessTree::spawn_granted(command("no"), &grant, custody) {
            Ok(tree) => tree,
            Err(error) if error.kind() == io::ErrorKind::Unsupported => {
                assert!(error.get_ref().unwrap().is::<ExecutionRefusal>());
                // SAFETY: the original attachment is still owned here.
                assert_eq!(unsafe { segment.address.cast::<u8>().read_volatile() }, 0);
                eprintln!("confined child refused: {error}");
                refused += 1;
                continue;
            }
            Err(error) => panic!("confined IPC probe admission: {error}"),
        };
        complete(tree);
        admitted += 1;
        // SAFETY: the original attachment is live; the child has exited.
        assert_eq!(unsafe { segment.address.cast::<u8>().read_volatile() }, 0);
    }
    // This genuine child can mutate the same host object when unrestricted.
    complete(ProcessTree::spawn(&mut command("yes")).unwrap());
    // SAFETY: the original attachment is live; the child has exited.
    assert_eq!(unsafe { segment.address.cast::<u8>().read_volatile() }, 1);
    eprintln!("original child coverage: confined={admitted}, refused={refused}, unrestricted=1");
}

fn complete(mut tree: ProcessTree) {
    let output = tree
        .wait_with_output(
            crate::CaptureSchedule::Unbounded,
            65536,
            None,
            || Ok(()),
            |_| Ok(Default::default()),
            None,
            None,
            None,
        )
        .unwrap();
    assert!(output.status.success(), "{output:?}");
}

#[test]
fn probe() {
    let Ok(id) = std::env::var("ACYCLIC_BOUNDARY_IPC_ID") else {
        return;
    };
    let id = id.parse().unwrap();
    let allowed = std::env::var("ACYCLIC_BOUNDARY_IPC_ALLOWED").unwrap() == "yes";
    // Exercise the original generated filter in the kernel, through the same
    // granted launch used above. Zero selects the caller; explicitly naming
    // even our own PID requires the authority denied to named-process queries.
    // SAFETY: these queries take only integer identifiers and no pointers.
    let group = unsafe { libc::getpgid(0) };
    assert!(group > 0, "self group: {}", io::Error::last_os_error());
    // SAFETY: zero selects this process's session without mutation.
    assert!(unsafe { libc::getsid(0) } > 0);
    #[cfg(target_arch = "x86_64")]
    // SAFETY: getpgrp has no arguments and only reads the caller's identity.
    assert_eq!(unsafe { libc::getpgrp() }, group);
    let own_pid = libc::c_long::from(std::process::id());
    for call in [libc::SYS_getpgid, libc::SYS_getsid] {
        // SAFETY: this names the live current process, with no pointer or
        // mutation. The unrestricted child is the identical negative control.
        let queried = unsafe { libc::syscall(call, own_pid) };
        if allowed {
            assert!(
                queried > 0,
                "unrestricted query: {}",
                io::Error::last_os_error()
            );
        } else {
            assert_eq!(queried, -1, "named process acquired query authority");
            assert_eq!(io::Error::last_os_error().raw_os_error(), Some(libc::EPERM));
        }
    }
    // SAFETY: this is the real host-supplied segment; no pointer is supplied.
    let address = unsafe { libc::shmat(id, core::ptr::null(), 0) };
    if address.addr() == usize::MAX {
        assert!(!allowed, "unconfined IPC negative control failed");
        assert_eq!(io::Error::last_os_error().raw_os_error(), Some(libc::EPERM));
        return;
    }
    let segment = Segment { id: -1, address };
    assert!(allowed, "a confined guest attached host shared memory");
    assert!(!segment.address.is_null());
    // SAFETY: the host segment has one live byte. The parent waits for this
    // child's exit before reading it; volatility retains the external write.
    unsafe { segment.address.cast::<u8>().write_volatile(1) };
}
