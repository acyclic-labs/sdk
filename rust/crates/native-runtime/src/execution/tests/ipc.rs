//! Real IPC objects come from shmget/shmat in this test, never fabricated
//! identifiers or captured text. The same compiled child is the negative
//! control, under the same UID and segment permissions, without confinement.

#![allow(unsafe_code, reason = "real System V shared-memory conformance FFI")]

use super::*;
use crate::{ProcessCommand, ProcessStdio, ProcessTree};

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
    // SAFETY: this test owns this live segment; no output buffer is used.
    assert_eq!(
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
    for custody in [None, Some(&custody)] {
        let tree = match ProcessTree::spawn_granted(command("no"), &grant, custody) {
            Ok(tree) => tree,
            Err(error) if error.kind() == io::ErrorKind::Unsupported => {
                assert!(error.get_ref().unwrap().is::<ExecutionRefusal>());
                // SAFETY: the original attachment is still owned here.
                assert_eq!(unsafe { segment.address.cast::<u8>().read_volatile() }, 0);
                continue;
            }
            Err(error) => panic!("confined IPC probe admission: {error}"),
        };
        complete(tree);
        // SAFETY: the original attachment is live; the child has exited.
        assert_eq!(unsafe { segment.address.cast::<u8>().read_volatile() }, 0);
    }
    // This genuine child can mutate the same host object when unrestricted.
    complete(ProcessTree::spawn(&mut command("yes")).unwrap());
    // SAFETY: the original attachment is live; the child has exited.
    assert_eq!(unsafe { segment.address.cast::<u8>().read_volatile() }, 1);
}

fn complete(mut tree: ProcessTree) {
    let output = tree
        .wait_with_output(
            std::time::Duration::from_secs(5),
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
