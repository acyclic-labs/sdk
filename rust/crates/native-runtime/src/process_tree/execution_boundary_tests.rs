use super::*;
use crate::execution::{ExecutionAccess, ExecutionGrant, ExecutionNetwork, ExecutionRefusal};
use std::os::fd::{AsRawFd, FromRawFd, OwnedFd};
use std::path::Path;

fn directory_command(cwd: &Path) -> ProcessCommand {
    let mut command = ProcessCommand::new("/bin/sh");
    command
        .args(["-c", "pwd -P"])
        .env_clear()
        .current_dir(cwd)
        .stdin(ProcessStdio::null())
        .stdout(ProcessStdio::piped())
        .stderr(ProcessStdio::piped());
    command
}

fn directory_output(mut command: ProcessCommand, custody: Option<&ProcessCustody>) -> String {
    let mut retained = None;
    match custody {
        Some(custody) => {
            ProcessTree::spawn_in_custody_controlled(&mut command, custody, None, &mut retained)
        }
        None => ProcessTree::spawn_controlled(&mut command, None, &mut retained),
    }
    .unwrap();
    let output = retained
        .unwrap()
        .wait_with_output(
            Duration::from_secs(5),
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
    String::from_utf8(output.stdout).unwrap()
}

#[test]
fn granted_cwd_keeps_the_original_object_across_path_replacement() {
    let root = tempfile::tempdir().unwrap();
    let workspace = root.path().join("workspace");
    let external = root.path().join("external");
    std::fs::create_dir(&workspace).unwrap();
    std::fs::create_dir(&external).unwrap();
    let custody = ProcessCustody::open(&root.path().join("custody")).unwrap();
    for custody in [None, Some(&custody)] {
        let selected = workspace.join("selected");
        let held = workspace.join("held");
        std::fs::create_dir(&selected).unwrap();
        let grant = ExecutionGrant::new(
            &workspace,
            ExecutionAccess::ReadOnly,
            crate::execution::tests::system_read_roots(),
            ExecutionNetwork::Off,
        )
        .unwrap();
        let mut command = directory_command(&selected);
        let cwd = grant
            .validate_launch(Some(&selected), command.get_env_clear())
            .unwrap();
        let boundary = match grant.prepare_launch(Some(cwd), None) {
            Ok(boundary) => boundary,
            Err(error) if error.kind() == io::ErrorKind::Unsupported => {
                assert!(error.get_ref().unwrap().is::<ExecutionRefusal>());
                return;
            }
            Err(error) => panic!("bound directory preparation failed: {error}"),
        };
        // Deterministically replace the path after preparation, before either
        // original launch path consumes its held object. No racing thread,
        // readiness loop or test-only launch callback is needed.
        std::fs::rename(&selected, &held).unwrap();
        // Path admission must not resolve this now-absent name outside the
        // original operation ledger. The old canonicalization failed here,
        // before native preparation could even reserve its first operation.
        let absent = grant.validate_launch(Some(&selected), true).unwrap();
        let denied = crate::tests::ReadBudgetMeter::new(0, u64::MAX);
        let error = grant
            .prepare_launch(Some(absent), Some(&denied))
            .unwrap_err();
        assert_eq!(error.kind(), io::ErrorKind::Other);
        std::os::unix::fs::symlink(&external, &selected).unwrap();
        command.boundary = Some(boundary);
        assert_eq!(
            directory_output(command, custody),
            format!("{}\n", std::fs::canonicalize(&held).unwrap().display())
        );
        // The identical real command with ordinary path selection follows
        // the replacement, proving the object pin is what prevents escape.
        assert_eq!(
            directory_output(directory_command(&selected), custody),
            format!("{}\n", std::fs::canonicalize(&external).unwrap().display())
        );
        // A replacement in the earlier validation/preparation interval must
        // instead refuse: openat2 cannot traverse this substituted symlink.
        let error = grant.prepare_launch(Some(cwd), None).unwrap_err();
        assert!(error.get_ref().unwrap().is::<ExecutionRefusal>());
        std::fs::remove_file(&selected).unwrap();
        std::fs::remove_dir(&held).unwrap();
    }
}

#[test]
fn closed_standard_streams_preserve_confined_setup() {
    for mode in [
        "direct-root",
        "direct-nested",
        "custody-root",
        "custody-nested",
    ] {
        let mut command = ProcessCommand::new(std::env::current_exe().unwrap());
        command
            .args([
                "--exact",
                "process_tree::execution_boundary_tests::closed_stream_probe",
                "--nocapture",
            ])
            .env_clear()
            .env("ACYCLIC_BOUNDARY_CLOSED_STREAMS", mode)
            .stdin(ProcessStdio::null())
            .stdout(ProcessStdio::piped())
            .stderr(ProcessStdio::piped());
        directory_output(command, None);
    }
}

// Only the isolated real child changes its standard streams. Restore them
// even on a failed assertion so the original harness can report that failure.
struct ClosedStreams([OwnedFd; 3]);

impl ClosedStreams {
    fn new() -> Self {
        let saved = [0, 1, 2].map(|fd| {
            // SAFETY: the parent explicitly supplied all three standard
            // streams; duplication returns a uniquely owned descriptor >= 3.
            let saved = unsafe { libc::fcntl(fd, libc::F_DUPFD_CLOEXEC, 3) };
            assert!(
                saved >= 3,
                "saving standard stream: {}",
                io::Error::last_os_error()
            );
            // SAFETY: the successful duplicate is uniquely owned here.
            unsafe { OwnedFd::from_raw_fd(saved) }
        });
        let guard = Self(saved);
        for fd in 0..3 {
            // SAFETY: this isolated child owns these explicit standard streams.
            assert_eq!(unsafe { libc::close(fd) }, 0);
        }
        guard
    }
}

impl Drop for ClosedStreams {
    fn drop(&mut self) {
        for (fd, saved) in (0..3).zip(&self.0) {
            // SAFETY: each saved descriptor remains live through restoration;
            // only this isolated child's standard stream targets are replaced.
            assert_eq!(unsafe { libc::dup2(saved.as_raw_fd(), fd) }, fd);
        }
    }
}

#[test]
fn closed_stream_probe() {
    let Ok(mode) = std::env::var("ACYCLIC_BOUNDARY_CLOSED_STREAMS") else {
        return;
    };
    let root = tempfile::tempdir().unwrap();
    let workspace = root.path().join("workspace");
    let nested = workspace.join("nested");
    std::fs::create_dir_all(&nested).unwrap();
    let selected = if mode.ends_with("nested") {
        &nested
    } else {
        &workspace
    };
    let grant = ExecutionGrant::new(
        &workspace,
        ExecutionAccess::WorkspaceWrite,
        crate::execution::tests::system_read_roots(),
        ExecutionNetwork::Off,
    )
    .unwrap();
    let custody = ProcessCustody::open(&root.path().join("custody")).unwrap();
    let streams = ClosedStreams::new();

    // Real negative control: without relocation a directory in slot zero is
    // replaced by the exact stdio operation made before guest entry.
    let unprotected = std::fs::File::open(selected).unwrap();
    assert_eq!(unprotected.as_raw_fd(), 0);
    assert!(unprotected.metadata().unwrap().is_dir());
    let null = std::fs::File::open("/dev/null").unwrap();
    // SAFETY: both descriptors are owned by this isolated child.
    assert_eq!(unsafe { libc::dup2(null.as_raw_fd(), 0) }, 0);
    assert!(!unprotected.metadata().unwrap().is_dir());
    drop(unprotected);
    drop(null);

    // Actual low-slot descriptor operations also exercise reservation denial,
    // late recording failure and the otherwise identical funded operation.
    for (limit, failure) in [(0, None), (1, Some(1)), (1, None)] {
        let file = std::fs::File::open(selected).unwrap();
        assert_eq!(file.as_raw_fd(), 0);
        let mut meter = crate::tests::ReadBudgetMeter::new(limit, u64::MAX);
        meter.fail_recorded_operation = failure;
        match crate::file::outside_standard_streams(file.into(), Some(&meter)) {
            Ok(file) => {
                assert_eq!((limit, failure), (1, None));
                assert!(std::fs::File::from(file).metadata().unwrap().is_dir());
            }
            Err(error) => {
                assert!(limit == 0 || failure.is_some());
                assert_eq!(error.kind(), io::ErrorKind::Other);
            }
        }
    }
    let mut command = ProcessCommand::new("/bin/sh");
    command
        .args(["-c", "printf guest > marker"])
        .env_clear()
        .current_dir(selected)
        .stdin(ProcessStdio::null())
        .stdout(ProcessStdio::piped())
        .stderr(ProcessStdio::piped());
    let cwd = grant
        .validate_launch(command.get_current_dir(), command.get_env_clear())
        .unwrap();
    let boundary = match grant.prepare_launch(Some(cwd), None) {
        Ok(boundary) => boundary,
        Err(error) if error.kind() == io::ErrorKind::Unsupported => {
            assert!(error.get_ref().unwrap().is::<ExecutionRefusal>());
            assert!(!selected.join("marker").exists());
            return;
        }
        Err(error) => panic!("closed-stream preparation failed: {error}"),
    };
    // Restoration itself remaps 0..2. All preparation objects must survive
    // that before the original launcher installs the guest's own streams.
    // Do this before starting an event driver that could acquire a low slot.
    drop(streams);
    command.boundary = Some(boundary);
    let custody = mode.starts_with("custody").then_some(&custody);
    assert!(directory_output(command, custody).is_empty());
    assert_eq!(
        std::fs::read_to_string(selected.join("marker")).unwrap(),
        "guest"
    );
}
