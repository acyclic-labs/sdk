use super::*;

#[cfg(target_os = "linux")]
mod ipc;

#[cfg(target_os = "linux")]
fn system_read_roots() -> Vec<PathBuf> {
    ["/usr", "/bin", "/lib", "/lib64", "/etc/ld.so.cache"]
        .into_iter()
        .map(PathBuf::from)
        .filter(|path| path.exists())
        .collect()
}

#[test]
fn child_grants_cannot_reacquire_revoked_capabilities() {
    let root = tempfile::tempdir().unwrap();
    let workspace = root.path().join("workspace");
    let nested = workspace.join("nested");
    let external = root.path().join("external");
    std::fs::create_dir_all(&nested).unwrap();
    std::fs::create_dir(&external).unwrap();
    let ports = BTreeSet::from([443, 8443]);
    let parent = ExecutionGrant::new(
        &workspace,
        ExecutionAccess::WorkspaceWrite,
        [external.clone()],
        ExecutionNetwork::TcpPorts(ports),
    )
    .unwrap();
    let child = match parent.attenuate(
        &nested,
        ExecutionAccess::ReadOnly,
        [external.clone()],
        ExecutionNetwork::TcpPorts(BTreeSet::from([443])),
    ) {
        Ok(child) => child,
        Err(error) if error.kind() == std::io::ErrorKind::Unsupported => {
            assert!(error.get_ref().unwrap().is::<ExecutionRefusal>());
            return;
        }
        Err(error) => panic!("grant attenuation failed: {error}"),
    };
    for (workspace, access, read, network) in [
        (
            &nested,
            ExecutionAccess::WorkspaceWrite,
            vec![],
            ExecutionNetwork::Off,
        ),
        (
            &workspace,
            ExecutionAccess::ReadOnly,
            vec![],
            ExecutionNetwork::Off,
        ),
        (
            &nested,
            ExecutionAccess::ReadOnly,
            vec![],
            ExecutionNetwork::On,
        ),
        (
            &nested,
            ExecutionAccess::ReadOnly,
            vec![],
            ExecutionNetwork::TcpPorts(BTreeSet::from([80])),
        ),
        (
            &nested,
            ExecutionAccess::ReadOnly,
            vec![root.path().to_path_buf()],
            ExecutionNetwork::Off,
        ),
    ] {
        let error = child
            .attenuate(workspace, access, read, network)
            .unwrap_err();
        assert!(error.to_string().starts_with("tool execution failed: "));
    }
    child
        .attenuate(
            &nested,
            ExecutionAccess::ReadOnly,
            [],
            ExecutionNetwork::Off,
        )
        .unwrap();
}

#[cfg(target_os = "linux")]
#[test]
fn kernel_boundary_confines_descendants_and_preserves_host_authority() {
    use crate::ProcessTree;
    use crate::{ProcessCommand as Command, ProcessStdio as Stdio};
    let root = tempfile::tempdir().unwrap();
    let workspace = root.path().join("workspace");
    let external = root.path().join("external");
    std::fs::create_dir(&workspace).unwrap();
    std::fs::create_dir(&external).unwrap();
    let sentinel = external.join("sentinel");
    std::fs::write(&sentinel, "original").unwrap();
    std::os::unix::fs::symlink(&sentinel, workspace.join("escape")).unwrap();
    let read = system_read_roots();
    let custody = crate::ProcessCustody::open(&root.path().join("custody")).unwrap();
    for (access, custody) in [ExecutionAccess::WorkspaceWrite, ExecutionAccess::ReadOnly]
        .into_iter()
        .flat_map(|access| [None, Some(&custody)].map(|custody| (access, custody)))
    {
        let grant =
            ExecutionGrant::new(&workspace, access, read.clone(), ExecutionNetwork::Off).unwrap();
        let make_command = || {
            let mut command = Command::new("/bin/sh");
            command.args(["-c", "(printf changed > escape) 2>/dev/null; (printf created > new) 2>/dev/null; cat escape 2>/dev/null; true"])
                .current_dir(&workspace).stdin(Stdio::null())
                .stdout(Stdio::piped()).stderr(Stdio::piped());
            command
        };
        // A valid filesystem grant cannot expose ambient host secrets. Refuse
        // before native preparation; the same real command below is admitted
        // only after its environment has been made explicit.
        let error = match ProcessTree::spawn_granted(make_command(), &grant, custody) {
            Ok(_) => panic!("confined command inherited the host environment"),
            Err(error) => error,
        };
        assert!(
            error
                .to_string()
                .contains("requires an explicit environment")
        );
        assert_eq!(std::fs::read_to_string(&sentinel).unwrap(), "original");
        assert!(!workspace.join("new").exists());
        let cancelled = crate::tests::ReadBudgetMeter::new(u64::MAX, u64::MAX);
        cancelled
            .cancelled
            .store(true, std::sync::atomic::Ordering::Release);
        let mut retained = None;
        let mut cancelled_command = make_command();
        cancelled_command.env_clear();
        let error = ProcessTree::spawn_granted_controlled(
            cancelled_command,
            &grant,
            custody,
            Some(&cancelled),
            &mut retained,
        )
        .expect_err("a cancelled invocation prepared a native boundary");
        assert_eq!(error.kind(), io::ErrorKind::Interrupted);
        assert!(retained.is_none());
        assert!(!workspace.join("new").exists());
        let mut command = make_command();
        command.env_clear();
        let original = crate::tests::ReadBudgetMeter::new(u64::MAX, u64::MAX);
        let mut retained = None;
        match ProcessTree::spawn_granted_controlled(
            command,
            &grant,
            custody,
            Some(&original),
            &mut retained,
        ) {
            Ok(()) => {}
            Err(error) if error.kind() == std::io::ErrorKind::Unsupported => {
                // An unavailable boundary must return a concrete refusal and
                // must not run even the workspace-authorized part of the guest.
                assert!(error.get_ref().unwrap().is::<ExecutionRefusal>());
                assert!(retained.is_none());
                assert_eq!(std::fs::read_to_string(&sentinel).unwrap(), "original");
                assert!(!workspace.join("new").exists());
                continue;
            }
            Err(error) => panic!("confined launch failed: {error}"),
        }
        // An existing recovery slot is authority, not replaceable output.
        // The rejected command would create another observable file if the
        // consuming entry accidentally launched or overwrote that slot.
        let mut rejected = Command::new("/bin/sh");
        rejected
            .args(["-c", "printf replaced > unexpected"])
            .env_clear()
            .current_dir(&workspace)
            .stdin(Stdio::null())
            .stdout(Stdio::null())
            .stderr(Stdio::null());
        let error = ProcessTree::spawn_granted_controlled(
            rejected,
            &grant,
            custody,
            Some(&original),
            &mut retained,
        )
        .expect_err("an occupied recovery slot was replaced");
        assert!(error.get_ref().unwrap().is::<ExecutionRefusal>());
        let mut tree = retained.expect("admitted tree retains its original recovery slot");
        let output = tree.wait_with_output(
            std::time::Duration::from_secs(5),
            65536,
            None,
            || Ok(()),
            |_| Ok(Default::default()),
            None,
            None,
            None,
        );
        assert!(output.is_ok(), "{output:?}");
        assert!(!workspace.join("unexpected").exists());
        assert_eq!(std::fs::read_to_string(&sentinel).unwrap(), "original");
        let output = output.unwrap();
        assert!(output.status.success(), "{output:?}");
        assert!(output.stdout.is_empty(), "outside reads escaped Landlock");
        match access {
            ExecutionAccess::WorkspaceWrite => {
                assert_eq!(
                    std::fs::read_to_string(workspace.join("new")).unwrap(),
                    "created"
                );
                std::fs::remove_file(workspace.join("new")).unwrap();
            }
            ExecutionAccess::ReadOnly => assert!(!workspace.join("new").exists()),
        }
    }
    // Enforcement must affect only the child, never the launching host thread.
    std::fs::write(&sentinel, "host").unwrap();
}

#[cfg(target_os = "linux")]
#[test]
fn replaced_grant_root_refuses_before_guest_execution() {
    let root = tempfile::tempdir().unwrap();
    let workspace = root.path().join("workspace");
    std::fs::create_dir(&workspace).unwrap();
    let grant = ExecutionGrant::new(
        &workspace,
        ExecutionAccess::WorkspaceWrite,
        [],
        ExecutionNetwork::Off,
    )
    .unwrap();
    // The grant itself must pin the deleted object. A numeric identity alone
    // could otherwise be reused by the replacement directory.
    std::fs::remove_dir(&workspace).unwrap();
    std::fs::create_dir(&workspace).unwrap();
    let error = match grant.prepare() {
        Ok(_) => panic!("replaced workspace acquired the previous grant"),
        Err(error) => error,
    };
    assert!(error.to_string().contains("root was replaced"), "{error}");
    // A lexical prefix alone used to refresh the replaced root's identity and
    // mint a new child grant. The original object's authority must survive.
    let nested = workspace.join("nested");
    std::fs::create_dir(&nested).unwrap();
    let error = grant
        .attenuate(
            &nested,
            ExecutionAccess::ReadOnly,
            [],
            ExecutionNetwork::Off,
        )
        .unwrap_err();
    assert!(error.to_string().contains("root was replaced"), "{error}");
}

#[cfg(target_os = "linux")]
#[test]
fn native_attenuation_resolution_cannot_follow_a_replaced_ancestor() {
    let original = tempfile::tempdir().unwrap();
    let external = tempfile::tempdir().unwrap();
    let nested = original.path().join("nested");
    std::fs::create_dir(&nested).unwrap();
    std::fs::write(external.path().join("secret"), "outside").unwrap();
    let parent = BoundPath::new(original.path()).unwrap().bind().unwrap();
    std::fs::rename(&nested, original.path().join("previous")).unwrap();
    std::os::unix::fs::symlink(external.path(), &nested).unwrap();
    let error = linux::open_beneath(&parent, Path::new("nested/secret")).unwrap_err();
    assert!(error.get_ref().unwrap().is::<ExecutionRefusal>());
    // Negative control: ordinary path resolution follows the same link.
    assert_eq!(
        std::fs::read_to_string(nested.join("secret")).unwrap(),
        "outside"
    );
}

#[cfg(target_os = "linux")]
#[test]
fn kernel_network_grants_attenuate_without_a_network_service() {
    use crate::ProcessTree;
    use crate::{ProcessCommand as Command, ProcessStdio as Stdio};
    let root = tempfile::tempdir().unwrap();
    let listener = std::net::TcpListener::bind((std::net::Ipv4Addr::LOCALHOST, 0)).unwrap();
    listener.set_nonblocking(true).unwrap();
    let destination = listener.local_addr().unwrap();
    let other = std::net::TcpListener::bind((std::net::Ipv4Addr::LOCALHOST, 0)).unwrap();
    let other_port = other.local_addr().unwrap().port();
    let executable = std::env::current_exe().unwrap();
    let mut read = system_read_roots();
    read.push(executable.clone());
    let custody = crate::ProcessCustody::open(&root.path().join("custody")).unwrap();
    for (network, allowed, custody) in [
        (ExecutionNetwork::Off, false),
        (ExecutionNetwork::On, true),
        (
            ExecutionNetwork::TcpPorts(BTreeSet::from([destination.port()])),
            true,
        ),
        (
            ExecutionNetwork::TcpPorts(BTreeSet::from([other_port])),
            false,
        ),
    ]
    .into_iter()
    .flat_map(|(network, allowed)| {
        [None, Some(&custody)].map(move |custody| (network.clone(), allowed, custody))
    }) {
        let grant = ExecutionGrant::new(
            root.path(),
            ExecutionAccess::ReadOnly,
            read.clone(),
            network,
        )
        .unwrap();
        let mut command = Command::new(&executable);
        command
            .args(["--exact", "execution::tests::network_probe", "--nocapture"])
            .current_dir(root.path())
            .env_clear()
            .env(
                "ACYCLIC_BOUNDARY_PROBE_DESTINATION",
                destination.to_string(),
            )
            .env(
                "ACYCLIC_BOUNDARY_PROBE_ALLOWED",
                if allowed { "yes" } else { "no" },
            )
            .stdin(Stdio::null())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped());
        let mut tree = match ProcessTree::spawn_granted(command, &grant, custody) {
            Ok(tree) => tree,
            Err(error) if error.kind() == std::io::ErrorKind::Unsupported => {
                assert!(error.get_ref().unwrap().is::<ExecutionRefusal>());
                assert_eq!(
                    listener.accept().unwrap_err().kind(),
                    std::io::ErrorKind::WouldBlock
                );
                continue;
            }
            Err(error) => panic!("confined launch failed: {error}"),
        };
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
        assert!(
            output.status.success(),
            "{}",
            String::from_utf8_lossy(&output.stderr)
        );
        if allowed {
            listener.accept().unwrap();
        } else {
            assert_eq!(
                listener.accept().unwrap_err().kind(),
                std::io::ErrorKind::WouldBlock
            );
        }
    }
}

#[cfg(target_os = "linux")]
#[test]
fn inherited_file_stdio_cannot_bypass_the_workspace_boundary() {
    use crate::{ProcessCommand, ProcessStdio, ProcessTree};
    let root = tempfile::tempdir().unwrap();
    let workspace = root.path().join("workspace");
    std::fs::create_dir(&workspace).unwrap();
    let sentinel = root.path().join("outside");
    std::fs::write(&sentinel, "original").unwrap();
    let read = system_read_roots();
    let grant = ExecutionGrant::new(
        &workspace,
        ExecutionAccess::WorkspaceWrite,
        read,
        ExecutionNetwork::Off,
    )
    .unwrap();
    let command = || {
        let mut command = ProcessCommand::new("/bin/sh");
        command
            .args(["-c", "printf escaped"])
            .current_dir(&workspace)
            .env_clear()
            .stdin(ProcessStdio::null())
            .stderr(ProcessStdio::null())
            .stdout(
                std::fs::OpenOptions::new()
                    .append(true)
                    .open(&sentinel)
                    .unwrap(),
            );
        command
    };
    let custody = crate::ProcessCustody::open(&root.path().join("custody")).unwrap();
    for custody in [None, Some(&custody)] {
        let original = crate::tests::ReadBudgetMeter::new(u64::MAX, u64::MAX);
        let mut retained = None;
        let error = ProcessTree::spawn_granted_controlled(
            command(),
            &grant,
            custody,
            Some(&original),
            &mut retained,
        )
        .expect_err("an outside writable stdio descriptor entered the guest");
        assert!(error.get_ref().unwrap().is::<ExecutionRefusal>());
        assert!(matches!(
            error.kind(),
            io::ErrorKind::PermissionDenied | io::ErrorKind::Unsupported
        ));
        if let Some(tree) = retained.as_mut() {
            // A failed guest admission can retain the original warden/record.
            // Cleanup is a separate admitted operation, never a hidden retry
            // against the failed invocation or an unmetered convenience path.
            let recovery = crate::tests::ReadBudgetMeter::new(u64::MAX, u64::MAX);
            tree.terminate(Some(&recovery)).unwrap();
            assert!(tree.is_reaped());
        }
        assert_eq!(std::fs::read_to_string(&sentinel).unwrap(), "original");
    }
    // Real negative control: the identical trusted command writes through
    // this descriptor without the boundary, even though it names no path.
    let mut tree = ProcessTree::spawn(&mut command()).unwrap();
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
    assert!(output.status.success());
    assert_eq!(
        std::fs::read_to_string(&sentinel).unwrap(),
        "originalescaped"
    );
}

// The same compiled test executable is the real native child, so the probe
// depends on no installed interpreter, external network or test-only launcher.
#[cfg(target_os = "linux")]
#[test]
fn network_probe() {
    let Some(destination) = std::env::var_os("ACYCLIC_BOUNDARY_PROBE_DESTINATION") else {
        return;
    };
    let connected = std::net::TcpStream::connect(destination.to_str().unwrap()).is_ok();
    assert_eq!(
        connected,
        std::env::var("ACYCLIC_BOUNDARY_PROBE_ALLOWED").unwrap() == "yes"
    );
}
