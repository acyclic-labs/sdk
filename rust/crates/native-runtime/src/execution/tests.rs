use super::*;

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
    let child = parent
        .attenuate(
            &nested,
            ExecutionAccess::ReadOnly,
            [external.clone()],
            ExecutionNetwork::TcpPorts(BTreeSet::from([443])),
        )
        .unwrap();
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
    use std::process::{Command, Stdio};
    let root = tempfile::tempdir().unwrap();
    let workspace = root.path().join("workspace");
    let external = root.path().join("external");
    std::fs::create_dir(&workspace).unwrap();
    std::fs::create_dir(&external).unwrap();
    let sentinel = external.join("sentinel");
    std::fs::write(&sentinel, "original").unwrap();
    std::os::unix::fs::symlink(&sentinel, workspace.join("escape")).unwrap();
    let read = ["/usr", "/bin", "/lib", "/lib64", "/etc/ld.so.cache"]
        .into_iter()
        .map(PathBuf::from)
        .filter(|path| path.exists())
        .collect::<Vec<_>>();
    for access in [ExecutionAccess::WorkspaceWrite, ExecutionAccess::ReadOnly] {
        let grant =
            ExecutionGrant::new(&workspace, access, read.clone(), ExecutionNetwork::Off).unwrap();
        let mut command = Command::new("/bin/sh");
        command.args(["-c", "(printf changed > escape) 2>/dev/null; (printf created > new) 2>/dev/null; cat escape 2>/dev/null; true"])
            .current_dir(&workspace).env_clear().stdin(Stdio::null())
            .stdout(Stdio::piped()).stderr(Stdio::piped());
        let mut tree = ProcessTree::spawn_granted(&mut command, &grant, None).unwrap();
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
        assert_eq!(std::fs::read_to_string(&sentinel).unwrap(), "original");
        let output = output.unwrap();
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
