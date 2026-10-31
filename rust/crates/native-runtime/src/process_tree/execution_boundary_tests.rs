use super::*;
use crate::execution::{ExecutionAccess, ExecutionGrant, ExecutionNetwork, ExecutionRefusal};
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
            .validate_launch(command.get_current_dir(), command.get_env_clear())
            .unwrap();
        let boundary = match grant.prepare_launch(Some(&cwd), None) {
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
        let error = grant.prepare_launch(Some(&cwd), None).unwrap_err();
        assert!(error.get_ref().unwrap().is::<ExecutionRefusal>());
        std::fs::remove_file(&selected).unwrap();
        std::fs::remove_dir(&held).unwrap();
    }
}
