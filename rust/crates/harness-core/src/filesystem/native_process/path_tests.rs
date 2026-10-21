use super::*;
use crate::filesystem::NativeViewOptions;

#[test]
fn approved_native_paths_compare_the_same_filesystem_spelling() {
    let directory = tempfile::tempdir().unwrap();
    let root = directory.path().join("view");
    let cwd = root.join("child");
    std::fs::create_dir_all(&cwd).unwrap();
    let view = NativeViewManifest {
        options: NativeViewOptions {
            root,
            maximum_volumes: 1,
            work_per_volume: acyclic_fs::WorkBudget::UNBOUNDED,
        },
        volumes: Vec::new(),
    };
    let mut request = NativeProcessRequest {
        executable: std::env::current_exe().unwrap(),
        argv: Vec::new(),
        cwd,
        environment: Default::default(),
        timeout_ms: 1,
        control_timeout_ms: 1,
        cancellation_poll_ms: 1,
        maximum_output_bytes: 1,
        maximum_result_bytes: 2052,
        mcp_stdio: None,
        standard_input: None,
        view: view.clone(),
    };
    request.validate_native_paths(&view).unwrap();
    request.cwd = view.options.root.clone();
    request.validate_native_paths(&view).unwrap();

    let mut changed = view.clone();
    changed.options.maximum_volumes += 1;
    assert!(matches!(
        request.validate_native_paths(&changed),
        Err(Error::Conflict(_))
    ));
    request.cwd = directory.path().to_path_buf();
    assert!(matches!(
        request.validate_native_paths(&view),
        Err(Error::Invalid(_))
    ));
    request.cwd = view.options.root.join("child/../..");
    assert!(matches!(
        request.validate_native_paths(&view),
        Err(Error::Invalid(_))
    ));
}

#[test]
fn approved_native_paths_reject_a_link_outside_the_view() {
    let directory = tempfile::tempdir().unwrap();
    let root = directory.path().join("view");
    let outside = directory.path().join("outside");
    std::fs::create_dir_all(&root).unwrap();
    std::fs::create_dir_all(&outside).unwrap();
    let link = root.join("escape");
    #[cfg(unix)]
    std::os::unix::fs::symlink(&outside, &link).unwrap();
    #[cfg(windows)]
    match std::os::windows::fs::symlink_dir(&outside, &link) {
        Ok(()) => {}
        Err(error) if error.kind() == std::io::ErrorKind::PermissionDenied => return,
        Err(error) => panic!("failed to create test symlink: {error}"),
    }
    let view = NativeViewManifest {
        options: NativeViewOptions {
            root,
            maximum_volumes: 1,
            work_per_volume: acyclic_fs::WorkBudget::UNBOUNDED,
        },
        volumes: Vec::new(),
    };
    let request = NativeProcessRequest {
        executable: std::env::current_exe().unwrap(),
        argv: Vec::new(),
        cwd: link,
        environment: Default::default(),
        timeout_ms: 1,
        control_timeout_ms: 1,
        cancellation_poll_ms: 1,
        maximum_output_bytes: 1,
        maximum_result_bytes: 2052,
        mcp_stdio: None,
        standard_input: None,
        view: view.clone(),
    };
    assert!(matches!(
        request.validate_native_paths(&view),
        Err(Error::Conflict(_))
    ));
}
