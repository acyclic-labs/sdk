use super::*;

async fn open_child(fixture: &Fixture) -> ControlPlane {
    acyclic_native_runtime::initialize_native_io_services().unwrap();
    acyclic_fs::initialize_native_mount_services().unwrap();
    let mut control = ControlPlane::open(fixture.data.clone()).await.unwrap();
    control
        .session_start(json!({"session_id":"git-review","cwd":fixture.root}))
        .await
        .unwrap();
    control
        .user_prompt(json!({"session_id":"git-review","turn_id":"root-turn"}))
        .unwrap();
    // Give branch forks an authored common base rather than unresolved source entries.
    for workspace in control.roots.values() {
        use acyclic_fs::facade::FsWork as _;
        workspace
            .exactify(
                "/",
                PublicationPermit::Unrestricted,
                &mut WorkBudget::UNBOUNDED.accounting(&CancellationToken::new()),
            )
            .await
            .unwrap();
    }
    control.pre_tool(json!({"session_id":"git-review","turn_id":"root-turn", "tool_use_id":"spawn-child", "tool_name":"spawn_agent","tool_input":{}})).await.unwrap();
    control
        .subagent_start(
            json!({"session_id":"git-review","turn_id":"child-turn","agent_id":"child"}),
        )
        .await
        .unwrap();
    control
}

async fn child_git(control: &mut ControlPlane, args: &[&str]) -> Result<Value, String> {
    let route = control.state.routes["child"].clone();
    control
        .git_tool(
            "child",
            &route,
            WorkspaceRootId::from_bytes(route.root_id),
            args.iter().map(|s| (*s).to_owned()).collect(),
        )
        .await
}

async fn write_child(control: &ControlPlane, name: &str, text: &'static str) {
    write_child_bytes(control, name, text.as_bytes()).await;
}

async fn write_child_bytes(control: &ControlPlane, name: &str, bytes: &'static [u8]) {
    let path = control.state.routes["child"]
        .active_path()
        .unwrap()
        .join(name);
    tokio::task::spawn_blocking(move || -> std::io::Result<()> {
        #[cfg(windows)]
        {
            write_host_file(&path, bytes)
        }
        #[cfg(not(windows))]
        fs::write(path, bytes)
    })
    .await
    .unwrap()
    .unwrap();
}

#[cfg(windows)]
fn write_host_file(path: &std::path::Path, bytes: &[u8]) -> std::io::Result<()> {
    use std::os::windows::process::CommandExt as _;
    // ProjFS does not report its provider process's own file I/O.
    // Invoke this test binary directly so Windows passes every path literally.
    let staging = tempfile::tempdir()?;
    let source = staging.path().join("host-write.bin");
    fs::write(&source, bytes)?;
    let output = std::process::Command::new(std::env::current_exe()?)
        .args([
            "--exact",
            "tests::git::external_host_writer_child",
            "--ignored",
        ])
        .env("ACYCLIC_COPY_SOURCE", &source)
        .env("ACYCLIC_COPY_DESTINATION", path)
        .creation_flags(0x0800_0000)
        .output()?;
    if !output.status.success() {
        return Err(std::io::Error::other(format!(
            "host write failed: {}: {}",
            output.status,
            String::from_utf8_lossy(&output.stderr)
        )));
    }
    Ok(())
}

#[cfg(windows)]
#[test]
#[ignore = "invoked in a separate host process by write_host_file"]
fn external_host_writer_child() {
    let source = std::env::var_os("ACYCLIC_COPY_SOURCE").expect("source path");
    let destination = std::env::var_os("ACYCLIC_COPY_DESTINATION").expect("destination path");
    fs::copy(source, destination).unwrap();
}

#[cfg(windows)]
#[test]
fn external_host_write_preserves_shell_characters_and_binary_bytes() {
    let directory = tempfile::Builder::new()
        .prefix("host-write-&%PATH%!^()")
        .tempdir()
        .unwrap();
    let path = directory.path().join("result & %PATH%!^().bin");
    let bytes = b"\xff\0\r\n&%PATH%!^()";
    write_host_file(&path, bytes).unwrap();
    assert_eq!(fs::read(path).unwrap(), bytes);
}

async fn write_child_text(control: &ControlPlane, text: &'static str) {
    use acyclic_fs::facade::FsWork as _;
    let route = &control.state.routes["child"];
    let root = WorkspaceRootId::from_bytes(route.root_id);
    let workspace = control.lazy_workspace_root(route, root).await.unwrap();
    let cancellation = CancellationToken::new();
    workspace
        .write(
            "/conflict.txt",
            bytes::Bytes::from_static(text.as_bytes()).into(),
            IdempotencyKey::new(),
            PublicationPermit::Unrestricted,
            &mut WorkBudget::UNBOUNDED.accounting(&cancellation),
        )
        .await
        .unwrap();
    control.mounts["child"].advance_to_head().await.unwrap();
}

#[test]
fn git_merge_error_exposes_conflict_markers_in_the_agent_mount() {
    run_large_stack("git-conflict-visibility", || async {
        let fixture = Fixture::new();
        fs::write(fixture.root.join("conflict.txt"), "base\n").unwrap();
        let mut control = open_child(&fixture).await;
        child_git(&mut control, &["commit", "-m", "base"])
            .await
            .unwrap();
        child_git(&mut control, &["checkout", "-b", "review-main"])
            .await
            .unwrap();
        child_git(&mut control, &["checkout", "-b", "review-other"])
            .await
            .unwrap();
        write_child_text(&control, "theirs\n").await;
        child_git(&mut control, &["commit", "-m", "theirs"])
            .await
            .unwrap();
        child_git(&mut control, &["checkout", "review-main"])
            .await
            .unwrap();
        write_child_text(&control, "ours\n").await;
        child_git(&mut control, &["commit", "-m", "ours"])
            .await
            .unwrap();
        let error = child_git(&mut control, &["merge", "review-other"])
            .await
            .unwrap_err();
        assert!(
            error.contains("Git join has unresolved conflicts"),
            "{error}"
        );
        let path = control.state.routes["child"]
            .active_path()
            .unwrap()
            .join("conflict.txt");
        let markers = tokio::task::spawn_blocking(move || fs::read_to_string(path))
            .await
            .unwrap()
            .unwrap();
        assert!(
            acyclic_fs::text_merge::has_conflict_markers(&markers),
            "{markers}"
        );
        assert!(
            markers.contains("ours") && markers.contains("theirs"),
            "{markers}"
        );
        control.shutdown().await.unwrap();
    });
}

async fn merge_child_attributes(attributes: &'static [u8]) {
    let fixture = Fixture::new();
    fs::write(fixture.root.join("base.txt"), "base\n").unwrap();
    fs::write(fixture.root.join(".gitattributes"), "*.txt merge=text\n").unwrap();
    let mut control = open_child(&fixture).await;
    write_child_bytes(&control, ".gitattributes", attributes).await;
    write_child(&control, ".gitignore", "ignored/\n").await;
    write_child(&control, "result.txt", "child result\n").await;
    control
        .agent_merge(json!({"agent":"child","_caller_turn_id":"root-turn"}))
        .await
        .unwrap();
    assert_eq!(
        fs::read_to_string(fixture.root.join("result.txt")).unwrap(),
        "child result\n"
    );
    assert_eq!(
        fs::read_to_string(fixture.root.join("base.txt")).unwrap(),
        "base\n"
    );
    assert_eq!(
        fs::read(fixture.root.join(".gitattributes")).unwrap(),
        attributes
    );
    control.shutdown().await.unwrap();
}

#[test]
fn child_merge_ignores_unregistered_child_attributes() {
    run_large_stack("child-merge-driver", || {
        merge_child_attributes(b"*.lock merge=ours\n")
    });
}

#[test]
fn child_merge_ignores_non_utf8_child_attributes() {
    run_large_stack("child-merge-non-utf8", || merge_child_attributes(b"\xff"));
}
