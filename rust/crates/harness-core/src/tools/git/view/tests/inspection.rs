//! Read-only commands must also leave cold and interrupted records untouched.
use super::*;

fn files(root: &Path) -> TestResult<BTreeMap<std::path::PathBuf, Vec<u8>>> {
    let mut result = BTreeMap::new();
    let mut pending = vec![root.to_owned()];
    while let Some(directory) = pending.pop() {
        for entry in std::fs::read_dir(directory)? {
            let entry = entry?;
            if entry.file_type()?.is_dir() {
                pending.push(entry.path());
            } else {
                result.insert(entry.path(), std::fs::read(entry.path())?);
            }
        }
    }
    Ok(result)
}

#[tokio::test]
async fn cold_status_and_log_do_not_create_compatibility_records() -> TestResult {
    let (_directory, fs, store, workspace) = fixture().await?;
    let before = files(store.root())?;
    for command in ["status", "log"] {
        execute(&fs, &store, (&workspace).into(), workspace.id(), &[command]).await?;
    }
    assert_eq!(
        files(store.root())?,
        before,
        "cold read-only commands must not create durable state files"
    );
    Ok(())
}

#[tokio::test]
async fn inspection_reads_previous_record_without_repairing_current() -> TestResult {
    let (_directory, fs, store, workspace) = fixture().await?;
    workspace.write_text("/file", "committed\n").await?;
    execute(
        &fs,
        &store,
        (&workspace).into(),
        workspace.id(),
        &["commit", "-m", "base"],
    )
    .await?;
    let record = store
        .root()
        .join("git-compat-v9")
        .join(format!("{}.json", hex::encode(workspace.id().into_bytes())));
    std::fs::copy(&record, record.with_extension("previous.json"))?;
    std::fs::write(&record, b"interrupted current record")?;
    let before = files(store.root())?;
    let result = execute(
        &fs,
        &store,
        (&workspace).into(),
        workspace.id(),
        &["status"],
    )
    .await?;
    assert!(matches!(result.command, Some(GitCommandOutput::Status(_))));
    assert_eq!(
        files(store.root())?,
        before,
        "inspection may read a valid previous record but must not publish its repair"
    );
    Ok(())
}

#[tokio::test]
async fn committed_status_diff_log_and_show_do_not_publish() -> TestResult {
    let (_directory, fs, store, workspace) = fixture().await?;
    workspace.write_text("/file", "committed\n").await?;
    execute(
        &fs,
        &store,
        (&workspace).into(),
        workspace.id(),
        &["commit", "-m", "base"],
    )
    .await?;
    workspace.write_text("/file", "changed\n").await?;
    let before = files(store.root())?;
    for command in [&["status"][..], &["diff"], &["log"], &["show", "HEAD:file"]] {
        let result = execute(&fs, &store, (&workspace).into(), workspace.id(), command).await?;
        assert!(
            result.command.is_some(),
            "read-only command produced no result"
        );
    }
    assert_eq!(
        files(store.root())?,
        before,
        "all read-only entry points must leave durable records unchanged"
    );
    Ok(())
}

#[tokio::test]
async fn native_diff_keeps_the_committed_side_when_references_match() -> TestResult {
    let (directory, fs, store, workspace) = fixture().await?;
    workspace.write_text("/file", "committed\n").await?;
    execute(
        &fs,
        &store,
        (&workspace).into(),
        workspace.id(),
        &["commit", "-m", "base"],
    )
    .await?;
    let physical = directory.path().join("checkout");
    std::fs::create_dir(&physical)?;
    std::fs::write(physical.join("file"), "physical edit\n")?;
    let cancellation = CancellationToken::new();
    let work = super::super::super::readonly::work(&cancellation)?;
    let options = acyclic_fs::CaptureOptions {
        expected_root_identity: work.root_identity(&physical)?,
        source_root: physical.clone(),
        maximum_paths: 16,
        maximum_extent_spans: 16,
    };
    let policy = acyclic_fs::CapturePolicy::default();
    let before = files(store.root())?;
    let generation = workspace.head(&work, &cancellation).await?.id();
    for args in [vec!["diff".into()], vec!["diff".into(), "--cached".into()]] {
        let result = super::super::super::inspect_with_guard(
            Workspace {
                filesystem: &fs,
                store: &store,
                view: (&workspace).into(),
                root: &physical,
                repository: workspace.id(),
                lease: None,
                checkout: None,
                reserved: &[],
                author: "test",
                authored_at_seconds: 1,
            },
            &args,
            acyclic_fs::NativeGitView {
                options: &options,
                policy: &policy,
                materialized: false,
            },
            || async { Ok(()) },
        )
        .await?;
        let Some(GitCommandOutput::Filesystem(GitFilesystemResult::Data { kind, value })) =
            result.command
        else {
            return Err("missing native diff result".into());
        };
        assert_eq!(kind, "diff");
        assert_eq!(value["fileChanges"], 1, "{args:?}: {value}");
        assert_ne!(value["from"], value["to"]);
    }
    assert_eq!(workspace.head(&work, &cancellation).await?.id(), generation);
    assert_eq!(files(store.root())?, before);
    Ok(())
}
