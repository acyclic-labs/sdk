//! Read-only git must not create control-plane publications.
use super::*;
use acyclic_fs::OperationWindowStore as _;

#[cfg(windows)]
mod windows_snapshot;

#[tokio::test]
async fn clean_status_does_not_append_core_state_frames() -> Result<()> {
    status_cost(true).await
}

#[tokio::test]
async fn root_clean_status_does_not_append_core_state_frames() -> Result<()> {
    status_cost(false).await
}

#[tokio::test]
async fn root_inspection_does_not_repair_an_interrupted_repository_record() -> Result<()> {
    interrupted_inspection(false).await
}

#[tokio::test]
async fn child_inspection_does_not_repair_an_interrupted_repository_record() -> Result<()> {
    interrupted_inspection(true).await
}

async fn interrupted_inspection(child: bool) -> Result<()> {
    let case = StatusFixture::open(child).await?;
    let record = case
        .tools
        .workspace
        .store
        .root()
        .join("git-compat-v9")
        .join(format!(
            "{}.json",
            hex::encode(case.tools.workspace.view().workspace().id().into_bytes())
        ));
    let valid = std::fs::read(&record).map_err(storage)?;
    let mut repaired = Vec::new();
    for (index, command) in [
        "status",
        "git diff",
        "git log",
        "git show HEAD:status-baseline",
    ]
    .into_iter()
    .enumerate()
    {
        std::fs::write(record.with_extension("previous.json"), &valid).map_err(storage)?;
        std::fs::write(&record, b"interrupted current record").map_err(storage)?;
        let before = durable_files(case.state.path())?;
        case.command(u32::try_from(index).map_err(storage)? + 200, command)
            .await?;
        if durable_files(case.state.path())? != before {
            repaired.push(command);
        }
    }
    eprintln!("INSPECTION_REPAIR child={child} repaired={repaired:?}");
    assert!(
        repaired.is_empty(),
        "read-only adapter entry points must not repair durable records"
    );
    Ok(())
}

struct StatusFixture {
    harness: LocalHarness,
    tools: Arc<CheckoutTools>,
    task: Arc<crate::runtime::TaskContext>,
    _directory: tempfile::TempDir,
    state: tempfile::TempDir,
}

impl StatusFixture {
    async fn open(child: bool) -> Result<Self> {
        let (harness, context, directory, state) = fixture(None).await?;
        let workspace = harness
            .workspace()
            .ok_or_else(|| Error::NotFound("checkout".into()))?;
        let task = Arc::new(context.task().clone());
        let root = Arc::new(
            CheckoutTools::new(
                Arc::clone(workspace),
                &harness.project,
                Arc::new(acyclic_stream::SystemUnixMillisClock),
                &harness.signed,
                Arc::clone(&harness.task_files),
            )
            .await?,
        );
        let selecting =
            ToolContext::new(Arc::clone(&task), OperationId::new(), "status-select", None)?;
        let tools = root.select(&selecting).await?.unwrap_or(root);
        let mut case = Self {
            harness,
            tools,
            task,
            _directory: directory,
            state,
        };
        // Commit the shared preimage before creating a sparse child view.
        // The measured child then inherits that immutable committed cut.
        case.seed().await?;
        if child {
            case.task = child_task(
                &case.harness,
                context
                    .task()
                    .durable_task_id()
                    .ok_or_else(|| Error::Invalid("fixture task is not durable".into()))?,
            )
            .await?;
            let selecting = ToolContext::new(
                Arc::clone(&case.task),
                OperationId::new(),
                "status-child-select",
                None,
            )?;
            case.tools =
                case.tools.select(&selecting).await?.ok_or_else(|| {
                    Error::Invalid("child status did not select a child view".into())
                })?;
            // The sparse view inherits file content, while its own Git
            // repository starts unborn. Commit that inherited content too.
            case.command(110, "git commit -m status-child-baseline")
                .await?;
        }
        Ok(case)
    }

    async fn seed(&self) -> Result<()> {
        use crate::conversation::ContentPublisher as _;
        super::super::super::publisher::CheckoutPublisher::new(
            Arc::clone(&self.tools),
            Arc::clone(&self.harness.files),
            &self.harness.project,
            &self.harness.verifier,
            self.harness.limits.file_bytes,
        )
        .stage_once(
            OperationId::new(),
            "status-baseline",
            b"committed bytes\n",
            "text/plain",
            "status-baseline",
            Default::default(),
        )?
        .await?;
        self.command(100, "git commit -m status-baseline").await?;
        Ok(())
    }

    async fn command(&self, index: u32, command: &str) -> Result<ToolResult> {
        let operation_id = OperationId::new();
        let call_id = format!("status-{index}");
        let invocation = crate::tool::ToolInvocation {
            operation_id,
            call_id: call_id.clone(),
            name: "fs".into(),
            arguments: serde_json::json!({"command": command}),
        };
        let context = ToolContext::new(Arc::clone(&self.task), operation_id, &call_id, None)?;
        let result = self.tools.execute(invocation, Some(context)).await?;
        assert!(result.value.get("error").is_none(), "{result:?}");
        assert!(
            result.value.get("result").is_some(),
            "command did not execute: {result:?}"
        );
        Ok(result)
    }

    async fn inspect_native_change(&self, first: &ToolResult) -> Result<()> {
        std::fs::write(
            self.tools.workspace.path.join("status-baseline"),
            b"changed native bytes\n",
        )
        .map_err(storage)?;
        let before = durable_files(self.state.path())?;
        let changed = self.command(5, "status").await?;
        assert_diff(&self.command(102, "git diff").await?, 1)?;
        let shown = decode(&self.command(103, "git show HEAD:status-baseline").await?)?;
        let acyclic_fs::GitCommandOutput::Filesystem(acyclic_fs::GitFilesystemResult::Data {
            kind,
            value,
        }) = shown
        else {
            return Err(Error::Invalid("expected committed file result".into()));
        };
        assert_eq!(kind, "file");
        assert_eq!(
            value["text"], "committed bytes\n",
            "show must retain committed bytes even when HEAD names the live lazy cut"
        );
        let (
            acyclic_fs::GitCommandOutput::Status(first),
            acyclic_fs::GitCommandOutput::Status(changed),
        ) = (decode(first)?, decode(&changed)?)
        else {
            return Err(Error::Invalid("expected typed status results".into()));
        };
        assert_eq!(
            first.dirty,
            acyclic_fs::GitDirtyState::Clean,
            "unchanged physical content must compare clean against committed HEAD"
        );
        assert_eq!(
            changed.dirty,
            acyclic_fs::GitDirtyState::Dirty,
            "native changes must compare dirty against committed HEAD"
        );
        if changed.branch.starts_with("agents/") {
            assert_diff(
                &self
                    .command(104, &format!("git diff {}", changed.branch))
                    .await?,
                0,
            )?;
        }
        assert_ne!(
            first.workspace, changed.workspace,
            "read-only status must inspect native changes"
        );
        assert_eq!(
            durable_files(self.state.path())?,
            before,
            "dirty status and diff must leave every durable state file unchanged"
        );
        Ok(())
    }
}

fn decode(result: &ToolResult) -> Result<acyclic_fs::GitCommandOutput> {
    serde_json::from_value(
        result
            .value
            .get("result")
            .cloned()
            .ok_or_else(|| Error::Invalid("missing Git result".into()))?,
    )
    .map_err(storage)
}

async fn status_cost(child: bool) -> Result<()> {
    let case = StatusFixture::open(child).await?;
    let (schedule, mut events) = acyclic_native_runtime::testing::Schedule::new();
    let driver = tokio::spawn(async move {
        while let Some(boundary) = events.recv().await {
            boundary.release();
        }
    });
    let result = schedule.scope(status_reads(&case, child)).await;
    schedule.drained().await;
    driver.abort();
    let _ = driver.await;
    let writes = schedule.durable_writes();
    eprintln!(
        "STATUS_DURABLE_WRITES child={child} operations={}",
        writes.len()
    );
    assert!(
        writes.is_empty(),
        "read-only Git must perform no durable writes: {writes:?}"
    );
    case.harness.disconnect_unfinished().await?;
    result
}

async fn status_reads(case: &StatusFixture, child: bool) -> Result<()> {
    let workspace = &case.tools.workspace;
    let status = async |index| case.command(index, "status").await;
    let log = workspace
        .store
        .root()
        .join("core-state-log-v1")
        .join(format!("{}.json", "0".repeat(32)));
    let windows = workspace.filesystem.operation_window_store();
    let workspace_id = workspace.view().workspace().id();
    let before_window = windows.load(workspace_id).await.map_err(storage)?;
    let before_revision = before_window.as_ref().map_or(0, |window| window.revision);
    let before = read_log(&log)?;
    let before_files = durable_files(case.state.path())?;
    let first = status(0).await?;
    let acyclic_fs::GitCommandOutput::Status(initial) = decode(&first)? else {
        return Err(Error::Invalid("expected initial status result".into()));
    };
    assert!(
        initial.head.is_some(),
        "the fixture must establish committed HEAD"
    );
    assert_diff(&case.command(101, "git diff").await?, 0)?;
    let before_frames = before.iter().filter(|byte| **byte == b'\n').count();
    let started = std::time::Instant::now();
    for index in 1..=4 {
        status(index).await?;
    }
    let elapsed = started.elapsed();
    assert_eq!(
        durable_files(case.state.path())?,
        before_files,
        "cold and warm status must leave every durable state file unchanged"
    );
    case.inspect_native_change(&first).await?;
    let after_window = windows.load(workspace_id).await.map_err(storage)?;
    let after_revision = after_window.as_ref().map_or(0, |window| window.revision);
    eprintln!(
        "STATUS_WINDOW child={child} calls=4 before_revision={before_revision} after_revision={after_revision}"
    );
    let after = read_log(&log)?;
    let after_frames = after.iter().filter(|byte| **byte == b'\n').count();
    eprintln!(
        "STATUS_COST child={child} calls=4 before_frames={before_frames} after_frames={after_frames} before_bytes={} after_bytes={} elapsed_us={}",
        before.len(),
        after.len(),
        elapsed.as_micros()
    );
    assert!(
        after == before,
        "clean read-only git must leave the core-state journal unchanged"
    );
    assert_eq!(
        after_revision, before_revision,
        "clean read-only git must not publish operation-window leases"
    );
    Ok(())
}

fn assert_diff(result: &ToolResult, expected: u64) -> Result<()> {
    let decoded: acyclic_fs::GitCommandOutput = serde_json::from_value(
        result
            .value
            .get("result")
            .cloned()
            .ok_or_else(|| Error::Invalid("missing diff result".into()))?,
    )
    .map_err(storage)?;
    let acyclic_fs::GitCommandOutput::Filesystem(acyclic_fs::GitFilesystemResult::Data {
        kind,
        value,
    }) = decoded
    else {
        return Err(Error::Invalid("expected typed diff result".into()));
    };
    assert_eq!(kind, "diff");
    eprintln!(
        "NATIVE_DIFF expected={expected} observed={}",
        value["fileChanges"]
    );
    assert_eq!(
        value["fileChanges"].as_u64(),
        Some(expected),
        "native diff must compare the immutable HEAD with the live physical view"
    );
    Ok(())
}

fn read_log(path: &std::path::Path) -> Result<Vec<u8>> {
    match std::fs::read(path) {
        Ok(bytes) => Ok(bytes),
        Err(cause) if cause.kind() == std::io::ErrorKind::NotFound => Ok(Vec::new()),
        Err(cause) => Err(storage(cause)),
    }
}

#[derive(Debug, Eq, PartialEq)]
enum DurableEntry {
    File(Vec<u8>),
    Link(std::path::PathBuf),
}

fn durable_files(
    root: &std::path::Path,
) -> Result<std::collections::BTreeMap<std::path::PathBuf, DurableEntry>> {
    let mut files = std::collections::BTreeMap::new();
    let mut pending = vec![root.to_owned()];
    while let Some(directory) = pending.pop() {
        for entry in std::fs::read_dir(directory).map_err(storage)? {
            let entry = entry.map_err(storage)?;
            let kind = entry.file_type().map_err(storage)?;
            if kind.is_dir() {
                pending.push(entry.path());
            } else if kind.is_symlink() {
                files.insert(
                    entry.path(),
                    DurableEntry::Link(std::fs::read_link(entry.path()).map_err(storage)?),
                );
            } else if kind.is_file() {
                // An empty live-owner lock file has no content to read;
                // Windows rejects speculative reads past its locked EOF.
                let bytes = if entry.metadata().map_err(storage)?.len() == 0 {
                    Vec::new()
                } else {
                    snapshot_bytes(&entry.path()).map_err(|cause| {
                        Error::Storage(format!("snapshot read {:?}: {cause}", entry.path()))
                    })?
                };
                files.insert(entry.path(), DurableEntry::File(bytes));
            }
        }
    }
    Ok(files)
}

#[cfg(windows)]
fn snapshot_bytes(path: &std::path::Path) -> std::io::Result<Vec<u8>> {
    match std::fs::read(path) {
        Err(error) if error.raw_os_error() == Some(33) => windows_snapshot::read(path),
        result => result,
    }
}

#[cfg(not(windows))]
fn snapshot_bytes(path: &std::path::Path) -> std::io::Result<Vec<u8>> {
    std::fs::read(path)
}

async fn child_task(
    harness: &LocalHarness,
    parent: crate::TaskId,
) -> Result<Arc<crate::runtime::TaskContext>> {
    let operation = OperationId::new();
    let Admission::Accepted(crate::runtime::RuntimeTask::Durable { task_id, .. }) = harness
        .submit_to(operation, Some(parent), "child status")
        .await?
    else {
        return Err(Error::Invalid("child status task is not durable".into()));
    };
    let worker = crate::distributed::Worker {
        id: "status-cost".into(),
        available: Default::default(),
        labels: Default::default(),
    };
    let crate::distributed::WorkPull::Claimed(lease) = harness
        .runtime
        .task_host()
        .pull_work(&worker, Some(operation), &mut None)
        .await?
    else {
        return Err(Error::Invalid("child status task was not claimed".into()));
    };
    harness
        .runtime
        .task_host()
        .start_task(&lease, &mut None)
        .await?;
    Ok(Arc::new(
        harness
            .runtime
            .task_context(
                task_id,
                crate::scheduler::LeaseFence::from(&lease.reservation),
            )
            .await?,
    ))
}

#[tokio::test]
async fn switched_inspection_reads_the_selected_physical_checkout() -> Result<()> {
    let case = StatusFixture::open(true).await?;
    case.command(300, "git switch -c inspection-selected")
        .await?;
    let selected = serialized(&case.tools.state, async |mut state| {
        case.tools
            .selected_workspace(
                &mut state,
                case.task
                    .owned_mailbox
                    .as_ref()
                    .ok_or_else(|| Error::Invalid("fixture owner absent".into()))?
                    .as_ref(),
            )
            .await
    })
    .await?;
    assert_ne!(selected.path, case.tools.workspace.path);
    write_native_inspection_file(
        &selected.path.join("status-baseline"),
        "selected native bytes\n",
    )?;
    let before = durable_files(case.state.path())?;
    let acyclic_fs::GitCommandOutput::Status(status) = decode(&case.command(301, "status").await?)?
    else {
        return Err(Error::Invalid("expected selected status".into()));
    };
    eprintln!(
        "SELECTED_INSPECTION dirty={:?} selected={:?} original={:?}",
        status.dirty, selected.path, case.tools.workspace.path
    );
    assert_eq!(
        status.dirty,
        acyclic_fs::GitDirtyState::Dirty,
        "inspection must read the selected physical checkout after a branch switch"
    );
    assert_diff(&case.command(302, "git diff").await?, 1)?;
    assert_eq!(
        durable_files(case.state.path())?,
        before,
        "selected checkout inspection must not publish a repair or capture"
    );
    case.harness.disconnect_unfinished().await?;
    Ok(())
}

#[tokio::test]
async fn bound_volume_inspection_does_not_publish_native_changes() -> Result<()> {
    let case = StatusFixture::open(false).await?;
    let mounts = &case.tools.workspace.mounts;
    let source = mounts
        .named(Some("scratch"))?
        .1
        .view
        .source
        .as_ref()
        .ok_or_else(|| Error::Invalid("fixture scratch binding absent".into()))?;
    workspace::volumes::native_scope(mounts, async |claims| {
        write_native_inspection_file(&source.destination.join("inspected"), "baseline\n")?;
        Ok(((), claims))
    })
    .await?;
    let cancellation = acyclic_fs::CancellationToken::new();
    let previous = source
        .workspace
        .workspace()
        .head(acyclic_fs::WorkBudget::UNBOUNDED, &cancellation)
        .await
        .map_err(storage)?
        .value
        .id();
    write_native_inspection_file(
        &source.destination.join("inspected"),
        "unpublished native bytes\n",
    )?;
    let before = durable_files(case.state.path())?;
    let acyclic_fs::GitCommandOutput::Status(status) =
        decode(&case.command(310, "git --volume scratch status").await?)?
    else {
        return Err(Error::Invalid("expected bound-volume status".into()));
    };
    let after = source
        .workspace
        .workspace()
        .head(acyclic_fs::WorkBudget::UNBOUNDED, &cancellation)
        .await
        .map_err(storage)?
        .value
        .id();
    let unchanged = durable_files(case.state.path())? == before;
    eprintln!(
        "BOUND_INSPECTION head_unchanged={} durable_unchanged={unchanged}",
        previous == after
    );
    assert!(
        unchanged && previous == after,
        "read-only bound volume inspection must not capture or publish native changes"
    );
    assert_ne!(
        status.workspace,
        acyclic_fs::GitTreeRef::exact(source.workspace.workspace().id(), previous),
        "bound-volume status must still inspect unpublished native content"
    );
    case.harness.disconnect_unfinished().await?;
    Ok(())
}

fn write_native_inspection_file(path: &std::path::Path, bytes: &str) -> Result<()> {
    let output = acyclic_native_runtime::process_output(
        std::process::Command::new(std::env::current_exe().map_err(storage)?)
            .args([
                "--exact",
                "local::checkout_tools::output::tests::status_cost::external_inspection_writer",
                "--nocapture",
            ])
            .env("ACYCLIC_INSPECTION_WRITE_PATH", path)
            .env("ACYCLIC_INSPECTION_WRITE_BYTES", bytes),
        std::time::Duration::from_secs(5),
        4096,
    )
    .map_err(storage)?;
    if !output.status.success() {
        return Err(Error::Storage(format!(
            "inspection client failed: {output:?}"
        )));
    }
    Ok(())
}

#[test]
fn external_inspection_writer() -> Result<()> {
    let Some(path) = crate::test_support::environment("ACYCLIC_INSPECTION_WRITE_PATH") else {
        return Ok(());
    };
    let bytes = crate::test_support::environment("ACYCLIC_INSPECTION_WRITE_BYTES")
        .ok_or_else(|| Error::Invalid("inspection client bytes absent".into()))?;
    std::fs::write(path, bytes.to_string_lossy().as_bytes()).map_err(storage)
}
