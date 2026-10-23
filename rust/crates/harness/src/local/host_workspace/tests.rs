use super::*;
use crate::runtime::{RuntimeTask, TaskStateProvider};

fn native_write(path: &Path, bytes: &[u8]) -> Result<()> {
    #[cfg(not(windows))]
    {
        std::fs::write(path, bytes).map_err(storage)
    }
    #[cfg(windows)]
    {
        use std::os::windows::process::CommandExt as _;
        // ProjFS deliberately excludes provider-process writes from callbacks.
        // Exercise an actual native host process instead of that bypass.
        let staging = tempfile::tempdir().map_err(storage)?;
        let input = staging.path().join("authored.bin");
        std::fs::write(&input, bytes).map_err(storage)?;
        let output = std::process::Command::new("cmd.exe")
            .args(["/d", "/v:off", "/c"])
            .raw_arg(r#"copy /b /y "%ACYCLIC_COPY_SOURCE%" "%ACYCLIC_COPY_DESTINATION%""#)
            .env("ACYCLIC_COPY_SOURCE", input)
            .env("ACYCLIC_COPY_DESTINATION", path)
            .creation_flags(0x0800_0000)
            .output()
            .map_err(storage)?;
        if !output.status.success() {
            return Err(Error::Conflict("native fixture write was denied".into()));
        }
        Ok(())
    }
}

struct Fixture {
    checkout: tempfile::TempDir,
    state: tempfile::TempDir,
    harness: LocalHarness,
}

impl Fixture {
    async fn open() -> Result<Self> {
        let checkout = tempfile::tempdir().map_err(storage)?;
        let state = tempfile::tempdir().map_err(storage)?;
        std::fs::write(checkout.path().join("source.txt"), b"root source").map_err(storage)?;
        let config = LocalHarnessConfig::native_host(checkout.path());
        let harness =
            LocalHarness::open(config.state_directory(state.path().join("runtime"))).await?;
        Ok(Self {
            checkout,
            state,
            harness,
        })
    }

    async fn task(&self, parent: Option<TaskId>) -> Result<TaskContext> {
        let operation = OperationId::new();
        let task = if let Some(parent) = parent {
            let parent = self.harness.bind_host_task(parent).await?;
            self.harness
                .admit_host_child(&parent, &operation.to_string())
                .await?
        } else {
            self.harness
                .admit_host_task(operation, "codex", &operation.to_string())
                .await?
        };
        self.harness.bind_host_task(task).await
    }

    async fn unallocated_child(&self, parent: &TaskContext) -> Result<TaskContext> {
        let definition = self
            .harness
            .runtime
            .harness()
            .task::<serde_json::Value, serde_json::Value>(super::super::host_task::TASK)?;
        let operation = OperationId::new();
        let admission = self
            .harness
            .runtime
            .harness()
            .admit(
                operation,
                &definition,
                serde_json::json!({"host":"codex","session":operation.to_string()}),
                Some(parent.owned_task_binding(None)?.0),
            )
            .await?;
        let Admission::Accepted(RuntimeTask::Durable { task_id, .. }) = admission else {
            return Err(Error::Conflict(
                "fixture unallocated child was not admitted".into(),
            ));
        };
        self.harness.bind_host_task(task_id).await
    }

    async fn attach(&self, context: &TaskContext, mounts: &Path) -> Result<HostWorkspace> {
        let workspace = self
            .harness
            .workspace()
            .ok_or_else(|| Error::NotFound("fixture workspace".into()))?;
        HostWorkspace::attach(
            Arc::clone(workspace),
            context,
            acyclic_fs::hold_root(mounts).map_err(storage)?,
        )
        .await
    }
}

#[tokio::test]
async fn native_writes_fork_and_reopen_without_execution() -> Result<()> {
    let fixture = Fixture::open().await?;
    let parent = fixture.task(None).await?;
    let root = acyclic_fs::hold_root(fixture.checkout.path()).map_err(storage)?;
    let mounts = root
        .create_directories(Path::new(HOST_VIEW_ROOT))
        .map_err(storage)?;
    let workspace = fixture.attach(&parent, mounts.path()).await?;
    std::fs::write(workspace.path().join("parent.txt"), b"host authored").map_err(storage)?;
    std::fs::write(mounts.path().join("reserved.txt"), b"not project history").map_err(storage)?;
    workspace.capture(&parent, OperationId::new()).await?;
    let unallocated = fixture.unallocated_child(&parent).await?;
    assert!(matches!(
        workspace.reopen(&parent, &unallocated).await,
        Err(Error::Unauthorized(_))
    ));
    assert!(matches!(
        workspace.fork(&parent, &unallocated).await,
        Err(Error::Unauthorized(_))
    ));
    let child = fixture.task(Some(workspace.task())).await?;
    let fork = workspace.fork(&parent, &child).await?;
    assert!(fork.path().starts_with(mounts.path()));
    assert_eq!(
        std::fs::read(fork.path().join("parent.txt")).map_err(storage)?,
        b"host authored"
    );
    assert!(
        !fork
            .path()
            .join(HOST_VIEW_ROOT)
            .join("reserved.txt")
            .exists()
    );
    native_write(&fork.path().join("child.txt"), b"child only")?;
    fork.capture(&child, OperationId::new()).await?;
    assert!(!fixture.checkout.path().join("child.txt").exists());
    let reopened = workspace.reopen(&parent, &child).await?;
    assert_eq!(
        std::fs::read(reopened.path().join("child.txt")).map_err(storage)?,
        b"child only"
    );
    let grandchild = fixture.task(Some(fork.task())).await?;
    let nested = fork.fork(&child, &grandchild).await?;
    assert!(nested.path().starts_with(mounts.path()));
    assert_eq!(
        std::fs::read(nested.path().join("child.txt")).map_err(storage)?,
        b"child only"
    );
    workspace
        .fs(
            &parent,
            OperationId::new(),
            &format!("merge agents/{}", fork.task()),
        )
        .await?;
    assert_eq!(
        std::fs::read(workspace.path().join("child.txt")).map_err(storage)?,
        b"child only"
    );
    assert!(fixture.harness.agent.is_none());
    Ok(())
}

#[tokio::test]
async fn task_grants_parentage_and_cancellation_fence_every_boundary() -> Result<()> {
    let fixture = Fixture::open().await?;
    let parent = fixture.task(None).await?;
    let foreign = fixture.task(None).await?;
    let root = acyclic_fs::hold_root(fixture.checkout.path()).map_err(storage)?;
    let mounts = root
        .create_directories(Path::new(HOST_VIEW_ROOT))
        .map_err(storage)?;
    let workspace = fixture.attach(&parent, mounts.path()).await?;
    let unrelated = fixture
        .task(Some(foreign.owned_task_binding(None)?.0))
        .await?;
    assert!(workspace.fork(&parent, &unrelated).await.is_err());
    assert!(workspace.reopen(&parent, &unrelated).await.is_err());
    assert!(
        workspace
            .fs(&foreign, OperationId::new(), "log")
            .await
            .is_err()
    );
    assert!(
        workspace
            .capture(&foreign, OperationId::new())
            .await
            .is_err()
    );
    let child = fixture.task(Some(workspace.task())).await?;
    let narrowed_parent = parent.scoped(
        Capabilities::new(std::iter::empty::<&str>()),
        parent.scope().limits(),
    )?;
    assert!(
        workspace
            .capture(&narrowed_parent, OperationId::new())
            .await
            .is_err()
    );
    assert!(workspace.fork(&narrowed_parent, &child).await.is_err());
    assert!(workspace.reopen(&narrowed_parent, &child).await.is_err());
    assert!(
        workspace
            .fs(&narrowed_parent, OperationId::new(), "log")
            .await
            .is_err()
    );
    assert!(
        fixture
            .attach(&narrowed_parent, mounts.path())
            .await
            .is_err()
    );
    let narrowed = child.scoped(
        Capabilities::new(std::iter::empty::<&str>()),
        child.scope().limits(),
    )?;
    assert!(workspace.fork(&parent, &narrowed).await.is_err());
    assert!(fixture.attach(&narrowed, mounts.path()).await.is_err());
    let fork = workspace.fork(&parent, &child).await?;
    fixture
        .harness
        .runtime
        .task_host()
        .cancel(fork.task())
        .await?;
    assert!(fork.capture(&child, OperationId::new()).await.is_err());
    assert!(fork.fs(&child, OperationId::new(), "log").await.is_err());
    assert!(workspace.fork(&parent, &child).await.is_err());
    assert!(workspace.reopen(&parent, &child).await.is_err());
    fixture
        .harness
        .runtime
        .task_host()
        .cancel(workspace.task())
        .await?;
    assert!(
        workspace
            .capture(&parent, OperationId::new())
            .await
            .is_err()
    );
    assert!(fixture.attach(&parent, mounts.path()).await.is_err());
    assert!(fixture.harness.agent.is_none());
    Ok(())
}

#[tokio::test]
async fn attachment_rejects_an_arbitrary_in_checkout_mount_root() -> Result<()> {
    let fixture = Fixture::open().await?;
    let parent = fixture.task(None).await?;
    let mounts = fixture.checkout.path().join("authored-directory");
    std::fs::create_dir(&mounts).map_err(storage)?;
    assert!(matches!(
        fixture.attach(&parent, &mounts).await,
        Err(Error::Invalid(_))
    ));
    assert!(!fixture.state.path().join("host").exists());
    let external = tempfile::tempdir().map_err(storage)?;
    assert!(matches!(
        fixture.attach(&parent, external.path()).await,
        Err(Error::Invalid(_))
    ));
    assert!(fixture.harness.agent.is_none());
    Ok(())
}

#[tokio::test]
async fn native_private_volume_writes_are_published_and_retained() -> Result<()> {
    let fixture = Fixture::open().await?;
    let parent = fixture.task(None).await?;
    let mounts = fixture
        .harness
        .workspace()
        .ok_or_else(|| Error::NotFound("fixture workspace".into()))?
        .held_task_mount_root()?;
    let workspace = fixture.attach(&parent, mounts.path()).await?;
    let child = fixture.task(Some(workspace.task())).await?;
    let fork = workspace.fork(&parent, &child).await?;
    let relative = Path::new(".volumes/scratch/private.txt");
    native_write(&fork.path().join(relative), b"native private")?;
    fork.capture(&child, OperationId::new()).await?;
    let source = fork
        .workspace
        .mounts
        .named(Some("scratch"))?
        .1
        .view
        .source
        .as_ref()
        .ok_or_else(|| Error::NotFound("fixture scratch projection".into()))?;
    let cancellation = acyclic_fs::CancellationToken::new();
    let head = source
        .workspace
        .workspace()
        .head(acyclic_fs::WorkBudget::UNBOUNDED, &cancellation)
        .await
        .map_err(storage)?
        .value;
    assert_eq!(
        head.read(
            "/private.txt",
            64,
            acyclic_fs::WorkBudget::UNBOUNDED,
            &cancellation
        )
        .await
        .map_err(storage)?
        .value
        .as_ref(),
        b"native private"
    );
    let task = fork.task();
    let allocation = Arc::downgrade(
        fork._allocation
            .as_ref()
            .ok_or_else(|| Error::NotFound("fixture child allocation".into()))?,
    );
    drop(fork);
    assert!(
        allocation.upgrade().is_none(),
        "the parent cache must not retain a closed native projection"
    );
    let reopened = workspace.reopen(&parent, &child).await?;
    assert_eq!(
        std::fs::read(reopened.path().join(relative)).map_err(storage)?,
        b"native private"
    );
    workspace
        .fs(&parent, OperationId::new(), &format!("merge agents/{task}"))
        .await?;
    assert!(!workspace.path().join(relative).exists());
    Ok(())
}

#[cfg(unix)]
#[tokio::test]
async fn native_private_binding_root_refuses_a_symlink_escape() -> Result<()> {
    let fixture = Fixture::open().await?;
    let parent = fixture.task(None).await?;
    let mounts = fixture
        .harness
        .workspace()
        .ok_or_else(|| Error::NotFound("fixture workspace".into()))?
        .held_task_mount_root()?;
    let workspace = fixture.attach(&parent, mounts.path()).await?;
    let outside = tempfile::tempdir().map_err(storage)?;
    std::os::unix::fs::symlink(outside.path(), mounts.path().join(".bindings")).map_err(storage)?;
    let child = fixture.task(Some(workspace.task())).await?;
    assert!(workspace.fork(&parent, &child).await.is_err());
    assert_eq!(
        std::fs::read_dir(outside.path()).map_err(storage)?.count(),
        0
    );
    Ok(())
}

#[tokio::test]
async fn native_private_callbacks_and_publication_reject_a_cancelled_owner() -> Result<()> {
    let fixture = Fixture::open().await?;
    let parent = fixture.task(None).await?;
    let mounts = fixture
        .harness
        .workspace()
        .ok_or_else(|| Error::NotFound("fixture workspace".into()))?
        .held_task_mount_root()?;
    let workspace = fixture.attach(&parent, mounts.path()).await?;
    let child = fixture.task(Some(workspace.task())).await?;
    let fork = workspace.fork(&parent, &child).await?;
    native_write(
        &fork.path().join(".volumes/scratch/pending.txt"),
        b"pending",
    )?;
    let source = fork
        .workspace
        .mounts
        .named(Some("scratch"))?
        .1
        .view
        .source
        .as_ref()
        .ok_or_else(|| Error::NotFound("fixture scratch projection".into()))?;
    fixture
        .harness
        .runtime
        .task_host()
        .cancel(fork.task())
        .await?;
    // Call the filesystem API directly: a preflight-only host check would
    // allow this pending write to publish after task ownership was revoked.
    let native = source.native().await;
    assert!(
        native
            .as_ref()
            .ok_or_else(|| Error::NotFound("native projection".into()))?
            .sync()
            .await
            .is_err()
    );
    assert!(native_write(&fork.path().join(".volumes/scratch/denied.txt"), b"denied").is_err());
    Ok(())
}

#[tokio::test]
async fn native_children_use_shared_allocations_and_never_dispatch_an_agent() -> Result<()> {
    let fixture = Fixture::open().await?;
    assert!(fixture.harness.agent.is_none());
    assert!(fixture.harness.settings.is_none());
    assert!(fixture.harness.tools().definitions(None)?.is_empty());
    assert!(fixture.harness.command_model(None).await.is_err());
    assert!(fixture.harness.stock_commands(None).await.is_err());
    let operation = OperationId::new();
    let task = fixture
        .harness
        .admit_host_task(operation, "codex", "native-session")
        .await?;
    assert_eq!(
        fixture
            .harness
            .admit_host_task(operation, "codex", "native-session")
            .await?,
        task
    );
    assert!(
        fixture
            .harness
            .admit_host_task(operation, "pi", "changed-session")
            .await
            .is_err()
    );
    let parent = fixture.harness.bind_host_task(task).await?;
    for grant in [
        "operation:declare",
        "operation:observe",
        "operation:cancel",
        "operation:wake",
    ] {
        assert!(parent.scope().grants().contains(grant));
    }
    assert!(!parent.scope().grants().contains("model:generate"));
    assert!(
        !parent
            .scope()
            .grants()
            .iter()
            .any(|grant| grant.starts_with("tool:call:"))
    );
    let root = acyclic_fs::hold_root(fixture.checkout.path()).map_err(storage)?;
    let mounts = root
        .create_directories(Path::new(HOST_VIEW_ROOT))
        .map_err(storage)?;
    let workspace = fixture.attach(&parent, mounts.path()).await?;
    std::fs::write(workspace.path().join("parent.txt"), b"native parent").map_err(storage)?;
    workspace.capture(&parent, OperationId::new()).await?;
    let child_task = fixture
        .harness
        .admit_host_child(&parent, "native-child")
        .await?;
    assert_eq!(
        fixture
            .harness
            .admit_host_child(&parent, "native-child")
            .await?,
        child_task
    );
    let child = fixture.harness.bind_host_task(child_task).await?;
    let fork = workspace.fork(&parent, &child).await?;
    assert!(fork.path().starts_with(mounts.path()));
    assert_eq!(
        std::fs::read(fork.path().join("parent.txt")).map_err(storage)?,
        b"native parent"
    );
    assert!(workspace.verify(&child).await.is_err());
    native_write(&fork.path().join("child.txt"), b"native child")?;
    fork.capture(&child, OperationId::new()).await?;
    workspace
        .fs(&parent, OperationId::new(), "merge agents/native-child")
        .await?;
    assert_eq!(
        std::fs::read(workspace.path().join("child.txt")).map_err(storage)?,
        b"native child"
    );
    let narrowed = parent.scoped(
        Capabilities::new(std::iter::empty::<&str>()),
        parent.scope().limits(),
    )?;
    assert!(
        fixture
            .harness
            .admit_host_child(&narrowed, "forbidden")
            .await
            .is_err()
    );
    fixture
        .harness
        .finish_host_task(&child, crate::Outcome::Succeeded(serde_json::Value::Null))
        .await?;
    assert!(fixture.harness.bind_host_task(child_task).await.is_err());
    assert!(fork.capture(&child, OperationId::new()).await.is_err());
    assert!(fixture.harness.agent.is_none());
    let machine = super::super::host_task::NativeHostTask::new();
    assert!(
        crate::workflow::ResumableMachine::transition(
            machine.as_ref(),
            &serde_json::Value::Null,
            &serde_json::Value::Null
        )
        .is_err()
    );
    Ok(())
}
