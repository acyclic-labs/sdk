use super::*;

pub(in crate::local) fn native_write(path: &Path, bytes: &[u8]) -> Result<()> {
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
    // The harness owns native projections below checkout and uses state on drop.
    harness: LocalHarness,
    checkout: tempfile::TempDir,
    state: tempfile::TempDir,
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

    async fn attach(&self, agent: AgentPath, mounts: &Path) -> Result<HostWorkspace> {
        let workspace = self
            .harness
            .workspace()
            .ok_or_else(|| Error::NotFound("fixture workspace".into()))?;
        HostWorkspace::attach(
            Arc::clone(workspace),
            agent,
            &self.harness.signed,
            acyclic_fs::hold_root(mounts).map_err(storage)?,
        )
        .await
    }
}

fn root() -> AgentPath {
    AgentPath::root(&crate::agent::AgentId::from_bytes(
        *uuid::Uuid::new_v4().as_bytes(),
    ))
}

#[tokio::test]
async fn native_workspace_teardown_removes_storage_after_retiring_retained_views() -> Result<()> {
    let fixture = Fixture::open().await?;
    let mounts = fixture
        .harness
        .workspace()
        .ok_or_else(|| Error::NotFound("fixture workspace".into()))?
        .held_task_mount_root()?;
    let workspace = fixture.attach(root(), mounts.path()).await?;
    native_write(
        &workspace.path().join(".volumes/scratch/retained.txt"),
        b"native private volume",
    )?;
    workspace.capture(OperationId::new()).await?;
    drop(workspace);
    drop(mounts);
    let checkout = fixture.checkout.path().to_path_buf();
    let state = fixture.state.path().to_path_buf();
    drop(fixture);
    assert!(
        !checkout.exists(),
        "checkout cleanup traversed a live native view"
    );
    assert!(!state.exists(), "native state outlived fixture shutdown");
    Ok(())
}

#[tokio::test]
async fn attachment_rejects_an_arbitrary_in_checkout_mount_root() -> Result<()> {
    let fixture = Fixture::open().await?;
    let mounts = fixture.checkout.path().join("authored-directory");
    std::fs::create_dir(&mounts).map_err(storage)?;
    assert!(matches!(
        fixture.attach(root(), &mounts).await,
        Err(Error::Invalid(_))
    ));
    assert!(!fixture.state.path().join("host").exists());
    let external = tempfile::tempdir().map_err(storage)?;
    assert!(matches!(
        fixture.attach(root(), external.path()).await,
        Err(Error::Invalid(_))
    ));
    // A spawned agent's view is mounted under its parent, never attached.
    let mounts = fixture
        .harness
        .workspace()
        .ok_or_else(|| Error::NotFound("fixture workspace".into()))?
        .held_task_mount_root()?;
    let child = root()
        .child(&crate::agent::AgentId::from_bytes([7; 16]))
        .map_err(storage)?;
    assert!(fixture.attach(child, mounts.path()).await.is_err());
    assert!(fixture.harness.agent.is_none());
    Ok(())
}
