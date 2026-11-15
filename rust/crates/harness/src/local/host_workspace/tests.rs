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
            &self.harness.verifier,
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
async fn native_root_capture_and_teardown_use_the_existing_checkout() -> Result<()> {
    let fixture = Fixture::open().await?;
    let mounts = fixture
        .harness
        .workspace()
        .ok_or_else(|| Error::NotFound("fixture workspace".into()))?
        .held_task_mount_root()?;
    let workspace = fixture.attach(root(), mounts.path()).await?;
    native_write(
        &workspace.path().join("retained.txt"),
        b"native checkout write",
    )?;
    workspace.capture(OperationId::new()).await?;
    assert_eq!(
        std::fs::read(workspace.path().join("retained.txt")).map_err(storage)?,
        b"native checkout write"
    );
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
async fn read_only_bindings_require_read_but_not_write() -> Result<()> {
    let checkout = tempfile::tempdir().map_err(storage)?;
    let state = tempfile::tempdir().map_err(storage)?;
    std::fs::write(checkout.path().join("source.txt"), b"read-only source").map_err(storage)?;
    let harness = LocalHarness::open(
        LocalHarnessConfig::native_host(checkout.path())
            .workspace_mounting(WorkspaceMounting::Physical)
            .state_directory(state.path().join("runtime")),
    )
    .await?;
    let workspace = harness
        .workspace()
        .ok_or_else(|| Error::NotFound("fixture workspace".into()))?;
    let project_read = harness
        .project
        .capability(VolumeOperation::Read, &mut None)?;
    let project_write = harness
        .project
        .capability(VolumeOperation::Write, &mut None)?;
    let agent_id = crate::AgentId::from_bytes([3; 16]);
    let agent = AgentPath::root(&crate::agent::AgentId::from_bytes([3; 16]));
    let signer = harness
        .root_authority
        .as_ref()
        .ok_or_else(|| Error::NotFound("test signing custody".into()))?;
    let read_only = signer.bootstrap_scope(
        "read-only-project",
        &crate::Capabilities::new(
            harness
                .signed
                .capabilities()
                .iter()
                .filter(|capability| *capability != project_write.as_str()),
        ),
        Some(agent_id),
        &mut None,
    )?;
    let authority = workspace
        .mount_authority
        .as_ref()
        .ok_or(Error::FilesystemAccessConfiguration(
            "fixture mount authority",
        ))?
        .for_scope(&read_only)?;
    let mut bindings = harness.session_bindings().to_vec();
    let primary_id = crate::agent::WorkspaceTable::primary_in(&bindings)
        .ok_or_else(|| Error::NotFound("fixture primary".into()))?
        .id;
    bindings
        .iter_mut()
        .find(|entry| entry.id == primary_id)
        .unwrap()
        .mode = crate::agent::WorkspaceMode::ReadOnly;
    let cancellation = acyclic_fs::CancellationToken::new();
    let work = acyclic_fs::NativeMaterializationWork::new(
        acyclic_fs::WorkBudget::UNBOUNDED,
        &cancellation,
    )
    .map_err(|(cause, work)| Error::NativeConstruction { cause, work })?;
    let view = workspace
        .view()
        .open_related(
            workspace.view().workspace().clone(),
            &mut acyclic_fs::kernel::KernelAccounting::native(&work, &cancellation),
        )
        .await
        .map_err(storage)?;
    let selected = workspace
        .bound_task_view(view, &harness.project, &bindings, &AgentOwner, &authority)
        .await?;
    let host = HostWorkspace::attach(
        Arc::clone(&selected),
        agent.clone(),
        &read_only,
        &harness.verifier,
        selected.held_task_mount_root()?,
    )
    .await?;
    host.verify().await?;
    assert_eq!(
        std::fs::read(host.host_cwd().join("source.txt")).map_err(storage)?,
        b"read-only source"
    );
    // The same scope cannot attach the original writable primary.
    assert!(matches!(
        HostWorkspace::attach(
            Arc::clone(workspace),
            agent.clone(),
            &read_only,
            &harness.verifier,
            workspace.held_task_mount_root()?,
        )
        .await,
        Err(Error::Unauthorized(_))
    ));
    let denied = signer.bootstrap_scope(
        "missing-project-read",
        &crate::Capabilities::new(
            read_only
                .capabilities()
                .iter()
                .filter(|capability| *capability != project_read.as_str()),
        ),
        Some(agent_id),
        &mut None,
    )?;
    assert!(matches!(
        HostWorkspace::attach(
            Arc::clone(&selected),
            agent,
            &denied,
            &harness.verifier,
            selected.held_task_mount_root()?,
        )
        .await,
        Err(Error::Unauthorized(_))
    ));
    Ok(())
}

#[tokio::test]
async fn child_views_select_roles_without_assuming_names_or_volume_classes() -> Result<()> {
    use crate::agent::{WorkspaceRole, WorkspaceTable};
    use crate::conversation::VolumeOwner;

    let checkout = tempfile::tempdir().map_err(storage)?;
    let state = tempfile::tempdir().map_err(storage)?;
    std::fs::write(checkout.path().join("source.txt"), b"parent cut").map_err(storage)?;
    let harness = LocalHarness::open(
        LocalHarnessConfig::native_host(checkout.path())
            .workspace_mounting(WorkspaceMounting::Physical)
            .state_directory(state.path().join("runtime")),
    )
    .await?;
    let workspace = harness
        .workspace()
        .ok_or_else(|| Error::NotFound("fixture workspace".into()))?;
    let parent = HostWorkspace::attach(
        Arc::clone(workspace),
        root(),
        &harness.signed,
        &harness.verifier,
        workspace.held_task_mount_root()?,
    )
    .await?;
    let signer = harness
        .root_authority
        .as_ref()
        .ok_or_else(|| Error::NotFound("fixture signing custody".into()))?;
    let cancellation = acyclic_fs::CancellationToken::new();
    let work = acyclic_fs::NativeMaterializationWork::new(
        acyclic_fs::WorkBudget::UNBOUNDED,
        &cancellation,
    )
    .map_err(|(cause, work)| Error::NativeConstruction { cause, work })?;
    for (index, role) in [(1_u8, WorkspaceRole::User), (2, WorkspaceRole::Home)] {
        let owner = crate::AgentId::from_bytes([index; 16]);
        let path_id = crate::agent::AgentId::from_bytes([index; 16]);
        let child = parent.agent().child(&path_id).map_err(storage)?;
        // The filesystem fork mechanism is independent of the logical class.
        let volume = VolumeRef::new(
            harness.project.provider().clone(),
            format!("selected-child-{index}"),
            VolumeClass::AgentPrivate,
            VolumeOwner::Agent(owner),
            &mut None,
        )?;
        workspace
            .view()
            .fork(
                &volume.storage_name(&mut None)?,
                acyclic_fs::IdempotencyKey::from_bytes([index; 16]),
                acyclic_fs::PublicationPermit::Unrestricted,
                &mut acyclic_fs::kernel::KernelAccounting::native(&work, &cancellation),
            )
            .await
            .map_err(storage)?;
        let mut entries = vec![WorkspaceEntry::initial_volume(
            role,
            "selected desk".into(),
            volume.clone(),
        )];
        if role == WorkspaceRole::User {
            // A Home named project must not replace the selected User slot.
            entries.insert(
                0,
                WorkspaceEntry::initial_volume(
                    WorkspaceRole::Home,
                    "project".into(),
                    harness.scratch.clone(),
                ),
            );
        }
        let read = volume.capability(VolumeOperation::Read, &mut None)?;
        let write = volume.capability(VolumeOperation::Write, &mut None)?;
        let signed = signer.bootstrap_scope(
            "selected-child",
            &crate::Capabilities::new(
                harness
                    .signed
                    .capabilities()
                    .iter()
                    .chain([read.as_str(), write.as_str()]),
            ),
            Some(owner),
            &mut None,
        )?;
        let reference = format!("selected-{index}");
        let opened = parent
            .open_child(&child, &entries, signed.copy(&mut None)?, &reference)
            .await?;
        assert_eq!(
            WorkspaceTable::primary_in(&entries).unwrap().volume()?,
            &volume
        );
        assert_eq!(opened.workspace.mounts.named(None)?.1.view.volume, volume);
        assert_eq!(
            std::fs::read(opened.host_cwd().join("source.txt")).map_err(storage)?,
            b"parent cut"
        );
        native_write(&opened.host_cwd().join("child-note"), b"selected write")?;
        opened.capture(OperationId::new()).await?;
        let head = opened
            .workspace
            .view()
            .workspace()
            .head(&work, &cancellation)
            .await
            .map_err(storage)?;
        assert_eq!(
            head.read("/child-note", None, &work, &cancellation)
                .await
                .map_err(storage)?
                .as_ref(),
            b"selected write"
        );
        assert!(!parent.host_cwd().join("child-note").exists());
        let reopened = parent
            .open_child(&child, &entries, signed.copy(&mut None)?, &reference)
            .await?;
        assert_eq!(reopened.path(), opened.path());
        // Attaching a root uses that same composed source, regardless of class.
        let attached = HostWorkspace::attach(
            Arc::clone(&opened.workspace),
            AgentPath::root(&path_id),
            &signed,
            &harness.verifier,
            opened.workspace.held_task_mount_root()?,
        )
        .await?;
        assert_eq!(attached.path(), opened.path());
        let denied = signer.bootstrap_scope(
            "missing-selected-read",
            &crate::Capabilities::new(
                signed
                    .capabilities()
                    .iter()
                    .filter(|cap| *cap != read.as_str()),
            ),
            Some(owner),
            &mut None,
        )?;
        assert!(matches!(
            HostWorkspace::attach(
                Arc::clone(&opened.workspace),
                AgentPath::root(&path_id),
                &denied,
                &harness.verifier,
                opened.workspace.held_task_mount_root()?,
            )
            .await,
            Err(Error::Unauthorized(_))
        ));
        let physical = opened.path().to_path_buf();
        drop(attached);
        drop(reopened);
        drop(opened);
        workspace.release_physical_bindings(&entries)?;
        assert!(!physical.exists());
        assert!(parent.path().exists());
        let nonprimary = [WorkspaceEntry::initial_volume(
            WorkspaceRole::Private,
            "project".into(),
            volume,
        )];
        assert!(matches!(
            parent
                .open_child(&child, &nonprimary, signed, &reference)
                .await,
            Err(Error::Invalid(_))
        ));
    }
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
