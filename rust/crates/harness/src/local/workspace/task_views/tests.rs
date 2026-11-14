use super::super::owner::{AgentOwner, ViewOwner};
use super::*;
use tokio::sync::oneshot;

#[tokio::test]
async fn physical_binding_reuses_its_preparation_and_rejects_another_scratch() -> Result<()> {
    let directory = tempfile::tempdir().map_err(storage)?;
    let state = tempfile::tempdir().map_err(storage)?;
    std::fs::create_dir(directory.path().join("selected")).map_err(storage)?;
    let harness = LocalHarness::open(
        crate::LocalHarnessConfig::native_host(directory.path())
            .workspace_mounting(WorkspaceMounting::Physical)
            .state_directory(state.path().join("runtime")),
    )
    .await?;
    let shared = VolumeRef::new(
        harness.shared.provider().clone(),
        "other-shared".into(),
        VolumeClass::SessionShared,
        harness.shared.owner().clone(),
        &mut None,
    )?;
    harness.files.create_volume(&shared).await?;
    let selected_agent = crate::AgentId::from_bytes([1; 16]);
    let private_home = VolumeRef::new(
        harness.scratch.provider().clone(),
        "selected-home".into(),
        harness.scratch.class(),
        crate::conversation::VolumeOwner::Agent(selected_agent),
        &mut None,
    )?;
    harness.files.create_volume(&private_home).await?;
    let private_ui = VolumeRef::new(
        harness.scratch.provider().clone(),
        "selected-private".into(),
        harness.scratch.class(),
        crate::conversation::VolumeOwner::Agent(selected_agent),
        &mut None,
    )?;
    harness.files.create_volume(&private_ui).await?;
    let read = shared.capability(crate::conversation::VolumeOperation::Read, &mut None)?;
    let write = shared.capability(crate::conversation::VolumeOperation::Write, &mut None)?;
    let home_read =
        private_home.capability(crate::conversation::VolumeOperation::Read, &mut None)?;
    let home_write =
        private_home.capability(crate::conversation::VolumeOperation::Write, &mut None)?;
    let private_read =
        private_ui.capability(crate::conversation::VolumeOperation::Read, &mut None)?;
    let private_write =
        private_ui.capability(crate::conversation::VolumeOperation::Write, &mut None)?;
    let grants = crate::Capabilities::new(harness.signed.capabilities().iter().chain([
        read.as_str(),
        write.as_str(),
        home_read.as_str(),
        home_write.as_str(),
        private_read.as_str(),
        private_write.as_str(),
    ]));
    let signed = harness
        .root_authority
        .as_ref()
        .ok_or_else(|| Error::NotFound("test signing custody".into()))?
        .bootstrap_scope("selected-volumes", &grants, Some(selected_agent), &mut None)?;
    let mut session_bindings = harness.session_bindings().to_vec();
    session_bindings[0].name = "repository".into();
    session_bindings[0].root = "selected".into();
    session_bindings[1].source = crate::agent::WorkspaceSource::Volume(private_home.clone());
    for (role, name) in [
        (crate::agent::WorkspaceRole::Private, "display"),
        (crate::agent::WorkspaceRole::Internal, "interaction"),
    ] {
        session_bindings.push(crate::agent::WorkspaceEntry::initial_volume(
            role,
            name.into(),
            if role == crate::agent::WorkspaceRole::Private {
                private_ui.clone()
            } else {
                harness.scratch.clone()
            },
        ));
    }
    session_bindings.push(crate::agent::WorkspaceEntry::initial_volume(
        crate::agent::WorkspaceRole::Output,
        "results".into(),
        private_ui.clone(),
    ));
    session_bindings
        .iter_mut()
        .find(|entry| entry.name == "display")
        .unwrap()
        .root = "ui".into();
    session_bindings
        .iter_mut()
        .find(|entry| entry.name == "results")
        .unwrap()
        .root = "ui".into();
    let mut archive = crate::agent::WorkspaceEntry::initial_volume(
        crate::agent::WorkspaceRole::User,
        "zarchive".into(),
        private_ui.clone(),
    );
    archive.root = "reports".into();
    session_bindings.push(archive);
    session_bindings
        .iter_mut()
        .find(|entry| entry.name == "shared")
        .ok_or_else(|| Error::NotFound("shared binding".into()))?
        .source = crate::agent::WorkspaceSource::Volume(shared);
    let mut changed_bindings = session_bindings.clone();
    changed_bindings[1].source = crate::agent::WorkspaceSource::Volume(harness.project.clone());
    let mut changed_mode = session_bindings.clone();
    changed_mode[1].mode = match changed_mode[1].mode {
        crate::agent::WorkspaceMode::ReadOnly => crate::agent::WorkspaceMode::ReadWrite,
        crate::agent::WorkspaceMode::ReadWrite => crate::agent::WorkspaceMode::ReadOnly,
    };
    let workspace = harness
        .workspace()
        .ok_or_else(|| Error::NotFound("workspace".into()))?;
    let cancellation = CancellationToken::new();
    let work = NativeMaterializationWork::new(WorkBudget::UNBOUNDED, &cancellation)
        .map_err(|(cause, work)| Error::NativeConstruction { cause, work })?;
    let view = || async {
        workspace
            .view()
            .open_related(
                workspace.view().workspace().clone(),
                &mut KernelAccounting::NativeOriginal {
                    work: &work,
                    cancellation: &cancellation,
                },
            )
            .await
            .map_err(storage)
    };
    let authority = crate::filesystem::NativeMountAuthority::admitted(
        harness.verifier.clone(),
        signed.copy(&mut None)?,
    );
    let (entered, observed) = oneshot::channel();
    let (release, released) = oneshot::channel();
    let owner = PausedOwner {
        original: AgentOwner,
        entered: crate::sync::SyncMutex::new(Some(entered)),
        release: AsyncMutex::new(Some(released)),
    };
    let preparing = workspace.bound_task_view(
        view().await?,
        &harness.project,
        &session_bindings,
        &owner,
        &authority,
    );
    tokio::pin!(preparing);
    tokio::select! {
        outcome = &mut preparing => {
            outcome?;
            return Err(Error::Conflict("physical binding completed before its owner released".into()));
        }
        observed = observed => {
            observed.map_err(|_| Error::Conflict("physical binding owner disappeared".into()))?;
        }
    }
    let second = workspace
        .bound_task_view(
            view().await?,
            &harness.project,
            &session_bindings,
            &owner.original,
            &authority,
        )
        .await?;
    let home = second.mounts.named(Some(crate::filesystem::HOME_VOLUME))?.1;
    assert_eq!(home.view.volume, private_home);
    assert!(home.native_root.override_path().is_none());
    for name in ["interaction", "project", "unknown"] {
        assert!(
            second.mounts.named(Some(name)).is_err(),
            "unexpected auxiliary {name}"
        );
    }
    assert_eq!(second.mounts.named(None)?.1.view.volume, harness.project);
    assert_eq!(
        second.mounts.named(Some("repository"))?.1.view.volume,
        harness.project
    );
    let private = second.mounts.named(Some("display"))?.1;
    assert_eq!(private.view.volume, private_ui);
    assert!(private.native_root.override_path().is_none());
    private.check(acyclic_fs::access::Action::Write, "ui/bound-note", 0)?;
    let output = second.mounts.named(Some("results"))?.1;
    assert!(output.mode.read_only());
    output.check(acyclic_fs::access::Action::Read, "ui/bound-note", 0)?;
    assert!(
        output
            .check(acyclic_fs::access::Action::Write, "ui/bound-note", 0)
            .is_err()
    );
    assert_ne!(
        private.view.source.as_ref().unwrap().destination,
        output.view.source.as_ref().unwrap().destination,
        "different binding modes shared a native projection",
    );
    let archive = second.mounts.named(Some("zarchive"))?.1;
    assert_ne!(
        private.view.source.as_ref().unwrap().destination,
        archive.view.source.as_ref().unwrap().destination,
        "different roots shared a native projection"
    );
    let logical = workspace.context_task_view(
        view().await?,
        &harness.project,
        &session_bindings,
        &authority,
    )?;
    assert_eq!(
        logical
            .mounts
            .named(Some(crate::filesystem::HOME_VOLUME))?
            .1
            .view
            .volume,
        private_home
    );
    for name in ["interaction", "project", "unknown"] {
        assert!(
            logical.mounts.named(Some(name)).is_err(),
            "unexpected auxiliary {name}"
        );
    }
    assert_eq!(logical.mounts.named(None)?.1.view.volume, harness.project);
    assert_eq!(
        logical.mounts.named(Some("repository"))?.1.view.volume,
        harness.project
    );
    assert_eq!(
        logical.mounts.named(Some("display"))?.1.view.volume,
        private_ui
    );
    assert!(logical.mounts.named(Some("results"))?.1.mode.read_only());
    assert!(matches!(
        workspace.context_task_view(view().await?, &private_home, &session_bindings, &authority),
        Err(Error::Unauthorized(_)),
    ));
    let remounted = second.mounts_at(&second.path).await?;
    for entry in &session_bindings {
        for table in [&second.mounts, &logical.mounts, &remounted] {
            let selected = table.by_entry(entry.id);
            if entry.role == crate::agent::WorkspaceRole::Internal {
                assert!(selected.is_none(), "internal slot must not be exposed");
            } else {
                let selected = selected.expect("selected stable workspace slot");
                assert_eq!(selected.view.name, entry.name);
                assert_eq!(&selected.view.volume, entry.volume()?);
                assert_eq!(selected.view.root, entry.root);
                assert_eq!(super::super::mounts::mode(selected), entry.mode);
            }
        }
    }
    assert_eq!(
        remounted.named(Some("repository"))?.1.view.volume,
        harness.project
    );
    assert!(remounted.named(Some("project")).is_err());
    assert!(remounted.named(Some("results"))?.1.mode.read_only());
    assert_eq!(remounted.named(Some("results"))?.1.view.root, "ui");
    assert_eq!(
        remounted.native_root_for(remounted.named(None)?.1),
        Some(second.path.join("selected").as_path()),
    );
    assert!(
        remounted.native_read_roots.is_empty(),
        "a scoped view must not add opaque read roots outside its subtree"
    );
    {
        let host = crate::local::host_workspace::HostWorkspace::attach(
            Arc::clone(&second),
            crate::agent::AgentPath::root(&crate::agent::AgentId::from_bytes([1; 16])),
            &signed,
            &harness.verifier,
            second.held_task_mount_root()?,
        )
        .await?;
        assert_eq!(host.path(), second.path.join("selected"));
        assert_eq!(
            Some(host.path()),
            second.mounts.native_root_for_entry(session_bindings[0].id)
        );
        assert_ne!(host.path(), second.path);
        crate::local::host_workspace::tests::native_write(
            &host.host_cwd().join("host-note"),
            b"selected host write",
        )?;
        host.capture(OperationId::new()).await?;
        assert_eq!(
            std::fs::read(second.path.join("selected/host-note")).map_err(storage)?,
            b"selected host write"
        );
        assert!(!second.path.join("host-note").exists());
    }
    let mut read_only_primary = session_bindings.clone();
    read_only_primary[0].mode = crate::agent::WorkspaceMode::ReadOnly;
    let read_only = workspace.context_task_view(
        view().await?,
        &harness.project,
        &read_only_primary,
        &authority,
    )?;
    let primary = read_only.mounts.named(None)?.1;
    assert!(primary.mode.read_only());
    primary.check(acyclic_fs::access::Action::Read, "selected/file", 0)?;
    assert!(
        primary
            .check(acyclic_fs::access::Action::Write, "selected/file", 0)
            .is_err()
    );
    let options = read_only
        .mount_options(read_only.view().workspace())
        .await?;
    let access = options
        .require_access_view()
        .map_err(storage)?
        .current()
        .map_err(storage)?;
    let now = harness.clock.now_unix_millis() / 1000;
    assert!(
        access
            .decide(acyclic_fs::access::Action::Read, "selected/file", now)
            .allowed
    );
    assert!(
        !access
            .decide(acyclic_fs::access::Action::Write, "selected/file", now)
            .allowed
    );
    assert!(
        !access
            .decide(acyclic_fs::access::Action::Read, "file", now)
            .allowed,
        "the authenticated source view must not admit a peer of the selected root"
    );
    {
        let selected = &second;
        let path = "bound-note";
        let tools = Arc::new(
            crate::local::checkout_tools::CheckoutTools::new(
                Arc::clone(selected),
                &harness.project,
                Arc::clone(&harness.clock),
                &signed,
                &harness.verifier,
                None,
            )
            .await?,
        );
        let publisher = crate::local::checkout_tools::publisher::CheckoutPublisher::new(
            tools,
            Arc::clone(&harness.files),
            &harness.project,
            &harness.verifier,
            harness.limits.file_bytes,
        );
        let scope = crate::runtime::RuntimeScope::new(
            crate::Capabilities::new(signed.capabilities().iter()),
            Default::default(),
        )?;
        let capture = crate::local::checkout_tools::capture::configuration(selected);
        let commands = super::super::git::Commands {
            workspace: selected,
            signed: &signed,
            verifier: &harness.verifier,
            clock: harness.clock.as_ref(),
            capture_options: &capture.0,
            capture_policy: &capture.1,
        };
        for command in [
            "status",
            "show HEAD:outside-root",
            "--volume display status",
        ] {
            assert!(
                matches!(
                    commands.fs_arguments(command, &scope),
                    Err(Error::Unauthorized(_))
                ),
                "whole-volume version-control access escaped the selected root: {command}"
            );
        }
        commands.fs_arguments("--volume home status", &scope)?;
        let (file, _) = crate::conversation::ContentPublisher::stage_once(
            &publisher,
            OperationId::new(),
            &format!("volume:///home/{path}"),
            b"selected private home",
            "text/plain",
            path,
            crate::conversation::ContentAccess::default(),
        )?
        .await?;
        assert_eq!(file.volume(), &private_home);
        let reference = crate::filesystem::workspace_ref(
            private_home.provider().clone(),
            &private_home.storage_name(&mut None)?,
        )?;
        assert_eq!(
            harness
                .files
                .read(&reference, None, &format!("/{path}"), 64, None)
                .await?
                .as_ref(),
            b"selected private home"
        );
        let (file, _) = crate::conversation::ContentPublisher::stage_once(
            &publisher,
            OperationId::new(),
            "volume:///display/bound-note",
            b"model-visible private state",
            "text/plain",
            path,
            crate::conversation::ContentAccess::default(),
        )?
        .await?;
        assert_eq!(file.volume(), &private_ui);
        assert_eq!(file.path(), "ui/bound-note");
        let reference = crate::filesystem::workspace_ref(
            private_ui.provider().clone(),
            &private_ui.storage_name(&mut None)?,
        )?;
        assert_eq!(
            harness
                .files
                .read(&reference, None, "/ui/bound-note", 64, None)
                .await?
                .as_ref(),
            b"model-visible private state",
        );
        let (file, _) = crate::conversation::ContentPublisher::stage_once(
            &publisher,
            OperationId::new(),
            "volume:///repository/project-note",
            b"selected project subtree",
            "text/plain",
            "project-note",
            crate::conversation::ContentAccess::default(),
        )?
        .await?;
        assert_eq!(file.path(), "selected/project-note");
        assert_eq!(
            std::fs::read(directory.path().join("selected/project-note")).map_err(storage)?,
            b"selected project subtree"
        );
        assert!(!directory.path().join("project-note").exists());
        let archive_operation = OperationId::new();
        let (file, _) = crate::conversation::ContentPublisher::stage_once(
            &publisher,
            archive_operation,
            "volume:///zarchive/bound-note",
            b"separate source root",
            "text/plain",
            path,
            crate::conversation::ContentAccess::default(),
        )?
        .await?;
        assert_eq!(file.path(), "reports/bound-note");
        assert_eq!(
            harness
                .files
                .read(&reference, None, "/reports/bound-note", 64, None)
                .await?
                .as_ref(),
            b"separate source root"
        );
        let (observed, _) = crate::conversation::ContentPublisher::observe_staged_once(
            &publisher,
            archive_operation,
            None,
            None,
            crate::conversation::ContentAccess::default(),
        )?
        .await?;
        assert_eq!(
            observed.as_ref(),
            Some(&file),
            "receipt recovery must find a later source-root alias"
        );
        let (edited, _) = crate::conversation::ContentPublisher::stage_at(
            &publisher,
            OperationId::new(),
            &file,
            b"edited source root",
            None,
            None,
        )?
        .await?;
        assert_eq!(edited.path(), "reports/bound-note");
        assert_eq!(
            harness
                .files
                .read(&reference, None, "/reports/bound-note", 64, None)
                .await?
                .as_ref(),
            b"edited source root"
        );
        let change = |path: &'static str| crate::conversation::ContentChange::CreateAbsent {
            path,
            bytes: b"atomic roots",
            media_type: "text/plain",
            display_name: "batch",
        };
        let (batch, _) = crate::conversation::ContentPublisher::stage_changes(
            &publisher,
            &private_ui,
            OperationId::new(),
            &[change("ui/batch"), change("reports/batch")],
            crate::conversation::ContentAccess::default(),
        )?
        .await?;
        assert_eq!(batch.members.len(), 2);
        for path in ["/ui/batch", "/reports/batch"] {
            assert_eq!(
                harness
                    .files
                    .read(&reference, None, path, 64, None)
                    .await?
                    .as_ref(),
                b"atomic roots"
            );
        }
        assert!(
            crate::conversation::ContentPublisher::stage_changes(
                &publisher,
                &private_ui,
                OperationId::new(),
                &[change("ui/not-committed"), change("outside/not-committed")],
                crate::conversation::ContentAccess::default(),
            )?
            .await
            .is_err()
        );
        assert!(
            harness
                .files
                .read(&reference, None, "/ui/not-committed", 64, None)
                .await
                .is_err(),
            "a peer path refusal must not publish its admitted sibling"
        );
        for name in ["interaction", "project", "unknown", "results"] {
            let path = format!("volume:///{name}/bound-note");
            assert!(
                crate::conversation::ContentPublisher::stage_once(
                    &publisher,
                    OperationId::new(),
                    &path,
                    b"refused",
                    "text/plain",
                    "refused",
                    crate::conversation::ContentAccess::default(),
                )?
                .await
                .is_err(),
                "unexpected writable binding {name}"
            );
        }
    }
    for selection in [&changed_bindings, &changed_mode] {
        assert!(matches!(
            workspace
                .bound_task_view(
                    view().await?,
                    &harness.project,
                    selection,
                    &owner.original,
                    &authority,
                )
                .await,
            Err(Error::Conflict(_)),
        ));
    }
    std::fs::write(second.path.join("shared.txt"), b"shared").map_err(storage)?;
    assert_eq!(
        second.path, workspace.path,
        "primary binding copied the admitted checkout"
    );
    assert_eq!(
        std::fs::read(directory.path().join("shared.txt")).map_err(storage)?,
        b"shared",
        "a bound task's write did not reach the admitted checkout"
    );
    assert!(
        !workspace
            .physical_task_path(workspace.view().workspace().id())
            .exists(),
        "an unchanged primary volume acquired a redundant physical copy"
    );
    release
        .send(())
        .map_err(|_| Error::Conflict("physical binding owner stopped".into()))?;
    let first = preparing.await?;
    assert!(
        Arc::ptr_eq(&first, &second),
        "binding callers received separate primary views"
    );
    assert!(Arc::ptr_eq(
        first.binding.as_ref().expect("first binding"),
        second.binding.as_ref().expect("reused binding"),
    ));
    assert_eq!(
        std::fs::read(first.path.join("shared.txt")).map_err(storage)?,
        b"shared"
    );
    shared_selection(&harness, workspace, &second, &owner.original).await?;
    let original = first
        .binding_owner
        .as_ref()
        .expect("keyed owner")
        .downgrade();
    drop(first);
    let third = workspace
        .bound_task_view(
            view().await?,
            &harness.project,
            &session_bindings,
            &owner.original,
            &authority,
        )
        .await?;
    assert!(
        Arc::ptr_eq(&second, &third),
        "completed binding lost its primary view"
    );
    let mut reordered = session_bindings.clone();
    reordered.reverse();
    let same_selection = workspace
        .bound_task_view(
            view().await?,
            &harness.project,
            &reordered,
            &owner.original,
            &authority,
        )
        .await?;
    assert!(Arc::ptr_eq(&third, &same_selection));
    drop(same_selection);
    let mut changed_shared = session_bindings.clone();
    changed_shared[2].source = crate::agent::WorkspaceSource::Volume(harness.project.clone());
    assert!(matches!(
        workspace
            .bound_task_view(
                view().await?,
                &harness.project,
                &changed_shared,
                &owner.original,
                &authority
            )
            .await,
        Err(Error::Conflict(_)),
    ));
    assert!(Arc::ptr_eq(
        second.binding.as_ref().expect("second binding"),
        third.binding.as_ref().expect("completed binding"),
    ));
    assert!(matches!(
        workspace
            .bound_task_view(
                view().await?,
                &harness.project,
                &changed_bindings,
                &owner.original,
                &authority,
            )
            .await,
        Err(Error::Conflict(_)),
    ));
    drop(second);
    assert!(original.upgrade().is_some());
    drop(third);
    assert!(original.upgrade().is_none());
    workspace
        .bound_task_view(
            view().await?,
            &harness.project,
            &changed_bindings,
            &owner.original,
            &authority,
        )
        .await?;
    Ok(())
}

async fn shared_selection(
    harness: &LocalHarness,
    admitted: &Arc<LocalWorkspace>,
    bound: &Arc<LocalWorkspace>,
    owner: &dyn ViewOwner,
) -> Result<()> {
    use crate::local::checkout_tools::capture;
    // The admitted owner performs this whole-volume branch transition. The
    // scoped task cannot invoke that raw facade, but must observe the same
    // original checkout selection and retain its own path restrictions.
    let (options, policy) = capture::configuration(admitted);
    let commands = super::super::git::Commands {
        workspace: admitted,
        signed: &harness.signed,
        verifier: &harness.verifier,
        clock: &*harness.clock,
        capture_options: &options,
        capture_policy: &policy,
    };
    let scope = RuntimeScope::new(harness.signed.capabilities().clone(), Default::default())?;
    commands
        .invoke(
            crate::tools::parse_fs_command("checkout -b topic")?,
            super::super::git::CommandContext {
                owner,
                operation: OperationId::new(),
                scope: &scope,
                request: None,
            },
            Box::pin(async { Ok(()) }),
        )
        .await?;
    let cancellation = CancellationToken::new();
    let before = admitted
        .view()
        .workspace()
        .head(WorkBudget::UNBOUNDED, &cancellation)
        .await
        .map_err(storage)?
        .value
        .id();
    std::fs::write(admitted.path.join("topic.txt"), b"topic only").map_err(storage)?;
    let mut state = crate::sync::owned::lock_owned(StateArc::clone(&admitted.state)).await;
    assert!(
        bound.state.try_lock().is_err(),
        "two handles can command the same files concurrently"
    );
    let State {
        active, watcher, ..
    } = &mut *state;
    let selected = active.as_ref().ok_or_else(|| {
        Error::Conflict("admitted handle lost the bound handle's checkout selection".into())
    })?;
    assert_ne!(selected.workspace().id(), admitted.view().workspace().id());
    capture::capture(
        (&options, &policy),
        (admitted, selected),
        watcher,
        (OperationId::new(), 0),
        acyclic_fs::PublicationPermit::Unrestricted,
    )
    .await?;
    let selected = selected
        .workspace()
        .head(WorkBudget::UNBOUNDED, &cancellation)
        .await
        .map_err(storage)?
        .value;
    let content = selected
        .read_range("/topic.txt", 0, 10, WorkBudget::UNBOUNDED, &cancellation)
        .await
        .map_err(storage)?
        .value;
    assert_eq!(content.bytes.as_ref(), b"topic only");
    assert_eq!(
        admitted
            .view()
            .workspace()
            .head(WorkBudget::UNBOUNDED, &cancellation)
            .await
            .map_err(storage)?
            .value
            .id(),
        before,
        "capture crossed into the original branch"
    );
    Ok(())
}

// Suspend the original owner's verification, after the real filesystem and
// authority have prepared this binding. Dropping either endpoint ends the wait.
struct PausedOwner {
    original: AgentOwner,
    entered: crate::sync::SyncMutex<Option<oneshot::Sender<()>>>,
    release: AsyncMutex<Option<oneshot::Receiver<()>>>,
}

impl ViewOwner for PausedOwner {
    fn verify(&self) -> BoxProviderFuture<'_, Result<()>> {
        Box::pin(async move {
            self.original.verify().await?;
            let entered = self
                .entered
                .lock()
                .map_err(|_| Error::Conflict("binding observation was poisoned".into()))?
                .take();
            if let Some(entered) = entered {
                entered
                    .send(())
                    .map_err(|_| Error::Conflict("binding observer closed".into()))?;
                let release = self
                    .release
                    .lock()
                    .await
                    .take()
                    .ok_or_else(|| Error::Conflict("binding release was consumed".into()))?;
                release
                    .await
                    .map_err(|_| Error::Conflict("binding release closed".into()))?;
            }
            Ok(())
        })
    }
}

#[tokio::test]
async fn binding_preparation_releases_the_checkout_owner() -> Result<()> {
    acyclic_native_runtime::initialize_native_io_services().map_err(storage)?;
    acyclic_fs::initialize_native_mount_services().map_err(storage)?;
    let directory = tempfile::tempdir().map_err(storage)?;
    let state = tempfile::tempdir().map_err(storage)?;
    let harness = LocalHarness::open(
        crate::LocalHarnessConfig::native_host(directory.path())
            .state_directory(state.path().join("runtime")),
    )
    .await?;
    let session_bindings = harness.session_bindings();
    let workspace = harness
        .workspace()
        .ok_or_else(|| Error::NotFound("binding workspace".into()))?;
    let cancellation = CancellationToken::new();
    let work = NativeMaterializationWork::new(WorkBudget::UNBOUNDED, &cancellation)
        .map_err(|(cause, work)| Error::NativeConstruction { cause, work })?;
    let view = workspace
        .view()
        .open_related(
            workspace.view().workspace().clone(),
            &mut KernelAccounting::NativeOriginal {
                work: &work,
                cancellation: &cancellation,
            },
        )
        .await
        .map_err(storage)?;
    let authority = crate::filesystem::NativeMountAuthority::admitted(
        harness.verifier.clone(),
        harness.signed.copy(&mut None)?,
    );
    let (entered, observed) = oneshot::channel();
    let (release, released) = oneshot::channel();
    let owner = PausedOwner {
        original: AgentOwner,
        entered: crate::sync::SyncMutex::new(Some(entered)),
        release: AsyncMutex::new(Some(released)),
    };
    let checkout_available = {
        let binding = workspace.bound_task_view(
            view,
            &harness.project,
            &session_bindings,
            &owner,
            &authority,
        );
        tokio::pin!(binding);
        tokio::select! {
            result = &mut binding => {
                result?;
                return Err(Error::Conflict("binding completed before its owner released".into()));
            }
            result = observed => {
                result.map_err(|_| Error::Conflict("binding producer disappeared".into()))?;
            }
        }
        let checkout_available = workspace.state.try_lock().is_ok();
        // Register the real selection while its original producer is paused.
        // Its successful workspace can retire before this observer is polled.
        let key = *blake3::hash(harness.project.storage_name(&mut None)?.as_bytes()).as_bytes();
        let (observer, duplicate) = bindings::Bindings::select(
            &workspace.bindings,
            key,
            bindings::auxiliary_key(&session_bindings)?,
            &work,
            &cancellation,
        )?;
        assert!(duplicate.is_none(), "observer created another producer");
        release
            .send(())
            .map_err(|_| Error::Conflict("binding producer stopped before release".into()))?;
        let bound = binding.await?;
        drop(bound);
        let mounts = observer.mounts().await;
        assert!(
            mounts.is_ok(),
            "registered binding observer lost its successful preparation"
        );
        drop(mounts);
        checkout_available
    };
    // Complete and retire the actual preparation before reporting the assertion.
    assert!(
        checkout_available,
        "binding preparation retains the unrelated checkout owner"
    );
    Ok(())
}

#[tokio::test]
async fn native_guard_retains_the_original_binding_until_physical_retirement() -> Result<()> {
    acyclic_native_runtime::initialize_native_io_services().map_err(storage)?;
    acyclic_fs::initialize_native_mount_services().map_err(storage)?;
    let directory = tempfile::tempdir().map_err(storage)?;
    let state = tempfile::tempdir().map_err(storage)?;
    let harness = LocalHarness::open(
        crate::LocalHarnessConfig::native_host(directory.path())
            .state_directory(state.path().join("runtime")),
    )
    .await?;
    let selected_agent = crate::AgentId::from_bytes([1; 16]);
    let private_home = VolumeRef::new(
        harness.scratch.provider().clone(),
        "selected-home".into(),
        harness.scratch.class(),
        crate::conversation::VolumeOwner::Agent(selected_agent),
        &mut None,
    )?;
    harness.files.create_volume(&private_home).await?;
    let read = private_home.capability(crate::conversation::VolumeOperation::Read, &mut None)?;
    let write = private_home.capability(crate::conversation::VolumeOperation::Write, &mut None)?;
    let grants = crate::Capabilities::new(
        harness
            .signed
            .capabilities()
            .iter()
            .chain([read.as_str(), write.as_str()]),
    );
    let signed = harness
        .root_authority
        .as_ref()
        .ok_or_else(|| Error::NotFound("test signing custody".into()))?
        .bootstrap_scope("selected-home", &grants, Some(selected_agent), &mut None)?;
    let mut session_bindings = harness.session_bindings().to_vec();
    // A reused parent source is owned by its parent, not by this new binding.
    // Exercise physical retirement of an auxiliary volume this binding creates.
    session_bindings
        .iter_mut()
        .find(|entry| entry.name == crate::filesystem::HOME_VOLUME)
        .ok_or_else(|| Error::NotFound("home binding".into()))?
        .source = crate::agent::WorkspaceSource::Volume(private_home);
    let workspace = harness
        .workspace()
        .ok_or_else(|| Error::NotFound("binding workspace".into()))?;
    let cancellation = CancellationToken::new();
    let work = NativeMaterializationWork::new(WorkBudget::UNBOUNDED, &cancellation)
        .map_err(|(cause, work)| Error::NativeConstruction { cause, work })?;
    let view = workspace
        .view()
        .open_related(
            workspace.view().workspace().clone(),
            &mut KernelAccounting::NativeOriginal {
                work: &work,
                cancellation: &cancellation,
            },
        )
        .await
        .map_err(storage)?;
    let authority = crate::filesystem::NativeMountAuthority::admitted(
        harness.verifier.clone(),
        signed.copy(&mut None)?,
    );
    let owner = AgentOwner;
    let bound = workspace
        .bound_task_view(
            view,
            &harness.project,
            &session_bindings,
            &owner,
            &authority,
        )
        .await?;
    let source = bound
        .binding
        .as_ref()
        .ok_or_else(|| Error::NotFound("original binding".into()))?
        .named(Some(crate::filesystem::HOME_VOLUME))?
        .1
        .view
        .source
        .clone()
        .ok_or_else(|| Error::NotFound("original home source".into()))?;
    let original = source
        ._binding
        .as_ref()
        .ok_or_else(|| Error::NotFound("original binding owner".into()))?
        .downgrade();
    let key = source
        ._binding
        .as_ref()
        .ok_or_else(|| Error::NotFound("original binding owner".into()))?
        .clone()
        .downcast::<bindings::Binding>()
        .ok_or_else(|| Error::Conflict("original binding has another owner type".into()))?
        .key;
    let (ready, duplicate) = bindings::Bindings::select(
        &workspace.bindings,
        key,
        bindings::auxiliary_key(&session_bindings)?,
        &work,
        &cancellation,
    )?;
    assert!(
        duplicate.is_none(),
        "ready binding created another producer"
    );
    let reused = ready.mounts().await?;
    assert!(Arc::ptr_eq(
        &reused,
        bound
            .binding
            .as_ref()
            .ok_or_else(|| Error::NotFound("original binding".into()))?
    ));
    drop(reused);
    let entered = source.enter(false).await?;
    let native = source.native().await;
    assert!(
        native.is_some(),
        "original native projection was not prepared"
    );
    drop(bound);
    drop(source);
    drop(entered);
    assert!(
        original.upgrade().is_some(),
        "physical native guard lost the original binding owner"
    );
    assert!(
        matches!(
            bindings::Bindings::select(
                &workspace.bindings,
                key,
                bindings::auxiliary_key(&session_bindings)?,
                &work,
                &cancellation
            ),
            Err(Error::Conflict(_))
        ),
        "retired result was reconstructed from physical custody"
    );
    // The owned guard is the last slot owner. Dropping it performs the real
    // projection retirement and then releases the keyed binding generation.
    drop(native);
    assert!(
        original.upgrade().is_none(),
        "retired native projection retained the original binding owner"
    );
    Ok(())
}
