use super::super::owner::{AgentOwner, ViewOwner};
use super::*;
use tokio::sync::oneshot;

#[tokio::test]
async fn physical_binding_reuses_its_preparation_and_rejects_another_scratch() -> Result<()> {
    let directory = tempfile::tempdir().map_err(storage)?;
    let state = tempfile::tempdir().map_err(storage)?;
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
    let read = shared.capability(crate::conversation::VolumeOperation::Read, &mut None)?;
    let write = shared.capability(crate::conversation::VolumeOperation::Write, &mut None)?;
    let home_read =
        private_home.capability(crate::conversation::VolumeOperation::Read, &mut None)?;
    let home_write =
        private_home.capability(crate::conversation::VolumeOperation::Write, &mut None)?;
    let grants = crate::Capabilities::new(harness.signed.capabilities().iter().chain([
        read.as_str(),
        write.as_str(),
        home_read.as_str(),
        home_write.as_str(),
    ]));
    let signed = harness
        .root_authority
        .as_ref()
        .ok_or_else(|| Error::NotFound("test signing custody".into()))?
        .bootstrap_scope("selected-volumes", &grants, Some(selected_agent), &mut None)?;
    let mut session_bindings = harness.session_bindings().to_vec();
    session_bindings[0].name = "repository".into();
    session_bindings[1].source = crate::agent::WorkspaceSource::Volume(private_home.clone());
    for (role, name) in [
        (
            crate::agent::WorkspaceRole::Private,
            crate::filesystem::PRIVATE_VOLUME,
        ),
        (crate::agent::WorkspaceRole::Internal, "interaction"),
    ] {
        session_bindings.push(crate::agent::WorkspaceEntry::initial_volume(
            role,
            name.into(),
            harness.scratch.clone(),
        ));
    }
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
    for name in [
        crate::filesystem::PRIVATE_VOLUME,
        "interaction",
        "repository",
    ] {
        assert!(
            second.mounts.named(Some(name)).is_err(),
            "unexpected auxiliary {name}"
        );
    }
    assert_eq!(second.mounts.named(None)?.1.view.volume, harness.project);
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
    for name in [
        crate::filesystem::PRIVATE_VOLUME,
        "interaction",
        "repository",
    ] {
        assert!(
            logical.mounts.named(Some(name)).is_err(),
            "unexpected auxiliary {name}"
        );
    }
    assert_eq!(logical.mounts.named(None)?.1.view.volume, harness.project);
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
        assert!(
            selected
                .mounts
                .named(Some(crate::filesystem::PRIVATE_VOLUME))
                .is_err()
        );
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
    let (options, policy) = capture::configuration(bound);
    let commands = super::super::git::Commands {
        workspace: bound,
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

#[tokio::test]
async fn child_projection_does_not_read_or_materialize_the_inherited_tree() -> Result<()> {
    let small = child_projection_work(1, false)
        .await?
        .expect("native projection");
    let large = child_projection_work(6_000, false)
        .await?
        .expect("native projection");
    assert_eq!(
        large.source_bytes_read, 0,
        "binding hydrated inherited payloads"
    );
    assert_eq!(large.materializations, 0, "binding copied inherited files");
    assert_eq!(
        large.source_entries_visited, small.source_entries_visited,
        "child binding enumerated the inherited tree"
    );
    Ok(())
}

#[tokio::test]
async fn child_projection_refuses_missing_provider_without_copying() -> Result<()> {
    assert!(child_projection_work(1, true).await?.is_none());
    Ok(())
}

async fn child_projection_work(
    entries: usize,
    unavailable: bool,
) -> Result<Option<acyclic_fs::WorkCounters>> {
    let checkout = tempfile::tempdir().map_err(storage)?;
    let state = tempfile::tempdir().map_err(storage)?;
    let absent_provider = tempfile::tempdir().map_err(storage)?;
    let payload = vec![b'x'; 10_240];
    for index in 0..entries {
        std::fs::write(
            checkout.path().join(format!("file-{index:06}.bin")),
            &payload,
        )
        .map_err(storage)?;
    }
    let mut config = crate::test_support::local_harness_config(
        checkout.path(),
        (
            crate::model::Model::new("scripted".into(), "projection".into(), "1".into())?,
            Arc::new(crate::test_support::ScriptedModelProvider::new([])),
        ),
    )?
    .state_directory(state.path())
    .workspace_mounting(WorkspaceMounting::Physical)
    .without_default_tools();
    if unavailable {
        config =
            config.native_mount_probe(acyclic_fs::NativeMountProbe::new(absent_provider.path()));
    }
    let harness = LocalHarness::open(config).await?;
    let root = harness.workspace.as_ref().expect("checkout");
    let project = VolumeRef::new(
        harness.project.provider().clone(),
        "child".into(),
        VolumeClass::Project,
        harness.project.owner().clone(),
        &mut None,
    )?;
    let agent = crate::AgentId::from_bytes([53; 16]);
    let private_home = VolumeRef::new(
        harness.scratch.provider().clone(),
        "child-home".into(),
        crate::conversation::VolumeClass::AgentPrivate,
        crate::conversation::VolumeOwner::Agent(agent),
        &mut None,
    )?;
    harness.files.create_volume(&private_home).await?;
    let mut bindings = harness.session_bindings();
    bindings
        .iter_mut()
        .find(|binding| binding.role == crate::agent::WorkspaceRole::Home)
        .expect("home binding")
        .source = crate::agent::WorkspaceSource::Volume(private_home.clone());
    let cancellation = CancellationToken::new();
    let (view, _) = root
        .view()
        .fork(
            &project.storage_name(&mut None)?,
            acyclic_fs::IdempotencyKey::from_bytes([47; 16]),
            acyclic_fs::PublicationPermit::Unrestricted,
            &mut WorkBudget::UNBOUNDED.accounting(&cancellation),
        )
        .await
        .map_err(storage)?;
    let before = view
        .workspace()
        .head(WorkBudget::UNBOUNDED, &cancellation)
        .await
        .map_err(storage)?
        .value
        .id();
    let read = project.capability(crate::conversation::VolumeOperation::Read, &mut None)?;
    let write = project.capability(crate::conversation::VolumeOperation::Write, &mut None)?;
    let home_read =
        private_home.capability(crate::conversation::VolumeOperation::Read, &mut None)?;
    let home_write =
        private_home.capability(crate::conversation::VolumeOperation::Write, &mut None)?;
    let grants = crate::Capabilities::new(harness.signed.capabilities().iter().chain([
        read.as_str(),
        write.as_str(),
        home_read.as_str(),
        home_write.as_str(),
    ]));
    let signed = harness
        .root_authority
        .as_ref()
        .expect("signing custody")
        .bootstrap_scope("child-projection", &grants, Some(agent), &mut None)?;
    let authority =
        crate::filesystem::NativeMountAuthority::admitted(harness.verifier.clone(), signed);
    let started = std::time::Instant::now();
    let result = root
        .bound_task_view(view, &project, &bindings, &AgentOwner, &authority)
        .await;
    if unavailable {
        assert!(
            matches!(result, Err(Error::MountUnavailable { .. })),
            "missing projection provider did not refuse at admission"
        );
        assert!(!root.mount_directory.join(".physical").exists());
        assert_eq!(
            std::fs::read(checkout.path().join("file-000000.bin")).map_err(storage)?,
            payload
        );
        harness.disconnect().await?;
        return Ok(None);
    }
    let bound = result?;
    let elapsed = started.elapsed();
    let home = bound.mounts.named(Some(crate::filesystem::HOME_VOLUME))?.1;
    assert_eq!(home.view.volume, private_home);
    assert!(
        home.view
            .source
            .as_ref()
            .expect("home binding")
            .options
            .is_some(),
        "native child inherited the physical root's logical-only home"
    );
    let mounted = bound
        .mounted
        .as_ref()
        .expect("child uses sparse projection");
    let work = mounted.native_work().map_err(storage)?.receipt();
    assert_eq!(
        bound
            .view()
            .workspace()
            .head(WorkBudget::UNBOUNDED, &cancellation)
            .await
            .map_err(storage)?
            .value
            .id(),
        before,
        "binding authored inherited entries"
    );
    eprintln!(
        "CHILD_PROJECTION entries={entries} elapsed_us={} work={work:?}",
        elapsed.as_micros()
    );
    for index in [0, entries - 1] {
        assert_eq!(
            std::fs::read(bound.path.join(format!("file-{index:06}.bin"))).map_err(storage)?,
            payload
        );
    }
    if entries == 1 {
        let source = home.view.source.as_ref().expect("home binding");
        volumes::native_scope(&bound.mounts, async |claims| {
            let mut environment = std::collections::BTreeMap::new();
            crate::filesystem::native_process::shell::workspace_environment(
                &bound.mounts,
                &mut environment,
            )?;
            let temporary = PathBuf::from(environment.get("TMPDIR").expect("private scratch"));
            assert_eq!(temporary, source.destination.join("tmp"));
            std::fs::write(temporary.join("receipt"), b"private scratch").map_err(storage)?;
            Ok(((), claims))
        })
        .await?;
        let persisted = source
            .workspace
            .workspace()
            .head(WorkBudget::UNBOUNDED, &cancellation)
            .await
            .map_err(storage)?
            .value
            .read_range("/tmp/receipt", 0, 15, WorkBudget::UNBOUNDED, &cancellation)
            .await
            .map_err(storage)?
            .value;
        assert_eq!(persisted.bytes.as_ref(), b"private scratch");
        assert!(!checkout.path().join("tmp").exists());
    }
    std::fs::write(bound.path.join("file-000000.bin"), b"child edit").map_err(storage)?;
    mounted.sync().await.map_err(storage)?;
    assert_eq!(
        std::fs::read(checkout.path().join("file-000000.bin")).map_err(storage)?,
        payload,
        "child mutation crossed its fork cut"
    );
    drop(bound);
    harness.disconnect().await?;
    Ok(Some(work))
}
