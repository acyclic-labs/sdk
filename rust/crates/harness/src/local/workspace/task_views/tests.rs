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
    let session_bindings = harness.session_bindings();
    let mut changed_bindings = session_bindings.clone();
    changed_bindings[1].volume = harness.project.clone();
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
        harness.signed.copy(&mut None)?,
    );
    let (entered, observed) = oneshot::channel();
    let (release, released) = oneshot::channel();
    let owner = PausedOwner {
        original: AgentOwner(crate::agent::AgentPath::root(
            &crate::agent::AgentId::from_bytes([1; 16]),
        )),
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
    assert!(
        second
            .mounts
            .named(Some(crate::filesystem::HOME_VOLUME))
            .is_err()
    );
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
    std::fs::write(second.path.join("shared.txt"), b"shared").map_err(storage)?;
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
    changed_shared[2].volume = harness.project.clone();
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

    fn lease(&self, operation: OperationId) -> acyclic_fs::OperationLeaseId {
        self.original.lease(operation)
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
        original: AgentOwner(crate::agent::AgentPath::root(
            &crate::agent::AgentId::from_bytes([1; 16]),
        )),
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
    let owner = AgentOwner(crate::agent::AgentPath::root(
        &crate::agent::AgentId::from_bytes([1; 16]),
    ));
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
    let root = source
        .destination
        .ancestors()
        .nth(2)
        .ok_or_else(|| Error::Invalid("home binding has no root".into()))?;
    let entered = source.enter(root, volumes::HOME_PATH, false).await?;
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
