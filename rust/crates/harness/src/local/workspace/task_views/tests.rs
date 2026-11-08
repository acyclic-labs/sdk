use super::super::owner::{AgentOwner, ViewOwner};
use super::*;
use tokio::sync::oneshot;

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
    let directory = tempfile::tempdir().map_err(storage)?;
    let state = tempfile::tempdir().map_err(storage)?;
    let harness = LocalHarness::open(
        crate::LocalHarnessConfig::native_host(directory.path())
            .state_directory(state.path().join("runtime")),
    )
    .await?;
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
    let (_release, released) = oneshot::channel();
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
            harness.scratch.clone(),
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
        workspace.state.try_lock().is_ok()
    };
    // Destroy the actual preparation future before reporting the assertion, so
    // failure also exercises its normal cancellation cleanup.
    assert!(
        checkout_available,
        "binding preparation retains the unrelated checkout owner"
    );
    Ok(())
}

#[tokio::test]
async fn native_guard_retains_the_original_binding_until_physical_retirement() -> Result<()> {
    let directory = tempfile::tempdir().map_err(storage)?;
    let state = tempfile::tempdir().map_err(storage)?;
    let harness = LocalHarness::open(
        crate::LocalHarnessConfig::native_host(directory.path())
            .state_directory(state.path().join("runtime")),
    )
    .await?;
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
            harness.scratch.clone(),
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
    // The owned guard is the last slot owner. Dropping it performs the real
    // projection retirement and then releases the keyed binding generation.
    drop(native);
    assert!(
        original.upgrade().is_none(),
        "retired native projection retained the original binding owner"
    );
    Ok(())
}
