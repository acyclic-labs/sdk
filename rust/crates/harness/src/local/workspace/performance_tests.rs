use super::*;
use crate::test_support::ScriptedModelProvider;
use proptest::prelude::*;

async fn open(directory: &Path) -> Result<LocalHarness> {
    let volume = super::super::tests::volume(directory).await?;
    LocalHarness::open(
        super::super::tests::volume_config(
            directory,
            volume,
            Arc::new(ScriptedModelProvider::new([])),
        )?
        .without_default_tools()
        .compaction(crate::context::CompactionPolicy::Disabled),
    )
    .await
}

async fn update_work(
    store: &LocalCoreStateStore,
    id: acyclic_fs::WorkspaceId,
    updates: usize,
) -> Result<(u64, acyclic_fs::LazyWorkspaceState)> {
    let cancellation = acyclic_fs::CancellationToken::new();
    let work = NativeMaterializationWork::new(acyclic_fs::WorkBudget::UNBOUNDED, &cancellation)
        .map_err(|(cause, work)| Error::NativeConstruction { cause, work })?;
    let mut accounting = KernelAccounting::NativeOriginal {
        work: &work,
        cancellation: &cancellation,
    };
    let mut state = store
        .load_lazy_workspace(id, &mut accounting)
        .await
        .map_err(storage)?
        .ok_or_else(|| Error::NotFound("lazy state".into()))?;
    let before = work.receipt().durability_operations;
    for _ in 0..updates {
        let revision = state.revision;
        state.revision += 1;
        assert!(
            store
                .compare_and_swap_lazy_workspace(id, revision, state.clone(), &mut accounting)
                .await
                .map_err(storage)?
        );
    }
    Ok((work.receipt().durability_operations - before, state))
}
fn one_barrier_per_update(actual: u64, updates: usize) -> proptest::test_runner::TestCaseResult {
    prop_assert_eq!(
        actual,
        updates as u64,
        "each update must flush only its WAL frame"
    );
    Ok(())
}
proptest! {
    #![proptest_config(ProptestConfig::with_cases(4))]
    #[test]
    fn standalone_workspace_updates_do_not_checkpoint_each_transaction(updates in 1usize..6) {
        let (actual, old) = tokio::runtime::Builder::new_current_thread().enable_all()
            .build().unwrap().block_on(async {
                let directory = tempfile::tempdir().map_err(storage)?;
                let local = open(directory.path()).await?;
                let workspace = local.workspace.as_ref().unwrap();
                let id = workspace.view().workspace().id();
                let (actual, mut state) = update_work(&workspace.store, id, updates).await?;
                let old = LocalCoreStateStore::new(directory.path().join("shared-negative-control"));
                let cancellation = acyclic_fs::CancellationToken::new();
                let work = NativeMaterializationWork::new(acyclic_fs::WorkBudget::UNBOUNDED, &cancellation)
                    .map_err(|(cause, work)| Error::NativeConstruction { cause, work })?;
                let mut accounting = KernelAccounting::NativeOriginal { work: &work, cancellation: &cancellation };
                state.revision = 1;
                assert!(old.compare_and_swap_lazy_workspace(id, 0, state, &mut accounting).await.map_err(storage)?);
                let (old, _) = update_work(&old, id, updates).await?;
                Ok::<_, Error>((actual, old))
            }).unwrap();
        one_barrier_per_update(actual, updates)?;
        prop_assert!(one_barrier_per_update(old, updates).is_err());
    }
}

#[tokio::test]
async fn standalone_core_owner_survives_clones_and_reopens_after_release() -> Result<()> {
    let directory = tempfile::tempdir().map_err(storage)?;
    let local = open(directory.path()).await?;
    let store = local.workspace.as_ref().unwrap().store.clone();
    let id = local.workspace.as_ref().unwrap().view().workspace().id();
    let path = store.root().to_owned();
    assert!(
        LocalCoreStateStore::open_owned(&path).is_err(),
        "standalone namespace is shared"
    );
    drop(local);
    assert!(
        LocalCoreStateStore::open_owned(&path).is_err(),
        "clone lost ownership"
    );
    let (_, expected) = update_work(&store, id, 1).await?;
    drop(store);
    let reopened = LocalCoreStateStore::open_owned(&path).map_err(storage)?;
    let (_, actual) = update_work(&reopened, id, 0).await?;
    assert_eq!(actual, expected, "WAL state was lost across reopen");
    Ok(())
}

#[tokio::test]
async fn standalone_reopen_joins_the_owner_retained_by_a_saved_view() -> Result<()> {
    let directory = tempfile::tempdir().map_err(storage)?;
    let local = open(directory.path()).await?;
    let workspace = local.workspace.as_ref().unwrap();
    let cancellation = acyclic_fs::CancellationToken::new();
    let work = NativeMaterializationWork::new(acyclic_fs::WorkBudget::UNBOUNDED, &cancellation)
        .map_err(|(cause, work)| Error::NativeConstruction { cause, work })?;
    let mut accounting = KernelAccounting::NativeOriginal {
        work: &work,
        cancellation: &cancellation,
    };
    let view = workspace
        .view()
        .open_related(workspace.view().workspace().clone(), &mut accounting)
        .await
        .map_err(storage)?;
    drop(local);
    let reopened = open(directory.path()).await?;
    let store = &reopened.workspace.as_ref().unwrap().store;
    let id = view.workspace().id();
    update_work(store, id, 1).await?;
    // The old view remains usable while the replacement harness owns the log.
    view.snapshot(&mut accounting).await.map_err(storage)?;
    Ok(())
}
