//! Production log barriers under concurrent independent workspace publications.
#![cfg(all(
    not(target_arch = "wasm32"),
    feature = "local",
    feature = "test-support"
))]

use acyclic_fs::facade::FsWork as _;
use acyclic_fs::{
    CancellationToken, LazyWorkspaceState, LazyWorkspaceStore, LocalCoreStateStore, WorkBudget,
    WorkspaceId,
};
use acyclic_native_runtime::testing::{DurableFault, Schedule};

fn state(id: WorkspaceId) -> Result<LazyWorkspaceState, serde_json::Error> {
    let shadows = [3_u8; 32];
    serde_json::from_value(serde_json::json!({
        "schema_version": 10,
        "revision": 1,
        "workspace_id": id,
        "parent_workspace_id": null,
        "fork_origin": null,
        "selected_import_roots": null,
        "source": acyclic_fs::demand::SourceReference { identity: [1; 16], epoch: 0 },
        "overlay": acyclic_fs::LazyOverlayId::from_bytes([2; 32]),
        "shadows": shadows,
        "pending_remove": null
    }))
}

async fn publish(
    store: &LocalCoreStateStore,
    id: WorkspaceId,
) -> Result<bool, Box<dyn std::error::Error>> {
    let cancellation = CancellationToken::new();
    Ok(store
        .compare_and_swap_lazy_workspace(
            id,
            0,
            state(id)?,
            &mut WorkBudget::UNBOUNDED.accounting(&cancellation),
        )
        .await?)
}

#[tokio::test]
async fn parallel_publications_share_durability_barriers() -> Result<(), Box<dyn std::error::Error>>
{
    acyclic_native_runtime::initialize_native_io_services()?;
    let mut maximum_barriers = 0;
    for agents in [1_u128, 4, 16, 64, 256] {
        let directory = tempfile::tempdir()?;
        let store = LocalCoreStateStore::open_owned(directory.path())?;
        assert!(publish(&store, WorkspaceId::from_bytes([0xff; 16])).await?);
        let (schedule, mut events) = Schedule::new();
        let driver = tokio::spawn(async move {
            while let Some(boundary) = events.recv().await {
                boundary.release();
            }
        });
        let started = std::time::Instant::now();
        let results = schedule
            .scope(futures::future::join_all((1..=agents).map(|agent| {
                publish(&store, WorkspaceId::from_bytes(agent.to_le_bytes()))
            })))
            .await;
        let elapsed = started.elapsed();
        for result in results {
            assert!(result?);
        }
        schedule.drained().await;
        driver.abort();
        let _ = driver.await;
        let trace = schedule.durable_writes();
        let barriers = trace
            .iter()
            .filter(|call| call.operation == "file:sync_data")
            .count();
        let total_barriers = trace
            .iter()
            .filter(|call| call.operation.starts_with("file:sync"))
            .count();
        assert!(
            barriers > 0,
            "acknowledged publications require a physical durability barrier"
        );
        eprintln!(
            "CORE_GROUP_COMMIT agents={agents} barriers={barriers} total_barriers={total_barriers} elapsed_us={}",
            elapsed.as_micros()
        );
        if agents == 256 {
            maximum_barriers = barriers;
        }
        drop(store);
        let reopened = LocalCoreStateStore::open_owned(directory.path())?;
        for agent in 1..=agents {
            let token = CancellationToken::new();
            let loaded = reopened
                .load_lazy_workspace(
                    WorkspaceId::from_bytes(agent.to_le_bytes()),
                    &mut WorkBudget::UNBOUNDED.accounting(&token),
                )
                .await?;
            assert!(
                loaded.is_some(),
                "acknowledged publication must survive reopen"
            );
        }
    }
    assert!(
        maximum_barriers < 256,
        "parallel publications must share log durability barriers"
    );
    Ok(())
}

#[tokio::test]
async fn failed_barrier_preserves_original_cause_and_blocks_cached_after_images()
-> Result<(), Box<dyn std::error::Error>> {
    acyclic_native_runtime::initialize_native_io_services()?;
    // Find the original barrier in a real successful publication. Faults are
    // applied to that physical boundary, not to a replacement store.
    let probe = tempfile::tempdir()?;
    let probe_store = LocalCoreStateStore::open_owned(probe.path())?;
    assert!(publish(&probe_store, WorkspaceId::from_bytes([0xff; 16])).await?);
    let (schedule, mut events) = Schedule::new();
    let driver = tokio::spawn(async move {
        while let Some(boundary) = events.recv().await {
            boundary.release();
        }
    });
    assert!(
        schedule
            .scope(publish(&probe_store, WorkspaceId::from_bytes([1; 16])))
            .await?
    );
    schedule.drained().await;
    driver.abort();
    let _ = driver.await;
    let trace = schedule.durable_writes();
    let barrier = trace
        .iter()
        .find(|call| call.operation == "file:sync_data")
        .ok_or("publication omitted its physical barrier")?
        .ordinal;
    for fault in [
        DurableFault::Before,
        DurableFault::After,
        DurableFault::Failed,
    ] {
        let directory = tempfile::tempdir()?;
        let store = LocalCoreStateStore::open_owned(directory.path())?;
        let acknowledged = WorkspaceId::from_bytes([0xff; 16]);
        assert!(publish(&store, acknowledged).await?);
        let (schedule, mut events) = Schedule::new();
        schedule.cut_at(barrier, fault);
        let driver = tokio::spawn(async move {
            while let Some(boundary) = events.recv().await {
                boundary.release();
            }
        });
        let id = WorkspaceId::from_bytes([1; 16]);
        let result = schedule.scope(publish(&store, id)).await;
        schedule.drained().await;
        driver.abort();
        let _ = driver.await;
        let cause = schedule
            .storage_fault()
            .ok_or("physical barrier fault was not reached")?;
        assert_eq!(cause.operation, "file:sync_data");
        let error = result.expect_err("a failed physical barrier must not acknowledge");
        let mut original: &(dyn std::error::Error + 'static) = error.as_ref();
        while original
            .downcast_ref::<acyclic_native_runtime::testing::InjectedStorageFault>()
            .is_none()
            && !original
                .downcast_ref::<std::io::Error>()
                .and_then(std::io::Error::get_ref)
                .is_some_and(|cause| {
                    cause.is::<acyclic_native_runtime::testing::InjectedStorageFault>()
                })
        {
            original = original
                .source()
                .ok_or("original physical barrier cause was erased")?;
        }
        let cancellation = CancellationToken::new();
        assert!(
            store
                .load_lazy_workspace(id, &mut WorkBudget::UNBOUNDED.accounting(&cancellation))
                .await
                .is_err(),
            "a failed owner must not serve an uncertain resident after-image"
        );
        drop(store);
        let reopened = LocalCoreStateStore::open_owned(directory.path())?;
        assert!(
            reopened
                .load_lazy_workspace(
                    acknowledged,
                    &mut WorkBudget::UNBOUNDED.accounting(&cancellation)
                )
                .await?
                .is_some(),
            "an earlier acknowledged frame must survive every later barrier fault"
        );
    }
    Ok(())
}
