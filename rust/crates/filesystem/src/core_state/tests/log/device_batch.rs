//! Checkpoint durability drains the device once after staging its leaves.
use super::*;
use acyclic_native_runtime::testing::Schedule;

#[tokio::test]
async fn checkpoint_batches_one_device_drain_after_all_record_writes()
-> Result<(), Box<dyn std::error::Error>> {
    acyclic_native_runtime::initialize_native_io_services()?;
    let mut observed_drains = Vec::new();
    for native in [false, true] {
        let directory = tempfile::tempdir()?;
        let cancellation = crate::CancellationToken::new();
        let budget = super::super::native::finite_budget();
        let work = crate::NativeMaterializationWork::new(budget, &cancellation)
            .map_err(|(error, _)| error)?;
        let store = LocalCoreStateStore::new(directory.path());
        let store = if native {
            store.materialization(work.clone())
        } else {
            store
        };
        let namespace = store.namespace()?;
        let mut log = CoreLog::open(&namespace, Arc::default())?;
        let mut change = if native {
            Change::new(&work)?
        } else {
            Change::default()
        };
        for byte in 1..=16 {
            change.put(
                (
                    RecordKey::Context(WorkspaceContextId::from_bytes([byte; 16])),
                    None,
                )
                    .into(),
            )?;
        }
        log.commit(&namespace, change)?;
        // A checkpoint of acknowledged records starts after the commit's WAL
        // barrier. Checkpoint still drains an unacknowledged WAL before leaves.
        log.flush_physical(&namespace)?;
        let wal = namespace.record(CORE_LOG_FAMILY, &[0; 16])?.current;
        assert!(std::fs::metadata(&wal)?.len() > 0);
        let (schedule, mut events) = Schedule::new();
        let driver = tokio::spawn(async move {
            while let Some(boundary) = events.recv().await {
                boundary.release();
            }
        });
        let (checkpoint, namespace, elapsed) = schedule
            .scope(async {
                acyclic_native_runtime::run_blocking_io(
                    move || {
                        let started = Instant::now();
                        let result = log.checkpoint(&namespace);
                        (result, namespace, started.elapsed())
                    },
                    None,
                )?
                .await
            })
            .await?;
        schedule.drained().await;
        driver.abort();
        let _ = driver.await;
        checkpoint?;
        let writes = schedule.durable_writes();
        let drains = writes
            .iter()
            .filter(|call| {
                matches!(
                    call.operation,
                    "file:sync_all" | "file:sync_data" | "file:sync_checkpoint_batch"
                )
            })
            .count();
        eprintln!(
            "CHECKPOINT_DEVICE_DRAIN native={native} records=16 drains={drains} elapsed_us={}",
            elapsed.as_micros()
        );
        assert_eq!(std::fs::metadata(&wal)?.len(), 0);
        let reopened = CoreLog::open(&namespace, Arc::default())?;
        assert!(reopened.dirty.is_empty());
        for byte in 1..=16 {
            let record = RecordKey::Context(WorkspaceContextId::from_bytes([byte; 16]));
            assert_eq!(std::fs::read(record.paths(&namespace)?.current)?, b"null");
        }
        work.receipt().verify(budget)?;
        observed_drains.push(drains);
    }
    let expected = if cfg!(target_os = "macos") { 1 } else { 16 };
    assert_eq!(
        observed_drains,
        vec![expected; 2],
        "one checkpoint must drain its device after staging every leaf and directory; non-Apple full file flush policy is retained"
    );
    Ok(())
}

async fn checkpoint_cut_preserves_wal(
    native: bool,
    cut: Option<(usize, acyclic_native_runtime::testing::DurableFault)>,
) -> Vec<acyclic_native_runtime::testing::DurableWrite> {
    let directory = tempfile::tempdir().unwrap();
    let cancellation = crate::CancellationToken::new();
    let budget = super::super::native::finite_budget();
    let work = crate::NativeMaterializationWork::new(budget, &cancellation).unwrap();
    let store = LocalCoreStateStore::new(directory.path());
    let store = if native {
        store.materialization(work.clone())
    } else {
        store
    };
    let namespace = store.namespace().unwrap();
    let mut log = CoreLog::open(&namespace, Arc::default()).unwrap();
    let mut change = if native {
        Change::new(&work).unwrap()
    } else {
        Change::default()
    };
    for index in 1..=4_u8 {
        change
            .put(
                (
                    RecordKey::Lineage(WorkspaceId::from_bytes([index; 16])),
                    None,
                )
                    .into(),
            )
            .unwrap();
    }
    log.commit(&namespace, change).unwrap();
    let wal = namespace.record(CORE_LOG_FAMILY, &[0; 16]).unwrap().current;
    let authoritative = std::fs::read(&wal).unwrap();
    assert!(!authoritative.is_empty());
    let (schedule, mut events) = Schedule::new();
    if let Some((ordinal, fault)) = cut {
        schedule.cut_at(ordinal, fault);
    }
    let driver = tokio::spawn(async move {
        while let Some(boundary) = events.recv().await {
            boundary.release();
        }
    });
    let (result, namespace) = schedule
        .scope(async {
            acyclic_native_runtime::run_blocking_io(
                move || {
                    let result = log.checkpoint(&namespace);
                    (result, namespace)
                },
                None,
            )
            .unwrap()
            .await
            .unwrap()
        })
        .await;
    schedule.drained().await;
    driver.abort();
    let _ = driver.await;
    if let Some((ordinal, _)) = cut {
        assert_eq!(schedule.storage_fault().unwrap().ordinal, ordinal);
        let LocalCoreStateStoreError::Io(error) = result.unwrap_err() else {
            panic!("checkpoint erased the original staged-write error");
        };
        assert!(error.get_ref().is_some_and(|cause| {
            cause.is::<acyclic_native_runtime::testing::InjectedStorageFault>()
        }));
        assert_eq!(
            std::fs::read(&wal).unwrap(),
            authoritative,
            "a failed staged write or final device drain cannot retire the WAL"
        );
        let mut reopened = CoreLog::open(&namespace, Arc::default()).unwrap();
        // Opening an ordinary log replays and checkpoints its durable frames.
        // The pre-open byte assertion proves the failed checkpoint kept them;
        // the empty WAL and installed leaves prove recovery consumed them.
        assert_eq!(reopened.length, 0);
        assert_eq!(std::fs::metadata(&wal).unwrap().len(), 0);
        for index in 1..=4_u8 {
            let key = RecordKey::Lineage(WorkspaceId::from_bytes([index; 16]));
            assert_eq!(
                std::fs::read(key.paths(&namespace).unwrap().current).unwrap(),
                b"null"
            );
            assert!(
                reopened
                    .get::<WorkspaceLineageRecord>(
                        &namespace,
                        WorkspaceId::from_bytes([index; 16]),
                        None,
                        None
                    )
                    .unwrap()
                    .is_none()
            );
        }
    } else {
        result.unwrap();
        assert_eq!(std::fs::metadata(&wal).unwrap().len(), 0);
    }
    work.receipt().verify(budget).unwrap();
    schedule.durable_writes()
}

#[tokio::test]
async fn every_checkpoint_stage_and_device_drain_cut_keeps_replay_authoritative() {
    use acyclic_native_runtime::testing::DurableFault;
    acyclic_native_runtime::initialize_native_io_services().unwrap();
    for native in [false, true] {
        let trace = checkpoint_cut_preserves_wal(native, None).await;
        let reset = trace
            .iter()
            .rev()
            .find(|call| call.operation == "file:truncate")
            .expect("the successful checkpoint must retire its WAL")
            .ordinal;
        assert!(reset > 4);
        for ordinal in 0..reset {
            for fault in [
                DurableFault::Before,
                DurableFault::After,
                DurableFault::Failed,
            ] {
                checkpoint_cut_preserves_wal(native, Some((ordinal, fault))).await;
            }
        }
    }
}
