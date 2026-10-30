use super::*;
use std::sync::mpsc;
mod checkpoint;

fn finite_budget() -> crate::WorkBudget {
    let mut budget = serde_json::to_value(crate::WorkBudget::UNBOUNDED).unwrap();
    for value in budget.as_object_mut().unwrap().values_mut() {
        *value = serde_json::Value::from(100_000_000_u64);
    }
    serde_json::from_value(budget).unwrap()
}

#[test]
fn admission_precedes_append_and_prepaid_barrier_survives_cancellation() {
    acyclic_native_runtime::initialize_native_io_services().unwrap();
    for cancel_after_append in [false, true] {
        let directory = tempfile::tempdir().unwrap();
        let owner = LocalCoreStateStore::open_owned(directory.path()).unwrap();
        let namespace = owner.namespace().unwrap();
        namespace.with_log(|_| Ok(())).unwrap();
        let wal = namespace.record(CORE_LOG_FAMILY, &[0; 16]).unwrap().current;
        let before = std::fs::read(&wal).unwrap();
        drop(namespace);
        let cancellation = crate::CancellationToken::new();
        let mut budget = finite_budget();
        // Both append repair and acknowledgement must be admitted before the
        // first frame byte. One credit is intentionally insufficient.
        budget.durability_operations = if cancel_after_append { 2 } else { 1 };
        let work = crate::NativeMaterializationWork::new(budget, &cancellation).unwrap();
        let original_ledger_bytes = work.live_allocation_bytes();
        let store = owner.materialization(work.clone());
        let namespace = store.namespace().unwrap();
        let result = namespace.with_log(|log| {
            let mut change = Change::new(&work)?;
            let value = super::super::tests::crash_lineage();
            let key = RecordKey::Lineage(value.workspace_id);
            let (value, allocation) = native_records::clone_native_record_value(
                &RecordValue::Lineage(value),
                &work,
                &cancellation,
            )?;
            change.put(Resident {
                key,
                value: Some(value),
                allocation: Some(allocation),
            })?;
            log.commit(&namespace, change)?;
            if cancel_after_append {
                cancellation.cancel();
            }
            Ok(())
        });
        if cancel_after_append {
            assert!(
                result.is_ok(),
                "the admitted barrier must settle after caller cancellation: {result:?}"
            );
            assert_eq!(
                work.receipt().durability_operations,
                1,
                "the actual shared barrier must stay on its original ledger"
            );
        } else {
            assert!(
                result.is_err(),
                "unfunded acknowledgement must fail before appending"
            );
            assert_eq!(std::fs::read(&wal).unwrap(), before);
            assert_eq!(work.receipt().durability_operations, 0);
        }
        work.receipt().verify(budget).unwrap();
        drop((namespace, store, owner));
        assert_eq!(
            work.live_allocation_bytes(),
            original_ledger_bytes,
            "the shared file and resident allocations must retire with their owner"
        );
        let reopened = LocalCoreStateStore::open_owned(directory.path()).unwrap();
        let namespace = reopened.namespace().unwrap();
        let present = namespace
            .with_log(|log| {
                log.get::<WorkspaceLineageRecord>(
                    &namespace,
                    super::super::tests::crash_lineage().workspace_id,
                    None,
                    None,
                )
            })
            .unwrap();
        assert_eq!(present.is_some(), cancel_after_append);
    }
}

/// Control the real owner barrier, rather than relying on disk timing to make
/// independent appenders overlap. The old immediate-flush path fails below.
#[test]
fn appends_overlap_but_acknowledgements_and_cached_reads_wait() {
    acyclic_native_runtime::initialize_native_io_services().unwrap();
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    let _entered = runtime.enter();
    let directory = tempfile::tempdir().unwrap();
    let store = LocalCoreStateStore::open_owned(directory.path()).unwrap();
    {
        let Ownership::Owned(owner) = &store.ownership else {
            panic!("owned fixture")
        };
        let gate = owner.flush_gate.lock().unwrap();
        let (appended, ready) = mpsc::channel();
        let (acknowledged, acknowledgements) = mpsc::channel();
        let mut workers = Vec::new();
        // Unit tests checkpoint after three frames; two outstanding publications
        // exercise grouping without crossing the separate checkpoint boundary.
        for agent in 1..=2 {
            let namespace = store.namespace().unwrap();
            let appended = appended.clone();
            let acknowledged = acknowledged.clone();
            workers.push(std::thread::spawn(move || {
                namespace
                    .with_log(|log| {
                        let mut state = super::super::tests::crash_lineage();
                        state.workspace_id = WorkspaceId::from_bytes([agent; 16]);
                        let mut change = Change::default();
                        change.put(
                            (
                                RecordKey::Lineage(state.workspace_id),
                                Some(RecordValue::Lineage(state)),
                            )
                                .into(),
                        )?;
                        log.commit(&namespace, change)?;
                        appended.send(agent).unwrap();
                        Ok(())
                    })
                    .unwrap();
                acknowledged.send(agent).unwrap();
            }));
        }
        for _ in 0..2 {
            ready.recv().unwrap();
        }
        let pending = {
            let slot = owner.log();
            let log = slot.as_ref().unwrap();
            log.length > log.flushed
        };
        let no_early_ack = matches!(acknowledgements.try_recv(), Err(mpsc::TryRecvError::Empty));
        let id = WorkspaceId::from_bytes([1; 16]);
        let read = store.load_logged::<WorkspaceLineageRecord>(id);
        let mut read = std::pin::pin!(read);
        let mut context = std::task::Context::from_waker(futures::task::noop_waker_ref());
        let read_poll = std::future::Future::poll(read.as_mut(), &mut context);
        let read_waited = read_poll.is_pending();
        drop(gate);
        let value = match read_poll {
            std::task::Poll::Pending => runtime.block_on(read),
            std::task::Poll::Ready(value) => value,
        };
        assert!(value.unwrap().is_some());
        for worker in workers {
            worker.join().unwrap();
        }
        assert!(
            pending,
            "independent appends must proceed while the physical barrier is occupied"
        );
        assert!(
            no_early_ack,
            "no publication may acknowledge ahead of its durability barrier"
        );
        assert!(
            read_waited,
            "the resident-cache fast path must not expose an unflushed after-image"
        );
        {
            let slot = owner.log();
            let log = slot.as_ref().unwrap();
            assert_eq!(log.flushed, log.length);
        }
    }
    drop(store);
    let reopened = LocalCoreStateStore::open_owned(directory.path()).unwrap();
    for agent in 1..=2 {
        assert!(
            runtime
                .block_on(
                    reopened.load_logged::<WorkspaceLineageRecord>(WorkspaceId::from_bytes(
                        [agent; 16]
                    ))
                )
                .unwrap()
                .is_some()
        );
    }
}
