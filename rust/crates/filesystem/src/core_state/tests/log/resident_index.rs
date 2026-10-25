//! The same cached read must not revisit unrelated agents' records.
use super::*;

#[test]
fn cached_absence_lookup_cost_is_bounded_by_record_identity_width() {
    acyclic_native_runtime::initialize_native_io_services().unwrap();
    let mut measured = Vec::new();
    for count in [1_usize, 4, 16, 64, 256] {
        let directory = tempfile::tempdir().unwrap();
        let cancellation = crate::CancellationToken::new();
        let budget = super::super::native::finite_budget();
        let work = crate::NativeMaterializationWork::new(budget, &cancellation).unwrap();
        let namespace = LocalCoreStateStore::new(directory.path())
            .materialization(work.clone())
            .namespace()
            .unwrap();
        let mut log = CoreLog::open(&namespace, Arc::default()).unwrap();
        let mut change = Change::new(&work).unwrap();
        for index in 0..count {
            let id = WorkspaceId::from_bytes(u128::try_from(index).unwrap().to_le_bytes());
            change.put((RecordKey::Lineage(id), None).into()).unwrap();
        }
        log.commit(&namespace, change).unwrap();
        log.adopt_native(&work).unwrap();
        let before = work.receipt().items_examined;
        let id = WorkspaceId::from_bytes(u128::try_from(count - 1).unwrap().to_le_bytes());
        assert!(
            log.get::<WorkspaceLineageRecord>(&namespace, id, None, None)
                .unwrap()
                .is_none()
        );
        let examined = work.receipt().items_examined - before;
        eprintln!("CACHED_RECORD_LOOKUP agents={count} items_examined={examined}");
        measured.push((count, examined));
        work.receipt().verify(budget).unwrap();
    }
    assert!(
        measured.iter().all(|(_, examined)| *examined <= 137),
        "one 136-bit record identity must bound the complete cached-read lookup: {measured:?}"
    );
}

#[test]
fn already_adopted_residents_are_not_reexamined_on_each_call() {
    acyclic_native_runtime::initialize_native_io_services().unwrap();
    let directory = tempfile::tempdir().unwrap();
    let cancellation = crate::CancellationToken::new();
    let budget = super::super::native::finite_budget();
    let work = crate::NativeMaterializationWork::new(budget, &cancellation).unwrap();
    let namespace = LocalCoreStateStore::new(directory.path())
        .materialization(work.clone())
        .namespace()
        .unwrap();
    let mut log = CoreLog::open(&namespace, Arc::default()).unwrap();
    let mut change = Change::new(&work).unwrap();
    for index in 0..256_u128 {
        change
            .put(
                (
                    RecordKey::Lineage(WorkspaceId::from_bytes(index.to_le_bytes())),
                    None,
                )
                    .into(),
            )
            .unwrap();
    }
    log.commit(&namespace, change).unwrap();
    log.adopt_native(&work).unwrap();
    let before = work.receipt().items_examined;
    log.adopt_native(&work).unwrap();
    let examined = work.receipt().items_examined - before;
    eprintln!("CACHED_RECORD_ADOPTION agents=256 items_examined={examined}");
    assert_eq!(
        examined, 0,
        "retained funding must be consumed once at installation"
    );
    work.receipt().verify(budget).unwrap();
}

#[test]
fn borrowing_a_funded_cache_does_not_reallocate_its_original_backing() {
    acyclic_native_runtime::initialize_native_io_services().unwrap();
    let directory = tempfile::tempdir().unwrap();
    let cancellation = crate::CancellationToken::new();
    let budget = super::super::native::finite_budget();
    let original = crate::NativeMaterializationWork::new(budget, &cancellation).unwrap();
    let namespace = LocalCoreStateStore::new(directory.path())
        .materialization(original.clone())
        .namespace()
        .unwrap();
    let mut log = CoreLog::open(&namespace, Arc::default()).unwrap();
    let mut change = Change::new(&original).unwrap();
    for index in 0..256_u128 {
        change
            .put(
                (
                    RecordKey::Lineage(WorkspaceId::from_bytes(index.to_le_bytes())),
                    None,
                )
                    .into(),
            )
            .unwrap();
    }
    log.commit(&namespace, change).unwrap();
    log.adopt_native(&original).unwrap();
    let before = original.live_allocation_bytes();
    let borrower = crate::NativeMaterializationWork::new(budget, &cancellation).unwrap();
    assert!(!original.shares_allocation_context(&borrower));
    let borrowed_before = borrower.receipt();
    log.adopt_native(&borrower).unwrap();
    let copied = borrower.receipt().bytes_copied - borrowed_before.bytes_copied;
    eprintln!(
        "CACHED_RECORD_CUSTODY agents=256 copied_bytes={copied} original_before={before} original_after={}",
        original.live_allocation_bytes()
    );
    assert_eq!(
        copied, 0,
        "a borrowed cache keeps its original backing and original ledger"
    );
    assert_eq!(original.live_allocation_bytes(), before);
    drop(log);
    drop(namespace);
    assert_eq!(
        original.live_allocation_bytes(),
        crate::NativeMaterializationWork::accounting_state_bytes()
    );
    original.receipt().verify(budget).unwrap();
    borrower.receipt().verify(budget).unwrap();
}

#[test]
fn ordinary_log_reads_and_writes_do_not_inherit_cancelled_cache_funding() {
    acyclic_native_runtime::initialize_native_io_services().unwrap();
    let directory = tempfile::tempdir().unwrap();
    let cancellation = crate::CancellationToken::new();
    let work =
        crate::NativeMaterializationWork::new(super::super::native::finite_budget(), &cancellation)
            .unwrap();
    let native = LocalCoreStateStore::new(directory.path())
        .materialization(work.clone())
        .namespace()
        .unwrap();
    let ordinary = LocalCoreStateStore::new(directory.path())
        .namespace()
        .unwrap();
    let mut log = CoreLog::open(&native, Arc::default()).unwrap();
    let mut change = Change::new(&work).unwrap();
    change
        .put(
            (
                RecordKey::Lineage(WorkspaceId::from_bytes([0xa1; 16])),
                None,
            )
                .into(),
        )
        .unwrap();
    log.commit(&native, change).unwrap();
    cancellation.cancel();
    assert!(
        log.get::<WorkspaceLineageRecord>(
            &ordinary,
            WorkspaceId::from_bytes([0xa2; 16]),
            None,
            None
        )
        .unwrap()
        .is_none()
    );
    let mut change = Change::default();
    change
        .put(
            (
                RecordKey::Lineage(WorkspaceId::from_bytes([0xa3; 16])),
                None,
            )
                .into(),
        )
        .unwrap();
    log.commit(&ordinary, change).unwrap();
    assert!(
        log.get::<WorkspaceLineageRecord>(
            &ordinary,
            WorkspaceId::from_bytes([0xa3; 16]),
            None,
            None
        )
        .unwrap()
        .is_none()
    );
}
