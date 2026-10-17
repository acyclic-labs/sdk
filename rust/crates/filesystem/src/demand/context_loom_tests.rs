#[cfg(feature = "native-watch")]
fn read_and_invalidate(legacy_inspection: bool) {
    use super::{NativeMountAllocationCustody, SourcePayloadOwnership};
    use crate::sync::{Mutex, atomic::AtomicU64};
    use crate::{CancellationToken, NativeMaterializationWork, WorkCounters};
    use loom::sync::Condvar;
    use std::sync::Arc;
    crate::initialize_test_native_io();
    loom::model(move || {
        let budget = crate::performance::BOUNDARY_BUDGET;
        let original = NativeMaterializationWork::new(budget, &CancellationToken::new())
            .map_err(|(error, _)| error)
            .unwrap();
        let mut resource = original.native_volume_work(budget).unwrap();
        let custody = NativeMountAllocationCustody::new(&mut resource).unwrap();
        resource
            .with_original_leaf(|work| custody.claim(37, work))
            .unwrap();
        // A real retirement notification admits the reconciliation scan.
        custody.retire(1).unwrap();
        let expected_items = original.receipt().items_examined + 1;
        let weak = Arc::downgrade(&custody);
        // Custody is now the sole owner of resource metadata. Its destruction
        // really calls NativeAllocation::drop -> release -> original leaf.
        drop(resource);
        let ownership = SourcePayloadOwnership::Registered(custody, AtomicU64::new(36));
        // Callback result mutex: acquired only after dropping leaf, and
        // released before any demand call or joining invalidation.
        let complete = Arc::new((Mutex::new(None), Condvar::new()));
        let read_complete = complete.clone();
        let read_work = original.clone();
        let read = loom::thread::spawn(move || {
            let items = read_work.with_original_leaf(|work| {
                if legacy_inspection {
                    // The previous mount_bytes implementation. Yield while its
                    // temporary Arc pins custody to explore final-owner release.
                    let owner = weak.upgrade();
                    loom::thread::yield_now();
                    std::hint::black_box(owner.map_or(0, |owner| owner.retained_bytes()));
                } else {
                    loom::thread::yield_now();
                }
                work.charge(WorkCounters {
                    items_examined: 1,
                    ..WorkCounters::UNCHARGED
                })
                .unwrap();
                work.work.items_examined
            });
            // readdirplus waits for the source-read result without owning leaf.
            let (result, ready) = &*read_complete;
            *result.lock().unwrap() = Some(items);
            ready.notify_one();
        });
        let invalidation_work = original.clone();
        let invalidation = loom::thread::spawn(move || {
            drop(ownership);
            invalidation_work.charge(WorkCounters::UNCHARGED).unwrap();
        });
        let (result, ready) = &*complete;
        let mut result = result.lock().unwrap();
        while result.is_none() {
            result = ready.wait(result).unwrap();
        }
        assert_eq!(*result, Some(expected_items));
        drop(result);
        read.join().unwrap();
        invalidation.join().unwrap();
        original.charge(WorkCounters::UNCHARGED).unwrap();
        assert_eq!(
            original.live_allocation_bytes(),
            NativeMaterializationWork::accounting_state_bytes()
        );
        let spent = original.receipt();
        assert_eq!(spent.items_examined, expected_items);
        spent.verify(budget).unwrap();
    });
}
#[cfg(feature = "native-watch")]
#[test]
fn custody_inspection_completes_read_callback_and_invalidation() {
    read_and_invalidate(false);
}
#[cfg(feature = "native-watch")]
#[test]
fn previous_custody_inspection_deadlocks_the_real_leaf() {
    const BROKEN: &str = "ACYCLIC_FS_LOOM_LEGACY_CUSTODY";
    if std::env::var_os(BROKEN).is_some() {
        read_and_invalidate(true);
        return;
    }
    // A recursive destructor lock deadlocks Loom; unwinding that destructor
    // can itself re-enter Loom after its scheduler has stopped. Isolate the
    // broken path so its expected abort cannot hide the positive model result.
    let output = std::process::Command::new(std::env::current_exe().unwrap())
        .args([
            "--exact",
            "demand::context_loom_tests::previous_custody_inspection_deadlocks_the_real_leaf",
            "--nocapture",
        ])
        .env(BROKEN, "1")
        .output()
        .unwrap();
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(!output.status.success(), "the old weak upgrade must fail");
    assert!(
        stderr.contains("deadlock; threads") || stderr.contains("lock order:"),
        "{stderr}"
    );
}
