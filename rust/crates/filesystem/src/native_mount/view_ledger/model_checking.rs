use super::*;
use std::cell::Cell;
use std::mem::ManuallyDrop;
use std::panic::{AssertUnwindSafe, catch_unwind};

loom::thread_local! {
    static MAP_BEFORE_METER: Cell<bool> = Cell::new(false);
}

pub(super) fn hold_map_while_charging(
    map: &RwLock<ChangePositions>,
) -> Option<crate::sync::RwLockWriteGuard<'_, ChangePositions>> {
    MAP_BEFORE_METER
        .with(Cell::get)
        .then(|| map.write().unwrap())
}

#[test]
fn concurrent_reader_and_growth_share_the_original_meter() {
    crate::initialize_test_native_io();
    let exercise = |broken| {
        loom::model(move || {
            let work = crate::NativeMaterializationWork::new(
                crate::WorkBudget::UNBOUNDED,
                &crate::CancellationToken::new(),
            )
            .unwrap();
            let map = Arc::new(RwLock::new(ChangePositions::default()));
            let mut carrier = NativeDemandCarrier::Native(&work);
            record(&map, 0, ViewStamp(1), &mut carrier).unwrap();
            let capacity = map.read().unwrap().latest.capacity();
            for key in 1..capacity as u64 {
                record(&map, key, ViewStamp(1), &mut carrier).unwrap();
            }
            assert_eq!(map.read().unwrap().latest.len(), capacity);
            // Retain owners through Loom's deliberate deadlock panic; their
            // accounting destructors otherwise try to lock during teardown.
            let keeper = ManuallyDrop::new((Arc::clone(&map), work.clone()));
            let writer = loom::thread::spawn({
                let (map, work) = (Arc::clone(&map), work.clone());
                move || {
                    MAP_BEFORE_METER.with(|fault| fault.set(broken));
                    record(
                        &map,
                        capacity as u64,
                        ViewStamp(2),
                        &mut NativeDemandCarrier::Native(&work),
                    )
                    .unwrap();
                }
            });
            // Production lookup lock order: original meter, then ledger map,
            // concurrently with the production record/grow algorithm.
            work.with_original_leaf(|leaf| {
                leaf.check().unwrap();
                assert!(unchanged_since(&map, ViewStamp(2), 0..capacity as u64));
            });
            writer.join().unwrap();
            assert!(map.read().unwrap().latest.capacity() > capacity);
            assert!(unchanged_since(&map, ViewStamp(1), 0..capacity as u64));
            assert!(!unchanged_since(&map, ViewStamp(1), [capacity as u64]));
            record(&map, capacity as u64, ViewStamp(1), &mut carrier).unwrap();
            assert!(!unchanged_since(&map, ViewStamp(1), [capacity as u64]));
            drop(ManuallyDrop::into_inner(keeper));
        });
    };
    exercise(false);
    let error = catch_unwind(AssertUnwindSafe(|| exercise(true)))
        .expect_err("holding the map while charging must deadlock");
    let message = error
        .downcast_ref::<String>()
        .expect("Loom deadlock message");
    assert!(message.contains("deadlock"), "{message}");
}
