use super::*;

fn limits(items: u64) -> WorkBudget {
    WorkBudget {
        items_examined: items,
        ..WorkBudget::UNBOUNDED
    }
}

fn model(run: impl Fn() + Send + Sync + 'static) {
    let mut builder = loom::model::Builder::new();
    builder.preemption_bound = None;
    builder.max_permutations = None;
    builder.max_duration = None;
    builder.checkpoint_file = None;
    builder.expect_explicit_explore = false;
    builder.check(run);
}

#[test]
fn block_scan_and_competing_admission_share_the_real_leaf() {
    crate::initialize_test_native_io();
    model(|| {
        let root = NativeMaterializationWork::new(limits(1), &CancellationToken::new()).unwrap();
        let view = root.native_volume_work(limits(1)).unwrap();
        let scan = view.clone();
        let scanner = loom::thread::spawn(move || block_has_data(&[1], &scan).is_ok());
        let writer = loom::thread::spawn(move || {
            view.charge(crate::materializer::NativeResourceCredit::Items.delta(1))
                .is_ok()
        });
        let (scanned, admitted) = (scanner.join().unwrap(), writer.join().unwrap());
        assert_eq!(usize::from(scanned) + usize::from(admitted), 1);
        assert_eq!(root.receipt().items_examined, 1);
        assert_eq!(root.remaining().unwrap().items_examined, 0);
    });
}

#[test]
fn model_rejects_split_admission_and_recording() {
    crate::initialize_test_native_io();
    let detected = std::panic::catch_unwind(|| {
        model(|| {
            let root =
                NativeMaterializationWork::new(limits(1), &CancellationToken::new()).unwrap();
            let threads = [root.clone(), root.clone()].map(|work| {
                loom::thread::spawn(move || {
                    let item = crate::materializer::NativeResourceCredit::Items.delta(1);
                    // Deliberately broken: release the real original leaf after
                    // admission, allowing both scans to spend its single item.
                    if work.admit(item).is_ok() {
                        loom::thread::yield_now();
                        let _ = work.record_admitted(item);
                    }
                })
            });
            for thread in threads {
                thread.join().unwrap();
            }
            let spent = root.receipt().items_examined;
            assert_eq!(
                spent.min(1),
                spent,
                "split admission overspent the item budget"
            );
        })
    });
    let failure = detected.expect_err("the model must reject split admission");
    let message = failure.downcast_ref::<String>().expect("assertion message");
    assert!(message.contains("split admission overspent"), "{message}");
}

#[test]
fn cancellation_preserves_completed_items_and_returns_unused_credit() {
    crate::initialize_test_native_io();
    let exercise = |leak_reservation| {
        model(move || {
            let cancellation = CancellationToken::new();
            let root = NativeMaterializationWork::new(limits(2), &cancellation).unwrap();
            let scan = root.clone();
            let scanner = loom::thread::spawn(move || {
                if leak_reservation {
                    // Deliberately broken: abandon accepted credit without
                    // completing or refunding it when cancellation wins.
                    let _ = scan.with_original_leaf(|work| {
                        work.reserve(crate::materializer::NativeResourceCredit::Items.delta(2))?;
                        loom::thread::yield_now();
                        work.check()
                    });
                } else {
                    let _ = block_has_data(&[0, 1], &scan);
                }
            });
            let canceller = loom::thread::spawn(move || cancellation.cancel());
            scanner.join().unwrap();
            canceller.join().unwrap();
            let spent = root.receipt().items_examined;
            let remaining = root
                .with_original_leaf(|work| work.remaining())
                .unwrap()
                .items_examined;
            assert!(
                spent == 0 || spent == 2,
                "a block is either unstarted or fully scanned"
            );
            assert_eq!(spent + remaining, 2, "reserved item conservation");
        })
    };
    exercise(false);
    let failure = std::panic::catch_unwind(|| exercise(true)).expect_err("leaked credit must fail");
    let message = failure.downcast_ref::<String>().expect("assertion message");
    assert!(message.contains("reserved item conservation"), "{message}");
}
