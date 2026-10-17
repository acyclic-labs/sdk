use super::*;
use proptest::prelude::*;

fn limits(items: u64) -> WorkBudget {
    WorkBudget {
        items_examined: items,
        ..WorkBudget::UNBOUNDED
    }
}

fn fixture(
    limit: u64,
    used: u64,
    nested: &[u64],
    recheck: bool,
) -> (
    CancellationToken,
    NativeMaterializationWork,
    NativeMaterializationWork,
) {
    let cancellation = CancellationToken::new();
    let root = NativeMaterializationWork::new(limits(limit), &cancellation).unwrap();
    root.charge(crate::materializer::NativeResourceCredit::Items.delta(used.min(limit)))
        .unwrap();
    let mut view = root.clone();
    for &limit in nested {
        view = view.native_volume_work(limits(limit)).unwrap();
    }
    if recheck {
        view = view.recheck_admitted();
    }
    (cancellation, root, view)
}

fn legacy(block: &[u8], work: &NativeMaterializationWork) -> Result<bool, CaptureError> {
    for byte in block {
        capture_charge(
            work,
            crate::materializer::NativeResourceCredit::Items.delta(1),
        )?;
        if *byte != 0 {
            return Ok(true);
        }
    }
    Ok(false)
}

fn outcome<T: std::fmt::Debug, E: std::fmt::Debug>(value: Result<T, E>) -> String {
    format!("{value:?}")
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(128))]
    // Exact result, refusal, examined prefix and remaining nested credit must
    // match byte-at-a-time admission, including reused item admission.
    #[test]
    fn block_scan_matches_scalar_admission_and_receipts(
        zeroes in 0_usize..65, tail in prop::collection::vec(any::<u8>(), 0..32),
        limit in 0_u64..97, used in 0_u64..97,
        nested in prop::collection::vec(0_u64..97, 0..3),
        recheck in any::<bool>(), cancelled in any::<bool>(),
    ) {
        let mut block = vec![0; zeroes];
        block.extend_from_slice(&tail);
        let (a_cancel, a_root, a_view) = fixture(limit, used, &nested, recheck);
        let (b_cancel, b_root, b_view) = fixture(limit, used, &nested, recheck);
        if cancelled { a_cancel.cancel(); b_cancel.cancel(); }
        prop_assert_eq!(outcome(block_has_data(&block, &a_view)), outcome(legacy(&block, &b_view)));
        prop_assert_eq!(a_root.receipt(), b_root.receipt());
        prop_assert_eq!(outcome(a_view.remaining()), outcome(b_view.remaining()));
        // A subsequent operation detects leaked reservations and pending credit.
        let item = crate::materializer::NativeResourceCredit::Items.delta(1);
        prop_assert_eq!(outcome(a_view.charge(item)), outcome(b_view.charge(item)));
        prop_assert_eq!(a_root.receipt(), b_root.receipt());
    }
}

#[test]
fn differential_property_rejects_charging_the_unexamined_tail() {
    let mut runner = proptest::test_runner::TestRunner::new(ProptestConfig {
        cases: 32,
        failure_persistence: None,
        ..ProptestConfig::default()
    });
    let result = runner.run(&(2_usize..65), |length| {
        let mut block = vec![0; length];
        block[0] = 1;
        let (_, root, current) = fixture(64, 0, &[], false);
        let (_, expected_root, expected) = fixture(64, 0, &[], false);
        let correct = legacy(&block, &expected).unwrap();
        // Deliberately broken: charge the whole admitted block despite the
        // first byte ending the scan. The differential property must fail.
        current
            .charge(crate::materializer::NativeResourceCredit::Items.delta(length as u64))
            .unwrap();
        prop_assert_eq!(block.iter().any(|byte| *byte != 0), correct);
        prop_assert_eq!(root.receipt(), expected_root.receipt());
        Ok(())
    });
    assert!(matches!(
        result,
        Err(proptest::test_runner::TestError::Fail(_, _))
    ));
}
