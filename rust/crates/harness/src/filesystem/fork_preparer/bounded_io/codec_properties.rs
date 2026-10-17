//! Input counters, independent of the production accounting implementation.
use super::*;
use crate::executor::json::JsonCodecCharge;
use proptest::prelude::*;
use proptest::test_runner::TestCaseResult;

#[derive(Clone, Copy)]
enum Fault {
    None,
    OmitCharge,
    DoubleCharge,
    MutateRejected,
}

fn field(work: &mut WorkCounters, index: usize) -> &mut u64 {
    match index {
        0 => &mut work.bytes_encoded,
        1 => &mut work.source_bytes_read,
        2 => &mut work.items_examined,
        3 => &mut work.bytes_copied,
        4 => &mut work.allocation_operations,
        _ => unreachable!(),
    }
}

fn history(
    limits: [u64; 5],
    events: &[(u8, u64)],
    cancel_at: Option<usize>,
    native: bool,
    fault: Fault,
) -> TestCaseResult {
    let cancellation = CancellationToken::new();
    let mut budget = acyclic_fs::performance::work_budget(100_000, 16_000_000);
    for (index, limit) in limits.into_iter().enumerate() {
        *field(&mut budget, index) = limit;
    }
    // Native construction funds one ledger allocation before codec work.
    budget.allocation_operations += u64::from(native);
    let original =
        native.then(|| acyclic_fs::NativeMaterializationWork::new(budget, &cancellation).unwrap());
    let io = match &original {
        Some(original) => PreparationIo::from_native_work(original).unwrap(),
        None => PreparationIo::from_budget(budget, &cancellation).unwrap(),
    };
    let mut expected = io.receipt();
    let mut observed = Vec::new();
    let mut wanted = Vec::new();
    let mut cancelled = false;
    for (step, &(kind, amount)) in events.iter().enumerate() {
        if cancel_at == Some(step) {
            cancelled = true;
            cancellation.cancel();
        }
        let (charge, encode, index) = match kind {
            0 => (
                JsonCodecCharge::Work {
                    bytes: amount,
                    items: 1,
                },
                true,
                0,
            ),
            1 => (
                JsonCodecCharge::Work {
                    bytes: amount,
                    items: 1,
                },
                false,
                1,
            ),
            2 => (JsonCodecCharge::Copy { bytes: amount }, true, 3),
            3 => (JsonCodecCharge::Read { bytes: amount }, true, 1),
            4 => (JsonCodecCharge::Inspect { items: amount }, true, 2),
            5 => (
                JsonCodecCharge::AllocationOperations { operations: amount },
                true,
                4,
            ),
            _ => unreachable!(),
        };
        let mut next = expected;
        let value = field(&mut next, index);
        let mut accepted = !cancelled;
        match value.checked_add(amount) {
            Some(sum) => *value = sum,
            None => accepted = false,
        }
        if kind <= 1 {
            match next.items_examined.checked_add(1) {
                Some(sum) => next.items_examined = sum,
                None => accepted = false,
            }
        }
        for index in 0..5 {
            accepted &= *field(&mut next, index) <= *field(&mut budget, index);
        }
        let result = if matches!(fault, Fault::OmitCharge) {
            Ok(())
        } else {
            io.codec_charge(charge, encode)
        };
        if matches!(fault, Fault::DoubleCharge) && result.is_ok() {
            let _ = io.codec_charge(charge, encode);
        }
        if matches!(fault, Fault::MutateRejected) && result.is_err() {
            let _ = io.spent(WorkCounters {
                bytes_copied: 1,
                ..WorkCounters::UNCHARGED
            });
        }
        if accepted {
            expected = next;
        }
        observed.push((result.is_ok(), io.receipt()));
        wanted.push((accepted, expected));
    }
    prop_assert_eq!(
        observed,
        wanted,
        "codec receipts differ from input counters"
    );
    Ok(())
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(64))]
    #[test]
    fn codec_charges_preserve_exact_receipts_and_rejection(
        limits in prop::array::uniform5(0u64..17),
        events in prop::collection::vec((0u8..6, prop_oneof![7 => 0u64..17, 1 => any::<u64>()]), 0..17),
        cancel_at in prop::option::of(0usize..17),
    ) {
        for native in [false, true] {
            // Force every event kind to succeed, then exceed a bound, even if
            // the generated limits are all zero or the history is empty.
            history([8; 5], &[(0,1),(1,1),(2,1),(3,1),(4,1),(5,1),(2,9),(0,u64::MAX)], None, native, Fault::None)?;
            history(limits, &events, cancel_at, native, Fault::None)?;
        }
    }
}

#[test]
fn codec_accounting_negative_controls() {
    for native in [false, true] {
        for (fault, limits, event) in [
            (Fault::OmitCharge, [8; 5], (0, 1)),
            (Fault::DoubleCharge, [8; 5], (0, 1)),
            (Fault::MutateRejected, [0; 5], (2, 1)),
        ] {
            let error = history(limits, &[event], None, native, fault).unwrap_err();
            assert!(
                error
                    .to_string()
                    .contains("codec receipts differ from input counters"),
                "{error}"
            );
        }
    }
}
