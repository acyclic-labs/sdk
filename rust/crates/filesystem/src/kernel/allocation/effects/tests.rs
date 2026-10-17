use super::*;
use crate::WorkBudget;
use proptest::prelude::*;

proptest! {
    #![proptest_config(ProptestConfig::with_cases(48))]

    // Moving the sole owner, partial settlement and Drop must conserve the
    // original allowance, including after caller cancellation. Expected credit
    // comes only from the generated input, never from the owner's counters.
    #[test]
    fn moved_credit_refunds_only_unused_work(
        reserved in 0_u64..65,
        prefix in 0_u64..65,
        cancelled in any::<bool>(),
    ) {
        let actual = prefix.min(reserved);
        let cancellation = CancellationToken::new();
        let work = NativeMaterializationWork::new(WorkBudget {
            bytes_copied: 64,
            ..WorkBudget::UNBOUNDED
        }, &cancellation).unwrap();
        let mut credit = EffectCredit::new(&work, &cancellation);
        credit.reserve(WorkCounters {
            bytes_copied: reserved,
            ..WorkCounters::UNCHARGED
        }, false).unwrap();
        // No observer Drop implementation participates in this handoff.
        let mut moved = credit;
        if cancelled { cancellation.cancel(); }
        let completed = moved.complete(WorkCounters {
            bytes_copied: actual,
            ..WorkCounters::UNCHARGED
        });
        prop_assert_eq!(completed.is_err(), cancelled);
        prop_assert_eq!(work.receipt().bytes_copied, actual);
        prop_assert_eq!(work.state(|state| state.budget.bytes_copied), 64 - reserved + actual);
        drop(moved);
        prop_assert_eq!(work.state(|state| state.budget.bytes_copied), 64);
        prop_assert_eq!(work.receipt().bytes_copied, actual);
    }
}

// Negative control: the same budget oracle observes a skipped refund. Only
// Drop returns credit, so leaking the owner leaves the reservation withheld.
#[test]
fn leaked_credit_is_observed_as_withheld() {
    let cancellation = CancellationToken::new();
    let work = NativeMaterializationWork::new(
        WorkBudget {
            bytes_copied: 64,
            ..WorkBudget::UNBOUNDED
        },
        &cancellation,
    )
    .unwrap();
    let mut credit = EffectCredit::new(&work, &cancellation);
    credit
        .reserve(
            WorkCounters {
                bytes_copied: 40,
                ..WorkCounters::UNCHARGED
            },
            false,
        )
        .unwrap();
    std::mem::forget(credit);
    assert_eq!(work.state(|state| state.budget.bytes_copied), 24);
    assert_eq!(work.receipt().bytes_copied, 0);
}
