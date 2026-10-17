use super::*;
use std::panic::{AssertUnwindSafe, catch_unwind};

std::thread_local! {
    static OMIT_RESTORE: std::cell::Cell<bool> = const { std::cell::Cell::new(false) };
}
pub(super) fn omit_restore() -> bool {
    OMIT_RESTORE.with(std::cell::Cell::get)
}

proptest::proptest! {
    // Property: arbitrary nested scopes restore the caller's value on both
    // return and unwind, without undoing work completed inside the scope.
    #[test]
    fn nested_scopes_restore_policy_and_preserve_completed_work(
        original in proptest::bool::ANY,
        frames in proptest::collection::vec((proptest::bool::ANY, proptest::num::u8::ANY), 1..33),
        unwind in proptest::bool::ANY,
    ) {
        check::<true>(original, &frames, unwind);
    }
}

fn assert_policy<const ASSERT: bool>(actual: bool, expected: bool) {
    if ASSERT {
        assert_eq!(actual, expected, "scope policy restoration");
    }
}

fn check<const ASSERT: bool>(original: bool, frames: &[(bool, u8)], unwind: bool) {
    let cancellation = CancellationToken::new();
    let mut work = DemandWork::new(crate::performance::BOUNDARY_BUDGET, &cancellation);
    work.recheck_items = original;
    let result = catch_unwind(AssertUnwindSafe(|| {
        nested::<ASSERT>(&mut work, frames, unwind)
    }));
    assert_policy::<ASSERT>(work.recheck_items, original);
    assert_eq!(result.is_err(), unwind);
    if let Err(panic) = result {
        assert!(
            panic.is::<()>(),
            "only the deliberate callback panic may escape"
        );
    }
    assert_eq!(
        work.work.bytes_copied,
        frames
            .iter()
            .map(|(_, bytes)| u64::from(*bytes))
            .sum::<u64>()
    );
}

fn nested<const ASSERT: bool>(work: &mut DemandWork, frames: &[(bool, u8)], unwind: bool) {
    let Some((&(policy, bytes), remaining)) = frames.split_first() else {
        if unwind {
            std::panic::panic_any(());
        }
        return;
    };
    let original = work.recheck_items;
    let result = catch_unwind(AssertUnwindSafe(|| {
        work.scope(
            |work| &mut work.recheck_items,
            policy,
            |work| {
                assert_policy::<ASSERT>(work.recheck_items, policy);
                work.work.bytes_copied += u64::from(bytes);
                nested::<ASSERT>(work, remaining, unwind);
                assert_policy::<ASSERT>(work.recheck_items, policy);
            },
        )
    }));
    assert_policy::<ASSERT>(work.recheck_items, original);
    if let Err(panic) = result {
        std::panic::resume_unwind(panic);
    }
}

#[test]
fn missing_production_restoration_requires_the_policy_assertion() {
    for unwind in [false, true] {
        for original in [false, true] {
            OMIT_RESTORE.with(|flag| flag.set(true));
            let result = catch_unwind(|| check::<true>(original, &[(!original, 7)], unwind));
            OMIT_RESTORE.with(|flag| flag.set(false));
            let panic = result.expect_err("omitted production restoration must fail");
            let message = panic
                .downcast_ref::<String>()
                .map(String::as_str)
                .or_else(|| panic.downcast_ref::<&str>().copied())
                .unwrap_or("");
            assert!(message.contains("scope policy restoration"), "{message}");
            OMIT_RESTORE.with(|flag| flag.set(true));
            let without_assertion =
                catch_unwind(|| check::<false>(original, &[(!original, 7)], unwind));
            OMIT_RESTORE.with(|flag| flag.set(false));
            assert!(
                without_assertion.is_ok(),
                "the same mutant passes only if the policy assertion is deleted"
            );
        }
    }
}
