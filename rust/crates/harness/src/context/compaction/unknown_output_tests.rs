use super::*;
use proptest::prelude::*;

thread_local! {
    pub(super) static REQUIRE_OUTPUT_BOUND: std::cell::Cell<bool> = const {std::cell::Cell::new(false)};
    pub(super) static IGNORE_CONTEXT_PRESSURE: std::cell::Cell<bool> = const {std::cell::Cell::new(false)};
}

fn context_budget(context: u32, fixed: u32) {
    let capacity = ModelContextCapacity {
        context_tokens: context,
        output_tokens: None,
    };
    let result = ThresholdCompaction::for_capacity(capacity, fixed);
    assert_eq!(
        result.is_ok(),
        context.checked_sub(fixed).is_some_and(|n| n >= 3),
        "unknown output preserves every fundable context budget"
    );
    if let Ok(policy) = result {
        let reserve = policy.validate(capacity, None).unwrap();
        assert!(reserve > 0);
        assert!(
            u64::from(fixed) + u64::from(reserve) + 2 * u64::from(policy.recent_tokens)
                <= u64::from(context)
        );
        assert_eq!(capacity.output_tokens, None);
        assert!(
            policy
                .needs_compaction(capacity, u64::from(context))
                .unwrap(),
            "unknown output must still trigger context compaction"
        );
        assert!(!policy.needs_compaction(capacity, 0).unwrap());
    }
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(64))]
    // Unknown output capacity never disables context pressure or permits a
    // request to exceed its context. The reserve is a policy budget only.
    #[test]
    fn unknown_output_still_compacts_against_context(
        context in 3u32..=u32::MAX, fixed in any::<u32>(),
    ) {
        context_budget(context, fixed);
    }
}

#[test]
fn negative_controls_detect_unknown_output_disabling_admission_or_compaction() {
    for (flag, expected) in [
        (
            &REQUIRE_OUTPUT_BOUND,
            "unknown output preserves every fundable context budget",
        ),
        (
            &IGNORE_CONTEXT_PRESSURE,
            "unknown output must still trigger context compaction",
        ),
    ] {
        flag.with(|flag| flag.set(true));
        let detected = std::panic::catch_unwind(|| context_budget(32_768, 512));
        flag.with(|flag| flag.set(false));
        let panic =
            detected.expect_err("restored broken production path must fail the budget oracle");
        let text = panic
            .downcast_ref::<String>()
            .map(String::as_str)
            .or_else(|| panic.downcast_ref::<&str>().copied())
            .unwrap_or("");
        assert!(text.contains(expected), "{text}");
    }
}
