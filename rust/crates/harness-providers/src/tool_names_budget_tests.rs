use super::*;
use proptest::prelude::*;

thread_local! {
    pub(super) static SKIP_HASH_ADMISSION: std::cell::Cell<bool> = const { std::cell::Cell::new(false) };
    pub(super) static REDUNDANT_WIRE_SORT: std::cell::Cell<bool> = const { std::cell::Cell::new(false) };
    pub(super) static ACTUAL_HASHED: std::cell::Cell<u64> = const { std::cell::Cell::new(0) };
}

#[cfg(not(target_arch = "wasm32"))]
fn funded(request: &ModelRequest, maximum: u64, expected: u64, succeeds: bool) {
    let cancellation = acyclic_fs::CancellationToken::new();
    let work = acyclic_fs::NativeMaterializationWork::new(
        acyclic_fs::WorkBudget {
            bytes_hashed: maximum,
            ..acyclic_fs::WorkBudget::UNBOUNDED
        },
        &cancellation,
    )
    .unwrap();
    ACTUAL_HASHED.with(|counter| counter.set(0));
    let result = Names::funded(request, Some(&work));
    assert_eq!(result.is_ok(), succeeds, "digest admission outcome");
    let actual = ACTUAL_HASHED.with(std::cell::Cell::get);
    assert_eq!(
        work.receipt().bytes_hashed,
        actual,
        "every hash must be funded first"
    );
    assert!(actual <= maximum, "denial must precede hashing");
    if succeeds {
        assert_eq!(
            actual, expected,
            "exact digest bytes including domain and salt"
        );
    }
}

#[cfg(not(target_arch = "wasm32"))]
proptest! {
    #![proptest_config(ProptestConfig::with_cases(32))]
    #[test]
    fn digest_attempts_are_funded_exactly_before_hashing(length in 65usize..180, taken in 0u32..8) {
        let long = format!("mcp.server.{}", "t".repeat(length));
        let mut names = crate::tool_name_tests::adversarial(taken);
        names.push(long.clone());
        let request = crate::tool_name_tests::request_with(&names.iter().map(String::as_str).collect::<Vec<_>>());
        // Independent byte count for the literal digest framing and salt.
        let expected = 21 + 4 + long.len() as u64 + u64::from(taken + 1) * (21 + 4 + 7);
        funded(&request, expected, expected, true);
        funded(&request, expected - 1, expected, false);
        funded(&request, 0, expected, false);
    }
}

#[cfg(not(target_arch = "wasm32"))]
#[test]
fn negative_control_detects_unfunded_production_digest() {
    let long = format!("mcp.server.{}", "t".repeat(80));
    let request = crate::tool_name_tests::request_with(&[&long]);
    SKIP_HASH_ADMISSION.with(|flag| flag.set(true));
    let detected = std::panic::catch_unwind(|| funded(&request, 0, 116, true));
    SKIP_HASH_ADMISSION.with(|flag| flag.set(false));
    let panic = detected.expect_err("unfunded production hashing must fail the ledger oracle");
    assert!(
        panic
            .downcast_ref::<String>()
            .unwrap()
            .contains("every hash must be funded first")
    );
}

fn collision_free(names: &[String]) {
    let request =
        crate::tool_name_tests::request_with(&names.iter().map(String::as_str).collect::<Vec<_>>());
    let (_, built) = Names::built(&request);
    assert_eq!(
        built.wire_sorts,
        u64::from(!names.is_empty()),
        "one wire sort for unchanged bindings"
    );
    if names.len() > 1 {
        assert!(
            built.wire_comparisons > 0,
            "count the production comparator"
        );
    }
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(32))]
    #[test]
    fn unchanged_bindings_have_one_wire_sort(count in 0usize..80) {
        collision_free(&(0..count).map(|index| format!("mcp.server.tool{index}")).collect::<Vec<_>>());
    }
}

#[test]
fn negative_control_detects_redundant_production_sort() {
    REDUNDANT_WIRE_SORT.with(|flag| flag.set(true));
    let detected = std::panic::catch_unwind(|| collision_free(&["mcp.a".into(), "mcp.b".into()]));
    REDUNDANT_WIRE_SORT.with(|flag| flag.set(false));
    let panic = detected.expect_err("redundant production sort must fail the same work oracle");
    assert!(
        panic
            .downcast_ref::<String>()
            .unwrap()
            .contains("one wire sort for unchanged bindings")
    );
}
