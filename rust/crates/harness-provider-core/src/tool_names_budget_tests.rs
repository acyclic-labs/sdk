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
        acyclic_fs_work::performance::WorkBudget {
            bytes_hashed: maximum,
            ..acyclic_fs_work::performance::WorkBudget::UNBOUNDED
        },
        &cancellation,
    )
    .unwrap();
    ACTUAL_HASHED.with(|counter| counter.set(0));
    let result = Names::funded(request, Some(&work));
    assert_eq!(result.is_ok(), succeeds, "digest admission outcome");
    let actual = ACTUAL_HASHED.with(std::cell::Cell::get);
    assert_eq!(
        work.receipt().unwrap().bytes_hashed,
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
        let mut names = crate::fixtures::adversarial(taken);
        names.push(long.clone());
        let request = crate::fixtures::request_with(&names.iter().map(String::as_str).collect::<Vec<_>>());
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
    SKIP_HASH_ADMISSION.with(|flag| flag.set(true));
    let detected = std::panic::catch_unwind(|| {
        let request = crate::fixtures::request_with(&[&long]);
        funded(&request, 0, 116, true);
    });
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
        crate::fixtures::request_with(&names.iter().map(String::as_str).collect::<Vec<_>>());
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

#[test]
fn aliases_are_exactly_sized_and_the_build_matches_its_claim() {
    let long = format!("mcp.server.{}", "t".repeat(80));
    let request = crate::fixtures::request_with(&[&long]);
    let (names, built) = Names::built(&request);
    let aliases: Vec<_> = names.aliases().collect();
    assert_eq!(aliases, [(64, 64)], "digest alias capacity is exact");
    // One 40-byte binding, the 64-byte digest buffer, the 64-byte alias,
    // and one wire index entry: no growth and no uncounted temporary.
    #[cfg(target_pointer_width = "64")]
    assert_eq!(
        built,
        crate::tool_names::Built {
            allocations: 4,
            bytes: 40 + 64 + 64 + 8,
            probes: 0,
            hashed: 116,
            wire_sorts: 1,
            wire_comparisons: 0,
        }
    );
    assert!(built.within(&Names::claim(1).unwrap()));
    // A readable alias needs neither the digest buffer nor any slack.
    let request = crate::fixtures::request_with(&["mcp.docs.search", "read"]);
    let (names, built) = Names::built(&request);
    assert_eq!(names.aliases().collect::<Vec<_>>(), [(17, 17)]);
    assert_eq!(built.allocations, 3);
    assert!(built.within(&Names::claim(2).unwrap()));
    for size in 0..64 {
        let names: Vec<String> = (0..size)
            .map(|index| format!("mcp.s{index}.{}", "x".repeat(index * 3)))
            .collect();
        let request =
            crate::fixtures::request_with(&names.iter().map(String::as_str).collect::<Vec<_>>());
        let (table, built) = Names::built(&request);
        assert!(table.aliases().all(|(length, capacity)| length == capacity));
        assert!(built.within(&Names::claim(size as u64).unwrap()));
    }
}

#[test]
fn negative_control_a_grown_alias_retains_slack_beyond_its_claim() {
    // The former builder reserved the readable prefix, then appended the digest.
    let mut grown = String::with_capacity(47);
    grown.push_str(&"t".repeat(47));
    grown.push_str(&"0".repeat(17));
    assert!(grown.capacity() > grown.len(), "growth left no slack");
    assert!(
        grown.capacity() > 64,
        "the per-alias claim would hold slack"
    );
}
