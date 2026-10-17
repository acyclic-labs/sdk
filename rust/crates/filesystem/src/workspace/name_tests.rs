use super::*;
use acyclic_test_support::{AllocationInfo, measure};
use proptest::prelude::*;
use proptest::test_runner::TestCaseError;
use std::cell::Cell;

#[test]
fn cancellation_after_encoding_precedes_length_errors_and_retention() {
    for text in ["é".repeat(127), "é".repeat(128)] {
        let cancellation = CancellationToken::new();
        let mut accounting = KernelAccounting::standalone(WorkBudget::UNBOUNDED, &cancellation);
        let mut copied = 0;
        let result = WorkspaceName::with_probe(
            &text,
            &mut accounting,
            char::is_ascii,
            &mut |tag, _, bytes| {
                if tag == 2 {
                    copied += bytes;
                    if copied == 254 {
                        cancellation.cancel();
                    }
                }
            },
        );
        assert!(
            matches!(result, Err(WorkspaceError::Cancelled(_))),
            "{result:?}"
        );
    }
}

std::thread_local! {
    static EXTRA_ALLOCATION: Cell<bool> = const { Cell::new(false) };
}
pub(super) fn redundant_allocation() {
    if EXTRA_ALLOCATION.with(Cell::get) {
        drop(std::hint::black_box(Box::new([0_u8; 17])));
    }
}

fn fixture(
    mode: u8,
    quota: WorkBudget,
    cancellation: &CancellationToken,
) -> (
    crate::NativeMaterializationWork,
    crate::NativeMaterializationWork,
) {
    let mut root_limit = quota;
    // Fund only the standing original ledger before applying the generated
    // name allowance. Resource/invocation setup uses its unbounded parent.
    root_limit.allocation_operations = root_limit.allocation_operations.saturating_add(1);
    root_limit.peak_allocation_bytes = root_limit
        .peak_allocation_bytes
        .saturating_add(crate::NativeMaterializationWork::accounting_state_bytes());
    let root = crate::NativeMaterializationWork::new(
        if mode == 1 {
            root_limit
        } else {
            WorkBudget::UNBOUNDED
        },
        cancellation,
    )
    .unwrap();
    let work = match mode {
        2 => root.native_volume_work(quota).unwrap(),
        3 => root
            .invocation_view()
            .unwrap()
            .0
            .native_volume_work(quota)
            .unwrap(),
        _ => root.clone(),
    };
    (root, work)
}

fn run(
    name: &str,
    quota: WorkBudget,
    mode: u8,
    recheck: bool,
    failure: u8,
    variant: u8,
) -> (
    Result<String, String>,
    crate::WorkCounters,
    Vec<(u8, u64, u64)>,
    AllocationInfo,
) {
    let root_cancel = CancellationToken::new();
    let caller = CancellationToken::new();
    let (_root, work) = fixture(mode, quota, &root_cancel);
    let work = if recheck {
        work.recheck_admitted()
    } else {
        work
    };
    let mut accounting = if mode == 0 {
        KernelAccounting::standalone(quota, &caller)
    } else {
        KernelAccounting::native(&work, &caller)
    };
    // Initialize debug checker TLS before observing the physical heap.
    let _ = accounting.record_completed(crate::WorkCounters::UNCHARGED);
    match failure {
        1 => caller.cancel(),
        2 if mode != 0 => root_cancel.cancel(),
        3 if mode != 0 => work.state(|state| state.poison_native_original_work()),
        4 if mode != 0 => {
            root_cancel.cancel();
            caller.cancel();
        }
        5 if mode != 0 => {
            work.state(|state| state.poison_native_original_work());
            caller.cancel();
        }
        _ => (),
    }
    let mut trace = Vec::with_capacity(8192);
    let mut output = None;
    let heap = measure(|| {
        let mut probe = |tag, value, bytes| trace.push((tag, value, bytes));
        output = Some(match variant {
            1 => reference::decode(name, &mut accounting, true, &mut probe),
            2 => reference::decode(name, &mut accounting, false, &mut probe),
            3 => WorkspaceName::with_probe(name, &mut accounting, |_| false, &mut probe),
            4 => WorkspaceName::with_probe(name, &mut accounting, |_| true, &mut probe),
            _ => WorkspaceName::with_probe(name, &mut accounting, char::is_ascii, &mut probe),
        });
    });
    let output = output
        .unwrap()
        .map(|name| name.as_str().to_owned())
        .map_err(|error| format!("{error:?}"));
    (output, accounting.spent(), trace, heap)
}

fn compare(
    name: &str,
    quota: WorkBudget,
    mode: u8,
    recheck: bool,
    failure: u8,
    variant: u8,
) -> Result<(), &'static str> {
    let actual = run(name, quota, mode, recheck, failure, variant);
    let expected = run(name, quota, mode, recheck, failure, 1);
    if actual.0 != expected.0 {
        return Err("output or exact error");
    }
    if actual.2 != expected.2 {
        return Err("actual operation trace");
    }
    if actual.1 != expected.1 {
        return Err("full receipt");
    }
    if actual.3 != expected.3 {
        return Err("physical allocation statistics");
    }
    // The untouched baseline must never succeed where the candidate rejects.
    let old = run(name, quota, mode, recheck, failure, 2);
    if old.0.is_ok() && actual.0 != old.0 {
        return Err("baseline admission guarantee");
    }
    Ok(())
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(128))]

    // Independently frozen parsing plus explicit projection of eliminated ASCII
    // decomposition/buffering. Every finite failure prefix has exact work and
    // visit/copy traces; compare against raw main as an admission guarantee.
    #[test]
    fn independent_parser_matches_outputs_errors_and_every_observed_phase(
        text in prop::collection::vec(any::<char>(), 0..300).prop_map(|c| c.into_iter().collect::<String>()),
        items in 0_u64..1025, copies in 0_u64..1025,
        operations in 0_u64..5, peak in 0_u64..4097,
        mode in 0_u8..4, recheck in any::<bool>(), failure in 0_u8..6,
    ) {
        let quota = WorkBudget { items_examined: items, bytes_copied: copies,
            allocation_operations: operations, peak_allocation_bytes: peak,
            ..WorkBudget::UNBOUNDED };
        compare(&text, quota, mode, recheck, failure, 0).map_err(|reason|
            TestCaseError::fail(format!("{reason}: {text:?}, mode={mode}, quota={quota:?}")))?;
        compare(&text, WorkBudget::UNBOUNDED, mode, recheck, 0, 0)
            .map_err(TestCaseError::fail)?;
    }

    #[test]
    fn generated_ascii_failure_prefixes_keep_exact_receipts_and_visits(
        text in prop_oneof![
            "[a-zA-Z0-9_-]{0,270}",
            prop::collection::vec(0_u8..128, 0..271)
                .prop_map(|bytes| String::from_utf8(bytes).unwrap()),
        ],
        items in 0_u64..541, copies in 0_u64..541, mode in 0_u8..4,
        recheck in any::<bool>(), failure in 0_u8..6,
    ) {
        compare(&text, WorkBudget { items_examined: items, bytes_copied: copies,
            ..WorkBudget::UNBOUNDED }, mode, recheck, failure, 0).map_err(TestCaseError::fail)?;
    }
}

fn control_bytes() -> u64 {
    let mut control = None;
    let heap =
        measure(|| control = Some(Arc::new((String::new(), KernelOwnedAllocation::Standalone))));
    drop(control);
    assert_eq!(heap.count_total, 1);
    heap.bytes_total
}

fn physical_bound(length: usize, broken: bool) -> Result<(), &'static str> {
    let name = "n".repeat(length);
    let control = control_bytes();
    let cancellation = CancellationToken::new();
    let original =
        crate::NativeMaterializationWork::new(WorkBudget::UNBOUNDED, &cancellation).unwrap();
    let baseline = original.live_allocation_bytes();
    let before = original.receipt();
    struct Restore(bool);
    impl Drop for Restore {
        fn drop(&mut self) {
            EXTRA_ALLOCATION.with(|flag| flag.set(self.0));
        }
    }
    let restore = Restore(EXTRA_ALLOCATION.with(|flag| flag.replace(broken)));
    let mut value = None;
    // Observe the ordinary public entry point with its zero-sized no-op probe.
    let heap =
        measure(|| value = Some(WorkspaceName::new(&name, &&original, &cancellation).unwrap()));
    drop(restore);
    let bytes = length as u64 + control;
    let expected = AllocationInfo {
        count_total: 2,
        count_current: 2,
        count_max: 2,
        bytes_total: bytes,
        bytes_current: bytes as i64,
        bytes_max: bytes,
    };
    if heap != expected {
        return Err("physical allocation bound");
    }
    assert_eq!(value.as_ref().unwrap().as_str(), name);
    assert_eq!(original.live_allocation_bytes(), baseline + bytes);
    assert_eq!(
        original.receipt(),
        before
            .checked_add(crate::WorkCounters {
                items_examined: length as u64,
                bytes_copied: length as u64,
                allocation_operations: 2,
                peak_allocation_bytes: baseline + bytes,
                ..crate::WorkCounters::UNCHARGED
            })
            .unwrap()
    );
    let freed = measure(|| drop(value.take()));
    assert_eq!(freed.count_total, 0);
    assert_eq!(freed.count_current, -2);
    assert_eq!(freed.bytes_current, -(bytes as i64));
    assert_eq!(original.live_allocation_bytes(), baseline);
    Ok(())
}

#[test]
fn ascii_names_have_exact_physical_storage_and_a_temporary_allocation_is_detected() {
    for length in [1, 7, 64, 255] {
        physical_bound(length, false).unwrap();
    }
    assert_eq!(physical_bound(7, true), Err("physical allocation bound"));
}

#[test]
fn redundant_normalization_and_missing_unicode_normalization_fail_the_same_oracle() {
    compare("private", WorkBudget::UNBOUNDED, 1, false, 0, 0).unwrap();
    assert_eq!(
        compare("private", WorkBudget::UNBOUNDED, 1, false, 0, 3),
        Err("actual operation trace")
    );
    assert_eq!(
        compare("cafe\u{301}", WorkBudget::UNBOUNDED, 1, false, 0, 4),
        Err("output or exact error")
    );
}

#[test]
fn every_boundary_budget_matches_the_independent_failure_prefix() {
    for text in [
        "",
        ".",
        "..",
        "ab",
        "a/b",
        "a\0b",
        "cafe\u{301}",
        "a\u{315}\u{300}\u{344}",
    ] {
        for mode in 0..4 {
            for items in 0..17 {
                for copies in 0..17 {
                    compare(
                        text,
                        WorkBudget {
                            items_examined: items,
                            bytes_copied: copies,
                            ..WorkBudget::UNBOUNDED
                        },
                        mode,
                        false,
                        0,
                        0,
                    )
                    .unwrap();
                }
            }
        }
    }
}

#[test]
fn one_original_leaf_guard_covers_the_entire_planning_pass() {
    let cancellation = CancellationToken::new();
    for mode in 1..4 {
        let (root, work) = fixture(mode, WorkBudget::UNBOUNDED, &cancellation);
        for text in ["n", "private", "cafe\u{301}", "a\u{315}\u{300}\u{344}"] {
            let mut accounting = KernelAccounting::native(&work, &cancellation);
            let (result, locks) = root.state.count_locks(|| {
                plan::inspect(text, &mut accounting, char::is_ascii, &mut |_, _, _| {})
            });
            result.unwrap();
            assert_eq!(locks, 1);
            let (result, locks) = root.state.count_locks(|| {
                accounting.check()?;
                plan::inspect(text, &mut accounting, char::is_ascii, &mut |_, _, _| {})
            });
            result.unwrap();
            assert_ne!(locks, 1, "a redundant uncharged lock must be observed");
        }
    }
}
