use super::*;
use acyclic_test_support::{AllocationInfo, measure};
use proptest::prelude::*;
use proptest::test_runner::TestCaseError;
use std::cell::Cell;

#[tokio::test]
async fn creation_retains_complete_name_backing_on_original_accounting()
-> Result<(), Box<dyn std::error::Error>> {
    use crate::workspace::accounting_tests::ObservedAuthority;

    crate::initialize_test_native_io();
    let authority = Arc::new(ObservedAuthority::default());
    let fs = crate::Fs::new(
        authority.clone(),
        Arc::new(crate::MemoryObjectStore::default()),
        crate::EmbeddedCapabilities::MEMORY,
    );
    let budget = crate::performance::BOUNDARY_BUDGET;
    let header = crate::arc_layout::allocation_bytes(std::alloc::Layout::new::<String>())? as u64
        + crate::arc_layout::allocation_bytes(std::alloc::Layout::new::<
            crate::kernel::RetainedAllocation,
        >())? as u64;
    // Exercise the public ordinary entry point: its caller frame is already
    // gone when the name is returned. Every alias must keep the same backing.
    let cancellation = CancellationToken::new();
    let receipt = WorkspaceName::new("retained-name", &budget, &cancellation)?;
    let name = receipt.value;
    let live = name
        .0
        .standalone_live_bytes()
        .ok_or("workspace name lost its original ordinary ledger")?;
    assert_eq!(
        live, receipt.work.peak_allocation_bytes,
        "workspace name lost original backing credit"
    );
    let alias = name.clone();
    drop(name);
    assert_eq!(
        alias.0.standalone_live_bytes(),
        Some(live),
        "workspace name alias lost original backing credit"
    );
    assert_eq!(alias.as_str(), "retained-name");
    drop(alias);
    for (input, canonical) in [
        ("original-name", "original-name"),
        ("cafe\u{301}", "caf\u{e9}"),
    ] {
        let cancellation = CancellationToken::new();
        *authority.replay_minimum.lock().unwrap() = Some(canonical.len() as u64 + header);
        let created = fs
            .create_workspace_with_operation::<_, (), WorkspaceError>(
                input,
                crate::VolumeInitialization::new(
                    crate::VolumeConfig::portable(crate::Lifecycle::Ephemeral),
                    None,
                ),
                crate::OperationId::new(),
                budget,
                &cancellation,
                None,
            )
            .await?;
        assert_eq!(created.value.workspace().name().as_str(), canonical);
        created.work.verify(budget)?;
        assert!(authority.replay_minimum.lock().unwrap().is_none());
    }

    let cancellation = CancellationToken::new();
    let original =
        crate::NativeMaterializationWork::new(budget, &cancellation).map_err(|(error, _)| error)?;
    let name = "native-original-name";
    *authority.replay_minimum.lock().unwrap() =
        Some(original.live_allocation_bytes() + name.len() as u64 + header);
    let created = fs
        .create_workspace_with_operation::<_, (), WorkspaceError>(
            name,
            crate::VolumeInitialization::new(
                crate::VolumeConfig::portable(crate::Lifecycle::Ephemeral),
                None,
            ),
            crate::OperationId::new(),
            &original,
            &cancellation,
            None,
        )
        .await?;
    assert_eq!(created.workspace().name().as_str(), name);
    assert!(authority.replay_minimum.lock().unwrap().is_none());
    original.receipt().verify(budget)?;

    // The String may be admitted, but its shared header must be admitted too
    // before any durable observation or publication is dispatched.
    *authority.replay_minimum.lock().unwrap() = Some(0);
    let failure = fs
        .create_workspace(
            "refused-name",
            WorkBudget {
                allocation_operations: 1,
                ..budget
            },
            &cancellation,
        )
        .await
        .err()
        .ok_or("name header escaped its allocation allowance")?;
    assert!(matches!(
        failure.error,
        WorkspaceError::Work(crate::WorkError::BudgetExceeded {
            counter: "allocation_operations",
            ..
        })
    ));
    assert_eq!(failure.work.backend_read_operations, 0);
    assert!(authority.replay_minimum.lock().unwrap().is_some());

    cancellation.cancel();
    let failure = fs
        .create_workspace("cancelled-name", budget, &cancellation)
        .await
        .err()
        .ok_or("cancelled identity reached authority storage")?;
    assert!(matches!(failure.error, WorkspaceError::Cancelled(_)));
    assert_eq!(*failure.work, crate::WorkCounters::UNCHARGED);
    assert!(authority.replay_minimum.lock().unwrap().is_some());
    Ok(())
}

#[test]
fn cancellation_after_encoding_precedes_length_errors_and_retention() {
    for (count, skip) in [(127, false), (128, false), (128, true)] {
        let text = "é".repeat(count);
        let cancellation = CancellationToken::new();
        let mut accounting = KernelAccounting::standalone(WorkBudget::UNBOUNDED, &cancellation);
        let mut copied = 0;
        SKIP_LENGTH_CANCELLATION.with(|flag| flag.set(skip));
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
        SKIP_LENGTH_CANCELLATION.with(|flag| flag.set(false));
        assert_eq!(
            matches!(result, Err(WorkspaceError::Cancelled(_))),
            !skip,
            "{result:?}"
        );
        if skip {
            assert!(matches!(
                result,
                Err(WorkspaceError::Name(WorkspaceNameError::TooLong))
            ));
        }
    }
}

std::thread_local! {
    static EXTRA_ALLOCATION: Cell<bool> = const { Cell::new(false) };
    static SKIP_LENGTH_CANCELLATION: Cell<bool> = const { Cell::new(false) };
}
pub(super) fn skip_length_cancellation() -> bool {
    SKIP_LENGTH_CANCELLATION.with(Cell::get)
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
    let [
        ((expected_output, expected_work, expected_heap), expected_trace),
        ((actual_output, actual_work, actual_heap), actual_trace),
    ] = acyclic_test_support::differential::pair::<_, _, (u8, u64, u64)>(
        || (),
        |(), baseline| {
            let (output, work, trace, heap) = run(
                name,
                quota,
                mode,
                recheck,
                failure,
                if baseline { 1 } else { variant },
            );
            for event in trace {
                acyclic_test_support::differential::record(event);
            }
            (output, work, heap)
        },
    );
    if actual_output != expected_output {
        return Err("output or exact error");
    }
    if actual_trace != expected_trace {
        return Err("actual operation trace");
    }
    if actual_work != expected_work {
        return Err("full receipt");
    }
    if actual_heap != expected_heap {
        return Err("physical allocation statistics");
    }
    // The untouched baseline must never succeed where the candidate rejects.
    let old = run(name, quota, mode, recheck, failure, 2);
    if old.0.is_ok() && actual_output != old.0 {
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
        text in prop_oneof![
            prop::collection::vec(any::<char>(), 0..300).prop_map(|c| c.into_iter().collect::<String>()),
            "[a-zA-Z0-9_-]{1,255}",
        ],
        items in 0_u64..1025, copies in 0_u64..1025,
        operations in 0_u64..5, peak in 0_u64..4097,
        mode in 0_u8..4, recheck in any::<bool>(), failure in 0_u8..6,
    ) {
        name_fuzz::check_workspace_name(text.as_bytes());
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
    let cancellation = CancellationToken::new();
    let original =
        crate::NativeMaterializationWork::new(WorkBudget::UNBOUNDED, &cancellation).unwrap();
    let admission = crate::kernel::allocation::retained::RetainedArc::<String>::admit(
        &mut KernelAccounting::native(&original, &cancellation),
    )
    .unwrap();
    let mut control = None;
    let heap = measure(|| control = Some(admission.into_value(String::new())));
    drop(control);
    assert_eq!(heap.count_total, 2);
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
        count_total: 3,
        count_current: 3,
        count_max: 3,
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
                allocation_operations: 3,
                peak_allocation_bytes: baseline + bytes,
                ..crate::WorkCounters::UNCHARGED
            })
            .unwrap()
    );
    let mut alias = None;
    let before_clone = original.receipt();
    let cloned = measure(|| alias = value.as_ref().cloned());
    assert_eq!(cloned, AllocationInfo::default());
    assert_eq!(original.receipt(), before_clone);
    let first_drop = measure(|| drop(value.take()));
    assert_eq!(first_drop, AllocationInfo::default());
    assert_eq!(original.live_allocation_bytes(), baseline + bytes);
    let freed = measure(|| drop(alias.take()));
    assert_eq!(freed.count_total, 0);
    assert_eq!(freed.count_current, -3);
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

fn complete_name_observation(
    text: &str,
    mode: u8,
    recheck: bool,
    baseline: bool,
    redundant: Option<u8>,
) -> ((String, crate::WorkCounters, u64, AllocationInfo), usize) {
    let cancellation = CancellationToken::new();
    let (root, work) = fixture(mode, WorkBudget::UNBOUNDED, &cancellation);
    let work = if recheck {
        work.recheck_admitted()
    } else {
        work
    };
    let mut accounting = KernelAccounting::native(&work, &cancellation);
    accounting
        .record_completed(crate::WorkCounters::UNCHARGED)
        .unwrap();
    let mut output = None;
    let mut locks = 0;
    let mut injected = false;
    let heap = measure(|| {
        let mut probe = |tag, _, _| {
            if redundant == Some(tag) && !injected {
                // Encoding and append have released their charge's leaf guard.
                work.admit(crate::WorkCounters::UNCHARGED).unwrap();
                injected = true;
            }
        };
        let (result, count) = root.state.count_locks(|| {
            if baseline {
                reference::decode(text, &mut accounting, false, &mut probe)
            } else {
                WorkspaceName::with_probe(text, &mut accounting, char::is_ascii, &mut probe)
            }
        });
        output = Some(result);
        locks = count;
    });
    assert_eq!(injected, redundant.is_some());
    let name = output.unwrap().unwrap();
    let observation = (
        name.as_str().to_owned(),
        accounting.spent(),
        work.live_allocation_bytes(),
        heap,
    );
    drop(name);
    (observation, locks)
}

fn check_complete_name_leaf_pin(text: &str, mode: u8, recheck: bool) {
    let baseline = complete_name_observation(text, mode, recheck, true, None);
    let candidate = complete_name_observation(text, mode, recheck, false, None);
    assert_eq!(baseline.0.0, candidate.0.0);
    assert_eq!(baseline.0.3, candidate.0.3);
    let normalized = unicode_normalization::UnicodeNormalization::nfc(text).count();
    let decomposed = unicode_normalization::UnicodeNormalization::nfd(text).count();
    // Old: twelve fixed boundaries, one per input/decomposition visit, and
    // five per normalized scalar (check, admission, two record guards, validate).
    assert_eq!(
        baseline.1,
        12 + text.chars().count() + decomposed + 5 * normalized
    );
    // New: one planning guard. Unicode retains one charge and one validation
    // per normalized scalar; ASCII omits normalization and its terminal check.
    let expected = if text.is_ascii() {
        11
    } else {
        12 + 2 * normalized
    };
    let pin = |observed| {
        if observed == expected {
            Ok(())
        } else {
            Err("complete-name leaf bound")
        }
    };
    assert_eq!(pin(candidate.1), Ok(()));
    for tag in [2, 4] {
        if tag == 2 && text.is_ascii() {
            continue;
        }
        let control = complete_name_observation(text, mode, recheck, false, Some(tag));
        assert_eq!(
            candidate.0, control.0,
            "admission changed output, work, storage or heap"
        );
        assert_eq!(control.1, expected + 1);
        assert_eq!(pin(control.1), Err("complete-name leaf bound"));
    }
}

#[test]
fn complete_names_pin_encoding_and_append_and_reject_redundant_admission() {
    let wide = "a".repeat(255);
    for mode in 1..5 {
        for recheck in [false, true] {
            for text in ["n", "private", "cafe\u{301}", "\u{e9}", "\u{ac01}", &wide] {
                check_complete_name_leaf_pin(text, mode, recheck);
            }
        }
    }
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(32))]
    // Every valid generated name has exactly the closed-form boundary count;
    // an extra admission at either actual copy effect preserves every other
    // observation while failing that same count.
    #[test]
    fn complete_name_boundaries_follow_scalar_counts(
        scalars in proptest::collection::vec(
            prop_oneof![Just('a'), Just('\u{e9}'), Just('\u{301}'),
                        Just('\u{1100}'), Just('\u{1161}'), Just('\u{11a8}')], 1..33),
        mode in 1u8..5,
        recheck in any::<bool>(),
    ) {
        let text: String = scalars.into_iter().collect();
        check_complete_name_leaf_pin(&text, mode, recheck);
    }
}
