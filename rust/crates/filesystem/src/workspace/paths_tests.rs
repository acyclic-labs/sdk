use super::*;
use acyclic_test_support::{AllocationInfo, measure};
use proptest::prelude::*;

// Frozen main restore sequence (cancel check, admit, allocate, copy, record),
// initialized on a separate ledger. Single-threaded it is the reference for
// both callers; the old label includes its redundant second admit.
fn old(
    prefix: &str,
    text: &str,
    accounting: &mut KernelAccounting<'_>,
    cancellation: Option<&CancellationToken>,
) -> Result<crate::kernel::Owned<String>, WorkspaceError> {
    if let Some(cancellation) = cancellation {
        cancellation.check()?;
    }
    let length = prefix.len() + text.len();
    let copied = copy_work(length as u64);
    accounting.admit(copied)?;
    let mut path = crate::kernel::Owned::<Vec<u8>>::allocate(length, accounting)?;
    if cancellation.is_none() {
        accounting.admit(copied)?;
    }
    path.value.extend_from_slice(prefix.as_bytes());
    path.value.extend_from_slice(text.as_bytes());
    observed_copy(path.value.len() as u64);
    let path = crate::kernel::Owned::new(
        String::from_utf8(path.value).map_err(WorkspaceError::engine)?,
        path.allocation,
        path.bytes,
    );
    if let Err(error) = accounting.record_completed(copied) {
        path.retire(accounting)?;
        return Err(error.into());
    }
    Ok(path)
}

// A label copies a prebuilt name; a restore copies "/" plus the text.
fn candidate(
    name: Option<&WorkspaceName>,
    text: &str,
    accounting: &mut KernelAccounting<'_>,
    cancellation: &CancellationToken,
) -> Result<crate::kernel::Owned<String>, WorkspaceError> {
    match name {
        Some(name) => copy_workspace_label(name, accounting),
        None => restore_absolute_path(text, accounting, cancellation),
    }
}

fn name(text: &str) -> Option<WorkspaceName> {
    WorkspaceName::new(
        text,
        &crate::WorkBudget::UNBOUNDED,
        &CancellationToken::new(),
    )
    .ok()
    .map(|name| name.value)
}

fn live(accounting: &mut KernelAccounting<'_>, root: &crate::NativeMaterializationWork) -> u64 {
    match accounting.standalone_parts() {
        Some((allocations, ..)) => allocations.live_bytes(),
        None => root.live_allocation_bytes(),
    }
}

// Native setup storage, discovered on an independent unbounded chain, so the
// generated allocation allowance is what remains for the copy itself.
fn setup_peak(mode: u8, recheck: bool) -> u64 {
    if mode == 0 {
        return 0;
    }
    let cancellation = CancellationToken::new();
    let root =
        crate::NativeMaterializationWork::new(crate::WorkBudget::UNBOUNDED, &cancellation).unwrap();
    let mut work = root.clone();
    for _ in 0..mode.saturating_sub(1).min(2) {
        work = work
            .native_volume_work(crate::WorkBudget::UNBOUNDED)
            .unwrap();
    }
    if mode == 4 {
        work = work.invocation_view().unwrap().0;
    }
    if recheck {
        work = work.recheck_admitted();
    }
    let peak = root.live_allocation_bytes();
    drop(work);
    peak
}

fn measured<T>(operation: impl FnOnce() -> T) -> (T, AllocationInfo) {
    let mut output = None;
    let heap = measure(|| output = Some(operation()));
    (output.unwrap(), heap)
}

fn observed<T>(operation: impl FnOnce() -> T) -> ((T, AllocationInfo), Vec<u64>) {
    acyclic_test_support::differential::observe(
        || ACTUAL_COPY.with(|copied| copied.set(None)),
        |()| {
            let result = measured(operation);
            // Publish outside heap measurement: the observer's own Vec is
            // independent of the allocation being checked.
            if let Some(copied) = ACTUAL_COPY.with(|copied| copied.take()) {
                acyclic_test_support::differential::record(copied);
            }
            result
        },
    )
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(128))]

    // Output, exact errors, every receipt field, live storage and heap match
    // the old path under copy/allocation limits, prior work, cancellation,
    // poison and nested policies; no copy credit stays reserved.
    #[test]
    fn reserved_copies_preserve_output_errors_receipts_storage_and_heap(
        text in "[a-z0-9]{1,24}", restore in any::<bool>(),
        copy_limit in 0_u64..32, allocation_limit in 0_u64..48,
        prior_copy in 0_u64..16, mode in 0_u8..5, recheck in any::<bool>(),
        failure in 0_u8..4,
    ) {
        let prefix = if restore { "/" } else { "" };
        let label = if restore { None } else { name(&text) };
        prop_assume!(restore || label.as_ref().is_some_and(|name| name.as_str() == text));
        let limit = crate::WorkBudget {
            bytes_copied: copy_limit,
            peak_allocation_bytes: setup_peak(mode, recheck) + allocation_limit,
            ..crate::WorkBudget::UNBOUNDED
        };
        let roots_cancel = [CancellationToken::new(), CancellationToken::new()];
        let callers = [CancellationToken::new(), CancellationToken::new()];
        let roots = [0, 1].map(|i| crate::NativeMaterializationWork::new(
            if mode == 1 { limit } else { crate::WorkBudget::UNBOUNDED }, &roots_cancel[i],
        ).unwrap());
        let work = roots.each_ref().map(|root| {
            let mut work = root.clone();
            for _ in 0..mode.saturating_sub(1).min(2) { work = work.native_volume_work(limit).unwrap(); }
            if mode == 4 { work = work.invocation_view().unwrap().0; }
            if recheck { work = work.recheck_admitted(); }
            work
        });
        let mut accounting = [0, 1].map(|i| if mode == 0 {
            KernelAccounting::standalone(limit, &callers[i])
        } else { KernelAccounting::native(&work[i], &callers[i]) });
        for i in [0, 1] {
            let _ = accounting[i].record_completed(copy_work(prior_copy));
            match failure {
                1 => callers[i].cancel(),
                2 if mode != 0 => roots_cancel[i].cancel(),
                3 if mode != 0 => work[i].state(|state| state.poison_native_original_work()),
                _ => (),
            }
        }
        let live_before = [live(&mut accounting[0], &roots[0]), live(&mut accounting[1], &roots[1])];
        let ((before, old_heap), old_trace) = observed(|| {
            old(prefix, &text, &mut accounting[0], restore.then_some(&callers[0]))
        });
        let ((after, new_heap), new_trace) = observed(|| {
            candidate(label.as_ref(), &text, &mut accounting[1], &callers[1])
        });
        prop_assert_eq!(new_heap, old_heap);
        prop_assert_eq!(new_trace, old_trace);
        let expected = format!("{prefix}{text}");
        if after.is_ok() {
            // One exactly sized backing allocation and nothing else.
            prop_assert_eq!(new_heap.count_total, 1);
            prop_assert_eq!(new_heap.bytes_total, expected.len() as u64);
        }
        prop_assert_eq!(accounting[1].spent(), accounting[0].spent());
        let held = [live(&mut accounting[0], &roots[0]), live(&mut accounting[1], &roots[1])];
        prop_assert_eq!(held[1] - live_before[1], held[0] - live_before[0]);
        let outcome = |result: Result<crate::kernel::Owned<String>, WorkspaceError>,
                       accounting: &mut KernelAccounting<'_>| match result {
            Ok(owned) => {
                let observed = (owned.value.clone(), owned.bytes);
                owned.retire(accounting).unwrap();
                Ok(observed)
            }
            Err(error) => Err(format!("{error:?}")),
        };
        let before = outcome(before, &mut accounting[0]);
        let after = outcome(after, &mut accounting[1]);
        if let Ok((value, bytes)) = &after {
            prop_assert_eq!(value, &expected);
            prop_assert_eq!(*bytes, expected.len() as u64);
            prop_assert_eq!(accounting[1].spent().bytes_copied, prior_copy + expected.len() as u64);
        }
        prop_assert_eq!(after, before);
        prop_assert_eq!(live(&mut accounting[1], &roots[1]), live_before[1]);
        let credit = work[1].state(|state| (state.budget.bytes_copied, state.original_budget().bytes_copied));
        prop_assert_eq!(credit.0, credit.1);
    }
}

// The copy limit still wins over an exhausted allocation limit: the preflight
// runs before storage is claimed, so neither limit's work is recorded.
#[test]
fn copy_limit_precedes_allocation_and_one_byte_less_has_no_effect() {
    let cancellation = CancellationToken::new();
    for restore in [false, true] {
        for (copies, allocation, denied) in [
            (4, 4, None),
            (3, 0, Some("bytes_copied")),
            (4, 3, Some("peak_allocation_bytes")),
        ] {
            let setup = setup_peak(1, false);
            let limit = crate::WorkBudget {
                bytes_copied: copies,
                peak_allocation_bytes: setup + allocation,
                ..crate::WorkBudget::UNBOUNDED
            };
            let root = crate::NativeMaterializationWork::new(limit, &cancellation).unwrap();
            let mut accounting = KernelAccounting::native(&root, &cancellation);
            let text = if restore { "abc" } else { "abcd" };
            let label = (!restore).then(|| name(text).unwrap());
            let result = candidate(label.as_ref(), text, &mut accounting, &cancellation);
            match denied {
                None => {
                    let owned = result.unwrap();
                    assert_eq!(owned.value.len(), 4);
                    assert_eq!(root.receipt().bytes_copied, 4);
                    assert_eq!(root.live_allocation_bytes(), setup + 4);
                    owned.retire(&mut accounting).unwrap();
                }
                Some(field) => {
                    let error = format!("{:?}", result.err().unwrap());
                    assert!(error.contains(field), "{error}");
                    assert_eq!(root.receipt().bytes_copied, 0);
                }
            }
            assert_eq!(root.live_allocation_bytes(), setup);
            let credit = root
                .state(|state| state.budget.bytes_copied == state.original_budget().bytes_copied);
            assert!(credit);
        }
    }
}

std::thread_local! {
    static REDUNDANT: std::cell::Cell<bool> = const { std::cell::Cell::new(false) };
    static ACTUAL_COPY: std::cell::Cell<Option<u64>> = const { std::cell::Cell::new(None) };
}

pub(super) fn observed_copy(bytes: u64) {
    ACTUAL_COPY.with(|copied| copied.set(Some(bytes)));
}

// Negative control: restores a discarded copy preflight before the claim.
pub(super) fn redundant_preflight() -> bool {
    REDUNDANT.with(std::cell::Cell::get)
}

std::thread_local! {
    static EARLY: std::cell::Cell<bool> = const { std::cell::Cell::new(false) };
}

// Negative control: charges the copy with the storage claim, before the
// allocator has produced the capacity.
pub(super) fn charge_before_allocating() -> bool {
    EARLY.with(std::cell::Cell::get)
}

// An allocator failure after the combined check and claim must leave no copy
// charged, no credit reserved and no storage held, on every scope; charging the copy
// before allocating is caught by the same assertions.
#[test]
fn allocator_failure_charges_no_copy() {
    let failed = |early: bool, depth: u8| {
        let cancellation = CancellationToken::new();
        let limit = crate::WorkBudget {
            bytes_copied: 2,
            ..crate::WorkBudget::UNBOUNDED
        };
        let root = crate::NativeMaterializationWork::new(limit, &cancellation).unwrap();
        let mut work = root.clone();
        for level in 0..depth {
            work = work
                .native_volume_work(crate::WorkBudget {
                    bytes_copied: 3 - u64::from(level),
                    ..crate::WorkBudget::UNBOUNDED
                })
                .unwrap();
        }
        let setup = root.live_allocation_bytes();
        let mut accounting = KernelAccounting::native(&work, &cancellation);
        crate::kernel::FAIL_RESERVE.with(|flag| flag.set(true));
        EARLY.with(|flag| flag.set(early));
        let result = restore_absolute_path("x", &mut accounting, &cancellation);
        crate::kernel::FAIL_RESERVE.with(|flag| flag.set(false));
        EARLY.with(|flag| flag.set(false));
        let error = format!("{:?}", result.err().unwrap());
        assert!(error.contains("AllocationFailed"), "{error}");
        assert_eq!(root.live_allocation_bytes(), setup);
        let uncharged = root.receipt().bytes_copied == 0;
        let root_credit =
            root.state(|state| state.budget.bytes_copied == state.original_budget().bytes_copied);
        let scopes = work
            .resource_chain()
            .iter()
            .all(|(local, pending)| local.bytes_copied == 0 && pending.bytes_copied == 0);
        uncharged && root_credit && scopes
    };
    for depth in [0, 2] {
        assert!(failed(false, depth), "depth={depth}");
        assert!(!failed(true, depth), "an early copy charge must be caught");
    }
}

// Copy limits win over storage limits wherever each is bounded in the chain,
// and a storage limit alone still reports storage.
#[test]
fn copy_limits_precede_storage_limits_across_independent_scopes() {
    let cancellation = CancellationToken::new();
    for (root_copy, child_peak, field) in [
        (0, 0, "bytes_copied"),
        (2, 0, "peak_allocation_bytes"),
        (0, 2, "bytes_copied"),
    ] {
        let root = crate::NativeMaterializationWork::new(
            crate::WorkBudget {
                bytes_copied: root_copy,
                ..crate::WorkBudget::UNBOUNDED
            },
            &cancellation,
        )
        .unwrap();
        let child = root
            .native_volume_work(crate::WorkBudget {
                peak_allocation_bytes: child_peak,
                ..crate::WorkBudget::UNBOUNDED
            })
            .unwrap();
        let before = root.live_allocation_bytes();
        let mut accounting = KernelAccounting::native(&child, &cancellation);
        let error = format!(
            "{:?}",
            restore_absolute_path("x", &mut accounting, &cancellation)
                .err()
                .unwrap()
        );
        assert!(error.contains(field), "{root_copy} {child_peak}: {error}");
        assert_eq!(root.receipt().bytes_copied, 0);
        assert_eq!(root.live_allocation_bytes(), before);
    }
}

// (original leaf, resource mutex) acquisitions for one successful copy.
fn trace(
    old_path: bool,
    restore: bool,
    mode: u8,
    recheck: bool,
) -> ((usize, usize), crate::WorkCounters) {
    let cancellation = CancellationToken::new();
    let root =
        crate::NativeMaterializationWork::new(crate::WorkBudget::UNBOUNDED, &cancellation).unwrap();
    let mut work = root.clone();
    for _ in 0..mode.saturating_sub(1).min(2) {
        work = work
            .native_volume_work(crate::WorkBudget::UNBOUNDED)
            .unwrap();
    }
    if mode == 4 {
        work = work.invocation_view().unwrap().0;
    }
    if recheck {
        work = work.recheck_admitted();
    }
    let label = (!restore).then(|| name("abcd").unwrap());
    let mut accounting = KernelAccounting::native(&work, &cancellation);
    let ((owned, leaf), resource) = crate::materializer::count_resource_locks(|| {
        root.state.count_locks(|| match old_path {
            true if restore => old("/", "abc", &mut accounting, Some(&cancellation)),
            // Main's label also re-admitted the copy after allocating.
            true => {
                let copied = copy_work(4);
                let length = 4;
                (|| -> Result<_, WorkspaceError> {
                    accounting.admit(copied)?;
                    let mut bytes =
                        crate::kernel::Owned::<Vec<u8>>::allocate(length, &mut accounting)?;
                    accounting.admit(copied)?;
                    bytes.value.extend_from_slice(b"abcd");
                    accounting.record_completed(copied)?;
                    Ok(owned_text(bytes)?)
                })()
            }
            false => candidate(label.as_ref(), "abc", &mut accounting, &cancellation),
        })
    });
    owned.unwrap().retire(&mut accounting).unwrap();
    ((leaf, resource), root.receipt())
}

// Exact lock pins against main's sequence on every native context: the
// reserved copy takes fewer leaf passes and no more resource-mutex passes
// than check-only admission did, and a single redundant preflight is
// caught even though it leaves every receipt unchanged.
#[test]
fn reserved_copies_take_fewer_locks_than_check_only_admission() {
    // (mode, recheck) -> [main, fused] (leaf, resource) acquisitions. Mode 1
    // is a bare root, 2 and 3 add one and two resource scopes, 4 views two.
    let pins = |main: usize, per_level: usize| -> [((u8, bool), [(usize, usize); 2]); 6] {
        [
            ((1, false), [(main, 0), (3, 0)]),
            ((1, true), [(main, 0), (3, 0)]),
            ((2, false), [(main, per_level), (3, 6)]),
            ((3, false), [(main, 2 * per_level), (3, 12)]),
            ((3, true), [(main, 2 * per_level), (3, 12)]),
            ((4, false), [(main, 2 * per_level), (3, 12)]),
        ]
    };
    for (restore, pins) in [(false, pins(7, 9)), (true, pins(6, 7))] {
        let mut observed = pins;
        for ((mode, recheck), locks) in &mut observed {
            let (main, main_receipt) = trace(true, restore, *mode, *recheck);
            let (fused, receipt) = trace(false, restore, *mode, *recheck);
            *locks = [main, fused];
            assert!(fused.0 < main.0 && fused.1 <= main.1, "{main:?} {fused:?}");
            assert_eq!(receipt, main_receipt);
            REDUNDANT.with(|flag| flag.set(true));
            let (redundant, redundant_receipt) = trace(false, restore, *mode, *recheck);
            REDUNDANT.with(|flag| flag.set(false));
            assert_eq!(redundant_receipt, receipt);
            assert!(
                redundant > fused,
                "a redundant preflight must break the pin"
            );
        }
        assert_eq!(observed, pins, "restore={restore}");
    }
}

// Paired per-OS timing, run explicitly in release on each host:
// `cargo test --release --lib workspace::paths::tests::paired -- --ignored --nocapture`.
// Rounds alternate order; prints the median ns per copy and paired change.
#[test]
#[ignore = "timing report, not a check"]
fn paired_copy_timing_report() {
    const COPIES: usize = 4096;
    for (mode, recheck) in [(1_u8, false), (2, false), (3, true), (4, false)] {
        for restore in [false, true] {
            let cancellations = [CancellationToken::new(), CancellationToken::new()];
            let roots = cancellations.each_ref().map(|cancellation| {
                crate::NativeMaterializationWork::new(crate::WorkBudget::UNBOUNDED, cancellation)
                    .unwrap()
            });
            let work = roots.each_ref().map(|root| {
                let mut work = root.clone();
                for _ in 0..mode.saturating_sub(1).min(2) {
                    work = work
                        .native_volume_work(crate::WorkBudget::UNBOUNDED)
                        .unwrap();
                }
                if mode == 4 {
                    work = work.invocation_view().unwrap().0;
                }
                if recheck {
                    work = work.recheck_admitted();
                }
                work
            });
            let label = (!restore).then(|| name("workspace-name").unwrap());
            let text = "workspace-name";
            let run = |old_path: bool| {
                let index = usize::from(!old_path);
                let cancellation = &cancellations[index];
                let mut accounting = KernelAccounting::native(&work[index], cancellation);
                let start = std::time::Instant::now();
                for _ in 0..COPIES {
                    let owned = match (old_path, restore) {
                        (true, true) => old("/", text, &mut accounting, Some(cancellation)),
                        (true, false) => old("", text, &mut accounting, None),
                        (false, _) => {
                            candidate(label.as_ref(), text, &mut accounting, cancellation)
                        }
                    };
                    std::hint::black_box(&owned);
                    owned.unwrap().retire(&mut accounting).unwrap();
                }
                start.elapsed().as_nanos() as f64 / COPIES as f64
            };
            let mut pairs = Vec::new();
            for round in 0..31 {
                let (before, after) = if round % 2 == 0 {
                    let before = run(true);
                    (before, run(false))
                } else {
                    let after = run(false);
                    (run(true), after)
                };
                pairs.push((before, after, 100.0 * (1.0 - after / before)));
            }
            let median = |mut values: Vec<f64>| {
                values.sort_by(f64::total_cmp);
                values[values.len() / 2]
            };
            println!(
                "PAIRED os={} mode={mode} recheck={recheck} restore={restore} main_ns={:.1} fused_ns={:.1} faster_percent={:.1}",
                std::env::consts::OS,
                median(pairs.iter().map(|pair| pair.0).collect()),
                median(pairs.iter().map(|pair| pair.1).collect()),
                median(pairs.iter().map(|pair| pair.2).collect()),
            );
        }
    }
}
