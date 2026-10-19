use super::tests::{budget, fixture, scan};
use super::*;
use acyclic_fs::{CancellationToken, WorkBudget, WorkCounters};
use proptest::prelude::*;

fn seeded_io(
    budget: WorkBudget,
    live: u64,
    initial: WorkCounters,
    cancellation: &CancellationToken,
) -> Result<PreparationIo> {
    use acyclic_fs::kernel::{KernelAccounting, allocation::AllocationLedger};
    let mut allocations = AllocationLedger::default();
    let mut spent = WorkCounters::default();
    KernelAccounting::borrowed_standalone(
        &mut allocations,
        &mut spent,
        WorkBudget::UNBOUNDED,
        cancellation,
    )
    .claim_owned(live, 0)?;
    spent = spent
        .checked_add(initial)
        .map_err(acyclic_fs::FsError::from)?;
    Ok(PreparationIo::Owned {
        budget,
        work: Box::new(crate::sync::SyncMutex::new((spent, Ok(allocations)))),
        cancellation: cancellation.clone(),
    })
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(64))]
    // Moving the lock boundary preserves every receipt field and error even
    // after prior work, retained storage, budget exhaustion and cancellation.
    #[test]
    fn scoped_validation_matches_original_preflights_with_prior_work(
        rows in 0..32_u32,
        query in "[a-z./_0-9-]{0,32}",
        live in 0..64_u64,
        prior_items in 0..24_u64, prior_source in 0..1024_u64, prior_hashed in 0..16_u64,
        allowed_items in 0..64_u64, allowed_source in 0..4096_u64,
        allowed_hashed in 0..24_u64, allowed_peak in 0..128_u64,
        cancelled in any::<bool>(),
        cancel_at in prop::option::of(0..64_usize),
    ) {
        let store = fixture(rows)?;
        let cancellations = [CancellationToken::default(), CancellationToken::default()];
        let initial = WorkCounters { items_examined: prior_items, source_bytes_read: prior_source, bytes_hashed: prior_hashed, ..Default::default() };
        let budget = WorkBudget { bytes_hashed: allowed_hashed, peak_allocation_bytes: allowed_peak, ..budget(allowed_items,allowed_source) };
        let before = seeded_io(budget,live,initial,&cancellations[0])?;
        let after = seeded_io(budget,live,initial,&cancellations[1])?;
        if cancelled { for cancellation in &cancellations { cancellation.cancel(); } }
        #[cfg(debug_assertions)]
        let (original, actual) = {
            let (original, original_trace) = crate::accounting_trace::capture(&cancellations[0],cancel_at,||scan(&store,&query,&before));
            let (actual, actual_trace) = crate::accounting_trace::capture(&cancellations[1],cancel_at,||store.validate_retained_path(&query,Some(&after)));
            crate::accounting_trace::assert_matches(&original_trace,&actual_trace);
            (original, actual)
        };
        #[cfg(not(debug_assertions))]
        let (original, actual) = {
            let _ = cancel_at;
            (scan(&store,&query,&before),store.validate_retained_path(&query,Some(&after)))
        };
        prop_assert_eq!(original.is_ok(),actual.is_ok());
        prop_assert_eq!(original.as_ref().err().map(std::mem::discriminant),actual.as_ref().err().map(std::mem::discriminant));
        prop_assert_eq!(original.err().map(|error|error.to_string()),actual.err().map(|error|error.to_string()));
        prop_assert_eq!(before.receipt(),after.receipt());
        prop_assert_eq!(before.owned_bytes()?,after.owned_bytes()?);
    }
}

#[test]
fn overflow_and_invalid_ledger_preserve_original_empty_scan_and_failure_order() -> Result<()> {
    for rows in [0, 1] {
        let store = fixture(rows)?;
        for invalid in [false, true] {
            let cancellation = CancellationToken::default();
            let initial = WorkCounters {
                source_bytes_read: u64::MAX - 2,
                ..Default::default()
            };
            let before = seeded_io(budget(2, u64::MAX - 1), 0, initial, &cancellation)?;
            let after = seeded_io(budget(2, u64::MAX - 1), 0, initial, &cancellation)?;
            if invalid {
                for io in [&before, &after] {
                    if let PreparationIo::Owned { work, .. } = io {
                        work.lock().unwrap().1 = Err(acyclic_fs::WorkError::Overflow);
                    }
                }
            }
            let original = scan(&store, "files/new", &before);
            let actual = store.validate_retained_path("files/new", Some(&after));
            assert_eq!(original.is_ok(), rows == 0);
            assert_eq!(
                original.err().map(|error| error.to_string()),
                actual.err().map(|error| error.to_string())
            );
            assert_eq!(before.receipt(), after.receipt());
        }
    }
    Ok(())
}

#[test]
fn retained_path_with_accounting() -> Result<()> {
    profile(true)
}

#[test]
fn retained_path_without_accounting() -> Result<()> {
    profile(false)
}

fn profile(accounted: bool) -> Result<()> {
    let store = fixture(4096)?;
    let path = "files/new";
    let selected = crate::native_environment::variable("HARNESS_PATH_PROFILE")
        .and_then(|value| value.into_string().ok());
    let syscalls = crate::native_environment::variable("HARNESS_PATH_SYSCALLS").is_some();
    for tracing in [false, true] {
        let dispatch = if tracing {
            tracing::Dispatch::new(tracing_subscriber::registry())
        } else {
            tracing::Dispatch::none()
        };
        let _guard = tracing::dispatcher::set_default(&dispatch);
        assert_eq!(tracing::enabled!(tracing::Level::TRACE), tracing);
        let cancellation = CancellationToken::default();
        let ledgers = [
            PreparationIo::from_budget(budget(1_000_000, 64 * 1024 * 1024), &cancellation)?,
            PreparationIo::from_budget(budget(1_000_000, 64 * 1024 * 1024), &cancellation)?,
        ];
        let operation = |index: usize| -> Result<()> {
            let store = std::hint::black_box(&store);
            let path = std::hint::black_box(path);
            let io = accounted.then_some(&ledgers[index]);
            if index == 0 && accounted {
                scan(store, path, &ledgers[index])
            } else {
                store.validate_retained_path(path, io)
            }
        };
        let enabled = |index: usize| {
            selected
                .as_deref()
                .is_none_or(|name| (name == "scan") == (index == 0))
        };
        let mut times = [Vec::with_capacity(64), Vec::with_capacity(64)];
        for sample in 0..64 {
            for index in if sample % 2 == 0 { [0, 1] } else { [1, 0] } {
                if !enabled(index) {
                    continue;
                }
                if syscalls {
                    eprintln!("PATH_BEGIN");
                }
                let started = std::time::Instant::now();
                operation(index)?;
                let elapsed = started.elapsed();
                if syscalls {
                    eprintln!("PATH_END");
                }
                times[index].push(elapsed);
            }
        }
        for index in 0..2 {
            if !enabled(index) {
                continue;
            }
            times[index].sort_unstable();
            let mut result = Ok(());
            let allocation = acyclic_test_support::measure(|| result = operation(index));
            result?;
            assert_eq!(allocation.count_total, 0);
            assert_eq!(allocation.bytes_total, 0);
            assert_eq!(allocation.bytes_max, 0);
            let expected = if accounted {
                WorkCounters {
                    items_examined: 65 * 4096,
                    source_bytes_read: 65 * 4096 * 23,
                    ..Default::default()
                }
            } else {
                WorkCounters::default()
            };
            assert_eq!(ledgers[index].receipt(), expected);
            eprintln!(
                "path-profile accounted={accounted} tracing={tracing} legacy={} median={:?} alloc={} bytes={} peak={} receipt={:?}",
                index == 0,
                times[index].get(32),
                allocation.count_total,
                allocation.bytes_total,
                allocation.bytes_max,
                ledgers[index].receipt()
            );
        }
    }
    Ok(())
}

#[test]
fn poisoned_accounting_preserves_empty_scans_and_error_order() -> Result<()> {
    for rows in [0, 1] {
        let store = fixture(rows)?;
        let before = PreparationIo::from_budget(budget(10, 1024), &CancellationToken::default())?;
        let after = PreparationIo::from_budget(budget(10, 1024), &CancellationToken::default())?;
        for io in [&before, &after] {
            if let PreparationIo::Owned { work, .. } = io {
                let poisoned = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                    let _guard = work.lock().unwrap();
                    panic!("poison control");
                }));
                assert!(poisoned.is_err());
            }
        }
        let old = scan(&store, "files/new", &before);
        let new = store.validate_retained_path("files/new", Some(&after));
        assert_eq!(old.is_ok(), rows == 0);
        assert_eq!(
            old.err().map(|error| error.to_string()),
            new.err().map(|error| error.to_string())
        );
        assert_eq!(before.receipt(), after.receipt());
    }
    Ok(())
}

#[test]
fn contended_retained_path_accounting() -> Result<()> {
    use std::sync::Barrier;
    use std::time::Instant;
    let store = fixture(4096)?;
    let selected = crate::native_environment::variable("HARNESS_PATH_PROFILE")
        .and_then(|value| value.into_string().ok());
    let syscalls = crate::native_environment::variable("HARNESS_PATH_SYSCALLS").is_some();
    let samples = if syscalls { 1 } else { 8 };
    let calls = if syscalls { 1_u32 } else { 32 };
    for tracing in [false, true] {
        let dispatch = if tracing {
            tracing::Dispatch::new(tracing_subscriber::registry())
        } else {
            tracing::Dispatch::none()
        };
        let mut times = [Vec::with_capacity(samples), Vec::with_capacity(samples)];
        for sample in 0..samples {
            for index in if sample % 2 == 0 { [0, 1] } else { [1, 0] } {
                if selected
                    .as_deref()
                    .is_some_and(|name| (name == "scan") != (index == 0))
                {
                    continue;
                }
                let io = PreparationIo::from_budget(
                    budget(1_000_000, 64 * 1024 * 1024),
                    &CancellationToken::default(),
                )?;
                let barrier = Barrier::new(3);
                let elapsed = std::thread::scope(|scope| {
                    let mut threads = Vec::with_capacity(2);
                    for _ in 0..2 {
                        let (store, io, barrier, dispatch) = (&store, &io, &barrier, &dispatch);
                        threads.push(scope.spawn(move || {
                            let _guard = tracing::dispatcher::set_default(dispatch);
                            barrier.wait();
                            barrier.wait();
                            assert_eq!(tracing::enabled!(tracing::Level::TRACE), tracing);
                            if syscalls {
                                eprintln!("PATH_BEGIN");
                            }
                            let allocations = acyclic_test_support::measure(|| {
                                for _ in 0..calls {
                                    if index == 0 {
                                        scan(store, "files/new", io).unwrap();
                                    } else {
                                        store
                                            .validate_retained_path("files/new", Some(io))
                                            .unwrap();
                                    }
                                }
                            });
                            if syscalls {
                                eprintln!("PATH_END");
                            }
                            assert_eq!(allocations.count_total, 0);
                            assert_eq!(allocations.bytes_total, 0);
                            assert_eq!(allocations.bytes_max, 0);
                        }));
                    }
                    barrier.wait();
                    let started = Instant::now();
                    barrier.wait();
                    for thread in threads {
                        thread.join().unwrap();
                    }
                    started.elapsed()
                });
                assert_eq!(
                    io.receipt(),
                    WorkCounters {
                        items_examined: 2 * u64::from(calls) * 4096,
                        source_bytes_read: 2 * u64::from(calls) * 4096 * 23,
                        ..Default::default()
                    }
                );
                times[index].push(elapsed / (2 * calls));
            }
        }
        for (index, samples) in times.iter_mut().enumerate() {
            samples.sort_unstable();
            if let Some(median) = samples.get(samples.len() / 2) {
                eprintln!(
                    "contended-path tracing={tracing} legacy={} median={median:?} alloc=0 bytes=0 peak=0",
                    index == 0
                );
            }
        }
    }
    Ok(())
}

#[cfg(debug_assertions)]
#[path = "retention_scan_trace_tests.rs"]
mod trace_tests;
