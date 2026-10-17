use acyclic_fs::{CancellationToken, NativeMaterializationWork, WorkCounters, WorkspaceName};
use divan::Bencher;
use std::alloc::Layout;
use unicode_normalization::UnicodeNormalization;

fn fail<T>(error: impl std::fmt::Debug) -> T {
    eprintln!("workspace-name benchmark failed: {error:?}");
    std::process::exit(1);
}
fn name(case: u8) -> &'static str {
    match case {
        0 => "private",
        1 => "generation-pin-0123456789abcdef0123456789abcdef",
        2 => "cafe\u{301}",
        3 => "a\u{315}\u{300}\u{344}",
        4 => {
            "nnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnn"
        }
        _ => "😀😀😀😀😀😀😀😀😀😀😀😀😀😀😀😀",
    }
}
fn delta(case: u8, baseline: u64) -> WorkCounters {
    let input = name(case);
    let legacy = std::env::var_os("ACYCLIC_BENCH_LEGACY_NAME").is_some();
    let mut items = input.chars().count() as u64;
    for character in input.chars() {
        if legacy || !character.is_ascii() {
            unicode_normalization::char::decompose_canonical(character, |_| items += 1);
        }
    }
    let length = input.nfc().collect::<String>().len() as u64;
    // An independent layout of the standard Arc header and retained tuple.
    let (control, _) = Layout::new::<[usize; 2]>()
        .extend(Layout::new::<(
            String,
            acyclic_fs::kernel::KernelOwnedAllocation,
        )>())
        .unwrap_or_else(fail);
    WorkCounters {
        items_examined: items,
        bytes_copied: length * if legacy || !input.is_ascii() { 2 } else { 1 },
        allocation_operations: 2,
        peak_allocation_bytes: baseline + length + control.pad_to_align().size() as u64,
        ..WorkCounters::UNCHARGED
    }
}
fn marker(work: &NativeMaterializationWork, case: u8, begin: bool) {
    if std::env::var_os("ACYCLIC_BENCH_MARKERS").is_some() {
        let meter = work.runtime_meter();
        let meter = meter.allocation_owner().unwrap_or(meter);
        eprintln!(
            "ACYCLIC_NAME_BOUNDARY case={case} begin={begin} leaf={:#x} leaf_bytes={}",
            std::sync::Arc::as_ptr(&meter) as *const () as usize,
            std::mem::size_of_val(meter.as_ref())
        );
    }
}

#[divan::bench(args = [(0, false), (0, true), (1, false), (1, true), (2, false), (2, true), (3, false), (3, true), (4, false), (4, true), (5, false), (5, true)], threads = [1, 4])]
fn workspace_name(bencher: Bencher, (case, native): (u8, bool)) {
    let cancellation = CancellationToken::new();
    let budget = acyclic_fs::performance::BOUNDARY_BUDGET;
    let fixture = || {
        native.then(|| NativeMaterializationWork::new(budget, &cancellation).unwrap_or_else(fail))
    };
    // Run exact full-receipt assertions outside the timed operation.
    if let Some(work) = fixture() {
        let before = work.receipt();
        let expected = before
            .checked_add(delta(case, work.live_allocation_bytes()))
            .unwrap_or_else(fail);
        marker(&work, case, true);
        let value = WorkspaceName::new(name(case), &&work, &cancellation).unwrap_or_else(fail);
        marker(&work, case, false);
        assert_eq!(work.receipt(), expected);
        assert_eq!(value.as_str(), name(case).nfc().collect::<String>());
        if std::env::var_os("ACYCLIC_BENCH_WORK").is_some() {
            eprintln!(
                "workspace_name case={case} native={native} before={before:?} after={expected:?} value={}",
                value.as_str()
            );
        }
    } else {
        let result = WorkspaceName::new(name(case), &budget, &cancellation).unwrap_or_else(fail);
        assert_eq!(result.work, delta(case, 0));
        assert_eq!(result.value.as_str(), name(case).nfc().collect::<String>());
        if std::env::var_os("ACYCLIC_BENCH_WORK").is_some() {
            eprintln!(
                "workspace_name case={case} native={native} before={:?} after={:?} value={}",
                WorkCounters::UNCHARGED,
                result.work,
                result.value.as_str()
            );
        }
    }
    bencher.with_inputs(fixture).bench_refs(|original| {
        if let Some(work) = original.as_ref() {
            WorkspaceName::new(name(case), &work, &cancellation).unwrap_or_else(fail)
        } else {
            WorkspaceName::new(name(case), &budget, &cancellation)
                .unwrap_or_else(fail)
                .value
        }
    });
}

#[divan::bench(args = [0, 1, 2, 3, 4, 5], sample_count = 32, sample_size = 1)]
fn workspace_name_shared(bencher: Bencher, case: u8) {
    let cancellation = CancellationToken::new();
    let fixture = || {
        NativeMaterializationWork::new(acyclic_fs::performance::BOUNDARY_BUDGET, &cancellation)
            .unwrap_or_else(fail)
    };
    let operation = |work: &NativeMaterializationWork| {
        // Thread coordination is part of this fixed-batch measurement. A fresh
        // ledger per batch keeps all original limits independent of samples.
        let start = std::sync::Barrier::new(4);
        std::thread::scope(|scope| {
            for _ in 0..4 {
                let start = &start;
                let cancellation = &cancellation;
                scope.spawn(move || {
                    start.wait();
                    for _ in 0..16 {
                        let value = WorkspaceName::new(name(case), &work, cancellation)
                            .unwrap_or_else(fail);
                        std::hint::black_box(value);
                    }
                });
            }
        });
    };
    if std::env::var_os("ACYCLIC_BENCH_WORK").is_some()
        || std::env::var_os("ACYCLIC_BENCH_MARKERS").is_some()
    {
        let work = fixture();
        let before = work.receipt();
        let baseline = work.live_allocation_bytes();
        let one = delta(case, 0);
        marker(&work, case, true);
        operation(&work);
        marker(&work, case, false);
        let after = work.receipt();
        let mut expected = before;
        for _ in 0..64 {
            expected = expected.checked_add(one).unwrap_or_else(fail);
        }
        assert!(after.peak_allocation_bytes >= baseline + one.peak_allocation_bytes);
        assert!(after.peak_allocation_bytes <= baseline + 4 * one.peak_allocation_bytes);
        expected.peak_allocation_bytes = after.peak_allocation_bytes;
        assert_eq!(after, expected);
        eprintln!("workspace_name_shared case={case} work={after:?}");
    }
    bencher
        .with_inputs(fixture)
        .bench_refs(|work| operation(work));
}
