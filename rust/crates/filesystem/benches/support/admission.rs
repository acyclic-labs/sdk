use acyclic_fs::facade::FsWork;
use acyclic_fs::{CancellationToken, NativeMaterializationWork, WorkCounters};
use divan::Bencher;

fn fail<T>(error: impl std::fmt::Debug) -> T {
    eprintln!("admission benchmark failed: {error:?}");
    std::process::exit(1);
}

fn marker(original: &NativeMaterializationWork, context: u8, recheck: bool, begin: bool) {
    if std::env::var_os("ACYCLIC_BENCH_MARKERS").is_some() {
        let meter = original.runtime_meter();
        let meter = meter.allocation_owner().unwrap_or(meter);
        let leaf = std::sync::Arc::as_ptr(&meter) as *const () as usize;
        let bytes = std::mem::size_of_val(meter.as_ref());
        eprintln!(
            "ACYCLIC_ADMISSION_BOUNDARY context={context} recheck={recheck} begin={begin} leaf={leaf:#x} leaf_bytes={bytes}"
        );
    }
}

const CHARGES_PER_BATCH: usize = 1024;

fn fixture(context: u8, recheck: bool) -> NativeMaterializationWork {
    let cancellation = CancellationToken::new();
    let budget = acyclic_fs::performance::BOUNDARY_BUDGET;
    let original = NativeMaterializationWork::new(budget, &cancellation).unwrap_or_else(fail);
    let work = match context {
        0 => original,
        1 => original.native_volume_work(budget).unwrap_or_else(fail),
        _ => original.invocation_view().unwrap_or_else(fail).0,
    };
    if recheck {
        work.recheck_admitted()
    } else {
        work
    }
}

#[divan::bench(args = [(0, false), (0, true), (1, false), (1, true), (2, false), (2, true)], threads = [1, 4])]
fn admission_charge(bencher: Bencher, (context, recheck): (u8, bool)) {
    let delta = WorkCounters {
        items_examined: 1,
        bytes_copied: 1,
        ..WorkCounters::UNCHARGED
    };
    if std::env::var_os("ACYCLIC_BENCH_WORK").is_some()
        || std::env::var_os("ACYCLIC_BENCH_MARKERS").is_some()
    {
        let work = fixture(context, recheck);
        eprintln!(
            "admission context={context} recheck={recheck} batch={CHARGES_PER_BATCH} before live={} work={:?}",
            work.live_allocation_bytes(),
            work.receipt()
        );
        marker(&work, context, recheck, true);
        for _ in 0..CHARGES_PER_BATCH {
            work.charge(delta).unwrap_or_else(fail);
        }
        marker(&work, context, recheck, false);
        eprintln!(
            "admission context={context} recheck={recheck} batch={CHARGES_PER_BATCH} after live={} work={:?}",
            work.live_allocation_bytes(),
            work.receipt()
        );
    }
    // The lifetime's finite budget covers one fixed batch. Input generation and
    // retirement stay outside timing; sample count cannot exhaust a shared run.
    // Parallel workers use independent scopes; accounting_shared retains the
    // separate benchmark of original-leaf contention.
    bencher
        .counter(divan::counter::ItemsCount::new(CHARGES_PER_BATCH))
        .with_inputs(|| fixture(context, recheck))
        .bench_refs(|work| {
            for _ in 0..CHARGES_PER_BATCH {
                work.charge(delta).unwrap_or_else(fail);
            }
        });
}

#[divan::bench(args = [(0, false), (0, true), (1, false), (1, true), (2, false), (2, true)], threads = [1, 4])]
fn kernel_charge(bencher: Bencher, (context, recheck): (u8, bool)) {
    let cancellation = CancellationToken::new();
    let delta = WorkCounters {
        items_examined: 1,
        bytes_copied: 1,
        ..WorkCounters::UNCHARGED
    };
    let operation = |work: &NativeMaterializationWork| {
        let mut accounting = acyclic_fs::kernel::KernelAccounting::NativeOriginal {
            work,
            cancellation: &cancellation,
        };
        for _ in 0..CHARGES_PER_BATCH {
            accounting.charge(delta).unwrap_or_else(fail);
        }
    };
    if std::env::var_os("ACYCLIC_BENCH_WORK").is_some()
        || std::env::var_os("ACYCLIC_BENCH_MARKERS").is_some()
    {
        let work = fixture(context, recheck);
        eprintln!(
            "kernel_charge context={context} recheck={recheck} before live={} work={:?}",
            work.live_allocation_bytes(),
            work.receipt()
        );
        marker(&work, context, recheck, true);
        operation(&work);
        marker(&work, context, recheck, false);
        eprintln!(
            "kernel_charge context={context} recheck={recheck} after live={} work={:?}",
            work.live_allocation_bytes(),
            work.receipt()
        );
    }
    bencher
        .counter(divan::counter::ItemsCount::new(CHARGES_PER_BATCH))
        .with_inputs(|| fixture(context, recheck))
        .bench_refs(|work| operation(work));
}

#[divan::bench(args = [0, 1], threads = [4], sample_count = 64, sample_size = 8)]
fn kernel_charge_shared(bencher: Bencher, context: u8) {
    let cancellation = CancellationToken::new();
    let work = fixture(context, false);
    let delta = WorkCounters {
        items_examined: 1,
        bytes_copied: 1,
        ..WorkCounters::UNCHARGED
    };
    marker(&work, context, false, true);
    bencher.bench(|| {
        let mut accounting = acyclic_fs::kernel::KernelAccounting::NativeOriginal {
            work: &work,
            cancellation: &cancellation,
        };
        accounting.charge(delta).unwrap_or_else(fail);
    });
    marker(&work, context, false, false);
    if std::env::var_os("ACYCLIC_BENCH_WORK").is_some() {
        eprintln!(
            "kernel_charge_shared context={context} work={:?}",
            work.receipt()
        );
    }
}

#[divan::bench]
fn admission_charge_local(bencher: Bencher) {
    let cancellation = CancellationToken::new();
    let budget = acyclic_fs::performance::BOUNDARY_BUDGET;
    let delta = WorkCounters {
        items_examined: 1,
        bytes_copied: 1,
        ..WorkCounters::UNCHARGED
    };
    bencher
        .counter(divan::counter::ItemsCount::new(CHARGES_PER_BATCH))
        .with_inputs(|| budget.accounting(&cancellation))
        .bench_local_refs(|accounting| {
            for _ in 0..CHARGES_PER_BATCH {
                accounting.charge(delta).unwrap_or_else(fail);
            }
        });
}

#[divan::bench(args = [0, 1], threads = [1, 4])]
fn allocation_release_zero(bencher: Bencher, context: u8) {
    let operation = || {
        let work = fixture(context, false);
        let allocation = work
            .claim(0, 0, WorkCounters::UNCHARGED)
            .unwrap_or_else(fail);
        (work, allocation)
    };
    if std::env::var_os("ACYCLIC_BENCH_WORK").is_some()
        || std::env::var_os("ACYCLIC_BENCH_MARKERS").is_some()
    {
        let (work, mut allocation) = operation();
        let before = work.receipt();
        let live = work.live_allocation_bytes();
        marker(&work, context, false, true);
        for _ in 0..CHARGES_PER_BATCH {
            allocation
                .release(std::hint::black_box(0))
                .unwrap_or_else(fail);
        }
        marker(&work, context, false, false);
        assert_eq!(work.receipt(), before);
        assert_eq!(work.live_allocation_bytes(), live);
        eprintln!(
            "empty_release context={context} count={CHARGES_PER_BATCH} live={live} work={before:?}"
        );
    }
    bencher
        .counter(divan::counter::ItemsCount::new(CHARGES_PER_BATCH))
        .with_inputs(operation)
        .bench_refs(|(_, allocation)| {
            for _ in 0..CHARGES_PER_BATCH {
                allocation
                    .release(std::hint::black_box(0))
                    .unwrap_or_else(fail);
            }
        });
}

#[divan::bench(args = [(0, false), (0, true), (1, false), (1, true), (2, false), (2, true)], threads = [1, 4])]
fn copy_preflight(bencher: Bencher, (context, recheck): (u8, bool)) {
    let cancellation = CancellationToken::new();
    let delta = WorkCounters {
        bytes_copied: 8,
        ..WorkCounters::UNCHARGED
    };
    if std::env::var_os("ACYCLIC_BENCH_WORK").is_some()
        || std::env::var_os("ACYCLIC_BENCH_MARKERS").is_some()
    {
        let work = fixture(context, recheck);
        let accounting = acyclic_fs::kernel::KernelAccounting::NativeOriginal {
            work: &work,
            cancellation: &cancellation,
        };
        let before = work.receipt();
        let live = work.live_allocation_bytes();
        marker(&work, context, recheck, true);
        for _ in 0..CHARGES_PER_BATCH {
            accounting
                .admit(std::hint::black_box(delta))
                .unwrap_or_else(fail);
        }
        marker(&work, context, recheck, false);
        assert_eq!(work.receipt(), before);
        assert_eq!(work.live_allocation_bytes(), live);
        eprintln!(
            "copy_preflight context={context} recheck={recheck} count={CHARGES_PER_BATCH} live={live} work={before:?}"
        );
    }
    bencher
        .counter(divan::counter::ItemsCount::new(CHARGES_PER_BATCH))
        .with_inputs(|| fixture(context, recheck))
        .bench_refs(|work| {
            let accounting = acyclic_fs::kernel::KernelAccounting::NativeOriginal {
                work,
                cancellation: &cancellation,
            };
            for _ in 0..CHARGES_PER_BATCH {
                accounting
                    .admit(std::hint::black_box(delta))
                    .unwrap_or_else(fail);
            }
        });
}
