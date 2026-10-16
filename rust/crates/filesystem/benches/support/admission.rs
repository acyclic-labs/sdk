use acyclic_fs::{CancellationToken, NativeMaterializationWork, WorkCounters};
use divan::Bencher;

fn fail<T>(error: impl std::fmt::Debug) -> T {
    eprintln!("admission benchmark failed: {error:?}");
    std::process::exit(1);
}

fn marker(original: &NativeMaterializationWork, context: u8, recheck: bool, begin: bool) {
    if std::env::var_os("ACYCLIC_BENCH_MARKERS").is_some() {
        let meter = original.runtime_meter();
        let leaf = std::sync::Arc::as_ptr(&meter) as *const () as usize;
        let bytes = std::mem::size_of_val(meter.as_ref());
        eprintln!(
            "ACYCLIC_ADMISSION_BOUNDARY context={context} recheck={recheck} begin={begin} leaf={leaf:#x} leaf_bytes={bytes}"
        );
    }
}

#[divan::bench(args = [(0, false), (0, true), (1, false), (1, true), (2, false), (2, true)], threads = [1, 4])]
fn admission_charge(bencher: Bencher, (context, recheck): (u8, bool)) {
    let cancellation = CancellationToken::new();
    let budget = acyclic_fs::performance::BOUNDARY_BUDGET;
    let original = NativeMaterializationWork::new(budget, &cancellation).unwrap_or_else(fail);
    let work = match context {
        0 => original.clone(),
        1 => original.native_volume_work(budget).unwrap_or_else(fail),
        _ => original.invocation_view().unwrap_or_else(fail).0,
    };
    let work = if recheck {
        work.recheck_admitted()
    } else {
        work
    };
    let delta = WorkCounters {
        items_examined: 1,
        bytes_copied: 1,
        ..WorkCounters::UNCHARGED
    };
    if std::env::var_os("ACYCLIC_BENCH_WORK").is_some() {
        eprintln!(
            "admission context={context} recheck={recheck} before live={} work={:?}",
            original.live_allocation_bytes(),
            original.receipt()
        );
    }
    marker(&original, context, recheck, true);
    bencher.bench(|| work.charge(delta).unwrap_or_else(fail));
    marker(&original, context, recheck, false);
    if std::env::var_os("ACYCLIC_BENCH_WORK").is_some() {
        eprintln!(
            "admission context={context} recheck={recheck} after live={} work={:?}",
            original.live_allocation_bytes(),
            original.receipt()
        );
    }
}

#[divan::bench]
fn admission_charge_local(bencher: Bencher) {
    let mut counters = WorkCounters::UNCHARGED;
    let delta = WorkCounters {
        items_examined: 1,
        bytes_copied: 1,
        ..WorkCounters::UNCHARGED
    };
    bencher.bench_local(|| {
        counters
            .charge(&delta, &acyclic_fs::performance::BOUNDARY_BUDGET)
            .unwrap_or_else(fail);
    });
}
