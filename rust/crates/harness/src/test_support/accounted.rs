//! Native ledger and custody observations for differential runs.
use crate::filesystem::PreparationIo;
use acyclic_fs::{CancellationToken, NativeMaterializationWork, WorkCounters};

// Include the original native ledger and live custody on both sides of IO drop;
// copying returned bytes in `run` releases their producer custody before sampling.
pub(crate) fn accounted<R>(
    native: bool,
    maximum: u64,
    cancelled: bool,
    run: impl FnOnce(&PreparationIo) -> R,
) -> Result<(R, WorkCounters, u64, WorkCounters, u64, u64), (String, WorkCounters)> {
    let cancellation = CancellationToken::new();
    let budget = acyclic_fs::performance::work_budget(1_000_000, maximum);
    let work = if native {
        Some(
            NativeMaterializationWork::new(budget, &cancellation)
                .map_err(|(cause, receipt)| (cause.to_string(), *receipt))?,
        )
    } else {
        None
    };
    let io = match &work {
        Some(work) => PreparationIo::from_native_work(work),
        None => PreparationIo::from_budget(budget, &cancellation),
    }
    .unwrap();
    if cancelled {
        cancellation.cancel();
    }
    let result = run(&io);
    let receipt = io.receipt();
    let owned = io.owned_bytes().unwrap();
    let held = work
        .as_ref()
        .map_or(0, NativeMaterializationWork::live_allocation_bytes);
    drop(io);
    Ok((
        result,
        receipt,
        owned,
        work.as_ref()
            .map_or(WorkCounters::default(), NativeMaterializationWork::receipt),
        held,
        work.as_ref()
            .map_or(0, NativeMaterializationWork::live_allocation_bytes),
    ))
}

thread_local! {
    static CHUNK_SCAN: std::cell::Cell<bool> = const { std::cell::Cell::new(false) };
    static CHUNK_METER: std::cell::Cell<bool> = const { std::cell::Cell::new(false) };
}

fn with_chunk_meter<R>(run: impl FnOnce() -> R) -> R {
    struct Reset(bool);
    impl Drop for Reset {
        fn drop(&mut self) {
            CHUNK_METER.set(self.0);
        }
    }
    let _reset = Reset(CHUNK_METER.replace(true));
    run()
}

pub(crate) fn chunk_scan_enabled() -> bool {
    CHUNK_SCAN.get()
}

pub(crate) fn with_chunk_scan<R>(enabled: bool, run: impl FnOnce() -> R) -> R {
    struct Reset(bool);
    impl Drop for Reset {
        fn drop(&mut self) {
            CHUNK_SCAN.set(self.0);
        }
    }
    let _reset = Reset(CHUNK_SCAN.replace(enabled));
    with_chunk_meter(run)
}

// Measure the real projection future, including every poll's SDK work. The
// transcript and canonical admission are intentionally outside this scope.
pub(crate) async fn observe_chunk<F: std::future::Future>(
    future: F,
    io: Option<&PreparationIo>,
) -> F::Output {
    if !CHUNK_METER.get()
        || acyclic_test_support::differential::reserve::<(&'static str, u64)>(0).is_none()
    {
        return future.await;
    }
    let before = io.map(PreparationIo::receipt).unwrap_or_default();
    let mut future = std::pin::pin!(future);
    let output =
        futures::future::poll_fn(|cx| observe_chunk_poll(|| future.as_mut().poll(cx))).await;
    record_chunk_native(before, io.map(PreparationIo::receipt).unwrap_or_default());
    output
}

pub(crate) fn observe_chunk_sync<R>(run: impl FnOnce() -> R, io: Option<&PreparationIo>) -> R {
    if !CHUNK_METER.get()
        || acyclic_test_support::differential::reserve::<(&'static str, u64)>(0).is_none()
    {
        return run();
    }
    let before = io.map(PreparationIo::receipt).unwrap_or_default();
    let output = observe_chunk_poll(run);
    record_chunk_native(before, io.map(PreparationIo::receipt).unwrap_or_default());
    output
}

fn observe_chunk_poll<R>(run: impl FnOnce() -> R) -> R {
    let capacity = acyclic_test_support::differential::reserve::<(&'static str, u64)>(64).unwrap();
    let mut output = None;
    let (_, items) = super::measure_task_volume_work(|| {
        let allocations = acyclic_test_support::measure(|| output = Some(run()));
        assert_eq!(
            acyclic_test_support::differential::reserve::<(&'static str, u64)>(0),
            Some(capacity)
        );
        record("chunk.allocations", allocations.count_total);
        record("chunk.allocated-bytes", allocations.bytes_total);
    });
    record("chunk.items", items);
    output.unwrap()
}

fn record(name: &'static str, value: u64) {
    acyclic_test_support::differential::record((name, value));
}

fn record_chunk_native(before: WorkCounters, after: WorkCounters) {
    record(
        "chunk.native-allocations",
        after.allocation_operations - before.allocation_operations,
    );
    record(
        "chunk.native-copy",
        after.bytes_copied - before.bytes_copied,
    );
    record(
        "chunk.native-encoding",
        after.bytes_encoded - before.bytes_encoded,
    );
    record(
        "chunk.native-hash",
        after.bytes_hashed - before.bytes_hashed,
    );
}

thread_local! {
    static CHUNK_COPY: std::cell::Cell<bool> = const { std::cell::Cell::new(false) };
}
pub(crate) fn chunk_copy_enabled() -> bool {
    CHUNK_COPY.get()
}
pub(crate) fn with_chunk_copy<R>(enabled: bool, run: impl FnOnce() -> R) -> R {
    struct Reset(bool);
    impl Drop for Reset {
        fn drop(&mut self) {
            CHUNK_COPY.set(self.0);
        }
    }
    let _reset = Reset(CHUNK_COPY.replace(enabled));
    with_chunk_meter(run)
}

thread_local! {
    static APPEND_METER: std::cell::Cell<bool> = const { std::cell::Cell::new(false) };
    static APPEND_SCAN: std::cell::Cell<bool> = const { std::cell::Cell::new(false) };
}
pub(crate) fn append_scan_enabled() -> bool {
    APPEND_SCAN.get()
}
pub(crate) fn with_append_scan<R>(scan: bool, run: impl FnOnce() -> R) -> R {
    struct Reset(bool, bool);
    impl Drop for Reset {
        fn drop(&mut self) {
            APPEND_METER.set(self.0);
            APPEND_SCAN.set(self.1);
        }
    }
    let _reset = Reset(APPEND_METER.replace(true), APPEND_SCAN.replace(scan));
    run()
}
// Whole production append, including construction, payload authentication,
// canonical replay, retries, publication and every future poll. Projection
// measurements remain separate and cannot nest this observer.
pub(crate) async fn observe_append<F: std::future::Future>(
    future: F,
    io: Option<&PreparationIo>,
) -> F::Output {
    if !APPEND_METER.get() {
        return future.await;
    }
    assert!(!CHUNK_METER.get());
    let before = io.map(PreparationIo::receipt).unwrap_or_default();
    let mut future = std::pin::pin!(future);
    let output = futures::future::poll_fn(|cx| {
        let capacity = acyclic_test_support::differential::reserve::<(&'static str, u64)>(
            64 * crate::executor::EXECUTION_REPLAY_PAGE_RECORDS as usize,
        )
        .unwrap();
        let mut result = None;
        let (_, items) = super::measure_task_volume_work(|| {
            let allocations =
                acyclic_test_support::measure(|| result = Some(future.as_mut().poll(cx)));
            assert_eq!(
                acyclic_test_support::differential::reserve::<(&'static str, u64)>(0),
                Some(capacity)
            );
            record("append.allocations", allocations.count_total);
            record("append.allocated-bytes", allocations.bytes_total);
        });
        record("append.items", items);
        result.unwrap()
    })
    .await;
    let after = io.map(PreparationIo::receipt).unwrap_or_default();
    record("append.calls", 1);
    record(
        "append.native-allocations",
        after.allocation_operations - before.allocation_operations,
    );
    record(
        "append.native-copy",
        after.bytes_copied - before.bytes_copied,
    );
    record(
        "append.native-encoding",
        after.bytes_encoded - before.bytes_encoded,
    );
    record(
        "append.native-hash",
        after.bytes_hashed - before.bytes_hashed,
    );
    record(
        "append.native-items",
        after.items_examined - before.items_examined,
    );
    output
}
