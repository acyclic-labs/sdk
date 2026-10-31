//! Independent terminal, progress and physical disabled-path witnesses.
#![cfg(not(target_arch = "wasm32"))]

use acyclic_observability::{Metrics, Operations};
use std::{
    future::Future,
    pin::pin,
    task::{Context, Poll, Waker},
};
use tracing_subscriber::prelude::*;

acyclic_test_support::counting_allocator!();

fn operation(fail: bool) {
    let _ =
        acyclic_observability::operation!("acyclic.test.operation", |()| "refused", || if fail {
            Err(())
        } else {
            Ok(())
        });
}

async fn async_operation() -> Result<(), ()> {
    acyclic_observability::async_operation!(
        "acyclic.test.async_operation",
        |()| "refused",
        acyclic_native_runtime::obs::OperationSpan::new,
        std::future::ready(Ok(()))
    )
}

fn ready<F: Future>(future: F) -> Result<F::Output, std::io::Error> {
    match pin!(future)
        .as_mut()
        .poll(&mut Context::from_waker(Waker::noop()))
    {
        Poll::Ready(value) => Ok(value),
        Poll::Pending => Err(std::io::Error::other(
            "producer event did not settle its query",
        )),
    }
}

#[test]
fn credited_export_keeps_original_cause_and_cancellation_with_a_blocked_sink()
-> Result<(), Box<dyn std::error::Error>> {
    use acyclic_observability::{Export, Outcome, Sink};
    use std::sync::{Arc, mpsc};
    use tokio::sync::{Semaphore, oneshot};
    let credit = Arc::new(Semaphore::new(1));
    let (export, mut output) = Export::channel(credit.clone(), true);
    let metrics = Metrics::default();
    let query = metrics.query("acyclic.test.export");
    let dispatch = tracing::Dispatch::new(
        tracing_subscriber::registry().with(Operations((metrics, export.clone()))),
    );
    let baseline = export.stats();
    let (send, receive) = mpsc::channel::<()>();
    drop(send);
    let nested = std::io::Error::new(
        std::io::ErrorKind::BrokenPipe,
        receive.recv().err().ok_or("producer unexpectedly alive")?,
    );
    let nested_kind = acyclic_native_runtime::obs::kind(&nested);
    let original = std::io::Error::other(nested);
    // The flattening negative control loses the actual channel producer cause.
    assert_ne!(acyclic_native_runtime::obs::kind(&original), nested_kind);
    let original = tracing::dispatcher::with_default(&dispatch, || {
        let span = tracing::info_span!(
            "acyclic.test.export",
            outcome = tracing::field::Empty,
            error.kind = tracing::field::Empty
        );
        span.record("error.kind", nested_kind);
        span.record("error.kind", "other");
        acyclic_observability::finish(&span, Err::<(), _>(original), |_| "other")
    })
    .err()
    .ok_or("original failure lost")?;
    assert_eq!(
        original
            .get_ref()
            .and_then(|error| error.downcast_ref::<std::io::Error>())
            .map(std::io::Error::kind),
        Some(std::io::ErrorKind::BrokenPipe)
    );
    let held = ready(output.recv())?.ok_or("terminal output absent")?;
    assert_eq!(held.sample().outcome, Outcome::Err);
    assert_eq!(held.sample().error_kind, Some(nested_kind));
    let refused = acyclic_test_support::measure(|| export.record(&held.sample()));
    assert_eq!(
        refused.count_total, 0,
        "refused output copied evidence before credit"
    );
    assert_eq!(
        credit.available_permits(),
        0,
        "dequeue released consumer-owned credit"
    );

    // The output consumer holds its only credit. Cancelling original work must
    // still retire its receiver and update the independent selected query.
    let (send, receive) = oneshot::channel::<()>();
    tracing::dispatcher::with_default(&dispatch, || {
        let span = tracing::info_span!(
            "acyclic.test.export",
            outcome = tracing::field::Empty,
            error.kind = tracing::field::Empty
        );
        let future = acyclic_observability::observe(&span, receive, |_| "producer_closed");
        let mut future = Box::pin(future);
        assert!(
            future
                .as_mut()
                .poll(&mut Context::from_waker(Waker::noop()))
                .is_pending()
        );
        drop(future);
    });
    assert!(
        send.send(()).is_err(),
        "blocked output retained cancelled work"
    );
    assert_eq!((query.snapshot().err, query.snapshot().incomplete), (1, 1));
    drop(held);
    let after = export.stats();
    assert_eq!(after.retained, 0);
    assert_eq!(after.high_water, 1);
    assert_eq!(after.full, Some(2));
    // A sampled maximum misses the whole admitted/retired record interval.
    assert_eq!(baseline.retained.max(after.retained), 0);
    assert!(after.high_water > baseline.retained.max(after.retained));
    assert_eq!(credit.available_permits(), 1);

    drop(output);
    tracing::dispatcher::with_default(&dispatch, || {
        let span = tracing::info_span!("acyclic.test.export", outcome = tracing::field::Empty);
        acyclic_observability::record_outcome(&span, None);
    });
    assert_eq!(export.stats().closed, Some(1));
    assert_eq!(query.snapshot().ok, 1);
    assert_eq!(credit.available_permits(), 1);
    let (producer, mut receiver) = Export::channel(credit, false);
    let dispatch =
        tracing::Dispatch::new(tracing_subscriber::registry().with(Operations(producer.clone())));
    tracing::dispatcher::with_default(&dispatch, || operation(true));
    let record = ready(receiver.recv())?.ok_or("detail-free terminal absent")?;
    assert_eq!(record.sample().error_kind, None);
    drop(dispatch);
    drop(producer);
    assert!(ready(receiver.recv())?.is_none());
    drop(record);
    Ok(())
}

#[test]
fn one_terminal_sample_survives_retained_handles_and_producer_loss()
-> Result<(), Box<dyn std::error::Error>> {
    use std::sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    };
    struct WakeCount(AtomicUsize);
    impl std::task::Wake for WakeCount {
        fn wake(self: Arc<Self>) {
            self.0.fetch_add(1, Ordering::Relaxed);
        }
        fn wake_by_ref(self: &Arc<Self>) {
            self.0.fetch_add(1, Ordering::Relaxed);
        }
    }
    let metrics = Metrics::default();
    let mut query = metrics.query("acyclic.test.operation");
    let mut unrelated = metrics.query("acyclic.test.unrelated");
    let dispatch =
        tracing::Dispatch::new(tracing_subscriber::registry().with(Operations(metrics.clone())));
    let wakes = Arc::new(WakeCount(AtomicUsize::new(0)));
    let waker = Waker::from(Arc::clone(&wakes));
    let mut context = Context::from_waker(&waker);
    let mut pending = Box::pin(query.changed());
    assert!(pending.as_mut().poll(&mut context).is_pending());
    let retained = tracing::dispatcher::with_default(&dispatch, || {
        let span = tracing::info_span!("acyclic.test.operation", outcome = tracing::field::Empty);
        span.record("outcome", "ok");
        span.record("outcome", "err"); // Duplicate completion must not change the first terminal result.
        span.clone()
    });
    assert!(
        wakes.0.load(Ordering::Relaxed) > 0,
        "completion did not wake its selected query"
    );
    let first = ready(pending.as_mut())??;
    drop(pending);
    assert_eq!((first.ok, first.err, first.incomplete), (1, 0, 0));
    assert_eq!(first.latency_ns.iter().sum::<u64>(), 1);
    assert!(
        pin!(unrelated.changed())
            .as_mut()
            .poll(&mut Context::from_waker(Waker::noop()))
            .is_pending()
    );
    tracing::dispatcher::with_default(&dispatch, || {
        operation(true);
        drop(tracing::info_span!(
            "acyclic.test.operation",
            outcome = tracing::field::Empty
        ));
    });
    drop(retained);
    let last = ready(query.changed())??;
    assert_eq!((last.ok, last.err, last.incomplete), (1, 1, 1));
    assert_eq!(last.latency_ns.iter().sum::<u64>(), 3);
    // Negative schedule: a child retains the span after its producer future is
    // abandoned. The owner, rather than the last tracing handle, must notify.
    let (retained, abandoned) = tracing::dispatcher::with_default(&dispatch, || {
        let span = tracing::info_span!("acyclic.test.operation", outcome = tracing::field::Empty);
        let future = acyclic_observability::observe(
            &span,
            std::future::pending::<Result<(), ()>>(),
            |()| "refused",
        );
        (span, future)
    });
    let mut pending = Box::pin(query.changed());
    assert!(pending.as_mut().poll(&mut context).is_pending());
    drop(abandoned);
    let abandoned = ready(pending.as_mut())??;
    drop(pending);
    assert_eq!(abandoned.incomplete, 2);
    drop(retained);
    assert!(
        pin!(query.changed())
            .as_mut()
            .poll(&mut context)
            .is_pending()
    );
    // The negative schedule omits every completion. Losing its producer must
    // settle by the same watch-close event instead of waiting for another sample.
    let mut closed = Box::pin(query.changed());
    assert!(closed.as_mut().poll(&mut context).is_pending());
    let before = wakes.0.load(Ordering::Relaxed);
    drop(dispatch);
    drop(metrics);
    assert!(
        wakes.0.load(Ordering::Relaxed) > before,
        "producer loss did not wake its selected query"
    );
    assert_eq!(
        ready(closed.as_mut())?,
        Err(acyclic_observability::QueryError)
    );
    assert_eq!(
        ready(unrelated.changed())?,
        Err(acyclic_observability::QueryError)
    );
    Ok(())
}

#[test]
fn disabled_operation_allocates_nothing_with_enabled_negative_control()
-> Result<(), Box<dyn std::error::Error>> {
    tracing::dispatcher::with_default(&tracing::Dispatch::none(), || {
        operation(false);
        assert!(matches!(ready(async_operation()), Ok(Ok(()))));
        for count in [1, 100_000] {
            let work = acyclic_test_support::measure(|| {
                for _ in 0..count {
                    operation(std::hint::black_box(false));
                    assert!(matches!(ready(async_operation()), Ok(Ok(()))));
                }
            });
            assert_eq!(work.count_total, 0, "disabled instrumentation allocated");
            eprintln!(
                "disabled operations={count}, allocations={}",
                work.count_total
            );
        }
    });
    let metrics = Metrics::default();
    let query = metrics.query("acyclic.test.operation");
    let dispatch = tracing::Dispatch::new(tracing_subscriber::registry().with(Operations(metrics)));
    let enabled = tracing::dispatcher::with_default(&dispatch, || {
        acyclic_test_support::measure(|| operation(false))
    });
    assert!(
        enabled.count_total > 0,
        "negative control failed to detect an attached subscriber"
    );
    eprintln!("enabled operation allocations={}", enabled.count_total);
    assert_eq!(query.snapshot().ok, 1);
    Ok(())
}

#[test]
#[ignore = "on-demand trace cost measurement"]
fn trace_on_off_cost() -> Result<(), Box<dyn std::error::Error>> {
    use acyclic_observability::Export;
    use std::{sync::Arc, time::Instant};
    use tokio::sync::Semaphore;
    const COUNT: usize = 100_000;
    tracing::dispatcher::with_default(&tracing::Dispatch::none(), || {
        operation(false);
        let start = Instant::now();
        let allocations = acyclic_test_support::measure(|| {
            for _ in 0..COUNT {
                operation(std::hint::black_box(false));
            }
        });
        assert_eq!(allocations.count_total, 0);
        eprintln!(
            "trace=off operations={COUNT} elapsed_ns={} allocations={}",
            start.elapsed().as_nanos(),
            allocations.count_total
        );
    });
    let metrics = Metrics::default();
    let query = metrics.query("acyclic.test.operation");
    let dispatch = tracing::Dispatch::new(tracing_subscriber::registry().with(Operations(metrics)));
    tracing::dispatcher::with_default(&dispatch, || {
        operation(false);
        let start = Instant::now();
        let allocations = acyclic_test_support::measure(|| {
            for _ in 0..COUNT {
                operation(std::hint::black_box(false));
            }
        });
        eprintln!(
            "trace=metrics operations={COUNT} elapsed_ns={} allocations={}",
            start.elapsed().as_nanos(),
            allocations.count_total
        );
    });
    assert_eq!(query.snapshot().ok, COUNT as u64 + 1);
    let (export, mut output) = Export::channel(Arc::new(Semaphore::new(1)), true);
    let dispatch =
        tracing::Dispatch::new(tracing_subscriber::registry().with(Operations(export.clone())));
    tracing::dispatcher::with_default(&dispatch, || -> Result<(), std::io::Error> {
        operation(true);
        drop(ready(output.recv())?);
        let start = Instant::now();
        let mut result = Ok(());
        let allocations = acyclic_test_support::measure(|| {
            result = (|| -> Result<(), std::io::Error> {
                for _ in 0..COUNT {
                    operation(std::hint::black_box(true));
                    drop(ready(output.recv())?.ok_or_else(|| {
                        std::io::Error::other("export producer closed before its terminal record")
                    })?);
                }
                Ok(())
            })();
        });
        result?;
        eprintln!(
            "trace=detail-export operations={COUNT} elapsed_ns={} allocations={}",
            start.elapsed().as_nanos(),
            allocations.count_total
        );
        Ok(())
    })?;
    assert_eq!(export.stats().full, Some(0));
    assert_eq!(export.stats().retained, 0);
    assert_eq!(export.stats().high_water, 1);
    Ok(())
}

#[test]
fn filtered_operation_cannot_complete_its_enabled_parent() {
    let metrics = Metrics::default();
    let parent = metrics.query("acyclic.test.parent");
    let dispatch = tracing::Dispatch::new(tracing_subscriber::registry().with(
        Operations(metrics).with_filter(tracing_subscriber::filter::filter_fn(|metadata| {
            metadata.name() != "acyclic.test.operation"
        })),
    ));
    tracing::dispatcher::with_default(&dispatch, || {
        let span = tracing::info_span!("acyclic.test.parent", outcome = tracing::field::Empty);
        span.in_scope(|| operation(true));
        assert_eq!(parent.snapshot().err, 0);
        // The former current-span completion path would mutate this parent.
        span.in_scope(|| {
            acyclic_observability::record_outcome(&tracing::Span::current(), Some("refused"));
        });
        assert_eq!(parent.snapshot().err, 1);
    });
}

#[test]
fn method_queries_preserve_the_complete_typed_key() {
    let metrics = Metrics::default();
    let without_method = metrics.query("acyclic.test.rpc");
    let empty_method = metrics.query_rpc("acyclic.test.rpc", "");
    let method = metrics.query_rpc("acyclic.test.rpc", "Read\0");
    let embedded_separator = metrics.query("acyclic.test.rpc\0Read");
    let dispatch = tracing::Dispatch::new(tracing_subscriber::registry().with(Operations(metrics)));
    tracing::dispatcher::with_default(&dispatch, || {
        let span = tracing::info_span!(
            "acyclic.test.rpc",
            rpc = "Read\0",
            outcome = tracing::field::Empty
        );
        acyclic_observability::record_outcome(&span, None);
    });
    assert_eq!(method.snapshot().ok, 1);
    assert_eq!(without_method.snapshot().ok, 0);
    assert_eq!(empty_method.snapshot().rpc.as_deref(), Some(""));
    assert_eq!(embedded_separator.snapshot().ok, 0);
    // A delimiter-only index aliases these two keys despite their different
    // operation identities. The selected query above must remain isolated.
    assert_eq!(
        format!("{}\0{}", "acyclic.test.rpc", "Read\0"),
        format!("{}\0", "acyclic.test.rpc\0Read"),
    );
}

#[test]
fn slo_file_refuses_empty_and_ambiguous_evidence() -> Result<(), Box<dyn std::error::Error>> {
    use acyclic_observability::{Slo, Snapshot};
    let file = br#"{"version":1,"operation":"acyclic.test.operation","success_ppm":990000,"latency":{"maximum_ns":10,"target_ppm":990000}}"#;
    let mut slo: Slo = serde_json::from_slice(file)?;
    let metrics = Metrics::default();
    let mut live = metrics.query_slo(slo.clone())?;
    assert_eq!(live.snapshot().verdict.success, None);
    let dispatch =
        tracing::Dispatch::new(tracing_subscriber::registry().with(Operations(metrics.clone())));
    tracing::dispatcher::with_default(&dispatch, || operation(false));
    assert_eq!(ready(live.changed())??.verdict.success, Some(true));
    // Replacing the file selects its new operation; the previous operation's
    // successful cohort must not leak into the new file's view.
    let mut replacement = slo.clone();
    replacement.operation = "acyclic.test.unrelated".into();
    assert_eq!(
        metrics.query_slo(replacement)?.snapshot().verdict.success,
        None
    );
    let mut snapshot = Snapshot {
        operation: slo.operation.clone(),
        latency_ns: vec![0; 129],
        ..Snapshot::default()
    };
    assert_eq!(slo.evaluate(&snapshot)?.success, None);
    snapshot.ok = 1;
    snapshot.latency_ns[4] = 1; // [8,15] ns crosses the requested threshold.
    let verdict = slo.evaluate(&snapshot)?;
    assert_eq!((verdict.success, verdict.latency), (Some(true), None));
    slo.latency
        .as_mut()
        .ok_or("missing latency SLO")?
        .maximum_ns = 15;
    assert_eq!(slo.evaluate(&snapshot)?.latency, Some(true));
    snapshot.err = 1;
    snapshot.latency_ns[5] = 1;
    let verdict = slo.evaluate(&snapshot)?;
    assert_eq!(
        (verdict.success, verdict.latency),
        (Some(false), Some(false))
    );
    slo.version = 2;
    assert_eq!(
        slo.evaluate(&snapshot),
        Err(acyclic_observability::SloError::Version)
    );
    Ok(())
}

#[test]
#[ignore = "subprocess entry point for the syscall counting witness"]
fn syscall_probe() -> Result<(), Box<dyn std::error::Error>> {
    use std::io::Write as _;
    assert!(std::env::var_os("ACYCLIC_OBSERVATION_PROBE").is_some());
    let negative = std::env::var_os("ACYCLIC_OBSERVATION_NEGATIVE").is_some();
    operation(false);
    assert!(matches!(ready(async_operation()), Ok(Ok(()))));
    let mut output = std::io::stderr().lock();
    output.write_all(b"observation-begin\n")?;
    let mut negative_result = Ok(());
    let allocations = acyclic_test_support::measure(|| {
        for _ in 0..100_000 {
            operation(false);
            assert!(matches!(ready(async_operation()), Ok(Ok(()))));
        }
        if negative {
            negative_result = std::fs::File::open("/dev/null").map(drop);
        }
    });
    output.write_all(b"observation-end\n")?;
    negative_result?;
    if !negative {
        assert_eq!(allocations.count_total, 0);
    }
    Ok(())
}

#[cfg(target_os = "linux")]
#[test]
#[ignore = "requires strace; run explicitly to count actual kernel crossings"]
fn disabled_operation_has_zero_syscalls_with_real_io_negative_control()
-> Result<(), Box<dyn std::error::Error>> {
    for negative in [false, true] {
        let mut command = std::process::Command::new("strace");
        command
            .args(["-f", "-qq", "-s", "256"])
            .arg(std::env::current_exe()?)
            .args([
                "--exact",
                "syscall_probe",
                "--ignored",
                "--nocapture",
                "--test-threads=1",
            ])
            .env("ACYCLIC_OBSERVATION_PROBE", "1");
        if negative {
            command.env("ACYCLIC_OBSERVATION_NEGATIVE", "1");
        }
        let result = command.output().map_err(|error| std::io::Error::new(error.kind(), format!("syscall trace failed to start: {error}; install strace and rerun cargo test -p acyclic-observability --test operations disabled_operation_has_zero_syscalls_with_real_io_negative_control -- --ignored --exact")))?;
        assert!(
            result.status.success(),
            "syscall probe failed: {}",
            String::from_utf8_lossy(&result.stderr)
        );
        let trace = String::from_utf8(result.stderr)?;
        let mut lines = trace
            .lines()
            .skip_while(|line| !line.contains("observation-begin"));
        assert!(lines.next().is_some(), "missing trace start marker");
        let mut calls = 0;
        let mut ended = false;
        for line in lines {
            if line.contains("observation-end") {
                ended = true;
                break;
            }
            if line.contains('(') {
                calls += 1;
            }
        }
        assert!(ended, "missing trace end marker");
        eprintln!(
            "sync_operations=100000, async_operations=100000, real_io_negative={negative}, syscalls={calls}"
        );
        assert_eq!(
            calls == 0,
            !negative,
            "unexpected syscall count {calls}: {trace}"
        );
    }
    Ok(())
}
