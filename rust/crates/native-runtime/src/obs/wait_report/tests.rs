use super::*;
use std::{future::Future, pin::Pin, task::Poll};
use tracing_subscriber::layer::SubscriberExt as _;

type Wait = Pin<Box<crate::obs::Scoped<tokio::sync::oneshot::Receiver<()>>>>;

fn pending(report: &WaitReport, key: usize) -> (tokio::sync::oneshot::Sender<()>, Wait) {
    let dispatch = tracing::Dispatch::new(tracing_subscriber::registry().with(report.clone()));
    let (sender, receiver) = tokio::sync::oneshot::channel();
    let wait = tracing::dispatcher::with_default(&dispatch, || {
        crate::obs::waiting("channel-close", key, receiver)
    });
    (sender, Box::pin(wait))
}

fn poll(wait: &mut Wait) -> Poll<Result<(), tokio::sync::oneshot::error::RecvError>> {
    wait.as_mut()
        .poll(&mut std::task::Context::from_waker(std::task::Waker::noop()))
}

fn snapshot(report: &WaitReport) -> String {
    let mut bytes = Vec::new();
    report.report(&mut bytes).unwrap();
    String::from_utf8(bytes).unwrap()
}

#[test]
fn producer_drop_is_typed_and_foreign_polling_keeps_the_original_report() {
    let report = WaitReport::default();
    let foreign = WaitReport::default();
    let (sender, mut wait) = pending(&report, 17);
    let dispatch = tracing::Dispatch::new(tracing_subscriber::registry().with(foreign.clone()));
    tracing::dispatcher::with_default(&dispatch, || {
        assert!(poll(&mut wait).is_pending());
        assert!(snapshot(&report).starts_with("waiting: channel-close 17 since "));
        assert!(snapshot(&foreign).is_empty());
        drop(sender);
        assert!(matches!(poll(&mut wait), Poll::Ready(Err(_))));
        drop(wait);
    });
    assert!(snapshot(&report).is_empty());
    assert!(snapshot(&foreign).is_empty());
}

#[test]
fn closing_adjacent_waits_preserves_all_other_live_keys() {
    let report = WaitReport::default();
    let mut waits: Vec<_> = (0..1024).map(|key| Some(pending(&report, key))).collect();
    // Exercise middle, first, last and newly adjacent removals. Only the
    // owner's two neighbors change; reporting must retain every other key.
    for key in [512, 0, 1023, 511, 513] {
        drop(waits[key].take());
    }
    let lines = snapshot(&report);
    assert_eq!(lines.lines().count(), 1019);
    for (key, wait) in waits.iter().enumerate() {
        assert_eq!(
            lines.contains(&format!("channel-close {key} since ")),
            wait.is_some()
        );
    }
    // Reverse closure exercises every tail update, including the empty list.
    while let Some(wait) = waits.pop() {
        drop(wait);
    }
    assert!(snapshot(&report).is_empty());
    let (_sender, wait) = pending(&report, 2048);
    assert!(snapshot(&report).contains("channel-close 2048 since "));
    drop(wait);
    assert!(snapshot(&report).is_empty());
}

#[test]
fn output_can_close_a_wait_and_failure_does_not_hold_the_registry() {
    struct Output {
        report: WaitReport,
        wait: Option<Wait>,
        bytes: Vec<u8>,
    }
    impl Write for Output {
        fn write(&mut self, bytes: &[u8]) -> io::Result<usize> {
            if self.report.0.try_lock().is_err() {
                return Err(io::ErrorKind::WouldBlock.into());
            }
            drop(self.wait.take());
            self.bytes.extend_from_slice(bytes);
            Ok(bytes.len())
        }
        fn flush(&mut self) -> io::Result<()> {
            Err(io::ErrorKind::BrokenPipe.into())
        }
    }
    let report = WaitReport::default();
    let (_sender, wait) = pending(&report, 23);
    let mut output = Output {
        report: report.clone(),
        wait: Some(wait),
        bytes: Vec::new(),
    };
    // Negative control: writing while holding the registry refuses immediately.
    let held = report.0.lock().unwrap();
    assert_eq!(
        output.write(b"blocked").unwrap_err().kind(),
        io::ErrorKind::WouldBlock
    );
    drop(held);
    assert_eq!(
        report.report(&mut output).unwrap_err().kind(),
        io::ErrorKind::BrokenPipe
    );
    assert!(
        String::from_utf8(output.bytes)
            .unwrap()
            .starts_with("waiting: channel-close 23 since ")
    );
    assert!(snapshot(&report).is_empty());
}

#[test]
fn span_registry_admission_and_retirement_do_not_scan_live_waits() -> io::Result<()> {
    if crate::tests::passed_in_exclusive_process(
        "obs::wait_report::tests::span_registry_admission_and_retirement_do_not_scan_live_waits",
    )? {
        return Ok(());
    }
    for count in [1, 100_000] {
        let report = WaitReport::default();
        let dispatch = tracing::Dispatch::new(tracing_subscriber::registry().with(report.clone()));
        let mut waits = Vec::with_capacity(count);
        tracing::dispatcher::with_default(&dispatch, || {
            REGISTRY_LOCKS.set(0);
            REGISTRY_ACQUIRE_NANOS.set(0);
            let started = Instant::now();
            let admitted = acyclic_test_support::measure(|| {
                for _ in 0..count {
                    waits.push(crate::obs::waiting(
                        "channel-close",
                        "same-key",
                        std::future::pending::<()>(),
                    ));
                }
            });
            let arm = started.elapsed();
            let arm_acquire = REGISTRY_ACQUIRE_NANOS.get();
            #[cfg(target_os = "linux")]
            let rss = std::fs::read_to_string("/proc/self/status")
                .unwrap()
                .lines()
                .find(|line| line.starts_with("VmRSS:"))
                .unwrap()
                .to_owned();
            #[cfg(not(target_os = "linux"))]
            let rss = "RSS source unavailable on this platform";
            assert_eq!(REGISTRY_LOCKS.get(), count);
            REGISTRY_LOCKS.set(0);
            REGISTRY_ACQUIRE_NANOS.set(0);
            let captured = acyclic_test_support::measure(|| {
                let snapshot = report.snapshot();
                assert_eq!(snapshot.nodes.len(), count);
            });
            assert_eq!(REGISTRY_LOCKS.get(), 1);
            assert_eq!(captured.count_total, 1);
            assert_eq!(
                captured.bytes_total,
                (count * std::mem::size_of::<Arc<Node>>()) as u64
            );
            let snapshot = report.snapshot();
            let growing = acyclic_test_support::measure(|| {
                let mut copied = Vec::new();
                for node in &snapshot.nodes {
                    copied.push(Arc::clone(node));
                }
                std::hint::black_box(copied);
            });
            assert!(growing.bytes_total > captured.bytes_total);
            drop(snapshot);
            REGISTRY_LOCKS.set(0);
            REGISTRY_ACQUIRE_NANOS.set(0);
            let started = Instant::now();
            let retired = acyclic_test_support::measure(|| {
                while let Some(wait) = waits.pop() {
                    drop(wait);
                }
            });
            let drop_time = started.elapsed();
            let close_acquire = REGISTRY_ACQUIRE_NANOS.get();
            assert_eq!(REGISTRY_LOCKS.get(), count);
            println!(
                "span-registry waits={count} arm={arm:?} drop={drop_time:?} locks={count}/1/{count} arm-lock-acquire-ns={arm_acquire} close-lock-acquire-ns={close_acquire} rss={rss} admission-allocations={} admission-bytes={} retained-bytes={} snapshot-allocations={} snapshot-bytes={} retirement-allocations={} retirement-bytes={}",
                admitted.count_total,
                admitted.bytes_total,
                admitted.bytes_current,
                captured.count_total,
                captured.bytes_total,
                retired.count_total,
                retired.bytes_total
            );
            // A historical/live-list scan added to each admission fails the
            // same fixed-critical-section oracle, without relying on a timer.
            let negative =
                crate::obs::waiting("channel-close", "control", std::future::pending::<()>());
            REGISTRY_LOCKS.set(0);
            drop(report.snapshot());
            drop(negative);
            assert_ne!(REGISTRY_LOCKS.get(), 1);
        });
    }
    Ok(())
}
