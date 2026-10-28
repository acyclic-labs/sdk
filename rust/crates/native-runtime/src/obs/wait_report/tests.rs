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
