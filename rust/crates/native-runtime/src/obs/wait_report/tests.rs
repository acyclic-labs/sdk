use super::*;
use std::{future::Future, pin::Pin, task::Poll};
use tracing_subscriber::layer::SubscriberExt as _;

type Wait = Pin<Box<crate::obs::Waiting<tokio::sync::oneshot::Receiver<()>, usize>>>;

fn pending(report: &WaitReport, key: usize) -> (tokio::sync::oneshot::Sender<()>, Wait) {
    let dispatch = tracing::Dispatch::new(tracing_subscriber::registry().with(report.clone()));
    let (sender, receiver) = tokio::sync::oneshot::channel();
    let wait = tracing::dispatcher::with_default(&dispatch, || {
        crate::obs::waiting("channel-close", key, receiver)
    });
    (sender, Box::pin(wait))
}

/// A wait whose producer is pending, so it is reported until dropped.
fn blocked(report: &WaitReport, key: usize) -> (tokio::sync::oneshot::Sender<()>, Wait) {
    let (sender, mut wait) = pending(report, key);
    assert!(poll(&mut wait).is_pending());
    (sender, wait)
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
fn synchronous_wait_retires_on_reply_producer_loss_and_unwind() {
    for outcome in ["reply", "producer-dropped", "unwind"] {
        let report = WaitReport::default();
        let dispatch = tracing::Dispatch::new(tracing_subscriber::registry().with(report.clone()));
        std::thread::scope(|scope| {
            // Release lives inside this scope: an assertion failure drops the
            // sole producer before the scope joins the original receiver.
            let (release, receiver) = std::sync::mpsc::channel();
            let (started, entered) = std::sync::mpsc::channel();
            let work = scope.spawn(move || {
                tracing::dispatcher::with_default(&dispatch, || {
                    crate::obs::waiting_sync("channel-close", outcome, || {
                        started.send(tracing::info_span!("retained-child")).unwrap();
                        let result = receiver.recv();
                        if outcome == "unwind" {
                            panic!("unwind after original producer closes");
                        }
                        result
                    })
                })
            });
            let child = entered.recv().unwrap();
            assert!(snapshot(&report).contains(&format!("channel-close {outcome} since ")));
            if outcome == "reply" {
                release.send(29).unwrap();
            }
            drop(release);
            match (outcome, work.join()) {
                ("reply", Ok(Ok(value))) => assert_eq!(value, 29),
                ("producer-dropped", Ok(Err(std::sync::mpsc::RecvError))) | ("unwind", Err(_)) => {}
                (_, result) => panic!("unexpected synchronous wait result: {result:?}"),
            }
            // A retained child must not keep a completed channel wait listed.
            assert!(snapshot(&report).is_empty());
            drop(child);
        });
    }
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
        assert!(snapshot(&report).is_empty());
        drop(wait);
    });
    assert!(snapshot(&report).is_empty());
    assert!(snapshot(&foreign).is_empty());
}

#[test]
fn a_retained_descendant_does_not_retain_a_finished_or_cancelled_wait() {
    for outcome in ["completed", "producer-dropped", "cancelled"] {
        let report = WaitReport::default();
        let dispatch = tracing::Dispatch::new(tracing_subscriber::registry().with(report.clone()));
        let (sender, receiver) = tokio::sync::oneshot::channel::<()>();
        let (child_sender, child_receiver) = std::sync::mpsc::channel();
        let mut wait = Box::pin(tracing::dispatcher::with_default(&dispatch, || {
            crate::obs::waiting("channel-close", outcome, async move {
                // The report opens on the first pending poll; the child is
                // created after it, under the open report.
                let mut first = true;
                std::future::poll_fn(|_| {
                    if std::mem::take(&mut first) {
                        Poll::Pending
                    } else {
                        Poll::Ready(())
                    }
                })
                .await;
                child_sender
                    .send(tracing::info_span!("retained-child"))
                    .unwrap();
                receiver.await
            })
        }));
        let mut cx = std::task::Context::from_waker(std::task::Waker::noop());
        assert!(wait.as_mut().poll(&mut cx).is_pending());
        assert!(child_receiver.try_recv().is_err());
        assert!(wait.as_mut().poll(&mut cx).is_pending());
        let child = child_receiver.try_recv().unwrap();
        assert!(snapshot(&report).contains(outcome));
        match outcome {
            "completed" => {
                sender.send(()).unwrap();
                assert!(matches!(wait.as_mut().poll(&mut cx), Poll::Ready(Ok(()))));
                assert!(snapshot(&report).is_empty());
                drop(wait);
            }
            "producer-dropped" => {
                drop(sender);
                assert!(matches!(wait.as_mut().poll(&mut cx), Poll::Ready(Err(_))));
                assert!(snapshot(&report).is_empty());
                drop(wait);
            }
            _ => drop(wait),
        }
        // Negative control: the original layer kept this parent listed until
        // the child closed, even though no primitive wait remained.
        assert!(snapshot(&report).is_empty(), "{outcome}");
        drop(child);
        assert!(snapshot(&report).is_empty());
    }
}

#[test]
fn closing_adjacent_waits_preserves_all_other_live_keys() {
    let report = WaitReport::default();
    let mut waits: Vec<_> = (0..7).map(|key| Some(blocked(&report, key))).collect();
    // Exercise middle, first, last and newly adjacent removals. Only the
    // owner's two neighbors change; reporting must retain every other key.
    for key in [3, 0, 6, 2, 4] {
        drop(waits.get_mut(key).unwrap().take());
    }
    let lines = snapshot(&report);
    assert_eq!(lines.lines().count(), 2);
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
    let (_sender, wait) = blocked(&report, 2048);
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
    let (_sender, wait) = blocked(&report, 23);
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
fn a_wait_ready_on_its_first_poll_opens_no_span() {
    struct Opened(std::sync::Arc<std::sync::atomic::AtomicUsize>);
    impl<S: tracing::Subscriber> tracing_subscriber::Layer<S> for Opened {
        fn on_new_span(
            &self,
            _: &tracing::span::Attributes<'_>,
            _: &tracing::span::Id,
            _: tracing_subscriber::layer::Context<'_, S>,
        ) {
            self.0.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        }
    }
    let report = WaitReport::default();
    let opened = std::sync::Arc::default();
    let dispatch = tracing::Dispatch::new(
        tracing_subscriber::registry()
            .with(report.clone())
            .with(Opened(std::sync::Arc::clone(&opened))),
    );
    let mut cx = std::task::Context::from_waker(std::task::Waker::noop());
    tracing::dispatcher::with_default(&dispatch, || {
        let mut ready = Box::pin(crate::obs::waiting(
            "channel-close",
            1,
            std::future::ready(()),
        ));
        assert!(ready.as_mut().poll(&mut cx).is_ready());
        drop(ready);
        assert_eq!(opened.load(std::sync::atomic::Ordering::SeqCst), 0);
        // Negative control: the same subscriber records a wait that blocks.
        let mut blocked = Box::pin(crate::obs::waiting(
            "channel-close",
            2,
            std::future::pending::<()>(),
        ));
        assert!(blocked.as_mut().poll(&mut cx).is_pending());
        assert_eq!(opened.load(std::sync::atomic::Ordering::SeqCst), 1);
        assert!(snapshot(&report).starts_with("waiting: channel-close 2 since "));
        drop(blocked);
        assert!(snapshot(&report).is_empty());
    });
}

#[test]
fn a_subscriber_admitting_only_wait_spans_reports_them() {
    use tracing_subscriber::Layer as _;
    // The CLI admits exactly the wait span's target and name.
    let report = WaitReport::default();
    let only_waits = tracing_subscriber::filter::filter_fn(|metadata| {
        metadata.target() == "acyclic.wait" && metadata.name() == "waiting"
    });
    let dispatch = tracing::Dispatch::new(
        tracing_subscriber::registry().with(report.clone().with_filter(only_waits)),
    );
    tracing::dispatcher::with_default(&dispatch, || {
        let wait = crate::obs::blocked("channel-close", 3, std::future::pending::<()>());
        assert!(snapshot(&report).starts_with("waiting: channel-close 3 since "));
        drop(wait);
    });
    assert!(snapshot(&report).is_empty());
    // Negative control: a filter refusing wait spans reports none.
    let report = WaitReport::default();
    let no_waits = tracing_subscriber::filter::filter_fn(|metadata| metadata.name() != "waiting");
    let dispatch = tracing::Dispatch::new(
        tracing_subscriber::registry().with(report.clone().with_filter(no_waits)),
    );
    tracing::dispatcher::with_default(&dispatch, || {
        let wait = crate::obs::blocked("channel-close", 4, std::future::pending::<()>());
        assert!(snapshot(&report).is_empty());
        drop(wait);
    });
}
