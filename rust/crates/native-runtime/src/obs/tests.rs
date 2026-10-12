#![allow(clippy::unwrap_used, clippy::expect_used)]

use acyclic_grpc_observability::tests::Capture;
use std::{future::Future, pin::Pin, task::{Context, Poll, Waker}, time::{Duration, Instant}};
use tracing_subscriber::{Layer as _, layer::SubscriberExt as _};

#[test]
fn admitted_work_keeps_observation_after_observer_drop() {
    for (native_control, filtered) in [(false, false), (true, false), (false, true), (true, true)] {
        let temporary = tempfile::tempfile().unwrap();
        let native = crate::NativeFile::from_file(temporary.try_clone().unwrap(), None)
            .map_err(|(error, _)| error).unwrap();
        let seen = Capture::default();
        let origin = tracing::Dispatch::new(tracing_subscriber::registry().with(seen.clone().with_filter(
            tracing_subscriber::filter::filter_fn(move |metadata| !filtered || *metadata.level() == tracing::Level::INFO))));
        let foreign = tracing::Dispatch::new(tracing_subscriber::registry());
        let (started_tx, started_rx) = std::sync::mpsc::channel();
        let (release_tx, release_rx) = std::sync::mpsc::channel();
        let (done_tx, done_rx) = std::sync::mpsc::channel();
        let mut observer: Box<dyn Future<Output = std::io::Result<()>> + Unpin> =
            tracing::dispatcher::with_default(&origin, || tracing::info_span!("test.caller", outcome = "sentinel", error.kind = "sentinel").in_scope(|| {
                let work = move || {
                    started_tx.send(()).unwrap();
                    release_rx.recv_timeout(Duration::from_secs(10)).unwrap();
                    drop(tracing::info_span!("test.worker_nested"));
                };
                if native_control {
                    Box::new(native.control_async(move |file, _| {
                        work();
                        let result = file.set_len(73);
                        done_tx.send(()).unwrap();
                        result
                    }, None).expect("unmetered control construction cannot fail"))
                } else {
                    Box::new(crate::run_blocking_io(move || { work(); done_tx.send(()).unwrap(); }, None)
                        .expect("unmetered task construction cannot fail"))
                }
            }));
        assert!(matches!(Pin::new(&mut observer).poll(&mut Context::from_waker(Waker::noop())), Poll::Pending));
        started_rx.recv_timeout(Duration::from_secs(10)).unwrap();
        drop(observer);
        tracing::dispatcher::with_default(&foreign, || release_tx.send(()).unwrap());
        done_rx.recv_timeout(Duration::from_secs(10)).unwrap();
        let deadline = Instant::now() + Duration::from_secs(10);
        while seen.closed("test.caller") == 0 {
            assert!(Instant::now() < deadline);
            std::thread::sleep(Duration::from_millis(1));
        }
        assert!(seen.has_ancestor("test.worker_nested", "test.caller"));
        assert!(!seen.0.lock().unwrap().records.iter().any(|(name, _, _)| name == "test.caller"));
        if native_control { assert_eq!(temporary.metadata().unwrap().len(), 73); }
    }
}
