#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    reason = "fixture failures abort the test directly"
)]

use acyclic_grpc_observability::tests::Capture;
use std::{
    future::Future,
    pin::Pin,
    task::{Context, Poll, Waker},
    time::{Duration, Instant},
};
use tracing_subscriber::{Layer as _, layer::SubscriberExt as _};

fn assert_context(
    origin_kind: u8,
    parent_enabled: bool,
    operation_kind: u8,
    actions: &[(bool, u8)],
) {
    use std::{cell::RefCell, rc::Rc};
    use tracing::callsite::Callsite as _;

    // Every clone/drop sequence must preserve the creator's dispatcher and ancestry,
    // even when the caller has a foreign parent or the operation was filtered.
    struct Producer(Rc<RefCell<Vec<Option<tracing::Id>>>>);
    impl Future for Producer {
        type Output = ();
        fn poll(self: Pin<&mut Self>, _: &mut Context<'_>) -> Poll<()> {
            self.0.borrow_mut().push(tracing::Span::current().id());
            drop(tracing::info_span!("test.context.poll"));
            Poll::Ready(())
        }
    }
    impl Drop for Producer {
        fn drop(&mut self) {
            self.0.borrow_mut().push(tracing::Span::current().id());
            drop(tracing::info_span!("test.context.drop"));
        }
    }

    let seen = Capture::default();
    let origin = match origin_kind {
        0 => tracing::Dispatch::none(),
        1 => tracing::Dispatch::new(
            tracing_subscriber::registry().with(
                seen.clone()
                    .with_filter(tracing_subscriber::filter::LevelFilter::INFO),
            ),
        ),
        _ => tracing::Dispatch::new(tracing_subscriber::registry().with(seen.clone())),
    };
    let operation = tracing::dispatcher::with_default(&origin, || {
        let parent = if parent_enabled {
            tracing::info_span!("test.origin")
        } else {
            tracing::Span::none()
        };
        parent.in_scope(|| {
            super::OperationSpan::new(match operation_kind {
                0 => tracing::Span::none(),
                1 => tracing::debug_span!("test.operation"),
                _ => tracing::Span::new_disabled(
                    tracing::callsite! {
                        name: "test.filtered",
                        kind: tracing::metadata::Kind::SPAN,
                        level: tracing::Level::DEBUG,
                        fields:
                    }
                    .metadata(),
                ),
            })
        })
    });
    // NoSubscriber uses a sentinel ID for unfiltered callsites without tracking it.
    let expected = (origin_kind != 0)
        .then(|| operation.context().id())
        .flatten();
    let parents = Rc::new(RefCell::new(Vec::with_capacity(2)));
    let metadata = operation.metadata().map(|metadata| metadata.name());
    drop(origin);
    let foreign_seen = Capture::default();
    let foreign = tracing::Dispatch::new(tracing_subscriber::registry().with(foreign_seen.clone()));
    tracing::dispatcher::with_default(&foreign, || {
        let parent = tracing::info_span!("test.foreign");
        let _entered = parent.enter();
        let mut copies = vec![operation];
        for &(clone, index) in actions {
            let index = usize::from(index) % copies.len();
            if clone {
                copies.push(copies.get(index).unwrap().clone());
            } else if copies.len() > 1 {
                drop(copies.swap_remove(index));
            }
        }
        let cloned = copies.pop().unwrap();
        assert_eq!(cloned.metadata().map(|metadata| metadata.name()), metadata);
        let mut producer = Box::pin(cloned.scope(Producer(parents.clone())));
        drop(cloned);
        drop(copies);
        assert!(
            producer
                .as_mut()
                .poll(&mut Context::from_waker(Waker::noop()))
                .is_ready()
        );
        drop(producer);
        assert_eq!(tracing::Span::current().id(), parent.id());
    });
    assert_eq!(*parents.borrow(), [expected.clone(), expected]);
    for name in ["test.context.poll", "test.context.drop"] {
        assert_eq!(seen.closed(name), usize::from(origin_kind != 0));
        assert_eq!(foreign_seen.closed(name), 0);
    }
    assert_eq!(foreign_seen.closed("test.foreign"), 1);
}

proptest::proptest! {
    #[test]
    fn context_clone_retains_dispatcher_and_parent_for_poll_and_drop(
        actions in proptest::collection::vec((proptest::bool::ANY, proptest::num::u8::ANY), 0..128),
    ) {
        for origin_kind in 0..3 {
            for parent in [false, true] {
                for operation in 0..3 {
                    assert_context(origin_kind, parent, operation, &actions);
                }
            }
        }
    }
}

#[test]
fn admitted_work_keeps_observation_after_observer_drop() {
    for (native_control, filtered) in [(false, false), (true, false), (false, true), (true, true)] {
        let temporary = tempfile::tempfile().unwrap();
        let native = crate::NativeFile::from_file(temporary.try_clone().unwrap(), None)
            .map_err(|(error, _)| error)
            .unwrap();
        let seen = Capture::default();
        let origin = tracing::Dispatch::new(
            tracing_subscriber::registry().with(seen.clone().with_filter(
                tracing_subscriber::filter::filter_fn(move |metadata| {
                    !filtered || *metadata.level() == tracing::Level::INFO
                }),
            )),
        );
        let foreign = tracing::Dispatch::new(tracing_subscriber::registry());
        let (started_tx, started_rx) = std::sync::mpsc::channel();
        let (release_tx, release_rx) = std::sync::mpsc::channel();
        let (done_tx, done_rx) = std::sync::mpsc::channel();
        let mut observer: Box<dyn Future<Output = std::io::Result<()>> + Unpin> =
            tracing::dispatcher::with_default(&origin, || {
                tracing::info_span!("test.caller", outcome = "sentinel", error.kind = "sentinel")
                    .in_scope(|| {
                        let work = move || {
                            started_tx.send(()).unwrap();
                            release_rx.recv_timeout(Duration::from_secs(10)).unwrap();
                            drop(tracing::info_span!("test.worker_nested"));
                        };
                        if native_control {
                            Box::new(
                                native
                                    .control_async(
                                        move |file, _| {
                                            work();
                                            let result = file.set_len(73);
                                            done_tx.send(()).unwrap();
                                            result
                                        },
                                        None,
                                    )
                                    .expect("unmetered control construction cannot fail"),
                            )
                        } else {
                            Box::new(
                                crate::run_blocking_io(
                                    move || {
                                        work();
                                        done_tx.send(()).unwrap();
                                    },
                                    None,
                                )
                                .expect("unmetered task construction cannot fail"),
                            )
                        }
                    })
            });
        assert!(matches!(
            Pin::new(&mut observer).poll(&mut Context::from_waker(Waker::noop())),
            Poll::Pending
        ));
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
        assert!(
            !seen
                .0
                .lock()
                .unwrap()
                .records
                .iter()
                .any(|(name, _, _)| name == "test.caller")
        );
        if native_control {
            assert_eq!(temporary.metadata().unwrap().len(), 73);
        }
    }
}
