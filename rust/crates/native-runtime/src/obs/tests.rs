#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    reason = "fixture failures abort the test directly"
)]

use acyclic_grpc_observability::tests::Capture;
use acyclic_test_support::differential::oracle::{Observer, Oracle, Stop};
use std::{
    future::Future,
    pin::Pin,
    task::{Context, Poll, Waker},
    time::{Duration, Instant},
};
use tracing_subscriber::{Layer as _, layer::SubscriberExt as _};

#[test]
fn unobserved_future_and_stream_bypass_dispatcher_installation() {
    observation_oracle().check(&Case::Idle);
}

fn unobserved_bypass(broken: bool, see: &mut Observer<'_, Seen>) -> Result<(), Stop> {
    use std::cell::Cell;
    struct Producer<'a>(&'a Cell<usize>);
    impl Future for Producer<'_> {
        type Output = ();
        fn poll(self: Pin<&mut Self>, _: &mut Context<'_>) -> Poll<()> {
            self.0.set(self.0.get() + 1);
            Poll::Ready(())
        }
    }
    impl futures_core::Stream for Producer<'_> {
        type Item = ();
        fn poll_next(self: Pin<&mut Self>, _: &mut Context<'_>) -> Poll<Option<()>> {
            self.0.set(self.0.get() + 1);
            Poll::Ready(Some(()))
        }
    }
    impl Drop for Producer<'_> {
        fn drop(&mut self) {
            self.0.set(self.0.get() + 1);
        }
    }
    tracing::dispatcher::with_default(&tracing::Dispatch::none(), || {
        let mut operation = super::OperationSpan::new(tracing::Span::none());
        if broken {
            // A nonempty origin forces the former dispatcher-installation path.
            operation.dispatch = tracing::Dispatch::new(tracing_subscriber::registry());
        }
        for stream in [false, true] {
            let calls = Cell::new(0);
            DISPATCH_INSTALLS.with(|count| count.set(0));
            let mut producer = Box::pin(operation.scope(Producer(&calls)));
            let mut context = Context::from_waker(Waker::noop());
            let ready = if stream {
                futures_core::Stream::poll_next(producer.as_mut(), &mut context).is_ready()
            } else {
                producer.as_mut().poll(&mut context).is_ready()
            };
            drop(producer);
            let installs = DISPATCH_INSTALLS.with(std::cell::Cell::get);
            see.see(Seen::Idle {
                installs,
                calls: calls.get(),
                ready,
            })?;
        }
        Ok(())
    })
}

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

#[derive(Debug, PartialEq)]
enum Progress {
    Woken,
    Started,
}

struct ProgressWake(std::sync::mpsc::Sender<Progress>);

impl std::task::Wake for ProgressWake {
    fn wake(self: std::sync::Arc<Self>) {
        let _ = self.0.send(Progress::Woken);
    }
}

/// Forced order: the pool is full at the first poll, so the work waits for
/// admission and starts only after the observer is polled again.
fn start_after_admission(
    observer: &mut (dyn Future<Output = std::io::Result<()>> + Unpin),
    pool: &'static std::sync::LazyLock<std::io::Result<crate::NativeWorkers>>,
    progress_tx: std::sync::mpsc::Sender<Progress>,
    progress_rx: &std::sync::mpsc::Receiver<Progress>,
) {
    let saturation = crate::tests::Saturation::new(pool).unwrap();
    let waker = Waker::from(std::sync::Arc::new(ProgressWake(progress_tx)));
    assert!(matches!(
        Pin::new(&mut *observer).poll(&mut Context::from_waker(&waker)),
        Poll::Pending
    ));
    assert!(progress_rx.try_recv().is_err());
    drop(saturation);
    while progress_rx.recv_timeout(Duration::from_secs(10)).unwrap() == Progress::Woken {
        assert!(matches!(
            Pin::new(&mut *observer).poll(&mut Context::from_waker(&waker)),
            Poll::Pending
        ));
    }
}

#[test]
fn admitted_work_keeps_observation_after_observer_drop() {
    // The test fills a process-global pool, so it must not share one.
    if crate::tests::passed_in_exclusive_process(
        "obs::tests::admitted_work_keeps_observation_after_observer_drop",
    )
    .unwrap()
    {
        return;
    }
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
        let (progress_tx, progress_rx) = std::sync::mpsc::channel();
        let started_tx = progress_tx.clone();
        let (release_tx, release_rx) = std::sync::mpsc::channel();
        let (done_tx, done_rx) = std::sync::mpsc::channel();
        let mut observer: Box<dyn Future<Output = std::io::Result<()>> + Unpin> =
            tracing::dispatcher::with_default(&origin, || {
                tracing::info_span!("test.caller", outcome = "sentinel", error.kind = "sentinel")
                    .in_scope(|| {
                        let work = move || {
                            started_tx.send(Progress::Started).unwrap();
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
        start_after_admission(
            &mut *observer,
            if native_control {
                &crate::NATIVE_WORKERS
            } else {
                &crate::BLOCKING_WORKERS
            },
            progress_tx,
            &progress_rx,
        );
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

fn root_context_case(mode: u8, broken: bool, see: &mut Observer<'_, Seen>) -> Result<(), Stop> {
    use tracing::callsite::Callsite as _;
    let seen = Capture::default();
    let _registration = tracing::Dispatch::new(tracing::subscriber::NoSubscriber::default());
    let subscriber = tracing::Dispatch::new(tracing_subscriber::registry().with(seen.clone()));
    let root = tracing::dispatcher::with_default(&subscriber, || {
        let caller = tracing::info_span!("test.root.caller");
        caller.in_scope(|| {
            let span = match mode {
                0 => tracing::Span::none(),
                1 => tracing::Span::new_disabled(
                    tracing::callsite! {
                        name: "test.root.filtered", kind: tracing::metadata::Kind::SPAN,
                        level: tracing::Level::DEBUG, fields:
                    }
                    .metadata(),
                ),
                _ => tracing::info_span!(parent: None, "test.root.enabled"),
            };
            if broken {
                super::OperationSpan::new(span)
            } else {
                super::OperationSpan::new_root(span)
            }
        })
    });
    let released = seen.closed("test.root.caller");
    let own_context = root.context().id() == root.id();
    drop(root);
    see.see(Seen::Root {
        released,
        own_context,
    })
}

proptest::proptest! {
    #[test]
    fn root_context_preserves_only_its_own_span(mode in 0_u8..3) {
        observation_oracle().check(&Case::Root(mode));
    }
}

fn causal_registry_case(
    foreign: bool,
    broken: bool,
    see: &mut Observer<'_, Seen>,
) -> Result<(), Stop> {
    let first = Capture::default();
    let second = Capture::default();
    let _registration = tracing::Dispatch::new(tracing::subscriber::NoSubscriber::default());
    let a = tracing::Dispatch::new(tracing_subscriber::registry().with(first.clone()));
    let b = tracing::Dispatch::new(tracing_subscriber::registry().with(second));
    let root = tracing::dispatcher::with_default(&a, || {
        super::OperationSpan::new_root(tracing::info_span!(parent: None, "test.causal.root"))
    });
    let dispatch = if foreign { &b } else { &a };
    let cause = tracing::dispatcher::with_default(dispatch, || {
        super::OperationSpan::new(tracing::info_span!(parent: None, "test.causal.interest"))
    });
    if broken {
        root.span.follows_from(cause.context().id());
    } else {
        root.follows_from_same_subscriber(&cause);
    }
    let history = first.0.lock().unwrap().spans.clone();
    let root = history
        .iter()
        .find(|span| span.name == "test.causal.root")
        .unwrap();
    see.see(Seen::Registry {
        actual: root.follows_from.len(),
        expected: usize::from(!foreign),
    })
}

proptest::proptest! {
    #[test]
    fn causal_links_require_identical_subscriber_registries(foreign in proptest::bool::ANY) {
        observation_oracle().check(&Case::Registry(foreign));
    }
}

std::thread_local! {
    pub(super) static DISPATCH_INSTALLS: std::cell::Cell<usize> = const { std::cell::Cell::new(0) };
}

#[derive(Debug)]
enum Case {
    Idle,
    Root(u8),
    Registry(bool),
}
#[derive(Debug)]
enum Fault {
    InstalledIdleDispatcher,
    RetainedCaller,
    ForeignRegistry,
}
#[derive(Debug)]
enum Seen {
    Idle {
        installs: usize,
        calls: usize,
        ready: bool,
    },
    Root {
        released: usize,
        own_context: bool,
    },
    Registry {
        actual: usize,
        expected: usize,
    },
}

fn observation_oracle() -> Oracle<Case, Fault, Seen> {
    Oracle::declare(|input, fault, see| match input {
        Case::Idle => unobserved_bypass(matches!(fault, Some(Fault::InstalledIdleDispatcher)), see),
        Case::Root(mode) => {
            root_context_case(*mode, matches!(fault, Some(Fault::RetainedCaller)), see)
        }
        Case::Registry(foreign) => {
            causal_registry_case(*foreign, matches!(fault, Some(Fault::ForeignRegistry)), see)
        }
    })
    .invariant("idle", |event| match event {
        Seen::Idle {
            installs,
            calls,
            ready,
        } if *installs != 0 || *calls != 2 || !ready => Err(format!("{event:?}")),
        _ => Ok(()),
    })
    .invariant("root", |event| match event {
        Seen::Root {
            released,
            own_context,
        } if *released != 1 || !own_context => Err(format!("{event:?}")),
        _ => Ok(()),
    })
    .invariant("registry", |event| match event {
        Seen::Registry { actual, expected } if actual != expected => Err(format!("{event:?}")),
        _ => Ok(()),
    })
    .control("idle", Case::Idle, Fault::InstalledIdleDispatcher)
    .control("root", Case::Root(1), Fault::RetainedCaller)
    .control("registry", Case::Registry(true), Fault::ForeignRegistry)
    .build()
}

#[test]
fn observation_controls_are_coupled_to_named_invariants() {
    let oracle = observation_oracle();
    oracle.check(&Case::Idle);
    for mode in 0..3 {
        oracle.check(&Case::Root(mode));
    }
    for foreign in [false, true] {
        oracle.check(&Case::Registry(foreign));
    }
}
