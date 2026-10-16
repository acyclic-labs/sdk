use super::*;

mod follow_data;
pub use follow_data::verify_follow_data;

fn fixture(channel: TransportChannel) -> (Pool, mpsc::Receiver<Result<Delivery, StreamError>>) {
    let (_, commands) = mpsc::channel(2);
    let released = Arc::new(signal::Signal::new());
    let mut pool = Pool {
        connections: super::super::super::Connections::new(),
        executor: std::sync::Arc::new(super::super::super::runtime::PlatformExecutor),
        identity: Bytes::from_static(b"model"),
        connect: std::sync::Arc::new(|_| Box::pin(future::ready(Err(StreamError::Unavailable)))),
        home: "good".into(),
        routes: BTreeMap::new(),
        links: BTreeMap::new(),
        reads: FuturesUnordered::new(),
        dials: FuturesUnordered::new(),
        connecting: BTreeSet::new(),
        pending: VecDeque::new(),
        home_channel: native::endpoint(
            channel.clone(),
            Some("Bearer fixture".parse().unwrap()),
            None,
        ),
        next_link: 3,
        active: Arc::new(AtomicU64::new(0)),
        attempt_epoch: Arc::new(AtomicU64::new(0)),
        abandoned: Arc::new(gate::Gate::new()),
        dial_epoch: 0,
        commands,
        removed: Arc::new(signal::Signal::new()),
        released: Arc::clone(&released),
        release_sender: released,
    };
    for (node, epoch) in [("bad", 1), ("good", 2)] {
        pool.links.insert(
            node.into(),
            Link {
                observation: None,
                epoch,
                credit: wire::SessionWindow {
                    bytes: crate::MAX_COMMAND_BYTES as u64,
                    frames: 2,
                },
                channel: native::endpoint(
                    channel.clone(),
                    Some("Bearer fixture".parse().unwrap()),
                    None,
                ),
                sender: Arc::new(Outbound::new(4, crate::MAX_COMMAND_BYTES)),
                revision: 0,
                applied_revision: 0,
                released: wire::SessionWindow::default(),
            },
        );
    }
    let mut healthy = None;
    for (id, node, kind) in [
        (
            1,
            "bad",
            Kind::Children(wire::ChildrenRequest {
                parent: Some("agents".into()),
                limit: 1,
            }),
        ),
        (
            3,
            "good",
            Kind::Follow(wire::SessionFollow {
                delivery: wire::SessionDelivery::All as i32,
                path: "healthy".into(),
                ..Default::default()
            }),
        ),
    ] {
        let (records, receiver) = mpsc::channel(2);
        if id == 3 {
            healthy = Some(receiver);
        }
        pool.routes.insert(
            id,
            Route {
                raw: false,
                watermark: None,
                node: node.into(),
                attempt_epoch: 0,
                kind,
                generation: 1,
                placement_epoch: 0,
                received: 0,
                received_opaque: Bytes::new(),
                acknowledge: false,
                acknowledged: 0,
                opened: true,
                credit: wire::SessionWindow {
                    bytes: crate::MAX_COMMAND_BYTES as u64,
                    frames: 2,
                },
                progress: Arc::new(Progress::new(0)),
                records,
                ready: None,
                observation: crate::obs::OperationSpan::new(tracing::Span::none()),
            },
        );
    }
    (pool, healthy.unwrap())
}

/// Models the real abandonment fence, wake registration, new Open and late error.
pub fn verify_abandoned_reconnect(broken: bool) {
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    let _entered = runtime.enter();
    let channel = tonic::transport::Endpoint::from_static("http://model.invalid").connect_lazy();
    for old_first in [true, false] {
        let channel = channel.clone();
        loom::model(move || {
            let (mut pool, _) = fixture(channel.clone());
            pool.routes.clear();
            let mut links = std::mem::take(&mut pool.links);
            let (commands, _) = mpsc::channel(2);
            let client = Client {
                connections: super::super::super::Connections::new(),
                executor: std::sync::Arc::new(super::super::super::runtime::PlatformExecutor),
                commands,
                removed: Arc::clone(&pool.removed),
                next_id: Arc::new(AtomicU64::new(1)),
                active: Arc::clone(&pool.active),
                attempt_epoch: Arc::clone(&pool.attempt_epoch),
                abandoned: Arc::clone(&pool.abandoned),
            };
            let prepare = |client: &Client, id| {
                let progress = Arc::new(Progress::new(0));
                let (records, receiver) = mpsc::channel(2);
                let cursor = client.cursor(id, Arc::clone(&progress), receiver).unwrap();
                let (ready, opened) = oneshot::channel();
                let observation = cursor.observation.operation.clone();
                (
                    cursor,
                    Command::Open {
                        raw: false,
                        id,
                        attempt_epoch: client.attempt_epoch.load(Ordering::Acquire),
                        acknowledge: false,
                        kind: Kind::Follow(wire::SessionFollow {
                            delivery: wire::SessionDelivery::All as i32,
                            path: "healthy".into(),
                            ..Default::default()
                        }),
                        progress,
                        records,
                        ready,
                        observation,
                    },
                    opened,
                )
            };
            let (first, command, _first_opened) = prepare(&client, 1);
            pool.command(command).unwrap();
            assert_eq!(pool.dials.len(), 1);
            let events = Arc::new(signal::Signal::new());
            let opening = Arc::clone(&events);
            let cancel = loom::thread::spawn(move || {
                client.abandon_pending().unwrap();
                drop(first);
                let (second, command, opened) = prepare(&client, 3);
                opening.send(command).unwrap();
                (second, opened)
            });
            // Abandonment must wake dispatch regardless of wait registration order.
            loom::future::block_on(pool.abandoned.changed(false)).unwrap();
            for index in 0..2 {
                if (index == 0) == old_first {
                    if broken {
                        pool.fail_node("good", &StreamError::AccessDenied);
                    } else {
                        pool.dial_ready("good".into(), 0, Err(StreamError::AccessDenied))
                            .unwrap();
                    }
                } else {
                    pool.command(loom::future::block_on(events.next()).unwrap())
                        .unwrap();
                }
            }
            let (second, mut opened) = cancel.join().unwrap();
            assert!(
                pool.routes.contains_key(&3),
                "an abandoned dial must not fail a new interest"
            );
            assert_eq!(pool.dial_epoch, 1);
            assert_eq!(
                pool.dials.len(),
                1,
                "new interest must own a fresh pending dial"
            );
            assert!(pool.connecting.contains("good"));
            pool.links
                .insert("good".into(), links.remove("good").unwrap());
            assert!(pool.receive_frame(
                "good",
                frame(
                    3,
                    1,
                    Body::Opened(wire::SessionOpened {
                        resume: Some(wire::SessionCursor::default()),
                        ..Default::default()
                    })
                )
            ));
            assert_eq!(opened.try_recv().unwrap().unwrap(), 3);
            drop(second);
        });
    }
}

/// Uses the real cursor reservation/drop, dispatcher, idle cleanup and link locks.
pub fn verify_pool_lifetime(fault: u8) {
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    let _entered = runtime.enter();
    let channel = tonic::transport::Endpoint::from_static("http://model.invalid").connect_lazy();
    loom::model(move || {
        let (mut pool, _) = fixture(channel.clone());
        pool.routes.clear();
        let links = std::mem::take(&mut pool.links);
        let (commands, _) = mpsc::channel(2);
        let client = Client {
            connections: super::super::super::Connections::new(),
            executor: std::sync::Arc::new(super::super::super::runtime::PlatformExecutor),
            commands,
            removed: Arc::clone(&pool.removed),
            next_id: Arc::new(AtomicU64::new(1)),
            active: Arc::clone(&pool.active),
            attempt_epoch: Arc::clone(&pool.attempt_epoch),
            abandoned: Arc::clone(&pool.abandoned),
        };
        let prepare = |id| {
            let progress = Arc::new(Progress::new(0));
            let (records, receiver) = mpsc::channel(2);
            let cursor = client.cursor(id, Arc::clone(&progress), receiver).unwrap();
            let (ready, opened) = oneshot::channel();
            let command = Command::Open {
                raw: false,
                id,
                attempt_epoch: 0,
                acknowledge: false,
                kind: Kind::Follow(wire::SessionFollow {
                    delivery: wire::SessionDelivery::All as i32,
                    path: "healthy".into(),
                    ..Default::default()
                }),
                progress,
                records,
                ready,
                observation: cursor.observation.operation.clone(),
            };
            (cursor, command, opened)
        };
        let (first, command, _first_opened) = prepare(1);
        pool.command(command).unwrap();
        assert_eq!(pool.dials.len(), 1);
        // B reserves before its Open can reach dispatch; Welcome is still pending.
        let (second, command, _second_opened) = prepare(3);
        let events = Arc::new(signal::Signal::new());
        let cancelled = Arc::clone(&events);
        let abort = loom::thread::spawn(move || {
            drop(first);
            cancelled.send(None).unwrap();
        });
        let opening = Arc::clone(&events);
        let open = loom::thread::spawn(move || opening.send(Some(command)).unwrap());
        abort.join().unwrap();
        open.join().unwrap();
        for _ in 0..2 {
            match loom::future::block_on(events.next()).unwrap() {
                Some(command) => pool.command(command).unwrap(),
                None => {
                    let id = loom::future::block_on(pool.removed.next()).unwrap();
                    pool.remove(id).unwrap();
                    if fault == 1 {
                        pool.dials.clear();
                    }
                }
            }
            if fault == 2 && pool.routes.is_empty() {
                pool.close_links();
            } else {
                pool.close_idle();
            }
            assert_eq!(
                pool.dials.len(),
                1,
                "a cancelled waiter must preserve shared initialization"
            );
        }
        assert_eq!(pool.active.load(Ordering::Acquire), 1);
        pool.links = links;
        let senders: Vec<_> = pool
            .links
            .values()
            .map(|link| Arc::clone(&link.sender))
            .collect();
        drop(second);
        let id = loom::future::block_on(pool.removed.next()).unwrap();
        pool.remove(id).unwrap();
        if fault != 3 {
            pool.close_idle();
        }
        assert!(
            pool.links.is_empty(),
            "the last interest must close cached links"
        );
        assert!(pool.dials.is_empty());
        assert_eq!(pool.active.load(Ordering::Acquire), 0);
        for sender in senders {
            assert!(matches!(
                loom::future::block_on(sender.next()),
                Err(StreamError::Unavailable)
            ));
        }
    });
}

/// Checks real cursor reservation/drop against an arbitrary live-cursor fold.
pub fn verify_interest_accounting(operations: Vec<(u8, bool)>) {
    loom::model(move || {
        let (commands, _) = mpsc::channel(2);
        let client = Client {
            connections: super::super::super::Connections::new(),
            executor: std::sync::Arc::new(super::super::super::runtime::PlatformExecutor),
            commands,
            removed: Arc::new(signal::Signal::new()),
            next_id: Arc::new(AtomicU64::new(1)),
            active: Arc::new(AtomicU64::new(0)),
            attempt_epoch: Arc::new(AtomicU64::new(0)),
            abandoned: Arc::new(gate::Gate::new()),
        };
        let mut live = BTreeMap::new();
        for &(key, open) in &operations {
            if open {
                let (_, receiver) = mpsc::channel(2);
                let cursor = client
                    .cursor(u64::from(key) * 2 + 1, Arc::new(Progress::new(0)), receiver)
                    .unwrap();
                live.insert(key, cursor);
            } else {
                live.remove(&key);
            }
            assert_eq!(client.active.load(Ordering::Acquire), live.len() as u64);
        }
        drop(live);
        assert_eq!(client.active.load(Ordering::Acquire), 0);
    });
}

/// Models actual pool dispatch with a malformed peer and an independent delivery.
pub fn verify_link_isolation(broken: bool) {
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    let _entered = runtime.enter();
    // This unused I/O handle is outside the model; the actual SDK queue, cursor,
    // signals and link-close locks below use the crate-local Loom shim.
    let channel = tonic::transport::Endpoint::from_static("http://model.invalid").connect_lazy();
    loom::model(move || {
        let (mut pool, mut healthy) = fixture(channel.clone());
        let bad = Arc::clone(&pool.links["bad"].sender);
        let events = Arc::new(signal::Signal::new());
        let bad_events = Arc::clone(&events);
        let failing = loom::thread::spawn(move || {
            bad_events
                .send((
                    "bad",
                    frame(
                        1,
                        1,
                        Body::Data(wire::SessionData {
                            after: Some(wire::SessionCursor {
                                next: 1,
                                opaque: Bytes::from_static(b"other/child"),
                            }),
                            accepted: 1,
                            durable: 0,
                            item: Some(Item::Child(wire::Child {
                                path: "other/child".into(),
                            })),
                        }),
                    ),
                ))
                .unwrap();
        });
        let healthy_events = Arc::clone(&events);
        let delivering = loom::thread::spawn(move || {
            healthy_events
                .send((
                    "good",
                    frame(
                        3,
                        1,
                        Body::Data(wire::SessionData {
                            after: Some(wire::SessionCursor {
                                next: 1,
                                opaque: Bytes::new(),
                            }),
                            accepted: 1,
                            durable: 0,
                            item: Some(Item::Record(wire::Record {
                                sequence: 0,
                                value: Bytes::from_static(b"healthy"),
                                commit_id: vec![7; 32].into(),
                                committed_at_micros: 0,
                            })),
                        }),
                    ),
                ))
                .unwrap();
        });
        // Dispatch has no awaits inside frame handling. Both arrival orders are
        // possible; a frame cannot interrupt another frame's state transition.
        failing.join().unwrap();
        delivering.join().unwrap();
        for _ in 0..2 {
            let (node, value) = loom::future::block_on(events.next()).unwrap();
            if broken {
                // Former run(): receive's error escapes dispatch and fails all routes.
                if let Err(error) = pool.receive(node, value) {
                    pool.fail(&error);
                }
            } else {
                pool.receive_frame(node, value);
            }
        }
        assert!(
            pool.routes.contains_key(&3),
            "a peer fault must preserve the healthy route"
        );
        assert!(!pool.links.contains_key("bad"));
        assert!(pool.links.contains_key("good"));
        assert!(matches!(
            loom::future::block_on(bad.next()),
            Err(StreamError::Unavailable)
        ));
        let delivery = healthy.try_recv().unwrap().unwrap();
        let Received::Record(record) = &delivery.item else {
            panic!("healthy route must deliver its record");
        };
        assert!(
            pool.routes[&3]
                .progress
                .consume_record(
                    delivery.generation,
                    delivery.sequence,
                    *record.commit_id.as_bytes()
                )
                .unwrap()
        );
        drop(delivery.credit);
        let released = loom::future::block_on(pool.released.next()).unwrap();
        pool.released(released).unwrap();
        // An unrelated later open still reaches the real subscription queue.
        let (records, _receiver) = mpsc::channel(2);
        let (ready, mut opened) = oneshot::channel();
        pool.command(Command::Open {
            raw: false,
            id: 5,
            attempt_epoch: 0,
            acknowledge: false,
            kind: Kind::Follow(wire::SessionFollow {
                delivery: wire::SessionDelivery::All as i32,
                path: "later".into(),
                ..Default::default()
            }),
            progress: Arc::new(Progress::new(0)),
            records,
            ready,
            observation: crate::obs::OperationSpan::new(tracing::Span::none()),
        })
        .unwrap();
        assert!(pool.receive_frame(
            "good",
            frame(
                5,
                1,
                Body::Opened(wire::SessionOpened {
                    resume: Some(wire::SessionCursor::default()),
                    ..Default::default()
                })
            )
        ));
        assert_eq!(opened.try_recv().unwrap().unwrap(), 5);
        assert!(pool.routes.contains_key(&3));
    });
}

/// Real deadline race and cursor destruction, with Loom-instrumented guard state.
pub fn verify_registration_deadline(broken: bool) {
    use std::future::Future as _;
    for (complete, cancel) in [(false, false), (true, false), (false, true), (true, true)] {
        loom::model(move || {
            let (commands, _commands) = mpsc::channel(1);
            let client = Client {
                connections: super::super::super::Connections::new(),
                executor: std::sync::Arc::new(super::super::super::runtime::PlatformExecutor),
                commands,
                removed: Arc::new(signal::Signal::new()),
                next_id: Arc::new(AtomicU64::new(1)),
                active: Arc::new(AtomicU64::new(0)),
                attempt_epoch: Arc::new(AtomicU64::new(0)),
                abandoned: Arc::new(gate::Gate::new()),
            };
            let (_, receiver) = mpsc::channel(2);
            let progress = Arc::new(Progress::new(0));
            let cursor = client.cursor(1, Arc::clone(&progress), receiver).unwrap();
            let expiry = Arc::new(gate::Gate::new());
            let opened = Arc::new(gate::Gate::new());
            let expiring = Arc::clone(&expiry);
            let expiry_thread = loom::thread::spawn(move || expiring.set(true).unwrap());
            let opening = Arc::clone(&opened);
            let open_thread = loom::thread::spawn(move || {
                if complete {
                    opening.set(true).unwrap();
                }
            });
            let mut registration = Some(Box::pin(async move {
                let result = deadline::until(
                    async {
                        opened.changed(false).await.unwrap();
                    },
                    async {
                        if broken {
                            future::pending::<()>().await;
                        }
                        expiry.changed(false).await.unwrap();
                    },
                )
                .await;
                drop(cursor); // Existing guard owns cancellation and removal.
                result
            }));
            let wakes = Arc::new(AtomicU64::new(0));
            let waker =
                std::task::Waker::from(std::sync::Arc::new(RegistrationWake(Arc::clone(&wakes))));
            let mut context = std::task::Context::from_waker(&waker);
            let early = registration.as_mut().unwrap().as_mut().poll(&mut context);
            if cancel {
                drop(registration.take());
            }
            expiry_thread.join().unwrap();
            open_thread.join().unwrap();
            if !cancel && early.is_pending() {
                assert!(
                    registration
                        .as_mut()
                        .unwrap()
                        .as_mut()
                        .poll(&mut context)
                        .is_ready(),
                    "expired registration must complete"
                );
                assert!(
                    wakes.load(Ordering::Acquire) > 0,
                    "a parked registration must be woken"
                );
            }
            drop(registration);
            assert!(progress.cancelled());
            assert_eq!(client.active.load(Ordering::Acquire), 0);
            assert_eq!(loom::future::block_on(client.removed.next()).unwrap(), 1);
        });
    }
}

struct RegistrationWake(Arc<AtomicU64>);
impl std::task::Wake for RegistrationWake {
    fn wake(self: std::sync::Arc<Self>) {
        self.0.fetch_add(1, Ordering::Release);
    }
}
