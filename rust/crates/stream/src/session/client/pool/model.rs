use super::*;

fn fixture(channel: TransportChannel) -> (Pool, mpsc::Receiver<Result<Delivery, StreamError>>) {
    let (_, commands) = mpsc::channel(2);
    let released = Arc::new(signal::Signal::new());
    let mut pool = Pool {
        identity: Bytes::from_static(b"model"),
        authorization: "Bearer fixture".parse().unwrap(),
        connect: std::sync::Arc::new(|_| Box::pin(future::ready(Err(StreamError::Unavailable)))),
        home: "good".into(),
        routes: BTreeMap::new(),
        links: BTreeMap::new(),
        reads: FuturesUnordered::new(),
        dials: FuturesUnordered::new(),
        connecting: BTreeSet::new(),
        pending: VecDeque::new(),
        home_channel: channel.clone(),
        next_link: 3,
        commands,
        removed: Arc::new(signal::Signal::new()),
        released: Arc::clone(&released),
        release_sender: released,
    };
    for (node, epoch) in [("bad", 1), ("good", 2)] {
        pool.links.insert(
            node.into(),
            Link {
                epoch,
                credit: wire::SessionWindow {
                    bytes: crate::MAX_COMMAND_BYTES as u64,
                    frames: 2,
                },
                channel: channel.clone(),
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
                node: node.into(),
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
            },
        );
    }
    (pool, healthy.unwrap())
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
        assert!(matches!(delivery.item, Received::Record(_)));
        assert!(
            pool.routes[&3]
                .progress
                .consume(delivery.generation, delivery.sequence)
                .unwrap()
        );
        drop(delivery.credit);
        let released = loom::future::block_on(pool.released.next()).unwrap();
        pool.released(released).unwrap();
        // An unrelated later open still reaches the real subscription queue.
        let (records, _receiver) = mpsc::channel(2);
        let (ready, mut opened) = oneshot::channel();
        pool.command(Command::Open {
            id: 5,
            acknowledge: false,
            kind: Kind::Follow(wire::SessionFollow {
                path: "later".into(),
                ..Default::default()
            }),
            progress: Arc::new(Progress::new(0)),
            records,
            ready,
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
