use super::*;

pub(in crate::session::client) fn fixture(
    channel: TransportChannel,
) -> (Pool, mpsc::Receiver<Result<Delivery, StreamError>>) {
    let (_, commands) = mpsc::channel(2);
    let released = Arc::new(signal::Signal::new());
    let mut pool = Pool {
        identity: Bytes::from_static(b"model"),
        authorization: Some("Bearer fixture".parse().unwrap()),
        credential: None,
        connect: std::sync::Arc::new(|_| Box::pin(future::ready(Err(StreamError::Unavailable)))),
        home: "good".into(),
        routes: BTreeMap::new(),
        links: BTreeMap::new(),
        reads: FuturesUnordered::new(),
        dials: BTreeMap::new(),
        pending: VecDeque::new(),
        home_channel: channel.clone(),
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
