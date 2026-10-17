//! Initial snapshot admission races the real bounded outbound transport queue.
use super::fixture::fixture;
use super::*;

pub fn verify_discovery_admission(bypass_discovery: bool) {
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    let _entered = runtime.enter();
    let channel = tonic::transport::Endpoint::from_static("http://model.invalid").connect_lazy();
    loom::model(move || {
        let (mut pool, _) = fixture(channel.clone());
        pool.routes.clear();
        // Smallest queue/interleaving that distinguishes snapshot flooding from
        // discovery-paced admission. These are the production Outbound locks,
        // pool command dispatcher and subscription/error paths, not a counter
        // standing in for a channel.
        let outbound = Arc::new(Outbound::new(1, crate::MAX_COMMAND_BYTES));
        pool.links.get_mut("good").unwrap().sender = Arc::clone(&outbound);
        let children = Arc::new(signal::Signal::new());
        let delivering = Arc::clone(&children);
        let reading = Arc::clone(&outbound);
        let transport = loom::thread::spawn(move || {
            for child in 0..2 {
                delivering.send(child).unwrap();
                let Ok(frame) = loom::future::block_on(reading.next()) else {
                    return false;
                };
                assert!(matches!(frame.body, Some(Body::Interest(_))));
            }
            true
        });
        let mut admitted = true;
        let mut receivers = Vec::new();
        for child in 0..2 {
            if !bypass_discovery {
                assert_eq!(loom::future::block_on(children.next()).unwrap(), child);
            }
            let id = 3 + child * 2;
            let (records, receiver) = mpsc::channel(2);
            let (ready, _) = oneshot::channel();
            receivers.push(receiver);
            pool.command(Command::Open {
                id,
                attempt_epoch: 0,
                acknowledge: false,
                raw: true,
                kind: Kind::Follow(wire::SessionFollow {
                    path: format!("swarm/a{child}/status"),
                    delivery: wire::SessionDelivery::Latest as i32,
                    ..Default::default()
                }),
                progress: Arc::new(Progress::new(0)),
                records,
                ready,
                observation: crate::obs::OperationSpan::new(tracing::Span::none()),
            })
            .unwrap();
            if !pool.routes.contains_key(&id) {
                admitted = false;
                outbound.close().unwrap();
                break;
            }
        }
        let delivered = transport.join().unwrap();
        assert!(
            admitted && delivered,
            "membership snapshot bypassed discovery backpressure"
        );
    });
}
