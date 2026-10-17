//! Status admission races the real bounded outbound transport queue.
use super::fixture::fixture;
use super::*;

/// Explores a transport drain racing snapshot or live membership admission.
pub fn verify_discovery_admission(live: bool, bypass_delivery: bool) {
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    let _entered = runtime.enter();
    let channel = tonic::transport::Endpoint::from_static("http://model.invalid").connect_lazy();
    loom::model(move || {
        let (mut pool, _) = fixture(channel.clone());
        for (id, route) in &mut pool.routes {
            route.node = "good".into();
            route.kind = Kind::Follow(wire::SessionFollow {
                path: format!("swarm/a{id}/status"),
                delivery: wire::SessionDelivery::Latest as i32,
                ..Default::default()
            });
        }
        // The source subscription is a hierarchy snapshot or the checked ALL
        // parent-volume follow. Both deliver before a status Open is admitted.
        pool.routes.get_mut(&1).unwrap().kind = if live {
            Kind::Follow(wire::SessionFollow {
                path: "swarm/files".into(),
                delivery: wire::SessionDelivery::All as i32,
                preceding_commit_id: vec![0; 32].into(),
            })
        } else {
            Kind::Children(wire::ChildrenRequest {
                parent: Some("swarm".into()),
                limit: 1,
            })
        };
        // Smallest queue/interleaving that distinguishes snapshot or live flooding from
        // discovery-paced admission. These are the production Outbound locks,
        // pool subscription dispatcher and error paths, not a counter
        // standing in for a channel.
        let outbound = Arc::new(Outbound::new(1, crate::MAX_COMMAND_BYTES));
        pool.links.get_mut("good").unwrap().sender = Arc::clone(&outbound);
        // The first registration has filled the queue. Only the second Open
        // races its drain; modeling the setup's unrelated admission locks and
        // a second future park adds no queue interleaving and is unnecessary.
        pool.subscribe(1).unwrap();
        let children = Arc::new(signal::Signal::new());
        let delivering = Arc::clone(&children);
        let reading = Arc::clone(&outbound);
        let transport = loom::thread::spawn(move || {
            let frame = loom::future::block_on(reading.next()).unwrap();
            assert!(matches!(frame.body, Some(Body::Interest(_))));
            delivering.send(()).unwrap();
        });
        if !bypass_delivery {
            loom::future::block_on(children.next()).unwrap();
        }
        pool.subscribe(3).unwrap();
        transport.join().unwrap();
        let admitted = pool.routes.contains_key(&3);
        assert!(
            admitted,
            "membership bypassed session delivery backpressure"
        );
    });
}
