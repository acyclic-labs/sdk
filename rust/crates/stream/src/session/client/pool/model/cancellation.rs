use super::super::super::*;
use super::super::fixture::fixture;

/// The real Cursor drop, removal signal and Open dispatcher, with both wake
/// registration orders. No Tokio synchronization is used for model ordering.
pub fn verify_cancel_before_open(broken: bool) {
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    let _entered = runtime.enter();
    let channel = tonic::transport::Endpoint::from_static("http://model.invalid").connect_lazy();
    loom::model(move || {
        let (mut pool, receiver) = fixture(channel.clone());
        let client = super::super::fixture::client(&pool);
        let old = client
            .cursor(3, Arc::clone(&pool.routes[&3].progress), receiver)
            .unwrap();
        let events = Arc::new(signal::Signal::new());
        let opening = Arc::clone(&events);
        let owner = loom::thread::spawn(move || {
            drop(old);
            let progress = Arc::new(Progress::new(0));
            let (records, receiver) = mpsc::channel(2);
            let cursor = client.cursor(5, Arc::clone(&progress), receiver).unwrap();
            let (ready, _opened) = oneshot::channel();
            opening
                .send(Command::Open {
                    id: 5,
                    attempt_epoch: 0,
                    acknowledge: false,
                    kind: Kind::Follow(wire::SessionFollow {
                        path: "replacement".into(),
                        delivery: wire::SessionDelivery::All as i32,
                        ..Default::default()
                    }),
                    raw: false,
                    progress,
                    records,
                    ready,
                    observation: cursor.observation.operation.clone(),
                })
                .unwrap();
            cursor
        });
        let command = loom::future::block_on(events.next()).unwrap();
        if broken {
            pool.apply_command(command).unwrap();
        } else {
            pool.command(command).unwrap();
        }
        let successor = owner.join().unwrap();
        assert!(
            !pool.routes.contains_key(&3),
            "cancelled follow survived replacement admission"
        );
        assert!(pool.routes.contains_key(&5));
        let outbound = &pool.links["good"].sender;
        let removed = loom::future::block_on(outbound.next()).unwrap();
        let opened = loom::future::block_on(outbound.next()).unwrap();
        assert!(
            matches!(removed.body, Some(Body::Interest(i)) if i.remove.iter().any(|c| c.channel_id == 3))
        );
        assert!(
            matches!(opened.body, Some(Body::Interest(i)) if i.upsert.iter().any(|s| s.channel.as_ref().is_some_and(|c| c.channel_id == 5)))
        );
        drop(successor);
    });
}
