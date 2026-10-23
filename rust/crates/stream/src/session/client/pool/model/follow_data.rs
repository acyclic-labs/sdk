use super::*;

/// Explores LATEST cursor consumption racing a placement fence, then exercises
/// standalone watermark conflation through the production receive/credit path.
pub fn verify_follow_data(broken: u8) {
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    let _entered = runtime.enter();
    let channel = tonic::transport::Endpoint::from_static("http://model.invalid").connect_lazy();
    loom::model(move || {
        let progress = Arc::new(Progress::new(0));
        let consuming = progress.clone();
        let reader = loom::thread::spawn(move || consuming.consume_latest(1, 4).unwrap());
        let fencing = progress.clone();
        let writer = loom::thread::spawn(move || {
            if broken != 1 {
                fencing.fence().unwrap();
            }
        });
        reader.join().unwrap();
        writer.join().unwrap();
        assert!(
            !progress.consume_latest(1, 9).unwrap(),
            "fenced LATEST must not advance its cursor"
        );
        assert!(matches!(progress.next().unwrap(), 0 | 5));

        let (mut pool, _old) = fixture(&channel);
        let (records, mut received) = mpsc::channel(3);
        let route = pool.routes.get_mut(&3).unwrap();
        route.records = records;
        route.raw = broken != 2;
        route.kind = Kind::Follow(wire::SessionFollow {
            resource: crate::wire_codec::session::stream_resource("healthy"),
            delivery: wire::SessionDelivery::Latest as i32,
            ..Default::default()
        });
        for accepted in [3, 8, 12] {
            assert!(pool.receive_frame(
                "good",
                frame(
                    3,
                    1,
                    Body::Watermark(wire::SessionWatermark {
                        accepted,
                        durable: accepted,
                        ..Default::default()
                    })
                )
            ));
        }
        let first = received
            .try_recv()
            .expect("standalone watermark must be delivered")
            .unwrap();
        assert!(matches!(
            first.item,
            Received::Frame(wire::SessionFrame {
                body: Some(Body::Watermark(wire::SessionWatermark { accepted: 3, .. })),
                ..
            })
        ));
        drop(first.credit);
        let released = loom::future::block_on(pool.released.next()).unwrap();
        pool.released(released).unwrap();
        let latest = received
            .try_recv()
            .expect("latest durability must follow without data")
            .unwrap();
        assert!(matches!(
            latest.item,
            Received::Frame(wire::SessionFrame {
                body: Some(Body::Watermark(wire::SessionWatermark { accepted: 12, .. })),
                ..
            })
        ));
        assert!(received.try_recv().is_err());
        // Controls cannot spend aggregate data credit or admit extra data.
        assert_eq!(pool.links["good"].credit.frames, 2);
        assert_eq!(pool.routes[&3].credit.frames, 2);
        drop(latest);
    });
}
