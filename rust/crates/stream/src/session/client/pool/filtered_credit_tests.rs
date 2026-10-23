use super::*;

async fn qualify_prefix_credit(omit_held_data: bool) -> Result<(), Box<dyn std::error::Error>> {
    let endpoint =
        tonic::transport::Endpoint::from_static("http://filtered-credit.invalid").connect_lazy();
    let (mut pool, _) = fixture::fixture(&endpoint);
    let (records, mut receiver) = mpsc::channel(3);
    let route = pool.routes.get_mut(&3).ok_or("missing route")?;
    route.records = records;
    route.raw = true;
    let watermark = |next: Option<u64>| {
        frame(
            3,
            1,
            Body::Watermark(wire::SessionWatermark {
                accepted: 5,
                durable: 5,
                after: next.map(super::super::super::cursor),
                preceding_commit_id: next.map_or_else(Bytes::new, |_| vec![1; 32].into()),
            }),
        )
    };
    let data = |sequence: u64| {
        frame(
            3,
            1,
            Body::Data(wire::SessionData {
                accepted: 5,
                durable: 5,
                after: Some(super::super::super::cursor(sequence + 1)),
                item: Some(Item::Record(wire_codec::record_wire(crate::Record {
                    headers: Default::default(),
                    sequence,
                    value: Bytes::from_static(b"match").into(),
                    commit_id: crate::CommitId::from_bytes([7; 32]),
                    committed_at_micros: 0,
                }))),
            }),
        )
    };
    for input in [
        watermark(Some(1)),
        watermark(Some(2)),
        watermark(None),
        data(2),
        watermark(Some(4)),
        data(4),
    ] {
        pool.receive("good", input)?;
    }
    let idle = receiver.try_recv()??;
    assert!(idle.consume(&pool.routes.get(&3).ok_or("route")?.progress)?);
    drop(idle.credit);
    let release = pool.released.try_next()?.ok_or("control release")?;
    pool.released(release)?;
    assert_eq!(pool.routes.get(&3).ok_or("route")?.credit.frames, 0);
    let mut first = receiver.try_recv()??;
    let progress = Arc::clone(&pool.routes.get(&3).ok_or("route")?.progress);
    let Some(Received::Frame(before)) = first.consume_before(&progress)? else {
        return Err("durability conflation erased the skipped prefix".into());
    };
    assert!(matches!(
        before.body,
        Some(Body::Watermark(wire::SessionWatermark {
            after: Some(wire::SessionCursor { next: 2, .. }),
            ..
        }))
    ));
    assert_eq!(progress.next()?, 2);
    // The consumer has yielded the prefix, but still owns its data delivery.
    // Negative control reproduces the old grant calculation: a dequeued data
    // envelope disappeared from capacity while its prefix was being yielded.
    if omit_held_data {
        pool.routes.get_mut(&3).ok_or("route")?.buffered_data = 0;
    }
    pool.grant(3)?;
    if omit_held_data {
        pool.routes.get_mut(&3).ok_or("route")?.buffered_data = 2;
    }
    let route = pool.routes.get(&3).ok_or("route")?;
    assert_eq!(route.buffered_data, 2);
    assert_eq!(
        route.credit.frames, 0,
        "a prefix must not enlarge the data window"
    );
    assert!(first.consume(&progress)?);
    assert_eq!(progress.next()?, 3);
    drop(first.credit);
    let release = pool.released.try_next()?.ok_or("data release")?;
    pool.released(release)?;
    let route = pool.routes.get(&3).ok_or("route")?;
    assert_eq!(route.buffered_data + route.credit.frames, 2);
    let mut second = receiver.try_recv()??;
    assert!(second.consume_before(&progress)?.is_some());
    assert_eq!(progress.next()?, 4);
    assert!(second.consume(&progress)?);
    assert_eq!(progress.next()?, 5);
    drop(second.credit);
    let release = pool.released.try_next()?.ok_or("second data release")?;
    pool.released(release)?;
    let route = pool.routes.get(&3).ok_or("route")?;
    assert_eq!((route.buffered_data, route.credit.frames), (0, 2));
    Ok(())
}

#[tokio::test]
async fn skipped_cuts_share_data_credit_and_survive_durability_conflation()
-> Result<(), Box<dyn std::error::Error>> {
    qualify_prefix_credit(false).await
}

#[tokio::test]
#[should_panic(expected = "a prefix must not enlarge the data window")]
async fn omitted_held_data_negative_control() {
    qualify_prefix_credit(true).await.unwrap();
}
