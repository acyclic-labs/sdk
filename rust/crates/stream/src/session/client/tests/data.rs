use super::*;

fn subscription(path: &str, delivery: wire::SessionDelivery) -> wire::SessionSubscription {
    wire::SessionSubscription {
        credential: 0,
        filter: None,
        resume: Some(wire::SessionCursor::default()),
        kind: Some(Kind::Follow(wire::SessionFollow {
            resource: crate::wire_codec::session::stream_resource(path),
            delivery: delivery as i32,
            ..Default::default()
        })),
        channel: None,
    }
}

#[tokio::test]
async fn opening_cuts_do_not_claim_a_consumed_prefix() -> TestResult {
    for from in [0, 1] {
        let provider = Arc::new(MemoryStream::default());
        append(&provider, "events", 1).await?;
        let fixture = Fixture::open(Arc::clone(&provider), "node").await?;
        let client = fixture.client()?.session_client()?;
        let mut request = subscription("events", wire::SessionDelivery::All);
        request.resume = Some(cursor(from));
        if let Some(Kind::Follow(follow)) = &mut request.kind {
            follow.opening_cut = true;
        }
        let mut source = client.subscribe(request).await?;
        let mut opening = source
            .next()
            .await
            .ok_or("opening source ended")??
            .into_wire();
        assert!(matches!(
            opening.body.as_ref(),
            Some(Body::Watermark(wire::SessionWatermark {
                opening_tail: Some(1),
                after: None,
                preceding_commit_id,
                ..
            })) if preceding_commit_id.is_empty()
        ));
        if from == 0 {
            assert_eq!(
                source
                    .next()
                    .await
                    .ok_or("backfill source ended")??
                    .into_record(&StreamPath::new("events")?)?
                    .sequence,
                0
            );
        }
        append(&provider, "events", 1).await?;
        assert_eq!(
            source
                .next()
                .await
                .ok_or("live source ended")??
                .into_record(&StreamPath::new("events")?)?
                .sequence,
            1
        );
        // Mutate the original server delivery to reproduce the invalid claim.
        // The shared pool must terminate it rather than wait for another record.
        let Some(Body::Watermark(mark)) = &mut opening.body else {
            return Err("opening watermark missing".into());
        };
        mark.after = Some(cursor(0));
        fixture.controls.send(Ok(opening))?;
        assert!(matches!(
            source.next().await,
            Some(Err(StreamError::InvalidResponse))
        ));
        assert!(source.next().await.is_none());
    }
    Ok(())
}

#[tokio::test]
async fn raw_follow_keeps_checked_anchors_and_standalone_watermarks() -> TestResult {
    let provider = Arc::new(MemoryStream::default());
    append(&provider, "events", 1).await?;
    let fixture = Fixture::open(provider, "node").await?;
    let client = Client::with_connector(
        fixture._channel.clone(),
        Some("Bearer fixture".parse()?),
        None,
        Arc::new(|_| Box::pin(future::ready(Err(StreamError::Unavailable)))),
    );
    client.register_credential(0, crate::session::testing::credential())?;
    let mut all = client
        .subscribe(subscription("events", wire::SessionDelivery::All))
        .await?;
    let record = all.next().await.ok_or("missing data")??.into_wire();
    let Some(Body::Data(data)) = record.body else {
        return Err("expected data".into());
    };
    let Some(Item::Record(value)) = data.item else {
        return Err("expected record".into());
    };
    let checked = wire::SessionSubscription {
        credential: 0,
        filter: None,
        resume: data.after,
        kind: Some(Kind::Follow(wire::SessionFollow {
            opening_cut: false,
            key: None,
            read_limit: 0,
            bootstrap_head: false,
            when_absent: Default::default(),
            resource: crate::wire_codec::session::stream_resource("events"),
            delivery: wire::SessionDelivery::All as i32,
            preceding_commit_id: value.commit_id,
            until: 0,
            at_least: Default::default(),
        })),
        channel: None,
    };
    let _checked = client.subscribe(checked).await?;
    fixture.controls.send(Ok(frame(
        record.channel_id,
        record.generation,
        Body::Watermark(wire::SessionWatermark {
            accepted: 1,
            durable: 1,
            ..Default::default()
        }),
    )))?;
    let mark = all.next().await.ok_or("missing watermark")??;
    assert!(matches!(
        mark.wire().body,
        Some(Body::Watermark(wire::SessionWatermark {
            accepted: 1,
            durable: 1,
            ..
        }))
    ));
    assert_eq!(fixture.counts.sessions.load(Ordering::Relaxed), 1);
    Ok(())
}

#[tokio::test]
async fn latest_follows_keep_attention_and_resume_the_consumed_cut() -> TestResult {
    let provider = Arc::new(MemoryStream::default());
    let values = (0..8)
        .map(|sequence| {
            crate::AppendRecord::latest(OwnedBytes::from(vec![sequence]), "agent", sequence == 3)
        })
        .collect::<Result<Vec<_>, _>>()?;
    provider
        .append(AppendRequest {
            path: StreamPath::new("status")?,
            records: values,
            if_tail: None,
            idempotency_key: crate::IdempotencyKey::unique(),
        })
        .await?;
    let fixture = Fixture::open(provider, "node").await?;
    let client = Client::with_connector(
        fixture._channel.clone(),
        Some("Bearer fixture".parse()?),
        None,
        Arc::new(|_| Box::pin(future::ready(Err(StreamError::Unavailable)))),
    );
    client.register_credential(0, crate::session::testing::credential())?;
    let mut latest = client
        .subscribe(subscription("status", wire::SessionDelivery::Latest))
        .await?;
    let mut cursor = 0;
    let mut attention = false;
    while cursor < 8 {
        let frame = latest.next().await.ok_or("missing status")??;
        let Some(Body::Data(data)) = frame.into_wire().body else {
            return Err("expected status".into());
        };
        let next = data.after.ok_or("missing cursor")?.next;
        assert!(next > cursor);
        cursor = next;
        let Some(Item::LatestValue(value)) = data.item else {
            return Err("expected latest value".into());
        };
        attention |= value.attention;
    }
    assert!(attention);
    let first = client.connections().changed(0).await?;
    fixture.reset()?;
    client.connections().changed(first).await?;
    (async {
        loop {
            let changed = fixture.counts.changed.notified();
            if fixture
                .counts
                .resumes
                .lock()
                .unwrap()
                .iter()
                .any(|(_, generation, next)| *generation == 2 && *next == 8)
            {
                break;
            }
            changed.await;
        }
    })
    .await;
    Ok(())
}

#[tokio::test]
async fn filtered_loopback_preserves_idle_cuts_matches_and_checked_resume() -> TestResult {
    let provider = Arc::new(MemoryStream::default());
    let fixture = Fixture::open(provider, "filtered-node").await?;
    let client = fixture.client()?;
    crate::filtered_follow_tests::qualify_progress(&client).await?;
    assert_eq!(fixture.counts.sessions.load(Ordering::Relaxed), 1);
    assert_eq!(fixture.counts.actors.load(Ordering::Relaxed), 0);
    Ok(())
}
