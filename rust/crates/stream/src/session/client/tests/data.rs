use super::*;

fn subscription(path: &str, delivery: wire::SessionDelivery) -> wire::SessionSubscription {
    wire::SessionSubscription {
        credential: 0,
        filter: None,
        resume: Some(wire::SessionCursor::default()),
        kind: Some(Kind::Follow(wire::SessionFollow {
            path: path.into(),
            delivery: delivery as i32,
            ..Default::default()
        })),
        channel: None,
    }
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
    let mut all = client
        .subscribe(subscription("events", wire::SessionDelivery::All))
        .await?;
    let record = all.next().await.ok_or("missing data")??;
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
            path: "events".into(),
            delivery: wire::SessionDelivery::All as i32,
            preceding_commit_id: value.commit_id,
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
    let mark = tokio::time::timeout(Duration::from_secs(1), all.next())
        .await?
        .ok_or("missing watermark")??;
    assert!(matches!(
        mark.body,
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
            wire::SessionLatestValue {
                key: Bytes::from_static(b"agent"),
                value: Bytes::copy_from_slice(&[sequence]),
                attention: sequence == 3,
            }
            .encode_to_vec()
            .into()
        })
        .collect();
    provider
        .append(AppendRequest {
            path: StreamPath::new("status")?,
            records: values,
            if_tail: None,
            idempotency_key: None,
        })
        .await?;
    let fixture = Fixture::open(provider, "node").await?;
    let client = Client::with_connector(
        fixture._channel.clone(),
        Some("Bearer fixture".parse()?),
        None,
        Arc::new(|_| Box::pin(future::ready(Err(StreamError::Unavailable)))),
    );
    let mut latest = client
        .subscribe(subscription("status", wire::SessionDelivery::Latest))
        .await?;
    let mut cursor = 0;
    let mut attention = false;
    while cursor < 8 {
        let frame = tokio::time::timeout(Duration::from_secs(1), latest.next())
            .await?
            .ok_or("missing status")??;
        let Some(Body::Data(data)) = frame.body else {
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
    tokio::time::timeout(Duration::from_secs(1), client.connections().changed(first)).await??;
    tokio::time::timeout(Duration::from_secs(1), async {
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
    .await?;
    Ok(())
}
