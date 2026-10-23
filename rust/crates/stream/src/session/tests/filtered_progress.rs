use super::*;

async fn keyed(provider: &MemoryStream, key: &'static str) -> TestResult {
    provider
        .append(AppendRequest {
            path: StreamPath::new("progress")?,
            records: vec![crate::AppendRecord {
                value: key.into(),
                headers: crate::RecordHeaders::new(vec![key.into()], vec![])?,
            }],
            if_tail: None,
            idempotency_key: None,
        })
        .await?;
    Ok(())
}

#[tokio::test]
async fn idle_skips_need_no_credit_and_never_pass_a_queued_match() -> TestResult {
    let provider = Arc::new(MemoryStream::default());
    keyed(&provider, "private").await?;
    let (sender, receiver) = mpsc::unbounded();
    sender.unbounded_send(Ok(hello(1_000_000, 0)))?;
    let mut subscription = subscribe(1, "progress", 1, false);
    subscription.filter = Some(wire::SessionFilter {
        keys: vec![wire::KeyRange {
            start: b"public".as_slice().into(),
            end: b"publid".as_slice().into(),
        }],
        attributes: vec![],
    });
    sender.unbounded_send(Ok(interest(1, vec![subscription])))?;
    let mut output = serve(
        Arc::clone(&provider),
        crate::session::testing::authority(),
        "node".into(),
        Limits::default(),
        receiver.boxed(),
    )?;
    assert!(matches!(
        next(&mut output).await?.body,
        Some(Body::Welcome(_))
    ));
    assert!(matches!(
        next(&mut output).await?.body,
        Some(Body::InterestApplied(_))
    ));
    assert!(matches!(
        next(&mut output).await?.body,
        Some(Body::Opened(_))
    ));
    keyed(&provider, "private").await?;
    let mark = next(&mut output).await?;
    assert!(matches!(
        mark.body,
        Some(Body::Watermark(wire::SessionWatermark {
            after: Some(wire::SessionCursor { next: 2, .. }),
            ..
        }))
    ));
    keyed(&provider, "public").await?;
    keyed(&provider, "private").await?;
    assert!(
        futures::poll!(output.next()).is_pending(),
        "progress passed an unsent matching record"
    );
    for channel in [0, 1] {
        sender.unbounded_send(Ok(frame(
            channel,
            u64::from(channel != 0),
            Body::Window(wire::SessionWindow {
                bytes: 1_000_000,
                frames: 1,
            }),
        )))?;
    }
    let data = next(&mut output).await?;
    assert!(matches!(
        data.body,
        Some(Body::Data(wire::SessionData {
            item: Some(Item::Record(wire::Record { sequence: 2, .. })),
            ..
        }))
    ));
    let progress = next(&mut output).await?;
    assert!(matches!(
        progress.body,
        Some(Body::Watermark(wire::SessionWatermark {
            after: Some(wire::SessionCursor { next: 4, .. }),
            ..
        }))
    ));
    Ok(())
}
