use super::*;
use crate::testing::Counted;

fn follow(
    provider: Arc<Counted<crate::MemoryStream>>,
    path: StreamPath,
) -> Follow<Counted<crate::MemoryStream>> {
    Follow {
        provider,
        path,
        next: 0,
        preceding: None,
        until: None,
        end: None,
        filter: Default::default(),
        at_least: Default::default(),
        when_absent: crate::WhenAbsent::NotFound,
        records: None,
        key: None,
        read_limit: 0,
        key_records: None,
        bootstrap_head: false,
        bootstrap: None,
        opened: false,
    }
}

#[tokio::test]
async fn original_follow_eof_is_terminal_without_reopening_the_provider()
-> Result<(), Box<dyn std::error::Error>> {
    let provider = Arc::new(Counted::new(crate::MemoryStream::default()));
    let path = StreamPath::new("original-follow")?;
    let append = |value: &'static [u8], tail| crate::AppendRequest {
        path: path.clone(),
        records: vec![Bytes::from_static(value).into()],
        if_tail: Some(tail),
        idempotency_key: crate::IdempotencyKey::unique(),
    };
    provider.append(append(b"first", 0)).await?;
    let mut original = follow(Arc::clone(&provider), path.clone());
    assert!(matches!(original.event().await?, Event::Opened(_, _)));
    // End the actual provider's stream after its real retained record. The
    // former path silently reopened at the next cursor and never returned.
    original.records = Some(
        original
            .records
            .take()
            .ok_or("missing follow")?
            .take(1)
            .boxed(),
    );
    assert!(
        matches!(original.event().await?, Event::Record(record) if record.value.as_slice() == b"first")
    );
    let before = provider.requests();
    assert!(matches!(
        original.event().await,
        Err(StreamError::Unavailable)
    ));
    let ended = provider.requests().since(before);
    assert_eq!(ended.control(), 0);
    assert_eq!(ended.data(), 0);

    // A separate invocation remains live at the same real provider; its
    // pending wait ends on the original append event without reopening.
    let mut explicit = follow(Arc::clone(&provider), path.clone());
    explicit.next = 1;
    assert!(matches!(explicit.event().await?, Event::Opened(_, _)));
    let before = provider.requests();
    let mut pending = std::pin::pin!(explicit.event());
    assert!(futures::poll!(pending.as_mut()).is_pending());
    provider.append(append(b"second", 1)).await?;
    assert!(
        matches!(pending.await?, Event::Record(record) if record.sequence == 1 && record.value.as_slice() == b"second")
    );
    let delivered = provider.requests().since(before);
    assert_eq!(delivered.control(), 0);
    assert_eq!(delivered.data(), 1, "only the original append");

    // The same physical EOF is a clean end only when the caller's finite
    // range has already been delivered.
    original.end = Some(original.next);
    assert!(matches!(original.event().await?, Event::End));
    Ok(())
}
