//! In-process I/O uses the public pool, including checked admission and credit.
use super::*;

#[tokio::test]
async fn in_process_follow_preserves_history_anchors_and_credit() -> TestResult {
    for records in [1, 2, 8] {
        let provider = Arc::new(MemoryStream::default());
        append(&provider, "events", records).await?;
        let client = Client::in_process(
            provider.clone(),
            Arc::new(super::super::super::runtime::PlatformExecutor),
        );
        let path = StreamPath::new("events")?;
        let mut ordinary = client
            .follow(path.clone(), 0, crate::FollowFilter::default())
            .await?;
        let first = next(&mut ordinary).await?;
        assert_eq!(first.sequence, 0);
        assert!(matches!(
            client
                .follow_checked(
                    path.clone(),
                    1,
                    crate::CommitId::from_bytes([42; 32]),
                    crate::FollowFilter::default()
                )
                .await,
            Err(StreamError::PrefixNotRetained)
        ));
        let mut checked = client
            .follow_checked(path, 1, first.commit_id, crate::FollowFilter::default())
            .await?;
        append(&provider, "events", 1).await?;
        for sequence in 1..=records as u64 {
            assert_eq!(next(&mut ordinary).await?.sequence, sequence);
            assert_eq!(next(&mut checked).await?.sequence, sequence);
        }
        assert_eq!(client.connections.changed(0).await?, 1);
        drop((ordinary, checked));
        assert_eq!(client.active.load(super::super::Ordering::Acquire), 0);
    }
    Ok(())
}
