//! In-process I/O uses the public pool, including checked admission and credit.
use super::*;

#[tokio::test]
async fn in_process_follow_preserves_history_anchors_and_credit() -> TestResult {
    for records in [1, 2, 8] {
        let provider = Arc::new(MemoryStream::default());
        append(&provider, "events", records).await?;
        let client = Client::in_process(
            provider.clone(),
            crate::session::testing::authority(),
            Arc::new(crate::runtime::PlatformExecutor),
        );
        client.register_credential(0, crate::session::testing::credential())?;
        let path = StreamPath::new("events")?;
        let mut ordinary = client
            .follow(crate::FollowRequest::new(path.clone(), 0))
            .map_ok(crate::records_only)
            .await?;
        let first = next(&mut ordinary).await?;
        assert_eq!(first.sequence, 0);
        assert!(matches!(
            client
                .follow(crate::FollowRequest {
                    when_absent: Default::default(),
                    path: path.clone(),
                    from: 1,
                    preceding: Some(crate::CommitId::from_bytes([42; 32])),
                    filter: crate::FollowFilter::default(),
                    progress: false,
                    at_least: Default::default(),
                })
                .map_ok(crate::records_only)
                .await,
            Err(StreamError::PrefixNotRetained)
        ));
        let mut checked = client
            .follow(crate::FollowRequest {
                when_absent: Default::default(),
                path: path,
                from: 1,
                preceding: Some(first.commit_id),
                filter: crate::FollowFilter::default(),
                progress: false,
                at_least: Default::default(),
            })
            .map_ok(crate::records_only)
            .await?;
        append(&provider, "events", 1).await?;
        for sequence in 1..=records as u64 {
            assert_eq!(next(&mut ordinary).await?.sequence, sequence);
            assert_eq!(next(&mut checked).await?.sequence, sequence);
        }
        assert_eq!(client.pool.connections.changed(0).await?, 1);
        drop((ordinary, checked));
        assert_eq!(client.pool.active.load(super::super::Ordering::Acquire), 0);
    }
    Ok(())
}
