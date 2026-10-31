//! In-process I/O uses the public pool, including checked admission and credit.
use super::*;

#[tokio::test]
async fn in_process_follow_preserves_history_anchors_and_credit() -> TestResult {
    for records in [1, 2, 8] {
        let provider = Arc::new(MemoryStream::default());
        let payload = OwnedBytes::from(b"delta".to_vec());
        let backing = payload.backing().ok_or("missing original capacity")?;
        provider
            .append(AppendRequest {
                path: StreamPath::new("events")?,
                records: vec![payload.clone().into(); records],
                if_tail: None,
                idempotency_key: crate::IdempotencyKey::unique(),
            })
            .await?;
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
        assert_eq!(
            first.value.backing(),
            Some(backing.clone()),
            "ordinary follow lost original backing custody"
        );
        let mut typed = client
            .follow_records(crate::FollowRequest::new(path.clone(), 0))
            .await?;
        assert_eq!(
            next(&mut typed).await?.value.backing(),
            Some(backing.clone())
        );
        drop(typed);
        let mut finite = client
            .read(crate::ReadRequest {
                key: None,
                path: path.clone(),
                from: 0,
                limit: 1,
                at_least: Default::default(),
            })
            .await?;
        assert_eq!(
            next(&mut finite).await?.value.backing(),
            Some(backing.clone())
        );
        assert!(
            finite.next().await.is_none(),
            "finite owned read waited beyond its admitted end"
        );
        drop(finite);
        let mut application = client
            .deliveries(super::application::subscription())
            .await?;
        let delivery = super::application::data(&mut application).await?;
        let original = delivery
            .original_record()
            .ok_or("application lost provider record")?;
        assert_eq!(original.value.backing(), Some(backing));
        assert_eq!(original.value.as_slice(), first.value.as_slice());
        assert_eq!(
            application
                .checkpoint()?
                .resume
                .ok_or("missing initial cut")?
                .next,
            0
        );
        assert!(delivery.commit()?);
        assert_eq!(
            application
                .checkpoint()?
                .resume
                .ok_or("missing committed cut")?
                .next,
            1
        );
        drop(application);
        if records == 1 {
            let fixture = Fixture::open(provider.clone(), "node").await?;
            let network = fixture.client()?.session_client()?;
            let mut remote = network
                .follow_records(crate::FollowRequest::new(path.clone(), 0))
                .await?;
            let received = next(&mut remote).await?;
            assert_eq!(received.value.as_slice(), first.value.as_slice());
            assert!(
                received.value.backing().is_none(),
                "wire transport invented allocation capacity"
            );
        }
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
