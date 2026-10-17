use super::*;

#[tokio::test]
async fn malformed_discovery_closes_only_its_node_and_keeps_later_opens_live() -> TestResult {
    tokio::time::timeout(Duration::from_secs(3), async {
        let memory = Arc::new(MemoryStream::default());
        append(&memory, "healthy", 1).await?;
        append(&memory, "agents/first", 1).await?;
        let good = Fixture::open(Arc::clone(&memory), "good").await?;
        let bad = Fixture::open(Arc::clone(&memory), "bad").await?;
        let destination = bad.channel.clone();
        let connect: Connect = Arc::new(move |node| {
            assert_eq!(node.node_id, "bad");
            let channel = destination.clone();
            Box::pin(async move { Ok(channel) })
        });
        let client = Client::with_connector(
            good.channel.clone(),
            Some("Bearer fixture".parse()?),
            None,
            connect,
        );
        let mut healthy = client.follow(StreamPath::new("healthy")?, 1).await?;
        let mut children = client
            .children(crate::ChildrenRequest {
                parent: Some(StreamPath::new("agents")?),
                limit: 1,
            })
            .await?;
        assert_eq!(
            children
                .next()
                .await
                .ok_or("discovery ended")??
                .path
                .as_str(),
            "agents/first"
        );
        let id = good
            .counts
            .resumes
            .lock()
            .map_err(|_| "resumes")?
            .last()
            .ok_or("missing child route")?
            .0;
        good.controls.send(Ok(frame(
            id,
            1,
            Body::Moved(wire::SessionMoved {
                node: Some(wire::SessionNode {
                    node_id: "bad".into(),
                    endpoint: "https://bad.invalid".into(),
                    placement_epoch: 1,
                }),
                resume: None,
            }),
        )))?;
        loop {
            let changed = bad.counts.changed.notified();
            if bad.counts.windows.load(Ordering::Relaxed) > 0 {
                break;
            }
            changed.await;
        }
        bad.controls.send(Ok(frame(
            id,
            2,
            Body::Data(wire::SessionData {
                after: Some(wire::SessionCursor {
                    next: 2,
                    opaque: Bytes::from_static(b"other/child"),
                }),
                accepted: 2,
                durable: 0,
                item: Some(Item::Child(wire::Child {
                    path: "other/child".into(),
                })),
            }),
        )))?;
        assert_eq!(children.next().await, Some(Err(StreamError::Unavailable)));
        assert!(children.next().await.is_none());
        let mut later = client.follow(StreamPath::new("healthy")?, 1).await?;
        append(&memory, "healthy", 1).await?;
        assert_eq!(next(&mut healthy).await?.sequence, 1);
        assert_eq!(next(&mut later).await?.sequence, 1);
        for fixture in [&good, &bad] {
            assert_eq!(fixture.counts.sessions.load(Ordering::Relaxed), 1);
            assert_eq!(fixture.counts.connections.load(Ordering::Relaxed), 1);
            assert_eq!(fixture.counts.actors.load(Ordering::Relaxed), 0);
        }
        Ok::<_, Box<dyn std::error::Error + Send + Sync>>(())
    })
    .await??;
    Ok(())
}
