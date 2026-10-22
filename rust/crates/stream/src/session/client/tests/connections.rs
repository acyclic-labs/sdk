use super::*;

struct ReplacedNode {
    provider: Arc<MemoryStream>,
    replacement: AtomicUsize,
    destinations: Mutex<Vec<String>>,
    closed: Arc<tokio::sync::Notify>,
}

struct NotifySessionClosed(Arc<tokio::sync::Notify>);

impl Drop for NotifySessionClosed {
    fn drop(&mut self) {
        self.0.notify_one();
    }
}

impl transport::Transport for ReplacedNode {
    fn session(
        &self,
        mut source: transport::Requests,
    ) -> BoxProviderFuture<'_, Result<transport::Responses, StreamError>> {
        Box::pin(async move {
            let hello = source.next().await.ok_or(StreamError::Unavailable)?;
            let Some(Body::Hello(ref value)) = hello.body else {
                return Err(StreamError::InvalidArgument);
            };
            self.destinations
                .lock()
                .unwrap()
                .push(value.destination_node.clone());
            let node = if self.replacement.load(Ordering::Acquire) == 0 {
                "old"
            } else {
                "new"
            };
            if !value.destination_node.is_empty() && value.destination_node != node {
                return Err(StreamError::InvalidArgument);
            }
            let output = super::super::super::serve(
                self.provider.clone(),
                node.into(),
                Limits::default(),
                Box::pin(stream::once(future::ready(Ok(hello))).chain(source.map(Ok))),
            )?;
            let lifetime = NotifySessionClosed(self.closed.clone());
            let response: transport::Responses = Box::pin(stream::unfold(
                (output, lifetime),
                |(mut output, lifetime)| async move {
                    output.next().await.map(|frame| (frame, (output, lifetime)))
                },
            ));
            Ok(response)
        })
    }
}

#[tokio::test]
async fn same_client_discovers_replacement_after_idle_close() -> TestResult {
    tokio::time::timeout(Duration::from_secs(10), async {
        let provider = Arc::new(MemoryStream::default());
        append(&provider, "events", 1).await?;
        let endpoint = Arc::new(ReplacedNode {
            provider,
            replacement: AtomicUsize::new(0),
            destinations: Mutex::new(Vec::new()),
            closed: Arc::new(tokio::sync::Notify::new()),
        });
        let client = Client::with_transport(
            endpoint.clone(),
            Arc::new(|_| Box::pin(future::ready(Err(StreamError::Unavailable)))),
            Arc::new(super::super::super::runtime::PlatformExecutor),
        );
        let mut first = client
            .follow(
                StreamPath::new("events")?,
                0,
                crate::FollowFilter::default(),
            )
            .await?;
        assert_eq!(next(&mut first).await?.sequence, 0);
        endpoint.replacement.store(1, Ordering::Release);
        drop(first);
        endpoint.closed.notified().await;
        let mut second = client
            .follow(
                StreamPath::new("events")?,
                0,
                crate::FollowFilter::default(),
            )
            .await?;
        assert_eq!(next(&mut second).await?.sequence, 0);
        assert_eq!(*endpoint.destinations.lock().unwrap(), ["", ""]);
        Ok::<_, Box<dyn std::error::Error + Send + Sync>>(())
    })
    .await?
}

#[tokio::test]
async fn failed_home_dial_resets_pending_admissions_only_for_connection_errors() -> TestResult {
    let channel = Endpoint::from_static("http://unused.invalid").connect_lazy();
    for (error, link_failure) in [
        (StreamError::Unavailable, false),
        (StreamError::InvalidArgument, false),
        (StreamError::AccessDenied, false),
        (StreamError::Unavailable, true),
        (StreamError::InvalidArgument, true),
        (StreamError::AccessDenied, true),
    ] {
        let (mut pool, _healthy) = pool::fixture::fixture(&channel);
        pool.admissions.insert(5, Arc::new(Progress::new(0)))?;
        if link_failure {
            pool.fail_link("good", &error);
        } else {
            pool.dial_ready("good", 0, Err(error.clone()))?;
        }
        let reset = error != StreamError::AccessDenied;
        assert_eq!(pool.home, if reset { "" } else { "good" });
        assert_eq!(pool.admissions.interested(""), reset);
        assert_eq!(pool.admissions.interested("good"), !reset);
        assert!(pool.routes.contains_key(&1), "other nodes remain usable");
    }
    Ok(())
}

#[tokio::test]
async fn idle_close_resets_home_and_pending_admissions() -> TestResult {
    let channel = Endpoint::from_static("http://unused.invalid").connect_lazy();
    let (mut pool, _healthy) = pool::fixture::fixture(&channel);
    pool.close_idle();
    assert!(pool.home.is_empty());
    pool.admissions.insert(5, Arc::new(Progress::new(0)))?;
    assert!(pool.admissions.interested(""));
    assert!(!pool.admissions.interested("good"));
    assert!(pool.links.is_empty());
    Ok(())
}

#[tokio::test]
async fn discovery_and_home_recovery_reuse_the_first_installed_link() -> TestResult {
    let channel = Endpoint::from_static("http://unused.invalid").connect_lazy();
    // Discovery reuses the recovered home: only the newly bound route is queued.
    // Recovery reuses the discovered home: its routes were queued at install,
    // and a second same-generation upsert would end the whole session.
    for (destination, queued) in [("", vec![(1, 1)]), ("good", vec![])] {
        let (mut pool, _healthy) = pool::fixture::fixture(&channel);
        pool.reset_home();
        let (records, _pending) = mpsc::channel(2);
        let route = pool.routes.get_mut(&1).unwrap();
        route.node = destination.into();
        route.records = records;
        route.opened = false;
        let redundant = pool.links.remove("bad").unwrap();
        pool.dial_ready(
            destination,
            0,
            Ok(("good".into(), redundant, Box::pin(stream::empty()))),
        )?;
        assert_eq!(pool.links.get("good").unwrap().epoch, 2);
        assert_eq!(pool.routes.get(&1).unwrap().node, "good");
        assert_eq!(pool.pending.iter().copied().collect::<Vec<_>>(), queued);
        assert!(pool.routes.get(&3).unwrap().opened);
    }
    Ok(())
}

#[tokio::test]
async fn successful_home_recovery_rebinds_discovery_but_moved_nodes_do_not() -> TestResult {
    let channel = Endpoint::from_static("http://unused.invalid").connect_lazy();
    for home_endpoint in [true, false] {
        let (mut pool, _healthy) = pool::fixture::fixture(&channel);
        pool.reset_home();
        pool.admissions.insert(5, Arc::new(Progress::new(0)))?;
        let (records, _pending) = mpsc::channel(2);
        let route = pool.routes.get_mut(&1).unwrap();
        route.node.clear();
        route.records = records;
        route.opened = false;
        let mut recovered = pool.links.remove("bad").unwrap();
        if home_endpoint {
            recovered.channel = pool.home_channel.clone();
        }
        pool.dial_ready(
            "good",
            0,
            Ok(("good".into(), recovered, Box::pin(stream::empty()))),
        )?;
        assert_eq!(pool.home, if home_endpoint { "good" } else { "" });
        assert_eq!(pool.admissions.interested("good"), home_endpoint);
        assert_eq!(pool.admissions.interested(""), !home_endpoint);
        assert_eq!(
            pool.routes.get(&1).unwrap().node,
            if home_endpoint { "good" } else { "" }
        );
        assert_eq!(
            pool.pending.iter().copied().collect::<Vec<_>>(),
            if home_endpoint { vec![(1, 1)] } else { vec![] }
        );
    }
    Ok(())
}

#[tokio::test]
async fn reconnect_rediscovers_only_the_authenticated_home_endpoint() -> TestResult {
    let channel = Endpoint::from_static("http://unused.invalid").connect_lazy();
    for home_endpoint in [true, false] {
        let (mut pool, _healthy) = pool::fixture::fixture(&channel);
        if home_endpoint {
            pool.links.get_mut("good").unwrap().channel = pool.home_channel.clone();
        } else {
            pool.home = "bad".into();
            pool.admissions.bind_home("bad")?;
        }
        pool.admissions.insert(5, Arc::new(Progress::new(0)))?;
        pool.reconnect("good")?;
        let destination = if home_endpoint { "" } else { "good" };
        let home = if home_endpoint { "" } else { "bad" };
        assert_eq!(pool.home, home);
        assert!(pool.admissions.interested(home));
        assert_eq!(pool.routes.get(&3).unwrap().node, destination);
        assert_eq!(pool.routes.get(&3).unwrap().generation, 2);
        assert!(!pool.routes.get(&3).unwrap().opened);
        assert_eq!(
            pool.dials.keys().map(String::as_str).collect::<Vec<_>>(),
            [destination]
        );
        assert_eq!(pool.links.get("bad").unwrap().epoch, 1);
        assert!(pool.routes.get(&1).unwrap().opened);
    }
    Ok(())
}

#[tokio::test]
async fn active_home_replacement_rediscovers_and_resumes_over_tls() -> TestResult {
    let provider = Arc::new(MemoryStream::default());
    provider
        .append(AppendRequest {
            path: StreamPath::new("events")?,
            records: (0..6).map(|index| OwnedBytes::from(vec![index])).collect(),
            if_tail: None,
            idempotency_key: None,
        })
        .await?;
    let mut expected = provider
        .read(crate::ReadRequest {
            path: StreamPath::new("events")?,
            from: 0,
            limit: 6,
        })
        .await?;
    let fixture = Fixture::with_options(
        provider.clone(),
        "old",
        fixture::Options {
            tls: true,
            ..Default::default()
        },
    )
    .await?;
    let client = fixture.client()?;
    let mut records = client.follow(StreamPath::new("events")?, 0).await?;
    for _ in 0..2 {
        assert_eq!(next(&mut records).await?, next(&mut expected).await?);
    }
    *fixture
        .counts
        .node
        .lock()
        .map_err(|_| "fixture node poisoned")? = "replacement".into();
    fixture.reset()?;
    tokio::time::timeout(
        Duration::from_secs(1),
        fixture.until(|counts| {
            counts
                .resumes
                .lock()
                .is_ok_and(|resumes| resumes.contains(&(1, 2, 2)))
        }),
    )
    .await?;
    for _ in 2..6 {
        assert_eq!(next(&mut records).await?, next(&mut expected).await?);
    }
    let mut later = client.follow(StreamPath::new("events")?, 6).await?;
    append(&provider, "events", 1).await?;
    let mut tail = provider
        .read(crate::ReadRequest {
            path: StreamPath::new("events")?,
            from: 6,
            limit: 1,
        })
        .await?;
    let tail = next(&mut tail).await?;
    assert_eq!(next(&mut records).await?, tail);
    assert_eq!(next(&mut later).await?, tail);
    assert_eq!(fixture.counts.sessions.load(Ordering::Relaxed), 2);
    assert_eq!(fixture.counts.connections.load(Ordering::Relaxed), 1);
    assert_eq!(fixture.counts.actors.load(Ordering::Relaxed), 0);
    assert_eq!(
        *fixture
            .counts
            .destinations
            .lock()
            .map_err(|_| "fixture destinations poisoned")?,
        ["", ""]
    );
    assert_eq!(
        *fixture
            .counts
            .resumes
            .lock()
            .map_err(|_| "fixture resumes poisoned")?,
        [(1, 1, 0), (1, 2, 2), (3, 1, 6)]
    );
    Ok(())
}

#[tokio::test]
async fn reconnect_pushes_a_generation_without_new_records() -> TestResult {
    let provider = Arc::new(MemoryStream::default());
    append(&provider, "idle", 1).await?;
    let fixture = Fixture::open(provider, "node").await?;
    let client = Client::with_connector(
        fixture._channel.clone(),
        Some("Bearer fixture".parse()?),
        None,
        Arc::new(|_| Box::pin(future::ready(Err(StreamError::Unavailable)))),
    );
    let connections = client.connections();
    let _idle = client
        .follow(StreamPath::new("idle")?, 1, crate::FollowFilter::default())
        .await?;
    let first = connections.changed(0).await?;
    fixture.reset()?;
    let second = tokio::time::timeout(Duration::from_secs(1), connections.changed(first)).await??;
    assert!(second > first);
    Ok(())
}
