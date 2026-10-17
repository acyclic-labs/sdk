use super::*;

struct PausedPeer {
    counts: Arc<Counts>,
    resume: Arc<tokio::sync::Notify>,
    closed: Arc<tokio::sync::Notify>,
}
struct Closed(Arc<tokio::sync::Notify>);
impl Drop for Closed {
    fn drop(&mut self) {
        self.0.notify_one();
    }
}
#[tonic::async_trait]
impl StreamSessionService for PausedPeer {
    type SessionStream = BoxStream<'static, Result<wire::SessionFrame, Status>>;
    async fn session(
        &self,
        request: Request<Streaming<wire::SessionFrame>>,
    ) -> Result<Reply<Self::SessionStream>, Status> {
        assert_eq!(
            request
                .metadata()
                .get("authorization")
                .and_then(|value| value.to_str().ok()),
            Some("Bearer fixture")
        );
        self.counts.sessions.fetch_add(1, Ordering::Relaxed);
        let mut input = request.into_inner();
        let hello = input
            .message()
            .await?
            .ok_or_else(|| Status::invalid_argument("missing hello"))?;
        assert!(matches!(hello.body, Some(Body::Hello(_))));
        let welcome = frame(
            0,
            0,
            Body::Welcome(wire::SessionWelcome {
                version: 1,
                node_id: "paused".into(),
                receive_bytes: crate::MAX_COMMAND_BYTES as u64,
                receive_frames: u32::try_from(crate::MAX_ITEMS)
                    .map_err(|_| Status::internal("frame limit does not fit"))?,
            }),
        );
        let guard = Closed(Arc::clone(&self.closed));
        let following = stream::try_unfold(
            (input, false, guard, Arc::clone(&self.resume)),
            |(mut input, moved, guard, resume)| async move {
                if moved {
                    // Retain the request and response while consuming no more
                    // controls. HTTP/2 eventually exhausts its receive window.
                    resume.notified().await;
                    while input.message().await?.is_some() {}
                    drop(guard);
                    return Ok(None);
                }
                let interest = input
                    .message()
                    .await?
                    .ok_or_else(|| Status::invalid_argument("missing interest"))?;
                let Some(Body::Interest(interest)) = interest.body else {
                    return Err(Status::invalid_argument("expected interest"));
                };
                let channel = interest
                    .upsert
                    .first()
                    .and_then(|subscription| subscription.channel)
                    .ok_or_else(|| Status::invalid_argument("missing channel"))?;
                let moved = frame(
                    channel.channel_id,
                    channel.generation,
                    Body::Moved(wire::SessionMoved {
                        node: Some(wire::SessionNode {
                            node_id: "healthy".into(),
                            endpoint: "https://healthy.invalid".into(),
                            placement_epoch: 1,
                        }),
                        resume: None,
                    }),
                );
                Ok(Some((moved, (input, true, guard, resume))))
            },
        );
        Ok(Reply::new(
            stream::once(future::ready(Ok(welcome)))
                .chain(following)
                .boxed(),
        ))
    }
}

async fn paused_peer() -> Result<
    (Fixture, Arc<tokio::sync::Notify>, Arc<tokio::sync::Notify>),
    Box<dyn std::error::Error + Send + Sync>,
> {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
    let endpoint = format!("http://{}", listener.local_addr()?);
    let counts = Arc::new(Counts::default());
    let accepted = Arc::clone(&counts);
    let incoming = stream::try_unfold(listener, move |listener| {
        let counts = Arc::clone(&accepted);
        async move {
            let (socket, _) = listener.accept().await?;
            socket.set_nodelay(true)?;
            counts.connections.fetch_add(1, Ordering::Relaxed);
            Ok::<_, std::io::Error>(Some((socket, listener)))
        }
    });
    let closed = Arc::new(tokio::sync::Notify::new());
    let resume = Arc::new(tokio::sync::Notify::new());
    let host = PausedPeer {
        counts: Arc::clone(&counts),
        resume: Arc::clone(&resume),
        closed: Arc::clone(&closed),
    };
    let server = tokio::spawn(
        tonic::transport::Server::builder()
            .add_service(StreamSessionServiceServer::new(host))
            .serve_with_incoming(incoming),
    );
    let channel = Endpoint::from_shared(endpoint)?
        .tcp_nodelay(true)
        .connect()
        .await?;
    let (controls, _) = tokio::sync::broadcast::channel(16);
    Ok((
        Fixture {
            controls,
            _channel: channel,
            tls_client: None,
            counts,
            server,
        },
        resume,
        closed,
    ))
}

#[tokio::test]
async fn a_peer_that_stops_reading_controls_cannot_stall_healthy_nodes_or_cancellation()
-> TestResult {
    let phase = std::cell::Cell::new("connecting nodes");
    let result = tokio::time::timeout(Duration::from_secs(8), async {
        let memory = Arc::new(MemoryStream::default());
        append(&memory, "healthy", 1).await?;
        let healthy = Fixture::open(Arc::clone(&memory), "healthy").await?;
        let (paused, resume, closed) = paused_peer().await?;
        let destination = healthy._channel.clone();
        let connect: Connect = Arc::new(move |node| {
            assert_eq!(node.node_id, "healthy");
            let channel = destination.clone();
            Box::pin(async move { Ok(channel) })
        });
        let client = Client::with_connector(
            paused._channel.clone(),
            Some("Bearer fixture".parse()?),
            None,
            connect,
        );
        phase.set("opening redirected healthy follow");
        let mut records = client.follow(StreamPath::new("healthy")?, 1).await?;
        let rejected = Arc::new(tokio::sync::Notify::new());
        let rejection = Arc::clone(&rejected);
        let opening = client.clone();
        let storm = tokio::spawn(async move {
            let path = StreamPath::new("x".repeat(4096)).expect("bounded stream path");
            let mut opens: FuturesUnordered<_> =
                (0..1400).map(|_| opening.follow(path.clone(), 0)).collect();
            while let Some(result) = opens.next().await {
                if matches!(result, Err(StreamError::Capacity)) {
                    rejection.notify_one();
                }
            }
        });
        phase.set("waiting for bounded admission rejection");
        rejected.notified().await;
        phase.set("delivering while the peer is blocked");
        assert_eq!(paused.counts.sessions.load(Ordering::Relaxed), 1);
        for sequence in 1..=4 {
            append(&memory, "healthy", 1).await?;
            assert_eq!(next(&mut records).await?.sequence, sequence);
        }
        phase.set("cancelling blocked opens");
        storm.abort();
        assert!(storm.await.is_err_and(|error| error.is_cancelled()));
        phase.set("delivering after cancellation");
        for sequence in 5..=8 {
            append(&memory, "healthy", 1).await?;
            assert_eq!(next(&mut records).await?.sequence, sequence);
        }
        assert_eq!(healthy.counts.connections.load(Ordering::Relaxed), 1);
        assert_eq!(healthy.counts.sessions.load(Ordering::Relaxed), 1);
        assert_eq!(healthy.counts.actors.load(Ordering::Relaxed), 0);
        drop(records);
        drop(client);
        // Hyper cannot finish a buffered request body while the peer withholds
        // HTTP/2 credit. All isolation/cancellation assertions ran while paused;
        // now allow the peer to drain and observe the request's eventual EOF.
        resume.notify_one();
        phase.set("waiting for cancelled RPC cleanup");
        closed.notified().await;
        Ok::<_, Box<dyn std::error::Error + Send + Sync>>(())
    })
    .await;
    if result.is_err() {
        eprintln!("blocked-peer watchdog expired while {}", phase.get());
    }
    result?
}

#[tokio::test]
async fn native_discovery_pushes_creations_after_snapshot_exhaustion() -> TestResult {
    let provider = Arc::new(MemoryStream::default());
    append(&provider, "agents/first", 1).await?;
    let fixture = Fixture::open(Arc::clone(&provider), "discovery").await?;
    let client = fixture.client()?;
    let mut children = client
        .children(crate::ChildrenRequest {
            parent: Some(StreamPath::new("agents")?),
            limit: 1,
        })
        .await?;
    assert_eq!(
        tokio::time::timeout(Duration::from_secs(1), children.next())
            .await?
            .ok_or("snapshot ended")??
            .path
            .as_str(),
        "agents/first"
    );
    assert!(futures::poll!(children.next()).is_pending());
    let mut records = client.follow(StreamPath::new("agents/first")?, 1).await?;
    append(&provider, "agents/later", 1).await?;
    assert_eq!(
        tokio::time::timeout(Duration::from_secs(1), children.next())
            .await?
            .ok_or("discovery ended after snapshot exhaustion")??
            .path
            .as_str(),
        "agents/later"
    );
    assert!(futures::poll!(children.next()).is_pending());
    append(&provider, "agents/first", 1).await?;
    assert_eq!(next(&mut records).await?.sequence, 1);
    assert_eq!(fixture.counts.connections.load(Ordering::Relaxed), 1);
    assert_eq!(fixture.counts.sessions.load(Ordering::Relaxed), 1);
    Ok(())
}
