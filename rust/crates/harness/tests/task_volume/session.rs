use super::*;
use acyclic_stream::{grpc, session, wire};
use std::sync::atomic::{AtomicUsize, Ordering};
use tokio::sync::{broadcast, mpsc};
use tonic::{Request, Response, Status, Streaming};
use wire::stream_session_service_server::{StreamSessionService, StreamSessionServiceServer};

#[derive(Default)]
struct Counts {
    connections: AtomicUsize,
    sessions: AtomicUsize,
    follows: AtomicUsize,
    children: AtomicUsize,
    actors: AtomicUsize,
}
struct Host {
    provider: Arc<MemoryStream>,
    counts: Arc<Counts>,
    resets: broadcast::Sender<()>,
    closed: mpsc::Sender<()>,
}
struct SessionLifetime(mpsc::Sender<()>);
impl Drop for SessionLifetime {
    fn drop(&mut self) {
        // A failed assertion may drop the receiver before fixture teardown.
        let _ = self.0.try_send(());
    }
}

#[tonic::async_trait]
impl StreamSessionService for Host {
    type SessionStream =
        acyclic_stream::BoxProviderStream<'static, std::result::Result<wire::SessionFrame, Status>>;

    async fn session(
        &self,
        request: Request<Streaming<wire::SessionFrame>>,
    ) -> std::result::Result<Response<Self::SessionStream>, Status> {
        assert_eq!(
            request
                .metadata()
                .get("authorization")
                .and_then(|value| value.to_str().ok()),
            Some("Bearer publication-test")
        );
        self.counts.sessions.fetch_add(1, Ordering::Relaxed);
        let counts = Arc::clone(&self.counts);
        let input = request
            .into_inner()
            .map(move |frame| {
                if let Ok(wire::SessionFrame {
                    body: Some(wire::session_frame::Body::Interest(interest)),
                    ..
                }) = &frame
                {
                    for subscription in &interest.upsert {
                        use wire::session_subscription::Kind;
                        let count = match subscription.kind {
                            Some(Kind::Follow(_)) => &counts.follows,
                            Some(Kind::Children(_)) => &counts.children,
                            Some(Kind::ActorSocket(_) | Kind::ActorCall(_)) => &counts.actors,
                            _ => continue,
                        };
                        count.fetch_add(1, Ordering::Relaxed);
                    }
                }
                frame.map_err(|_| acyclic_stream::StreamError::Unavailable)
            })
            .boxed();
        let output = session::serve(
            Arc::clone(&self.provider),
            "node".into(),
            session::Limits::default(),
            input,
        )
        .map_err(|error| Status::invalid_argument(error.code()))?;
        let lifetime = SessionLifetime(self.closed.clone());
        let resets = self.resets.subscribe();
        Ok(Response::new(futures::stream::unfold(
            Some((output, resets, lifetime)), |state| async move {
                let (mut output, mut resets, lifetime) = state?;
                tokio::select! {
                    frame = output.next() => frame.map(|frame| (
                        frame.map_err(|error| Status::failed_precondition(error.code())),
                        Some((output, resets, lifetime)),
                    )),
                    _ = resets.recv() => Some((Err(Status::unavailable("reset publication Session")), None)),
                }
            },
        ).boxed()))
    }
}

#[tokio::test]
async fn publication_observes_share_one_streams_session_and_resume_at_the_snapshot_cut() -> Result {
    let provider = Arc::new(MemoryStream::default());
    let volume_path = StreamPath::new("volumes/task")?;
    let writer = TaskVolume::open(Arc::clone(&provider), volume_path.clone()).await?;
    let counts = Arc::new(Counts::default());
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
    let endpoint = format!("https://localhost:{}", listener.local_addr()?.port());
    let certificate = rcgen::generate_simple_self_signed(vec!["localhost".into()])?;
    let pem = certificate.cert.pem();
    let accepted = Arc::clone(&counts);
    let incoming = futures::stream::try_unfold(listener, move |listener| {
        let counts = Arc::clone(&accepted);
        async move {
            let (socket, _) = listener.accept().await?;
            socket.set_nodelay(true)?;
            counts.connections.fetch_add(1, Ordering::Relaxed);
            Ok::<_, std::io::Error>(Some((socket, listener)))
        }
    });
    let unary = grpc::Service::new(Arc::clone(&provider));
    let (resets, _) = broadcast::channel(1);
    let (closed, mut ended) = mpsc::channel(2);
    let host = Host {
        resets: resets.clone(),
        closed,
        provider,
        counts: Arc::clone(&counts),
    };
    let server = tokio::spawn(
        tonic::transport::Server::builder()
            .tls_config(tonic::transport::ServerTlsConfig::new().identity(
                tonic::transport::Identity::from_pem(
                    pem.clone(),
                    certificate.signing_key.serialize_pem(),
                ),
            ))?
            .add_service(wire::stream_service_server::StreamServiceServer::new(unary))
            .add_service(StreamSessionServiceServer::new(host))
            .serve_with_incoming(incoming),
    );
    let result = tokio::time::timeout(Duration::from_secs(8), async {
        let client = Arc::new(grpc::Client::connect_eager_with_ca_certificate(
            endpoint, "publication-test", pem.as_bytes(),
        ).await?);
        let reader = TaskVolume::open(Arc::clone(&client), volume_path.clone()).await?;
        let query = Query::Root;
        let seed = writer.snapshot(&query, 1).await?;
        let mut generation = writer.append(seed.generation, &write("GOAL.md", "initial")?).await?;
        let mut observations = futures::future::try_join_all((0..32).map(|_| reader.observe(query.clone(), None, 1))).await?;
        let mut folded = BTreeMap::new();
        for observation in &observations {
            assert!(!observation.reset);
            let snapshot = observation.snapshot.as_ref().ok_or("initial query omitted snapshot")?;
            assert_eq!(snapshot.cursor.position, generation.position);
            assert_eq!(snapshot.files.len(), 1);
            folded.insert(snapshot.files[0].key.clone(), snapshot.files[0].clone());
        }
        // Keep an unread channel saturated while independent file queries advance.
        let unread = client.follow(volume_path, generation.position).await?;
        let mut children = client.children(acyclic_stream::ChildrenRequest { parent: Some(StreamPath::new("volumes")?), limit: 1 }).await?;
        assert_eq!(children.next().await.ok_or("discovery ended")??.path.as_str(), "volumes/task");
        let mut measured = Vec::new();
        let mut last = seed.cursor;
        for index in 0..40 {
            if index == 20 {
                assert_eq!(counts.sessions.load(Ordering::Relaxed), 1);
                resets.send(())?;
            }
            let content = format!("goal-{index}");
            let started = Instant::now();
            generation = writer.append(generation, &write("GOAL.md", &content)?).await?;
            for (view, observation) in observations.iter_mut().enumerate() {
                let change = observation.changes.next().await.ok_or("publication follow ended")??;
                if view == 0 && index >= 8 { measured.push(started.elapsed().as_nanos()); }
                assert_eq!(change.cursor.position, generation.position);
                assert_eq!(change.cursor.commit, generation.commit);
                let file = change.file.ok_or("root query filtered a publication")?;
                assert_eq!(file.bytes.as_ref(), content.as_bytes());
                if view == 0 { folded.insert(file.key.clone(), file); last = change.cursor; }
            }
        }
        let pinned = reader.snapshot_at(&query, last.clone(), 1).await?;
        assert_eq!(pinned.files, folded.values().cloned().collect::<Vec<_>>());
        drop(observations.remove(0));
        generation = writer.append(generation, &write("GOAL.md", "between-tiers")?).await?;
        let mut resumed = reader.observe(query.clone(), Some(last.clone()), 1).await?;
        assert!(!resumed.reset);
        assert!(resumed.snapshot.is_none());
        let change = resumed.changes.next().await.ok_or("resume ended")??;
        assert_eq!(change.cursor.position, last.position + 1);
        assert_eq!(change.cursor.commit, generation.commit);
        let file = change.file.ok_or("resumed change omitted file")?;
        folded.insert(file.key.clone(), file);
        let mut incompatible = change.cursor;
        incompatible.commit[0] ^= 1;
        let reset = reader.observe(query.clone(), Some(incompatible), 1).await?;
        assert!(reset.reset);
        let snapshot = reset.snapshot.ok_or("reset omitted replacement")?;
        assert_eq!(snapshot.files, folded.values().cloned().collect::<Vec<_>>());
        assert_eq!(writer.snapshot(&query, 1).await?.files, snapshot.files);
        let _later = TaskVolume::open(Arc::clone(&client), StreamPath::new("volumes/later")?).await?;
        assert_eq!(children.next().await.ok_or("discovery ended")??.path.as_str(), "volumes/later");
        assert_eq!(counts.connections.load(Ordering::Relaxed), 1);
        assert_eq!(counts.sessions.load(Ordering::Relaxed), 2);
        assert_eq!(counts.follows.load(Ordering::Relaxed), 68);
        assert_eq!(counts.children.load(Ordering::Relaxed), 2);
        assert_eq!(counts.actors.load(Ordering::Relaxed), 0);
        measured.sort_unstable();
        println!("publication_session follows=68 children=2 sessions=2 connections=1 actor_channels=0 samples=32 p50_ns={} p99_ns={}", measured[15], measured[31]);
        drop(unread);
        drop(children);
        drop(observations);
        drop(resumed);
        drop(reset.changes);
        // Every Session must close before force-closing the fixture server.
        for _ in 0..2 { ended.recv().await.ok_or("Session shutdown omitted")?; }
        Ok(()) as Result
    }).await;
    server.abort();
    result?
}
