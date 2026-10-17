use super::*;
use std::sync::atomic::{AtomicUsize, Ordering};
use tonic::{Request, Response, Status, Streaming};
use wire::stream_session_service_server::{StreamSessionService, StreamSessionServiceServer};

type TestResult = Result<(), Box<dyn std::error::Error + Send + Sync>>;
struct Host {
    service: crate::session::Service<MemoryStream>,
    sessions: Arc<AtomicUsize>,
}
#[tonic::async_trait]
impl StreamSessionService for Host {
    type SessionStream = futures::stream::BoxStream<'static, Result<wire::SessionFrame, Status>>;
    async fn session(
        &self,
        request: Request<Streaming<wire::SessionFrame>>,
    ) -> Result<Response<Self::SessionStream>, Status> {
        assert_eq!(
            request
                .metadata()
                .get("authorization")
                .and_then(|value| value.to_str().ok()),
            Some("Bearer fixture-token")
        );
        self.sessions.fetch_add(1, Ordering::Relaxed);
        self.service.session(request).await
    }
}
async fn append(provider: &MemoryStream, path: &str) -> Result<(), StreamError> {
    provider
        .append(AppendRequest {
            path: StreamPath::new(path)?,
            records: vec![OwnedBytes::from_static(b"delta")],
            if_tail: None,
            idempotency_key: None,
        })
        .await?;
    Ok(())
}

/// The open overview keeps the shared native session in use.
type Overview = futures::stream::BoxStream<'static, Result<wire::SessionData, StreamError>>;
/// A Latest overview over HTTP yields the attention value with its cut.
async fn latest_overview(
    provider: &MemoryStream,
    client: &HttpStream,
) -> Result<Overview, Box<dyn std::error::Error + Send + Sync>> {
    let latest_value = wire::SessionLatestValue {
        key: Bytes::from_static(b"agent"),
        value: Bytes::from_static(b"running"),
        attention: true,
    };
    provider
        .append(AppendRequest {
            path: StreamPath::new("status")?,
            records: vec![latest_value.encode_to_vec().into()],
            if_tail: None,
            idempotency_key: None,
        })
        .await?;
    let mut overview = client
        .follow_data(
            wire::SessionFollow {
                path: "status".into(),
                delivery: wire::SessionDelivery::Latest as i32,
                ..Default::default()
            },
            0,
        )
        .await?;
    let data = overview.next().await.ok_or("latest ended")??;
    assert_eq!(
        data.item,
        Some(wire::session_data::Item::LatestValue(latest_value))
    );
    assert_eq!(data.after.as_ref().map(|cursor| cursor.next), Some(1));
    assert_eq!((data.accepted, data.durable), (1, 0));
    Ok(overview)
}

/// Sample append-to-follow latency on the shared session after a warm-up.
async fn measure_append_follow(provider: &MemoryStream, measured: &mut RecordStream) -> TestResult {
    let mut latencies = Vec::new();
    for index in 0..144 {
        let started = std::time::Instant::now();
        let (_, record, elapsed) = crate::test_latency::sample(
            append(provider, "events"),
            async { measured.next().await.ok_or(StreamError::Unavailable)? },
            || started.elapsed().as_nanos(),
        )
        .await?;
        assert_eq!(record.sequence, index + 2);
        if index >= 16 {
            latencies.push(elapsed);
        }
    }
    latencies.sort_unstable();
    println!(
        "http_session_append_follow samples=128 p50_ns={} p99_ns={}",
        latencies.get(64).ok_or("p50")?,
        latencies.get(126).ok_or("p99")?
    );
    Ok(())
}

/// A loopback server task for the unary and session services.
type Server = tokio::task::JoinHandle<Result<(), tonic::transport::Error>>;
/// Serve both services on loopback, counting accepted connections and sessions.
async fn serve(
    provider: &Arc<MemoryStream>,
    connections: &Arc<AtomicUsize>,
    sessions: &Arc<AtomicUsize>,
) -> Result<(String, Server), Box<dyn std::error::Error + Send + Sync>> {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
    let endpoint = format!("http://{}", listener.local_addr()?);
    let accepted = Arc::clone(connections);
    let incoming = stream::try_unfold(listener, move |listener| {
        let accepted = Arc::clone(&accepted);
        async move {
            let (socket, _) = listener.accept().await?;
            socket.set_nodelay(true)?;
            accepted.fetch_add(1, Ordering::Relaxed);
            Ok::<_, std::io::Error>(Some((socket, listener)))
        }
    });
    let host = Host {
        service: crate::session::Service::new(
            Arc::clone(provider),
            "node".into(),
            crate::session::Limits::default(),
        ),
        sessions: Arc::clone(sessions),
    };
    let unary = crate::grpc::Service::new(Arc::clone(provider));
    let server = tokio::spawn(
        tonic::transport::Server::builder()
            .add_service(wire::stream_service_server::StreamServiceServer::new(unary))
            .add_service(StreamSessionServiceServer::new(host))
            .serve_with_incoming(incoming),
    );
    Ok((endpoint, server))
}

#[tokio::test]
async fn http_follows_and_live_children_share_one_native_session() -> TestResult {
    let provider = Arc::new(MemoryStream::default());
    append(&provider, "events").await?;
    let connections = Arc::new(AtomicUsize::new(0));
    let sessions = Arc::new(AtomicUsize::new(0));
    let (endpoint, server) = serve(&provider, &connections, &sessions).await?;
    let result: TestResult = async {
        let client = HttpStream::new(&endpoint, "fixture-token", MAX_COMMAND_BYTES)?;
        let _overview = latest_overview(&provider, &client).await?;
        let path = StreamPath::new("events")?;
        assert_eq!(
            client.watermarks(path.clone()).await?,
            StreamWatermarks {
                accepted: 1,
                durable: 0
            }
        );
        let clones: Vec<_> = (0..64).map(|_| client.clone()).collect();
        let mut follows =
            futures::future::try_join_all(clones.iter().map(|clone| clone.follow(path.clone(), 1)))
                .await?;
        let anchor = provider
            .read(ReadRequest {
                path: path.clone(),
                from: 0,
                limit: 1,
            })
            .await?
            .next()
            .await
            .ok_or("missing seed")??
            .commit_id;
        assert!(matches!(
            client
                .follow_checked(path.clone(), 1, CommitId::from_bytes([42; 32]))
                .await,
            Err(StreamError::PrefixNotRetained)
        ));
        let mut checked = client.follow_checked(path.clone(), 1, anchor).await?;
        let mut children = client
            .children(ChildrenRequest {
                parent: Some(StreamPath::new("agents")?),
                limit: 1,
            })
            .await?;
        assert!(futures::poll!(children.next()).is_pending());
        for name in ["m", "a"] {
            let path = format!("agents/{name}");
            append(&provider, &path).await?;
            assert_eq!(
                tokio::time::timeout(Duration::from_secs(1), children.next())
                    .await?
                    .ok_or("children ended")??
                    .path
                    .as_str(),
                path
            );
        }
        append(&provider, "events").await?;
        assert_eq!(
            tokio::time::timeout(Duration::from_secs(1), checked.next())
                .await?
                .ok_or("checked follow ended")??
                .sequence,
            1
        );
        drop(checked);
        for follow in &mut follows {
            assert_eq!(
                tokio::time::timeout(Duration::from_secs(1), follow.next())
                    .await?
                    .ok_or("follow ended")??
                    .sequence,
                1
            );
        }
        assert!(futures::poll!(children.next()).is_pending());
        drop(children);
        let mut measured = follows.pop().ok_or("missing measured follow")?;
        drop(follows);
        measure_append_follow(&provider, &mut measured).await?;
        assert_eq!(connections.load(Ordering::Relaxed), 1);
        assert_eq!(sessions.load(Ordering::Relaxed), 1);
        Ok(())
    }
    .await;
    server.abort();
    result
}

#[tokio::test]
async fn http_session_preserves_the_callers_additional_tls_root() -> TestResult {
    let certified = rcgen::generate_simple_self_signed(["localhost".to_owned()])?;
    let pem = certified.cert.pem();
    let identity =
        tonic::transport::Identity::from_pem(&pem, certified.signing_key.serialize_pem());
    let provider = Arc::new(MemoryStream::default());
    append(&provider, "events").await?;
    let incoming = tonic::transport::server::TcpIncoming::bind(([127, 0, 0, 1], 0).into())?;
    let endpoint = format!("https://localhost:{}", incoming.local_addr()?.port());
    let sessions = Arc::new(AtomicUsize::new(0));
    let host = Host {
        service: crate::session::Service::new(
            Arc::clone(&provider),
            "tls".into(),
            crate::session::Limits::default(),
        ),
        sessions: Arc::clone(&sessions),
    };
    let server = tokio::spawn(
        tonic::transport::Server::builder()
            .tls_config(tonic::transport::ServerTlsConfig::new().identity(identity))?
            .add_service(StreamSessionServiceServer::new(host))
            .serve_with_incoming(incoming),
    );
    let result: TestResult = async {
        let client = HttpStream::with_ca_certificate(
            &endpoint,
            "fixture-token",
            MAX_COMMAND_BYTES,
            Some(pem.as_bytes()),
        )?;
        let mut records = client.follow(StreamPath::new("events")?, 1).await?;
        let mut children = client
            .children(ChildrenRequest {
                parent: Some(StreamPath::new("agents")?),
                limit: 1,
            })
            .await?;
        append(&provider, "events").await?;
        append(&provider, "agents/one").await?;
        assert_eq!(
            tokio::time::timeout(Duration::from_secs(1), records.next())
                .await?
                .ok_or("missing TLS record")??
                .sequence,
            1
        );
        assert_eq!(
            tokio::time::timeout(Duration::from_secs(1), children.next())
                .await?
                .ok_or("missing TLS child")??
                .path
                .as_str(),
            "agents/one"
        );
        assert_eq!(sessions.load(Ordering::Relaxed), 1);
        Ok(())
    }
    .await;
    server.abort();
    result
}
