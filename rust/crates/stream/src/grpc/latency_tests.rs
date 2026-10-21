use super::tests::TestResult;
use super::*;

#[tokio::test]
async fn append_follow_latency() -> TestResult {
    let provider = crate::MemoryStream::default();
    measure(&provider, &provider, "memory").await?;
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
    let endpoint = format!("http://{}", listener.local_addr()?);
    let incoming = stream::try_unfold(listener, |listener| async {
        let (socket, _) = listener.accept().await?;
        socket.set_nodelay(true)?;
        Ok::<_, std::io::Error>(Some((socket, listener)))
    });
    let server = tokio::spawn(async move {
        let provider = Arc::new(crate::MemoryStream::default());
        tonic::transport::Server::builder()
            .add_service(wire::stream_service_server::StreamServiceServer::new(
                Service::new(Arc::clone(&provider)),
            ))
            .add_service(
                wire::stream_session_service_server::StreamSessionServiceServer::new(
                    crate::session::Service::new(
                        provider,
                        "latency-node".into(),
                        crate::session::Limits::default(),
                    ),
                ),
            )
            .serve_with_incoming(incoming)
            .await
    });
    let channel = Endpoint::from_shared(endpoint)?
        .tcp_nodelay(true)
        .connect()
        .await?;
    let transport = Client::from_channel(channel, authorization("latency-fixture")?);
    let result = measure(&transport, &transport, "grpc_tcp").await;
    server.abort();
    result
}

async fn measure<P: StreamProvider>(writer: &P, reader: &P, label: &str) -> TestResult {
    let path = StreamPath::new("latency/events")?;
    writer
        .append(AppendRequest {
            path: path.clone(),
            records: vec![Bytes::from_static(b"seed").into()],
            if_tail: None,
            idempotency_key: None,
        })
        .await?;
    let mut follow = reader.follow(path.clone(), 1).await?;
    let mut samples = Vec::new();
    for sequence in 1..=144 {
        let started = std::time::Instant::now();
        let (_, record, latency) = crate::test_latency::sample(
            async {
                Ok::<_, Box<dyn std::error::Error + Send + Sync>>(
                    writer
                        .append(AppendRequest {
                            path: path.clone(),
                            records: vec![Bytes::from_static(b"delta").into()],
                            if_tail: None,
                            idempotency_key: None,
                        })
                        .await?,
                )
            },
            async {
                // Latency is reported, not bounded: a loaded host only slows it.
                Ok(follow.next().await.ok_or("follow ended")??)
            },
            || started.elapsed().as_nanos(),
        )
        .await?;
        assert_eq!(record.sequence, sequence);
        assert_eq!(record.value.as_slice(), b"delta");
        if sequence > 16 {
            samples.push(latency);
        }
    }
    samples.sort_unstable();
    let p50 = samples.get(63).ok_or("missing p50")?;
    let p99 = samples.get(126).ok_or("missing p99")?;
    println!(
        "append_to_follower {label} n={} p50_ns={p50} p99_ns={p99}",
        samples.len()
    );
    Ok(())
}
