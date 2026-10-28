use super::*;
use futures::TryFutureExt as _;

struct RotatingHost {
    service: crate::session::Service<MemoryStream>,
    calls: std::sync::Arc<AtomicUsize>,
    changed: std::sync::Arc<tokio::sync::Notify>,
    controls: tokio::sync::broadcast::Sender<Result<wire::SessionFrame, Status>>,
}
#[tonic::async_trait]
impl StreamSessionService for RotatingHost {
    type SessionStream = BoxStream<'static, Result<wire::SessionFrame, Status>>;
    async fn session(
        &self,
        request: Request<Streaming<wire::SessionFrame>>,
    ) -> Result<Reply<Self::SessionStream>, Status> {
        let call = self.calls.fetch_add(1, Ordering::SeqCst) + 1;
        self.changed.notify_one();
        if request
            .metadata()
            .get("authorization")
            .and_then(|value| value.to_str().ok())
            != Some(format!("Bearer rotated-{call}").as_str())
        {
            return Err(Status::unauthenticated("expired credential"));
        }
        if call == 1 {
            return Err(Status::unavailable("retry this physical RPC"));
        }
        let output = self.service.session(request).await?.into_inner();
        let controls = stream::unfold(self.controls.subscribe(), |mut receiver| async move {
            receiver.recv().await.ok().map(|frame| (frame, receiver))
        });
        Ok(Reply::new(stream::select(output, controls).boxed()))
    }
}

struct RotatingFixture {
    endpoint: String,
    controls: tokio::sync::broadcast::Sender<Result<wire::SessionFrame, Status>>,
    server: tokio::task::JoinHandle<Result<(), tonic::transport::Error>>,
}
impl Drop for RotatingFixture {
    fn drop(&mut self) {
        self.server.abort();
    }
}
impl RotatingFixture {
    async fn open(
        provider: Arc<MemoryStream>,
        node: &str,
        calls: std::sync::Arc<AtomicUsize>,
        changed: std::sync::Arc<tokio::sync::Notify>,
        certificate: &str,
        key: &str,
    ) -> Result<Self, Box<dyn std::error::Error + Send + Sync>> {
        use tonic::transport::{Identity, Server, ServerTlsConfig};
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
        let endpoint = format!("https://localhost:{}", listener.local_addr()?.port());
        let incoming = stream::unfold(listener, |listener| async move {
            listener
                .accept()
                .await
                .ok()
                .map(|(socket, _)| (Ok::<_, std::io::Error>(socket), listener))
        });
        let (controls, _) = tokio::sync::broadcast::channel(8);
        let host = RotatingHost {
            service: crate::session::Service::new(
                provider,
                crate::session::testing::authority(),
                node.into(),
                Limits::default(),
            ),
            calls,
            changed,
            controls: controls.clone(),
        };
        let server = tokio::spawn(
            Server::builder()
                .tls_config(ServerTlsConfig::new().identity(Identity::from_pem(certificate, key)))?
                .add_service(StreamSessionServiceServer::new(host))
                .serve_with_incoming(incoming),
        );
        Ok(Self {
            endpoint,
            controls,
            server,
        })
    }
}

#[tokio::test]
async fn session_credentials_renew_on_handshake_retry_reconnect_and_move() -> TestResult {
    tokio::time::timeout(Duration::from_secs(4), async {
        let identity = rcgen::generate_simple_self_signed(["localhost".to_owned()])?;
        let certificate = identity.cert.pem();
        let key = identity.signing_key.serialize_pem();
        let provider = Arc::new(MemoryStream::default());
        append(&provider, "events", 1).await?;
        let calls = std::sync::Arc::new(AtomicUsize::new(0));
        let changed = std::sync::Arc::new(tokio::sync::Notify::new());
        let old = RotatingFixture::open(
            Arc::clone(&provider),
            "old",
            calls.clone(),
            changed.clone(),
            &certificate,
            &key,
        )
        .await?;
        let new = RotatingFixture::open(
            Arc::clone(&provider),
            "new",
            calls.clone(),
            changed.clone(),
            &certificate,
            &key,
        )
        .await?;
        let resolutions = std::sync::Arc::new(AtomicUsize::new(0));
        let client = rotating_client(&old.endpoint, &certificate, resolutions.clone()).await?;
        client
            .session_client()?
            .register_credential(0, crate::session::testing::credential())?;
        let mut records = client
            .follow(crate::FollowRequest::new(StreamPath::new("events")?, 0))
            .map_ok(crate::records_only)
            .await?;
        assert_eq!(next(&mut records).await?.sequence, 0);
        assert_eq!(calls.load(Ordering::SeqCst), 2);
        old.controls.send(Err(Status::unavailable("disconnect")))?;
        loop {
            let notified = changed.notified();
            if calls.load(Ordering::SeqCst) == 3 {
                break;
            }
            notified.await;
        }
        append(&provider, "events", 1).await?;
        assert_eq!(next(&mut records).await?.sequence, 1);
        assert_eq!(calls.load(Ordering::SeqCst), 3);
        old.controls.send(Ok(frame(
            1,
            2,
            Body::Moved(wire::SessionMoved {
                node: Some(wire::SessionNode {
                    node_id: "new".into(),
                    endpoint: new.endpoint.clone(),
                    placement_epoch: 1,
                }),
                resume: None,
            }),
        )))?;
        loop {
            let notified = changed.notified();
            if calls.load(Ordering::SeqCst) == 4 {
                break;
            }
            notified.await;
        }
        append(&provider, "events", 1).await?;
        assert_eq!(next(&mut records).await?.sequence, 2);
        assert_eq!(resolutions.load(Ordering::SeqCst), 4);
        let denied = client.with_credential_provider(grpc::CredentialProvider::new(|| {
            future::ready(Err(grpc::CredentialError::Invalid))
        }));
        assert!(matches!(
            denied
                .follow(crate::FollowRequest::new(StreamPath::new("events")?, 0))
                .map_ok(crate::records_only)
                .await,
            Err(StreamError::AccessDenied)
        ));
        assert_eq!(
            calls.load(Ordering::SeqCst),
            4,
            "failed renewal must not send an RPC"
        );
        Ok::<_, Box<dyn std::error::Error + Send + Sync>>(())
    })
    .await??;
    Ok(())
}

async fn rotating_client(
    endpoint: &str,
    certificate: &str,
    resolutions: std::sync::Arc<AtomicUsize>,
) -> Result<grpc::Client, Box<dyn std::error::Error + Send + Sync>> {
    let count = resolutions;
    let client = grpc::Client::connect_eager_with_credential(
        endpoint,
        grpc::CredentialProvider::new(move || {
            let index = count.fetch_add(1, Ordering::SeqCst) + 1;
            future::ready(Ok(format!("rotated-{index}")))
        }),
        Some(certificate.as_bytes()),
    )
    .await?;
    Ok(client)
}

#[tokio::test]
async fn queued_credentials_stream_sparse_slots_and_preserve_revocation() -> TestResult {
    tokio::time::timeout(Duration::from_secs(8), async {
        let provider = Arc::new(MemoryStream::default());
        append(&provider, "events", 1).await?;
        let fixture = Fixture::open(provider.clone(), "node").await?;
        let client = Client::with_connector(
            fixture._channel.clone(),
            Some("Bearer fixture".parse()?),
            None,
            Arc::new(|_| Box::pin(future::ready(Err(StreamError::Unavailable)))),
        );
        let credential = crate::session::testing::credential();
        let count = (crate::MAX_COMMAND_BYTES / credential.len()).max(crate::MAX_ITEMS) + 1;
        // No yield: all updates queue before the pool runs, and their aggregate
        // exceeds the former Hello/slot-byte limit. Slot IDs remain sparse.
        for slot in 0..count {
            client.register_credential(u32::try_from(slot)?, credential.clone())?;
        }
        client.register_credential(u32::MAX, credential)?;
        let mut frames = client
            .subscribe(wire::SessionSubscription {
                credential: u32::MAX,
                resume: Some(cursor(0)),
                kind: Some(Kind::Follow(wire::SessionFollow {
                    resource: crate::wire_codec::session::stream_resource("events"),
                    delivery: wire::SessionDelivery::All as i32,
                    ..Default::default()
                })),
                ..Default::default()
            })
            .await?;
        loop {
            let value = frames.next().await.ok_or("missing record")??;
            if let Some(Body::Data(data)) = value.body {
                assert!(matches!(data.item, Some(Item::Record(record)) if record.sequence == 0));
                break;
            }
        }
        let mut records = client
            .follow_records(crate::FollowRequest::new(StreamPath::new("events")?, 0))
            .await?;
        assert_eq!(next(&mut records).await?.sequence, 0);
        client.register_credential(u32::MAX, Bytes::new())?;
        let error = loop {
            match frames.next().await {
                Some(Ok(_)) => (),
                Some(Err(error)) => break error,
                None => return Err("revocation must refuse its channel".into()),
            }
        };
        assert_eq!(error, StreamError::AccessDenied);
        // The same physical session and independent slot remain usable.
        append(&provider, "events", 1).await?;
        assert_eq!(next(&mut records).await?.sequence, 1);
        assert_eq!(fixture.counts.sessions.load(Ordering::Relaxed), 1);
        Ok::<_, Box<dyn std::error::Error + Send + Sync>>(())
    })
    .await?
}
