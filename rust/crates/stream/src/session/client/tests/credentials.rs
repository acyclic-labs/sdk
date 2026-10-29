use super::*;

#[tokio::test]
async fn native_session_refuses_outgoing_commands_after_lease_change_without_a_reply() -> TestResult
{
    use acyclic_auth::credential::{CredentialCache, CredentialSnapshot, CredentialValidity};
    for change in [
        "unchanged",
        "account",
        "expiry",
        "unavailable",
        "invalidate",
    ] {
        let fixture = Fixture::open(Arc::new(MemoryStream::default()), "node").await?;
        let validity = Arc::new(std::sync::Mutex::new(CredentialValidity {
            now_unix_seconds: 1,
            revision: Some([1; 32].into()),
        }));
        let observed = validity.clone();
        let cache = CredentialCache::new(
            |_| async { CredentialSnapshot::new("fixture".into(), [1; 32].into(), 100) },
            move || observed.lock().unwrap().clone(),
        );
        let endpoint = crate::session::client::native::endpoint(
            fixture._channel.clone(),
            None,
            Some(cache.clone().into()),
        );
        let (send, receive) = tokio::sync::mpsc::channel(1);
        send.send(frame(
            0,
            0,
            Body::Hello(wire::SessionHello {
                version: 1,
                client_id: bytes::Bytes::from_static(b"credential-owner"),
                destination_node: "node".into(),
                receive_bytes: crate::MAX_COMMAND_BYTES as u64,
                receive_frames: crate::MAX_ITEMS as u32,
                credentials: vec![],
            }),
        ))
        .await?;
        let source = stream::unfold(receive, |mut receive| async {
            receive.recv().await.map(|frame| (frame, receive))
        });
        let mut response = endpoint.session(Box::pin(source)).await?;
        assert!(matches!(
            response.next().await.ok_or("welcome producer ended")??.body,
            Some(Body::Welcome(_))
        ));
        for revision in [1, 2] {
            if revision == 2 {
                match change {
                    "account" => validity.lock().unwrap().revision = Some([2; 32].into()),
                    "expiry" => validity.lock().unwrap().now_unix_seconds = 100,
                    "unavailable" => validity.lock().unwrap().revision = None,
                    "invalidate" => cache.invalidate()?,
                    _ => {}
                }
            }
            let received = fixture.counts.received_bytes.load(Ordering::Relaxed);
            send.send(frame(
                0,
                0,
                Body::Interest(wire::SessionInterest {
                    revision,
                    upsert: vec![],
                    remove: vec![],
                }),
            ))
            .await?;
            let result = response
                .next()
                .await
                .ok_or("session result producer ended")?;
            if revision == 1 || change == "unchanged" {
                assert!(
                    matches!(result?.body, Some(Body::InterestApplied(applied)) if applied.revision == revision)
                );
                assert!(fixture.counts.received_bytes.load(Ordering::Relaxed) > received);
            } else {
                assert!(matches!(result, Err(StreamError::AccessDenied)));
                assert_eq!(
                    fixture.counts.received_bytes.load(Ordering::Relaxed),
                    received,
                    "refused command reached the server"
                );
            }
        }
        if change == "unchanged" {
            use futures::FutureExt as _;
            let mut waiting = Box::pin(response.next());
            assert!(waiting.as_mut().now_or_never().is_none());
            cache.invalidate()?;
            assert!(
                matches!(
                    waiting.now_or_never(),
                    Some(Some(Err(StreamError::AccessDenied)))
                ),
                "source notification must retire an idle session without another frame"
            );
        }
        *validity.lock().unwrap() = CredentialValidity {
            now_unix_seconds: 1,
            revision: Some([1; 32].into()),
        };
        // Local dispatch refusal never records an authenticated rejection.
        let current = cache.resolve().await?;
        cache.check(&current)?;
    }
    Ok(())
}

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
