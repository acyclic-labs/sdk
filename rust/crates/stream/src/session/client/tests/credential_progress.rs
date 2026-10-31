use super::*;
use acyclic_auth::credential::{
    CredentialAction, CredentialBinding, CredentialCache, CredentialCacheError, CredentialError,
    CredentialPending, CredentialProvider, CredentialRequest, CredentialResolution,
    CredentialSnapshot, CredentialSource, CredentialValidity, CredentialWatch, ResolvedCredential,
};

struct OwnerPause {
    reject: bool,
    entered: oneshot::Sender<()>,
    release: oneshot::Receiver<()>,
}
type Pause = std::sync::Arc<std::sync::Mutex<Option<OwnerPause>>>;

struct Source {
    provider: CredentialProvider,
    pause: Pause,
}
impl CredentialSource for Source {
    fn resolve(&self) -> CredentialResolution<'_> {
        Box::pin(async {
            ResolvedCredential::from_custodian(OwnerRequest {
                lease: self.provider.resolve().await?,
                pause: self.pause.clone(),
            })
            .await
        })
    }
    fn watch(&self) -> CredentialAction<Option<CredentialWatch>> {
        self.provider.watch()
    }
}

struct OwnerRequest {
    lease: ResolvedCredential,
    pause: Pause,
}
impl OwnerRequest {
    fn wait(&self, reject: bool) -> Option<CredentialPending<()>> {
        let mut pause = self.pause.lock().unwrap();
        if pause.as_ref()?.reject != reject {
            return None;
        }
        let pause = pause.take().unwrap();
        Some(Box::pin(async move {
            let _ = pause.entered.send(());
            pause
                .release
                .await
                .map_err(|_| CredentialError::Provider(Box::new(CredentialCacheError::Unavailable)))
        }))
    }
}
impl CredentialRequest for OwnerRequest {
    fn token(&self) -> &str {
        self.lease.as_str()
    }
    fn binding(&self) -> Option<CredentialBinding> {
        self.lease.binding()
    }
    fn check(&self) -> CredentialAction<()> {
        let Some(wait) = self.wait(false) else {
            return self.lease.check();
        };
        let lease = self.lease.clone();
        CredentialAction::Pending(Box::pin(async move {
            wait.await?;
            lease.check().await
        }))
    }
    fn reject(&self) -> CredentialAction<bool> {
        let Some(wait) = self.wait(true) else {
            return self.lease.reject();
        };
        let lease = self.lease.clone();
        CredentialAction::Pending(Box::pin(async move {
            wait.await?;
            lease.reject().await
        }))
    }
}

struct HeaderRefusal;
#[tonic::async_trait]
impl StreamSessionService for HeaderRefusal {
    type SessionStream = futures::stream::BoxStream<'static, Result<wire::SessionFrame, Status>>;
    async fn session(
        &self,
        _: Request<Streaming<wire::SessionFrame>>,
    ) -> Result<Reply<Self::SessionStream>, Status> {
        Err(Status::unauthenticated("synthetic remote refusal"))
    }
}
struct HeaderFixture {
    channel: tonic::transport::Channel,
    server: tokio::task::JoinHandle<Result<(), tonic::transport::Error>>,
}
impl HeaderFixture {
    async fn open() -> Result<Self, Box<dyn std::error::Error + Send + Sync>> {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
        let address = format!("http://{}", listener.local_addr()?);
        let incoming = tokio_stream::wrappers::TcpListenerStream::new(listener);
        let server = tokio::spawn(
            tonic::transport::Server::builder()
                .add_service(StreamSessionServiceServer::new(HeaderRefusal))
                .serve_with_incoming(incoming),
        );
        let channel = Endpoint::from_shared(address)?.connect().await?;
        Ok(Self { channel, server })
    }
}
impl Drop for HeaderFixture {
    fn drop(&mut self) {
        self.server.abort();
    }
}

#[tokio::test]
async fn source_change_cancels_pending_reply_admission_and_remote_rejection() -> TestResult {
    use futures::{FutureExt as _, future::Either};
    for stage in ["reply", "trailers", "headers"] {
        for completion in ["invalidate", "owner-loss", "release"] {
            let fixture = Fixture::open(Arc::new(MemoryStream::default()), "node").await?;
            let cache = CredentialCache::new(
                |_| async { CredentialSnapshot::new("fixture".into(), [1; 32].into(), 100) },
                || CredentialValidity {
                    now_unix_seconds: 1,
                    revision: Some([1; 32].into()),
                },
            );
            let pause = Pause::default();
            let provider = CredentialProvider::from_custodian(Source {
                provider: cache.clone().into(),
                pause: pause.clone(),
            });
            let (release, released) = oneshot::channel();
            let (entered, entered_owner) = oneshot::channel();
            let owner = OwnerPause {
                reject: stage != "reply",
                entered,
                release: released,
            };
            // Real gRPC headers carry UNAUTHENTICATED before any response body.
            let header_fixture = if stage == "headers" {
                Some(HeaderFixture::open().await?)
            } else {
                None
            };
            let channel = if let Some(header) = &header_fixture {
                header.channel.clone()
            } else {
                fixture._channel.clone()
            };
            let endpoint = crate::session::client::native::endpoint(channel, None, Some(provider));
            let (send, receive) = tokio::sync::mpsc::channel(1);
            send.send(frame(
                0,
                0,
                Body::Hello(wire::SessionHello {
                    version: 1,
                    client_id: bytes::Bytes::from_static(b"deferred-owner"),
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
            let operation = async {
                if stage == "headers" {
                    *pause.lock().unwrap() = Some(owner);
                    return endpoint.session(Box::pin(source)).await.map(|_| None);
                }
                let mut response = endpoint.session(Box::pin(source)).await?;
                assert!(matches!(
                    response
                        .next()
                        .await
                        .ok_or(StreamError::Unavailable)??
                        .into_wire()
                        .body,
                    Some(Body::Welcome(_))
                ));
                *pause.lock().unwrap() = Some(owner);
                fixture
                    .controls
                    .send(if stage == "reply" {
                        Ok(frame(
                            0,
                            0,
                            Body::InterestApplied(wire::SessionInterestApplied { revision: 7 }),
                        ))
                    } else {
                        Err(Status::unauthenticated("synthetic remote refusal"))
                    })
                    .map_err(|_| StreamError::Unavailable)?;
                response
                    .next()
                    .await
                    .ok_or(StreamError::Unavailable)?
                    .map(Some)
            };
            futures::pin_mut!(operation);
            match futures::future::select(operation.as_mut(), entered_owner).await {
                Either::Right((entered, _)) => entered?,
                Either::Left(_) => panic!("{stage}/{completion}: owner wait was bypassed"),
            }
            assert!(operation.as_mut().now_or_never().is_none());
            match completion {
                "invalidate" => {
                    cache.invalidate()?;
                    assert!(
                        matches!(
                            operation.now_or_never(),
                            Some(Err(StreamError::AccessDenied))
                        ),
                        "{stage}: source change must cancel the owner wait immediately"
                    );
                    assert!(release.is_closed(), "pending owner interest was retained");
                }
                "owner-loss" => {
                    drop(release);
                    assert!(matches!(
                        operation.now_or_never(),
                        Some(Err(StreamError::AccessDenied))
                    ));
                }
                _ => {
                    release.send(()).unwrap();
                    let result = operation.await;
                    if stage == "reply" {
                        assert!(
                            matches!(result?.map(Frame::into_wire), Some(wire::SessionFrame { body: Some(Body::InterestApplied(applied)), .. }) if applied.revision == 7)
                        );
                    } else {
                        assert!(matches!(result, Err(StreamError::AccessDenied)));
                    }
                }
            }
        }
    }
    Ok(())
}
