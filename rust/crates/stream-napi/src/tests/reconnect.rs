use super::*;
use std::sync::atomic::AtomicUsize;
use tonic::{Response, Streaming};
use wire::stream_session_service_server::{StreamSessionService, StreamSessionServiceServer};

struct DisconnectingHost {
    service: acyclic_stream::session::Service<acyclic_stream::MemoryStream>,
    calls: Arc<AtomicUsize>,
    disconnect: Arc<tokio::sync::Notify>,
}
#[tonic::async_trait]
impl StreamSessionService for DisconnectingHost {
    type SessionStream =
        futures::stream::BoxStream<'static, std::result::Result<wire::SessionFrame, Status>>;
    async fn session(
        &self,
        request: Request<Streaming<wire::SessionFrame>>,
    ) -> std::result::Result<Response<Self::SessionStream>, Status> {
        if self.calls.fetch_add(1, Ordering::SeqCst) > 0 {
            return Err(Status::unavailable("reconnect rejected"));
        }
        let output = self.service.session(request).await?.into_inner();
        let disconnect = Arc::clone(&self.disconnect);
        let failure = futures::stream::once(async move {
            disconnect.notified().await;
            Err(Status::unavailable("physical session disconnected"))
        });
        Ok(Response::new(
            futures::stream::select(output, failure).boxed(),
        ))
    }
}

#[tokio::test]
async fn failed_session_reconnect_surfaces_to_native_children() {
    let result = tokio::time::timeout(Duration::from_secs(3), async {
        let identity = rcgen::generate_simple_self_signed(["localhost".to_owned()])?;
        let certificate = identity.cert.pem();
        let key = identity.signing_key.serialize_pem();
        let provider = Arc::new(acyclic_stream::MemoryStream::default());
        provider
            .append(AppendRequest {
                path: StreamPath::new("agents/first")?,
                records: vec![vec![1].into()],
                if_tail: None,
                idempotency_key: None,
            })
            .await?;
        let calls = Arc::new(AtomicUsize::new(0));
        let disconnect = Arc::new(tokio::sync::Notify::new());
        let host = DisconnectingHost {
            service: acyclic_stream::session::Service::new(
                provider,
                "native".into(),
                Default::default(),
            ),
            calls: Arc::clone(&calls),
            disconnect: Arc::clone(&disconnect),
        };
        let listener = TcpListener::bind("127.0.0.1:0").await?;
        let endpoint = format!("https://localhost:{}", listener.local_addr()?.port());
        let incoming = futures::stream::unfold(listener, |listener| async move {
            listener
                .accept()
                .await
                .ok()
                .map(|(socket, _)| (Ok::<_, std::io::Error>(socket), listener))
        });
        let server = tokio::spawn(
            Server::builder()
                .tls_config(ServerTlsConfig::new().identity(Identity::from_pem(&certificate, key)))?
                .add_service(StreamSessionServiceServer::new(host))
                .serve_with_incoming(incoming),
        );
        // Abort even when an assertion fails; no fixture server outlives the test.
        struct Abort(tokio::task::JoinHandle<std::result::Result<(), tonic::transport::Error>>);
        impl Drop for Abort {
            fn drop(&mut self) {
                self.0.abort();
            }
        }
        let _server = Abort(server);
        let client =
            grpc::Client::connect_eager_with_ca_certificate(&endpoint, "fixture", &certificate)
                .await?;
        let children = operations::open_children(
            &Arc::new(client),
            Buffer::from(
                wire::ChildrenRequest {
                    parent: Some("agents".into()),
                    limit: 1,
                }
                .encode_to_vec(),
            ),
            None,
        )
        .await
        .map_err(|error| error.message)?;
        let first = children.next_result().await?;
        assert!(first.value.is_some() && first.error.is_none());
        disconnect.notify_one();
        let failure = children.next_result().await?;
        assert!(failure.value.is_none());
        assert_eq!(
            failure
                .error
                .ok_or("terminal failure became clean EOF")?
                .code,
            "unavailable"
        );
        assert_eq!(
            calls.load(Ordering::SeqCst),
            4,
            "initial RPC plus three bounded reconnect attempts"
        );
        let end = children.next_result().await?;
        assert!(end.value.is_none() && end.error.is_none());
        Ok::<_, Box<dyn std::error::Error + Send + Sync>>(())
    })
    .await;
    assert!(
        matches!(result, Ok(Ok(()))),
        "native reconnect regression: {result:?}"
    );
}

proptest::proptest! {
    #![proptest_config(proptest::test_runner::Config::with_cases(64))]
    #[test]
    fn error_after_any_buffered_prefix_is_observable(
        prefix in proptest::collection::vec(proptest::collection::vec(proptest::prelude::any::<u8>(), 0..16), 0..8),
    ) {
        let frames: FollowFrames = futures::stream::iter(prefix.iter().cloned().map(Ok)
            .chain([Err(StreamError::Unavailable)])
            .collect::<Vec<_>>()).boxed();
        let follow = NativeStreamFollow {
            state: FollowState {
                records: Arc::new(Mutex::new(Some(frames))),
                token: CancellationToken::new(),
                closed: Arc::new(AtomicBool::new(false)),
            },
        };
        futures::executor::block_on(async {
            for expected in prefix {
                let next = follow.next_result().await.unwrap();
                proptest::prop_assert!(next.error.is_none());
                let value = next.value.unwrap();
                proptest::prop_assert_eq!(value.as_ref(), expected.as_slice());
            }
            let error = follow.next_result().await.unwrap();
            proptest::prop_assert!(error.value.is_none());
            proptest::prop_assert_eq!(error.error.expect("terminal failure was swallowed").code, "unavailable");
            let end = follow.next_result().await.unwrap();
            proptest::prop_assert!(end.value.is_none() && end.error.is_none());
            Ok(())
        })?;
    }
}
