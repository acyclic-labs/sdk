use super::*;
mod reconnect;
use acyclic_stream::{
    AppendOutcome, AppendRequest, ChildStream, ChildrenPage, ChildrenPageRequest, ChildrenRequest,
    CommitId, CommitOutcome, CommitRequest, CommittedEnvelope, ForkRequest, IdempotencyKey,
    IdempotencyObservation, ReadRequest, RecordStream, StreamBounds, StreamProvider,
};
use async_trait::async_trait;
use futures::StreamExt;
use std::sync::atomic::{AtomicBool, Ordering};
use std::task::Poll;
use std::time::Duration;
use tokio::net::TcpListener;
use tonic::transport::{Identity, Server, ServerTlsConfig};
use tonic::{Request, Status};

#[test]
fn platform_verifier_failure_is_transport_and_retains_its_cause() {
    let metadata = connect_error(&grpc::ConnectError::PlatformVerifier(
        rustls::Error::General("native trust initialization failed".to_owned()),
    ));
    assert_eq!(metadata.code, "transport");
    assert!(
        metadata
            .message
            .contains("native trust initialization failed")
    );
}

struct DropSentinel(Arc<AtomicBool>);

impl Drop for DropSentinel {
    fn drop(&mut self) {
        self.0.store(true, Ordering::SeqCst);
    }
}

fn cancellation_error(
    result: std::result::Result<(), NativeStreamErrorMetadata>,
) -> NativeStreamErrorMetadata {
    match result {
        Err(error) => error,
        Ok(()) => panic!("cancellation helper unexpectedly completed the operation"),
    }
}

#[tokio::test]
async fn cancellation_is_checked_before_polling_operation() {
    let cancellation = CancellationToken::new();
    cancellation.cancel();
    let polled = Arc::new(AtomicBool::new(false));
    let operation_polled = Arc::clone(&polled);
    let operation = std::future::poll_fn(move |_| {
        operation_polled.store(true, Ordering::SeqCst);
        Poll::Ready(Ok::<(), NativeStreamErrorMetadata>(()))
    });

    let error = cancellation_error(run_with_cancellation(operation, Some(cancellation)).await);
    assert_eq!(error.code, "cancelled");
    assert!(!polled.load(Ordering::SeqCst));
}

#[tokio::test]
async fn pending_operation_is_interrupted_by_cancellation() {
    let cancellation = CancellationToken::new();
    let polled = Arc::new(AtomicBool::new(false));
    let operation_polled = Arc::clone(&polled);
    let operation = std::future::poll_fn(move |_| {
        operation_polled.store(true, Ordering::SeqCst);
        Poll::Pending::<std::result::Result<(), NativeStreamErrorMetadata>>
    });
    let pending = tokio::spawn(run_with_cancellation(operation, Some(cancellation.clone())));

    while !polled.load(Ordering::SeqCst) {
        tokio::task::yield_now().await;
    }
    cancellation.cancel();

    let error = cancellation_error(pending.await.expect("cancellation task panicked"));
    assert_eq!(error.code, "cancelled");
}

#[tokio::test]
async fn ready_operation_finishes_if_cancellation_arrives_during_its_poll() {
    let cancellation = CancellationToken::new();
    let first_poll = Arc::new(AtomicBool::new(false));
    let operation_first_poll = Arc::clone(&first_poll);
    let operation_cancellation = cancellation.clone();
    let operation = std::future::poll_fn(move |_| {
        if !operation_first_poll.swap(true, Ordering::SeqCst) {
            // The cancellation branch was already observed as pending
            // for this select poll. Cancellation cannot preempt an
            // operation that returns Ready from its current poll.
            operation_cancellation.cancel();
        }
        Poll::Ready(Ok::<(), NativeStreamErrorMetadata>(()))
    });

    let result = run_with_cancellation(operation, Some(cancellation)).await;
    assert!(result.is_ok());
    assert!(first_poll.load(Ordering::SeqCst));
}

#[tokio::test]
async fn concurrent_follow_reads_are_serialized_instead_of_false_end() {
    let polled = Arc::new(AtomicBool::new(false));
    let dropped = Arc::new(AtomicBool::new(false));
    let stream_polled = Arc::clone(&polled);
    let stream_dropped = Arc::clone(&dropped);
    let sentinel = DropSentinel(stream_dropped);
    let records: acyclic_stream::RecordStream = Box::pin(futures::stream::poll_fn(move |_| {
        let _sentinel = &sentinel;
        stream_polled.store(true, Ordering::SeqCst);
        Poll::Pending
    }));
    let follow = Arc::new(NativeStreamFollow {
        state: FollowState {
            records: Arc::new(Mutex::new(Some(record_frames(records)))),
            token: CancellationToken::new(),
            closed: Arc::new(AtomicBool::new(false)),
        },
    });

    let first = tokio::spawn({
        let follow = Arc::clone(&follow);
        async move { follow.next_result().await }
    });
    while !polled.load(Ordering::SeqCst) {
        tokio::task::yield_now().await;
    }
    let mut second = tokio::spawn({
        let follow = Arc::clone(&follow);
        async move { follow.next_result().await }
    });
    assert!(
        tokio::time::timeout(Duration::from_millis(50), &mut second)
            .await
            .is_err()
    );

    follow.close().await;
    assert!(dropped.load(Ordering::SeqCst));
    let first = first
        .await
        .expect("first follow task panicked")
        .expect("first read failed");
    assert!(first.value.is_none());
    assert!(first.error.is_none());
    let second = second
        .await
        .expect("second follow task panicked")
        .expect("second read failed");
    assert!(second.value.is_none());
    assert!(second.error.is_none());
}

#[tokio::test]
async fn terminal_unavailable_is_reported_before_end() {
    let records = futures::stream::once(async { Err(StreamError::Unavailable) }).boxed();
    let follow = NativeStreamFollow {
        state: FollowState {
            records: Arc::new(Mutex::new(Some(record_frames(records)))),
            token: CancellationToken::new(),
            closed: Arc::new(AtomicBool::new(false)),
        },
    };
    let result = follow.next_result().await.expect("follow read rejected");
    assert!(result.value.is_none());
    assert_eq!(
        result.error.expect("terminal error swallowed").code,
        "unavailable"
    );
    let end = follow.next_result().await.expect("follow end rejected");
    assert!(end.value.is_none() && end.error.is_none());
}

#[tokio::test]
async fn close_during_ready_poll_drops_cursor_without_reinsertion() {
    use std::sync::Mutex as StdMutex;

    let follow_state = FollowState {
        records: Arc::new(Mutex::new(None)),
        token: CancellationToken::new(),
        closed: Arc::new(AtomicBool::new(false)),
    };
    let follow = NativeStreamFollow {
        state: follow_state.clone(),
    };
    let close_task = Arc::new(StdMutex::new(None));
    let close_task_slot = Arc::clone(&close_task);
    let record = acyclic_stream::Record {
        sequence: 0,
        value: vec![1_u8, 2].into(),
        commit_id: acyclic_stream::CommitId::default(),
        committed_at_micros: 0,
    };
    let mut record = Some(record);
    let records: acyclic_stream::RecordStream = Box::pin(futures::stream::poll_fn(move |_| {
        let close_state = follow_state.clone();
        close_state.closed.store(true, Ordering::Release);
        close_state.token.cancel();
        let close_task = tokio::spawn(async move {
            NativeStreamFollow { state: close_state }.close().await;
        });
        *close_task_slot.lock().expect("close task slot poisoned") = Some(close_task);
        Poll::Ready(record.take().map(Ok))
    }));
    *follow.state.records.lock().await = Some(record_frames(records));

    let first = follow.next_result().await.expect("first read failed");
    assert!(first.value.is_none());
    assert!(first.error.is_none());
    let close_task = close_task
        .lock()
        .expect("close task slot poisoned")
        .take()
        .expect("ready poll did not start close");
    close_task.await.expect("close task panicked");
    let second = follow.next_result().await.expect("second read failed");
    assert!(second.value.is_none());
    assert!(second.error.is_none());
}

struct TcpDropProbe {
    operation_claims: acyclic_stream::OperationClaims,
    started: Arc<tokio::sync::Notify>,
    dropped: Arc<tokio::sync::Notify>,
}
struct TcpDropped(Arc<tokio::sync::Notify>);
impl Drop for TcpDropped {
    fn drop(&mut self) {
        self.0.notify_one();
    }
}

#[async_trait]
impl StreamProvider for TcpDropProbe {
    fn operation_claims(&self) -> &acyclic_stream::OperationClaims {
        &self.operation_claims
    }

    async fn inspect_idempotency(
        &self,
        _idempotency_key: IdempotencyKey,
    ) -> std::result::Result<Option<IdempotencyObservation>, StreamError> {
        Err(StreamError::Unsupported)
    }

    async fn tail(&self, _path: StreamPath) -> std::result::Result<u64, StreamError> {
        Err(StreamError::Unsupported)
    }

    async fn bounds(&self, _path: StreamPath) -> std::result::Result<StreamBounds, StreamError> {
        Err(StreamError::Unsupported)
    }

    async fn append(
        &self,
        _request: AppendRequest,
    ) -> std::result::Result<AppendOutcome, StreamError> {
        Err(StreamError::Unsupported)
    }

    async fn fork(
        &self,
        _request: ForkRequest,
    ) -> std::result::Result<acyclic_stream::ForkReceipt, StreamError> {
        Err(StreamError::Unsupported)
    }

    async fn read(&self, _request: ReadRequest) -> std::result::Result<RecordStream, StreamError> {
        Err(StreamError::Unsupported)
    }

    async fn follow(
        &self,
        _path: StreamPath,
        _from: u64,
    ) -> std::result::Result<RecordStream, StreamError> {
        self.started.notify_one();
        let dropped = Arc::clone(&self.dropped);
        let sentinel = TcpDropped(dropped);
        Ok(futures::stream::poll_fn(move |_| {
            let _sentinel = &sentinel;
            Poll::Pending
        })
        .boxed())
    }

    async fn children(
        &self,
        _request: ChildrenRequest,
    ) -> std::result::Result<ChildStream, StreamError> {
        let sentinel = TcpDropped(Arc::clone(&self.dropped));
        Ok(futures::stream::once(async {
            Ok(acyclic_stream::Child {
                path: StreamPath::new("agents/new")?,
            })
        })
        .chain(futures::stream::poll_fn(move |_| {
            let _sentinel = &sentinel;
            Poll::Pending
        }))
        .boxed())
    }

    async fn children_page(
        &self,
        _request: ChildrenPageRequest,
    ) -> std::result::Result<ChildrenPage, StreamError> {
        Err(StreamError::Unsupported)
    }

    async fn commit(
        &self,
        _request: CommitRequest,
    ) -> std::result::Result<CommitOutcome, StreamError> {
        Err(StreamError::Unsupported)
    }

    async fn read_commit(
        &self,
        _commit_id: CommitId,
    ) -> std::result::Result<CommittedEnvelope, StreamError> {
        Err(StreamError::Unsupported)
    }
}

#[allow(
    clippy::too_many_lines,
    reason = "the real TCP lifecycle test keeps connection, follow, close, and server-drop assertions together"
)]
#[tokio::test]
async fn real_tcp_follow_close_releases_the_server_stream() {
    let result = async {
        let identity = rcgen::generate_simple_self_signed(["localhost".to_owned()])?;
        let certificate_pem = identity.cert.pem();
        let private_key_pem = identity.signing_key.serialize_pem();
        let client_ca = certificate_pem.as_bytes().to_vec();
        let listener = TcpListener::bind("127.0.0.1:0").await?;
        let address = listener.local_addr()?;
        let (shutdown_tx, shutdown_rx) = tokio::sync::oneshot::channel();
        let started = Arc::new(tokio::sync::Notify::new());
        let dropped = Arc::new(tokio::sync::Notify::new());
        let provider = Arc::new(TcpDropProbe {
            operation_claims: Default::default(),
            started: Arc::clone(&started),
            dropped: Arc::clone(&dropped),
        });
        let authorize = |request: Request<()>| {
            if request
                .metadata()
                .get("authorization")
                .and_then(|value| value.to_str().ok())
                != Some("Bearer exact-token")
            {
                return Err(Status::unauthenticated("missing exact bearer credential"));
            }
            Ok(request)
        };
        let sessions =
            wire::stream_session_service_server::StreamSessionServiceServer::with_interceptor(
                acyclic_stream::session::Service::new(
                    Arc::clone(&provider),
                    "native".into(),
                    acyclic_stream::session::Limits::default(),
                ),
                authorize,
            );
        let service = wire::stream_service_server::StreamServiceServer::with_interceptor(
            acyclic_stream::grpc::Service::new(provider),
            authorize,
        );
        let incoming = futures::stream::unfold(listener, |listener| async move {
            listener
                .accept()
                .await
                .ok()
                .map(|(socket, _)| (Ok::<_, std::io::Error>(socket), listener))
        });
        let server = tokio::spawn(async move {
            Server::builder()
                .tls_config(
                    ServerTlsConfig::new()
                        .identity(Identity::from_pem(certificate_pem, private_key_pem)),
                )?
                .add_service(service)
                .add_service(sessions)
                .serve_with_incoming_shutdown(incoming, async {
                    let _ = shutdown_rx.await;
                })
                .await
        });

        let endpoint = format!("https://localhost:{}", address.port());
        let connected = NativeStreamClient::connect_with_ca_result(
            endpoint,
            "exact-token".to_owned(),
            Buffer::from(client_ca),
            None,
        )
        .await?;
        assert!(
            connected.error.is_none(),
            "connect returned an error envelope"
        );
        let client = connected.client.expect("connect result omitted client");
        let request = wire::FollowRequest {
            path: "accounts/events".to_owned(),
            from: 0,
        };
        let follow = Arc::new(
            open_follow(
                &client.provider,
                Buffer::from(request.encode_to_vec()),
                None,
            )
            .await
            .map_err(|error| error.message)?,
        );
        let pending = tokio::spawn({
            let follow = Arc::clone(&follow);
            async move { follow.next_result().await }
        });
        tokio::time::timeout(Duration::from_secs(1), started.notified()).await?;

        follow.close().await;
        let closed = tokio::time::timeout(Duration::from_secs(1), pending)
            .await??
            .map_err(|_| "pending next_result rejected")?;
        assert!(closed.value.is_none());
        assert!(closed.error.is_none());
        let closed_again = follow.next_result().await?;
        assert!(closed_again.value.is_none());
        assert!(closed_again.error.is_none());

        tokio::time::timeout(Duration::from_secs(1), dropped.notified())
            .await
            .map_err(|_| "server follow stream was not dropped after close")?;

        let children = open_children(
            &client.provider,
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
        let value = children.next_result().await?;
        assert!(value.error.is_none());
        let value = value.value.ok_or("child response missing")?;
        let value = wire::ChildrenResponse::decode(value.as_ref())?;
        assert_eq!(value.child.ok_or("child missing")?.path, "agents/new");
        children.close().await;
        tokio::time::timeout(Duration::from_secs(1), dropped.notified()).await?;

        drop(children);
        drop(follow);
        drop(client);

        let _ = shutdown_tx.send(());
        server.await??;
        Ok::<(), Box<dyn std::error::Error + Send + Sync>>(())
    }
    .await;
    assert!(
        result.is_ok(),
        "real TCP follow lifecycle failed: {result:?}"
    );
}
