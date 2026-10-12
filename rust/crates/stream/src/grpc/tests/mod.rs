use super::*;
use crate::MemoryStream;
use crate::OwnedBytes;
use rcgen::generate_simple_self_signed;
use tokio::{io::AsyncReadExt, net::TcpListener};
use tokio_stream::wrappers::TcpListenerStream;
use tonic::transport::Server;
use wire::stream_service_server::{StreamService, StreamServiceServer};

struct FiniteFollow {
    inner: MemoryStream,
    follows: AtomicUsize,
    tail_delay: std::time::Duration,
    denial: Option<usize>,
}

#[async_trait]
impl StreamProvider for FiniteFollow {
    async fn inspect_idempotency(
        &self,
        key: IdempotencyKey,
    ) -> Result<Option<IdempotencyObservation>, StreamError> {
        self.inner.inspect_idempotency(key).await
    }

    async fn tail(&self, path: StreamPath) -> Result<u64, StreamError> {
        tokio::time::sleep(self.tail_delay).await;
        self.inner.tail(path).await
    }

    async fn bounds(&self, path: StreamPath) -> Result<StreamBounds, StreamError> {
        tokio::time::sleep(self.tail_delay).await;
        self.inner.bounds(path).await
    }

    async fn append(&self, request: AppendRequest) -> Result<AppendOutcome, StreamError> {
        self.inner.append(request).await
    }

    async fn fork(&self, request: ForkRequest) -> Result<ForkReceipt, StreamError> {
        self.inner.fork(request).await
    }

    async fn read(&self, request: ReadRequest) -> Result<RecordStream, StreamError> {
        self.inner.read(request).await
    }

    async fn follow(&self, path: StreamPath, from: u64) -> Result<RecordStream, StreamError> {
        self.follows.fetch_add(1, Ordering::Relaxed);
        match self.denial {
            None => Ok(stream::empty().boxed()),
            Some(0) => Err(StreamError::AccessDenied),
            Some(count) => {
                let limit = u32::try_from(count).map_err(|_| StreamError::InvalidArgument)?;
                let records = self.inner.read(ReadRequest { path, from, limit }).await?;
                Ok(records
                    .chain(stream::once(async { Err(StreamError::AccessDenied) }))
                    .boxed())
            }
        }
    }

    async fn children(&self, request: ChildrenRequest) -> Result<ChildStream, StreamError> {
        self.inner.children(request).await
    }

    async fn commit(&self, request: CommitRequest) -> Result<CommitOutcome, StreamError> {
        self.inner.commit(request).await
    }

    async fn read_commit(&self, commit_id: CommitId) -> Result<CommittedEnvelope, StreamError> {
        self.inner.read_commit(commit_id).await
    }
}

fn provider_channel<S: StreamService + Clone>(service: S) -> Channel {
    Endpoint::from_static("http://fixture.invalid").connect_with_connector_lazy(tower::service_fn(
        move |_| {
            let service = service.clone();
            async move {
                let (client, server) = tokio::io::duplex(64 * 1024);
                tokio::spawn(async move {
                    let incoming = stream::once(async { Ok::<_, std::io::Error>(server) });
                    let _ = tonic::transport::Server::builder()
                        .add_service(StreamServiceServer::new(service))
                        .serve_with_incoming(incoming)
                        .await;
                });
                Ok::<_, std::io::Error>(hyper_util::rt::TokioIo::new(client))
            }
        },
    ))
}

#[derive(Clone)]
struct MixedFrames(
    Service<MemoryStream>,
    Option<&'static str>,
    Option<Arc<AckFixture>>,
);

#[async_trait]
impl StreamService for MixedFrames {
    type ReadStream = futures::stream::BoxStream<'static, Result<wire::ReadResponse, Status>>;
    type FollowStream = futures::stream::BoxStream<'static, Result<wire::ReadResponse, Status>>;
    type ChildrenStream =
        futures::stream::BoxStream<'static, Result<wire::ChildrenResponse, Status>>;

    async fn acknowledge_delivery(
        &self,
        request: Request<wire::AcknowledgeDeliveryRequest>,
    ) -> Result<Response<wire::AcknowledgeDeliveryResponse>, Status> {
        let Some(fixture) = &self.2 else {
            return self.0.acknowledge_delivery(request).await;
        };
        assert_eq!(request.get_ref().delivery_token.as_ref(), &[7; 32]);
        fixture.entered.fetch_add(1, Ordering::SeqCst);
        fixture.changed.notify_one();
        if fixture.fail {
            return Err(Status::unavailable("receipt service unavailable"));
        }
        let _guard = AckGuard(Arc::clone(fixture));
        tokio::time::sleep(std::time::Duration::from_secs(10)).await;
        Ok(Response::new(wire::AcknowledgeDeliveryResponse {}))
    }

    async fn inspect_idempotency(
        &self,
        request: Request<wire::InspectIdempotencyRequest>,
    ) -> Result<Response<wire::InspectIdempotencyResponse>, Status> {
        self.0.inspect_idempotency(request).await
    }
    async fn append(
        &self,
        request: Request<wire::AppendRequest>,
    ) -> Result<Response<wire::AppendResponse>, Status> {
        self.0.append(request).await
    }
    async fn tail(
        &self,
        request: Request<wire::TailRequest>,
    ) -> Result<Response<wire::TailResponse>, Status> {
        self.0.tail(request).await
    }
    async fn fork(
        &self,
        request: Request<wire::ForkRequest>,
    ) -> Result<Response<wire::ForkReceipt>, Status> {
        self.0.fork(request).await
    }
    async fn read(
        &self,
        request: Request<wire::ReadRequest>,
    ) -> Result<Response<Self::ReadStream>, Status> {
        if self.1 == Some("unavailable_read") {
            return Err(Status::unavailable("read on another endpoint"));
        }
        if let Some(fixture) = &self.2 {
            let records = (request.get_ref().from..request.get_ref().from + 2)
                .map(|sequence| wire::Record {
                    sequence,
                    value: Bytes::from_static(b"received"),
                    commit_id: Bytes::from(vec![1; 32]),
                    committed_at_micros: 123,
                })
                .collect();
            let mut frame = crate::wire_codec::zstd_read_response(records);
            frame.delivery_token = fixture.token_length.map(|length| vec![7; length].into());
            if self.1 == Some("bad_cursor") {
                frame = read_response_wire(vec![
                    wire::Record {
                        commit_id: vec![1; 32].into(),
                        ..Default::default()
                    },
                    wire::Record {
                        sequence: 4,
                        commit_id: vec![1; 32].into(),
                        ..Default::default()
                    },
                ]);
                frame.delivery_token = Some(vec![7; 32].into());
            }
            return Ok(Response::new(
                stream::once(async move { Ok(frame) })
                    .chain(stream::pending())
                    .boxed(),
            ));
        }
        if let Some(fault) = self.1 {
            let valid = read_response_wire(vec![wire::Record {
                commit_id: Bytes::from(vec![0; 32]),
                ..Default::default()
            }]);
            let frames: Self::ReadStream = match fault {
                "malformed" => {
                    stream::iter([Ok(read_response_wire(vec![wire::Record::default()]))]).boxed()
                }
                "error" => {
                    stream::iter([Ok(valid), Err(Status::permission_denied("denied"))]).boxed()
                }
                "pending" => stream::once(async move { Ok(valid) })
                    .chain(stream::pending())
                    .boxed(),
                _ => stream::iter([Ok(valid)]).boxed(),
            };
            return Ok(Response::new(frames));
        }
        let frames = self
            .0
            .read(request)
            .await?
            .into_inner()
            .collect::<Vec<_>>()
            .await;
        let mut records = Vec::new();
        for frame in frames {
            records.extend(read_response_records(frame?).map_err(|error| error_status(&error))?);
        }
        let regrouped = records
            .chunks(3)
            .enumerate()
            .map(|(index, chunk)| {
                Ok(if index % 2 == 0 {
                    crate::wire_codec::zstd_read_response(chunk.to_vec())
                } else {
                    read_response_wire(chunk.to_vec())
                })
            })
            .collect::<Vec<_>>();
        Ok(Response::new(stream::iter(regrouped).boxed()))
    }
    async fn follow(
        &self,
        request: Request<wire::FollowRequest>,
    ) -> Result<Response<Self::FollowStream>, Status> {
        if self.2.is_some() {
            let request = request.into_inner();
            return self
                .read(Request::new(wire::ReadRequest {
                    path: request.path,
                    from: request.from,
                    limit: wire::StreamLimit::MaxItems as u32,
                }))
                .await;
        }
        self.0.follow(request).await
    }
    async fn children(
        &self,
        request: Request<wire::ChildrenRequest>,
    ) -> Result<Response<Self::ChildrenStream>, Status> {
        if self.1 == Some("malformed_children") {
            return Ok(Response::new(
                stream::iter([Ok(wire::ChildrenResponse { child: None })]).boxed(),
            ));
        }
        self.0.children(request).await
    }
    async fn children_page(
        &self,
        request: Request<wire::ChildrenPageRequest>,
    ) -> Result<Response<wire::ChildrenPageResponse>, Status> {
        self.0.children_page(request).await
    }
    async fn commit(
        &self,
        request: Request<wire::CommitRequest>,
    ) -> Result<Response<wire::CommitResponse>, Status> {
        self.0.commit(request).await
    }
    async fn read_commit(
        &self,
        request: Request<wire::ReadCommitRequest>,
    ) -> Result<Response<wire::CommittedEnvelope>, Status> {
        self.0.read_commit(request).await
    }
}

struct AckFixture {
    token_length: Option<usize>,
    fail: bool,
    entered: AtomicUsize,
    cancelled: AtomicUsize,
    changed: tokio::sync::Notify,
}

impl AckFixture {
    fn new(token_length: Option<usize>, fail: bool) -> Arc<Self> {
        Arc::new(Self {
            token_length,
            fail,
            entered: AtomicUsize::new(0),
            cancelled: AtomicUsize::new(0),
            changed: tokio::sync::Notify::new(),
        })
    }
}

struct AckGuard(Arc<AckFixture>);

impl Drop for AckGuard {
    fn drop(&mut self) {
        self.0.cancelled.fetch_add(1, Ordering::SeqCst);
        self.0.changed.notify_one();
    }
}

async fn ack_loopback(
    fixture: Arc<AckFixture>,
    fault: Option<&'static str>,
) -> Result<(Channel, tokio::task::JoinHandle<()>), Box<dyn std::error::Error + Send + Sync>> {
    let listener = TcpListener::bind("127.0.0.1:0").await?;
    let endpoint = format!("http://{}", listener.local_addr()?);
    let service = MixedFrames(
        Service::new(Arc::new(MemoryStream::default())),
        fault,
        Some(fixture),
    );
    let server = tokio::spawn(async move {
        let _ = Server::builder()
            .add_service(StreamServiceServer::new(service))
            .serve_with_incoming(TcpListenerStream::new(listener))
            .await;
    });
    let channel = Endpoint::from_shared(endpoint)?.connect().await?;
    Ok((channel, server))
}

async fn wait_for_ack_count(
    fixture: &AckFixture,
    counter: &AtomicUsize,
) -> Result<(), tokio::time::error::Elapsed> {
    tokio::time::timeout(std::time::Duration::from_secs(1), async {
        while counter.load(Ordering::SeqCst) == 0 {
            fixture.changed.notified().await;
        }
    })
    .await
}

fn stalled_channel() -> Channel {
    Endpoint::from_static("http://stalled.invalid").connect_with_connector_lazy(tower::service_fn(
        |_| async {
            let (client, server) = tokio::io::duplex(64 * 1024);
            tokio::spawn(async move {
                let _server = server;
                std::future::pending::<()>().await;
            });
            Ok::<_, std::io::Error>(hyper_util::rt::TokioIo::new(client))
        },
    ))
}

async fn assert_unary_retry(
    error: Status,
    retry: bool,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let client = Client::connect(["https://fixture.invalid"], "fixture", None).await?;
    let body = wire::AppendRequest {
        path: "/fixture".to_owned(),
        records: vec![Bytes::from_static(b"mutation")],
        if_tail: Some(3),
        idempotency_key: Some(Bytes::from_static(b"stable-key")),
    };
    let mut attempts = Vec::new();
    let mut first = Some(error);
    let result = tokio::time::timeout(
        std::time::Duration::from_secs(1),
        client.unary(
            OPERATION_ENDPOINT_ATTEMPT_TIMEOUT,
            body.clone(),
            |_, request| {
                attempts.push(request.into_inner());
                let result = first.take().map_or_else(|| Ok(Response::new(())), Err);
                Box::pin(async move { result })
            },
        ),
    )
    .await?
    .and_then(|response| decode_response(response, Ok));
    if retry {
        assert_eq!(result, Ok(()));
        assert_eq!(attempts, vec![body.clone(), body]);
    } else {
        assert!(result.is_err());
        assert_eq!(attempts, vec![body]);
    }
    Ok(())
}

mod contract;
mod delivery;
mod observation;
mod retry;
mod tls;
