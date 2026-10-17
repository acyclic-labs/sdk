// Shared by stream unit tests and the harness volume qualification.
use acyclic_stream::{
    StreamError, StreamProvider, grpc,
    session::{self, Limits},
    wire,
};
use bytes::Bytes;
use futures::{
    StreamExt as _,
    stream::{self, BoxStream},
};
use prost::Message as _;
use std::sync::{
    Arc, Mutex,
    atomic::{AtomicUsize, Ordering},
};
use tonic::{
    Request, Response as Reply, Status, Streaming,
    transport::{Channel as TransportChannel, Endpoint},
};
use wire::stream_session_service_server::{StreamSessionService, StreamSessionServiceServer};
use wire::{session_frame::Body, session_subscription::Kind};

pub(super) struct Options {
    pub(super) tls: bool,
    pub(super) withhold_welcome: bool,
    pub(super) confirm_checked: bool,
}
impl Default for Options {
    fn default() -> Self {
        Self {
            tls: false,
            withhold_welcome: false,
            confirm_checked: true,
        }
    }
}

#[derive(Clone, Default)]
pub(super) struct Traffic {
    pub(super) granted_frames: u64,
    pub(super) granted_bytes: u64,
    pub(super) sent_bytes: u64,
    pub(super) positions: Vec<u64>,
}

#[derive(Default)]
pub(super) struct Counts {
    pub(super) connections: AtomicUsize,
    pub(super) sessions: AtomicUsize,
    pub(super) closed_sessions: AtomicUsize,
    pub(super) received_bytes: AtomicUsize,
    pub(super) transmitted_bytes: AtomicUsize,
    pub(super) sent: AtomicUsize,
    pub(super) follows: AtomicUsize,
    pub(super) removals: AtomicUsize,
    pub(super) children: AtomicUsize,
    // Fixture locks are never nested and never cross an await.
    pub(super) traffic: Mutex<std::collections::BTreeMap<(usize, u64, u64), Traffic>>,
    pub(super) actors: AtomicUsize,
    pub(super) acknowledgements: AtomicUsize,
    pub(super) windows: AtomicUsize,
    pub(super) changed: tokio::sync::Notify,
    // Fixture-only lock: never nested, and never held across an await.
    pub(super) resumes: Mutex<Vec<(u64, u64, u64)>>,
    pub(super) guards: Mutex<Vec<(u64, u64, Bytes)>>,
}
struct CountSessionDrop(Arc<Counts>);
impl Drop for CountSessionDrop {
    fn drop(&mut self) {
        self.0.closed_sessions.fetch_add(1, Ordering::Relaxed);
        self.0.changed.notify_one();
    }
}
struct Host<P> {
    provider: Arc<P>,
    counts: Arc<Counts>,
    node: String,
    controls: tokio::sync::broadcast::Sender<Result<wire::SessionFrame, Status>>,
    withhold_welcome: bool,
    confirm_checked: bool,
}
#[tonic::async_trait]
impl<P: StreamProvider> StreamSessionService for Host<P> {
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
        let session = self.counts.sessions.fetch_add(1, Ordering::Relaxed) + 1;
        self.counts.changed.notify_one();
        if self.withhold_welcome {
            let input = request.into_inner();
            return Ok(Reply::new(
                stream::unfold(input, |mut input| async move {
                    while input.message().await.ok().flatten().is_some() {}
                    None::<(
                        Result<wire::SessionFrame, Status>,
                        Streaming<wire::SessionFrame>,
                    )>
                })
                .boxed(),
            ));
        }
        let counts = Arc::clone(&self.counts);
        let input = request
            .into_inner()
            .map(move |input| {
                if let Ok(frame) = &input {
                    // Uncompressed gRPC message prefix plus the complete protobuf.
                    counts
                        .received_bytes
                        .fetch_add(5 + frame.encoded_len(), Ordering::Relaxed);
                }
                if let Ok(wire::SessionFrame {
                    body: Some(Body::Interest(interest)),
                    ..
                }) = &input
                {
                    counts
                        .removals
                        .fetch_add(interest.remove.len(), Ordering::Relaxed);
                    counts.changed.notify_one();
                    for subscription in &interest.upsert {
                        if matches!(subscription.kind, Some(Kind::Children(_))) {
                            counts.children.fetch_add(1, Ordering::Relaxed);
                        }
                        match subscription.kind {
                            Some(Kind::Follow(_) | Kind::Children(_)) => {
                                counts.follows.fetch_add(1, Ordering::Relaxed);
                                if let (Some(channel), Some(resume)) =
                                    (subscription.channel, subscription.resume.as_ref())
                                {
                                    counts
                                        .resumes
                                        .lock()
                                        .map_err(|_| StreamError::Unavailable)?
                                        .push((
                                            channel.channel_id,
                                            channel.generation,
                                            resume.next,
                                        ));
                                }
                                counts.changed.notify_one();
                                if let (Some(channel), Some(Kind::Follow(follow))) =
                                    (subscription.channel, &subscription.kind)
                                {
                                    counts
                                        .guards
                                        .lock()
                                        .map_err(|_| StreamError::Unavailable)?
                                        .push((
                                            channel.channel_id,
                                            channel.generation,
                                            follow.preceding_commit_id.clone(),
                                        ));
                                }
                            }
                            Some(Kind::ActorSocket(_) | Kind::ActorCall(_)) => {
                                counts.actors.fetch_add(1, Ordering::Relaxed);
                            }
                            _ => {}
                        }
                    }
                }
                if matches!(
                    &input,
                    Ok(wire::SessionFrame {
                        body: Some(Body::Ack(_)),
                        ..
                    })
                ) {
                    counts.acknowledgements.fetch_add(1, Ordering::Relaxed);
                }
                if matches!(&input, Ok(wire::SessionFrame {
                    channel_id, body: Some(Body::Window(_)), ..
                }) if *channel_id != 0)
                {
                    if let Ok(wire::SessionFrame {
                        channel_id,
                        generation,
                        body: Some(Body::Window(grant)),
                        ..
                    }) = &input
                    {
                        let mut traffic = counts
                            .traffic
                            .lock()
                            .map_err(|_| StreamError::Unavailable)?;
                        let flow = traffic
                            .entry((session, *channel_id, *generation))
                            .or_default();
                        flow.granted_frames += u64::from(grant.frames);
                        flow.granted_bytes += grant.bytes;
                    }
                    counts.windows.fetch_add(1, Ordering::Relaxed);
                    counts.changed.notify_one();
                }
                input.map_err(|_| StreamError::Unavailable)
            })
            .boxed();
        let output = session::serve(
            Arc::clone(&self.provider),
            self.node.clone(),
            Limits::default(),
            input,
        )
        .map_err(|error| Status::invalid_argument(error.code()))?;
        let controls = stream::unfold(self.controls.subscribe(), |mut receiver| async move {
            receiver.recv().await.ok().map(|frame| (frame, receiver))
        });
        let sent = Arc::clone(&self.counts);
        let confirm_checked = self.confirm_checked;
        let lifetime = CountSessionDrop(Arc::clone(&self.counts));
        Ok(Reply::new(
            stream::select(
                output.map(move |mut item| {
                    if !confirm_checked
                        && let Ok(wire::SessionFrame {
                            body: Some(Body::Opened(opened)),
                            ..
                        }) = &mut item
                    {
                        opened.preceding_commit_id = Bytes::new();
                    }
                    if item
                        .as_ref()
                        .is_ok_and(|value| matches!(value.body, Some(Body::Data(_))))
                    {
                        if let Ok(wire::SessionFrame {
                            channel_id,
                            generation,
                            body: Some(Body::Data(data)),
                            ..
                        }) = &item
                        {
                            let mut traffic =
                                sent.traffic.lock().expect("fixture traffic poisoned");
                            let flow = traffic
                                .entry((session, *channel_id, *generation))
                                .or_default();
                            flow.positions
                                .push(data.after.as_ref().expect("data cursor").next);
                            flow.sent_bytes +=
                                item.as_ref().expect("data frame").encoded_len() as u64;
                        }
                        sent.sent.fetch_add(1, Ordering::Relaxed);
                        sent.changed.notify_one();
                    }
                    item.map_err(|error| Status::failed_precondition(error.code()))
                }),
                controls,
            )
            .inspect(move |item| {
                if let Ok(frame) = item {
                    lifetime
                        .0
                        .transmitted_bytes
                        .fetch_add(5 + frame.encoded_len(), Ordering::Relaxed);
                    lifetime.0.changed.notify_one();
                }
            })
            .boxed(),
        ))
    }
}
pub(super) struct Fixture {
    pub(super) controls: tokio::sync::broadcast::Sender<Result<wire::SessionFrame, Status>>,
    pub(super) channel: TransportChannel,
    pub(super) tls_client: Option<grpc::Client>,
    pub(super) counts: Arc<Counts>,
    pub(super) server: tokio::task::JoinHandle<Result<(), tonic::transport::Error>>,
}
impl Fixture {
    pub(super) async fn open<P: StreamProvider>(
        provider: Arc<P>,
        node: &str,
    ) -> Result<Self, Box<dyn std::error::Error + Send + Sync>> {
        Self::configured(provider, node, false, true).await
    }
    pub(super) async fn configured<P: StreamProvider>(
        provider: Arc<P>,
        node: &str,
        withhold_welcome: bool,
        confirm_checked: bool,
    ) -> Result<Self, Box<dyn std::error::Error + Send + Sync>> {
        Self::with_options(
            provider,
            node,
            Options {
                withhold_welcome,
                confirm_checked,
                tls: false,
            },
        )
        .await
    }
    pub(super) async fn with_options<P: StreamProvider>(
        provider: Arc<P>,
        node: &str,
        options: Options,
    ) -> Result<Self, Box<dyn std::error::Error + Send + Sync>> {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
        let certificate = options
            .tls
            .then(|| rcgen::generate_simple_self_signed(vec!["localhost".into()]))
            .transpose()?;
        let pem = certificate
            .as_ref()
            .map(|certificate| certificate.cert.pem());
        let endpoint = if options.tls {
            format!("https://localhost:{}", listener.local_addr()?.port())
        } else {
            format!("http://{}", listener.local_addr()?)
        };
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
        let (controls, _) = tokio::sync::broadcast::channel(16);
        let unary = grpc::Service::new(Arc::clone(&provider));
        let host = Host {
            provider,
            counts: Arc::clone(&counts),
            node: node.into(),
            controls: controls.clone(),
            withhold_welcome: options.withhold_welcome,
            confirm_checked: options.confirm_checked,
        };
        let mut builder = tonic::transport::Server::builder();
        if let Some(certificate) = &certificate {
            builder = builder.tls_config(tonic::transport::ServerTlsConfig::new().identity(
                tonic::transport::Identity::from_pem(
                    certificate.cert.pem(),
                    certificate.signing_key.serialize_pem(),
                ),
            ))?;
        }
        let server = tokio::spawn(
            builder
                .add_service(wire::stream_service_server::StreamServiceServer::new(unary))
                .add_service(StreamSessionServiceServer::new(host))
                .serve_with_incoming(incoming),
        );
        let tls_client = match pem.as_ref() {
            Some(pem) => Some(
                grpc::Client::connect_eager_with_ca_certificate(&endpoint, "fixture", pem).await?,
            ),
            None => None,
        };
        let mut endpoint = Endpoint::from_shared(endpoint)?.tcp_nodelay(true);
        if let Some(pem) = pem {
            endpoint = endpoint.tls_config(
                tonic::transport::ClientTlsConfig::new()
                    .ca_certificate(tonic::transport::Certificate::from_pem(pem)),
            )?;
        }
        let channel = if tls_client.is_some() {
            endpoint.connect_lazy()
        } else {
            endpoint.connect().await?
        };
        Ok(Self {
            controls,
            channel,
            tls_client,
            counts,
            server,
        })
    }
    pub(super) fn reset(&self) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        self.controls
            .send(Err(Status::unavailable("fixture Session reset")))?;
        Ok(())
    }
    pub(super) async fn until(&self, ready: impl Fn(&Counts) -> bool) {
        loop {
            let changed = self.counts.changed.notified();
            if ready(&self.counts) {
                return;
            }
            changed.await;
        }
    }
}
impl Drop for Fixture {
    fn drop(&mut self) {
        self.server.abort();
    }
}
