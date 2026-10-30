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

/// Only live accepted sockets are retained. Reset shuts down their original
/// OS connection; errors observed by clients come from real HTTP/2 I/O.
#[derive(Default)]
pub(super) struct Connections(Mutex<std::collections::HashMap<usize, std::net::TcpStream>>);

pub(super) struct Socket {
    socket: tokio::net::TcpStream,
    connections: Arc<Connections>,
    id: usize,
}

impl Connections {
    pub(super) fn incoming(
        self: &Arc<Self>,
        listener: tokio::net::TcpListener,
        counts: Arc<Counts>,
    ) -> impl futures::Stream<Item = std::io::Result<Socket>> + Send + 'static {
        let connections = Arc::clone(self);
        stream::try_unfold(listener, move |listener| {
            let connections = Arc::clone(&connections);
            let counts = Arc::clone(&counts);
            async move {
                let (socket, _) = listener.accept().await?;
                socket.set_nodelay(true)?;
                #[cfg(unix)]
                let retained = std::net::TcpStream::from(
                    std::os::fd::AsFd::as_fd(&socket).try_clone_to_owned()?,
                );
                #[cfg(windows)]
                let retained = std::net::TcpStream::from(
                    std::os::windows::io::AsSocket::as_socket(&socket).try_clone_to_owned()?,
                );
                let id = counts.connections.fetch_add(1, Ordering::Relaxed);
                connections
                    .0
                    .lock()
                    .map_err(|_| std::io::Error::other("accepted sockets poisoned"))?
                    .insert(id, retained);
                Ok::<_, std::io::Error>(Some((
                    Socket {
                        socket,
                        connections,
                        id,
                    },
                    listener,
                )))
            }
        })
    }

    pub(super) fn reset(&self) -> std::io::Result<()> {
        // Detach under the registry lock; shutdown never runs with it held.
        let sockets = std::mem::take(
            &mut *self
                .0
                .lock()
                .map_err(|_| std::io::Error::other("accepted sockets poisoned"))?,
        );
        if sockets.is_empty() {
            return Err(std::io::Error::from(std::io::ErrorKind::NotConnected));
        }
        for socket in sockets.into_values() {
            socket.shutdown(std::net::Shutdown::Both)?;
        }
        Ok(())
    }
}

impl Drop for Socket {
    fn drop(&mut self) {
        self.connections
            .0
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .remove(&self.id);
    }
}

impl tonic::transport::server::Connected for Socket {
    type ConnectInfo = <tokio::net::TcpStream as tonic::transport::server::Connected>::ConnectInfo;
    fn connect_info(&self) -> Self::ConnectInfo {
        <tokio::net::TcpStream as tonic::transport::server::Connected>::connect_info(&self.socket)
    }
}

impl tokio::io::AsyncRead for Socket {
    fn poll_read(
        self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
        buffer: &mut tokio::io::ReadBuf<'_>,
    ) -> std::task::Poll<std::io::Result<()>> {
        tokio::io::AsyncRead::poll_read(std::pin::Pin::new(&mut self.get_mut().socket), cx, buffer)
    }
}

impl tokio::io::AsyncWrite for Socket {
    fn poll_write(
        self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
        buffer: &[u8],
    ) -> std::task::Poll<std::io::Result<usize>> {
        tokio::io::AsyncWrite::poll_write(
            std::pin::Pin::new(&mut self.get_mut().socket),
            cx,
            buffer,
        )
    }
    fn poll_flush(
        self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<std::io::Result<()>> {
        tokio::io::AsyncWrite::poll_flush(std::pin::Pin::new(&mut self.get_mut().socket), cx)
    }
    fn poll_shutdown(
        self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<std::io::Result<()>> {
        tokio::io::AsyncWrite::poll_shutdown(std::pin::Pin::new(&mut self.get_mut().socket), cx)
    }
}

/// Retain the actual serving task's completion, including abort or unwind.
pub(super) fn serve(
    serving: impl std::future::Future<Output = Result<(), tonic::transport::Error>> + Send + 'static,
) -> (
    tokio::task::JoinHandle<Result<(), tonic::transport::Error>>,
    tokio::sync::watch::Receiver<()>,
) {
    let (alive, ended) = tokio::sync::watch::channel(());
    let server = tokio::spawn(async move {
        let _alive = alive;
        serving.await
    });
    (server, ended)
}

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
    pub(super) node: Mutex<String>,
    pub(super) destinations: Mutex<Vec<String>>,
    pub(super) connections: AtomicUsize,
    pub(super) sessions: AtomicUsize,
    pub(super) closed_sessions: AtomicUsize,
    pub(super) received_bytes: AtomicUsize,
    pub(super) transmitted_bytes: AtomicUsize,
    pub(super) sent: AtomicUsize,
    pub(super) follows: AtomicUsize,
    pub(super) removals: AtomicUsize,
    // Fixture locks are never nested and never cross an await.
    pub(super) traffic: Mutex<std::collections::BTreeMap<(usize, u64, u64), Traffic>>,
    pub(super) actors: AtomicUsize,
    /// Calls served by the unary `StreamService` beside the session.
    pub(super) unary: AtomicUsize,
    pub(super) acknowledgements: AtomicUsize,
    pub(super) windows: AtomicUsize,
    pub(super) changed: tokio::sync::Notify,
    // Fixture-only lock: never nested, and never held across an await.
    pub(super) resumes: Mutex<Vec<(u64, u64, u64)>>,
    pub(super) guards: Mutex<Vec<(u64, u64, Bytes)>>,
}
impl Counts {
    /// Count one inbound frame of `session` before the server consumes it.
    fn observe_input(&self, session: usize, frame: &wire::SessionFrame) -> Result<(), StreamError> {
        // Uncompressed gRPC message prefix plus the complete protobuf.
        self.received_bytes
            .fetch_add(5 + frame.encoded_len(), Ordering::Relaxed);
        match &frame.body {
            Some(Body::Hello(hello)) => {
                self.destinations
                    .lock()
                    .map_err(|_| StreamError::Unavailable)?
                    .push(hello.destination_node.clone());
            }
            Some(Body::Interest(interest)) => self.observe_interest(interest)?,
            Some(Body::Ack(_)) => {
                self.acknowledgements.fetch_add(1, Ordering::Relaxed);
            }
            Some(Body::Window(grant)) if frame.channel_id != 0 => {
                let mut traffic = self.traffic.lock().map_err(|_| StreamError::Unavailable)?;
                let flow = traffic
                    .entry((session, frame.channel_id, frame.generation))
                    .or_default();
                flow.granted_frames += u64::from(grant.frames);
                flow.granted_bytes += grant.bytes;
                drop(traffic);
                self.windows.fetch_add(1, Ordering::Relaxed);
                self.changed.notify_one();
            }
            _ => {}
        }
        Ok(())
    }
    fn observe_interest(&self, interest: &wire::SessionInterest) -> Result<(), StreamError> {
        self.removals
            .fetch_add(interest.remove.len(), Ordering::Relaxed);
        for subscription in &interest.upsert {
            match subscription.kind {
                Some(Kind::Follow(_)) => {
                    self.follows.fetch_add(1, Ordering::Relaxed);
                    if let (Some(channel), Some(resume)) =
                        (subscription.channel, subscription.resume.as_ref())
                    {
                        self.resumes
                            .lock()
                            .map_err(|_| StreamError::Unavailable)?
                            .push((channel.channel_id, channel.generation, resume.next));
                    }
                    if let (Some(channel), Some(Kind::Follow(follow))) =
                        (subscription.channel, &subscription.kind)
                    {
                        self.guards
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
                    self.actors.fetch_add(1, Ordering::Relaxed);
                }
                _ => {}
            }
        }
        // Publish the entire interest before waking readers of resumes and guards.
        self.changed.notify_one();
        Ok(())
    }
    /// Count one outbound data frame of `session`.
    fn observe_output(&self, session: usize, frame: &wire::SessionFrame) {
        let Some(Body::Data(data)) = &frame.body else {
            return;
        };
        let mut traffic = self.traffic.lock().expect("fixture traffic poisoned");
        let flow = traffic
            .entry((session, frame.channel_id, frame.generation))
            .or_default();
        flow.positions
            .push(data.after.as_ref().expect("data cursor").next);
        flow.sent_bytes += frame.encoded_len() as u64;
        drop(traffic);
        self.sent.fetch_add(1, Ordering::Relaxed);
        self.changed.notify_one();
    }
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
            let lifetime = CountSessionDrop(Arc::clone(&self.counts));
            return Ok(Reply::new(
                stream::unfold((input, lifetime), |(mut input, lifetime)| async move {
                    let _lifetime = lifetime;
                    while input.message().await.ok().flatten().is_some() {}
                    None::<(
                        Result<wire::SessionFrame, Status>,
                        (Streaming<wire::SessionFrame>, CountSessionDrop),
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
                    counts.observe_input(session, frame)?;
                }
                input.map_err(|_| StreamError::Unavailable)
            })
            .boxed();
        let output = session::serve(
            Arc::clone(&self.provider),
            acyclic_stream::session::testing::authority(),
            self.counts
                .node
                .lock()
                .map_err(|_| Status::internal("fixture node poisoned"))?
                .clone(),
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
                output.map(move |item| {
                    let mut item = item.map(session::Frame::into_wire);
                    if !confirm_checked
                        && let Ok(wire::SessionFrame {
                            body: Some(Body::Opened(opened)),
                            ..
                        }) = &mut item
                    {
                        opened.preceding_commit_id = Bytes::new();
                    }
                    if let Ok(frame) = &item {
                        sent.observe_output(session, frame);
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
    // Retains the raw transport for consumer-specific RPC fault probes.
    pub(super) _channel: TransportChannel,
    pub(super) tls_client: Option<grpc::Client>,
    pub(super) counts: Arc<Counts>,
    pub(super) server: tokio::task::JoinHandle<Result<(), tonic::transport::Error>>,
    pub(super) server_ended: tokio::sync::watch::Receiver<()>,
    pub(super) connections: Arc<Connections>,
}
impl Fixture {
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
        *counts.node.lock().map_err(|_| "fixture node poisoned")? = node.into();
        let connections = Arc::new(Connections::default());
        let incoming = connections.incoming(listener, Arc::clone(&counts));
        let (controls, _) = tokio::sync::broadcast::channel(16);
        let unary = grpc::Service::new(Arc::clone(&provider));
        let unary_counts = Arc::clone(&counts);
        let host = Host {
            provider,
            counts: Arc::clone(&counts),
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
        let serving = builder
            .add_service(
                wire::stream_service_server::StreamServiceServer::with_interceptor(
                    unary,
                    move |request| {
                        unary_counts.unary.fetch_add(1, Ordering::Relaxed);
                        Ok(request)
                    },
                ),
            )
            .add_service(StreamSessionServiceServer::new(host))
            .serve_with_incoming(incoming);
        let (server, server_ended) = serve(serving);
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
            _channel: channel,
            tls_client,
            counts,
            server,
            server_ended,
            connections,
        })
    }
    pub(super) fn reset(&self) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        self.connections.reset()?;
        Ok(())
    }
    pub(super) async fn until(&self, ready: impl Fn(&Counts) -> bool) -> Result<(), StreamError> {
        let mut ended = self.server_ended.clone();
        loop {
            let changed = self.counts.changed.notified();
            if ready(&self.counts) {
                return Ok(());
            }
            tokio::select! {
                _ = changed => {},
                _ = ended.changed() => return Err(StreamError::Unavailable),
            }
        }
    }
}
impl Drop for Fixture {
    fn drop(&mut self) {
        let _closed = self.connections.reset();
        self.server.abort();
    }
}

#[cfg(test)]
mod observation_tests {
    use super::*;
    use std::{
        future::Future as _,
        sync::atomic::AtomicBool,
        task::{Context, Poll, Wake, Waker},
    };

    #[derive(Default)]
    struct InterestWake {
        counts: Arc<Counts>,
        woke: AtomicBool,
        complete: AtomicBool,
    }

    impl Wake for InterestWake {
        fn wake(self: Arc<Self>) {
            self.wake_by_ref();
        }

        fn wake_by_ref(self: &Arc<Self>) {
            let resumed = self
                .counts
                .resumes
                .lock()
                .expect("resumes")
                .contains(&(1, 2, 4));
            let guarded = self.counts.guards.lock().expect("guards").contains(&(
                1,
                2,
                Bytes::from_static(b"yielded-anchor"),
            ));
            self.complete.store(resumed && guarded, Ordering::Relaxed);
            self.woke.store(true, Ordering::Relaxed);
        }
    }

    #[test]
    fn interest_notification_publishes_resume_and_guard_together() {
        let observer = Arc::new(InterestWake::default());
        let waker = Waker::from(Arc::clone(&observer));
        let mut context = Context::from_waker(&waker);
        let mut changed = std::pin::pin!(observer.counts.changed.notified());
        assert_eq!(changed.as_mut().poll(&mut context), Poll::Pending);

        observer
            .counts
            .observe_interest(&wire::SessionInterest {
                revision: 1,
                upsert: vec![wire::SessionSubscription {
                    channel: Some(wire::SessionChannel {
                        channel_id: 1,
                        generation: 2,
                    }),
                    resume: Some(wire::SessionCursor {
                        next: 4,
                        opaque: Bytes::new(),
                    }),
                    kind: Some(Kind::Follow(wire::SessionFollow {
                        preceding_commit_id: Bytes::from_static(b"yielded-anchor"),
                        ..Default::default()
                    })),
                    ..Default::default()
                }],
                ..Default::default()
            })
            .expect("observe interest");

        assert!(observer.woke.load(Ordering::Relaxed));
        assert!(observer.complete.load(Ordering::Relaxed));
    }
}
