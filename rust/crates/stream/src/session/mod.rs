//! Multiplexed Streams follows shared by client and node-to-node sessions.

use crate::StreamProvider;
use crate::StreamProviderExt as _;
use crate::{StreamError, StreamPath, wire, wire_codec};
use bytes::Bytes;
#[cfg(all(feature = "grpc", not(target_arch = "wasm32")))]
use futures::stream::BoxStream;
use futures::{StreamExt, future, stream};
use prost::Message as _;
use std::collections::BTreeSet;
use std::collections::{BTreeMap, VecDeque};
use std::sync::Arc;
use tokio::sync::oneshot;
use wire::{session_data::Item, session_frame::Body, session_subscription::Kind};

mod admission;
pub use admission::{Admission, CapabilityVerifier, CredentialVerifier};
mod authority;
#[cfg(any(test, feature = "test-support"))]
#[doc(hidden)]
pub use authority::testing;
pub use authority::{Authority, AuthorityState, TrustSnapshot};
mod sources;
#[cfg(loom)]
pub use sources::model::{verify_source_recheck, verify_source_wakes};

mod calls;
mod pages;
mod resources;
mod sockets;
pub use calls::{CallAuthorization, CallProvider};
pub use resources::{
    ResourceAuthorization, ResourceFollowRequest, ResourceProvider, ResourceRevalidation,
    ResourceSubscription,
};
pub use sockets::{SocketChannel, SocketInbound, SocketProvider};
mod client;
mod connection;
mod local;
pub use connection::Connections;
#[cfg(loom)]
pub use connection::verify_connections;
mod gate;
mod interest;
mod policy;
mod queue;
mod runtime;
#[cfg(all(feature = "grpc", not(target_arch = "wasm32")))]
mod service;
use crate::runtime::{signal, sync};
#[cfg(all(feature = "grpc", not(target_arch = "wasm32")))]
pub(crate) use client::Cache;
pub use client::transport;
pub use client::{
    ActorSocket, ActorSocketSender, ApplicationDelivery, ApplicationSubscription, Client,
};
#[cfg(all(loom, feature = "grpc", not(target_arch = "wasm32")))]
pub use client::{
    verify_abandoned_reconnect, verify_cache, verify_cancel_before_open, verify_follow_data,
    verify_initial_retry_yield, verify_interest_accounting, verify_link_isolation,
    verify_node_dials, verify_pool_lifetime, verify_registration_deadline,
    verify_terminal_cursor_dial,
};
#[cfg(loom)]
pub use client::{verify_admission_binding, verify_admission_cancellation};
#[cfg(all(feature = "grpc", not(target_arch = "wasm32")))]
pub use service::Service;
const DATA_FRAME_BYTES: usize = crate::MAX_RECORD_BYTES + 128;
#[cfg(loom)]
pub use client::verify_latest_progress;
#[cfg(test)]
#[cfg(not(target_arch = "wasm32"))]
mod tests;

/// Finite session admission independent of the number of stored streams.
#[derive(Clone, Copy, Debug)]
pub struct Limits {
    /// Maximum active follows on this node connection.
    pub channels: usize,
    /// Maximum unsent data frames, including lossless attention records.
    pub pending_frames: usize,
    /// Maximum unsent encoded data bytes.
    pub pending_bytes: usize,
}
impl Default for Limits {
    fn default() -> Self {
        Self {
            channels: 16_384,
            pending_frames: crate::MAX_ITEMS,
            pending_bytes: crate::MAX_COMMAND_BYTES,
        }
    }
}

struct Channel {
    binding: Option<authority::Binding>,
    generation: u64,
    latest: bool,
    opaque_cursor: bool,
    /// The followed stream an unspelled record origin names.
    followed: Option<StreamPath>,
    revalidate: Option<ResourceRevalidation>,
    pending: VecDeque<Pending>,
    progress: Option<wire::SessionWatermark>,
    indexed: Option<usize>,
    credit: wire::SessionWindow,
    sent: wire::SessionCursor,
    applied: wire::SessionCursor,
    /// The `SessionClosed` code sent once every queued frame has drained.
    ending: Option<&'static str>,
    /// Peer frames of an actor socket; dropping it ends that socket.
    socket: Option<Box<dyn SocketInbound>>,
}
struct Pending {
    frame: wire::SessionFrame,
    key: Option<Bytes>,
}
enum Event {
    /// Confirmed anchor and the applied revision the open was served at.
    Opened(Option<crate::CommitId>, Option<wire::Revision>),
    Record(crate::Record),
    Progress(wire::SessionWatermark),
    /// One fragment of a finite one-shot reply, carried in `item`.
    CallChunk {
        index: u64,
        total: u64,
        bytes: Bytes,
        item: fn(Bytes) -> Item,
    },
    /// One frame to an actor socket's peer, counted from 1.
    Socket {
        next: u64,
        bytes: Bytes,
    },
    /// A bounded follow delivered its whole range.
    End,
}
type SourceItem = (wire::SessionChannel, Result<Event, StreamError>);
type Source = crate::BoxProviderStream<'static, SourceItem>;

struct Session<P: ?Sized> {
    authority: Arc<Authority>,
    slots: authority::Slots,
    trust_revision: u64,
    deadline: Option<u64>,
    provider: Arc<P>,
    resources: Option<Arc<dyn ResourceProvider>>,
    calls: Option<Arc<dyn CallProvider>>,
    sockets: Option<Arc<dyn SocketProvider>>,
    executor: Arc<dyn crate::runtime::Executor>,
    limits: Limits,
    node: String,
    hello: bool,
    prefer_control: bool,
    revision: u64,
    channels: BTreeMap<u64, Channel>,
    sources: sources::Sources,
    terminals: sources::Sources,
    incoming: transport::Responses,
    credit: wire::SessionWindow,
    controls: VecDeque<wire::SessionFrame>,
    pending_frames: usize,
    pending_bytes: usize,
    runnable: BTreeSet<(usize, u64)>,
    ready_after: Option<(usize, u64)>,
    /// A renewed credential its verifier is deciding. Later frames wait for
    /// it, in order; channels keep sending and trust is still enforced.
    verifying: Option<authority::Verifying>,
    #[cfg(test)]
    head_visits: usize,
}

enum ServingSession<P: ?Sized> {
    Active(Session<P>, Option<crate::BoxProviderFuture<'static, ()>>),
    Closing(
        std::collections::btree_map::IntoIter<u64, Channel>,
        std::collections::vec_deque::IntoIter<wire::SessionFrame>,
    ),
}

impl<P: ?Sized> ServingSession<P> {
    fn close(
        mut channels: std::collections::btree_map::IntoIter<u64, Channel>,
        mut controls: std::collections::vec_deque::IntoIter<wire::SessionFrame>,
    ) -> Option<(wire::SessionFrame, Self)> {
        if let Some(frame) = controls.next() {
            return Some((frame, Self::Closing(channels, controls)));
        }
        let (id, channel) = channels.next()?;
        Some((
            frame(
                id,
                channel.generation,
                Body::Closed(wire::SessionClosed {
                    code: StreamError::Unavailable.code().into(),
                    message: String::new(),
                }),
            ),
            Self::Closing(channels, controls),
        ))
    }
}

/// Opens one bidirectional node session over the canonical provider.
/// The host binds authenticated public trust, audience and presenting account.
pub fn serve<P: StreamProvider + ?Sized>(
    provider: Arc<P>,
    authority: Arc<Authority>,
    node: String,
    limits: Limits,
    incoming: transport::Responses,
) -> Result<transport::Responses, StreamError> {
    serve_on(
        provider,
        authority,
        node,
        limits,
        incoming,
        Arc::new(crate::runtime::PlatformExecutor),
    )
}
fn serve_on<P: StreamProvider + ?Sized>(
    provider: Arc<P>,
    authority: Arc<Authority>,
    node: String,
    limits: Limits,
    incoming: transport::Responses,
    executor: Arc<dyn crate::runtime::Executor>,
) -> Result<transport::Responses, StreamError> {
    serve_on_with_resources(provider, authority, None, node, limits, incoming, executor)
}

/// Opens a session whose product follows are resolved by their owning provider.
pub fn serve_with_resources<P: StreamProvider + ?Sized>(
    provider: Arc<P>,
    authority: Arc<Authority>,
    resources: Arc<dyn ResourceProvider>,
    node: String,
    limits: Limits,
    incoming: transport::Responses,
) -> Result<transport::Responses, StreamError> {
    serve_on_with_resources(
        provider,
        authority,
        Some(resources),
        node,
        limits,
        incoming,
        Arc::new(crate::runtime::PlatformExecutor),
    )
}

fn serve_on_with_resources<P: StreamProvider + ?Sized>(
    provider: Arc<P>,
    authority: Arc<Authority>,
    resources: Option<Arc<dyn ResourceProvider>>,
    node: String,
    limits: Limits,
    incoming: transport::Responses,
    executor: Arc<dyn crate::runtime::Executor>,
) -> Result<transport::Responses, StreamError> {
    serve_on_with_products(
        provider,
        authority,
        Products {
            resources,
            ..Products::default()
        },
        node,
        limits,
        incoming,
        executor,
        None,
    )
}

/// Opens follows and canonical Actors calls on one authenticated session.
/// Completing `shutdown` drops producers and closes admitted channels without
/// data credit. Its owner must complete it when the serving runtime stops.
pub fn serve_with_products<P: StreamProvider + ?Sized>(
    provider: Arc<P>,
    authority: Arc<Authority>,
    resources: Option<Arc<dyn ResourceProvider>>,
    calls: Arc<dyn CallProvider>,
    node: String,
    limits: Limits,
    incoming: transport::Responses,
    shutdown: Option<crate::BoxProviderFuture<'static, ()>>,
) -> Result<transport::Responses, StreamError> {
    serve_on_with_products(
        provider,
        authority,
        Products {
            resources,
            calls: Some(calls),
            sockets: None,
        },
        node,
        limits,
        incoming,
        Arc::new(crate::runtime::PlatformExecutor),
        shutdown,
    )
}

/// The owning product dispatchers one session routes to.
#[derive(Clone, Default)]
pub(crate) struct Products {
    pub(crate) resources: Option<Arc<dyn ResourceProvider>>,
    pub(crate) calls: Option<Arc<dyn CallProvider>>,
    pub(crate) sockets: Option<Arc<dyn SocketProvider>>,
}

fn serve_on_with_products<P: StreamProvider + ?Sized>(
    provider: Arc<P>,
    authority: Arc<Authority>,
    products: Products,
    node: String,
    limits: Limits,
    incoming: transport::Responses,
    executor: Arc<dyn crate::runtime::Executor>,
    shutdown: Option<crate::BoxProviderFuture<'static, ()>>,
) -> Result<transport::Responses, StreamError> {
    if node.is_empty()
        || limits.channels == 0
        || limits.pending_frames == 0
        || limits.pending_bytes < crate::MAX_RECORD_BYTES + 128
    {
        return Err(StreamError::InvalidArgument);
    }
    let trust_revision = authority
        .snapshot()
        .map_err(|_| StreamError::AccessDenied)?
        .revision;
    let Products {
        resources,
        calls,
        sockets,
    } = products;
    let session = Session {
        authority,
        slots: authority::Slots::default(),
        trust_revision,
        deadline: None,
        provider,
        resources,
        calls,
        sockets,
        executor,
        node,
        limits,
        hello: false,
        prefer_control: true,
        revision: 0,
        channels: BTreeMap::new(),
        sources: sources::Sources::new(),
        terminals: sources::Sources::new(),
        incoming,
        credit: wire::SessionWindow::default(),
        controls: VecDeque::new(),
        pending_frames: 0,
        pending_bytes: 0,
        runnable: BTreeSet::new(),
        ready_after: None,
        verifying: None,
        #[cfg(test)]
        head_visits: 0,
    };
    Ok(Box::pin(stream::try_unfold(
        ServingSession::Active(session, shutdown),
        |state| async move {
            let (mut session, mut shutdown) = match state {
                ServingSession::Active(session, shutdown) => (session, shutdown),
                ServingSession::Closing(channels, controls) => {
                    return Ok(ServingSession::close(channels, controls));
                }
            };
            let frame = if let Some(stopped) = shutdown.as_mut() {
                let next = match future::select(stopped, Box::pin(session.next_frame())).await {
                    future::Either::Right((frame, _)) => Some(frame?),
                    future::Either::Left(((), pending)) => {
                        drop(pending);
                        None
                    }
                };
                match next {
                    Some(frame) => frame,
                    None => {
                        // Drop input, calls and follows before publishing closure.
                        // Preserve terminal responses for channels already removed.
                        // Reuse both collections without allocating a shutdown queue.
                        session
                            .controls
                            .retain(|frame| matches!(frame.body, Some(Body::Closed(_))));
                        return Ok(ServingSession::close(
                            session.channels.into_iter(),
                            session.controls.into_iter(),
                        ));
                    }
                }
            } else {
                session.next_frame().await?
            };
            Ok(frame.map(|frame| (frame, ServingSession::Active(session, shutdown))))
        },
    )))
}

impl<P: StreamProvider + ?Sized> Session<P> {
    async fn next_frame(&mut self) -> Result<Option<wire::SessionFrame>, StreamError> {
        loop {
            self.fence_changed()?;
            if let Some(frame) = self.controls.pop_front() {
                return Ok(Some(frame));
            }
            if let Some(frame) = self.ready()? {
                return Ok(Some(frame));
            }
            if self.verifying.is_some() {
                self.await_verdict().await?;
                continue;
            }
            if self.sources.is_empty() && self.terminals.is_empty() {
                let changed = self.authority.changed(self.trust_revision, self.deadline);
                let input = match future::select(self.incoming.next(), Box::pin(changed)).await {
                    future::Either::Left((input, _)) => Some(input),
                    future::Either::Right(((), pending)) => {
                        drop(pending);
                        None
                    }
                };
                match input {
                    Some(Some(frame)) => self.receive(frame?).await?,
                    Some(None) => return Ok(None),
                    None => self.revalidate(None)?,
                }
                continue;
            }
            // Alternate deterministic priority. This bounds ready-side work
            // and lets the model explore the actual wakeup path without RNG.
            let input = self.incoming.next();
            let room = policy::room(
                self.limits.pending_frames,
                self.limits.pending_bytes,
                self.pending_frames,
                self.pending_bytes,
                DATA_FRAME_BYTES,
                self.credit.frames > 0 && self.credit.bytes > 0,
            );
            let source = self.sources.next_with_control(&mut self.terminals, room);
            let changed = Box::pin(self.authority.changed(self.trust_revision, self.deadline));
            let event = async {
                if self.prefer_control {
                    match future::select(input, source).await {
                        future::Either::Left((input, _)) => future::Either::Left(input),
                        future::Either::Right((source, _)) => future::Either::Right(source),
                    }
                } else {
                    match future::select(source, input).await {
                        future::Either::Left((source, _)) => future::Either::Right(source),
                        future::Either::Right((input, _)) => future::Either::Left(input),
                    }
                }
            };
            let event = match future::select(Box::pin(event), changed).await {
                future::Either::Left((event, _)) => Some(event),
                future::Either::Right(((), pending)) => {
                    drop(pending);
                    None
                }
            };
            let Some(event) = event else {
                self.revalidate(None)?;
                continue;
            };
            self.prefer_control = !self.prefer_control;
            match event {
                future::Either::Left(Some(frame)) => self.receive(frame?).await?,
                future::Either::Left(None) => return Ok(None),
                future::Either::Right(source) => {
                    if let Some((channel, event)) = source? {
                        self.record(channel, event)?;
                    }
                }
            }
        }
    }
}

impl<P: StreamProvider + ?Sized> Session<P> {
    /// Waits for the pending credential's verdict while serving every
    /// channel and enforcing trust changes; registers it once it is in.
    async fn await_verdict(&mut self) -> Result<(), StreamError> {
        let Some(verifying) = self.verifying.as_mut() else {
            return Ok(());
        };
        let changed = Box::pin(self.authority.changed(self.trust_revision, self.deadline));
        let event = if self.sources.is_empty() && self.terminals.is_empty() {
            match future::select(&mut verifying.verdict, changed).await {
                future::Either::Left((verdict, _)) => Some(Ok(verdict)),
                future::Either::Right(((), _)) => None,
            }
        } else {
            let room = policy::room(
                self.limits.pending_frames,
                self.limits.pending_bytes,
                self.pending_frames,
                self.pending_bytes,
                DATA_FRAME_BYTES,
                self.credit.frames > 0 && self.credit.bytes > 0,
            );
            let source = Box::pin(self.sources.next_with_control(&mut self.terminals, room));
            match future::select(future::select(&mut verifying.verdict, source), changed).await {
                future::Either::Left((future::Either::Left((verdict, _)), _)) => Some(Ok(verdict)),
                future::Either::Left((future::Either::Right((source, _)), _)) => Some(Err(source)),
                future::Either::Right(((), _)) => None,
            }
        };
        match event {
            Some(Ok(verdict)) => {
                if let Some(verifying) = self.verifying.take() {
                    self.authority.settle(
                        &mut self.slots,
                        verifying.slot,
                        verifying.length(),
                        verdict,
                        self.limits.channels,
                    )?;
                    self.revalidate(Some(verifying.slot))?;
                }
                Ok(())
            }
            Some(Err(source)) => {
                if let Some((channel, event)) = source? {
                    self.record(channel, event)?;
                }
                Ok(())
            }
            None => self.revalidate(None),
        }
    }
}

fn frame(channel_id: u64, generation: u64, body: Body) -> wire::SessionFrame {
    wire::SessionFrame {
        channel_id,
        generation,
        body: Some(body),
    }
}
fn cursor(next: u64) -> wire::SessionCursor {
    wire::SessionCursor {
        next,
        opaque: Bytes::new(),
    }
}

impl<P: StreamProvider + ?Sized> Session<P> {
    async fn receive(&mut self, input: wire::SessionFrame) -> Result<(), StreamError> {
        if input.encoded_len() > crate::MAX_COMMAND_BYTES {
            return Err(StreamError::LimitExceeded);
        }
        let body = input.body.ok_or(StreamError::InvalidArgument)?;
        if !self.hello {
            let Body::Hello(hello) = body else {
                return Err(StreamError::InvalidArgument);
            };
            if input.channel_id != 0
                || input.generation != 0
                || hello.version != 1
                || hello.client_id.is_empty()
                || (!hello.destination_node.is_empty() && hello.destination_node != self.node)
            {
                return Err(StreamError::InvalidArgument);
            }
            if hello.credentials.len() > self.limits.channels {
                return Err(StreamError::Capacity);
            }
            for (slot, credential) in hello.credentials.iter().enumerate() {
                self.authority
                    .register(
                        &mut self.slots,
                        u32::try_from(slot).map_err(|_| StreamError::Capacity)?,
                        credential,
                        self.limits.channels,
                    )
                    .await?;
            }
            self.deadline = self.authority.deadline(&mut self.slots);
            self.hello = true;
            self.credit = wire::SessionWindow {
                bytes: hello.receive_bytes,
                frames: hello.receive_frames,
            };
            self.controls.push_back(frame(
                0,
                0,
                Body::Welcome(wire::SessionWelcome {
                    version: 1,
                    node_id: self.node.clone(),
                    receive_bytes: crate::MAX_COMMAND_BYTES as u64,
                    receive_frames: u32::try_from(crate::MAX_ITEMS)
                        .map_err(|_| StreamError::Capacity)?,
                }),
            ));
            return Ok(());
        }
        if input.channel_id != 0 && matches!(body, Body::Window(_) | Body::Ack(_) | Body::Data(_)) {
            match self.channels.get(&input.channel_id) {
                None => return Ok(()),
                Some(channel) if input.generation < channel.generation => return Ok(()),
                Some(channel) if input.generation != channel.generation => {
                    return Err(StreamError::InvalidArgument);
                }
                Some(_) => {}
            }
        }
        match body {
            Body::Credential(credential) if input.channel_id == 0 && input.generation == 0 => {
                self.verifying = self.authority.verifying(
                    &mut self.slots,
                    credential.slot,
                    credential.credential.into(),
                    self.limits.channels,
                )?;
                if self.verifying.is_none() {
                    self.revalidate(Some(credential.slot))?;
                }
                Ok(())
            }
            Body::Interest(interest) if input.channel_id == 0 && input.generation == 0 => {
                self.interest(interest)
            }
            Body::Window(grant) if input.channel_id == 0 && input.generation == 0 => {
                wire_codec::session::grant_credit(&mut self.credit, &grant)?;
                Ok(())
            }
            Body::Window(grant) => {
                let channel = self.channel(input.channel_id, input.generation)?;
                wire_codec::session::grant_credit(&mut channel.credit, &grant)?;
                self.refresh_source(input.channel_id);
                Ok(())
            }
            Body::Ack(ack) => {
                let channel = self.channel(input.channel_id, input.generation)?;
                let applied = ack.applied.ok_or(StreamError::InvalidArgument)?;
                if (!channel.opaque_cursor && !applied.opaque.is_empty())
                    || (applied.next == channel.sent.next && applied.opaque != channel.sent.opaque)
                    || applied.next < channel.applied.next
                    || applied.next > channel.sent.next
                {
                    return Err(StreamError::OutOfRange);
                }
                channel.applied = applied;
                Ok(())
            }
            Body::Data(data) if input.channel_id != 0 => {
                let channel = self.channel(input.channel_id, input.generation)?;
                let (Some(socket), Some(Item::ActorSocket(bytes))) = (&channel.socket, data.item)
                else {
                    return Err(StreamError::InvalidArgument);
                };
                if channel.ending.is_some() {
                    return Ok(());
                }
                if bytes.is_empty() || bytes.len() > crate::MAX_RECORD_BYTES {
                    return Err(StreamError::LimitExceeded);
                }
                // Handed over without awaiting the owner, so one socket's
                // storage latency never stalls the session's other channels.
                if let Err(error) = socket.receive(bytes) {
                    self.end(input.channel_id, error.code())?;
                }
                Ok(())
            }
            _ => Err(StreamError::InvalidArgument),
        }
    }
    fn channel(&mut self, id: u64, generation: u64) -> Result<&mut Channel, StreamError> {
        self.channels
            .get_mut(&id)
            .filter(|channel| channel.generation == generation)
            .ok_or(StreamError::InvalidArgument)
    }
}
