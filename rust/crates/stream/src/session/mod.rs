//! Multiplexed Streams follows shared by client and node-to-node sessions.

use crate::StreamProvider;
use crate::{StreamError, StreamPath, wire, wire_codec};
use bytes::Bytes;
#[cfg(not(target_arch = "wasm32"))]
use futures::stream::BoxStream;
use futures::{StreamExt, future, stream};
use prost::Message as _;
use std::collections::BTreeSet;
use std::collections::{BTreeMap, VecDeque};
use std::sync::Arc;
use tokio::sync::oneshot;
use wire::{session_data::Item, session_frame::Body, session_subscription::Kind};

mod authority;
pub use authority::{Authority, AuthorityState, TrustSnapshot};
mod sources;
#[cfg(acyclic_stream_loom)]
pub use sources::model::{verify_source_recheck, verify_source_wakes};
mod children;
mod publications;
pub use publications::{
    PublicationProvider, PublicationRevalidation, PublicationStream, PublicationSubscription,
};
mod client;
mod connection;
mod local;
pub use connection::Connections;
#[cfg(acyclic_stream_loom)]
pub use connection::verify_connections;
mod gate;
mod interest;
mod policy;
mod queue;
pub mod runtime;
#[cfg(not(target_arch = "wasm32"))]
mod service;
mod signal;
mod sync;
#[cfg(not(target_arch = "wasm32"))]
pub(crate) use client::Cache;
pub use client::Client;
pub use client::transport;
#[cfg(acyclic_stream_loom)]
pub use client::{
    verify_abandoned_reconnect, verify_admission_binding, verify_cache, verify_cancel_before_open,
    verify_discovery_admission, verify_follow_data, verify_initial_retry_yield,
    verify_interest_accounting, verify_link_isolation, verify_node_dials, verify_pool_lifetime,
    verify_registration_deadline, verify_terminal_cursor_dial,
};
#[cfg(not(target_arch = "wasm32"))]
pub use service::Service;
// Child frames carry both their path and the provider-owned resume anchor.
const CHILD_FRAME_BYTES: usize = 2 * crate::MAX_PATH_BYTES + 128;
const DATA_FRAME_BYTES: usize = if CHILD_FRAME_BYTES > crate::MAX_RECORD_BYTES + 128 {
    CHILD_FRAME_BYTES
} else {
    crate::MAX_RECORD_BYTES + 128
};
#[cfg(acyclic_stream_loom)]
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
    generation: u64,
    latest: bool,
    opaque_cursor: bool,
    revalidate: Option<PublicationRevalidation>,
    pending: VecDeque<Pending>,
    indexed: Option<usize>,
    credit: wire::SessionWindow,
    sent: wire::SessionCursor,
    applied: wire::SessionCursor,
    ending: Option<StreamError>,
}
struct Pending {
    frame: wire::SessionFrame,
    key: Option<Bytes>,
}
enum Event {
    Opened(Option<crate::CommitId>),
    Record(crate::Record),
    Child(crate::Child, wire::SessionCursor),
    Publication(wire::SessionData),
}
type SourceItem = (wire::SessionChannel, Result<Event, StreamError>);
type Source = crate::BoxProviderStream<'static, SourceItem>;

struct Session<P: ?Sized> {
    provider: Arc<P>,
    publications: Option<Arc<dyn PublicationProvider>>,
    executor: Arc<dyn runtime::Executor>,
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
    #[cfg(test)]
    head_visits: usize,
}

/// Opens one bidirectional node session over the canonical provider.
/// Transport authentication and per-path capabilities remain the host's responsibility.
pub fn serve<P: StreamProvider + ?Sized>(
    provider: Arc<P>,
    node: String,
    limits: Limits,
    incoming: transport::Responses,
) -> Result<transport::Responses, StreamError> {
    serve_with_publications(provider, None, node, limits, incoming)
}

/// Opens a session with a filesystem publication provider.
pub fn serve_with_publications<P: StreamProvider + ?Sized>(
    provider: Arc<P>,
    publications: Option<Arc<dyn PublicationProvider>>,
    node: String,
    limits: Limits,
    incoming: transport::Responses,
) -> Result<transport::Responses, StreamError> {
    serve_on_with_publications(
        provider,
        publications,
        node,
        limits,
        incoming,
        Arc::new(runtime::PlatformExecutor),
    )
}
fn serve_on<P: StreamProvider + ?Sized>(
    provider: Arc<P>,
    node: String,
    limits: Limits,
    incoming: transport::Responses,
    executor: Arc<dyn runtime::Executor>,
) -> Result<transport::Responses, StreamError> {
    serve_on_with_publications(provider, None, node, limits, incoming, executor)
}

fn serve_on_with_publications<P: StreamProvider + ?Sized>(
    provider: Arc<P>,
    publications: Option<Arc<dyn PublicationProvider>>,
    node: String,
    limits: Limits,
    incoming: transport::Responses,
    executor: Arc<dyn runtime::Executor>,
) -> Result<transport::Responses, StreamError> {
    if node.is_empty()
        || limits.channels == 0
        || limits.pending_frames == 0
        || limits.pending_bytes < crate::MAX_RECORD_BYTES + 128
    {
        return Err(StreamError::InvalidArgument);
    }
    let session = Session {
        provider,
        publications,
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
        #[cfg(test)]
        head_visits: 0,
    };
    Ok(Box::pin(stream::try_unfold(
        session,
        |mut session| async move {
            loop {
                if let Some(frame) = session.controls.pop_front() {
                    return Ok(Some((frame, session)));
                }
                if let Some(frame) = session.ready()? {
                    return Ok(Some((frame, session)));
                }
                if session.sources.is_empty() && session.terminals.is_empty() {
                    let Some(frame) = session.incoming.next().await else {
                        return Ok(None);
                    };
                    session.receive(frame?)?;
                    continue;
                }
                // Alternate deterministic priority. This bounds ready-side work
                // and lets the model explore the actual wakeup path without RNG.
                let input = session.incoming.next();
                let room = policy::room(
                    session.limits.pending_frames,
                    session.limits.pending_bytes,
                    session.pending_frames,
                    session.pending_bytes,
                    DATA_FRAME_BYTES,
                    session.credit.frames > 0 && session.credit.bytes > 0,
                );
                let source = session
                    .sources
                    .next_with_control(&mut session.terminals, room);
                let event = if session.prefer_control {
                    match future::select(input, source).await {
                        future::Either::Left((input, _)) => future::Either::Left(input),
                        future::Either::Right((source, _)) => future::Either::Right(source),
                    }
                } else {
                    match future::select(source, input).await {
                        future::Either::Left((source, _)) => future::Either::Right(source),
                        future::Either::Right((input, _)) => future::Either::Left(input),
                    }
                };
                session.prefer_control = !session.prefer_control;
                match event {
                    future::Either::Left(Some(frame)) => session.receive(frame?)?,
                    future::Either::Left(None) => return Ok(None),
                    future::Either::Right(source) => {
                        if let Some((channel, event)) = source? {
                            session.record(channel, event)?;
                        }
                    }
                }
            }
        },
    )))
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
    fn receive(&mut self, input: wire::SessionFrame) -> Result<(), StreamError> {
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
        if input.channel_id != 0 && matches!(body, Body::Window(_) | Body::Ack(_)) {
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
