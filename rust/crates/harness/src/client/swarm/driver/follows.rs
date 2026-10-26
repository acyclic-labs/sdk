//! One event-loop owner of demand; the platform pool owns every wire identity.
use super::{ClientInterests, Error, Result};
use crate::task_volume::Cursor;
use acyclic_stream::{BoxProviderStream, StreamError, StreamPath, session::Client, wire};
use futures::{
    StreamExt as _, TryStreamExt as _,
    stream::{self, AbortHandle, Abortable, SelectAll},
};
use std::{
    cell::RefCell,
    collections::{BTreeMap, BTreeSet},
    rc::Rc,
};

pub(super) type Frames =
    BoxProviderStream<'static, std::result::Result<wire::SessionFrame, StreamError>>;
struct Content {
    frames: Rc<RefCell<Option<Frames>>>,
    abort: AbortHandle,
}
pub(super) struct Routed {
    pub agent: Option<StreamPath>,
    pub overview: bool,
    pub frame: std::result::Result<wire::SessionFrame, StreamError>,
}

pub(super) struct Follows {
    session: Client,
    agents: BTreeSet<StreamPath>,
    content: BTreeMap<StreamPath, Content>,
    sources: SelectAll<futures::stream::LocalBoxStream<'static, Routed>>,
    status: Option<wire::SessionSubscription>,
}

impl Follows {
    pub fn new(session: Client, root: &StreamPath) -> Result<Self> {
        let mut follows = Self {
            session,
            agents: BTreeSet::new(),
            content: BTreeMap::new(),
            sources: SelectAll::new(),
            status: Some(follow(root, "status", wire::SessionDelivery::Latest, None)?),
        };
        // The routed set never ends while the driver owns it.
        follows.source(None, false, Box::pin(stream::pending()));
        Ok(follows)
    }

    /// Opens the shared status follow once. The first admitted member proves
    /// the stream exists: an agent's first status precedes its membership.
    pub fn status(&mut self) {
        if let Some(subscription) = self.status.take() {
            let session = self.session.clone();
            let frames =
                stream::once(async move { session.subscribe(subscription).await }).try_flatten();
            self.source(None, true, Box::pin(frames));
        }
    }

    fn source(
        &mut self,
        agent: Option<StreamPath>,
        overview: bool,
        frames: futures::stream::LocalBoxStream<
            'static,
            std::result::Result<wire::SessionFrame, StreamError>,
        >,
    ) -> AbortHandle {
        let (abort, registration) = AbortHandle::new_pair();
        let frames = frames.chain(stream::once(async { Err(StreamError::Unavailable) }));
        self.sources
            .push(Box::pin(Abortable::new(frames, registration).map(
                move |frame| Routed {
                    agent: agent.clone(),
                    overview,
                    frame,
                },
            )));
        abort
    }

    pub fn discover(
        &mut self,
        demand: &ClientInterests,
        agent: StreamPath,
        maximum: usize,
    ) -> Result<bool> {
        if demand.tier(&agent).is_none() || &agent == demand.root() {
            return Err(Error::Invalid("discovered agent lies outside swarm".into()));
        }
        if self.agents.contains(&agent) {
            return Ok(false);
        }
        if self.agents.len() >= maximum {
            return Err(Error::Invalid("swarm agent bound reached".into()));
        }
        self.agents.insert(agent);
        Ok(true)
    }

    pub fn content(&mut self, agent: StreamPath, cursor: &Cursor) -> Result<()> {
        let subscription = follow(&agent, "files", wire::SessionDelivery::All, Some(cursor))?;
        let session = self.session.clone();
        let frames: Frames = Box::pin(
            stream::once(async move { session.subscribe(subscription).await }).try_flatten(),
        );
        self.remove(&agent);
        let content = self.owned(agent.clone(), false, frames);
        self.content.insert(agent, content);
        Ok(())
    }

    fn owned(&mut self, agent: StreamPath, overview: bool, frames: Frames) -> Content {
        // Each source has a removable owner. Taking it drops the
        // pool cursor immediately, before any replacement can register. The
        // wrapper and this map are polled exclusively by this event loop.
        let frames = Rc::new(RefCell::new(Some(frames)));
        let reader = frames.clone();
        let source = stream::poll_fn(move |cx| match reader.borrow_mut().as_mut() {
            Some(frames) => frames.as_mut().poll_next(cx),
            None => std::task::Poll::Ready(None),
        });
        let abort = self.source(Some(agent), overview, Box::pin(source));
        Content { frames, abort }
    }

    pub fn remove(&mut self, agent: &StreamPath) {
        self.content.remove(agent);
    }

    pub fn retire(&mut self, agent: &StreamPath) {
        self.remove(agent);
        self.agents.remove(agent);
    }
    pub fn contains(&self, agent: &StreamPath) -> bool {
        self.agents.contains(agent)
    }

    pub async fn next(&mut self) -> Option<Routed> {
        self.sources.next().await
    }
}

fn follow(
    agent: &StreamPath,
    suffix: &str,
    delivery: wire::SessionDelivery,
    cursor: Option<&Cursor>,
) -> Result<wire::SessionSubscription> {
    let path = StreamPath::new(format!("{}/{suffix}", agent.as_str()))
        .map_err(|error| Error::Invalid(error.to_string()))?;
    Ok(wire::SessionSubscription {
        credential: 0,
        filter: None,
        channel: None,
        resume: Some(wire::SessionCursor {
            next: cursor.map_or(0, |cursor| cursor.position),
            opaque: Default::default(),
        }),
        kind: Some(wire::session_subscription::Kind::Follow(
            wire::SessionFollow {
                resource: acyclic_stream::wire_codec::session::stream_resource(path.as_str()),
                delivery: delivery as i32,
                preceding_commit_id: cursor
                    .filter(|cursor| cursor.position > 0)
                    .map_or_else(Default::default, |cursor| cursor.commit.to_vec().into()),
            },
        )),
    })
}

impl Drop for Content {
    fn drop(&mut self) {
        // Owner-thread only: release the pool cursor before admitting a replacement.
        self.frames.borrow_mut().take();
        self.abort.abort();
    }
}

#[cfg(test)]
#[cfg(all(not(acyclic_harness_loom), not(target_arch = "wasm32")))]
mod tests;
