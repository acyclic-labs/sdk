//! One event-loop owner of demand; the platform pool owns every wire identity.
use super::{ClientInterests, Error, Result};
use crate::task_volume::Cursor;
use acyclic_stream::{BoxProviderStream, StreamError, StreamPath, session::Client, wire};
use futures::{
    StreamExt as _, TryStreamExt as _,
    stream::{self, AbortHandle, Abortable, SelectAll},
};
use std::{cell::RefCell, collections::BTreeMap, rc::Rc};

type Frames = BoxProviderStream<'static, std::result::Result<wire::SessionFrame, StreamError>>;
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
    agents: BTreeMap<StreamPath, Content>,
    content: BTreeMap<StreamPath, Content>,
    sources: SelectAll<futures::stream::LocalBoxStream<'static, Routed>>,
}

impl Follows {
    pub fn new(session: Client, discovery: Frames) -> Self {
        let mut follows = Self {
            session,
            agents: BTreeMap::new(),
            content: BTreeMap::new(),
            sources: SelectAll::new(),
        };
        follows.source(None, false, discovery);
        follows
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

    fn subscribe(
        &mut self,
        agent: StreamPath,
        overview: bool,
        subscription: wire::SessionSubscription,
    ) -> Content {
        let session = self.session.clone();
        // All newly discovered agents register concurrently as the follow is
        // driven, without serial network round trips or another queue/pool.
        let frames =
            stream::once(async move { session.subscribe(subscription).await }).try_flatten();
        self.owned(agent, overview, Box::pin(frames))
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
        if self.agents.contains_key(&agent) {
            return Ok(false);
        }
        if self.agents.len() >= maximum {
            return Err(Error::Invalid("swarm agent bound reached".into()));
        }
        let subscription = follow(&agent, "status", wire::SessionDelivery::Latest, None)?;
        let status = self.subscribe(agent.clone(), true, subscription);
        self.agents.insert(agent, status);
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
        self.agents.contains_key(agent)
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
        channel: None,
        resume: Some(wire::SessionCursor {
            next: cursor.map_or(0, |cursor| cursor.position),
            opaque: Default::default(),
        }),
        kind: Some(wire::session_subscription::Kind::Follow(
            wire::SessionFollow {
                path: path.to_string(),
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
