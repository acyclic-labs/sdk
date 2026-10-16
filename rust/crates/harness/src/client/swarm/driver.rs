//! Drives volume demand and platform session follows with one owner.
use super::{SwarmClientLimits, SwarmUpdate};
use crate::{
    Error, Result,
    client::{
        ClientInterests, SwarmChange, SwarmViewHandle, SwarmViewLimits, SwarmViews,
        interest_files::InterestFiles,
    },
    task_volume::{HeadSnapshot, TaskVolume},
};
use acyclic_stream::{StreamPath, StreamProvider, wire};
use futures::FutureExt as _;
use std::future::Future;
mod follows;
mod snapshots;

/// Client orchestration, using only the filesystem reader and Stream provider.
/// Agent children have `status` and `files` streams. Platform session I/O owns
/// connection placement, credit, acknowledgements and resume; this owns demand.
pub struct SwarmClient {
    root: StreamPath,
    limits: SwarmClientLimits,
    views: SwarmViewHandle,
}

impl SwarmClient {
    /// Binds one disposable renderer cache to authoritative volume queries.
    pub fn new(
        root: StreamPath,
        limits: SwarmClientLimits,
        view_limits: SwarmViewLimits,
    ) -> Result<Self> {
        if limits.agents == 0
            || limits.selected > limits.agents
            || limits.layout_bytes == 0
            || limits.config_files == 0
            || limits.snapshot_files == 0
            || view_limits.agents < limits.agents
            || view_limits.selected < limits.selected
        {
            return Err(Error::Invalid("invalid swarm client bounds".into()));
        }
        StreamPath::new(format!("{}/agent/files", root.as_str()))
            .map_err(|error| Error::Invalid(error.to_string()))?;
        let views = SwarmViewHandle::new(SwarmViews::new(root.clone(), view_limits)?);
        Ok(Self {
            root,
            limits,
            views,
        })
    }

    /// A read-only renderer handle; provider I/O never borrows its views.
    pub fn views(&self) -> SwarmViewHandle {
        self.views.clone()
    }

    /// Follows file-derived demand through the caller's shared Streams session.
    /// Children already provides snapshot plus live follow across every hierarchy
    /// page, so discovery needs no second enumeration or harness channel map.
    /// Status stays subscribed for every agent. Selected content starts from the
    /// complete checked cursor returned by `heads`, and promotion retains it.
    /// A fatal source or renderer error ends this cache; reset from fresh queries.
    pub async fn follow<P, Q, QFut, F>(
        self,
        files: &TaskVolume<P>,
        session: &acyclic_stream::session::Client,
        mut heads: Q,
        mut changed: F,
    ) -> Result<()>
    where
        P: StreamProvider + ?Sized,
        Q: FnMut(StreamPath, usize) -> QFut,
        QFut: Future<Output = crate::task_volume::Result<HeadSnapshot>>,
        F: AsyncFnMut(SwarmChange) -> Result<()>,
    {
        let views = self.views.clone();
        let mut publish = async |update| {
            let change = views.apply(update)?;
            changed(change).await
        };
        // Registration precedes reading the layout. The platform owns the
        // hierarchy snapshot, live creation cursor, credit and reconnect fence.
        let discovery = session
            .subscribe(wire::SessionSubscription {
                channel: None,
                resume: Some(wire::SessionCursor::default()),
                kind: Some(wire::session_subscription::Kind::Children(
                    wire::ChildrenRequest {
                        parent: Some(self.root.as_str().into()),
                        limit: acyclic_stream::MAX_ITEMS as u32,
                    },
                )),
            })
            .await
            .map_err(|error| Error::Storage(error.to_string()))?;
        let mut follows = follows::Follows::new(session.clone(), discovery);
        let mut demand = ClientInterests::from_layout(self.root.clone(), [], [], 0)?;
        let mut snapshots = snapshots::Snapshots::new(self.limits.selected);
        let mut files = InterestFiles::observe(
            files,
            self.root,
            self.limits.selected,
            self.limits.layout_bytes,
            self.limits.config_files,
        )
        .await?;
        loop {
            futures::select! {
                next = files.next().fuse() => {
                    let Some(next) = next? else { continue; };
                    let diff = next.changes(&demand)?;
                    for removed in diff.unsubscribe() { follows.remove(removed); }
                    snapshots.layout(&next)?;
                    let added: Vec<_> = diff.subscribe().cloned().collect();
                    publish(SwarmUpdate::Layout(next.clone())).await?;
                    demand = next;
                    for agent in added {
                        if follows.discover(&demand, agent.clone(), self.limits.agents)? {
                            publish(SwarmUpdate::Agent { agent: agent.clone() }).await?;
                        }
                        let pending = heads(agent.clone(), self.limits.snapshot_files);
                        snapshots.start(agent, pending)?;
                    }
                },
                completed = snapshots.next().fuse() => {
                    if let Some((agent, snapshot)) = snapshots.finish(completed) {
                        let snapshot = snapshot.map_err(|error| Error::Storage(error.to_string()))?;
                        follows.content(agent.clone(), &snapshot.cursor)?;
                        publish(SwarmUpdate::Snapshot { agent, snapshot }).await?;
                    }
                },
                routed = follows.next().fuse() => {
                    let routed = routed.ok_or_else(|| Error::Storage("swarm follows ended".into()))?;
                    let frame = routed.frame.map_err(|error| Error::Storage(error.to_string()))?;
                    match (routed.agent, frame.body) {
                        (None, Some(wire::session_frame::Body::Data(data))) => {
                            let Some(wire::session_data::Item::Child(child)) = data.item else {
                                return Err(Error::Invalid("discovery received non-child data".into()));
                            };
                            let agent = StreamPath::new(child.path).map_err(|error| Error::Invalid(error.to_string()))?;
                            if follows.discover(&demand, agent.clone(), self.limits.agents)? {
                                publish(SwarmUpdate::Agent { agent }).await?;
                            }
                        }
                        (Some(agent), Some(wire::session_frame::Body::Data(data))) => {
                            publish(SwarmUpdate::Data { agent, overview: routed.overview, data }).await?;
                        }
                        (Some(agent), Some(wire::session_frame::Body::Watermark(watermark))) => {
                            publish(SwarmUpdate::Watermark { agent, overview: routed.overview, watermark }).await?;
                        }
                        (_, Some(wire::session_frame::Body::Closed(closed))) => {
                            return Err(Error::Storage(format!("swarm subscription closed: {}: {}", closed.code, closed.message)));
                        }
                        _ => {}
                    }
                },
            }
        }
    }
}
