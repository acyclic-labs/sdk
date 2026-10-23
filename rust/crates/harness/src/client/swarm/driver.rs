//! Drives volume demand and platform session follows with one owner.
use super::{SwarmClientLimits, SwarmUpdate};
use crate::{
    Error, Result,
    client::{
        ClientInterests, ClientViewHandle, ClientViewLimits, ClientViews, SwarmChange,
        interest_files::InterestFiles,
    },
    task_volume::{HeadSnapshot, TaskVolume},
};
use acyclic_stream::{StreamPath, StreamProvider, wire};
use futures::FutureExt as _;
use std::future::Future;
mod follows;
mod membership;
mod snapshots;

/// Client orchestration, using only the filesystem reader and Stream provider.
/// Agent children have `status` and `files` streams. Platform session I/O owns
/// connection placement, credit, acknowledgements and resume; this owns demand.
pub struct SwarmClient {
    root: StreamPath,
    limits: SwarmClientLimits,
    views: ClientViewHandle,
}

impl SwarmClient {
    /// Binds one disposable renderer cache to authoritative volume queries.
    pub fn new(
        root: StreamPath,
        limits: SwarmClientLimits,
        view_limits: ClientViewLimits,
    ) -> Result<Self> {
        limits.validate(&view_limits)?;
        StreamPath::new(format!("{}/agent/files", root.as_str()))
            .map_err(|error| Error::Invalid(error.to_string()))?;
        let views = ClientViewHandle::new(ClientViews::new(root.clone(), view_limits)?);
        Ok(Self {
            root,
            limits,
            views,
        })
    }

    /// A read-only renderer handle; provider I/O never borrows its views.
    pub fn views(&self) -> ClientViewHandle {
        self.views.clone()
    }

    /// Follows file-derived demand through the caller's shared Streams session.
    /// Children already provides snapshot plus live follow across every hierarchy
    /// page, so discovery needs no second enumeration or harness channel map.
    /// Status stays subscribed for every agent. Selected content starts from the
    /// complete checked cursor returned by `heads`, and promotion retains it.
    /// A fatal source or renderer error ends this cache; reset from fresh queries.
    pub async fn follow<P, M, Q, QFut, F>(
        self,
        files: &TaskVolume<P>,
        parent: &TaskVolume<M>,
        session: &acyclic_stream::session::Client,
        mut heads: Q,
        mut changed: F,
    ) -> Result<()>
    where
        P: StreamProvider + ?Sized,
        M: StreamProvider + ?Sized,
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
                credential: 0,
                filter: None,
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
        let mut membership =
            membership::Membership::observe(parent, session, self.root.clone(), self.limits.agents)
                .await?;
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
            let admission = futures::select_biased! {
                completed = snapshots.next().fuse() => {
                    if let Some((agent, snapshot)) = snapshots.finish(completed) {
                        let snapshot = snapshot.map_err(|error| Error::Storage(error.to_string()))?;
                        follows.content(agent.clone(), &snapshot.cursor)?;
                        publish(SwarmUpdate::Snapshot { agent, snapshot }).await?;
                    }
                    None
                },
                member = membership.next().fuse() => {
                    match member? {
                        Some((agent, true)) => Some(agent),
                        Some((agent, false)) => {
                            snapshots.retire(&agent)?;
                            follows.retire(&agent);
                            publish(SwarmUpdate::Retired { agent }).await?;
                            None
                        }
                        None => None,
                    }
                },
                next = files.next().fuse() => {
                    let Some(next) = next? else { continue; };
                    let diff = next.changes(&demand)?;
                    for removed in diff.unsubscribe() { follows.remove(removed); }
                    snapshots.layout(&next)?;
                    let added: Vec<_> = diff.subscribe().cloned().collect();
                    publish(SwarmUpdate::Layout(next.clone())).await?;
                    demand = next;
                    for agent in added {
                        if !follows.contains(&agent) { continue; }
                        let pending = heads(agent.clone(), self.limits.snapshot_files);
                        snapshots.start(agent, pending)?;
                    }
                    None
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
                            // Stream subtrees retain archived history. Only the
                            // parent membership files can admit or retire a row.
                            if demand.tier(&agent).is_none() {
                                return Err(Error::Invalid("discovered agent lies outside swarm".into()));
                            }
                            membership.discovered(&agent).then_some(agent)
                        }
                        (Some(agent), Some(wire::session_frame::Body::Data(data))) => {
                            publish(SwarmUpdate::Data { agent, overview: routed.overview, data }).await?;
                            None
                        }
                        (Some(agent), Some(wire::session_frame::Body::Watermark(watermark))) => {
                            publish(SwarmUpdate::Watermark { agent, overview: routed.overview, watermark }).await?;
                            None
                        }
                        (_, Some(wire::session_frame::Body::Closed(closed))) => {
                            return Err(Error::Storage(format!("swarm subscription closed: {}: {}", closed.code, closed.message)));
                        }
                        _ => None
                    }
                },
            };
            // Snapshot children and live membership records share admission.
            // Both arrive through the credit-paced platform session above.
            if let Some(agent) = admission {
                if follows.discover(&demand, agent.clone(), self.limits.agents)? {
                    publish(SwarmUpdate::Agent {
                        agent: agent.clone(),
                    })
                    .await?;
                    if demand
                        .tier(&agent)
                        .is_some_and(|tier| tier != crate::client::InterestTier::Overview)
                    {
                        snapshots.start(agent.clone(), heads(agent, self.limits.snapshot_files))?;
                    }
                }
            }
        }
    }
}
