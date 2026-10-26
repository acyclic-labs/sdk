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
/// Agent children have `files` streams; one root `status` stream multiplexes
/// their opaque latest values. Platform session I/O owns
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
    /// The parent's settled membership files are the only discovery source: the
    /// snapshot admits one agent per turn, then live membership records follow.
    /// One status follow covers every agent. Selected content starts from the
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
        let Sources {
            mut membership,
            mut follows,
            mut demand,
            mut files,
        } = Sources::open(self.root, self.limits, files, parent, session).await?;
        let mut snapshots = snapshots::Snapshots::new(self.limits.selected);
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
                        (None, Some(wire::session_frame::Body::Data(mut data))) if routed.overview => {
                            let Some(agent) = status_member(parent, &demand, &mut membership, &follows, &mut data).await? else { continue; };
                            admit_agent(agent.clone(), &demand, &mut follows, &mut snapshots, self.limits, &mut heads, &mut publish).await?;
                            publish(SwarmUpdate::Data { agent, overview: true, data }).await?;
                            None
                        }
                        (None, Some(wire::session_frame::Body::Watermark(watermark))) if routed.overview => {
                            publish(SwarmUpdate::StatusWatermark(watermark)).await?;
                            None
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
            // Snapshot members and live membership records share admission.
            if let Some(agent) = admission {
                admit_agent(
                    agent,
                    &demand,
                    &mut follows,
                    &mut snapshots,
                    self.limits,
                    &mut heads,
                    &mut publish,
                )
                .await?;
            }
        }
    }
}

// Status and membership channels may arrive in either order. The parent
// remains authoritative: archived status history never re-admits an agent.
async fn status_member<P: StreamProvider + ?Sized>(
    parent: &TaskVolume<P>,
    demand: &ClientInterests,
    membership: &mut membership::Membership,
    follows: &follows::Follows,
    data: &mut wire::SessionData,
) -> Result<Option<StreamPath>> {
    let agent = super::status_agent(data)?;
    if agent == *demand.root() || demand.tier(&agent).is_none() {
        return Err(Error::Invalid("status agent lies outside swarm".into()));
    }
    if !follows.contains(&agent)
        && !membership.discovered(&agent)
        && !membership::active(parent, demand.root(), &agent).await?
    {
        return Ok(None);
    }
    Ok(Some(agent))
}

struct Sources {
    membership: membership::Membership,
    follows: follows::Follows,
    demand: ClientInterests,
    files: InterestFiles,
}
impl Sources {
    async fn open<P: StreamProvider + ?Sized, M: StreamProvider + ?Sized>(
        root: StreamPath,
        limits: SwarmClientLimits,
        files: &TaskVolume<P>,
        parent: &TaskVolume<M>,
        session: &acyclic_stream::session::Client,
    ) -> Result<Self> {
        let membership =
            membership::Membership::observe(parent, session, root.clone(), limits.agents).await?;
        let follows = follows::Follows::new(session.clone(), &root)?;
        let demand = ClientInterests::from_layout(root.clone(), [], [], 0)?;
        let files = InterestFiles::observe(
            files,
            root,
            limits.selected,
            limits.layout_bytes,
            limits.config_files,
        )
        .await?;
        Ok(Self {
            membership,
            follows,
            demand,
            files,
        })
    }
}

async fn admit_agent<'a, Q, QFut, F>(
    agent: StreamPath,
    demand: &ClientInterests,
    follows: &mut follows::Follows,
    snapshots: &mut snapshots::Snapshots<'a>,
    limits: SwarmClientLimits,
    heads: &mut Q,
    publish: &mut F,
) -> Result<()>
where
    Q: FnMut(StreamPath, usize) -> QFut,
    QFut: Future<Output = crate::task_volume::Result<HeadSnapshot>> + 'a,
    F: AsyncFnMut(SwarmUpdate) -> Result<()>,
{
    if follows.discover(demand, agent.clone(), limits.agents)? {
        follows.status();
        publish(SwarmUpdate::Agent {
            agent: agent.clone(),
        })
        .await?;
        if demand
            .tier(&agent)
            .is_some_and(|tier| tier != crate::client::InterestTier::Overview)
        {
            snapshots.start(agent.clone(), heads(agent, limits.snapshot_files))?;
        }
    }
    Ok(())
}
