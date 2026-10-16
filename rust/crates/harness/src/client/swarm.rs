//! File-driven interest orchestration over the platform's multiplexed session.

use super::{ClientInterests, interest_files::InterestFiles};
use crate::{
    Error, Result,
    task_volume::{HeadSnapshot, TaskVolume},
};
use acyclic_stream::{
    ChildrenPageRequest, CommitId, StreamError, StreamPath, StreamProvider, wire,
    wire_codec::session::record_cursor,
};
use futures::{FutureExt as _, Sink, SinkExt as _, Stream, StreamExt as _};
use std::{collections::BTreeMap, future::Future};

/// Admission bounds, independent of how many agents are currently rendered.
#[derive(Clone, Copy, Debug)]
pub struct SwarmClientLimits {
    /// Whole-swarm discovery and status capacity.
    pub agents: usize,
    /// Combined visible and prefetched agents.
    pub selected: usize,
    /// Combined layout and open-view document bytes.
    pub layout_bytes: usize,
    /// Maximum command/workspace files in the config query, independent of agents.
    pub config_files: usize,
    /// Maximum logical files in each selected snapshot.
    pub snapshot_files: usize,
}

/// Inputs for the shared ClientViews fold; no renderer or second wire schema.
pub enum SwarmUpdate {
    /// Rendering demand changed; prefetch remains followed without rendering.
    Layout(ClientInterests),
    /// Exact initial heads; subsequent content starts at this snapshot's cursor.
    Snapshot {
        /// Selected agent identity.
        agent: StreamPath,
        /// Zero-copy materialized files at C.
        snapshot: HeadSnapshot,
    },
    /// Platform-delivered status or full-fidelity content.
    Data {
        /// Source agent.
        agent: StreamPath,
        /// True for the always-on latest-value status subscription.
        overview: bool,
        /// The exact platform data, including cursor and durability watermarks.
        data: wire::SessionData,
    },
    /// Standalone durability progress, even when no later content arrives.
    Watermark {
        /// Source agent.
        agent: StreamPath,
        /// Whether the source is the always-on status follow.
        overview: bool,
        /// Exact pushed provider watermarks; this does not consume another record.
        watermark: wire::SessionWatermark,
    },
}

/// Client orchestration, using only the filesystem reader and Stream provider.
/// Agent children have `status` and `files` streams. Platform session I/O owns
/// connection placement, credit, acknowledgements and resume; this owns demand.
pub struct SwarmClient {
    root: StreamPath,
    limits: SwarmClientLimits,
}

struct Agent {
    content: Option<u64>,
}

struct Interests {
    revision: u64,
    next_channel: u64,
    agents: BTreeMap<StreamPath, Agent>,
    routes: BTreeMap<u64, (StreamPath, bool)>,
    demand: ClientInterests,
}

impl Interests {
    fn update(&self) -> Result<wire::SessionInterest> {
        let revision = self
            .revision
            .checked_add(1)
            .ok_or_else(|| Error::Invalid("interest revision exhausted".into()))?;
        Ok(wire::SessionInterest {
            revision,
            ..Default::default()
        })
    }

    async fn send<S: Sink<wire::SessionInterest, Error = Error> + Unpin>(
        &mut self,
        session: &mut S,
        update: wire::SessionInterest,
    ) -> Result<()> {
        if !update.upsert.is_empty() || !update.remove.is_empty() {
            let revision = update.revision;
            session.send(update).await?;
            self.revision = revision;
        }
        Ok(())
    }

    fn channel(&mut self) -> Result<u64> {
        let channel = self.next_channel;
        self.next_channel = self
            .next_channel
            .checked_add(1)
            .ok_or_else(|| Error::Invalid("session channel identity exhausted".into()))?;
        Ok(channel)
    }

    fn discover(
        &mut self,
        path: StreamPath,
        maximum: usize,
        update: &mut wire::SessionInterest,
    ) -> Result<()> {
        if self.demand.tier(&path).is_none() || path == *self.demand.root() {
            return Err(Error::Invalid("discovered agent lies outside swarm".into()));
        }
        if self.agents.contains_key(&path) {
            return Ok(());
        }
        if self.agents.len() >= maximum {
            return Err(Error::Invalid("swarm agent bound reached".into()));
        }
        StreamPath::new(format!("{}/status", path.as_str()))
            .map_err(|error| Error::Invalid(error.to_string()))?;
        let channel = self.channel()?;
        update.upsert.push(follow(
            channel,
            &path,
            "status",
            wire::SessionDelivery::Latest,
            wire::SessionCursor::default(),
        ));
        self.routes.insert(channel, (path.clone(), true));
        self.agents.insert(path, Agent { content: None });
        Ok(())
    }
}

fn follow(
    channel_id: u64,
    agent: &StreamPath,
    suffix: &str,
    delivery: wire::SessionDelivery,
    resume: wire::SessionCursor,
) -> wire::SessionSubscription {
    wire::SessionSubscription {
        channel: Some(wire::SessionChannel {
            channel_id,
            generation: 1,
        }),
        resume: Some(resume),
        kind: Some(wire::session_subscription::Kind::Follow(
            wire::SessionFollow {
                path: format!("{}/{suffix}", agent.as_str()),
                delivery: delivery as i32,
                ..Default::default()
            },
        )),
    }
}

impl SwarmClient {
    /// Binds authenticated providers; construction opens no connection or task.
    pub fn new(root: StreamPath, limits: SwarmClientLimits) -> Result<Self> {
        if limits.agents == 0
            || limits.selected > limits.agents
            || limits.layout_bytes == 0
            || limits.config_files == 0
            || limits.snapshot_files == 0
        {
            return Err(Error::Invalid("invalid swarm client bounds".into()));
        }
        // Validate the namespace before constructing any subscriptions.
        StreamPath::new(format!("{}/agent/files", root.as_str()))
            .map_err(|error| Error::Invalid(error.to_string()))?;
        Ok(Self { root, limits })
    }

    /// Runs one config-volume query and one multiplexed Streams session. `streams`
    /// enumerates the complete hierarchy cut; the session retains live discovery.
    /// `files`
    /// holds workspace/{layout,open-views}.json in the user's config volume. The platform
    /// session sink accepts canonical interest diffs; incoming data is already
    /// credit-admitted/fenced by that same platform session, never a harness socket.
    /// `heads` executes the provider's bounded root query on `<agent>/files`;
    /// it reuses the query owner's materialized index rather than replaying agent
    /// history into a second client index.
    /// Finite sources fail closed rather than returning success with stale views.
    pub async fn follow<D, P, S, I, Q, QFut, F, Fut>(
        &self,
        streams: &D,
        files: &TaskVolume<P>,
        mut session: S,
        incoming: I,
        mut heads: Q,
        mut publish: F,
    ) -> Result<()>
    where
        D: StreamProvider + ?Sized,
        P: StreamProvider + ?Sized,
        S: Sink<wire::SessionInterest, Error = Error> + Unpin,
        I: Stream<Item = Result<wire::SessionFrame>>,
        Q: FnMut(StreamPath, usize) -> QFut,
        QFut: Future<Output = crate::task_volume::Result<HeadSnapshot>>,
        F: FnMut(SwarmUpdate) -> Fut,
        Fut: Future<Output = Result<()>>,
    {
        let mut state = Interests {
            revision: 0,
            next_channel: 2,
            agents: BTreeMap::new(),
            routes: BTreeMap::new(),
            demand: ClientInterests::from_layout(self.root.clone(), [], [], 0)?,
        };
        let mut initial = state.update()?;
        initial.upsert.push(wire::SessionSubscription {
            channel: Some(wire::SessionChannel {
                channel_id: 1,
                generation: 1,
            }),
            kind: Some(wire::session_subscription::Kind::Children(
                wire::ChildrenRequest {
                    parent: Some(self.root.as_str().into()),
                    limit: acyclic_stream::MAX_ITEMS as u32,
                },
            )),
            ..Default::default()
        });
        state.send(&mut session, initial).await?;
        // Subscribe before enumerating: the platform queues subsequent child
        // changes while we acquire every page of one hierarchy cut. Duplicate
        // children from the subscription and snapshot share the same fold.
        let mut after = None;
        let mut hierarchy_version = None;
        // At most two complete admitted traversals, counting failed requests too.
        // Restarts deduplicate routes and cannot turn global hierarchy churn into
        // quadratic work or an unbounded retry loop.
        let mut remaining = self
            .limits
            .agents
            .checked_mul(2)
            .ok_or_else(|| Error::Invalid("swarm discovery bound overflow".into()))?;
        loop {
            if remaining == 0 {
                return Err(Error::Storage(
                    "swarm discovery work bound reached; reobserve".into(),
                ));
            }
            let page = match streams
                .children_page(ChildrenPageRequest {
                    parent: Some(self.root.clone()),
                    after,
                    hierarchy_version,
                    limit: remaining.min(acyclic_stream::MAX_ITEMS) as u32,
                })
                .await
            {
                Ok(page) => page,
                Err(StreamError::HierarchyChanged) => {
                    remaining -= 1;
                    after = None;
                    hierarchy_version = None;
                    continue;
                }
                Err(error) => return Err(Error::Storage(error.to_string())),
            };
            remaining = remaining
                .checked_sub(page.children.len().max(1))
                .ok_or_else(|| {
                    Error::Storage("swarm discovery exceeded requested page bound".into())
                })?;
            let mut update = state.update()?;
            for child in page.children {
                state.discover(child.path, self.limits.agents, &mut update)?;
            }
            state.send(&mut session, update).await?;
            let Some(next) = page.next_after else {
                break;
            };
            after = Some(next);
            hierarchy_version = Some(page.hierarchy_version);
        }
        let mut files = InterestFiles::observe(
            files,
            self.root.clone(),
            self.limits.selected,
            self.limits.layout_bytes,
            self.limits.config_files,
        )
        .await?;
        let incoming = incoming.fuse();
        futures::pin_mut!(incoming);
        loop {
            futures::select! {
                demand = files.next().fuse() => {
                    let Some(demand) = demand? else { continue; };
                    let diff = demand.changes(&state.demand)?;
                    let removed: Vec<_> = diff.unsubscribe().cloned().collect();
                    let added: Vec<_> = diff.subscribe().cloned().collect();
                    let mut update = state.update()?;
                    for path in removed {
                        if let Some(channel) = state.agents.get_mut(&path).and_then(|agent| agent.content.take()) {
                            update.remove.push(wire::SessionChannel { channel_id: channel, generation: 1 });
                            state.routes.remove(&channel);
                        }
                    }
                    for path in added {
                        state.discover(path.clone(), self.limits.agents, &mut update)?;
                        let snapshot = heads(path.clone(), self.limits.snapshot_files).await
                            .map_err(|error| Error::Storage(error.to_string()))?;
                        let resume = record_cursor(snapshot.cursor.position, CommitId::from_bytes(snapshot.cursor.commit))
                            .map_err(|error| Error::Storage(error.to_string()))?;
                        publish(SwarmUpdate::Snapshot { agent: path.clone(), snapshot }).await?;
                        let channel = state.channel()?;
                        update.upsert.push(follow(channel, &path, "files", wire::SessionDelivery::All, resume));
                        state.agents.get_mut(&path).expect("discovered agent").content = Some(channel);
                        state.routes.insert(channel, (path, false));
                    }
                    state.send(&mut session, update).await?;
                    publish(SwarmUpdate::Layout(demand.clone())).await?;
                    state.demand = demand;
                },
                frame = incoming.next() => {
                    let frame = frame.ok_or_else(|| Error::Storage("platform session ended".into()))??;
                    // Placement/reconnect generations belong to the platform
                    // session. Its incoming iterator has already fenced stale
                    // frames; channel identities here are never reused.
                    if let Some(wire::session_frame::Body::Closed(closed)) = &frame.body {
                        if frame.channel_id == 0 || frame.channel_id == 1 || state.routes.contains_key(&frame.channel_id) {
                            return Err(Error::Storage(format!("platform subscription {} closed: {}: {}", frame.channel_id, closed.code, closed.message)));
                        }
                    }
                    if let Some(wire::session_frame::Body::Watermark(watermark)) = &frame.body {
                        if let Some((agent, overview)) = state.routes.get(&frame.channel_id) {
                            publish(SwarmUpdate::Watermark { agent: agent.clone(), overview: *overview, watermark: *watermark }).await?;
                        }
                    }
                    if let Some(wire::session_frame::Body::Data(data)) = frame.body {
                        if frame.channel_id == 1 {
                            let Some(wire::session_data::Item::Child(child)) = data.item else {
                                return Err(Error::Invalid("discovery received non-child data".into()));
                            };
                            let path = StreamPath::new(child.path).map_err(|error| Error::Invalid(error.to_string()))?;
                            let mut update = state.update()?;
                            state.discover(path, self.limits.agents, &mut update)?;
                            state.send(&mut session, update).await?;
                        } else if let Some((agent, overview)) = state.routes.get(&frame.channel_id) {
                            publish(SwarmUpdate::Data { agent: agent.clone(), overview: *overview, data }).await?;
                        }
                    }
                },
            }
        }
    }
}
