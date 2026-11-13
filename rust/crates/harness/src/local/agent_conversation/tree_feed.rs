//! One event-driven multiplexer over each member's own source stream.
//! Discovery and source delivery share the ordinary authenticated session.
use super::*;
use acyclic_stream::{FollowRequest, Record, RecordStream, StreamPath, records_only};
use futures::{FutureExt as _, StreamExt as _, TryStreamExt as _, future::LocalBoxFuture};
use tokio::sync::watch;

pub(super) type MembershipUpdate = Option<(AgentPath, MembershipFollow)>;

/// An original record from one agent's own selected source stream.
pub struct TreeRecord {
    /// Original owner, established by the tree's committed membership.
    pub agent: AgentPath,
    /// Exact source path, without any inherited parent records.
    pub stream: StreamPath,
    /// Original sequence, contents and publication metadata.
    pub record: Record,
}

enum Update {
    Member(MembershipUpdate),
    Directory(AgentPath, DirectoryFollow),
    Opened(Option<MembershipFollow>, SourceFollow),
    Record(TreeRecord, SourceFollow),
}

type Job = LocalBoxFuture<'static, Result<Update>>;

/// Typed lifecycle/signal projection of the same general tree reader.
pub struct SupervisionFeed(TreeFeed);

/// One captured exclusive cut of a tree reader's root source. Descendant
/// discovery is independent; this cut never scans or waits for a whole tree.
pub struct RootCut {
    stream: StreamPath,
    through: u64,
}

impl Connection {
    /// Version-one supervision events for every session admitted by this
    /// platform owner. Single-tree platforms retain their admitted tree.
    pub async fn supervision_sessions(&self) -> Result<SupervisionFeed> {
        Ok(SupervisionFeed(
            self.follow_sessions(StreamPath::new("supervision").map_err(failed)?)
                .await?,
        ))
    }
    /// Version-one lifecycle and supervision events for this admitted tree.
    pub async fn supervision(&self) -> Result<SupervisionFeed> {
        Ok(SupervisionFeed(
            self.follow_tree(StreamPath::new("supervision").map_err(failed)?)
                .await?,
        ))
    }

    /// The selected agent's own events, without opening descendant sources.
    pub async fn supervision_own(&self) -> Result<SupervisionFeed> {
        Ok(SupervisionFeed(
            self.follow_sources(StreamPath::new("supervision").map_err(failed)?, false)
                .await?,
        ))
    }
}

impl SupervisionFeed {
    /// Point-select the current explicit owner decision, retaining its original
    /// event-stream cursor so a consumer can exclude historical controls.
    pub async fn owner_control(&self) -> Result<Option<(u64, crate::supervision::Event)>> {
        let mut records = match self
            .0
            .connection
            .session
            .read(acyclic_stream::ReadRequest {
                path: self.0.root_stream.clone(),
                from: 0,
                limit: 1,
                key: Some(acyclic_stream::ReadKeySeek::latest(
                    bytes::Bytes::from_static(crate::supervision::OWNER_CONTROL_KEY),
                    u64::MAX,
                )),
                at_least: Default::default(),
            })
            .await
        {
            Ok(records) => records,
            Err(acyclic_stream::StreamError::NotFound) => return Ok(None),
            Err(error) => return Err(failed(error)),
        };
        records
            .try_next()
            .await
            .map_err(failed)?
            .map(|record| {
                let event: crate::supervision::Event =
                    serde_json::from_slice(&record.value).map_err(failed)?;
                if event.version != 1
                    || event.agent != *self.0.connection.root()
                    || event.run.is_some()
                {
                    return Err(Error::Invalid("invalid owner control source".into()));
                }
                Ok((record.sequence, event))
            })
            .transpose()
    }
    /// Capture the root's current source cut through the normal session.
    pub async fn root_cut(&self) -> Result<RootCut> {
        self.0.root_cut().await
    }

    /// Whether this reader delivered every root event in the captured cut.
    pub fn reached(&self, cut: &RootCut) -> Result<bool> {
        self.0.reached(cut)
    }

    /// Wait for one original event and validate its owning source binding.
    pub async fn next(&mut self) -> Result<crate::supervision::Event> {
        Ok(self.next_record().await?.1)
    }

    /// Read the typed event with its original stream cursor, independent of
    /// the source fact the event references.
    pub async fn next_record(
        &mut self,
    ) -> Result<(crate::supervision::Position, crate::supervision::Event)> {
        let record = self.0.next().await?;
        let event: crate::supervision::Event =
            serde_json::from_slice(&record.record.value).map_err(failed)?;
        if event.version != 1 || event.agent != record.agent {
            return Err(Error::Invalid("invalid lifecycle event source".into()));
        }
        if let crate::supervision::Lifecycle::Signal { signal, .. } = &event.event
            && signal.version != 1
        {
            return Err(Error::Invalid(
                "unsupported supervision signal version".into(),
            ));
        }
        Ok((
            crate::supervision::Position {
                stream: record.stream,
                sequence: record.record.sequence,
            },
            event,
        ))
    }
}

/// A lazy live query over an admitted tree. It retains one membership and one
/// source continuation per subscribed agent, with no spawned tasks or scans.
/// Dropping it releases every follow; backpressure is the caller's `next`.
pub struct TreeFeed {
    connection: Connection,
    relative: StreamPath,
    jobs: futures::stream::FuturesUnordered<Job>,
    finished: watch::Sender<bool>,
    root_stream: StreamPath,
    root_next: u64,
    roots: std::collections::HashSet<AgentPath>,
}

impl Connection {
    /// Follow one relative source across independent sessions and their trees.
    /// Discovery follows the original placement directory, without paged scans,
    /// repeated selection, tasks or a second service. Root identities are retained
    /// only to avoid registering duplicate follows on ordinary attach events.
    pub async fn follow_sessions(&self, relative: StreamPath) -> Result<TreeFeed> {
        let files = Conversation::open(self.clone())
            .files(
                crate::client_files::FileRoot::Private {},
                vec!["status/session-directory.json".into()],
                None,
            )
            .await?;
        let feed = self.follow_tree(relative).await?;
        if let Some(file) = &files.files["status/session-directory.json"] {
            let selected: crate::client_files::SessionDirectory =
                serde_json::from_str(&file.text).map_err(failed)?;
            if selected.version != 1 {
                return Err(Error::Invalid(
                    "unsupported session directory version".into(),
                ));
            }
            // A placement can publish this descriptor to a tree-scoped client.
            // Check its original source using this observer's credential before
            // opening the optional directory follow. Tree observation remains
            // available without placement-wide authority.
            match feed
                .connection
                .session
                .heads(
                    acyclic_stream::HeadsRequest {
                        retained: Default::default(),
                        durable_through: None,
                        projection: acyclic_stream::HeadProjection::Tail,
                        paths: std::iter::once(selected.source.clone()).collect(),
                        children: None,
                        limit: 1,
                        at_least: Default::default(),
                    },
                    feed.connection.session.credential(),
                )
                .await
            {
                Ok(_) => {}
                Err(acyclic_stream::StreamError::AccessDenied) => return Ok(feed),
                Err(error) => return Err(failed(error)),
            }
            let records = records_only(
                feed.connection
                    .session
                    .follow(FollowRequest::new(selected.source, 0))
                    .await
                    .map_err(failed)?,
            );
            feed.jobs.push(directory(DirectoryFollow { records }));
        }
        Ok(feed)
    }
    /// Follow the same relative source stream of every committed tree member.
    /// Existing and newly created members arrive through their creation feed;
    /// absent sources wait on Streams rather than probing for readiness.
    /// Local and remote connections use this same session implementation.
    pub async fn follow_tree(&self, relative: StreamPath) -> Result<TreeFeed> {
        self.follow_sources(relative, true).await
    }

    async fn follow_sources(&self, relative: StreamPath, descendants: bool) -> Result<TreeFeed> {
        let (finished, _) = watch::channel(false);
        let feed = TreeFeed {
            connection: self.observation().await?,
            root_stream: StreamPath::new(format!("{}/{}", self.root(), relative))
                .map_err(failed)?,
            root_next: 0,
            roots: std::iter::once(self.root().clone()).collect(),
            relative,
            jobs: Default::default(),
            finished,
        };
        feed.jobs.push(node(
            feed.connection.clone(),
            feed.relative.clone(),
            None,
            descendants,
        ));
        Ok(feed)
    }
}

impl TreeFeed {
    /// Capture one named root head. The shared session supplies its monotone
    /// revision floor; the request buys a finite, causally admitted drain.
    /// It adds no subscription, readiness retry or background work.
    pub async fn root_cut(&self) -> Result<RootCut> {
        let heads = self
            .connection
            .session
            .heads(
                acyclic_stream::HeadsRequest {
                    retained: Default::default(),
                    durable_through: None,
                    projection: acyclic_stream::HeadProjection::Tail,
                    paths: std::iter::once(self.root_stream.clone()).collect(),
                    children: None,
                    limit: 1,
                    at_least: Default::default(),
                },
                self.connection.session.credential(),
            )
            .await
            .map_err(failed)?;
        let through = heads
            .heads
            .first()
            .and_then(Option::as_ref)
            .map_or(0, |head| head.tail);
        Ok(RootCut {
            stream: self.root_stream.clone(),
            through,
        })
    }

    /// Test this reader's delivered frontier against its original source cut.
    /// A cut from another source is refused rather than treated as caught up.
    pub fn reached(&self, cut: &RootCut) -> Result<bool> {
        if cut.stream != self.root_stream {
            return Err(Error::Invalid("root cut belongs to another source".into()));
        }
        Ok(self.root_next >= cut.through)
    }

    /// Wait for one original source record. Ending a live source is an error;
    /// an empty snapshot is never substituted for a lost producer.
    pub async fn next(&mut self) -> Result<TreeRecord> {
        while let Some(update) = self.jobs.next().await {
            match update? {
                Update::Directory(agent, follow) => {
                    self.jobs.push(directory(follow));
                    if self.roots.insert(agent.clone()) {
                        self.jobs.push(node(
                            self.connection.clone(),
                            self.relative.clone(),
                            Some(agent),
                            true,
                        ));
                    }
                }
                Update::Member(Some((agent, follow))) => {
                    let connection = follow.connection.clone();
                    self.jobs.push(member(follow, self.finished.subscribe()));
                    self.jobs
                        .push(node(connection, self.relative.clone(), Some(agent), true));
                }
                Update::Opened(members, records) => {
                    if let Some(members) = members {
                        self.jobs.push(member(members, self.finished.subscribe()));
                    }
                    self.jobs.push(source(records));
                }
                Update::Member(None) => {
                    return Err(Error::Storage("membership follow ended".into()));
                }
                Update::Record(record, follow) => {
                    if record.stream == self.root_stream {
                        self.root_next =
                            record.record.sequence.checked_add(1).ok_or_else(|| {
                                Error::Invalid("root source cursor overflows".into())
                            })?;
                    }
                    self.jobs.push(source(follow));
                    return Ok(record);
                }
            }
        }
        Err(Error::Storage("tree feed ended".into()))
    }
}

struct DirectoryFollow {
    records: RecordStream,
}

fn directory(mut follow: DirectoryFollow) -> Job {
    async move {
        loop {
            let record = follow
                .records
                .try_next()
                .await
                .map_err(failed)?
                .ok_or_else(|| Error::Storage("session directory follow ended".into()))?;
            match serde_json::from_slice::<crate::client_files::SessionDirectoryEntry>(
                &record.value,
            )
            .map_err(failed)?
            {
                crate::client_files::SessionDirectoryEntry::Created { version: 1 }
                    if record.sequence == 0 => {}
                crate::client_files::SessionDirectoryEntry::Root { version: 1, agent }
                    if record.sequence > 0 && agent.parent().is_none() =>
                {
                    return Ok(Update::Directory(agent, follow));
                }
                _ => return Err(Error::Invalid("invalid session directory record".into())),
            }
        }
    }
    .boxed_local()
}

// Binding is itself retained in the set, so cancellation of `next` cannot
// consume a membership event and lose that child's pending source admission.
fn node(
    connection: Connection,
    relative: StreamPath,
    selected: Option<AgentPath>,
    descendants: bool,
) -> Job {
    async move {
        let connection = match selected {
            Some(agent) => connection.select(agent).await?,
            None => connection,
        };
        let agent = connection.root().clone();
        let members = if descendants {
            Some(MembershipFollow::open(connection.clone(), agent.clone(), 0, false).await?)
        } else {
            None
        };
        let stream = StreamPath::new(format!("{agent}/{relative}")).map_err(failed)?;
        let mut request = FollowRequest::new(stream.clone(), 0);
        request.when_absent = acyclic_stream::WhenAbsent::Wait;
        let records = records_only(connection.session.follow(request).await.map_err(failed)?);
        Ok(Update::Opened(
            members,
            SourceFollow {
                agent,
                stream,
                records,
            },
        ))
    }
    .boxed_local()
}

fn member(follow: MembershipFollow, finished: watch::Receiver<bool>) -> Job {
    async move { follow.next(finished).await.map(Update::Member) }.boxed_local()
}

struct SourceFollow {
    agent: AgentPath,
    stream: StreamPath,
    records: RecordStream,
}

fn source(mut follow: SourceFollow) -> Job {
    async move {
        let record = follow
            .records
            .try_next()
            .await
            .map_err(failed)?
            .ok_or_else(|| Error::Storage("agent source follow ended".into()))?;
        Ok(Update::Record(
            TreeRecord {
                agent: follow.agent.clone(),
                stream: follow.stream.clone(),
                record,
            },
            follow,
        ))
    }
    .boxed_local()
}
pub(super) struct MembershipFollow {
    connection: Connection,
    agent: AgentPath,
    records: RecordStream,
    next: u64,
    bounded: bool,
}

impl MembershipFollow {
    pub(super) async fn open(
        connection: Connection,
        agent: AgentPath,
        next: u64,
        bounded: bool,
    ) -> Result<Self> {
        let mut request = FollowRequest::new(agent.membership().map_err(failed)?, next);
        request.until = bounded.then_some(u64::MAX);
        let records = records_only(connection.session.follow(request).await.map_err(failed)?);
        Ok(Self {
            connection,
            agent,
            records,
            next,
            bounded,
        })
    }

    pub(super) async fn next(
        mut self,
        mut finish: watch::Receiver<bool>,
    ) -> Result<MembershipUpdate> {
        loop {
            if !self.bounded && *finish.borrow() {
                self = Self::open(self.connection, self.agent, self.next, true).await?;
            }
            tokio::select! {
                biased;
                changed = finish.changed(), if !self.bounded => {
                    changed.map_err(failed)?;
                    self = Self::open(self.connection, self.agent, self.next, true).await?;
                }
                record = self.records.try_next() => {
                    let Some(record) = record.map_err(failed)? else {
                        return if self.bounded {
                            Ok(None)
                        } else {
                            Err(Error::Storage("membership follow ended".into()))
                        };
                    };
                    self.next = record.sequence + 1;
                    let member: crate::agent::Membership =
                        serde_json::from_slice(&record.value).map_err(failed)?;
                    if member.version != 1 || (member.agent != self.agent
                        && !member.agent.is_child_of(&self.agent)) {
                        return Err(Error::Invalid("invalid agent membership".into()));
                    }
                    if member.agent != self.agent {
                        return Ok(Some((member.agent, self)));
                    }
                }
            }
        }
    }
}
