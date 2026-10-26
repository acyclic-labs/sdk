use crate::{DemandError, DemandPolicy, StreamView, StreamViewPolicy, ViewDemand};
use acyclic_stream::{
    BoxProviderFuture, BoxProviderStream, StreamError, StreamPath, session::Client, wire, wire_codec,
};
use bytes::Bytes;
use futures::StreamExt;
use std::{
    collections::BTreeMap,
    future::Future as _,
    task::{Context, Poll},
};

type Follow = BoxProviderStream<'static, Result<wire::SessionFrame, StreamError>>;

enum SubscriptionState {
    Opening(BoxProviderFuture<'static, Result<Follow, StreamError>>),
    Open(Follow),
}

struct Subscription {
    request: wire::SessionSubscription,
    state: SubscriptionState,
}

/// Admission failures are distinct from authenticated provider refusals.
#[derive(Debug)]
pub enum WireSessionError {
    /// The replacement view exceeds this session's policy.
    Demand(DemandError),
    /// No full follow was requested for this agent's fork stream.
    NotRequested,
    /// The agent already owns its one full follow.
    AlreadyOpen,
    /// The selected agent has no open follow.
    NotOpen,
    /// A canonical subscription must request full records for one Streams path.
    InvalidSubscription,
    /// Original typed transport, authorization or provider refusal.
    Stream(StreamError),
}

impl std::fmt::Display for WireSessionError {
    fn fmt(&self, output: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Demand(error) => write!(output, "{error}"),
            Self::NotRequested => output.write_str("agent follow was not requested"),
            Self::AlreadyOpen => output.write_str("agent follow is already open"),
            Self::NotOpen => output.write_str("agent follow is not open"),
            Self::InvalidSubscription => output.write_str("invalid full stream subscription"),
            Self::Stream(error) => write!(output, "{error}"),
        }
    }
}

impl std::error::Error for WireSessionError {}

impl WireSessionError {
    /// Stable classification, including the original provider refusal code.
    #[must_use]
    pub fn code(&self) -> &'static str {
        match self {
            Self::Demand(DemandError::InvalidPolicy) => "invalid_policy",
            Self::Demand(DemandError::SelectedCapacity) => "selected_capacity",
            Self::Demand(DemandError::ExpandedCapacity) => "expanded_capacity",
            Self::NotRequested => "not_requested",
            Self::AlreadyOpen => "already_open",
            Self::NotOpen => "not_open",
            Self::InvalidSubscription => "invalid_subscription",
            Self::Stream(error) => error.code(),
        }
    }
}

/// One owner for view-selected logical follows on the canonical Streams pool.
/// The pool alone owns physical connections, cursor recovery and receive credit.
/// This layer adds no queue, reconnect loop or per-agent transport task.
pub struct WireSession {
    client: Client,
    policy: DemandPolicy,
    demand: ViewDemand,
    follows: BTreeMap<StreamPath, Subscription>,
    views: BTreeMap<StreamPath, StreamView>,
    view_policy: StreamViewPolicy,
    revision: u64,
}

impl WireSession {
    /// Take the platform-created pool; native and WASM use the same owner.
    #[must_use]
    pub fn new(client: Client, policy: DemandPolicy) -> Self {
        Self::with_view_policy(client, policy, StreamViewPolicy::default())
    }

    /// Use caller-selected finite display bounds on the same canonical pool.
    #[must_use]
    pub fn with_view_policy(client: Client, policy: DemandPolicy, view_policy: StreamViewPolicy) -> Self {
        Self {
            client,
            policy,
            demand: ViewDemand::default(),
            follows: BTreeMap::new(),
            views: BTreeMap::new(),
            view_policy,
            revision: 0,
        }
    }

    /// Borrow admitted view demand, including lazy child discovery demand.
    #[must_use]
    pub fn demand(&self) -> &ViewDemand {
        &self.demand
    }

    /// Monotonic revision of the complete selected fold, including status
    /// streams and interest replacement. Renderers must not use only the
    /// conversation revision to cache a composed agent view.
    #[must_use]
    pub const fn revision(&self) -> u64 {
        self.revision
    }

    /// Close deselected follows before any newly selected follow can open.
    /// The replacement is rechecked against this session's own policy.
    ///
    /// # Errors
    /// Excess demand leaves both existing demand and follows unchanged.
    pub fn set_view(&mut self, demand: ViewDemand) -> Result<(), WireSessionError> {
        let demand = ViewDemand::new(
            self.policy,
            demand.full().cloned(),
            std::iter::empty(),
            demand.expanded().cloned(),
        )
        .map_err(WireSessionError::Demand)?;
        if demand == self.demand {
            return Ok(());
        }
        let revision = self.revision.checked_add(1)
            .ok_or(WireSessionError::Stream(StreamError::Capacity))?;
        self.follows.retain(|agent, _| demand.contains_full(agent));
        self.views.retain(|agent, _| demand.contains_full(agent));
        self.demand = demand;
        self.revision = revision;
        Ok(())
    }

    /// Borrow the exact shared Rust fold for one selected stream. Closing its
    /// transport retains the final view until the UI removes that interest.
    #[must_use]
    pub fn view(&self, stream: &StreamPath) -> Option<&StreamView> {
        self.views.get(stream)
    }

    /// Replace a complete bounded set of stream subscriptions atomically.
    /// Existing selected follows keep their fold and cursor. New cursors apply
    /// only to newly selected streams. Validation finishes before any old
    /// interest is released, and removals precede new pool admission.
    pub fn replace_streams(
        &mut self,
        subscriptions: impl IntoIterator<Item = (StreamPath, wire::SessionSubscription)>,
    ) -> Result<(), WireSessionError> {
        let mut requested = BTreeMap::new();
        for (index, (path, subscription)) in subscriptions.into_iter().enumerate() {
            if index == self.policy.selected_capacity() {
                return Err(WireSessionError::Demand(DemandError::SelectedCapacity));
            }
            validate_subscription(&path, &subscription)?;
            if let Some(existing) = self.follows.get(&path)
                && (existing.request.credential != subscription.credential
                    || existing.request.kind != subscription.kind)
            {
                // Keeping a cursor must never silently retain another principal
                // or downgrade a checked follow's immutable history anchor.
                return Err(WireSessionError::InvalidSubscription);
            }
            if requested.insert(path, subscription).is_some() {
                return Err(WireSessionError::AlreadyOpen);
            }
        }
        let demand = ViewDemand::new(
            self.policy,
            requested.keys().cloned(),
            std::iter::empty(),
            self.demand.expanded().cloned(),
        ).map_err(WireSessionError::Demand)?;
        let opening = requested.keys().filter(|path| !self.follows.contains_key(*path)).count();
        // A replacement can change composition (for example focus) while its
        // stream membership is unchanged. Fence renderer caches in that case too.
        let last_revision = self.revision.checked_add(1 + opening as u64)
            .ok_or(WireSessionError::Stream(StreamError::Capacity))?;
        // These futures are lazy: staging them cannot admit a pool request.
        let mut staged = Vec::with_capacity(opening);
        let mut revision = self.revision + 1;
        for (path, subscription) in requested {
            if self.follows.contains_key(&path) { continue; }
            revision += 1;
            let from = subscription.resume.as_ref()
                .ok_or(WireSessionError::InvalidSubscription)?.next;
            let (view, subscription) = self.stage_stream(subscription, from, revision);
            staged.push((path, view, subscription));
        }
        self.follows.retain(|path, _| demand.contains_full(path));
        self.views.retain(|path, _| demand.contains_full(path));
        for (path, view, subscription) in staged {
            self.views.insert(path.clone(), view);
            self.follows.insert(path, subscription);
        }
        self.demand = demand;
        self.revision = last_revision;
        Ok(())
    }

    /// Delegate bounded credential registration to the canonical pool.
    ///
    /// # Errors
    /// Preserves the pool's original capacity and transport refusal.
    pub fn register_credential(
        &self,
        slot: u32,
        credential: Bytes,
    ) -> Result<(), WireSessionError> {
        self.client
            .register_credential(slot, credential)
            .map_err(WireSessionError::Stream)
    }

    /// Open the full record stream of one selected agent fork.
    /// The caller supplies the authenticated subscription and original cursor.
    /// An interrupted wait retains its bounded opening in this session. Removing
    /// the interest drops it through the pool's existing cancellation guard.
    ///
    /// # Errors
    /// Refuses unsolicited, duplicate or malformed follows before pool admission;
    /// preserves authenticated provider refusals without inventing revision zero.
    pub async fn open_stream(
        &mut self,
        agent: StreamPath,
        subscription: wire::SessionSubscription,
    ) -> Result<(), WireSessionError> {
        self.begin_stream(agent.clone(), subscription)?;
        futures::future::poll_fn(|context| self.poll_open(&agent, context)).await
    }

    /// Admit a lazy open without holding a mutable borrow across network I/O.
    /// Its pending future counts against the same selected-follow bound and is
    /// dropped with the interest. Polling drives the canonical pool admission.
    ///
    /// # Errors
    /// Rejects unsolicited, duplicate or malformed follows before admission.
    pub fn begin_stream(
        &mut self,
        agent: StreamPath,
        subscription: wire::SessionSubscription,
    ) -> Result<(), WireSessionError> {
        if !self.demand.contains_full(&agent) {
            return Err(WireSessionError::NotRequested);
        }
        if self.follows.contains_key(&agent) {
            return Err(WireSessionError::AlreadyOpen);
        }
        let from = validate_subscription(&agent, &subscription)?;
        let revision = self.revision.checked_add(1)
            .ok_or(WireSessionError::Stream(StreamError::Capacity))?;
        let (view, subscription) = self.stage_stream(subscription, from, revision);
        self.views.insert(agent.clone(), view);
        self.follows.insert(agent, subscription);
        self.revision = revision;
        Ok(())
    }

    fn stage_stream(
        &self,
        subscription: wire::SessionSubscription,
        from: u64,
        revision: u64,
    ) -> (StreamView, Subscription) {
        let request = subscription.clone();
        let client = self.client.clone();
        let opening = Box::pin(async move { client.subscribe(subscription).await });
        (
            StreamView::new(self.view_policy, from, revision),
            Subscription { request, state: SubscriptionState::Opening(opening) },
        )
    }

    fn poll_open(
        &mut self,
        agent: &StreamPath,
        context: &mut Context<'_>,
    ) -> Poll<Result<(), WireSessionError>> {
        let Some(subscription) = self.follows.get_mut(agent) else {
            return Poll::Ready(Err(WireSessionError::NotOpen));
        };
        let SubscriptionState::Opening(opening) = &mut subscription.state else {
            return Poll::Ready(Ok(()));
        };
        match opening.as_mut().poll(context) {
            Poll::Pending => Poll::Pending,
            Poll::Ready(Ok(stream)) => {
                subscription.state = SubscriptionState::Open(stream);
                Poll::Ready(Ok(()))
            }
            Poll::Ready(Err(error)) => {
                self.follows.remove(agent);
                Poll::Ready(Err(WireSessionError::Stream(error)))
            }
        }
    }

    /// Poll only the requested visible agent; never enumerate the stored swarm.
    /// No frame is copied or buffered here. The shared fold consumes it directly.
    /// Terminal delivery releases its subscription through the pool drop guard.
    pub fn poll_stream(
        &mut self,
        agent: &StreamPath,
        context: &mut Context<'_>,
    ) -> Poll<Result<Option<wire::SessionFrame>, WireSessionError>> {
        match self.poll_open(agent, context) {
            Poll::Pending => return Poll::Pending,
            Poll::Ready(Err(error)) => return Poll::Ready(Err(error)),
            Poll::Ready(Ok(())) => {}
        }
        let Some(Subscription { state: SubscriptionState::Open(stream), .. }) = self.follows.get_mut(agent) else {
            return Poll::Ready(Err(WireSessionError::NotOpen));
        };
        match stream.poll_next_unpin(context) {
            Poll::Pending => Poll::Pending,
            Poll::Ready(Some(Ok(frame))) => {
                let revision = self.revision.checked_add(1)
                    .ok_or(WireSessionError::Stream(StreamError::Capacity));
                let folded = revision.and_then(|revision| {
                    self.views.get_mut(agent)
                        .ok_or(WireSessionError::NotOpen)
                        .and_then(|view| view.apply(&frame, revision).map_err(WireSessionError::Stream))?;
                    self.revision = revision;
                    Ok(())
                });
                if let Err(error) = folded {
                    // End a malformed or over-budget fold at this boundary;
                    // never keep consuming past a failed display transition.
                    self.follows.remove(agent);
                    return Poll::Ready(Err(error));
                }
                if matches!(frame.body.as_ref(), Some(wire::session_frame::Body::Closed(_))) {
                    self.follows.remove(agent);
                }
                Poll::Ready(Ok(Some(frame)))
            }
            Poll::Ready(Some(Err(error))) => {
                self.follows.remove(agent);
                Poll::Ready(Err(WireSessionError::Stream(error)))
            }
            Poll::Ready(None) => {
                self.follows.remove(agent);
                Poll::Ready(Ok(None))
            }
        }
    }

    /// Admitted selected follows, including pending opens, independent of swarm size.
    #[must_use]
    pub fn followed_agents(&self) -> usize {
        self.follows.len()
    }

    /// Whether this selected stream still owns a pending or open follow.
    /// Terminal views remain readable after their follow has been released.
    #[must_use]
    pub fn is_following(&self, stream: &StreamPath) -> bool {
        self.follows.contains_key(stream)
    }
}

fn validate_subscription(
    path: &StreamPath,
    subscription: &wire::SessionSubscription,
) -> Result<u64, WireSessionError> {
    let Some(wire::session_subscription::Kind::Follow(follow)) = &subscription.kind else {
        return Err(WireSessionError::InvalidSubscription);
    };
    if follow.delivery != wire::SessionDelivery::All as i32
        || subscription.filter.is_some() || subscription.channel.is_some()
    {
        return Err(WireSessionError::InvalidSubscription);
    }
    let resume = subscription.resume.as_ref().ok_or(WireSessionError::InvalidSubscription)?;
    wire_codec::session::validate_follow(follow, resume).map_err(WireSessionError::Stream)?;
    if wire_codec::session::stream_path(&follow.resource).map_err(WireSessionError::Stream)? != *path {
        return Err(WireSessionError::NotRequested);
    }
    Ok(resume.next)
}
