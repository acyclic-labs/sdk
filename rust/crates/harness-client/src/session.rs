use crate::{DemandError, DemandPolicy, StreamView, StreamViewPolicy, ViewDemand};
use acyclic_stream::{
    BoxProviderFuture, BoxProviderStream, StreamError, StreamPath, session::Client, wire,
    wire_codec,
};
use bytes::Bytes;
use futures::StreamExt;
use prost::Message as _;
use std::{
    borrow::Borrow,
    collections::BTreeMap,
    task::{Context, Poll},
};

#[cfg(all(test, feature = "agents"))]
mod viewport_tests;

type Follow = BoxProviderStream<'static, Result<wire::SessionFrame, StreamError>>;

enum SubscriptionState {
    Opening(BoxProviderFuture<'static, Result<Follow, StreamError>>),
    Open(Follow),
}

struct Subscription {
    state: SubscriptionState,
}

// The fold owns its original selection even after its live follow ends. A
// viewport refresh must neither restart terminal interests nor forget which
// principal/filter produced their retained records.
struct RetainedView {
    request: wire::SessionSubscription,
    view: StreamView,
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
    views: BTreeMap<StreamPath, RetainedView>,
    resources: BTreeMap<String, Subscription>,
    resource_views: BTreeMap<String, RetainedView>,
    view_policy: StreamViewPolicy,
    revision: u64,
}

impl WireSession {
    /// Shared control handles borrow the same physical session ownership.
    pub(crate) const fn client(&self) -> &Client {
        &self.client
    }

    /// Take the platform-created pool; native and WASM use the same owner.
    #[must_use]
    pub fn new(client: Client, policy: DemandPolicy) -> Self {
        Self::with_view_policy(client, policy, StreamViewPolicy::default())
    }

    /// Use caller-selected finite display bounds on the same canonical pool.
    #[must_use]
    pub fn with_view_policy(
        client: Client,
        policy: DemandPolicy,
        view_policy: StreamViewPolicy,
    ) -> Self {
        Self {
            client,
            policy,
            demand: ViewDemand::default(),
            follows: BTreeMap::new(),
            views: BTreeMap::new(),
            resources: BTreeMap::new(),
            resource_views: BTreeMap::new(),
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
        if demand.full().count() + self.resource_views.len() > self.policy.selected_capacity() {
            return Err(WireSessionError::Demand(DemandError::SelectedCapacity));
        }
        if demand == self.demand {
            return Ok(());
        }
        let revision = self
            .revision
            .checked_add(1)
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
        self.views.get(stream).map(|retained| &retained.view)
    }

    /// Replace a complete bounded set of stream subscriptions atomically.
    /// Existing selected views keep their fold and cursor, including terminal
    /// views. New cursors apply only to newly selected streams. Validation finishes before any old
    /// interest is released, and removals precede new pool admission.
    pub fn replace_streams(
        &mut self,
        subscriptions: impl IntoIterator<Item = (StreamPath, wire::SessionSubscription)>,
    ) -> Result<(), WireSessionError> {
        self.replace_selected(subscriptions, None)
    }

    /// Replace the entire viewport, including tool-output panes. Removed
    /// resources relinquish both their follow and cached records before new
    /// interests can open. Invalid replacements leave the old viewport intact.
    pub fn replace_interests(
        &mut self,
        subscriptions: impl IntoIterator<Item = (StreamPath, wire::SessionSubscription)>,
        resources: impl IntoIterator<Item = wire::SessionSubscription>,
    ) -> Result<(), WireSessionError> {
        let mut requested = BTreeMap::new();
        for (index, subscription) in resources.into_iter().enumerate() {
            if index == self.policy.selected_capacity() {
                return Err(WireSessionError::Demand(DemandError::SelectedCapacity));
            }
            let (resource, _) = validate_resource_subscription(&subscription)?;
            if let Some(existing) = self.resource_views.get(&resource)
                && (existing.request.credential != subscription.credential
                    || existing.request.kind != subscription.kind
                    || existing.request.filter != subscription.filter)
            {
                return Err(WireSessionError::InvalidSubscription);
            }
            if requested.insert(resource, subscription).is_some() {
                return Err(WireSessionError::AlreadyOpen);
            }
        }
        self.replace_selected(subscriptions, Some(requested))
    }

    fn replace_selected(
        &mut self,
        subscriptions: impl IntoIterator<Item = (StreamPath, wire::SessionSubscription)>,
        resources: Option<BTreeMap<String, wire::SessionSubscription>>,
    ) -> Result<(), WireSessionError> {
        let mut requested = BTreeMap::new();
        for (index, (path, subscription)) in subscriptions.into_iter().enumerate() {
            if index == self.policy.selected_capacity() {
                return Err(WireSessionError::Demand(DemandError::SelectedCapacity));
            }
            validate_subscription(&path, &subscription)?;
            if let Some(existing) = self.views.get(&path)
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
        let resource_count = resources
            .as_ref()
            .map_or(self.resource_views.len(), BTreeMap::len);
        if requested.len() + resource_count > self.policy.selected_capacity() {
            return Err(WireSessionError::Demand(DemandError::SelectedCapacity));
        }
        let demand = ViewDemand::new(
            self.policy,
            requested.keys().cloned(),
            std::iter::empty(),
            self.demand.expanded().cloned(),
        )
        .map_err(WireSessionError::Demand)?;
        let opening_resources = resources.as_ref().map_or(0, |requested| {
            requested
                .keys()
                .filter(|key| !self.resource_views.contains_key(*key))
                .count()
        });
        let opening = opening_resources
            + requested
                .keys()
                .filter(|path| !self.views.contains_key(*path))
                .count();
        // A replacement can change composition (for example focus) while its
        // stream membership is unchanged. Fence renderer caches in that case too.
        let last_revision = self
            .revision
            .checked_add(1 + opening as u64)
            .ok_or(WireSessionError::Stream(StreamError::Capacity))?;
        // These futures are lazy: staging them cannot admit a pool request.
        let mut staged = Vec::with_capacity(opening);
        let mut revision = self.revision + 1;
        let mut staged_resources = Vec::with_capacity(opening_resources);
        if let Some(requested) = &resources {
            for (resource, subscription) in requested {
                if self.resource_views.contains_key(resource) {
                    continue;
                }
                revision += 1;
                let from = subscription
                    .resume
                    .as_ref()
                    .ok_or(WireSessionError::InvalidSubscription)?
                    .next;
                let (view, follow) = self.stage_stream(subscription.clone(), from, revision);
                staged_resources.push((resource.clone(), view, follow));
            }
        }
        for (path, subscription) in requested {
            if self.views.contains_key(&path) {
                continue;
            }
            revision += 1;
            let from = subscription
                .resume
                .as_ref()
                .ok_or(WireSessionError::InvalidSubscription)?
                .next;
            let (view, subscription) = self.stage_stream(subscription, from, revision);
            staged.push((path, view, subscription));
        }
        self.follows.retain(|path, _| demand.contains_full(path));
        self.views.retain(|path, _| demand.contains_full(path));
        if let Some(requested) = &resources {
            self.resources.retain(|key, _| requested.contains_key(key));
            self.resource_views
                .retain(|key, _| requested.contains_key(key));
        }
        for (path, view, subscription) in staged {
            self.views.insert(path.clone(), view);
            self.follows.insert(path, subscription);
        }
        for (resource, view, subscription) in staged_resources {
            self.resource_views.insert(resource.clone(), view);
            self.resources.insert(resource, subscription);
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
        self.admit_one(self.views.contains_key(&agent))?;
        let from = validate_subscription(&agent, &subscription)?;
        let revision = self
            .revision
            .checked_add(1)
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
    ) -> (RetainedView, Subscription) {
        let filtered = subscription.filter.is_some()
            || matches!(
                &subscription.kind,
                Some(wire::session_subscription::Kind::Follow(follow))
                    if follow.resource.starts_with(FS_RESOURCE)
                        || follow.delivery == wire::SessionDelivery::Latest as i32
            );
        let request = subscription.clone();
        let client = self.client.clone();
        let opening = Box::pin(async move { client.subscribe(subscription).await });
        (
            RetainedView {
                request,
                view: StreamView::new(self.view_policy, from, revision, filtered),
            },
            Subscription {
                state: SubscriptionState::Opening(opening),
            },
        )
    }

    fn poll_open(
        &mut self,
        agent: &StreamPath,
        context: &mut Context<'_>,
    ) -> Poll<Result<(), WireSessionError>> {
        poll_open(&mut self.follows, agent, context)
    }

    /// Poll only the requested visible agent; never enumerate the stored swarm.
    /// No frame is copied or buffered here. The shared fold consumes it directly.
    /// Terminal delivery releases its subscription through the pool drop guard.
    pub fn poll_stream(
        &mut self,
        agent: &StreamPath,
        context: &mut Context<'_>,
    ) -> Poll<Result<Option<wire::SessionFrame>, WireSessionError>> {
        poll_follow(
            &mut self.follows,
            &mut self.views,
            &mut self.revision,
            agent,
            context,
        )
    }

    /// Select one filesystem change follow, `fs:/<volume>/<path>`, on the same
    /// pool and selected budget as stream follows. A key filter, such as one
    /// directory's `[prefix, successor)` range, narrows it to a subtree; the
    /// owning filesystem provider enforces the credential's read policy. The
    /// canonical resource names the follow for polling, viewing and release.
    ///
    /// # Errors
    /// Refuses duplicate, over-budget or malformed follows before admission.
    /// Calling this explicitly reopens a terminal resource and replaces its view.
    pub fn begin_resource(
        &mut self,
        subscription: wire::SessionSubscription,
    ) -> Result<(), WireSessionError> {
        let (resource, from) = validate_resource_subscription(&subscription)?;
        if self.resources.contains_key(&resource) {
            return Err(WireSessionError::AlreadyOpen);
        }
        self.admit_one(self.resource_views.contains_key(&resource))?;
        let revision = self
            .revision
            .checked_add(1)
            .ok_or(WireSessionError::Stream(StreamError::Capacity))?;
        let (view, subscription) = self.stage_stream(subscription, from, revision);
        self.resource_views.insert(resource.clone(), view);
        self.resources.insert(resource, subscription);
        self.revision = revision;
        Ok(())
    }

    /// Poll one selected filesystem follow; frames fold into its bounded view.
    pub fn poll_resource(
        &mut self,
        resource: &str,
        context: &mut Context<'_>,
    ) -> Poll<Result<Option<wire::SessionFrame>, WireSessionError>> {
        poll_follow(
            &mut self.resources,
            &mut self.resource_views,
            &mut self.revision,
            resource,
            context,
        )
    }

    /// Borrow one selected filesystem follow's bounded fold.
    #[must_use]
    pub fn resource_view(&self, resource: &str) -> Option<&StreamView> {
        self.resource_views
            .get(resource)
            .map(|retained| &retained.view)
    }

    /// Release one filesystem follow and its view; dropping the subscription
    /// returns its interest through the pool's cancellation guard. Removing a
    /// view advances the session revision so composed renderer caches refresh.
    ///
    /// # Errors
    /// Refuses before any removal when the session revision would overflow.
    pub fn release_resource(&mut self, resource: &str) -> Result<bool, WireSessionError> {
        if !self.resource_views.contains_key(resource) && !self.resources.contains_key(resource) {
            return Ok(false);
        }
        self.revision = self
            .revision
            .checked_add(1)
            .ok_or(WireSessionError::Stream(StreamError::Capacity))?;
        self.resource_views.remove(resource);
        self.resources.remove(resource);
        Ok(true)
    }

    fn admit_one(&self, replacing_view: bool) -> Result<(), WireSessionError> {
        // Terminal follows retain their last view. Charge that custody too,
        // otherwise repeatedly opening different failed resources grows memory
        // even though the number of live subscriptions stays bounded.
        if !replacing_view
            && self.views.len() + self.resource_views.len() >= self.policy.selected_capacity()
        {
            return Err(WireSessionError::Demand(DemandError::SelectedCapacity));
        }
        Ok(())
    }

    /// Release every stream/resource interest and its retained view atomically.
    /// Dropping subscriptions uses the pool's existing cancellation custody.
    pub fn clear(&mut self) -> Result<(), WireSessionError> {
        let revision = self
            .revision
            .checked_add(1)
            .ok_or(WireSessionError::Stream(StreamError::Capacity))?;
        self.follows.clear();
        self.views.clear();
        self.resources.clear();
        self.resource_views.clear();
        self.demand = ViewDemand::default();
        self.revision = revision;
        Ok(())
    }

    /// Live and opening interests across all products on this bounded owner.
    #[must_use]
    pub fn followed_interests(&self) -> usize {
        self.follows.len() + self.resources.len()
    }

    /// Display payload/record charge across the bounded selected folds,
    /// including final views retained after a follow ends.
    #[must_use]
    pub fn retained_bytes(&self) -> usize {
        self.views
            .values()
            .chain(self.resource_views.values())
            .map(|retained| retained.view.retained_bytes())
            .sum()
    }

    /// All retained folds, including terminal resources, share one finite cap.
    #[must_use]
    pub fn retained_views(&self) -> usize {
        self.views.len() + self.resource_views.len()
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

    /// A failed resource keeps its final view but must never be polled again.
    #[must_use]
    pub fn is_resource_following(&self, resource: &str) -> bool {
        self.resources.contains_key(resource)
    }
}

fn validate_subscription(
    path: &StreamPath,
    subscription: &wire::SessionSubscription,
) -> Result<u64, WireSessionError> {
    validate_subscription_bytes(subscription)?;
    let Some(wire::session_subscription::Kind::Follow(follow)) = &subscription.kind else {
        return Err(WireSessionError::InvalidSubscription);
    };
    // LATEST and key filters are ordinary follows whose provider skips
    // records; the view folds them as gaps (see `stage_stream`).
    if subscription.channel.is_some() {
        return Err(WireSessionError::InvalidSubscription);
    }
    let resume = subscription
        .resume
        .as_ref()
        .ok_or(WireSessionError::InvalidSubscription)?;
    wire_codec::session::validate_follow(follow, resume).map_err(WireSessionError::Stream)?;
    if wire_codec::session::stream_path(&follow.resource).map_err(WireSessionError::Stream)?
        != *path
    {
        return Err(WireSessionError::NotRequested);
    }
    Ok(resume.next)
}

const FS_RESOURCE: &str = "fs:/";

fn poll_open<K: Ord + Borrow<Q>, Q: Ord + ?Sized>(
    follows: &mut BTreeMap<K, Subscription>,
    key: &Q,
    context: &mut Context<'_>,
) -> Poll<Result<(), WireSessionError>> {
    let Some(subscription) = follows.get_mut(key) else {
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
            follows.remove(key);
            Poll::Ready(Err(WireSessionError::Stream(error)))
        }
    }
}

/// The one poll and fold shared by stream and filesystem follows.
fn poll_follow<K: Ord + Borrow<Q>, Q: Ord + ?Sized>(
    follows: &mut BTreeMap<K, Subscription>,
    views: &mut BTreeMap<K, RetainedView>,
    session_revision: &mut u64,
    key: &Q,
    context: &mut Context<'_>,
) -> Poll<Result<Option<wire::SessionFrame>, WireSessionError>> {
    match poll_open(follows, key, context) {
        Poll::Pending => return Poll::Pending,
        Poll::Ready(Err(error)) => return Poll::Ready(Err(error)),
        Poll::Ready(Ok(())) => {}
    }
    let Some(Subscription {
        state: SubscriptionState::Open(stream),
        ..
    }) = follows.get_mut(key)
    else {
        return Poll::Ready(Err(WireSessionError::NotOpen));
    };
    match stream.poll_next_unpin(context) {
        Poll::Pending => Poll::Pending,
        Poll::Ready(Some(Ok(frame))) => {
            let revision = session_revision
                .checked_add(1)
                .ok_or(WireSessionError::Stream(StreamError::Capacity));
            let folded = revision.and_then(|revision| {
                views
                    .get_mut(key)
                    .ok_or(WireSessionError::NotOpen)
                    .and_then(|view| {
                        view.view
                            .apply(&frame, revision)
                            .map_err(WireSessionError::Stream)
                    })?;
                *session_revision = revision;
                Ok(())
            });
            if let Err(error) = folded {
                // End a malformed or over-budget fold at this boundary;
                // never keep consuming past a failed display transition.
                follows.remove(key);
                return Poll::Ready(Err(error));
            }
            if matches!(
                frame.body.as_ref(),
                Some(wire::session_frame::Body::Closed(_))
            ) {
                follows.remove(key);
            }
            Poll::Ready(Ok(Some(frame)))
        }
        Poll::Ready(Some(Err(error))) => {
            follows.remove(key);
            Poll::Ready(Err(WireSessionError::Stream(error)))
        }
        Poll::Ready(None) => {
            follows.remove(key);
            Poll::Ready(Ok(None))
        }
    }
}

/// A filesystem follow requests every record of its `fs:/` resource; unlike
/// a stream follow it may carry a key filter selecting a subtree.
fn validate_resource_subscription(
    subscription: &wire::SessionSubscription,
) -> Result<(String, u64), WireSessionError> {
    validate_subscription_bytes(subscription)?;
    let Some(wire::session_subscription::Kind::Follow(follow)) = &subscription.kind else {
        return Err(WireSessionError::InvalidSubscription);
    };
    if follow.delivery != wire::SessionDelivery::All as i32
        || subscription.channel.is_some()
        || !follow.resource.starts_with(FS_RESOURCE)
    {
        return Err(WireSessionError::InvalidSubscription);
    }
    let resume = subscription
        .resume
        .as_ref()
        .ok_or(WireSessionError::InvalidSubscription)?;
    wire_codec::session::validate_follow(follow, resume).map_err(WireSessionError::Stream)?;
    Ok((follow.resource.clone(), resume.next))
}

fn validate_subscription_bytes(
    subscription: &wire::SessionSubscription,
) -> Result<(), WireSessionError> {
    if subscription.encoded_len() > acyclic_stream::MAX_COMMAND_BYTES {
        return Err(WireSessionError::Stream(StreamError::LimitExceeded));
    }
    Ok(())
}
