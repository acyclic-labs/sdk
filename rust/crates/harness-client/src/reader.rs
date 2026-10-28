//! The client data layer: a lazy reader over committed stream history.
//!
//! It uses only existing stream primitives — `children_page` to list a
//! subtree, `read(from, limit)` and `head` for history and origins, and one
//! multiplexed session `follow` per displayed live tail — plus the one
//! [`RecordCache`]. It holds no agent-specific model; agent rows and views are
//! renders over its records (see [`crate::explorer`]).
//!
//! The reader is sans-I/O so native, WASM/TS and terminal clients share it
//! unchanged: a renderer states what it needs each frame ([`Want`]), the
//! reader decides which reads run ([`Reader::poll`]), the platform driver
//! performs them and reports the outcome ([`Reader::complete`]). One
//! scheduler, one deadline ([`Reader::next_deadline`]) and one cache serve
//! every agent; idle agents cost nothing.

mod backing;
mod cache;
mod credential;
mod driver;

pub use backing::RecordBytes;
use cache::Key;
pub use cache::{Listing, RecordCache, SnapshotError};
pub use credential::CredentialStatus;
use credential::{Access, Credentials};
pub use driver::Driver;

use acyclic_stream::OwnedBytes;
use acyclic_stream::{CommitId, ForkOrigin, PathHead, StreamError, StreamPath};
use std::collections::{BTreeMap, BTreeSet};

/// Finite budgets for one client, independent of the number of streams.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ReaderPolicy {
    /// One LRU byte budget for every cached record and listing page. It
    /// bounds caching, not reading: a displayed value the budget cannot hold
    /// is served uncached while it is wanted.
    pub cache_bytes: usize,
    /// Streams whose tail/fork facts are retained between frames, beside the
    /// byte budget; facts the current frame uses are never evicted.
    pub streams: usize,
    /// Reads in flight, all priorities.
    pub requests: usize,
    /// Of those, prefetch reads; visible reads always keep a slot. New
    /// visible work gets at most one request window's ordering advantage.
    pub prefetch_requests: usize,
    /// Live follows on the one multiplexed session.
    pub follows: usize,
    /// Records per read.
    pub read_limit: u32,
    /// Children per listing page.
    pub listing_limit: u32,
    /// A follow lagging its tail by more than this jumps to the last records.
    pub lag: u64,
    /// Follow deliveries folded per frame; the rest wait on session credit.
    pub deliveries: usize,
    /// A read unanswered after this is failed and its slot reused.
    pub timeout_ms: u64,
    /// First retry delay after a failure.
    pub backoff_min_ms: u64,
    /// Longest retry delay.
    pub backoff_max_ms: u64,
    /// Consecutive failures before the client reports itself offline.
    pub offline_after: u32,
    /// Whether likely-next reads run at all (a data-saver switch).
    pub prefetch: bool,
}

impl Default for ReaderPolicy {
    fn default() -> Self {
        Self {
            cache_bytes: 8 * 1024 * 1024,
            streams: 4096,
            requests: 48,
            prefetch_requests: 32,
            follows: 64,
            read_limit: 64,
            listing_limit: 64,
            lag: 16,
            deliveries: 256,
            timeout_ms: 10_000,
            backoff_min_ms: 250,
            backoff_max_ms: 8_000,
            offline_after: 3,
            prefetch: true,
        }
    }
}

impl ReaderPolicy {
    /// Adapt the cache budget to the device: 1/256 of its memory, clamped to
    /// `[1 MiB, 64 MiB]`. Everything else stays at its default.
    #[must_use]
    pub fn for_device_memory(bytes: u64) -> Self {
        let budget = (bytes / 256).clamp(1 << 20, 64 << 20);
        Self {
            cache_bytes: usize::try_from(budget).unwrap_or(64 << 20),
            ..Self::default()
        }
    }

    /// Refuse budgets above the client hard ceilings or with no capacity.
    ///
    /// # Errors
    /// `StreamError::LimitExceeded` names any out-of-range budget.
    pub fn validate(&self) -> Result<(), StreamError> {
        let ok = (64 * 1024..=256 << 20).contains(&self.cache_bytes)
            && (16..=65_536).contains(&self.streams)
            && (2..=64).contains(&self.requests)
            && self.prefetch_requests < self.requests
            && (1..=254).contains(&self.follows)
            && (1..=acyclic_stream::MAX_ITEMS as u32).contains(&self.read_limit)
            && (1..=acyclic_stream::MAX_ITEMS as u32).contains(&self.listing_limit)
            && self.lag >= 1
            && (1..=65_536).contains(&self.deliveries)
            && self.timeout_ms > 0
            && self.backoff_min_ms > 0
            && self.backoff_min_ms <= self.backoff_max_ms
            && self.offline_after > 0;
        if ok {
            Ok(())
        } else {
            Err(StreamError::LimitExceeded)
        }
    }
}

/// Visible needs get a bounded head start over likely-next ones. Older
/// prefetch work still runs under continuous visible arrivals.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub enum Priority {
    /// On screen now.
    Visible,
    /// Likely next: overscan, scroll direction, hover/focus, selection.
    Prefetch,
}

/// One rendering need, stated each frame.
#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub enum Want {
    /// Records `[from, to)` of a stream.
    Range {
        /// Stream.
        stream: StreamPath,
        /// First position.
        from: u64,
        /// Exclusive end.
        to: u64,
    },
    /// The last `count` records of a stream: its head.
    Last {
        /// Stream.
        stream: StreamPath,
        /// Records before the tail.
        count: u32,
    },
    /// Listing page `index` of a parent stream, projecting each child's
    /// `tail` relative stream head when given.
    Listing {
        /// `children_page` parent.
        parent: StreamPath,
        /// Page index.
        index: u32,
        /// Relative stream whose last record each child row carries.
        tail: Option<StreamPath>,
        /// A stream committed with every change to this parent's children
        /// (an agent's status, written by the step that spawns a child). A
        /// cached page revalidates once this stream's tail moves past the
        /// tail seen when the page was requested.
        witness: Option<StreamPath>,
    },
}

/// One stream read for the driver to perform once.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Op {
    /// `read(stream, from, limit)`.
    Read {
        /// Completion identity.
        id: u64,
        /// Stream.
        stream: StreamPath,
        /// First position.
        from: u64,
        /// Record bound.
        limit: u32,
    },
    /// `head(stream)`: accepted tail, last record and exact fork origin.
    Head {
        /// Completion identity.
        id: u64,
        /// Stream.
        stream: StreamPath,
    },
    /// `children_page(parent, after, limit, tail_stream)`.
    Children {
        /// Completion identity.
        id: u64,
        /// Parent.
        parent: StreamPath,
        /// Page index this fills.
        index: u32,
        /// Exclusive start.
        after: Option<StreamPath>,
        /// Child bound.
        limit: u32,
        /// Relative stream projected under each child.
        tail: Option<StreamPath>,
        /// Hierarchy version a continuation (`after`) page is read at.
        version: Option<CommitId>,
    },
}

impl Op {
    /// Completion identity.
    #[must_use]
    pub fn id(&self) -> u64 {
        match self {
            Self::Read { id, .. } | Self::Head { id, .. } | Self::Children { id, .. } => *id,
        }
    }
}

/// One committed record as the driver received it.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Fetched {
    /// Stream position.
    pub sequence: u64,
    /// Record value.
    pub value: OwnedBytes,
    /// Commit that introduced it; anchors a resumed follow.
    pub commit: CommitId,
}

/// A listed child and, when projected, its relative stream's last record.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ListedChild {
    /// Child path.
    pub path: StreamPath,
    /// Immediate origin of the listed path, not its projected descendant.
    pub origin: Option<ForkOrigin>,
    /// The projected stream's last record.
    pub tail: Option<Fetched>,
}

/// The driver's report for one op.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Outcome {
    /// Records from the requested position, ascending.
    Records(Vec<Fetched>),
    /// Atomic stream head, including the origin at this observation.
    Head(PathHead),
    /// One children page.
    Children {
        /// Ordered children (possibly none: unlistable ones are omitted).
        children: Vec<ListedChild>,
        /// The server's exclusive start of the next page, if one exists.
        next: Option<StreamPath>,
        /// Hierarchy version the page was read at.
        version: CommitId,
    },
    /// The op failed.
    Failed(Failure),
}

/// Failure classes the scheduler reacts to.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Failure {
    /// Transport or timeout: back off and retry.
    Unavailable,
    /// The server shed load: back off longer and pause prefetch.
    Overloaded,
    /// A definite refusal: do not retry this read.
    Refused,
    /// The credential cannot read this stream; surface it and discard its cache.
    Denied,
    /// A checked resume no longer has the retained prefix it named.
    RetentionLost,
    /// The hierarchy changed under a page cursor: re-learn the version from
    /// a first page and continue from the same cursor.
    Changed,
}

impl Failure {
    /// Classify an original stream error.
    #[must_use]
    pub fn of(error: &StreamError) -> Self {
        match error {
            StreamError::Unavailable | StreamError::DeadlineElapsed => Self::Unavailable,
            StreamError::Capacity => Self::Overloaded,
            StreamError::HierarchyChanged => Self::Changed,
            StreamError::AccessDenied => Self::Denied,
            StreamError::PrefixNotRetained => Self::RetentionLost,
            _ => Self::Refused,
        }
    }
}

/// A reader discontinuity that applications must show separately from outages.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ReadIssue {
    /// Authorization failed; cached bytes for the stream were discarded.
    AccessDenied,
    /// The resume anchor was lost; history must be loaded from a new cut.
    RetentionLost,
    /// A directory continuation observed concurrent hierarchy changes.
    HierarchyChanged,
}

/// What the client can promise about freshness.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Connectivity {
    /// Reads are succeeding.
    Online,
    /// Recent reads failed; retrying after `retry_at`.
    Degraded {
        /// Next attempt.
        retry_at: u64,
    },
    /// Rendering the cache read-only; probing after `retry_at`.
    Offline {
        /// First failure of this outage.
        since: u64,
        /// Next probe.
        retry_at: u64,
    },
}

#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord)]
enum OpKey {
    Read(StreamPath, u64),
    Head(StreamPath),
    Children(StreamPath, u32),
}

impl OpKey {
    fn path(&self) -> &StreamPath {
        match self {
            Self::Read(path, _) | Self::Head(path) | Self::Children(path, _) => path,
        }
    }
}

/// How a listing op is read and when its page goes stale.
#[derive(Clone, Debug, Default)]
struct Paging {
    after: Option<StreamPath>,
    tail: Option<StreamPath>,
    /// The witness stream's tail when the page was planned.
    witnessed: Option<u64>,
}

struct InFlight {
    key: OpKey,
    priority: Priority,
    deadline: u64,
    issued: u64,
    paging: Paging,
    observed: Option<(u64, u64)>,
}

#[derive(Debug, Eq, PartialEq)]
pub(super) enum HeadObservation {
    Applied,
    Superseded,
    Invalid,
}

struct Demand {
    priority: Priority,
    paging: Paging,
    /// Kept across frames; moved to the back after dispatch. Renderer
    /// ordering cannot reset another need's accumulated wait.
    ticket: u64,
}

/// Exact listing continuations at strided page indexes, so any page is
/// reachable from a bounded walk even after its predecessor was evicted.
struct Anchors {
    stride: u32,
    at: BTreeMap<u32, StreamPath>,
    used: u64,
}

const MAX_ANCHORED_PARENTS: usize = 64;
const MAX_ANCHORS: usize = 256;
const MAX_REFUSED: usize = 64;

/// Footprint of one client, for tests and diagnostics.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct Residency {
    /// One encoded snapshot awaiting source admission, separately bounded.
    pub snapshot_bytes: usize,
    /// Cache charge, never above the byte budget.
    pub cache_bytes: usize,
    /// Uncached charge, including payload retained by cloned views after eviction.
    pub passed_bytes: usize,
    /// Unique payload capacity across cache, pass-throughs and cloned views.
    /// Excludes ownership headers, allocator overhead and adapter copies.
    pub backing_bytes: usize,
    /// Streams with retained facts.
    pub streams: usize,
    /// Cache entries.
    pub cache_entries: usize,
    /// Reads in flight.
    pub requests: usize,
    /// Live follows wanted.
    pub follows: usize,
    /// Outstanding needs this frame.
    pub demand: usize,
    /// Parents with listing anchors.
    pub anchored: usize,
}

/// One client's scheduler and cache.
pub struct Reader {
    identity: std::sync::Arc<()>,
    pub(super) source_generation: u64,
    policy: ReaderPolicy,
    cache: RecordCache,
    credentials: Option<Credentials>,
    pending_snapshot: Option<Vec<u8>>,
    driver: Option<std::sync::Arc<()>>,
    demand: BTreeMap<OpKey, Demand>,
    next_ticket: u64,
    inflight: BTreeMap<u64, InFlight>,
    keys: BTreeMap<OpKey, u64>,
    cancelled: Vec<u64>,
    refused: BTreeSet<OpKey>,
    issues: BTreeMap<StreamPath, ReadIssue>,
    anchors: BTreeMap<StreamPath, Anchors>,
    follows: BTreeMap<StreamPath, u64>,
    delivered: usize,
    next_id: u64,
    failures: u32,
    retry_at: u64,
    hierarchy: Option<CommitId>,
    follow_backoff: bool,
    generation: u64,
    since: Option<u64>,
    prefetch_paused_until: u64,
    frame_mark: u64,
    now: u64,
    rtt_ms: u64,
    issued: u64,
}

impl Reader {
    /// A reader with an empty cache.
    ///
    /// # Errors
    /// Refuses an invalid policy.
    pub fn new(policy: ReaderPolicy) -> Result<Self, StreamError> {
        policy.validate()?;
        Ok(Self {
            identity: std::sync::Arc::new(()),
            source_generation: 0,
            policy,
            cache: RecordCache::new(policy.cache_bytes, policy.streams),
            credentials: None,
            pending_snapshot: None,
            driver: None,
            demand: BTreeMap::new(),
            next_ticket: 0,
            inflight: BTreeMap::new(),
            keys: BTreeMap::new(),
            cancelled: Vec::new(),
            refused: BTreeSet::new(),
            issues: BTreeMap::new(),
            anchors: BTreeMap::new(),
            follows: BTreeMap::new(),
            delivered: 0,
            next_id: 1,
            failures: 0,
            retry_at: 0,
            hierarchy: None,
            follow_backoff: false,
            generation: 0,
            since: None,
            prefetch_paused_until: 0,
            frame_mark: 0,
            now: 0,
            rtt_ms: 0,
            issued: 0,
        })
    }

    /// Register the configured source before restoring or reading retained data.
    /// Providers without trusted custody remain explicitly unqualified.
    ///
    /// # Errors
    /// Refuses an invalid policy.
    pub fn for_provider<P: acyclic_stream::StreamProvider + ?Sized>(
        policy: ReaderPolicy,
        provider: &P,
    ) -> Result<Self, StreamError> {
        let mut reader = Self::new(policy)?;
        reader.configure_source(provider, 0);
        Ok(reader)
    }

    pub(crate) fn configure_source<P: acyclic_stream::StreamProvider + ?Sized>(
        &mut self,
        provider: &P,
        now: u64,
    ) {
        self.credentials = provider
            .credential_provider()
            .cloned()
            .map(|provider| Credentials::new(provider, now, self.policy.timeout_ms));
        self.reset_source();
    }

    fn reset_source(&mut self) {
        self.source_generation += 1;
        self.cancelled.extend(self.inflight.keys().copied());
        self.inflight.clear();
        self.keys.clear();
        self.demand.clear();
        self.refused.clear();
        self.issues.clear();
        self.anchors.clear();
        self.follows.clear();
        self.hierarchy = None;
        self.failures = 0;
        self.since = None;
        self.retry_at = 0;
        self.follow_backoff = false;
        self.prefetch_paused_until = 0;
        let access = self
            .credentials
            .as_ref()
            .map_or(Access::Unqualified, Credentials::access);
        self.cache.reset_access(access);
        match self.credential_status() {
            CredentialStatus::Current => {
                if let Some(snapshot) = self.pending_snapshot.take() {
                    // A cache miss is harmless; it never authenticates a source
                    // or changes the requested observation's logical outcome.
                    let _ = self.cache.restore(&snapshot);
                }
            }
            CredentialStatus::Pending => {}
            _ => self.pending_snapshot = None,
        }
        self.generation += 1;
    }

    fn advance_credentials(&mut self, now: u64, cx: &mut std::task::Context<'_>) -> bool {
        let changed = self
            .credentials
            .as_mut()
            .is_some_and(|credentials| credentials.poll(now, cx));
        if changed {
            self.reset_source();
            cx.waker().wake_by_ref();
        }
        changed
    }

    /// Qualification is independent of transport connectivity.
    #[must_use]
    pub fn credential_status(&self) -> CredentialStatus {
        self.credentials
            .as_ref()
            .map_or(CredentialStatus::Unqualified, Credentials::status)
    }

    /// Revalidate custody before using any previously rendered projection.
    #[must_use]
    pub fn readable(&self) -> bool {
        self.cache.access.allows()
    }

    /// Borrow the one cache for rendering.
    #[must_use]
    pub fn cache(&self) -> &RecordCache {
        &self.cache
    }

    /// Mutable access for the renderer's recency marks and fork facts.
    pub fn cache_mut(&mut self) -> &mut RecordCache {
        &mut self.cache
    }

    /// The validated policy.
    #[must_use]
    pub fn policy(&self) -> &ReaderPolicy {
        &self.policy
    }

    /// Persist the cache.
    #[must_use]
    pub fn snapshot(&self) -> Vec<u8> {
        self.cache.snapshot()
    }

    /// Warm start from persisted bytes. While the registered source resolves,
    /// retain one bounded snapshot for atomic validation after lease admission.
    ///
    /// # Errors
    /// Refuses malformed bytes or replacement while reads or follows are active.
    pub fn restore(&mut self, bytes: &[u8]) -> Result<(), SnapshotError> {
        if !self.inflight.is_empty() || !self.follows.is_empty() || !self.demand.is_empty() {
            return Err(SnapshotError);
        }
        if self.credential_status() == CredentialStatus::Pending {
            if bytes.len() > self.cache.snapshot_limit() {
                return Err(SnapshotError);
            }
            self.pending_snapshot = Some(bytes.to_vec());
            self.generation += 1;
            return Ok(());
        }
        self.cache.restore(bytes)?;
        self.generation += 1;
        Ok(())
    }

    /// Adapt to memory pressure: shrink (or regrow) the one cache budget.
    pub fn set_cache_budget(&mut self, bytes: usize) {
        let bytes = bytes.clamp(64 * 1024, 256 << 20);
        self.policy.cache_bytes = bytes;
        self.cache.set_budget(bytes);
        if self
            .pending_snapshot
            .as_ref()
            .is_some_and(|snapshot| snapshot.len() > self.cache.snapshot_limit())
        {
            self.pending_snapshot = None;
            self.generation += 1;
        }
    }

    /// Reads issued since construction.
    #[must_use]
    pub fn issued(&self) -> u64 {
        self.issued
    }

    /// Smoothed observed round trip, for prefetch distance.
    #[must_use]
    pub fn rtt_ms(&self) -> u64 {
        self.rtt_ms
    }

    /// Current freshness promise.
    #[must_use]
    pub fn connectivity(&self) -> Connectivity {
        match self.since {
            Some(since) if self.failures >= self.policy.offline_after => Connectivity::Offline {
                since,
                retry_at: self.retry_at,
            },
            Some(_) => Connectivity::Degraded {
                retry_at: self.retry_at,
            },
            None => Connectivity::Online,
        }
    }

    /// The platform reports a network change. Coming online resumes at once
    /// from the cache's positions, so nothing cached is refetched; a failure
    /// re-enters backoff. Going offline stops everything but one probe.
    pub fn network_changed(&mut self, online: bool, now: u64) {
        self.generation += 1;
        if online {
            if let Some(credentials) = self.credentials.as_mut()
                && credentials.status() == CredentialStatus::Unavailable
            {
                credentials.retry(now);
                self.reset_source();
            }
            self.failures = 0;
            self.since = None;
            self.retry_at = now;
        } else {
            self.failures = self.failures.max(self.policy.offline_after);
            self.since.get_or_insert(now);
            self.retry_at = now.saturating_add(self.policy.backoff_min_ms);
        }
    }

    /// Footprint, independent of how many streams exist.
    #[must_use]
    pub fn residency(&self) -> Residency {
        Residency {
            snapshot_bytes: self.pending_snapshot.as_ref().map_or(0, Vec::len),
            cache_bytes: self.cache.bytes(),
            passed_bytes: self.cache.passed_bytes(),
            backing_bytes: self.cache.backing_bytes(),
            streams: self.cache.streams(),
            cache_entries: self.cache.len(),
            requests: self.inflight.len(),
            follows: self.follows.len(),
            demand: self.demand.len(),
            anchored: self.anchors.len(),
        }
    }

    /// Begin a frame: expire overdue reads, reset per-frame budgets and mark
    /// recency so entries displayed this frame are protected from prefetch.
    pub fn begin_frame(&mut self, now: u64) {
        self.now = now;
        self.delivered = 0;
        self.frame_mark = self.cache.begin_frame();
        let overdue: Vec<u64> = self
            .inflight
            .iter()
            .filter(|(_, flight)| flight.deadline <= now)
            .map(|(id, _)| *id)
            .collect();
        for id in overdue {
            if self.cancel(id) {
                self.fail(Failure::Unavailable, now);
            }
        }
    }

    /// Replace this frame's needs, preserving the age of outstanding work.
    /// Every need is retained; request credit bounds execution, not intent.
    /// Reads no longer needed at prefetch priority are cancelled. What the
    /// visible wants cover is marked displayed, so no insert evicts it, and
    /// passed-through values no want covers any longer are released.
    pub fn plan(&mut self, wants: impl IntoIterator<Item = (Want, Priority)>) {
        let wants: Vec<(Want, Priority)> = wants.into_iter().collect();
        // Rows of one page each state the same page want; display it once.
        let mut displayed = BTreeSet::new();
        for (want, priority) in &wants {
            if *priority == Priority::Visible && displayed.insert(want) {
                self.display(want);
            }
        }
        self.cache
            .retain_passed(|cache, key| wants.iter().any(|(want, _)| covers(cache, want, key)));
        let mut previous: BTreeMap<_, _> = std::mem::take(&mut self.demand)
            .into_iter()
            .map(|(key, demand)| (key, demand.ticket))
            .collect();
        let mut demand = BTreeMap::<OpKey, Demand>::new();
        for (want, priority) in wants {
            let Some((key, paging)) = self.resolve(&want) else {
                continue;
            };
            if let Some(existing) = demand.get_mut(&key) {
                existing.priority = existing.priority.min(priority);
                continue;
            }
            let ticket = previous.remove(&key).unwrap_or_else(|| {
                let ticket = self.next_ticket;
                self.next_ticket += 1;
                ticket
            });
            demand.insert(
                key,
                Demand {
                    priority,
                    paging,
                    ticket,
                },
            );
        }
        // A prefetch becoming visible counts as visible immediately, rather
        // than occupying prefetch credit until its original request ends.
        for flight in self.inflight.values_mut() {
            if let Some(wanted) = demand.get(&flight.key) {
                flight.priority = wanted.priority;
            }
        }
        let stale: Vec<u64> = self
            .inflight
            .iter()
            .filter(|(_, flight)| {
                flight.priority == Priority::Prefetch && !demand.contains_key(&flight.key)
            })
            .map(|(id, _)| *id)
            .collect();
        for id in stale {
            self.cancel(id);
        }
        self.demand = demand;
    }

    /// Mark one visible want's cached values and facts in use this frame.
    fn display(&mut self, want: &Want) {
        if let Some((stream, from, to)) = window(&self.cache, want) {
            let stream = stream.clone();
            self.cache.touch_range(&stream, from, to);
        }
        if let Want::Listing {
            parent,
            index,
            witness,
            ..
        } = want
        {
            for (stream, last) in projected(&self.cache, want) {
                self.cache.touch_record(&stream, last);
            }
            self.cache.touch_listing(parent, *index);
            self.cache.touch_stream(parent);
            if let Some(witness) = witness {
                self.cache.touch_stream(witness);
            }
        }
    }

    /// The next uncached read for one want, or `None` when it is satisfied.
    fn resolve(&self, want: &Want) -> Option<(OpKey, Paging)> {
        let path = match want {
            Want::Range { stream, .. } | Want::Last { stream, .. } => stream,
            Want::Listing { parent, .. } => parent,
        };
        if self.issues.get(path) == Some(&ReadIssue::AccessDenied) {
            return None;
        }
        match want {
            Want::Range { stream, from, to } => {
                if from >= to {
                    return None;
                }
                if !self.cache.origin_known(stream) {
                    return Some((OpKey::Head(stream.clone()), Paging::default()));
                }
                // Nothing exists past the known tail; a follow extends it.
                let to = self.cache.tail(stream).map_or(*to, |tail| (*to).min(tail));
                let missing = self.cache.first_missing(stream, *from, to)?;
                // Reuse cached inherited records, but read misses through the
                // requested path's grant. Its source need not be authorized.
                Some((OpKey::Read(stream.clone(), missing), Paging::default()))
            }
            Want::Last { stream, count } => match self.cache.tail(stream) {
                Some(tail) => self.resolve(&Want::Range {
                    stream: stream.clone(),
                    from: tail.saturating_sub(u64::from(*count)),
                    to: tail,
                }),
                None => Some((OpKey::Head(stream.clone()), Paging::default())),
            },
            Want::Listing {
                parent,
                index,
                tail,
                witness,
            } => {
                let witnessed = witness.as_ref().and_then(|w| self.cache.tail(w));
                let (index, after) = match self.cache.listing(parent, *index) {
                    Some(listing)
                        if listing.fresh
                            && (witness.is_none() || listing.witnessed == witnessed) =>
                    {
                        return None;
                    }
                    // Displayed stale page: revalidate from its own start.
                    Some(listing) => (*index, listing.after.clone()),
                    None => self.reachable(parent, *index)?,
                };
                // A cursor is only valid at a known hierarchy version; a
                // first page learns it.
                let (index, after) = if after.is_some() && self.hierarchy.is_none() {
                    (0, None)
                } else {
                    (index, after)
                };
                Some((
                    OpKey::Children(parent.clone(), index),
                    Paging {
                        after,
                        tail: tail.clone(),
                        witnessed,
                    },
                ))
            }
        }
    }

    /// The highest page at or before `index` whose start is known.
    fn reachable(&self, parent: &StreamPath, index: u32) -> Option<(u32, Option<StreamPath>)> {
        let anchor = self
            .anchors
            .get(parent)
            .and_then(|anchors| anchors.at.range(..=index).next_back());
        let floor = anchor.map_or(0, |(at, _)| *at);
        // Search retained pages instead of counting down from a potentially
        // enormous logical offset through evicted pages.
        let cached = self
            .cache
            .listings(parent)
            .filter(|(page, _)| *page < index && *page >= floor)
            .max_by_key(|(page, _)| *page);
        if let Some((page, listing)) = cached {
            return listing.next.clone().map(|next| (page + 1, Some(next)));
        }
        Some(anchor.map_or((0, None), |(page, after)| (*page, Some(after.clone()))))
    }

    /// Reads to start now, in aged priority order, within every budget.
    pub fn poll(&mut self, now: u64) -> Vec<Op> {
        self.now = now;
        let mut ops = Vec::new();
        if !self.ready(now) {
            return ops;
        }
        let offline = matches!(self.connectivity(), Connectivity::Offline { .. });
        // An outage admits one probe at a time; recovery restores the budget.
        let limit = if offline || self.since.is_some() {
            1
        } else {
            self.policy.requests
        };
        if self.inflight.len() >= limit {
            return ops;
        }
        let prefetch =
            self.policy.prefetch && self.since.is_none() && now >= self.prefetch_paused_until;
        let mut prefetching = self
            .inflight
            .values()
            .filter(|flight| flight.priority == Priority::Prefetch)
            .count();
        let mut demand: Vec<_> = self.demand.iter_mut().collect();
        demand.sort_by_key(|(_, demand)| {
            let head_start = if demand.priority == Priority::Prefetch {
                self.policy.requests as u128
            } else {
                0
            };
            (u128::from(demand.ticket) + head_start, demand.ticket)
        });
        for (key, pending) in demand {
            let Demand {
                priority,
                paging,
                ticket,
            } = pending;
            if self.inflight.len() >= limit {
                break;
            }
            if self.keys.contains_key(key)
                || self.refused.contains(key)
                || self.issues.get(key.path()) == Some(&ReadIssue::AccessDenied)
            {
                continue;
            }
            if *priority == Priority::Prefetch {
                if !prefetch || prefetching >= self.policy.prefetch_requests {
                    continue;
                }
                prefetching += 1;
            }
            let id = self.next_id;
            self.next_id += 1;
            let op = match key {
                OpKey::Read(stream, from) => Op::Read {
                    id,
                    stream: stream.clone(),
                    from: *from,
                    limit: self.policy.read_limit,
                },
                OpKey::Head(stream) => Op::Head {
                    id,
                    stream: stream.clone(),
                },
                OpKey::Children(parent, index) => Op::Children {
                    id,
                    parent: parent.clone(),
                    index: *index,
                    after: paging.after.clone(),
                    limit: self.policy.listing_limit,
                    tail: paging.tail.clone(),
                    version: paging.after.as_ref().and(self.hierarchy),
                },
            };
            self.inflight.insert(
                id,
                InFlight {
                    key: key.clone(),
                    priority: *priority,
                    deadline: now.saturating_add(self.policy.timeout_ms),
                    issued: now,
                    paging: paging.clone(),
                    observed: self.cache.observation(key.path()),
                },
            );
            self.keys.insert(key.clone(), id);
            *ticket = self.next_ticket;
            self.next_ticket += 1;
            self.issued += 1;
            ops.push(op);
        }
        ops
    }

    /// Reads the driver must abandon (timed out or no longer wanted).
    pub fn take_cancelled(&mut self) -> Vec<u64> {
        std::mem::take(&mut self.cancelled)
    }

    /// Fold one op's outcome into the cache. Late or cancelled outcomes are
    /// ignored; malformed ones count as refusals.
    pub fn complete(&mut self, id: u64, outcome: Outcome, now: u64) {
        if !self.readable() {
            return;
        }
        let Some(flight) = self.inflight.remove(&id) else {
            return;
        };
        self.generation += 1;
        self.keys.remove(&flight.key);
        let rtt = now.saturating_sub(flight.issued);
        self.rtt_ms = if self.rtt_ms == 0 {
            rtt
        } else {
            (self.rtt_ms * 7 + rtt) / 8
        };
        let protect = Some(self.frame_mark);
        let accepted = match (&flight.key, outcome) {
            (_, Outcome::Failed(Failure::Changed)) => {
                // This witness covers the whole hierarchy, not just the page
                // reporting it. Old continuations cannot be rebound to a new
                // version by an intervening reply or another driver wake.
                self.hierarchy = None;
                self.cache.clear_listings();
                self.anchors.clear();
                self.demand
                    .retain(|key, _| !matches!(key, OpKey::Children(..)));
                let cancelled: Vec<_> = self
                    .inflight
                    .iter()
                    .filter(|(_, flight)| matches!(flight.key, OpKey::Children(..)))
                    .map(|(id, _)| *id)
                    .collect();
                for id in cancelled {
                    self.cancel(id);
                }
                self.report_issue(flight.key.path(), ReadIssue::HierarchyChanged);
                return;
            }
            (_, Outcome::Failed(Failure::RetentionLost)) => {
                self.replaced(flight.key.path());
                return;
            }
            (_, Outcome::Failed(failure)) => {
                if failure == Failure::Denied {
                    self.denied(flight.key.path());
                }
                if matches!(failure, Failure::Refused | Failure::Denied) {
                    self.refuse(flight.key.clone());
                }
                self.fail(failure, now);
                return;
            }
            (OpKey::Read(stream, from), Outcome::Records(records)) => {
                let valid = records.len() <= self.policy.read_limit as usize
                    && records
                        .iter()
                        .enumerate()
                        .all(|(offset, record)| record.sequence == from + offset as u64);
                if valid {
                    if records.is_empty() {
                        // The stream ends here: stop wanting past it.
                        self.cache.observe_tail(stream, *from);
                    }
                    for record in records {
                        self.keep(stream, record, protect);
                    }
                }
                valid
            }
            (OpKey::Head(stream), Outcome::Head(head)) => {
                self.observed_head(stream, head, flight.observed) != HeadObservation::Invalid
            }
            (
                OpKey::Children(parent, index),
                Outcome::Children {
                    children,
                    next,
                    version,
                },
            ) => {
                let valid = children.len() <= self.policy.listing_limit as usize
                    && children.iter().all(|child| {
                        child
                            .origin
                            .as_ref()
                            .is_none_or(|origin| origin.source != child.path)
                    })
                    && children
                        .windows(2)
                        .all(|pair| pair[0].path.as_str() < pair[1].path.as_str());
                if valid {
                    self.hierarchy = Some(version);
                    self.listed(parent, *index, &flight.paging, children, next, protect);
                }
                valid
            }
            _ => false,
        };
        if accepted {
            // The driver may wake again before the renderer plans its next
            // frame. Completed work must not be dispatched a second time.
            self.demand.remove(&flight.key);
            self.failures = 0;
            self.since = None;
            self.retry_at = 0;
        } else {
            self.refuse(flight.key);
        }
    }

    fn listed(
        &mut self,
        parent: &StreamPath,
        index: u32,
        paging: &Paging,
        children: Vec<ListedChild>,
        next: Option<StreamPath>,
        protect: Option<u64>,
    ) {
        let mut paths = Vec::with_capacity(children.len());
        for child in children {
            self.learn_origin(&child.path, child.origin);
            if let Some(relative) = &paging.tail
                && let Ok(stream) =
                    StreamPath::new(format!("{}/{}", child.path.as_str(), relative.as_str()))
            {
                match child.tail {
                    // The projected record is the last one at the page cut.
                    Some(record) => {
                        self.cache
                            .observe_tail(&stream, record.sequence.saturating_add(1));
                        self.keep(&stream, record, protect);
                    }
                    // No projected record: the stream is empty at the page cut.
                    None => self.cache.observe_tail(&stream, 0),
                }
            }
            paths.push(child.path);
        }
        // A refreshed page that now ends elsewhere (a child was inserted or
        // removed before the cut) invalidates every later page and cursor.
        let moved = self
            .cache
            .listing(parent, index)
            .is_some_and(|old| old.next != next);
        if moved {
            self.cache.drop_listings_after(parent, index);
            let later = |key: &OpKey| matches!(key, OpKey::Children(path, at) if path == parent && *at > index);
            let cancelled: Vec<_> = self
                .inflight
                .iter()
                .filter(|(_, flight)| later(&flight.key))
                .map(|(id, _)| *id)
                .collect();
            for id in cancelled {
                self.cancel(id);
            }
            // The asynchronous driver can poll again before the next UI frame
            // replans demand. Do not reissue those same obsolete boundaries.
            self.demand.retain(|key, _| !later(key));
            if let Some(anchors) = self.anchors.get_mut(parent) {
                anchors.at.retain(|at, _| *at <= index);
            }
        }
        if let Some(next) = &next {
            self.anchor(parent, index + 1, next.clone());
        }
        if let Some(after) = &paging.after {
            self.anchor(parent, index, after.clone());
        }
        self.cache.insert_listing(
            parent,
            index,
            Listing {
                after: paging.after.clone(),
                children: paths,
                next,
                fresh: true,
                witnessed: paging.witnessed,
            },
            protect,
        );
    }

    fn keep(&mut self, stream: &StreamPath, record: Fetched, protect: Option<u64>) {
        self.cache
            .note_commit(stream, record.sequence, record.commit);
        self.cache
            .insert_record(stream, record.sequence, record.value, protect);
    }

    fn learn_origin(&mut self, stream: &StreamPath, origin: Option<ForkOrigin>) {
        if self.cache.origin_changed(stream, &origin) {
            self.replaced(stream);
        }
        self.cache.observe_origin(stream, origin);
    }

    /// Fold the same atomic head whether requested by demand or follow recovery.
    pub(super) fn observed_head(
        &mut self,
        stream: &StreamPath,
        head: PathHead,
        observed: Option<(u64, u64)>,
    ) -> HeadObservation {
        let valid = head.durable <= head.tail
            && match &head.last {
                Some(last) => last.sequence.checked_add(1) == Some(head.tail),
                None => head.tail == 0,
            }
            && head
                .origin
                .as_ref()
                .is_none_or(|origin| origin.source != *stream && origin.forked_at <= head.tail);
        if valid {
            let current = self.cache.observation(stream);
            if current != observed {
                if self.cache.origin_known(stream)
                    && self.cache.fork(stream) == head.origin.as_ref()
                {
                    // The exact origin still agrees. Newer value facts win;
                    // this smaller tail cannot establish rollback.
                    return HeadObservation::Applied;
                }
                if current.map(|(_, origin)| origin) != observed.map(|(_, origin)| origin) {
                    // A different origin observation won the race. The
                    // driver must obtain a fresh Head before accepting its
                    // follow, without treating this as transport failure.
                    return HeadObservation::Superseded;
                }
                let known = self.cache.origin_known(stream);
                self.learn_origin(stream, head.origin.clone());
                if !known {
                    // First origin observation after a projected tail. Keep
                    // newer value facts and let misses use the learned cut.
                    self.generation += 1;
                    return HeadObservation::Applied;
                }
                // Only values changed while this request waited. Its exact
                // changed publication supersedes the old known origin and
                // learn_origin has discarded that incompatible history.
            }
            let replaced = self.cache.tail(stream).is_some_and(|old| head.tail < old)
                || head.last.as_ref().is_some_and(|last| {
                    self.cache
                        .anchor(stream, last.sequence)
                        .is_some_and(|commit| commit != last.commit_id)
                });
            if replaced {
                self.replaced(stream);
            }
            self.learn_origin(stream, head.origin);
            self.cache.before_head(stream, head.tail);
            self.cache.observe_tail(stream, head.tail);
            if let Some(last) = head.last {
                self.keep(stream, driver::fetched(&last), Some(self.frame_mark));
            }
            self.generation += 1;
            HeadObservation::Applied
        } else {
            HeadObservation::Invalid
        }
    }

    fn anchor(&mut self, parent: &StreamPath, index: u32, after: StreamPath) {
        if !self.anchors.contains_key(parent) && self.anchors.len() >= MAX_ANCHORED_PARENTS {
            let oldest = self
                .anchors
                .iter()
                .min_by_key(|(_, anchors)| anchors.used)
                .map(|(path, _)| path.clone());
            if let Some(oldest) = oldest {
                self.anchors.remove(&oldest);
            }
        }
        let used = self.cache.clock();
        let anchors = self.anchors.entry(parent.clone()).or_insert(Anchors {
            stride: 1,
            at: BTreeMap::new(),
            used,
        });
        anchors.used = used;
        if index % anchors.stride != 0 {
            return;
        }
        anchors.at.insert(index, after);
        if anchors.at.len() > MAX_ANCHORS {
            anchors.stride *= 2;
            let stride = anchors.stride;
            anchors.at.retain(|index, _| index % stride == 0);
        }
    }

    fn refuse(&mut self, key: OpKey) {
        if self.refused.len() >= MAX_REFUSED
            && let Some(first) = self.refused.iter().next().cloned()
        {
            self.refused.remove(&first);
        }
        self.refused.insert(key);
    }

    fn fail(&mut self, failure: Failure, now: u64) {
        self.failures = self.failures.saturating_add(1);
        self.since.get_or_insert(now);
        let shift = (self.failures - 1).min(20);
        let backoff = self
            .policy
            .backoff_min_ms
            .saturating_mul(1 << shift)
            .min(self.policy.backoff_max_ms);
        // Deterministic jitter spreads reconnecting clients apart.
        let jitter = self.next_id.wrapping_mul(0x9E37_79B9) % (backoff / 4 + 1);
        self.retry_at = now.saturating_add(backoff + jitter);
        if failure == Failure::Overloaded {
            self.prefetch_paused_until = now.saturating_add(backoff.saturating_mul(4));
        }
    }

    /// Replace the live tails to follow. Each starts at the cache's tail, so a
    /// reconnect is just a follow from the last cached position. A follow
    /// whose stream tail ran more than `lag` ahead jumps to the last records;
    /// nothing is dropped, earlier records remain readable on demand.
    pub fn set_follows(&mut self, streams: impl IntoIterator<Item = StreamPath>) {
        let mut next = BTreeMap::new();
        for stream in streams {
            if self.issues.get(&stream) == Some(&ReadIssue::AccessDenied) {
                continue;
            }
            if next.len() >= self.policy.follows {
                break;
            }
            let Some(tail) = self.cache.tail(&stream) else {
                continue;
            };
            let from = self.cache.resume_from(&stream).unwrap_or_else(|| {
                match self.follows.get(&stream) {
                    Some(position) if tail.saturating_sub(*position) <= self.policy.lag => {
                        *position
                    }
                    _ => tail,
                }
            });
            next.insert(stream, from);
        }
        self.follows = next;
    }

    /// Wanted follows and their start positions, for the driver to diff.
    pub fn follows(&self) -> impl Iterator<Item = (&StreamPath, u64)> {
        self.follows.iter().map(|(path, from)| (path, *from))
    }

    /// Whether this frame still folds follow deliveries. When false the
    /// driver stops polling follows, so session credit applies backpressure.
    #[must_use]
    pub fn accepting(&self) -> bool {
        self.readable() && self.delivered < self.policy.deliveries
    }

    /// Fold one live record. `accepted` is the stream's tail as the server
    /// reported it; a follow that falls behind it jumps on the next frame.
    pub fn deliver(&mut self, stream: &StreamPath, record: Fetched, accepted: u64) {
        if !self.readable() {
            return;
        }
        self.delivered += 1;
        self.generation += 1;
        self.follow_backoff = false;
        self.cache.observe_tail(stream, accepted);
        if let Some(position) = self.follows.get_mut(stream) {
            *position = (*position).max(record.sequence + 1);
            self.keep(stream, record, Some(self.frame_mark));
        }
    }

    /// The commit a follow from `from` must find at `from - 1`: the cached
    /// suffix is provisional until the server confirms this anchor.
    #[must_use]
    pub fn follow_anchor(&self, stream: &StreamPath, from: u64) -> Option<CommitId> {
        from.checked_sub(1)
            .and_then(|at| self.cache.anchor(stream, at))
    }

    /// A checked follow found its anchor changed: the provisional suffix was
    /// replaced. Drop its backing namespaces and dependent aliases, then
    /// re-learn through the requested stream's own grant.
    pub fn replaced(&mut self, stream: &StreamPath) {
        self.generation += 1;
        self.report_issue(stream, ReadIssue::RetentionLost);
        self.invalidate(stream, true);
    }

    /// Read failures and cursor discontinuities, bounded independently of history.
    pub fn issues(&self) -> impl Iterator<Item = (&StreamPath, ReadIssue)> {
        self.issues.iter().map(|(path, issue)| (path, *issue))
    }

    /// Acknowledge a displayed issue. Retry a denial only after renewing authority.
    pub fn acknowledge_issue(&mut self, stream: &StreamPath) {
        self.issues.remove(stream);
        self.refused.retain(|key| key.path() != stream);
        self.generation += 1;
    }

    fn report_issue(&mut self, stream: &StreamPath, issue: ReadIssue) {
        if self.issues.len() == MAX_REFUSED
            && !self.issues.contains_key(stream)
            && let Some(first) = self.issues.keys().next().cloned()
        {
            self.issues.remove(&first);
        }
        self.issues.insert(stream.clone(), issue);
    }

    /// Refuse further delivery under a denied stream's old authority.
    pub fn denied(&mut self, stream: &StreamPath) {
        self.generation += 1;
        self.report_issue(stream, ReadIssue::AccessDenied);
        self.invalidate(stream, false);
    }

    fn invalidate(&mut self, stream: &StreamPath, inherited: bool) {
        let affected = self.cache.remove_stream(stream, inherited);
        self.demand.retain(|key, _| !affected.contains(key.path()));
        self.follows.retain(|path, _| !affected.contains(path));
        let cancelled: Vec<_> = self
            .inflight
            .iter()
            .filter(|(_, flight)| affected.contains(flight.key.path()))
            .map(|(id, _)| *id)
            .collect();
        for id in cancelled {
            self.cancel(id);
        }
    }

    fn cancel(&mut self, id: u64) -> bool {
        let Some(flight) = self.inflight.remove(&id) else {
            return false;
        };
        self.keys.remove(&flight.key);
        self.cancelled.push(id);
        true
    }

    /// Whether connections may be attempted now (outside a backoff).
    #[must_use]
    pub fn ready(&self, now: u64) -> bool {
        self.readable() && now >= self.retry_at
    }

    /// A follow failed to open or ended with an error: back off like a read.
    pub fn follow_failed(&mut self, failure: Failure, now: u64) {
        self.generation += 1;
        self.follow_backoff = true;
        self.fail(failure, now);
    }

    /// A wanted follow opened: it no longer waits on the retry deadline.
    pub fn follow_opened(&mut self, stream: &StreamPath, from: u64) {
        self.cache.resumed(stream, from);
        self.follow_backoff = false;
    }

    /// Counts completed work (replies, failures, deliveries). A renderer that
    /// sees it or [`Self::next_deadline`] change plans another frame.
    #[must_use]
    pub fn generation(&self) -> u64 {
        self.generation
    }

    /// The one client timer: the earliest read deadline or retry. A retry
    /// is armed while reads are wanted or a wanted follow waits to reopen.
    #[must_use]
    pub fn next_deadline(&self) -> Option<u64> {
        let read = self.inflight.values().map(|flight| flight.deadline).min();
        let waiting = !self.demand.is_empty() || (self.follow_backoff && !self.follows.is_empty());
        let retry = (waiting && self.retry_at > self.now).then_some(self.retry_at);
        let credential = self.credentials.as_ref().and_then(Credentials::deadline);
        [read, retry, credential].into_iter().flatten().min()
    }
}

/// The positions one want reads, once its stream's tail is known for `Last`.
fn window<'a>(cache: &RecordCache, want: &'a Want) -> Option<(&'a StreamPath, u64, u64)> {
    match want {
        Want::Range { stream, from, to } => {
            let to = cache.tail(stream).map_or(*to, |tail| (*to).min(tail));
            Some((stream, *from, to))
        }
        Want::Last { stream, count } => cache
            .tail(stream)
            .map(|tail| (stream, tail.saturating_sub(u64::from(*count)), tail)),
        Want::Listing { .. } => None,
    }
}

/// The projected last record of each child row of a cached listing page:
/// the page delivered them, so its want covers them while its rows show.
fn projected(cache: &RecordCache, want: &Want) -> Vec<(StreamPath, u64)> {
    let Want::Listing {
        parent,
        index,
        tail: Some(relative),
        ..
    } = want
    else {
        return Vec::new();
    };
    let Some(listing) = cache.listing(parent, *index) else {
        return Vec::new();
    };
    listing
        .children
        .iter()
        .filter_map(|child| {
            let stream =
                StreamPath::new(format!("{}/{}", child.as_str(), relative.as_str())).ok()?;
            let last = cache.tail(&stream)?.checked_sub(1)?;
            Some((stream, last))
        })
        .collect()
}

/// Whether `want` reads the value cached or passed through under `key`.
fn covers(cache: &RecordCache, want: &Want, key: &Key) -> bool {
    match (want, key) {
        (Want::Listing { parent, index, .. }, Key::Listing(path, at)) => {
            parent == path && index == at
        }
        (Want::Listing { .. }, Key::Record(..)) => projected(cache, want)
            .iter()
            .any(|(stream, last)| cache.resolve(stream, *last) == *key),
        (_, Key::Record(_, sequence)) => window(cache, want).is_some_and(|(stream, from, to)| {
            (from..to).contains(sequence) && cache.resolve(stream, *sequence) == *key
        }),
        _ => false,
    }
}

#[cfg(test)]
mod reader_tests {
    use super::*;

    fn head(tail: u64, commit: CommitId, value: &[u8], origin: Option<ForkOrigin>) -> PathHead {
        PathHead {
            retained_through: None,
            tail,
            durable: 0,
            origin,
            last: tail.checked_sub(1).map(|sequence| acyclic_stream::Record {
                sequence,
                value: value.to_vec().into(),
                commit_id: commit,
                committed_at_micros: 0,
                headers: Default::default(),
            }),
        }
    }

    #[test]
    fn delayed_head_cannot_roll_back_a_newer_projected_tail() {
        let stream = StreamPath::new("test/history").unwrap();
        let mut reader = Reader::new(ReaderPolicy::default()).unwrap();
        reader.cache.observe_origin(&stream, None);
        reader.cache.observe_tail(&stream, 10);
        let observed = reader.cache.observation(&stream);
        let old = head(10, CommitId::from_bytes([1; 32]), b"old", None);
        // A children projection completes while the saved Head awaits the
        // follow opening. Rendering recency changes do not count as cuts.
        reader.cache.touch_stream(&stream);
        assert_eq!(reader.cache.observation(&stream), observed);
        reader.cache.observe_tail(&stream, 11);
        reader.keep(&stream, fetched(10, 3), None);
        assert_eq!(
            reader.observed_head(&stream, old, observed),
            HeadObservation::Applied
        );
        assert_eq!(reader.cache.tail(&stream), Some(11));
        assert!(reader.cache.record(&stream, 10).is_some());
        assert!(reader.issues().next().is_none());

        // A fresh Head requested after that observation can establish a
        // genuine rollback; ignoring delayed responses must not hide it.
        let observed = reader.cache.observation(&stream);
        assert_eq!(
            reader.observed_head(
                &stream,
                head(10, CommitId::from_bytes([2; 32]), b"replacement", None),
                observed,
            ),
            HeadObservation::Applied
        );
        assert_eq!(reader.cache.tail(&stream), Some(10));
        assert_eq!(reader.issues.get(&stream), Some(&ReadIssue::RetentionLost));
    }

    #[test]
    fn replacement_head_discards_inherited_source_bytes_before_relearning() {
        let source = StreamPath::new("test/source").unwrap();
        let child = StreamPath::new("test/child").unwrap();
        let origin = ForkOrigin {
            source: source.clone(),
            forked_at: 1,
            commit_id: CommitId::from_bytes([9; 32]),
        };
        let mut reader = Reader::new(ReaderPolicy {
            requests: 2,
            prefetch_requests: 1,
            ..ReaderPolicy::default()
        })
        .unwrap();
        reader.cache.observe_origin(&source, None);
        reader.cache.observe_origin(&child, Some(origin.clone()));
        reader.cache.observe_tail(&child, 1);
        reader.keep(&child, fetched(0, 3), None);
        let observed = reader.cache.observation(&child);
        assert_eq!(
            reader.observed_head(
                &child,
                head(
                    1,
                    CommitId::from_bytes([2; 32]),
                    b"new",
                    Some(origin.clone())
                ),
                observed,
            ),
            HeadObservation::Applied
        );
        assert_eq!(reader.cache.record(&child, 0).unwrap().as_ref(), b"new");
        assert_eq!(reader.cache.record(&source, 0).unwrap().as_ref(), b"new");
        assert_eq!(
            reader.cache.anchor(&child, 0),
            Some(CommitId::from_bytes([2; 32]))
        );
        assert_eq!(reader.issues.get(&child), Some(&ReadIssue::RetentionLost));
        // A child denial does not revoke an independent source grant.
        reader.denied(&child);
        assert_eq!(reader.cache.record(&source, 0).unwrap().as_ref(), b"new");
        reader.acknowledge_issue(&child);
        reader.cache.observe_origin(&child, Some(origin));
        reader.plan([1, 2, 3].map(|from| {
            (
                Want::Range {
                    stream: child.clone(),
                    from,
                    to: from + 1,
                },
                Priority::Visible,
            )
        }));
        assert_eq!(reader.poll(0).len(), 2);
        // A checked follow's retention failure uses the same backing
        // invalidation as a conflicting Head, even without a new Head record.
        reader.replaced(&child);
        assert!(reader.cache.record(&source, 0).is_none());
        // Both in-flight reads and the unsent third request use the old cut.
        // Another driver wake must wait for the renderer to replan a Head.
        assert_eq!(reader.take_cancelled().len(), 2);
        assert!(reader.poll(1).is_empty());
    }

    #[test]
    fn record_completion_cannot_hide_a_changed_fork_publication() {
        let stream = StreamPath::new("test/child").unwrap();
        let origin = ForkOrigin {
            source: StreamPath::new("test/source").unwrap(),
            forked_at: 1,
            commit_id: CommitId::from_bytes([9; 32]),
        };
        let mut reader = Reader::new(ReaderPolicy::default()).unwrap();
        reader.cache.observe_origin(&stream, Some(origin.clone()));
        reader.cache.observe_tail(&stream, 2);
        reader.keep(&stream, fetched(1, 3), None);
        let observed = reader.cache.observation(&stream);
        // Only the publication changes; a preceding record commit still
        // matches. A concurrent history read must not erase that distinction.
        reader.keep(&stream, fetched(0, 3), None);
        let changed = ForkOrigin {
            commit_id: CommitId::from_bytes([10; 32]),
            ..origin
        };
        assert_eq!(
            reader.observed_head(
                &stream,
                head(
                    2,
                    CommitId::from_bytes([1; 32]),
                    b"same",
                    Some(changed.clone())
                ),
                observed,
            ),
            HeadObservation::Applied
        );
        assert_eq!(reader.cache.fork(&stream), Some(&changed));
        assert_eq!(reader.issues.get(&stream), Some(&ReadIssue::RetentionLost));

        // If a later primitive origin observation already won, an older
        // conflicting Head must reobserve before its follow can be accepted.
        let observed = reader.cache.observation(&stream);
        let newer = ForkOrigin {
            commit_id: CommitId::from_bytes([11; 32]),
            ..changed.clone()
        };
        reader.learn_origin(&stream, Some(newer.clone()));
        assert_eq!(
            reader.observed_head(
                &stream,
                head(2, CommitId::from_bytes([1; 32]), b"same", Some(changed)),
                observed,
            ),
            HeadObservation::Superseded
        );
        assert_eq!(reader.cache.fork(&stream), Some(&newer));
    }

    #[test]
    fn all_4097_wants_complete_through_request_credit() {
        let stream = StreamPath::new("test/many-wants").unwrap();
        let mut reader = Reader::new(ReaderPolicy {
            read_limit: 1,
            ..ReaderPolicy::default()
        })
        .unwrap();
        reader.cache.observe_origin(&stream, None);
        reader.cache.observe_tail(&stream, 8194);
        let wants: Vec<_> = (0..4097)
            .map(|index| {
                (
                    Want::Range {
                        stream: stream.clone(),
                        from: index * 2,
                        to: index * 2 + 1,
                    },
                    Priority::Visible,
                )
            })
            .collect();
        reader.begin_frame(0);
        reader.plan(wants.iter().cloned());
        assert_eq!(reader.residency().demand, 4097);
        let mut completed = BTreeSet::new();
        for now in 1..=4097 {
            // The driver can consume successive credit windows before the
            // renderer plans another frame. A completion retires that key.
            let ops = reader.poll(now);
            if ops.is_empty() {
                break;
            }
            assert!(ops.len() <= reader.policy.requests);
            for op in ops {
                let Op::Read { id, from, .. } = op else {
                    panic!("known-origin range read")
                };
                assert!(
                    completed.insert(from),
                    "already satisfied want was reissued"
                );
                reader.complete(id, Outcome::Records(vec![fetched(from, 1)]), now);
            }
        }
        assert_eq!(completed.len(), wants.len());
        assert!(reader.poll(4098).is_empty());
        assert!(frame(&mut reader, 4098, &wants).is_empty());
    }

    #[test]
    fn continuously_arriving_visible_wants_cannot_starve_older_prefetch() {
        let old = StreamPath::new("test/old-prefetch").unwrap();
        let mut reader = Reader::new(ReaderPolicy {
            requests: 2,
            prefetch_requests: 1,
            ..ReaderPolicy::default()
        })
        .unwrap();
        let mut served = false;
        for now in 0..8 {
            // Two fresh visible Heads always consume all request credit in
            // the old strict-priority order. Reversing renderer order also
            // must not discard the prefetch's age between frames.
            let mut wants: Vec<_> = (0..2)
                .map(|index| {
                    (
                        Want::Last {
                            stream: StreamPath::new(format!("test/visible-{now}-{index}")).unwrap(),
                            count: 1,
                        },
                        Priority::Visible,
                    )
                })
                .collect();
            wants.push((
                Want::Last {
                    stream: old.clone(),
                    count: 1,
                },
                Priority::Prefetch,
            ));
            for op in frame(&mut reader, now, &wants) {
                let Op::Head { id, stream } = op else {
                    panic!("uncached Head")
                };
                served |= stream == old;
                reader.complete(
                    id,
                    Outcome::Head(PathHead {
                        retained_through: None,
                        tail: 0,
                        last: None,
                        durable: 0,
                        origin: None,
                    }),
                    now,
                );
            }
            if served {
                break;
            }
        }
        assert!(
            served,
            "older prefetch starved under continuous visible arrivals"
        );
    }

    #[test]
    fn newer_head_preserves_the_cached_resume_check() {
        let stream = StreamPath::new("test/history").unwrap();
        let old = CommitId::from_bytes([1; 32]);
        let mut reader = Reader::new(ReaderPolicy::default()).unwrap();
        reader.cache.observe_origin(&stream, None);
        reader.cache.observe_tail(&stream, 1);
        reader.keep(&stream, fetched(0, 10), None);
        let observed = reader.cache.observation(&stream);
        assert_eq!(
            reader.observed_head(
                &stream,
                PathHead {
                    retained_through: None,
                    tail: 2,
                    durable: 0,
                    origin: None,
                    last: Some(acyclic_stream::Record {
                        sequence: 1,
                        value: b"later".to_vec().into(),
                        commit_id: CommitId::from_bytes([2; 32]),
                        committed_at_micros: 0,
                        headers: Default::default(),
                    }),
                },
                observed,
            ),
            HeadObservation::Applied
        );
        let mut restored = Reader::new(ReaderPolicy::default()).unwrap();
        restored.restore(&reader.snapshot()).unwrap();
        restored.set_follows([stream.clone()]);
        assert_eq!(restored.follows().collect::<Vec<_>>(), vec![(&stream, 1)]);
        assert_eq!(restored.follow_anchor(&stream, 1), Some(old));
        restored.follow_opened(&stream, 1);
        assert_eq!(restored.cache.resume_from(&stream), None);
    }

    #[test]
    fn changed_origin_cancels_dependent_reads_before_their_late_reply() {
        let source = StreamPath::new("test/source").unwrap();
        let child = StreamPath::new("test/child").unwrap();
        let mut reader = Reader::new(ReaderPolicy::default()).unwrap();
        reader.cache.observe_origin(&source, None);
        reader.cache.observe_origin(
            &child,
            Some(ForkOrigin {
                source: source.clone(),
                forked_at: 1,
                commit_id: CommitId::from_bytes([1; 32]),
            }),
        );
        reader.plan([(
            Want::Range {
                stream: child.clone(),
                from: 1,
                to: 2,
            },
            Priority::Visible,
        )]);
        let ops = reader.poll(0);
        let [op] = ops.as_slice() else {
            panic!("one dependent read");
        };
        reader.learn_origin(
            &source,
            Some(ForkOrigin {
                source: StreamPath::new("test/other").unwrap(),
                forked_at: 0,
                commit_id: CommitId::from_bytes([2; 32]),
            }),
        );
        assert_eq!(reader.take_cancelled(), vec![op.id()]);
        reader.complete(op.id(), Outcome::Records(vec![fetched(1, 10)]), 1);
        assert!(reader.cache.record(&child, 1).is_none());
        assert!(!reader.cache.origin_known(&child));
        assert_eq!(
            reader.issues().collect::<Vec<_>>(),
            vec![(&source, ReadIssue::RetentionLost)]
        );
    }

    #[test]
    fn moved_page_boundary_cancels_old_replies_and_unsent_demand() {
        let parent = StreamPath::new("test/children").unwrap();
        let old = StreamPath::new("test/children/c").unwrap();
        let new = StreamPath::new("test/children/b").unwrap();
        let mut reader = Reader::new(ReaderPolicy::default()).unwrap();
        reader.cache.insert_listing(
            &parent,
            0,
            Listing {
                after: None,
                children: Vec::new(),
                next: Some(old.clone()),
                fresh: true,
                witnessed: None,
            },
            None,
        );
        let key = OpKey::Children(parent.clone(), 1);
        let paging = Paging {
            after: Some(old.clone()),
            tail: None,
            witnessed: None,
        };
        reader.inflight.insert(
            7,
            InFlight {
                key: key.clone(),
                priority: Priority::Visible,
                deadline: 100,
                issued: 0,
                paging: paging.clone(),
                observed: None,
            },
        );
        reader.keys.insert(key.clone(), 7);
        reader.demand.insert(
            key,
            Demand {
                priority: Priority::Visible,
                paging,
                ticket: 0,
            },
        );
        reader.listed(&parent, 0, &Paging::default(), Vec::new(), Some(new), None);
        assert_eq!(reader.take_cancelled(), vec![7]);
        assert!(reader.poll(0).is_empty());
        reader.complete(
            7,
            Outcome::Children {
                children: Vec::new(),
                next: Some(old),
                version: CommitId::from_bytes([1; 32]),
            },
            1,
        );
        assert!(reader.cache.listing(&parent, 1).is_none());
    }

    #[test]
    fn denial_discards_aliases_cancels_reads_and_requires_explicit_retry() {
        let stream = StreamPath::new("test/source").unwrap();
        let child = StreamPath::new("test/child").unwrap();
        let mut reader = Reader::new(ReaderPolicy::default()).unwrap();
        reader.cache.observe_origin(&stream, None);
        reader.begin_frame(0);
        reader.plan([0, 10].map(|from| {
            (
                Want::Range {
                    stream: stream.clone(),
                    from,
                    to: from + 1,
                },
                Priority::Visible,
            )
        }));
        let ops = reader.poll(0);
        assert_eq!(ops.len(), 2);
        reader
            .cache
            .insert_record(&stream, 0, OwnedBytes::from_static(b"old"), None);
        reader.cache.observe_origin(
            &child,
            Some(ForkOrigin {
                source: stream.clone(),
                forked_at: 1,
                commit_id: CommitId::from_bytes([2; 32]),
            }),
        );
        let snapshot = reader.snapshot();
        assert!(reader.restore(&snapshot).is_err());
        reader.complete(ops[0].id(), Outcome::Failed(Failure::Denied), 0);
        reader.complete(
            ops[1].id(),
            Outcome::Records(vec![Fetched {
                sequence: 10,
                value: OwnedBytes::from_static(b"late"),
                commit: CommitId::from_bytes([1; 32]),
            }]),
            1,
        );
        assert!(reader.cache.record(&stream, 0).is_none());
        assert!(reader.cache.record(&child, 0).is_none());
        assert!(reader.cache.record(&stream, 10).is_none());
        assert_eq!(
            reader.issues().collect::<Vec<_>>(),
            vec![(&stream, ReadIssue::AccessDenied)]
        );
        let want = Want::Range {
            stream: stream.clone(),
            from: 0,
            to: 1,
        };
        reader.begin_frame(10_000);
        reader.plan([(want.clone(), Priority::Visible)]);
        assert!(reader.poll(10_000).is_empty());
        reader.acknowledge_issue(&stream);
        reader.plan([(want, Priority::Visible)]);
        assert_eq!(reader.poll(10_000).len(), 1);
    }

    fn fetched(sequence: u64, len: usize) -> Fetched {
        Fetched {
            sequence,
            value: OwnedBytes::from(vec![7; len]),
            commit: CommitId::from_bytes([1; 32]),
        }
    }

    /// Plans `wants` for one frame and returns the reads it issues.
    fn frame(reader: &mut Reader, now: u64, wants: &[(Want, Priority)]) -> Vec<Op> {
        reader.begin_frame(now);
        reader.plan(wants.iter().cloned());
        reader.poll(now)
    }

    /// A record as large as the whole (minimum) budget is still delivered:
    /// served uncached while wanted, never re-requested, released once no
    /// want covers it. Control: before pass-through the failed insert was
    /// dropped and every frame re-issued the same read.
    #[test]
    fn over_budget_record_is_served_uncached_and_never_rerequested() {
        let stream = StreamPath::new("test/large").unwrap();
        let mut reader = Reader::new(ReaderPolicy {
            cache_bytes: 64 * 1024,
            ..ReaderPolicy::default()
        })
        .unwrap();
        let want = [(
            Want::Range {
                stream: stream.clone(),
                from: 0,
                to: 1,
            },
            Priority::Visible,
        )];
        reader.cache.observe_origin(&stream, None);
        let ops = frame(&mut reader, 0, &want);
        let [op] = ops.as_slice() else {
            panic!("one read");
        };
        reader.complete(op.id(), Outcome::Records(vec![fetched(0, 64 * 1024)]), 1);
        for now in 2..6 {
            assert!(frame(&mut reader, now, &want).is_empty(), "re-requested");
            assert_eq!(reader.cache.record(&stream, 0).unwrap().len(), 64 * 1024);
        }
        let residency = reader.residency();
        assert!(residency.cache_bytes <= 64 * 1024);
        assert!(residency.passed_bytes > 64 * 1024);
        assert!(frame(&mut reader, 6, &[]).is_empty());
        assert!(reader.cache.record(&stream, 0).is_none());
        assert_eq!(reader.residency().passed_bytes, 0);
    }

    /// Two displayed records that do not fit together never evict each
    /// other: the second is passed through beside the first. Control: a
    /// visible insert that could evict the displayed one re-read them in turn.
    #[test]
    fn displayed_records_that_do_not_fit_together_do_not_thrash() {
        let stream = StreamPath::new("test/pair").unwrap();
        let mut reader = Reader::new(ReaderPolicy {
            cache_bytes: 64 * 1024,
            ..ReaderPolicy::default()
        })
        .unwrap();
        let want = [0, 1].map(|from| {
            (
                Want::Range {
                    stream: stream.clone(),
                    from,
                    to: from + 1,
                },
                Priority::Visible,
            )
        });
        reader.cache.observe_origin(&stream, None);
        let ops = frame(&mut reader, 0, &want);
        assert_eq!(ops.len(), 2);
        for (sequence, op) in ops.iter().enumerate() {
            reader.complete(
                op.id(),
                Outcome::Records(vec![fetched(sequence as u64, 40 * 1024)]),
                1,
            );
        }
        for now in 2..6 {
            assert!(frame(&mut reader, now, &want).is_empty(), "thrashed");
            assert!(reader.cache.has_range(&stream, 0, 2));
        }
        assert!(reader.residency().cache_bytes <= 64 * 1024);
    }

    /// Projected heads of a displayed listing page that do not fit together
    /// stay served while the page is wanted: the page is their only reader.
    /// Control: a listing want covered only the page, so the passed head
    /// was dropped and never requested again.
    #[test]
    fn projected_heads_stay_served_while_their_page_is_wanted() {
        let parent = StreamPath::new("test/agents").unwrap();
        let relative = StreamPath::new("conversation").unwrap();
        let mut reader = Reader::new(ReaderPolicy {
            cache_bytes: 64 * 1024,
            ..ReaderPolicy::default()
        })
        .unwrap();
        let want = [(
            Want::Listing {
                parent: parent.clone(),
                index: 0,
                tail: Some(relative),
                witness: None,
            },
            Priority::Visible,
        )];
        let ops = frame(&mut reader, 0, &want);
        let [op] = ops.as_slice() else {
            panic!("one page");
        };
        let children = ["a", "b"].map(|name| ListedChild {
            path: StreamPath::new(format!("test/agents/{name}")).unwrap(),
            origin: None,
            tail: Some(fetched(0, 40 * 1024)),
        });
        reader.complete(
            op.id(),
            Outcome::Children {
                children: children.to_vec(),
                next: None,
                version: CommitId::from_bytes([1; 32]),
            },
            1,
        );
        for now in 2..6 {
            assert!(frame(&mut reader, now, &want).is_empty());
            for name in ["a", "b"] {
                let head = StreamPath::new(format!("test/agents/{name}/conversation")).unwrap();
                assert!(reader.cache.record(&head, 0).is_some(), "{name} at {now}");
            }
        }
        assert!(reader.residency().cache_bytes <= 64 * 1024);
    }

    #[test]
    fn prefix_loss_is_reported_separately_from_transport_failure() {
        let stream = StreamPath::new("test/source").unwrap();
        let mut reader = Reader::new(ReaderPolicy::default()).unwrap();
        reader
            .cache
            .insert_record(&stream, 0, OwnedBytes::from_static(b"old"), None);
        reader.replaced(&stream);
        assert!(reader.cache.record(&stream, 0).is_none());
        assert_eq!(
            reader.issues().collect::<Vec<_>>(),
            vec![(&stream, ReadIssue::RetentionLost)]
        );
        assert_eq!(reader.connectivity(), Connectivity::Online);
        assert_eq!(
            Failure::of(&StreamError::PrefixNotRetained),
            Failure::RetentionLost
        );
    }
}
