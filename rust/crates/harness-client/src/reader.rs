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

pub(crate) mod backing;
mod cache;
mod credential;
mod demand;
mod driver;
#[cfg(test)]
mod dst;
pub mod live;
mod requests;

pub use backing::{RecordBytes, RecordEvidence};
pub use cache::{Listing, ListingProjection, ListingProjections, RecordCache, SnapshotError};
pub use credential::CredentialStatus;
use credential::{Access, Credentials};
use demand::Demands;
pub use driver::Driver;
use requests::Requests;

use acyclic_stream::OwnedBytes;
use acyclic_stream::{
    CommitId, ForkCut, PathHead, Provenance, StreamCategory, StreamError, StreamPath,
};
use std::collections::{BTreeMap, BTreeSet};

/// Finite budgets for one client, independent of the number of streams.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ReaderPolicy {
    /// One LRU byte budget for every cached record and listing page. It
    /// bounds caching, not reading: a displayed value the budget cannot hold
    /// is served uncached while it is wanted. Zero keeps only wanted values.
    pub cache_bytes: usize,
    /// Streams whose tail/fork facts are retained between frames, beside the
    /// byte budget; facts the current frame uses are never evicted. Zero
    /// permits any unused fact to be replaced when admitting a new stream.
    pub streams: usize,
    /// Reads in flight, all priorities.
    pub requests: usize,
    /// Of those, prefetch reads; visible reads always keep a slot. New
    /// visible work gets at most one request window's ordering advantage.
    pub prefetch_requests: usize,
    /// Concurrent follow openings on the one multiplexed session. All wanted
    /// tails are retained; completed opens release credit for the next batch.
    pub follow_requests: usize,
    /// Records per read.
    pub read_limit: u32,
    /// Default logical page size for consumers such as the explorer.
    /// Each listing dependency carries its own size; transport requests chunk it.
    pub listing_limit: u32,
    /// A follow lagging its tail by more than this jumps to the last records.
    pub lag: u64,
    /// Follow deliveries folded per frame; the rest wait on session credit.
    pub deliveries: usize,
    /// Optional caller deadline before an unanswered read is failed.
    pub timeout_ms: Option<u64>,
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
            follow_requests: 64,
            read_limit: 64,
            listing_limit: 64,
            lag: 16,
            deliveries: 256,
            timeout_ms: None,
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

    /// Validate dispatch capacity, page sizes and progress settings.
    ///
    /// # Errors
    /// `StreamError::LimitExceeded` names any out-of-range budget.
    pub fn validate(&self) -> Result<(), StreamError> {
        let ok = self.requests > 0
            && self.prefetch_requests < self.requests
            && self.follow_requests > 0
            && (1..=acyclic_stream::MAX_ITEMS as u64).contains(&u64::from(self.read_limit))
            && self.listing_limit > 0
            && self.lag >= 1
            && self.deliveries > 0
            && self.timeout_ms.is_none_or(|timeout| timeout > 0)
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

pub use acyclic_stream::demand::Want;

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
    /// `children_page(parent, offset = index * limit, limit, tail_stream)`.
    Children {
        /// Completion identity.
        id: u64,
        /// Parent.
        parent: StreamPath,
        /// Page index this fills.
        index: u32,
        /// Child bound.
        limit: u32,
        /// Relative stream projected under each child.
        tail: Option<StreamPath>,
        /// Hierarchy version a noninitial page is read at.
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
    /// Original append stream, distinct from the selected fork path.
    pub origin: StreamPath,
    /// Original commit time in Unix microseconds.
    pub committed_at_micros: u64,
}

/// A listed child and, when projected, its relative stream's last record.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ListedChild {
    /// Child path.
    pub path: StreamPath,
    /// Immediate origin of the listed path, not its projected descendant.
    pub origin: Option<Provenance<StreamPath, ForkCut>>,
    /// The projected stream's last record.
    pub tail: Option<Fetched>,
}

/// The driver's report for one op.
#[derive(Clone, Debug)]
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
    /// The original provider failure; scheduling is derived with [`Failure::of`].
    Failed(StreamError),
}

/// Failure classes the scheduler reacts to.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Failure {
    /// Transport or timeout: back off and retry.
    Unavailable,
    /// The server shed load: back off longer and pause prefetch.
    Overloaded,
    /// A definite refusal: do not retry this operation without acknowledgment.
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
        match error.category() {
            StreamCategory::Unavailable | StreamCategory::DeadlineElapsed => Self::Unavailable,
            StreamCategory::Capacity => Self::Overloaded,
            StreamCategory::HierarchyChanged => Self::Changed,
            StreamCategory::AccessDenied => Self::Denied,
            StreamCategory::PrefixNotRetained => Self::RetentionLost,
            _ => Self::Refused,
        }
    }
}

/// A reader discontinuity that applications must show separately from outages.
#[derive(Clone, Debug, thiserror::Error)]
pub enum ReadIssue {
    /// Authorization failed; cached bytes for the stream were discarded.
    #[error("reader source access denied")]
    AccessDenied,
    /// The resume anchor was lost; history must be loaded from a new cut.
    #[error("reader source retention lost")]
    RetentionLost,
    /// A directory continuation observed concurrent hierarchy changes.
    #[error("reader source hierarchy changed")]
    HierarchyChanged,
    /// The original definite provider refusal; acknowledge before following again.
    #[error("reader source refused: {0}")]
    Refused(#[source] StreamError),
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
    Children(StreamPath, u64, u64, Option<StreamPath>),
}

#[cfg(test)]
thread_local! {
    static OP_PATH_PROJECTIONS: std::cell::Cell<usize> = const { std::cell::Cell::new(0) };
    static WANT_RESOLUTIONS: std::cell::Cell<usize> = const { std::cell::Cell::new(0) };
}

impl OpKey {
    fn path(&self) -> &StreamPath {
        #[cfg(test)]
        OP_PATH_PROJECTIONS.with(|visits| visits.set(visits.get() + 1));
        match self {
            Self::Read(path, _) | Self::Head(path) | Self::Children(path, _, _, _) => path,
        }
    }

    fn children(path: &StreamPath, index: u32, limit: u32, tail: Option<StreamPath>) -> Self {
        Self::Children(path.clone(), u64::from(limit), u64::from(index), tail)
    }

    // Page indexes are u32. The exclusive index boundary includes every
    // projection of the final page without inventing a greatest stream path.
    fn children_end(path: &StreamPath, limit: u32) -> Self {
        Self::Children(
            path.clone(),
            u64::from(limit),
            u64::from(u32::MAX) + 1,
            None,
        )
    }

    fn path_ranges(path: &StreamPath) -> [(std::ops::Bound<Self>, std::ops::Bound<Self>); 3] {
        use std::ops::Bound::{Excluded, Included};
        [
            (
                Included(Self::Read(path.clone(), 0)),
                Included(Self::Read(path.clone(), u64::MAX)),
            ),
            (
                Included(Self::Head(path.clone())),
                Included(Self::Head(path.clone())),
            ),
            (
                Included(Self::Children(path.clone(), 0, 0, None)),
                Excluded(Self::Children(
                    path.clone(),
                    u64::from(u32::MAX) + 1,
                    0,
                    None,
                )),
            ),
        ]
    }
}

/// How a listing op is read and when its page goes stale.
#[derive(Debug, Default)]
struct Paging {
    /// Original witness paths and positions admitted before this request.
    /// Wants for the same projection share the original request, never a
    /// later witness observation attached to an already running operation.
    witnesses: BTreeMap<Option<StreamPath>, Option<u64>>,
}

struct InFlight {
    key: OpKey,
    priority: Priority,
    /// Original rotated age, retained even when the want is withdrawn.
    ticket: u64,
    deadline: Option<u64>,
    issued: u64,
    paging: Paging,
    observed: Option<(u64, u64)>,
}

#[cfg(test)]
std::thread_local! {
    static DEADLINE_READS: std::cell::Cell<usize> = const { std::cell::Cell::new(0) };
}

impl InFlight {
    fn deadline(&self) -> Option<u64> {
        #[cfg(test)]
        DEADLINE_READS.with(|reads| reads.set(reads.get() + 1));
        self.deadline
    }
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

struct FollowIntent {
    from: u64,
    selected: bool,
}

/// Footprint of one client, for tests and diagnostics.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct Residency {
    /// Staged cache charge awaiting source admission, under the cache budget.
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
}

/// One client's scheduler and cache.
pub struct Reader {
    identity: std::sync::Arc<()>,
    pub(super) source_generation: u64,
    policy: ReaderPolicy,
    cache: RecordCache,
    credentials: Option<Credentials>,
    pending_snapshot: Option<cache::PendingSnapshot>,
    driver: Option<std::sync::Arc<()>>,
    demand: Demands,
    next_ticket: u64,
    inflight: Requests,
    cancelled: Vec<u64>,
    refused: BTreeMap<OpKey, u64>,
    issues: BTreeMap<StreamPath, ReadIssue>,
    follows: BTreeMap<StreamPath, FollowIntent>,
    follow_mark: bool,
    #[cfg(test)]
    follow_insertions: usize,
    #[cfg(test)]
    follow_selection_visits: usize,
    follow_changes: BTreeSet<StreamPath>,
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
            demand: Demands::default(),
            next_ticket: 0,
            inflight: Requests::default(),
            cancelled: Vec::new(),
            refused: BTreeMap::new(),
            issues: BTreeMap::new(),
            follows: BTreeMap::new(),
            follow_mark: false,
            #[cfg(test)]
            follow_insertions: 0,
            #[cfg(test)]
            follow_selection_visits: 0,
            follow_changes: BTreeSet::new(),
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
        self.cancelled.extend(self.inflight.drain());
        self.demand.clear();
        self.refused.clear();
        self.issues.clear();
        self.follows.clear();
        self.follow_changes.clear();
        self.hierarchy = None;
        self.failures = 0;
        self.since = None;
        self.retry_at = 0;
        self.follow_backoff = false;
        self.prefetch_paused_until = 0;
        let access = self
            .credentials
            .as_ref()
            .map_or(Access::Local, Credentials::access);
        self.cache.reset_access(access);
        match self.credential_status() {
            CredentialStatus::Current => {
                if let Some(snapshot) = self.pending_snapshot.take() {
                    // A cache miss is harmless; it never authenticates a source
                    // or changes the requested observation's logical outcome.
                    let _ = self.cache.install_snapshot(snapshot);
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

    /// Persist the cache. `None` defers persistence while source admission is
    /// pending, so the caller retains its previous saved snapshot.
    #[must_use]
    pub fn snapshot(&self) -> Option<Vec<u8>> {
        if self.credentials.as_ref().is_some_and(Credentials::pending) {
            None
        } else {
            Some(self.cache.snapshot())
        }
    }

    /// Warm start from persisted bytes. While the registered source resolves,
    /// retain private cache residency for atomic validation after lease admission.
    ///
    /// # Errors
    /// Refuses malformed bytes or replacement while reads or follows are active.
    pub fn restore(&mut self, bytes: &[u8]) -> Result<(), SnapshotError> {
        if !self.inflight.is_empty() || !self.follows.is_empty() || !self.demand.is_empty() {
            return Err(SnapshotError);
        }
        if self.credential_status() == CredentialStatus::Pending {
            self.pending_snapshot = Some(self.cache.prepare_snapshot(bytes)?);
            self.generation += 1;
            return Ok(());
        }
        self.cache.restore(bytes)?;
        self.generation += 1;
        Ok(())
    }

    /// Set the exact cache byte budget, including zero under memory pressure.
    /// New wanted results exceeding this budget use pass-through.
    pub fn set_cache_budget(&mut self, bytes: usize) {
        self.policy.cache_bytes = bytes;
        self.cache.set_budget(bytes);
        if self
            .pending_snapshot
            .as_mut()
            .is_some_and(|snapshot| !snapshot.set_budget(bytes))
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
            snapshot_bytes: self
                .pending_snapshot
                .as_ref()
                .map_or(0, cache::PendingSnapshot::bytes),
            cache_bytes: self.cache.bytes(),
            passed_bytes: self.cache.passed_bytes(),
            backing_bytes: self.cache.backing_bytes(),
            streams: self.cache.streams(),
            cache_entries: self.cache.len(),
            requests: self.inflight.len(),
            follows: self.follows.len(),
            demand: self.demand.len(),
        }
    }

    /// Begin a frame: expire overdue reads, reset per-frame budgets and mark
    /// recency so entries displayed this frame are protected from prefetch.
    /// Time observations never move the reader's admission clock backwards.
    pub fn begin_frame(&mut self, now: u64) {
        self.now = self.now.max(now);
        let now = self.now;
        self.delivered = 0;
        self.frame_mark = self.cache.begin_frame();
        // IDs follow monotonic admission time and one immutable timeout.
        // Their existing order is therefore also the deadline order.
        while let Some((&id, flight)) = self.inflight.first_key_value() {
            if flight.deadline().is_none_or(|deadline| deadline > now) {
                break;
            }
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
        // Keep the first admission order, but resolve and retain each exact
        // intent only once at its strongest supplied priority.
        let mut positions = BTreeMap::<Want, usize>::new();
        let mut retained = Vec::<(Want, Priority)>::new();
        for (want, priority) in wants {
            match positions.entry(want) {
                std::collections::btree_map::Entry::Occupied(entry) => {
                    // Every position was inserted with the corresponding push;
                    // the vector does not remove entries during admission.
                    if let Some((_, retained_priority)) = retained.get_mut(*entry.get()) {
                        *retained_priority = (*retained_priority).min(priority);
                    }
                }
                std::collections::btree_map::Entry::Vacant(entry) => {
                    retained.push((entry.key().clone(), priority));
                    entry.insert(retained.len() - 1);
                }
            }
        }
        drop(positions);
        let pass_mark = self.cache.clock();
        for (want, priority) in &retained {
            if *priority == Priority::Visible {
                self.touch_want(want, true);
            }
        }
        for (want, priority) in &retained {
            if *priority == Priority::Prefetch {
                self.touch_want(want, false);
            }
        }
        self.cache.retain_passed(pass_mark);
        self.demand.begin_plan();
        for (want, priority) in retained {
            let Some((key, paging)) = self.resolve(&want) else {
                continue;
            };
            // Reclassify only the original request for this resolved intent.
            // Multiple projections keep their strongest admitted priority.
            let inflight = &mut self.inflight;
            let refused = &self.refused;
            self.demand.want(
                key,
                priority,
                paging,
                &mut self.next_ticket,
                |key, priority| {
                    inflight
                        .set_priority(key, priority)
                        .or_else(|| refused.get(key).copied())
                },
            );
        }
        for key in self.demand.finish_plan() {
            if let Some(id) = self.inflight.withdraw_prefetch(&key) {
                self.cancelled.push(id);
            }
        }
    }

    /// Retain a want's pass-throughs; only displayed wants protect cached
    /// values and facts against eviction.
    fn touch_want(&mut self, want: &Want, displayed: bool) {
        if let Some((stream, from, to)) = window(&self.cache, want) {
            let stream = stream.clone();
            if displayed {
                self.cache.touch_range(&stream, from, to);
            } else {
                self.cache.retain_range(&stream, from, to);
            }
        }
        if let Want::Listing {
            parent,
            index,
            limit,
            witness,
            ..
        } = want
        {
            for (stream, last) in projected(&self.cache, want) {
                if displayed {
                    self.cache.touch_record(&stream, last);
                } else {
                    self.cache.retain_range(&stream, last, last + 1);
                }
            }
            if displayed {
                self.cache.touch_listing(parent, *index, *limit);
                self.cache.touch_stream(parent);
                if let Some(witness) = witness {
                    self.cache.touch_stream(witness);
                }
            } else {
                self.cache.retain_listing(parent, *index, *limit);
            }
        }
    }

    /// The next uncached read for one want, or `None` when it is satisfied.
    fn resolve(&self, want: &Want) -> Option<(OpKey, Paging)> {
        #[cfg(test)]
        WANT_RESOLUTIONS.with(|visits| visits.set(visits.get() + 1));
        let path = match want {
            Want::Range { stream, .. } | Want::Last { stream, .. } => stream,
            Want::Listing { parent, .. } => parent,
        };
        if matches!(self.issues.get(path), Some(ReadIssue::AccessDenied)) {
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
                limit,
                tail,
                witness,
            } => {
                if *limit == 0 {
                    return None;
                }
                // A live listing must learn its witness before declaring the
                // page fresh. In particular, an empty page has no child heads
                // to discover it indirectly; without this head no follow can
                // be opened and the first child would remain invisible.
                if let Some(witness) = witness
                    && self.cache.tail(witness).is_none()
                    // A missing witness does not make the subtree absent.
                    // Cache its page without a freshness position, retaining
                    // the original refusal until the caller acknowledges it.
                    && !matches!(
                        self.issues.get(witness),
                        Some(ReadIssue::Refused(error)) if error.category() == StreamCategory::NotFound
                    )
                {
                    return Some((OpKey::Head(witness.clone()), Paging::default()));
                }
                let projection = ListingProjection {
                    tail: tail.clone(),
                    witness: witness.clone(),
                };
                let witnessed = witness.as_ref().and_then(|w| self.cache.tail(w));
                if let Some(listing) = self.cache.listing(parent, *index, *limit)
                    && listing.fresh
                    && listing.projections.get(&projection) == Some(&witnessed)
                {
                    return None;
                }
                Some((
                    OpKey::children(parent, *index, *limit, tail.clone()),
                    Paging {
                        witnesses: BTreeMap::from([(witness.clone(), witnessed)]),
                    },
                ))
            }
        }
    }

    /// Reads to start now, in aged priority order, within every budget.
    /// Older time samples cannot admit a deadline before an earlier request.
    pub fn poll(&mut self, now: u64) -> Vec<Op> {
        self.now = self.now.max(now);
        let now = self.now;
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
        let mut demand = self.demand.ordered();
        while let Some((key, pending)) = demand.next_admitted(
            prefetch && self.inflight.prefetch() < self.policy.prefetch_requests,
            self.policy.requests,
        ) {
            let Demand { priority, .. } = pending;
            if self.inflight.len() >= limit {
                break;
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
                // One cold page establishes the hierarchy cut. Every subsequent
                // offset request is fenced to it, without fetching page zero.
                OpKey::Children(..) if self.hierarchy.is_none() && self.inflight.has_children() => {
                    continue;
                }
                OpKey::Children(parent, limit, index, tail) => {
                    let (Ok(index), Ok(limit)) = (u32::try_from(*index), u32::try_from(*limit))
                    else {
                        continue;
                    };
                    Op::Children {
                        id,
                        parent: parent.clone(),
                        index,
                        limit,
                        tail: tail.clone(),
                        version: (index != 0).then_some(self.hierarchy).flatten(),
                    }
                }
            };
            self.inflight.insert(
                id,
                InFlight {
                    key: key.clone(),
                    priority: *priority,
                    ticket: 0,
                    deadline: self
                        .policy
                        .timeout_ms
                        .map(|timeout| now.saturating_add(timeout)),
                    issued: now,
                    paging: Paging::default(),
                    observed: self.cache.observation(key.path()),
                },
            );
            ops.push(op);
        }
        ops.retain(|op| {
            self.inflight
                .dispatch(&op.id(), &mut self.demand, &mut self.next_ticket)
        });
        self.issued += ops.len() as u64;
        ops
    }

    /// Reads the driver must abandon (timed out or no longer wanted).
    pub fn take_cancelled(&mut self) -> Vec<u64> {
        std::mem::take(&mut self.cancelled)
    }

    /// Fold one op's outcome into the cache. Late or cancelled outcomes are
    /// ignored; malformed ones count as refusals.
    pub fn complete(&mut self, id: u64, outcome: Outcome, now: u64) {
        let Some(mut flight) = self.inflight.remove(&id) else {
            return;
        };
        if !self.readable() {
            return;
        }
        self.generation += 1;
        let rtt = now.saturating_sub(flight.issued);
        self.rtt_ms = if self.rtt_ms == 0 {
            rtt
        } else {
            // Divide the whole and remainder terms separately so the weighted mean
            // stays in range and rounds down once.
            (self.rtt_ms / 8) * 7 + rtt / 8 + ((self.rtt_ms % 8) * 7 + rtt % 8) / 8
        };
        let protect = Some(self.frame_mark);
        let accepted = match (&flight.key, outcome) {
            (_, Outcome::Failed(error)) if error.category() == StreamCategory::HierarchyChanged => {
                // This witness covers the whole hierarchy, not just the page
                // reporting it. Old continuations cannot be rebound to a new
                // version by an intervening reply or another driver wake.
                self.hierarchy = None;
                self.cache.clear_listings();
                self.demand.clear_children();
                // Children is the final operation variant in the same key
                // index used at admission. Earlier history work is untouched.
                while let Some(id) = self.inflight.pop_children() {
                    self.cancelled.push(id);
                }
                self.report_issue(flight.key.path(), ReadIssue::HierarchyChanged);
                return;
            }
            (_, Outcome::Failed(error))
                if error.category() == StreamCategory::PrefixNotRetained =>
            {
                self.replaced(flight.key.path());
                return;
            }
            (_, Outcome::Failed(error)) => {
                let failure = Failure::of(&error);
                if failure != Failure::Denied {
                    self.demand.restore_paging(
                        &flight.key,
                        std::mem::take(&mut flight.paging),
                        failure != Failure::Refused,
                    );
                }
                if failure == Failure::Denied {
                    self.denied(flight.key.path());
                } else if failure == Failure::Refused {
                    self.report_issue(flight.key.path(), ReadIssue::Refused(error));
                }
                if matches!(failure, Failure::Refused | Failure::Denied) {
                    self.refused.insert(flight.key.clone(), flight.ticket);
                }
                self.fail(failure, now);
                return;
            }
            (OpKey::Read(stream, from), Outcome::Records(records)) => {
                let valid = records.len() <= self.policy.read_limit as usize
                    // Validate the exclusive end once before per-row arithmetic
                    // or mutation. The admitted count also bounds each offset.
                    && from.checked_add(records.len() as u64).is_some()
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
                OpKey::Children(parent, limit, index, tail),
                Outcome::Children {
                    children,
                    next,
                    version,
                },
            ) => {
                let (Ok(index), Ok(limit)) = (u32::try_from(*index), u32::try_from(*limit)) else {
                    return;
                };
                let valid = children.len() as u64 <= u64::from(limit)
                    && children.iter().all(|child| {
                        child
                            .tail
                            .as_ref()
                            .is_none_or(|row| row.sequence < u64::MAX)
                            && child
                                .origin
                                .as_ref()
                                .is_none_or(|origin| origin.source != child.path)
                    })
                    && children
                        .array_windows::<2>()
                        .all(|[left, right]| left.path.as_str() < right.path.as_str());
                if valid {
                    if self.hierarchy.is_some_and(|selected| selected != version) {
                        self.cache.clear_listings();
                        self.demand.clear_children();
                        while let Some(id) = self.inflight.pop_children() {
                            self.cancelled.push(id);
                        }
                    }
                    self.hierarchy = Some(version);
                    self.listed(
                        parent,
                        (index, limit),
                        (std::mem::take(&mut flight.paging), tail),
                        children,
                        next,
                        protect,
                    );
                }
                valid
            }
            _ => false,
        };
        if accepted {
            // The driver may wake again before the renderer plans its next
            // frame. Completed work must not be dispatched a second time.
            self.demand.remove(&flight.key);
            self.recovered();
        } else {
            self.demand
                .restore_paging(&flight.key, flight.paging, false);
            self.report_issue(
                flight.key.path(),
                ReadIssue::Refused(StreamError::InvalidResponse),
            );
            self.refused.insert(flight.key, flight.ticket);
        }
    }

    fn listed(
        &mut self,
        parent: &StreamPath,
        (index, limit): (u32, u32),
        (paging, tail): (Paging, &Option<StreamPath>),
        children: Vec<ListedChild>,
        next: Option<StreamPath>,
        protect: Option<u64>,
    ) {
        let mut paths = Vec::with_capacity(children.len());
        for child in children {
            self.learn_origin(&child.path, child.origin);
            if let Some(relative) = tail
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
            .listing(parent, index, limit)
            .is_some_and(|old| old.next != next);
        if moved {
            self.cache.drop_listings_after(parent, index, limit);
            if let Some(first) = index.checked_add(1) {
                self.cancel_range(
                    OpKey::children(parent, first, limit, None)..OpKey::children_end(parent, limit),
                );
            }
            // The asynchronous driver can poll again before the next UI frame
            // replans demand. Do not reissue those same obsolete boundaries.
            self.demand.remove_children_after(parent, index, limit);
        }
        self.cache.observe_listing(
            parent,
            index,
            limit,
            Listing {
                after: None,
                children: paths,
                next,
                fresh: true,
                projections: ListingProjections::default(),
                version: self.hierarchy,
            },
            paging.witnesses.into_iter().map(|(witness, witnessed)| {
                (
                    ListingProjection {
                        tail: tail.clone(),
                        witness,
                    },
                    witnessed,
                )
            }),
            protect,
        );
    }

    fn keep(&mut self, stream: &StreamPath, record: Fetched, protect: Option<u64>) {
        self.cache
            .note_commit(stream, record.sequence, record.commit);
        self.cache.insert_record(
            stream,
            record.sequence,
            record.value,
            Some(RecordEvidence {
                origin: record.origin,
                sequence: record.sequence,
                commit: record.commit,
                committed_at_micros: record.committed_at_micros,
            }),
            protect,
        );
    }

    fn learn_origin(
        &mut self,
        stream: &StreamPath,
        origin: Option<Provenance<StreamPath, ForkCut>>,
    ) {
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
            && head.origin.as_ref().is_none_or(|origin| {
                origin.source != *stream && origin.inherited.forked_at <= head.tail
            });
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
                self.keep(stream, driver::fetched(last), Some(self.frame_mark));
            }
            self.generation += 1;
            HeadObservation::Applied
        } else {
            HeadObservation::Invalid
        }
    }

    fn fail(&mut self, failure: Failure, now: u64) -> bool {
        let overloaded = match failure {
            Failure::Unavailable => false,
            Failure::Overloaded => true,
            Failure::Refused | Failure::Denied | Failure::RetentionLost | Failure::Changed => {
                return false;
            }
        };
        self.failures = self.failures.saturating_add(1);
        self.since.get_or_insert(now);
        let backoff = acyclic_auth::operation::Backoff::ceiling_ms_at(
            self.policy.backoff_min_ms,
            self.policy.backoff_max_ms,
            (self.failures - 1).min(20),
        );
        // Deterministic jitter spreads reconnecting clients apart.
        let sample = self.next_id.wrapping_mul(0x9E37_79B9);
        self.retry_at = now.saturating_add(acyclic_auth::operation::Backoff::reconnect_delay_ms(
            backoff, sample,
        ));
        if overloaded {
            self.prefetch_paused_until = now.saturating_add(backoff.saturating_mul(4));
        }
        true
    }

    fn recovered(&mut self) -> bool {
        if self.failures == 0 {
            return false;
        }
        self.failures = 0;
        self.since = None;
        self.retry_at = 0;
        true
    }

    /// Replace the live tails to follow. Each starts at the cache's tail, so a
    /// reconnect is just a follow from the last cached position. A follow
    /// whose stream tail ran more than `lag` ahead jumps to the last records;
    /// nothing is dropped, earlier records remain readable on demand.
    pub fn set_follows(&mut self, streams: impl IntoIterator<Item = StreamPath>) {
        self.follow_mark = !self.follow_mark;
        for stream in streams {
            self.set_follow(stream, true);
        }
        self.follows.retain(|path, intent| {
            #[cfg(test)]
            {
                self.follow_selection_visits += 1;
            }
            if intent.selected == self.follow_mark {
                true
            } else {
                self.follow_changes.insert(path.clone());
                false
            }
        });
    }

    /// Update one live-tail selection without visiting unrelated selections.
    /// Selecting uses the same observed tail, resume position and lag rule as
    /// [`Self::set_follows`]; a missing or denied tail is not admitted. Repeating
    /// an unchanged selection neither reopens its source nor consumes credit.
    /// Removing it releases its original pending or open source when the driver
    /// next runs, including while delivery or opening credit is exhausted.
    ///
    /// Work is keyed O(log selected streams), plus the original cache lookups.
    /// Full replacement still examines every supplied and retained selection.
    pub fn set_follow(&mut self, stream: StreamPath, selected: bool) {
        use std::collections::btree_map::Entry;
        #[cfg(test)]
        {
            self.follow_selection_visits += 1;
        }
        if !selected {
            if self.follows.remove(&stream).is_some() {
                self.follow_changes.insert(stream);
            }
            return;
        }
        if matches!(
            self.issues.get(&stream),
            Some(ReadIssue::AccessDenied | ReadIssue::Refused(_))
        ) {
            return;
        }
        let Some(tail) = self.cache.tail(&stream) else {
            return;
        };
        let resume = self.cache.resume_from(&stream);
        match self.follows.entry(stream) {
            Entry::Occupied(mut entry) => {
                let intent = entry.get_mut();
                let from = resume.unwrap_or_else(|| {
                    if tail.saturating_sub(intent.from) <= self.policy.lag {
                        intent.from
                    } else {
                        tail
                    }
                });
                intent.selected = self.follow_mark;
                if intent.from != from {
                    intent.from = from;
                    self.follow_changes.insert(entry.key().clone());
                }
            }
            Entry::Vacant(entry) => {
                #[cfg(test)]
                {
                    self.follow_insertions += 1;
                }
                self.follow_changes.insert(entry.key().clone());
                entry.insert(FollowIntent {
                    from: resume.unwrap_or(tail),
                    selected: self.follow_mark,
                });
            }
        }
    }

    /// Wanted follows and their current start positions.
    pub fn follows(&self) -> impl Iterator<Item = (&StreamPath, u64)> {
        self.follows
            .iter()
            .map(|(path, intent)| (path, intent.from))
    }

    #[cfg(test)]
    pub(crate) fn take_follow_selection_visits(&mut self) -> usize {
        std::mem::take(&mut self.follow_selection_visits)
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
        let Some(next) = record
            .sequence
            .checked_add(1)
            .filter(|next| *next <= accepted)
        else {
            self.refuse_follow(stream, StreamError::InvalidResponse);
            return;
        };
        self.delivered += 1;
        self.generation += 1;
        self.follow_backoff = false;
        self.recovered();
        self.cache.observe_tail(stream, accepted);
        if let Some(intent) = self.follows.get_mut(stream) {
            intent.from = intent.from.max(next);
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

    /// Unacknowledged read failures and cursor discontinuities. Source replacement
    /// retires its issues; unrelated failures cannot acknowledge them.
    pub fn issues(&self) -> impl Iterator<Item = (&StreamPath, ReadIssue)> {
        self.issues
            .iter()
            .map(|(path, issue)| (path, issue.clone()))
    }

    /// The latest unacknowledged issue for one selected source, borrowed from
    /// the existing keyed issue index. Operation retry custody remains separate.
    pub fn issue(&self, stream: &StreamPath) -> Option<&ReadIssue> {
        self.issues.get(stream)
    }

    /// Acknowledge a displayed issue. Retry a denial only after renewing authority.
    pub fn acknowledge_issue(&mut self, stream: &StreamPath) {
        self.issues.remove(stream);
        for range in OpKey::path_ranges(stream) {
            for (key, _) in self.refused.extract_if(range, |_, _| true) {
                self.demand.set_ready(&key, true);
            }
        }
        self.generation += 1;
    }

    fn report_issue(&mut self, stream: &StreamPath, issue: ReadIssue) {
        self.issues.insert(stream.clone(), issue);
    }

    fn refuse_follow(&mut self, stream: &StreamPath, error: StreamError) {
        self.generation += 1;
        self.report_issue(stream, ReadIssue::Refused(error));
        if self.follows.remove(stream).is_some() {
            self.follow_changes.insert(stream.clone());
        }
    }

    /// Refuse further delivery under a denied stream's old authority.
    pub fn denied(&mut self, stream: &StreamPath) {
        self.generation += 1;
        self.report_issue(stream, ReadIssue::AccessDenied);
        self.invalidate(stream, false);
    }

    fn invalidate(&mut self, stream: &StreamPath, inherited: bool) {
        let affected = self.cache.remove_stream(stream, inherited);
        for path in &affected {
            self.demand.remove_path(path);
            if self.follows.remove(path).is_some() {
                self.follow_changes.insert(path.clone());
            }
            for range in OpKey::path_ranges(path) {
                self.cancel_range(range);
            }
        }
    }

    /// Retire only requests in the original operation-key range. Removing
    /// both indexes together makes late completions inert before cancellation
    /// reaches the driver, without allocating an intermediate identity list.
    fn cancel_range(&mut self, range: impl std::ops::RangeBounds<OpKey>) {
        self.cancelled.extend(self.inflight.remove_range(range));
    }

    fn cancel(&mut self, id: u64) -> bool {
        let Some(flight) = self.inflight.remove(&id) else {
            return false;
        };
        self.demand.restore_paging(&flight.key, flight.paging, true);
        self.cancelled.push(id);
        true
    }

    /// Whether connections may be attempted now (outside a backoff).
    #[must_use]
    pub fn ready(&self, now: u64) -> bool {
        self.readable() && now >= self.retry_at
    }

    /// A live follow failed to open or ended: only transient failures back off,
    /// as with finite reads. Definite failures retain their source disposition.
    pub fn follow_failed(&mut self, failure: Failure, now: u64) {
        if self.fail(failure, now) {
            self.generation += 1;
            self.follow_backoff = true;
        }
    }

    /// A wanted follow opened: it no longer waits on the retry deadline.
    pub fn follow_opened(&mut self, stream: &StreamPath, from: u64) {
        self.cache.resumed(stream, from);
        self.follow_backoff = false;
        if self.recovered() {
            self.generation += 1;
        }
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
        let read = self
            .inflight
            .first_key_value()
            .and_then(|(_, flight)| flight.deadline());
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
        limit,
        tail: Some(relative),
        ..
    } = want
    else {
        return Vec::new();
    };
    let Some(listing) = cache.listing(parent, *index, *limit) else {
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

#[cfg(test)]
mod reader_tests {
    use super::*;

    #[test]
    fn all_requested_reads_and_follows_are_reachable_without_default_ceilings() {
        let mut reader = Reader::new(ReaderPolicy {
            requests: 5_000,
            prefetch_requests: 0,
            cache_bytes: 0,
            streams: 0,
            ..ReaderPolicy::default()
        })
        .unwrap();
        let streams: Vec<_> = (0..5_000)
            .map(|index| StreamPath::new(format!("test/{index}")).unwrap())
            .collect();
        reader.plan(streams.iter().map(|stream| {
            (
                Want::Last {
                    stream: stream.clone(),
                    count: 1,
                },
                Priority::Visible,
            )
        }));
        let ops = reader.poll(0);
        assert_eq!(ops.len(), streams.len());
        reader.begin_frame(20_000);
        assert!(
            reader.take_cancelled().is_empty(),
            "slow reads retain their original credit"
        );
        for op in ops {
            reader.complete(
                op.id(),
                Outcome::Head(head(0, CommitId::from_bytes([0; 32]), b"", None)),
                20_000,
            );
        }
        reader.set_follows(streams.clone());
        let followed: BTreeSet<_> = reader.follows().map(|(stream, _)| stream.clone()).collect();
        assert_eq!(followed, streams.into_iter().collect());
    }

    fn head(
        tail: u64,
        commit: CommitId,
        value: &[u8],
        origin: Option<Provenance<StreamPath, ForkCut>>,
    ) -> PathHead {
        PathHead {
            changed_at: 0,
            retained_through: None,
            tail,
            durable: 0,
            origin,
            last: tail.checked_sub(1).map(|sequence| acyclic_stream::Record {
                sequence,
                value: value.to_vec().into(),
                commit_id: commit,
                committed_at_micros: 0,
                origin: acyclic_stream::StreamPath::new("origin").unwrap(),
                headers: Default::default(),
            }),
        }
    }

    #[test]
    fn all_4097_wants_complete_through_request_credit() {
        let stream = StreamPath::new("test/many-wants").unwrap();
        let mut reader = Reader::new(ReaderPolicy {
            read_limit: 1,
            prefetch_requests: 0,
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
        // Move retained requests between priority indexes, reverse renderer
        // order, and repeat each key at lower priority. None resets its age
        // or allows the later duplicate to downgrade visible demand.
        reader.plan(
            wants
                .iter()
                .map(|(want, _)| (want.clone(), Priority::Prefetch)),
        );
        reader.plan(
            wants.iter().cloned().rev().chain(
                wants
                    .iter()
                    .map(|(want, _)| (want.clone(), Priority::Prefetch)),
            ),
        );
        assert_eq!(reader.residency().demand, 4097);
        let dormant: Vec<_> = (0..4097)
            .map(|index| {
                (
                    Want::Last {
                        stream: StreamPath::new(format!("test/dormant-{index}")).unwrap(),
                        count: 1,
                    },
                    Priority::Prefetch,
                )
            })
            .collect();
        reader.plan(
            wants
                .iter()
                .cloned()
                .rev()
                .chain(
                    wants
                        .iter()
                        .map(|(want, _)| (want.clone(), Priority::Prefetch)),
                )
                .chain(dormant.iter().cloned()),
        );
        assert_eq!(reader.residency().demand, 8194);
        let mut completed = BTreeSet::new();
        for now in 1..=4097 {
            // The driver can consume successive credit windows before the
            // renderer plans another frame. A completion retires that key.
            let ops = reader.poll(now);
            let visits = reader.demand.take_visits();
            assert!(
                visits <= ops.len() + 1,
                "dispatch visited {visits} candidates for {} requests",
                ops.len()
            );
            if ops.is_empty() {
                break;
            }
            assert!(ops.len() <= reader.policy.requests);
            if completed.is_empty() {
                assert_eq!(
                    ops.iter()
                        .map(|op| match op {
                            Op::Read { from, .. } => *from,
                            _ => panic!("known-origin range read"),
                        })
                        .collect::<Vec<_>>(),
                    (0..ops.len() as u64)
                        .map(|index| index * 2)
                        .collect::<Vec<_>>()
                );
            }
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
        assert_eq!(reader.residency().demand, dormant.len());
        assert!(reader.poll(4098).is_empty());
        assert_eq!(reader.demand.take_visits(), 0);
        assert!(frame(&mut reader, 4098, &wants).is_empty());
    }

    fn fetched(sequence: u64, len: usize) -> Fetched {
        Fetched {
            sequence,
            value: OwnedBytes::from(vec![7; len]),
            commit: CommitId::from_bytes([1; 32]),
            origin: StreamPath::new("test/generated").unwrap(),
            committed_at_micros: 0,
        }
    }

    /// Plans `wants` for one frame and returns the reads it issues.
    fn frame(reader: &mut Reader, now: u64, wants: &[(Want, Priority)]) -> Vec<Op> {
        reader.begin_frame(now);
        reader.plan(wants.iter().cloned());
        reader.poll(now)
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
}
