use super::*;

/// Opaque content-bound identity of one committed envelope.
#[derive(Clone, Copy, Debug, Default, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct CommitId([u8; 32]);

impl CommitId {
    /// Constructs an ID from exact bytes.
    #[must_use]
    pub const fn from_bytes(bytes: [u8; 32]) -> Self {
        Self(bytes)
    }

    /// Returns exact bytes.
    #[must_use]
    pub const fn as_bytes(&self) -> &[u8; 32] {
        &self.0
    }
}

/// Account-scoped stable retry identity.
#[derive(Clone, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct IdempotencyKey(pub(crate) OwnedBytes);

impl IdempotencyKey {
    /// Constructs a nonempty bounded key.
    pub fn new(value: impl Into<OwnedBytes>) -> Result<Self, StreamError> {
        let value = value.into();
        if value.is_empty() || value.len() > MAX_IDEMPOTENCY_KEY_BYTES {
            return Err(StreamError::InvalidArgument);
        }
        Ok(Self(value))
    }

    /// Exact caller bytes.
    #[must_use]
    pub fn as_bytes(&self) -> &[u8] {
        &self.0
    }
}

/// One immutable record.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct Record {
    /// Zero-based sequence.
    pub sequence: u64,
    /// Shared opaque value.
    pub value: OwnedBytes,
    /// Envelope that introduced the record.
    pub commit_id: CommitId,
    /// Stable replicated commit time in Unix microseconds.
    pub committed_at_micros: u64,
}

/// Successful contiguous append.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct AppendReceipt {
    /// First sequence.
    pub start: u64,
    /// Exclusive end sequence.
    pub end: u64,
    /// Resulting tail.
    pub tail: u64,
    /// Immutable envelope identity.
    pub commit_id: CommitId,
}

/// Tail-CAS append outcome.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum AppendOutcome {
    /// Entire batch committed.
    Committed(AppendReceipt),
    /// Tail differed and nothing changed.
    TailConflict {
        /// Linearizable tail observed by the provider.
        actual_tail: u64,
    },
}

/// One atomic append request.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct AppendRequest {
    /// Permanent destination.
    pub path: StreamPath,
    /// Nonempty contiguous batch.
    pub records: Vec<OwnedBytes>,
    /// Optional exact tail condition.
    pub if_tail: Option<u64>,
    /// Optional stable recovery identity.
    pub idempotency_key: Option<IdempotencyKey>,
}

/// Successful O(1) immutable-prefix fork.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ForkReceipt {
    /// Source path.
    pub source: StreamPath,
    /// New destination path.
    pub destination: StreamPath,
    /// Exclusive inherited prefix end.
    pub forked_at: u64,
    /// Initial destination tail.
    pub tail: u64,
    /// Immutable envelope identity.
    pub commit_id: CommitId,
}

/// One fork request. Absence of `at_tail` selects the source tail atomically.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ForkRequest {
    /// Existing source.
    pub source: StreamPath,
    /// Absent destination.
    pub destination: StreamPath,
    /// Optional exact prefix.
    pub at_tail: Option<u64>,
    /// Optional stable recovery identity.
    pub idempotency_key: Option<IdempotencyKey>,
}

/// Required-bounds finite read.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ReadRequest {
    /// Stream path.
    pub path: StreamPath,
    /// First sequence, inclusive.
    pub from: u64,
    /// Nonzero record bound.
    pub limit: u32,
}

/// Atomic observation of the next sequence.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct StreamBounds {
    /// Exclusive end of the currently committed history.
    pub tail: u64,
}

/// One immutable direct child.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct Child {
    /// Exact child path.
    pub path: StreamPath,
}

/// Fixed-snapshot direct-child page request.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ChildrenRequest {
    /// Parent, or `None` for top-level paths.
    pub parent: Option<StreamPath>,
    /// Nonzero result bound.
    pub limit: u32,
}

/// Bounded hierarchy traversal; continuations are valid until the hierarchy changes.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ChildrenPageRequest {
    /// Parent, or `None` for top-level paths.
    pub parent: Option<StreamPath>,
    /// Last path from the preceding page; exclusive.
    pub after: Option<StreamPath>,
    /// Last hierarchy-changing commit returned by the preceding page.
    pub hierarchy_version: Option<CommitId>,
    /// Nonzero result bound.
    pub limit: u32,
}

/// One coherent hierarchy page and its continuation boundary.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ChildrenPage {
    /// Changes only when a path is created.
    pub hierarchy_version: CommitId,
    /// Ordered direct children, at most the requested limit.
    pub children: Vec<Child>,
    /// Last returned path if another page exists.
    pub next_after: Option<StreamPath>,
}

/// Append fact retained in a committed envelope.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct CommittedAppend {
    /// Destination.
    pub path: StreamPath,
    /// First sequence.
    pub start: u64,
    /// Exclusive end.
    pub end: u64,
    /// Resulting tail.
    pub tail: u64,
    /// Records carrying the envelope ID.
    pub records: Vec<Record>,
}

/// Fork fact retained in a committed envelope.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct CommittedFork {
    /// Source.
    pub source: StreamPath,
    /// Destination.
    pub destination: StreamPath,
    /// Exclusive inherited prefix end.
    pub forked_at: u64,
    /// Destination tail after the fork and its records.
    pub tail: u64,
    /// Records appended after the inherited prefix, carrying the envelope ID.
    pub records: Vec<Record>,
}

/// Mutation in a successful immutable envelope.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum CommittedMutation {
    /// Contiguous append.
    Append(CommittedAppend),
    /// Immutable-prefix fork.
    Fork(CommittedFork),
}

/// Complete immutable successful mutation envelope.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct CommittedEnvelope {
    /// Content-bound identity.
    pub commit_id: CommitId,
    /// Canonically ordered mutation facts.
    pub mutations: Vec<CommittedMutation>,
}

/// Exact optimistic condition.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum CommitCondition {
    /// Existing tail must match.
    Tail {
        /// Existing path.
        path: StreamPath,
        /// Required exact tail.
        expected: u64,
    },
    /// Path must not exist.
    Absent {
        /// Permanently named path that must be unused.
        path: StreamPath,
    },
}

/// Mutation in one coordinated commit.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum CommitMutation {
    /// Append one nonempty batch.
    Append {
        /// Destination.
        path: StreamPath,
        /// Opaque records.
        records: Vec<OwnedBytes>,
    },
    /// Forks the selected prefix, then appends its records atomically.
    Fork {
        /// Source.
        source: StreamPath,
        /// Destination.
        destination: StreamPath,
        /// Exact source prefix end.
        at_tail: u64,
        /// Opaque records appended after the prefix; may be empty.
        records: Vec<OwnedBytes>,
    },
}

/// One bounded all-or-nothing optimistic commit.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct CommitRequest {
    /// Exact conditions for every participant.
    pub conditions: Vec<CommitCondition>,
    /// Nonempty mutation set.
    pub mutations: Vec<CommitMutation>,
    /// Required stable recovery identity.
    pub idempotency_key: IdempotencyKey,
}

/// Trusted clock for publication deadlines.
pub trait UnixMillisClock: Send + Sync + 'static {
    /// Current Unix time in milliseconds.
    fn now_unix_millis(&self) -> u64;
}

/// Production wall clock for deadline-aware providers.
#[derive(Clone, Copy, Debug, Default)]
pub struct SystemUnixMillisClock;

impl UnixMillisClock for SystemUnixMillisClock {
    fn now_unix_millis(&self) -> u64 {
        #[cfg(target_arch = "wasm32")]
        {
            let now = js_sys::Date::now().max(0.0).floor();
            #[expect(
                clippy::cast_possible_truncation,
                clippy::cast_sign_loss,
                reason = "JavaScript Date returns f64; the integer cast saturates at u64 bounds"
            )]
            let millis = now as u64;
            millis
        }
        #[cfg(not(target_arch = "wasm32"))]
        {
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .map_or(0, |duration| {
                    u64::try_from(duration.as_millis()).unwrap_or(u64::MAX)
                })
        }
    }
}

/// Failed exact condition.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum CommitConflict {
    /// Tail differed.
    Tail {
        /// Path.
        path: StreamPath,
        /// Required tail.
        expected: u64,
        /// Observed tail, or absence when the path does not exist.
        actual: Option<u64>,
    },
    /// Requested absent path exists.
    Exists {
        /// Path that already exists.
        path: StreamPath,
    },
}

/// Coordinated commit outcome.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum CommitOutcome {
    /// Every mutation committed.
    Committed(CommittedEnvelope),
    /// Nothing changed.
    Conflict(Vec<CommitConflict>),
}

/// Terminal result retained under one account-scoped retry identity.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum IdempotencyOutcome {
    /// Append success or tail conflict.
    Append(AppendOutcome),
    /// Successful immutable-prefix fork.
    Fork(ForkReceipt),
    /// Coordinated commit success or exact conflict.
    Commit(CommitOutcome),
}

/// Exact retained recovery fact for one caller-owned retry identity.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct IdempotencyObservation {
    /// Account-scoped identity supplied by the caller.
    pub idempotency_key: IdempotencyKey,
    /// Digest of the canonical request arguments bound to the identity.
    pub request_digest: [u8; 32],
    /// Original terminal result.
    pub outcome: IdempotencyOutcome,
}

/// Backpressured finite read or long-lived follow.
pub type RecordStream = BoxProviderStream<'static, Result<Record, StreamError>>;
/// Backpressured fixed-snapshot direct-child listing.
pub type ChildStream = futures::stream::BoxStream<'static, Result<Child, StreamError>>;
