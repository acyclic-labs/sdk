/// A nonempty slash-separated path relative to a fork source.
/// Uses the same canonical segment rules as account-relative stream paths.
pub type RelativePath = StreamPath;

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
pub struct IdempotencyKey(pub(crate) Bytes);

impl IdempotencyKey {
    /// Constructs a nonempty bounded key.
    pub fn new(value: impl Into<Bytes>) -> Result<Self, StreamError> {
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

    /// A key no other call in this process has used, for fixtures whose
    /// writes carry no retry identity of their own.
    #[cfg(any(test, feature = "test-support"))]
    #[must_use]
    pub fn unique() -> Self {
        static NEXT: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);
        let next = NEXT.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        Self(Bytes::from(format!("fixture/{next}")))
    }
}

/// One immutable record.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct Record {
    /// Zero-based sequence.
    pub sequence: u64,
    /// Shared opaque value.
    pub value: Bytes,
    /// Envelope that introduced the record.
    pub commit_id: CommitId,
    /// Stable replicated commit time in Unix microseconds.
    pub committed_at_micros: u64,
    /// Canonically sorted indexed keys, authenticated with the value.
    pub headers: RecordHeaders,
}

/// One immutable single-valued indexed header.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct Attribute {
    /// Header name.
    pub name: String,
    /// Opaque header value.
    pub value: Bytes,
}

/// Record value and its indexed headers, admitted atomically on every write path.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct AppendRecord {
    /// Opaque application value.
    pub value: Bytes,
    /// Canonical indexed keys and attributes, sharing their original owner.
    pub headers: RecordHeaders,
}

impl AppendRecord {
    /// An opaque keyed value eligible for LATEST delivery. Attention records
    /// retain their order and are never conflated. Payloads keep their native
    /// application encoding; Streams indexes only these authenticated headers.
    pub fn latest(
        value: impl Into<Bytes>,
        key: impl Into<Bytes>,
        attention: bool,
    ) -> Result<Self, StreamError> {
        Ok(Self {
            value: value.into(),
            headers: RecordHeaders::new(
                vec![key.into()],
                vec![Attribute {
                    name: "attention".into(),
                    value: Bytes::from_static(if attention { b"true" } else { b"false" }),
                }],
            )?,
        })
    }
}

impl From<Bytes> for AppendRecord {
    fn from(value: Bytes) -> Self {
        Self {
            value,
            headers: RecordHeaders::default(),
        }
    }
}

impl From<bytes::Bytes> for AppendRecord {
    fn from(value: bytes::Bytes) -> Self {
        Self::from(Bytes::from(value))
    }
}

impl From<Vec<u8>> for AppendRecord {
    fn from(value: Vec<u8>) -> Self {
        Self::from(Bytes::from(value))
    }
}

impl From<&[u8]> for AppendRecord {
    fn from(value: &[u8]) -> Self {
        Self::from(Bytes::from(value))
    }
}

impl From<&str> for AppendRecord {
    fn from(value: &str) -> Self {
        Self::from(Bytes::from(value))
    }
}

impl From<String> for AppendRecord {
    fn from(value: String) -> Self {
        Self::from(Bytes::from(value))
    }
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
    /// Committed authority revision; never a stream offset.
    pub revision: Revision,
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
    pub records: Vec<AppendRecord>,
    /// Optional exact tail condition.
    pub if_tail: Option<u64>,
    /// Stable recovery identity; a retry with the same key observes the first outcome.
    pub idempotency_key: IdempotencyKey,
}

/// Successful immutable subtree fork, constant in descendant count.
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
    /// Committed authority revision; never a stream offset.
    pub revision: Revision,
}

/// Forks a subtree at one pre-commit cut. `at_tail` selects the root stream
/// prefix; descendants inherit their entire pre-commit histories. Absence of
/// `at_tail` selects the root tail atomically.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ForkRequest {
    /// Segment-relative subtree prefixes omitted from the inherited namespace.
    pub exclude: Vec<RelativePath>,
    /// Existing source.
    pub source: StreamPath,
    /// Absent destination.
    pub destination: StreamPath,
    /// Optional exact prefix.
    pub at_tail: Option<u64>,
    /// Stable recovery identity; a retry with the same key observes the first outcome.
    pub idempotency_key: IdempotencyKey,
}

/// Required-bounds finite read.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ReadRequest {
    /// Optional exact indexed key. Results are ascending matches in
    /// `[from, min(key.before, tail at open))`, bounded by `limit` matches.
    /// EOF proves no further match in that pinned interval. A full page resumes
    /// at the last returned sequence plus one with the same exclusive bound.
    pub key: Option<ReadKeySeek>,
    /// Stream path.
    pub path: StreamPath,
    /// First sequence, inclusive.
    pub from: u64,
    /// Nonzero record bound.
    pub limit: u32,
    /// Minimum applied cut; a lagging provider parks, a foreign one refuses.
    pub at_least: RevisionCut,
}

/// A bounded historical lookup over opaque record header keys.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ReadKeySeek {
    /// Exact bytes, without interpreting the record payload.
    pub key: Bytes,
    /// Exclusive sequence bound; later appends cannot enter this read.
    pub before: u64,
}

impl ReadKeySeek {
    pub(crate) fn validate(&self) -> Result<(), StreamError> {
        if self.key.len() > crate::MAX_RECORD_KEY_BYTES {
            return Err(StreamError::LimitExceeded);
        }
        Ok(())
    }
}

/// Policy for a path absent at follow registration.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub enum WhenAbsent {
    /// Refuse an initially absent stream.
    #[default]
    NotFound,
    /// Observe creation without a gap, retaining the requested sequence.
    /// A missing checked-history anchor still refuses immediately.
    Wait,
}

impl WhenAbsent {
    pub(crate) fn from_wire(value: i32) -> Result<Self, StreamError> {
        match value {
            0 => Ok(Self::NotFound),
            1 => Ok(Self::Wait),
            _ => Err(StreamError::InvalidArgument),
        }
    }
    pub(crate) fn to_wire(self) -> i32 {
        match self {
            Self::NotFound => 0,
            Self::Wait => 1,
        }
    }
}

/// One atomic replay-to-live subscription, including its history fence.
#[derive(Clone, Debug, PartialEq)]
pub struct FollowRequest {
    /// Stream path.
    pub path: StreamPath,
    /// First sequence, inclusive.
    pub from: u64,
    /// Required commit at `from - 1`; missing or changed history refuses immediately.
    pub preceding: Option<CommitId>,
    /// Projection intersected with the authenticated authority ceiling.
    pub filter: crate::FollowFilter,
    /// Emit contiguous skipped-prefix progress. False retains indexed record-only wakes.
    pub progress: bool,
    /// Initial absence policy; deletion after observation remains an error.
    pub when_absent: WhenAbsent,
    /// Minimum applied cut, kept as the floor across every reconnect.
    pub at_least: RevisionCut,
}

impl FollowRequest {
    /// Every record from `from`, unanchored and unfiltered.
    #[must_use]
    pub fn new(path: StreamPath, from: u64) -> Self {
        Self {
            path,
            from,
            preceding: None,
            filter: crate::FollowFilter::default(),
            progress: false,
            when_absent: WhenAbsent::NotFound,
            at_least: RevisionCut::default(),
        }
    }
}

/// Accepted visibility and confirmed persistence at one observation cut.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct StreamWatermarks {
    /// Next sequence accepted by the stream.
    pub accepted: u64,
    /// Next sequence in the confirmed durable prefix; zero makes no persistence claim.
    pub durable: u64,
}

/// Proof that a path's records before `through` are in the provider's
/// irreversible prefix: no crash short of losing the whole stream removes them.
/// Only [`StreamProvider::durable_receipt`] issues it, from a confirmed durable
/// watermark (or, for `MemoryStream`, its retained tail, which it never
/// rolls back). A commit acknowledgement alone never yields one, so writes made
/// under `deferring_durability` cannot be mistaken for durable ones.
///
/// Nothing outside the provider can assemble one:
///
/// ```compile_fail
/// let forged = acyclic_stream::DurableReceipt {
///     path: acyclic_stream::StreamPath::new("forged").unwrap(),
///     through: 1,
/// };
/// ```
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct DurableReceipt {
    path: StreamPath,
    through: u64,
}

impl DurableReceipt {
    /// The covered path.
    pub fn path(&self) -> &StreamPath {
        &self.path
    }

    /// Every record ending at or before this sequence is covered.
    pub fn through(&self) -> u64 {
        self.through
    }

    pub(crate) fn covering(
        path: StreamPath,
        watermarks: &StreamWatermarks,
        through: u64,
    ) -> Option<Self> {
        (through <= watermarks.durable && watermarks.durable <= watermarks.accepted)
            .then_some(Self { path, through })
    }

    pub(crate) fn retained(path: StreamPath, tail: u64, through: u64) -> Option<Self> {
        (through <= tail).then_some(Self { path, through })
    }
}

/// One immutable direct child.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct Child {
    /// Immediate inherited source at the page's immutable namespace cut.
    pub origin: Option<crate::ForkOrigin>,
    /// Last record of the requested relative stream at this page cut.
    pub tail_record: Option<Record>,
    /// Exact child path.
    pub path: StreamPath,
}

/// Bounded hierarchy traversal. A continuation is valid only while the
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ChildrenPageRequest {
    /// Optional relative stream whose last record is projected under each child.
    pub tail_stream: Option<RelativePath>,
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
    /// Canonical segment-relative prefixes omitted at the captured cut.
    pub exclude: Vec<RelativePath>,
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
    /// Publication time, including forks that append no records.
    pub committed_at_micros: u64,
    /// Canonically ordered mutation facts.
    pub mutations: Vec<CommittedMutation>,
}

/// Exact optimistic condition.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum CommitCondition {
    /// Path must exist, including an empty fork root. No head read is required.
    Present {
        /// Permanently named path that must already exist.
        path: StreamPath,
    },
    /// Existing tail and the immutable record immediately before it must match.
    Head {
        /// Existing nonempty path.
        path: StreamPath,
        /// Required exact tail.
        expected: u64,
        /// Required identity of the record at `expected - 1`.
        commit_id: CommitId,
    },
    /// Tail must match; an absent path has tail zero.
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
        records: Vec<AppendRecord>,
    },
    /// Share the entire pre-commit subtree at a new destination. `at_tail`
    /// selects the root prefix; descendants retain their pre-commit heads. Append
    Fork {
        /// Segment-relative prefixes omitted from the inherited namespace.
        exclude: Vec<RelativePath>,
        /// Source.
        source: StreamPath,
        /// Destination.
        destination: StreamPath,
        /// Exact source prefix end.
        at_tail: u64,
        /// Opaque records appended after the prefix; may be empty.
        records: Vec<AppendRecord>,
    },
}

/// One bounded all-or-nothing optimistic commit.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct CommitRequest {
    /// Trusted-clock publication deadline; successful retries keep their original identity.
    pub deadline_unix_millis: Option<u64>,
    /// Exact conditions for every participant.
    pub conditions: Vec<CommitCondition>,
    /// Nonempty mutation set.
    pub mutations: Vec<CommitMutation>,
    /// Required stable recovery identity.
    pub idempotency_key: IdempotencyKey,
}

impl CommitRequest {
    /// Protobuf command size, without allocating or copying the record payloads.
    /// Returns `usize::MAX` if the size cannot be represented on this platform.
    #[must_use]
    pub fn wire_encoded_len(&self) -> usize {
        crate::wire_codec::requests::commit_len(self)
    }
}

/// Trusted clock used by providers to evaluate publication deadlines at the
pub trait UnixMillisClock: Send + Sync + 'static {
    /// Current Unix time in milliseconds.
    fn now_unix_millis(&self) -> u64;
}

/// Production wall clock for deadline-aware providers.
#[derive(Clone, Copy, Debug, Default)]
pub struct SystemUnixMillisClock;

impl UnixMillisClock for SystemUnixMillisClock {
    fn now_unix_millis(&self) -> u64 {
        #[cfg(all(feature = "test-support", not(target_arch = "wasm32")))]
        if let Some(millis) = acyclic_native_runtime::testing::unix_millis() {
            return millis;
        }
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
    /// Requested present path does not exist.
    Missing {
        /// Path that must already exist.
        path: StreamPath,
    },
    /// The tail matched but the preceding record belonged to another history.
    Head {
        /// Path.
        path: StreamPath,
        /// Required preceding commit.
        expected: CommitId,
        /// Observed preceding commit.
        actual: CommitId,
    },
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
    Committed(CommitReceipt),
    /// Nothing changed.
    Conflict(Vec<CommitConflict>),
}

/// Terminal result retained under one account-scoped retry identity.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum IdempotencyOutcome {
    /// Successful append. Failed conditions never retain an outcome.
    Append(AppendOutcome),
    /// Successful immutable-prefix fork.
    Fork(ForkReceipt),
    /// Successful coordinated commit. Conflicts leave the key unspent.
    Commit(CommittedEnvelope),
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
    /// The authority's revision of this publication in its current incarnation;
    /// absent where no authority is bound, such as an encoded snapshot.
    pub revision: Option<Revision>,
}

/// Backpressured finite read or long-lived follow.
pub type RecordStream = BoxProviderStream<'static, Result<Record, StreamError>>;

/// An ordered filtered follow observation. Skipped records carry only a cursor
/// and an immutable history anchor, never their values or indexed headers.
#[derive(Debug)]
pub enum FollowUpdate {
    /// A matching record after provider-side projection.
    Record(Record),
    /// A contiguous skipped prefix before the next matching record.
    Progress {
        /// Exclusive offset of the examined prefix.
        next: u64,
        /// Commit at `next - 1`, including filtered-out records.
        preceding: CommitId,
    },
}

/// Ordered records and skipped-prefix observations from one atomic follow.
pub type FollowStream = BoxProviderStream<'static, Result<FollowUpdate, StreamError>>;

/// Projects records from an existing follow without subscribing again.
pub fn records_only(updates: FollowStream) -> RecordStream {
    use futures::StreamExt as _;
    Box::pin(updates.filter_map(|update| {
        futures::future::ready(match update {
            Ok(FollowUpdate::Record(record)) => Some(Ok(record)),
            Ok(FollowUpdate::Progress { .. }) => None,
            Err(error) => Some(Err(error)),
        })
    }))
}
