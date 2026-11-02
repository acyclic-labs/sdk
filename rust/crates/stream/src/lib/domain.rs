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
    #[cfg(any(test, loom, feature = "test-support"))]
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
    /// Stream that appended the record. A fork shares the record unchanged,
    /// so a follow of the fork sees the source segment's path here.
    pub origin: StreamPath,
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

/// Required-bounds finite read. Invalid bounds or keys refuse before waiting
/// for the minimum revision or observing storage.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ReadRequest {
    /// Optional indexed selector; see [`ReadKeySeek`]. `limit` counts
    /// matches, and EOF proves no further match in the pinned interval.
    /// [`ReadKeySeek::advance`] turns a returned record into the continuation.
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

/// The original request, admitted before adapter IO or revision waits.
pub(crate) struct ValidatedReadRequest(ReadRequest);

impl ReadRequest {
    pub(crate) fn validate(self) -> Result<ValidatedReadRequest, StreamError> {
        crate::memory::validate_limit(self.limit)?;
        if let Some(key) = &self.key {
            key.validate()?;
        }
        Ok(ValidatedReadRequest(self))
    }
}

impl ValidatedReadRequest {
    pub(crate) fn request(&self) -> &ReadRequest {
        &self.0
    }

    pub(crate) fn into_request(self) -> ReadRequest {
        self.0
    }
}

/// Order of an indexed read.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub enum ReadOrder {
    /// Ascending `(key, sequence)` order.
    #[default]
    Ascending,
    /// Descending sequence order under one exact key: the first result is
    /// the latest record with that key, found by one indexed lookup.
    LatestFirst,
}

impl ReadOrder {
    pub(crate) fn from_wire(value: i32) -> Result<Self, StreamError> {
        match value {
            0 => Ok(Self::Ascending),
            1 => Ok(Self::LatestFirst),
            _ => Err(StreamError::InvalidArgument),
        }
    }
    pub(crate) fn to_wire(self) -> i32 {
        match self {
            Self::Ascending => 0,
            Self::LatestFirst => 1,
        }
    }
}

/// A bounded historical lookup over opaque record header keys, through the
/// per-stream key index; no read is proportional to stream length.
///
/// - Exact key (`end: None`): records carrying `key` with sequences in
///   `[from, min(before, tail at open))`, in `order`.
/// - Key range (`end: Some`): index entries `(k, sequence)` with
///   `key <= k < end`, `sequence < min(before, tail at open)`, and
///   `(k, sequence) >= (key, from)`, ascending in `(k, sequence)` order. A
///   record carrying several keys in the range appears once per such key.
///   Ranges are ascending only.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ReadKeySeek {
    /// Exact key, or the inclusive first key of a range. Bytes are compared
    /// lexically, without interpreting the record payload.
    pub key: Bytes,
    /// Exclusive last key of a range; `None` selects `key` alone.
    pub end: Option<Bytes>,
    /// Exclusive sequence bound; later appends cannot enter this read.
    pub before: u64,
    /// Result order.
    pub order: ReadOrder,
}

impl ReadKeySeek {
    /// Ascending matches of one exact key before `before`.
    pub fn exact(key: impl Into<Bytes>, before: u64) -> Self {
        Self {
            key: key.into(),
            end: None,
            before,
            order: ReadOrder::Ascending,
        }
    }

    /// Matches of one exact key before `before`, latest first.
    pub fn latest(key: impl Into<Bytes>, before: u64) -> Self {
        Self {
            order: ReadOrder::LatestFirst,
            ..Self::exact(key, before)
        }
    }

    /// Ascending index entries for every key in `[key, end)` before `before`.
    pub fn range(key: impl Into<Bytes>, end: impl Into<Bytes>, before: u64) -> Self {
        Self {
            end: Some(end.into()),
            ..Self::exact(key, before)
        }
    }

    pub(crate) fn validate(&self) -> Result<(), StreamError> {
        if self.key.len() > crate::MAX_RECORD_KEY_BYTES
            || self
                .end
                .as_ref()
                .is_some_and(|end| end.len() > crate::MAX_RECORD_KEY_BYTES)
        {
            return Err(StreamError::LimitExceeded);
        }
        if self.end.is_some() && self.order == ReadOrder::LatestFirst {
            return Err(StreamError::InvalidArgument);
        }
        Ok(())
    }

    /// Checks one returned record against this read at cursor `from`, then
    /// advances both to the continuation read after it. Every provider and
    /// every client that checks indexed results shares this one rule.
    pub fn advance<K: AsRef<[u8]>>(
        &mut self,
        from: &mut u64,
        sequence: u64,
        keys: &[K],
    ) -> Result<(), StreamError> {
        let selected = match &self.end {
            None => keys
                .iter()
                .any(|key| key.as_ref() == self.key.as_ref())
                .then_some(None),
            Some(end) => keys
                .iter()
                .map(AsRef::as_ref)
                .filter(|key| {
                    *key < end.as_ref()
                        && (*key > self.key.as_ref()
                            || (*key == self.key.as_ref() && sequence >= *from))
                })
                .min()
                .map(Some),
        };
        let Some(selected) = selected else {
            return Err(StreamError::InvalidResponse);
        };
        if sequence >= self.before || (self.end.is_none() && sequence < *from) {
            return Err(StreamError::InvalidResponse);
        }
        if let Some(key) = selected.filter(|key| *key != self.key.as_ref()) {
            self.key = Bytes::copy_from_slice(key);
        }
        match self.order {
            ReadOrder::Ascending => {
                *from = sequence.checked_add(1).ok_or(StreamError::OutOfRange)?;
            }
            ReadOrder::LatestFirst => self.before = sequence,
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
    /// Select the last record and live start from the opening snapshot.
    pub bootstrap_head: bool,
    /// Optional exclusive finite end, clamped to the opening tail.
    pub until: Option<u64>,
    /// Stream path.
    pub path: StreamPath,
    /// First sequence, inclusive.
    pub from: u64,
    /// Required commit at `from - 1`; missing or changed history refuses immediately.
    pub preceding: Option<CommitId>,
    /// Projection intersected with the authenticated authority ceiling.
    pub filter: crate::FollowFilter,
    /// Emit one fixed registration tail, then contiguous skipped-prefix progress.
    /// False retains indexed record-only wakes and emits no registration event.
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
            bootstrap_head: false,
            until: None,
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
/// Only [`StreamProviderExt::durable_receipt`] issues it, from a confirmed durable
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

    #[cfg(any(test, loom))]
    pub(crate) fn covering(
        path: StreamPath,
        watermarks: &StreamWatermarks,
        through: u64,
    ) -> Option<Self> {
        (through <= watermarks.durable && watermarks.durable <= watermarks.accepted)
            .then_some(Self { path, through })
    }
}

/// One immutable direct child.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct Child {
    /// Immediate inherited source at the page's immutable namespace cut.
    pub origin: Option<crate::Provenance<crate::StreamPath, crate::ForkCut>>,
    /// Exclusive tail of the selected relative stream at this page cut.
    /// `None` means that stream is missing (or no stream was selected);
    /// `Some(0)` means it exists and is empty.
    pub tail: Option<u64>,
    /// Last record of the requested relative stream at this page cut.
    pub tail_record: Option<Record>,
    /// Exact child path.
    pub path: StreamPath,
}

impl Child {
    pub(crate) fn validate_tail(&self) -> Result<(), StreamError> {
        let expected = self.tail.and_then(|tail| tail.checked_sub(1));
        if self.tail_record.as_ref().map(|record| record.sequence) != expected {
            return Err(StreamError::InvalidResponse);
        }
        Ok(())
    }
}

/// Bounded direct-child traversal at one authority snapshot per page.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ChildrenPageRequest {
    /// Optional relative stream whose last record is projected under each child.
    pub tail_stream: Option<RelativePath>,
    /// Parent, or `None` for top-level paths.
    pub parent: Option<StreamPath>,
    /// Last path from the preceding page; exclusive.
    pub after: Option<StreamPath>,
    /// Optional hierarchy fence returned by a preceding page. When supplied,
    /// any hierarchy change refuses the page. When absent, `after` seeks in
    /// the current snapshot; callers requiring stable membership must own
    /// its lifetime or retain the fence.
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
    /// Canonically ordered mutation facts of every applied effect; empty when
    /// the commit skipped every effect.
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

impl CommitCondition {
    /// The stream this condition observes.
    #[must_use]
    pub fn path(&self) -> &StreamPath {
        match self {
            Self::Head { path, .. }
            | Self::Tail { path, .. }
            | Self::Absent { path }
            | Self::Present { path } => path,
        }
    }
}

impl CommitMutation {
    /// The stream this mutation publishes: an append's path or a fork's
    /// destination, the key of its canonical order.
    #[must_use]
    pub fn path(&self) -> &StreamPath {
        match self {
            Self::Append { path, .. } => path,
            Self::Fork { destination, .. } => destination,
        }
    }
}

/// One all-or-none group of mutations guarded by its own exact conditions.
///
/// Every condition of every effect is evaluated on the pre-commit state. A
/// hard effect whose conditions fail refuses the whole commit as a
/// [`CommitOutcome::Conflict`]. A soft effect whose conditions fail is
/// skipped, and its index is reported in [`CommitReceipt::skipped`]; the rest
/// of the commit is unaffected. Several effects may condition the same path,
/// but each mutation path appears once in the whole commit.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct CommitEffect {
    /// Exact conditions, at most one per path.
    pub conditions: Vec<CommitCondition>,
    /// Mutations, applied all or none. A hard effect without any is a guard:
    /// its conditions gate the commit and it publishes nothing. A soft effect
    /// needs at least one.
    pub mutations: Vec<CommitMutation>,
    /// Whether failed conditions skip this effect instead of refusing the commit.
    pub soft: bool,
}

impl CommitEffect {
    /// Whether this effect may join a commit: a soft effect needs mutations,
    /// since one without any could not show on replay whether it was skipped.
    /// A hard effect without mutations is a guard over the whole commit.
    #[must_use]
    pub fn admissible(&self) -> bool {
        !self.soft || !self.mutations.is_empty()
    }

    /// An effect whose failed conditions refuse the whole commit.
    #[must_use]
    pub fn hard(conditions: Vec<CommitCondition>, mutations: Vec<CommitMutation>) -> Self {
        Self {
            conditions,
            mutations,
            soft: false,
        }
    }
}

/// One bounded optimistic commit: an ordered list of effects published at one
/// decision, or nothing.
///
/// A commit whose hard conditions hold publishes every effect it did not skip
/// and spends its key, even when it skipped every effect: the receipt then
/// lists no appends or forks, and a retry replays it instead of redelivering.
/// A conflict publishes nothing and leaves the key unspent.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct CommitRequest {
    /// Trusted-clock publication deadline; successful retries keep their original identity.
    pub deadline_unix_millis: Option<u64>,
    /// Effects in request order, carrying at least one mutation between them.
    pub effects: Vec<CommitEffect>,
    /// Required stable recovery identity.
    pub idempotency_key: IdempotencyKey,
}

impl CommitRequest {
    /// Every condition, effect by effect.
    pub fn conditions(&self) -> impl Iterator<Item = &CommitCondition> + Clone {
        self.effects.iter().flat_map(|effect| &effect.conditions)
    }

    /// Every mutation, effect by effect.
    pub fn mutations(&self) -> impl Iterator<Item = &CommitMutation> + Clone {
        self.effects.iter().flat_map(|effect| &effect.mutations)
    }

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
    /// Every effect not listed in the receipt's `skipped` committed.
    Committed(CommitReceipt),
    /// A hard effect's conditions failed; nothing changed.
    Conflict(Vec<CommitConflict>),
}

/// Terminal result retained under one account-scoped retry identity.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum IdempotencyOutcome {
    /// Successful append. Failed conditions never retain an outcome.
    Append(AppendOutcome),
    /// Successful immutable-prefix fork.
    Fork(ForkReceipt),
    /// Successful coordinated commit, including one that skipped every effect
    /// and so holds no mutation. Conflicts leave the key unspent.
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
pub type RecordStream = Opened<Record>;

/// An ordered filtered follow observation. Skipped records carry only a cursor
/// and an immutable history anchor, never their values or indexed headers.
#[derive(Debug)]
pub enum FollowUpdate {
    /// Fixed tail captured with the initial subscription, before any records.
    /// This does not advance the cursor; later appends never change this cut.
    Opening {
        /// Exclusive end of the history present at registration.
        tail: u64,
    },
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
pub type FollowStream = Opened<FollowUpdate>;

/// The choices made at the same immutable snapshot as an opened cursor.
#[derive(Clone, Debug)]
pub struct StreamSelection {
    /// Resolved first sequence, inclusive.
    pub from: u64,
    /// Checked or resolved history anchor immediately before `from`.
    pub preceding: Option<CommitId>,
    /// Exclusive finite end; absent for a live cursor.
    pub end: Option<u64>,
    /// Last record requested by a LATEST open, before filter projection.
    pub bootstrap: Option<Record>,
}

/// A cursor and the exact immutable observation that selected it.
///
/// Forwarders preserve this receipt together with the original cursor. The
/// revision is available before the first item, including empty and quiet opens.
pub struct Opened<T, E = StreamError> {
    /// Revision of the original selected snapshot, never a later applied sample.
    pub snapshot: Revision,
    /// Bounds, anchor and bootstrap from that same snapshot.
    pub selection: StreamSelection,
    cursor: BoxProviderStream<'static, Result<T, E>>,
}

impl<T, E> Opened<T, E> {
    /// Couples a selected snapshot with its original retained cursor.
    #[must_use]
    pub fn new(
        snapshot: Revision,
        selection: StreamSelection,
        cursor: BoxProviderStream<'static, Result<T, E>>,
    ) -> Self {
        Self {
            snapshot,
            selection,
            cursor,
        }
    }

    /// Projects cursor items without replacing their selection or ownership.
    #[must_use]
    pub fn map_cursor<U>(
        self,
        project: impl FnOnce(
            BoxProviderStream<'static, Result<T, E>>,
        ) -> BoxProviderStream<'static, Result<U, E>>,
    ) -> Opened<U, E> {
        Opened::new(self.snapshot, self.selection, project(self.cursor))
    }
}

impl<T, E> futures::Stream for Opened<T, E> {
    type Item = Result<T, E>;

    fn size_hint(&self) -> (usize, Option<usize>) {
        self.cursor.size_hint()
    }

    fn poll_next(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Option<Self::Item>> {
        self.cursor.as_mut().poll_next(cx)
    }
}

/// Projects records from an existing follow without subscribing again.
pub fn records_only(updates: FollowStream) -> RecordStream {
    use futures::StreamExt as _;
    updates.map_cursor(|updates| {
        Box::pin(updates.filter_map(|update| {
            futures::future::ready(match update {
                Ok(FollowUpdate::Record(record)) => Some(Ok(record)),
                Ok(FollowUpdate::Progress { .. } | FollowUpdate::Opening { .. }) => None,
                Err(error) => Some(Err(error)),
            })
        }))
    })
}
