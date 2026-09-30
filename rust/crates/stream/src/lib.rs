#![doc = include_str!("../README.md")]

use std::{fmt, sync::Arc};

use async_trait::async_trait;
use bytes::Bytes;
use futures::stream::BoxStream;
use thiserror::Error;

pub mod conformance;
#[cfg(feature = "grpc")]
pub mod grpc;
pub mod http_response;
pub mod preparation;
pub mod request;
// The WASM adapter consumes this module on browser builds; native builds keep
// it available for contract tests without pulling in JS bindings.
#[cfg(all(feature = "http", not(target_arch = "wasm32")))]
pub mod http;
#[allow(dead_code)]
mod http_codec;
#[allow(dead_code)]
mod http_validation;
#[cfg(feature = "local")]
mod local;
mod memory;
#[cfg(all(feature = "wasm", target_arch = "wasm32"))]
mod wasm;
#[allow(dead_code)]
mod wire_codec;

/// Generated canonical Stream v2 protocol.
#[allow(missing_docs, clippy::pedantic, clippy::too_many_lines)]
pub mod wire {
    include!(concat!(env!("OUT_DIR"), "/acyclic.stream.v2.rs"));
}
/// Canonical public descriptor set used by compatibility gates.
pub const FILE_DESCRIPTOR_SET: &[u8] = include_bytes!("../proto/stream/v2/stream_descriptor.bin");
#[cfg(feature = "local")]
pub use local::{
    LocalDurability, LocalStream, LocalStreamError, LocalStreamLimits, deferring_durability,
};
pub use memory::{MemoryLimits, MemoryStream};

/// Maximum opaque record body.
pub const MAX_RECORD_BYTES: usize = wire::StreamLimit::MaxRecordBytes as usize;
/// Maximum records, participants, mutations, or path segments in one request.
pub const MAX_ITEMS: usize = wire::StreamLimit::MaxItems as usize;
const REPLAY_PAGE: u32 = 1_024;
const _: () = assert!(REPLAY_PAGE as usize == MAX_ITEMS);
/// Maximum canonical application command, including metadata.
pub const MAX_COMMAND_BYTES: usize = wire::StreamLimit::MaxCommandBytes as usize;
/// Historical minimum replay window, preserved for source compatibility.
/// Expiry never authorizes re-executing an admitted identity. Memory and local
/// providers retain full outcomes indefinitely within their capacity limits.
pub const MIN_IDEMPOTENCY_RETENTION_SECS: u64 = 24 * 60 * 60;
/// Maximum caller retry-identity width.
pub const MAX_IDEMPOTENCY_KEY_BYTES: usize = wire::StreamLimit::MaxIdempotencyKeyBytes as usize;
/// Maximum canonical path text accepted by the single-command wire format.
pub const MAX_PATH_BYTES: usize = wire::StreamLimit::MaxPathBytes as usize;

/// Route-to-response families for the hosted HTTP projection.
///
/// The TypeScript adapter generates its route/result association from this table, so adding a
/// hosted route requires updating the canonical Rust validator and the generated client contract
/// together.
pub const HTTP_RESPONSE_CONTRACT: &[(&str, &str)] = &[
    ("idempotency/inspect", "observation"),
    ("tail", "sequence"),
    ("append", "append"),
    ("fork", "fork"),
    ("read", "records"),
    ("children", "children"),
    ("children/page", "children_page"),
    ("commit", "commit"),
    ("commits/read", "envelope"),
    ("tokens/create", "token"),
];

/// Canonical operation vocabulary accepted by Stream access-token grants.
///
/// The TypeScript client derives its public `TokenOperation` union from this
/// ordered inventory. Keep entries stable because the order is part of the
/// generated artifact and makes additions visible in code review.
pub const TOKEN_OPERATIONS: &[&str] = &[
    "list", "read", "follow", "append", "fork", "create", "commit",
];

/// Permanent account-relative slash-separated ASCII path.
#[derive(Clone, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct StreamPath(Arc<str>);

impl StreamPath {
    /// Validates and owns one path.
    pub fn new(path: impl AsRef<str>) -> Result<Self, StreamError> {
        let path = path.as_ref();
        if path.is_empty()
            || path.len() > MAX_PATH_BYTES
            || !path.is_ascii()
            || path.starts_with('/')
            || path.ends_with('/')
        {
            return Err(StreamError::InvalidPath);
        }
        let mut count = 0_usize;
        for segment in path.split('/') {
            count = count.checked_add(1).ok_or(StreamError::LimitExceeded)?;
            if segment.is_empty()
                || segment == "."
                || segment == ".."
                || segment
                    .bytes()
                    .any(|byte| byte.is_ascii_control() || byte.is_ascii_whitespace())
                || segment.as_bytes().contains(&b'\\')
            {
                return Err(StreamError::InvalidPath);
            }
        }
        if count > MAX_ITEMS {
            return Err(StreamError::LimitExceeded);
        }
        Ok(Self(Arc::from(path)))
    }

    /// Canonical path text.
    #[must_use]
    pub fn as_str(&self) -> &str {
        &self.0
    }

    /// Immediate parent, if any.
    #[must_use]
    #[allow(
        clippy::string_slice,
        reason = "`new` rejects any path that is not `is_ascii()`, so every byte offset from \
                  `rfind` is a valid char boundary and this can never panic"
    )]
    pub fn parent(&self) -> Option<Self> {
        self.0
            .rfind('/')
            .map(|index| Self(Arc::from(&self.0[..index])))
    }
}

impl fmt::Display for StreamPath {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(self.as_str())
    }
}

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
pub struct IdempotencyKey(Bytes);

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
    pub records: Vec<Bytes>,
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

/// One atomic replay boundary. `tail` is the next sequence and may advance
/// immediately after observation.
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

/// Bounded hierarchy traversal. A continuation is valid only while the
/// provider's hierarchy version remains unchanged; callers restart on drift.
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
        records: Vec<Bytes>,
    },
    /// Fork one pre-commit prefix into a new destination, then append
    /// `records` to that destination, all at the commit's one
    /// linearization point. A commit changes each path once, so this is
    /// the only way to create a path from a prefix and extend it together.
    Fork {
        /// Source.
        source: StreamPath,
        /// Destination.
        destination: StreamPath,
        /// Exact source prefix end.
        at_tail: u64,
        /// Opaque records appended after the prefix; may be empty.
        records: Vec<Bytes>,
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

/// Trusted clock used by providers to evaluate publication deadlines at the
/// same linearization point as a coordinated commit.
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
pub type RecordStream = BoxStream<'static, Result<Record, StreamError>>;
/// Backpressured fixed-snapshot direct-child listing.
pub type ChildStream = BoxStream<'static, Result<Child, StreamError>>;

/// Canonical provider contract. Placement and transport remain invisible.
#[async_trait]
pub trait StreamProvider: Send + Sync + 'static {
    /// Reads the retained terminal result for one caller-owned retry identity.
    async fn inspect_idempotency(
        &self,
        idempotency_key: IdempotencyKey,
    ) -> Result<Option<IdempotencyObservation>, StreamError>;
    /// Current next sequence.
    async fn tail(&self, path: StreamPath) -> Result<u64, StreamError>;
    /// Atomically observes both ends of the retained replay window.
    async fn bounds(&self, path: StreamPath) -> Result<StreamBounds, StreamError>;
    /// Atomic append or tail conflict.
    async fn append(&self, request: AppendRequest) -> Result<AppendOutcome, StreamError>;
    /// Atomic immutable-prefix fork.
    async fn fork(&self, request: ForkRequest) -> Result<ForkReceipt, StreamError>;
    /// Opens a bounded finite read.
    async fn read(&self, request: ReadRequest) -> Result<RecordStream, StreamError>;
    /// Replays and then remains live without a handoff gap.
    async fn follow(&self, path: StreamPath, from: u64) -> Result<RecordStream, StreamError>;
    /// Lists one fixed-snapshot direct-child page.
    async fn children(&self, request: ChildrenRequest) -> Result<ChildStream, StreamError>;
    /// Traverses arbitrarily large direct-child sets without silently
    /// duplicating or omitting entries across concurrent hierarchy changes.
    async fn children_page(
        &self,
        request: ChildrenPageRequest,
    ) -> Result<ChildrenPage, StreamError> {
        let _ = request;
        Err(StreamError::Unsupported)
    }
    /// Executes one all-or-nothing optimistic commit.
    async fn commit(&self, request: CommitRequest) -> Result<CommitOutcome, StreamError>;
    /// Executes one coordinated commit only if the provider's trusted clock is
    /// strictly before `deadline_unix_millis` at the linearization point.
    ///
    /// Exact idempotent replay is resolved before the deadline. Providers that
    /// cannot enforce this atomically fail closed with [`StreamError::Unsupported`].
    async fn commit_before(
        &self,
        request: CommitRequest,
        deadline_unix_millis: u64,
    ) -> Result<CommitOutcome, StreamError> {
        let _ = (request, deadline_unix_millis);
        Err(StreamError::Unsupported)
    }
    /// Reads one complete immutable successful envelope.
    async fn read_commit(&self, commit_id: CommitId) -> Result<CommittedEnvelope, StreamError>;
}

/// Minimal provider-bound client.
pub struct StreamClient<P> {
    provider: Arc<P>,
}

impl<P> Clone for StreamClient<P> {
    fn clone(&self) -> Self {
        Self {
            provider: Arc::clone(&self.provider),
        }
    }
}

impl<P: StreamProvider> StreamClient<P> {
    /// Binds one already authenticated provider.
    #[must_use]
    pub fn new(provider: Arc<P>) -> Self {
        Self { provider }
    }

    /// Observes the exact replay window without guessing from a failed read.
    pub async fn bounds(&self, path: &str) -> Result<StreamBounds, StreamError> {
        self.provider.bounds(StreamPath::new(path)?).await
    }

    /// Opens one path handle after local validation.
    pub fn stream(&self, path: impl AsRef<str>) -> Result<Stream<P>, StreamError> {
        Ok(Stream {
            client: Self::new(Arc::clone(&self.provider)),
            path: StreamPath::new(path)?,
        })
    }

    /// Reads the retained terminal result for one caller-owned retry identity.
    pub async fn inspect_idempotency(
        &self,
        idempotency_key: IdempotencyKey,
    ) -> Result<Option<IdempotencyObservation>, StreamError> {
        self.provider.inspect_idempotency(idempotency_key).await
    }

    /// Lists direct children from one fixed snapshot.
    pub async fn children(
        &self,
        parent: Option<&str>,
        limit: u32,
    ) -> Result<ChildStream, StreamError> {
        self.provider
            .children(ChildrenRequest {
                parent: parent.map(StreamPath::new).transpose()?,
                limit,
            })
            .await
    }

    /// Reads one page of direct children with an exact hierarchy version.
    pub async fn children_page(
        &self,
        parent: Option<&str>,
        after: Option<&str>,
        hierarchy_version: Option<CommitId>,
        limit: u32,
    ) -> Result<ChildrenPage, StreamError> {
        self.provider
            .children_page(ChildrenPageRequest {
                parent: parent.map(StreamPath::new).transpose()?,
                after: after.map(StreamPath::new).transpose()?,
                hierarchy_version,
                limit,
            })
            .await
    }

    /// Executes a coordinated commit.
    pub async fn commit(&self, request: CommitRequest) -> Result<CommitOutcome, StreamError> {
        self.provider.commit(request).await
    }

    /// Executes one coordinated commit under the provider's trusted deadline.
    pub async fn commit_before(
        &self,
        request: CommitRequest,
        deadline_unix_millis: u64,
    ) -> Result<CommitOutcome, StreamError> {
        self.provider
            .commit_before(request, deadline_unix_millis)
            .await
    }

    /// Reads a committed envelope.
    pub async fn read_commit(&self, commit_id: CommitId) -> Result<CommittedEnvelope, StreamError> {
        self.provider.read_commit(commit_id).await
    }
}

#[cfg(feature = "grpc")]
impl StreamClient<grpc::Client> {
    /// Connects the high-level API to an authenticated managed or customer-hosted endpoint.
    pub async fn connect(
        endpoint: impl AsRef<str>,
        bearer_token: impl AsRef<str>,
    ) -> Result<Self, grpc::ConnectError> {
        Ok(Self::new(Arc::new(
            grpc::Client::connect(endpoint, bearer_token).await?,
        )))
    }

    /// Connects the high-level API through ambient roots plus one caller-supplied private CA.
    pub async fn connect_with_ca_certificate(
        endpoint: impl AsRef<str>,
        bearer_token: impl AsRef<str>,
        certificate_pem: impl AsRef<[u8]>,
    ) -> Result<Self, grpc::ConnectError> {
        Ok(Self::new(Arc::new(
            grpc::Client::connect_with_ca_certificate(endpoint, bearer_token, certificate_pem)
                .await?,
        )))
    }
}

/// Handle to one permanent Stream path.
pub struct Stream<P> {
    client: StreamClient<P>,
    path: StreamPath,
}

impl<P> Clone for Stream<P> {
    fn clone(&self) -> Self {
        Self {
            client: self.client.clone(),
            path: self.path.clone(),
        }
    }
}

impl<P: StreamProvider> Stream<P> {
    /// Permanent path.
    #[must_use]
    pub const fn path(&self) -> &StreamPath {
        &self.path
    }

    /// Current tail.
    pub async fn tail(&self) -> Result<u64, StreamError> {
        self.client.provider.tail(self.path.clone()).await
    }

    /// Exact retained replay window observed atomically by the provider.
    pub async fn bounds(&self) -> Result<StreamBounds, StreamError> {
        self.client.provider.bounds(self.path.clone()).await
    }

    /// Unconditionally appends one record.
    pub async fn append(&self, value: impl Into<Bytes>) -> Result<AppendOutcome, StreamError> {
        self.append_batch(vec![value.into()], None, None).await
    }

    /// Appends one record only at the exact tail.
    pub async fn append_at(
        &self,
        value: impl Into<Bytes>,
        if_tail: u64,
    ) -> Result<AppendOutcome, StreamError> {
        self.append_batch(vec![value.into()], Some(if_tail), None)
            .await
    }

    /// Atomically appends a contiguous batch.
    pub async fn append_batch(
        &self,
        records: Vec<Bytes>,
        if_tail: Option<u64>,
        idempotency_key: Option<IdempotencyKey>,
    ) -> Result<AppendOutcome, StreamError> {
        self.client
            .provider
            .append(AppendRequest {
                path: self.path.clone(),
                records,
                if_tail,
                idempotency_key: Some(idempotency_key.unwrap_or_else(new_idempotency_key)),
            })
            .await
    }

    /// Forks the current or selected retained prefix.
    pub async fn fork(
        &self,
        destination: impl AsRef<str>,
        at_tail: Option<u64>,
        idempotency_key: Option<IdempotencyKey>,
    ) -> Result<ForkReceipt, StreamError> {
        self.client
            .provider
            .fork(ForkRequest {
                source: self.path.clone(),
                destination: StreamPath::new(destination)?,
                at_tail,
                idempotency_key: Some(idempotency_key.unwrap_or_else(new_idempotency_key)),
            })
            .await
    }

    /// Reads at most `limit` records from `from`.
    pub async fn read(&self, from: u64, limit: u32) -> Result<RecordStream, StreamError> {
        self.client
            .provider
            .read(ReadRequest {
                path: self.path.clone(),
                from,
                limit,
            })
            .await
    }

    /// Pages every record from `from` to the tail. See [`Replay`].
    #[must_use]
    pub fn replay(&self, from: u64) -> Replay<P> {
        Replay {
            stream: self.clone(),
            next: from,
            done: false,
        }
    }

    /// Replays from `from`, then remains live.
    pub async fn follow(&self, from: u64) -> Result<RecordStream, StreamError> {
        self.client.provider.follow(self.path.clone(), from).await
    }
}

/// Gapless, paged replay of one stream up to its tail.
///
/// A path that does not exist reads as empty from zero. Every record's
/// sequence is checked here, so callers never re-verify it; a provider that
/// returns one out of order fails as [`StreamError::Unavailable`], like any
/// other malformed reply.
pub struct Replay<P> {
    stream: Stream<P>,
    next: u64,
    done: bool,
}

impl<P: StreamProvider> Replay<P> {
    /// The next page in order, or `None` once the tail is reached.
    pub async fn next_page(&mut self) -> Result<Option<Vec<Record>>, StreamError> {
        use futures::TryStreamExt as _;
        if self.done {
            return Ok(None);
        }
        let page = match self.stream.read(self.next, REPLAY_PAGE).await {
            Ok(records) => records.try_collect::<Vec<_>>().await?,
            Err(StreamError::NotFound) if self.next == 0 => Vec::new(),
            Err(error) => return Err(error),
        };
        for record in &page {
            if record.sequence != self.next {
                return Err(StreamError::Unavailable);
            }
            self.next = self.next.checked_add(1).ok_or(StreamError::LimitExceeded)?;
        }
        self.done = page.is_empty();
        Ok((!self.done).then_some(page))
    }

    /// The sequence after the last record returned.
    #[must_use]
    pub fn cursor(&self) -> u64 {
        self.next
    }
}

fn new_idempotency_key() -> IdempotencyKey {
    IdempotencyKey(Bytes::copy_from_slice(uuid::Uuid::new_v4().as_bytes()))
}

/// Stable public failures. CAS and absence conflicts are values instead.
#[derive(Clone, Debug, Eq, Error, PartialEq)]
pub enum StreamError {
    /// Invalid path syntax.
    #[error("invalid stream path")]
    InvalidPath,
    /// Invalid operation shape.
    #[error("invalid stream argument")]
    InvalidArgument,
    /// Count or byte ceiling exceeded.
    #[error("stream limit exceeded")]
    LimitExceeded,
    /// Path does not exist.
    #[error("stream not found")]
    NotFound,
    /// Destination already exists.
    #[error("stream already exists")]
    AlreadyExists,
    /// Requested source prefix is not retained.
    #[error("stream prefix not retained")]
    PrefixNotRetained,
    /// Requested sequence is beyond the current tail.
    #[error("stream sequence out of range")]
    OutOfRange,
    /// A hierarchy changed between paginated reads; restart from the first page.
    #[error("stream hierarchy changed during pagination")]
    HierarchyChanged,
    /// Retry identity was reused with different arguments.
    #[error("idempotency mismatch")]
    IdempotencyMismatch,
    /// Provider's bounded retained state is exhausted.
    #[error("stream capacity exhausted")]
    Capacity,
    /// Access denied.
    #[error("stream access denied")]
    AccessDenied,
    /// Required authority is unavailable.
    #[error("stream unavailable")]
    Unavailable,
    /// A provider-evaluated commit deadline elapsed before linearization.
    #[error("stream commit deadline elapsed")]
    DeadlineElapsed,
    /// The provider cannot supply a required semantic capability.
    #[error("stream capability unsupported")]
    Unsupported,
}

#[cfg(test)]
mod token_operation_tests {
    use super::TOKEN_OPERATIONS;

    #[test]
    fn inventory_is_ordered_and_unique() {
        assert_eq!(
            TOKEN_OPERATIONS,
            &[
                "list", "read", "follow", "append", "fork", "create", "commit",
            ]
        );
        let mut sorted = TOKEN_OPERATIONS.to_vec();
        sorted.sort_unstable();
        sorted.dedup();
        assert_eq!(sorted.len(), TOKEN_OPERATIONS.len());
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used)]
mod replay_tests {
    use super::*;
    use futures::StreamExt as _;

    /// Delegates to memory but drops one sequence from every read, as a
    /// faulty provider would.
    struct Gapped {
        inner: MemoryStream,
        missing: u64,
    }

    #[async_trait]
    impl StreamProvider for Gapped {
        async fn inspect_idempotency(
            &self,
            key: IdempotencyKey,
        ) -> Result<Option<IdempotencyObservation>, StreamError> {
            self.inner.inspect_idempotency(key).await
        }
        async fn tail(&self, path: StreamPath) -> Result<u64, StreamError> {
            self.inner.tail(path).await
        }
        async fn bounds(&self, path: StreamPath) -> Result<StreamBounds, StreamError> {
            self.inner.bounds(path).await
        }
        async fn append(&self, request: AppendRequest) -> Result<AppendOutcome, StreamError> {
            self.inner.append(request).await
        }
        async fn fork(&self, request: ForkRequest) -> Result<ForkReceipt, StreamError> {
            self.inner.fork(request).await
        }
        async fn read(&self, request: ReadRequest) -> Result<RecordStream, StreamError> {
            let missing = self.missing;
            let records = self.inner.read(request).await?;
            Ok(records
                .filter(move |record| {
                    std::future::ready(!matches!(record, Ok(record) if record.sequence == missing))
                })
                .boxed())
        }
        async fn follow(&self, path: StreamPath, from: u64) -> Result<RecordStream, StreamError> {
            self.inner.follow(path, from).await
        }
        async fn children(&self, request: ChildrenRequest) -> Result<ChildStream, StreamError> {
            self.inner.children(request).await
        }
        async fn commit(&self, request: CommitRequest) -> Result<CommitOutcome, StreamError> {
            self.inner.commit(request).await
        }
        async fn read_commit(&self, commit_id: CommitId) -> Result<CommittedEnvelope, StreamError> {
            self.inner.read_commit(commit_id).await
        }
    }

    async fn filled<P: StreamProvider>(provider: P, records: u64) -> Stream<P> {
        let stream = StreamClient::new(Arc::new(provider))
            .stream("replay")
            .unwrap();
        let mut next = 0;
        while next < records {
            let batch = (next..records.min(next + 500))
                .map(|sequence| Bytes::from(sequence.to_be_bytes().to_vec()))
                .collect::<Vec<_>>();
            next += batch.len() as u64;
            stream.append_batch(batch, None, None).await.unwrap();
        }
        stream
    }

    async fn drain<P: StreamProvider>(replay: &mut Replay<P>) -> Result<Vec<u64>, StreamError> {
        let mut sequences = Vec::new();
        while let Some(page) = replay.next_page().await? {
            sequences.extend(page.into_iter().map(|record| record.sequence));
        }
        Ok(sequences)
    }

    #[tokio::test]
    async fn missing_stream_replays_as_empty() {
        let stream = StreamClient::new(Arc::new(MemoryStream::new(MemoryLimits::default())))
            .stream("absent")
            .unwrap();
        let mut replay = stream.replay(0);
        assert!(drain(&mut replay).await.unwrap().is_empty());
        assert_eq!(replay.cursor(), 0);
        assert!(replay.next_page().await.unwrap().is_none());
    }

    #[tokio::test]
    async fn replay_crosses_pages_in_order_from_any_cursor() {
        let stream = filled(MemoryStream::new(MemoryLimits::default()), 2_500).await;
        let mut replay = stream.replay(0);
        assert_eq!(
            drain(&mut replay).await.unwrap(),
            (0..2_500).collect::<Vec<_>>()
        );
        assert_eq!(replay.cursor(), 2_500);
        let mut replay = stream.replay(1_500);
        assert_eq!(
            drain(&mut replay).await.unwrap(),
            (1_500..2_500).collect::<Vec<_>>()
        );
    }

    #[tokio::test]
    async fn gaps_within_and_between_pages_fail_closed() {
        for missing in [7, u64::from(REPLAY_PAGE)] {
            let inner = MemoryStream::new(MemoryLimits::default());
            let stream = filled(Gapped { inner, missing }, 2_100).await;
            let mut replay = stream.replay(0);
            assert_eq!(drain(&mut replay).await, Err(StreamError::Unavailable));
        }
    }
}
