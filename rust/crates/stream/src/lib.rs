//! Canonical public Stream contract and deterministic in-memory implementation.

use std::{fmt, sync::Arc};

use async_trait::async_trait;
use bytes::Bytes;
use futures::stream::BoxStream;
use thiserror::Error;

pub mod conformance;
pub mod grpc;
mod memory;

/// Generated canonical Stream v2 protocol.
#[allow(missing_docs)]
pub mod wire {
    tonic::include_proto!("acyclic.stream.v2");
}
/// Canonical public descriptor set used by compatibility gates.
pub const FILE_DESCRIPTOR_SET: &[u8] = include_bytes!("../proto/stream/v2/stream_descriptor.bin");
pub use memory::{MemoryLimits, MemoryStream};

/// Maximum opaque record body.
pub const MAX_RECORD_BYTES: usize = 64 * 1024;
/// Maximum records, participants, mutations, or path segments in one request.
pub const MAX_ITEMS: usize = 1_024;
/// Maximum canonical application command, including metadata.
pub const MAX_COMMAND_BYTES: usize = 1024 * 1024 + 8 * 1024;
/// Minimum durable replay window required from a provider.
pub const MIN_IDEMPOTENCY_RETENTION_SECS: u64 = 24 * 60 * 60;
/// Maximum caller retry-identity width.
pub const MAX_IDEMPOTENCY_KEY_BYTES: usize = 256;

/// Permanent account-relative slash-separated ASCII path.
#[derive(Clone, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct StreamPath(Arc<str>);

impl StreamPath {
    /// Validates and owns one path.
    pub fn new(path: impl AsRef<str>) -> Result<Self, StreamError> {
        let path = path.as_ref();
        if path.is_empty()
            || path.len() > MAX_COMMAND_BYTES
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
                || segment.bytes().any(|byte| byte.is_ascii_control())
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
#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
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
    /// Optional exact retained prefix.
    pub at_tail: Option<u64>,
    /// Optional stable recovery identity.
    pub idempotency_key: Option<IdempotencyKey>,
}

/// Successful monotonic logical trim.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct TrimReceipt {
    /// Trimmed path.
    pub path: StreamPath,
    /// New earliest readable sequence.
    pub trim_point: u64,
    /// Immutable envelope identity.
    pub commit_id: CommitId,
}

/// Successful permanent logical deletion.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct DeleteReceipt {
    /// Retired permanent path.
    pub path: StreamPath,
    /// Immutable envelope identity.
    pub commit_id: CommitId,
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
    /// Initial destination tail.
    pub tail: u64,
}

/// Logical trim fact retained in a committed envelope.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct CommittedTrim {
    /// Trimmed path.
    pub path: StreamPath,
    /// New earliest readable sequence.
    pub trim_point: u64,
}

/// Permanent deletion fact retained in a committed envelope.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct CommittedDelete {
    /// Retired path.
    pub path: StreamPath,
}

/// Mutation in a successful immutable envelope.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum CommittedMutation {
    /// Contiguous append.
    Append(CommittedAppend),
    /// Immutable-prefix fork.
    Fork(CommittedFork),
    /// Logical prefix trim.
    Trim(CommittedTrim),
    /// Permanent path retirement.
    Delete(CommittedDelete),
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
    /// Path must never have existed or been retired.
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
    /// Fork one pre-commit prefix.
    Fork {
        /// Source.
        source: StreamPath,
        /// Destination.
        destination: StreamPath,
        /// Exact source prefix end.
        at_tail: u64,
    },
    /// Advance one path's logical trim point.
    Trim {
        /// Existing path.
        path: StreamPath,
        /// New earliest readable sequence.
        before: u64,
    },
    /// Permanently retire one path.
    Delete {
        /// Existing path without live descendants.
        path: StreamPath,
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
    /// Requested absent path or ancestor is retired.
    Retired {
        /// Retired path or descendant of a retired path.
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

/// Backpressured finite read or long-lived follow.
pub type RecordStream = BoxStream<'static, Result<Record, StreamError>>;
/// Backpressured fixed-snapshot direct-child listing.
pub type ChildStream = BoxStream<'static, Result<Child, StreamError>>;

/// Canonical provider contract. Placement and transport remain invisible.
#[async_trait]
pub trait StreamProvider: Send + Sync + 'static {
    /// Current next sequence.
    async fn tail(&self, path: StreamPath) -> Result<u64, StreamError>;
    /// Atomic append or tail conflict.
    async fn append(&self, request: AppendRequest) -> Result<AppendOutcome, StreamError>;
    /// Atomic immutable-prefix fork.
    async fn fork(&self, request: ForkRequest) -> Result<ForkReceipt, StreamError>;
    /// Monotonically advances a path's logical trim point.
    async fn trim(
        &self,
        path: StreamPath,
        before: u64,
        idempotency_key: IdempotencyKey,
    ) -> Result<TrimReceipt, StreamError>;
    /// Permanently retires a path with no live descendants.
    async fn delete(
        &self,
        path: StreamPath,
        idempotency_key: IdempotencyKey,
    ) -> Result<DeleteReceipt, StreamError>;
    /// Opens a bounded finite read.
    async fn read(&self, request: ReadRequest) -> Result<RecordStream, StreamError>;
    /// Replays and then remains live without a handoff gap.
    async fn follow(&self, path: StreamPath, from: u64) -> Result<RecordStream, StreamError>;
    /// Lists one fixed-snapshot direct-child page.
    async fn children(&self, request: ChildrenRequest) -> Result<ChildStream, StreamError>;
    /// Executes one all-or-nothing optimistic commit.
    async fn commit(&self, request: CommitRequest) -> Result<CommitOutcome, StreamError>;
    /// Reads one complete immutable successful envelope.
    async fn read_commit(&self, commit_id: CommitId) -> Result<CommittedEnvelope, StreamError>;
}

/// Minimal provider-bound client.
#[derive(Clone)]
pub struct StreamClient<P> {
    provider: Arc<P>,
}

impl<P: StreamProvider> StreamClient<P> {
    /// Binds one already authenticated provider.
    #[must_use]
    pub fn new(provider: Arc<P>) -> Self {
        Self { provider }
    }

    /// Opens one path handle after local validation.
    pub fn stream(&self, path: impl AsRef<str>) -> Result<Stream<P>, StreamError> {
        Ok(Stream {
            client: Self::new(Arc::clone(&self.provider)),
            path: StreamPath::new(path)?,
        })
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

    /// Executes a coordinated commit.
    pub async fn commit(&self, request: CommitRequest) -> Result<CommitOutcome, StreamError> {
        self.provider.commit(request).await
    }

    /// Reads a committed envelope.
    pub async fn read_commit(&self, commit_id: CommitId) -> Result<CommittedEnvelope, StreamError> {
        self.provider.read_commit(commit_id).await
    }
}

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
}

/// Handle to one permanent Stream path.
#[derive(Clone)]
pub struct Stream<P> {
    client: StreamClient<P>,
    path: StreamPath,
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

    /// Monotonically advances the earliest readable sequence.
    pub async fn trim(
        &self,
        before: u64,
        idempotency_key: Option<IdempotencyKey>,
    ) -> Result<TrimReceipt, StreamError> {
        self.client
            .provider
            .trim(
                self.path.clone(),
                before,
                idempotency_key.unwrap_or_else(new_idempotency_key),
            )
            .await
    }

    /// Permanently retires this path.
    pub async fn delete(
        &self,
        idempotency_key: Option<IdempotencyKey>,
    ) -> Result<DeleteReceipt, StreamError> {
        self.client
            .provider
            .delete(
                self.path.clone(),
                idempotency_key.unwrap_or_else(new_idempotency_key),
            )
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

    /// Replays from `from`, then remains live.
    pub async fn follow(&self, from: u64) -> Result<RecordStream, StreamError> {
        self.client.provider.follow(self.path.clone(), from).await
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
    /// Path or ancestor is permanently retired.
    #[error("stream path retired")]
    Retired,
    /// Requested source prefix is not retained.
    #[error("stream prefix not retained")]
    PrefixNotRetained,
    /// Requested sequence is beyond the current tail.
    #[error("stream sequence out of range")]
    OutOfRange,
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
}
