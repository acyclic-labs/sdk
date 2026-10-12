#![doc = include_str!("../README.md")]

use std::{fmt, sync::Arc};

use async_trait::async_trait;
use thiserror::Error;

pub mod conformance;
#[cfg(feature = "grpc")]
pub mod grpc;
pub mod http_response;
pub mod persistence;
mod preparation;
pub mod request;
#[cfg(all(feature = "http", not(target_arch = "wasm32")))]
pub mod http;
// Request projection is shared by the native HTTP client and the browser adapter.
#[cfg(all(feature = "wasm", target_arch = "wasm32"))]
mod browser;
#[cfg(any(
    all(feature = "http", not(target_arch = "wasm32")),
    all(feature = "wasm", target_arch = "wasm32")
))]
mod http_codec;
mod http_validation;
#[cfg(any(feature = "local", all(feature = "wasm", target_arch = "wasm32")))]
mod journal;
#[cfg(feature = "local")]
mod local;
mod memory;
#[cfg(all(
    not(target_arch = "wasm32"),
    any(feature = "grpc", feature = "http", feature = "local")
))]
mod obs;
#[cfg(all(feature = "wasm", target_arch = "wasm32"))]
pub use browser::{BrowserStream, BrowserStreamLimits};
#[cfg(all(feature = "wasm", target_arch = "wasm32"))]
mod wasm;
/// Conversion helpers between the canonical Rust domain types and generated wire messages.
pub mod wire_codec;

mod owned_bytes;
pub use owned_bytes::OwnedBytes;
#[cfg(not(target_arch = "wasm32"))]
pub use owned_bytes::{PreparedOwnedBytes, PreparedOwnedBytesError, PreparedOwnedBytesFailure};

/// Largest bearer credential, in bytes, that an SDK client accepts.
pub const MAX_BEARER_TOKEN_BYTES: usize = 12 * 1024;

/// Maximum caller-supplied private CA bundle bytes accepted by the HTTP and gRPC clients.
pub const MAX_CA_CERTIFICATE_BYTES: usize = 64 * 1024;

/// Generated canonical Stream v1 protocol.
#[allow(
    missing_docs,
    clippy::pedantic,
    clippy::allow_attributes_without_reason,
    reason = "generated prost and tonic bindings"
)]
pub mod wire {
    include!(concat!(env!("OUT_DIR"), "/acyclic.stream.v1.rs"));
}
/// Canonical public descriptor set used by compatibility gates.
pub const FILE_DESCRIPTOR_SET: &[u8] = include_bytes!("../proto/stream/v1/stream_descriptor.bin");
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
/// Maximum caller retry-identity width.
pub const MAX_IDEMPOTENCY_KEY_BYTES: usize = wire::StreamLimit::MaxIdempotencyKeyBytes as usize;
/// Maximum canonical path text accepted by the single-command wire format.
pub const MAX_PATH_BYTES: usize = wire::StreamLimit::MaxPathBytes as usize;

/// Route-to-response families for the hosted HTTP projection.
pub const HTTP_RESPONSE_CONTRACT: &[(&str, &str)] = &[
    ("idempotency/inspect", "observation"),
    ("delivery/acknowledge", "empty"),
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
pub const TOKEN_OPERATIONS: &[&str] = &[
    "list", "read", "follow", "append", "fork", "create", "commit",
];

/// Permanent account-relative slash-separated ASCII path.
#[derive(Clone)]
pub struct StreamPath(StreamPathStorage);

#[derive(Clone)]
enum StreamPathStorage {
    Plain(Arc<str>),
    #[cfg(not(target_arch = "wasm32"))]
    Native { owner: Arc<NativePathText>, length: usize },
}

#[cfg(not(target_arch = "wasm32"))]
struct NativePathText {
    text: String,
    _allocation: acyclic_native_runtime::ReadAllocation,
}

impl StreamPath {
    /// Validates and owns one path.
    pub fn new(path: impl AsRef<str>) -> Result<Self, StreamError> {
        let path = path.as_ref();
        Self::validate(path)?;
        Ok(Self(StreamPathStorage::Plain(Arc::from(path))))
    }

    fn validate(path: &str) -> Result<(), StreamError> {
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
        Ok(())
    }

    /// Owns canonical path text with last-alias original-ledger custody.
    #[cfg(not(target_arch = "wasm32"))]
    pub fn new_native(
        path: impl AsRef<str>,
        meter: &Arc<dyn acyclic_native_runtime::NativeReadMeter>,
    ) -> std::io::Result<Result<Self, StreamError>> {
        let path = path.as_ref();
        if let Err(error) = Self::validate(path) { return Ok(Err(error)); }
        let mut allocation = acyclic_native_runtime::ReadAllocation::new(meter, path.len())?;
        let mut text = String::new();
        if let Err(error) = text.try_reserve_exact(path.len()) {
            return Err(meter.allocation_failed(error));
        }
        allocation.reconcile_capacity(text.capacity())?;
        meter.admit_codec(0, path.len() as u64, 0)?;
        text.push_str(path);
        meter.record_codec(0, path.len() as u64, 0)?;
        let metadata = std::mem::size_of::<NativePathText>();
        meter.admit_codec(0, metadata as u64, 0)?;
        let mut owner_allocation = acyclic_native_runtime::ReadAllocation::new(meter, metadata)?;
        allocation.absorb(&mut owner_allocation)?;
        let owner = Arc::new(NativePathText { text, _allocation: allocation });
        meter.record_codec(0, metadata as u64, 0)?;
        Ok(Ok(Self(StreamPathStorage::Native { owner, length: path.len() })))
    }

    /// Canonical path text.
    #[must_use]
    pub fn as_str(&self) -> &str {
        match &self.0 {
            StreamPathStorage::Plain(text) => text,
            #[cfg(not(target_arch = "wasm32"))]
            StreamPathStorage::Native { owner, length } => &owner.text[..*length],
        }
    }

    /// Immediate parent, if any.
    #[must_use]
    #[allow(
        clippy::string_slice,
        reason = "`new` rejects any path that is not `is_ascii()`, so every byte offset from \
                  `rfind` is a valid char boundary and this can never panic"
    )]
    pub fn parent(&self) -> Option<Self> {
        let index = self.as_str().rfind('/')?;
        Some(Self(match &self.0 {
            StreamPathStorage::Plain(text) => StreamPathStorage::Plain(Arc::from(&text[..index])),
            #[cfg(not(target_arch = "wasm32"))]
            StreamPathStorage::Native { owner, .. } =>
                StreamPathStorage::Native { owner: Arc::clone(owner), length: index },
        }))
    }

    /// Produces an implicit parent without an unmetered path allocation.
    #[cfg(not(target_arch = "wasm32"))]
    pub(crate) fn parent_native(
        &self,
        meter: &Arc<dyn acyclic_native_runtime::NativeReadMeter>,
    ) -> std::io::Result<Option<Self>> {
        meter.check()?;
        let Some(index) = self.as_str().rfind('/') else { return Ok(None); };
        match &self.0 {
            StreamPathStorage::Native { owner, .. } => Ok(Some(Self(
                StreamPathStorage::Native { owner: Arc::clone(owner), length: index },
            ))),
            StreamPathStorage::Plain(_) => match Self::new_native(&self.as_str()[..index], meter)? {
                Ok(parent) => Ok(Some(parent)),
                // A prefix of an already valid ASCII path is itself valid.
                Err(_) => unreachable!("canonical stream path parent was invalid"),
            },
        }
    }

    /// Retains an existing native owner, or funds a real copy of plain text before it can outlive the caller's operation-scoped input claim.
    #[cfg(not(target_arch = "wasm32"))]
    pub(crate) fn retain_native(
        &self,
        meter: &Arc<dyn acyclic_native_runtime::NativeReadMeter>,
    ) -> std::io::Result<Self> {
        meter.check()?;
        match &self.0 {
            StreamPathStorage::Native { .. } => Ok(self.clone()),
            StreamPathStorage::Plain(_) => match Self::new_native(self.as_str(), meter)? {
                Ok(path) => Ok(path),
                Err(_) => unreachable!("canonical stream path became invalid"),
            },
        }
    }
}

impl fmt::Debug for StreamPath {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.debug_tuple("StreamPath").field(&self.as_str()).finish()
    }
}

impl PartialEq for StreamPath {
    fn eq(&self, other: &Self) -> bool { self.as_str() == other.as_str() }
}
impl Eq for StreamPath {}
impl PartialOrd for StreamPath {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> { Some(self.cmp(other)) }
}
impl Ord for StreamPath {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering { self.as_str().cmp(other.as_str()) }
}
impl std::hash::Hash for StreamPath {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        std::hash::Hash::hash(self.as_str(), state);
    }
}
impl std::borrow::Borrow<str> for StreamPath {
    fn borrow(&self) -> &str { self.as_str() }
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
pub struct IdempotencyKey(OwnedBytes);

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

/// One atomic replay boundary.
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

/// Bounded hierarchy traversal.
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
    /// Fork one pre-commit prefix into a new destination, then append `records` to that destination, all at the commit's one linearization point.
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

/// Trusted clock used by providers to evaluate publication deadlines at the same linearization point as a coordinated commit.
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

/// Original-native provider output with its actual newly allocated storage.
#[cfg(not(target_arch = "wasm32"))]
pub struct NativeStreamOutput<T> {
    value: T,
    allocation: acyclic_native_runtime::ReadAllocation,
}

#[cfg(not(target_arch = "wasm32"))]
impl<T> NativeStreamOutput<T> {
    /// Transfers already admitted storage; never adopts or allocates it again.
    pub fn new(value: T, allocation: acyclic_native_runtime::ReadAllocation) -> Self {
        Self { value, allocation }
    }

    /// Borrows the output without detaching its physical storage claim.
    pub fn value(&self) -> &T { &self.value }

    /// Borrows the producer's output while retaining its storage claim.
    pub fn value_mut(&mut self) -> &mut T { &mut self.value }

    /// Transfers both owners.
    pub fn into_parts(self) -> (T, acyclic_native_runtime::ReadAllocation) {
        (self.value, self.allocation)
    }
}

/// Preserves original observer failure separately from the provider's error.
#[cfg(not(target_arch = "wasm32"))]
pub type NativeStreamResult<T, E = StreamError> =
    std::io::Result<Result<NativeStreamOutput<T>, E>>;

/// A finite cursor preserves late original failures after its actual prefix.
#[cfg(not(target_arch = "wasm32"))]
pub type NativeRecordStream =
    BoxProviderStream<'static, std::io::Result<Result<NativeStreamOutput<Record>, StreamError>>>;

/// Object-safe provider future whose real heap frame is admitted before boxing.
#[cfg(not(target_arch = "wasm32"))]
pub enum NativeProviderFuture<'a, T, E = StreamError> {
    /// No physical future frame was allocated.
    Ready(Option<NativeStreamResult<T, E>>),
    /// The boxed frame drops before its original-ledger allocation guard.
    Funded {
        /// Canonical producer; no alternate request or work root.
        future: std::pin::Pin<Box<dyn Future<Output = NativeStreamResult<T, E>> + Send + 'a>>,
        /// Actual frame storage on the original ledger.
        allocation: acyclic_native_runtime::ReadAllocation,
    },
}

#[cfg(not(target_arch = "wasm32"))]
impl<'a, T, E> NativeProviderFuture<'a, T, E> {
    /// Admits the exact concrete frame before its sole physical allocation.
    pub fn new<F>(
        meter: &Arc<dyn acyclic_native_runtime::NativeReadMeter>,
        future: F,
    ) -> Self
    where
        F: Future<Output = NativeStreamResult<T, E>> + Send + 'a,
    {
        match Self::try_new(meter, future) {
            Ok(future) => future,
            Err(error) => Self::Ready(Some(Err(error))),
        }
    }

    /// Builds the same funded frame, reporting admission failure before a prepared ingress can be issued to its caller.
    pub(crate) fn try_new<F>(
        meter: &Arc<dyn acyclic_native_runtime::NativeReadMeter>,
        future: F,
    ) -> std::io::Result<Self>
    where
        F: Future<Output = NativeStreamResult<T, E>> + Send + 'a,
    {
        let bytes = std::mem::size_of::<F>();
        let allocation = if bytes == 0 {
            acyclic_native_runtime::ReadAllocation::empty(meter)
        } else {
            acyclic_native_runtime::ReadAllocation::new(meter, bytes)?
        };
        Ok(Self::Funded { future: Box::pin(future), allocation })
    }

    /// Returns a result without allocating a provider future.
    pub fn ready(result: NativeStreamResult<T, E>) -> Self {
        Self::Ready(Some(result))
    }
}

#[cfg(not(target_arch = "wasm32"))]
impl<T, E> Unpin for NativeProviderFuture<'_, T, E> {}

#[cfg(not(target_arch = "wasm32"))]
impl<T, E> Future for NativeProviderFuture<'_, T, E> {
    type Output = NativeStreamResult<T, E>;

    fn poll(
        self: std::pin::Pin<&mut Self>,
        context: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Self::Output> {
        match self.get_mut() {
            Self::Ready(result) => std::task::Poll::Ready(
                result.take().expect("native provider future polled after completion"),
            ),
            Self::Funded { future, .. } => future.as_mut().poll(context),
        }
    }
}

/// A genuine prepared Commit-family ingress.
#[cfg(not(target_arch = "wasm32"))]
pub struct PreparedNativeCommit {
    future: Option<NativeProviderFuture<'static, CommitOutcome>>,
    recovery: Option<NativeProviderFuture<'static, Option<IdempotencyObservation>>>,
    record: Arc<NativeCommitRecordSlot>,
    maximum_record_bytes: usize,
}

#[cfg(not(target_arch = "wasm32"))]
impl PreparedNativeCommit {
    pub(crate) fn new(
        future: NativeProviderFuture<'static, CommitOutcome>,
        recovery: NativeProviderFuture<'static, Option<IdempotencyObservation>>,
        record: Arc<NativeCommitRecordSlot>,
        maximum_record_bytes: usize,
    ) -> Self {
        Self { future: Some(future), recovery: Some(recovery), record, maximum_record_bytes }
    }

    /// Transfers the already funded same-key inspection future exactly once.
    pub fn take_recovery_future(
        &mut self,
    ) -> Option<NativeProviderFuture<'static, Option<IdempotencyObservation>>> {
        self.recovery.take()
    }

    /// Supplies the unchanged canonical request's one record and transfers its already funded future.
    pub fn finish(mut self, record: OwnedBytes) -> NativeProviderFuture<'static, CommitOutcome> {
        let record = if record.is_empty() {
            Err(StreamError::InvalidArgument)
        } else if record.len() > self.maximum_record_bytes {
            Err(StreamError::LimitExceeded)
        } else {
            Ok(record)
        };
        self.record.supply(record);
        self.future.take().expect("prepared Commit future is transferred once")
    }
}

#[cfg(not(target_arch = "wasm32"))]
impl Drop for PreparedNativeCommit {
    fn drop(&mut self) {
        if self.future.is_some() { self.record.close(); }
    }
}

#[cfg(not(target_arch = "wasm32"))]
struct NativeCommitRecordState {
    record: Option<Result<OwnedBytes, StreamError>>,
    waiter: Option<std::task::Waker>,
}

/// One private ingress cell, not a journal, authority or request queue.
#[cfg(not(target_arch = "wasm32"))]
pub(crate) struct NativeCommitRecordSlot {
    state: std::sync::Mutex<NativeCommitRecordState>,
    ready: std::sync::Condvar,
    _allocation: acyclic_native_runtime::ReadAllocation,
}

#[cfg(not(target_arch = "wasm32"))]
impl NativeCommitRecordSlot {
    pub(crate) fn new(
        meter: &Arc<dyn acyclic_native_runtime::NativeReadMeter>,
    ) -> std::io::Result<Arc<Self>> {
        let bytes = std::mem::size_of::<Self>();
        let allocation = acyclic_native_runtime::ReadAllocation::new(meter, bytes)?;
        meter.admit_codec(0, bytes as u64, 0)?;
        let record = Arc::new(Self {
            state: std::sync::Mutex::new(NativeCommitRecordState {
                record: None, waiter: None,
            }),
            ready: std::sync::Condvar::new(),
            _allocation: allocation,
        });
        meter.record_codec(0, bytes as u64, 0)?;
        meter.check()?;
        Ok(record)
    }

    pub(crate) async fn receive(&self) -> Result<OwnedBytes, StreamError> {
        futures::future::poll_fn(|context| {
            let mut state = self.state.lock().unwrap_or_else(std::sync::PoisonError::into_inner);
            if let Some(record) = &mut state.record {
                return std::task::Poll::Ready(std::mem::replace(record, Err(StreamError::Unavailable)));
            }
            if !state.waiter.as_ref().is_some_and(|waiter| waiter.will_wake(context.waker())) {
                state.waiter = Some(context.waker().clone());
            }
            std::task::Poll::Pending
        }).await
    }

    /// The already submitted Local journal worker waits without holding a provider visibility or journal lock.
    #[cfg(feature = "local")]
    pub(crate) fn receive_blocking(&self) -> Result<OwnedBytes, StreamError> {
        let mut state = self.state.lock().unwrap_or_else(std::sync::PoisonError::into_inner);
        loop {
            if let Some(record) = &mut state.record {
                return std::mem::replace(record, Err(StreamError::Unavailable));
            }
            state = self.ready.wait(state).unwrap_or_else(std::sync::PoisonError::into_inner);
        }
    }


    fn supply(&self, record: Result<OwnedBytes, StreamError>) {
        let waiter = {
            let mut state = self.state.lock().unwrap_or_else(std::sync::PoisonError::into_inner);
            state.record = Some(record);
            state.waiter.take()
        };
        if let Some(waiter) = waiter { waiter.wake(); }
        self.ready.notify_one();
    }

    pub(crate) fn close(&self) {
        let waiter = {
            let mut state = self.state.lock().unwrap_or_else(std::sync::PoisonError::into_inner);
            state.record.get_or_insert(Err(StreamError::Unavailable));
            state.waiter.take()
        };
        if let Some(waiter) = waiter { waiter.wake(); }
        self.ready.notify_one();
    }
}

#[cfg(not(target_arch = "wasm32"))]
pub(crate) fn capture_commit(
    source: &CommitRequest,
    meter: &Arc<dyn acyclic_native_runtime::NativeReadMeter>,
) -> std::io::Result<crate::memory::MemoryOutput<CommitRequest>> {
    let mut funding = crate::wire_codec::WireFunding::new(Some(meter));
    let mut conditions = funding.vector(source.conditions.len())?;
    for condition in &source.conditions {
        funding.examine(1)?;
        funding.copy_admission(std::mem::size_of::<CommitCondition>())?;
        conditions.push(condition.clone());
        funding.copy_completed(std::mem::size_of::<CommitCondition>())?;
    }
    let mut mutations = funding.vector(source.mutations.len())?;
    for mutation in &source.mutations {
        funding.examine(1)?;
        let mutation = match mutation {
            CommitMutation::Append { path, records } => CommitMutation::Append {
                path: path.clone(),
                records: funding.records(records)?,
            },
            CommitMutation::Fork { source, destination, at_tail, records } => CommitMutation::Fork {
                source: source.clone(),
                destination: destination.clone(),
                at_tail: *at_tail,
                records: funding.records(records)?,
            },
        };
        funding.copy_admission(std::mem::size_of::<CommitMutation>())?;
        mutations.push(mutation);
        funding.copy_completed(std::mem::size_of::<CommitMutation>())?;
    }
    Ok(funding.finish(CommitRequest {
        conditions, mutations, idempotency_key: source.idempotency_key.clone(),
    }))
}

/// Heap-allocated provider future with the platform's executor bound.
#[cfg(not(target_arch = "wasm32"))]
pub type BoxProviderFuture<'a, T> = futures::future::BoxFuture<'a, T>;
/// Heap-allocated provider future retaining browser event-loop ownership.
#[cfg(target_arch = "wasm32")]
pub type BoxProviderFuture<'a, T> = futures::future::LocalBoxFuture<'a, T>;

/// Bound for provider futures submitted to the platform executor.
#[cfg(not(target_arch = "wasm32"))]
pub trait ProviderTask: Send {}
#[cfg(not(target_arch = "wasm32"))]
impl<T: Send + ?Sized> ProviderTask for T {}
/// Browser executor futures retain event-loop ownership.
#[cfg(target_arch = "wasm32")]
pub trait ProviderTask {}
#[cfg(target_arch = "wasm32")]
impl<T: ?Sized> ProviderTask for T {}
/// Heap-allocated provider cursor with the platform's executor bound.
#[cfg(not(target_arch = "wasm32"))]
pub type BoxProviderStream<'a, T> = futures::stream::BoxStream<'a, T>;
/// Heap-allocated provider cursor retaining browser event-loop ownership.
#[cfg(target_arch = "wasm32")]
pub type BoxProviderStream<'a, T> = futures::stream::LocalBoxStream<'a, T>;

/// Platform ownership boundary for native threads or browser event loops.
#[cfg(not(target_arch = "wasm32"))]
pub trait ProviderPlatform: Send + Sync {}
#[cfg(not(target_arch = "wasm32"))]
impl<T: Send + Sync + ?Sized> ProviderPlatform for T {}
/// Browser providers may own thread-affine platform handles.
#[cfg(target_arch = "wasm32")]
pub trait ProviderPlatform {}
#[cfg(target_arch = "wasm32")]
impl<T: ?Sized> ProviderPlatform for T {}

#[cfg_attr(target_arch = "wasm32", async_trait(?Send))]
#[cfg_attr(not(target_arch = "wasm32"), async_trait)]
/// Canonical provider contract. Placement and transport remain invisible.
pub trait StreamProvider: ProviderPlatform + 'static {
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
    /// Traverses arbitrarily large direct-child sets without silently duplicating or omitting entries across concurrent hierarchy changes.
    async fn children_page(
        &self,
        request: ChildrenPageRequest,
    ) -> Result<ChildrenPage, StreamError> {
        let _ = request;
        Err(StreamError::Unsupported)
    }
    /// Executes one all-or-nothing optimistic commit.
    async fn commit(&self, request: CommitRequest) -> Result<CommitOutcome, StreamError>;
    /// Executes one coordinated commit only if the provider's trusted clock is strictly before `deadline_unix_millis` at the linearization point.
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

    /// Canonical inspection funded by the original native work owner.
    #[cfg(not(target_arch = "wasm32"))]
    fn native_inspect_idempotency(
        &self,
        key: IdempotencyKey,
        _meter: Arc<dyn acyclic_native_runtime::NativeReadMeter>,
    ) -> NativeProviderFuture<'_, Option<IdempotencyObservation>> {
        let _ = key;
        NativeProviderFuture::ready(Ok(Err(StreamError::Unsupported)))
    }

    /// Executes the genuine append family on the original native work owner.
    #[cfg(not(target_arch = "wasm32"))]
    fn native_append(
        &self,
        request: AppendRequest,
        _meter: Arc<dyn acyclic_native_runtime::NativeReadMeter>,
    ) -> NativeProviderFuture<'_, AppendOutcome> {
        let _ = request;
        NativeProviderFuture::ready(Ok(Err(StreamError::Unsupported)))
    }

    /// Canonical tail observation on the original ledger.
    #[cfg(not(target_arch = "wasm32"))]
    fn native_tail(
        &self,
        path: StreamPath,
        _meter: Arc<dyn acyclic_native_runtime::NativeReadMeter>,
    ) -> NativeProviderFuture<'_, u64> {
        let _ = path;
        NativeProviderFuture::ready(Ok(Err(StreamError::Unsupported)))
    }

    /// Canonical retained-window observation on the original ledger.
    #[cfg(not(target_arch = "wasm32"))]
    fn native_bounds(
        &self,
        path: StreamPath,
        _meter: Arc<dyn acyclic_native_runtime::NativeReadMeter>,
    ) -> NativeProviderFuture<'_, StreamBounds> {
        let _ = path;
        NativeProviderFuture::ready(Ok(Err(StreamError::Unsupported)))
    }

    /// Canonical finite read; original causes and output custody are retained.
    #[cfg(not(target_arch = "wasm32"))]
    fn native_read(
        &self,
        request: ReadRequest,
        _meter: Arc<dyn acyclic_native_runtime::NativeReadMeter>,
    ) -> NativeProviderFuture<'_, NativeRecordStream> {
        let _ = request;
        NativeProviderFuture::ready(Ok(Err(StreamError::Unsupported)))
    }

    /// Canonical commit with allocation admission before its future frame.
    #[cfg(not(target_arch = "wasm32"))]
    fn native_commit(
        &self,
        request: CommitRequest,
        _meter: Arc<dyn acyclic_native_runtime::NativeReadMeter>,
    ) -> NativeProviderFuture<'_, CommitOutcome> {
        let _ = request;
        NativeProviderFuture::ready(Ok(Err(StreamError::Unsupported)))
    }

    /// Executes the canonical native Commit with an owned original guard before every protected publication attempt.
    #[cfg(not(target_arch = "wasm32"))]
    fn native_commit_guarded<E, F, Fut>(
        &self,
        request: CommitRequest,
        deadline_unix_millis: Option<u64>,
        _meter: Arc<dyn acyclic_native_runtime::NativeReadMeter>,
        before: F,
    ) -> NativeProviderFuture<'_, CommitOutcome, E>
    where
        Self: Sized,
        E: From<StreamError> + Send + 'static,
        F: FnMut() -> Fut + Clone + Send + 'static,
        Fut: Future<Output = Result<(), E>> + Send + 'static,
    {
        let _ = (request, deadline_unix_millis, before);
        NativeProviderFuture::ready(Ok(Err(E::from(StreamError::Unsupported))))
    }

    /// Funds the canonical two-tail/one-append/one-record Commit ingress before physical action.
    #[cfg(not(target_arch = "wasm32"))]
    fn native_prepare_commit(
        &self,
        request: CommitRequest,
        maximum_record_bytes: usize,
        _meter: Arc<dyn acyclic_native_runtime::NativeReadMeter>,
    ) -> NativeProviderFuture<'_, PreparedNativeCommit> {
        let _ = (request, maximum_record_bytes);
        NativeProviderFuture::ready(Ok(Err(StreamError::Unsupported)))
    }

    /// Executes the same canonical commit with the provider's trusted clock checked at its actual linearization point, not at caller admission.
    #[cfg(not(target_arch = "wasm32"))]
    fn native_commit_before(
        &self,
        request: CommitRequest,
        deadline_unix_millis: u64,
        _meter: Arc<dyn acyclic_native_runtime::NativeReadMeter>,
    ) -> NativeProviderFuture<'_, CommitOutcome> {
        let _ = (request, deadline_unix_millis);
        NativeProviderFuture::ready(Ok(Err(StreamError::Unsupported)))
    }

}

/// Handle to one permanent Stream path.
pub struct Stream<P> {
    provider: Arc<P>,
    path: StreamPath,
}

impl<P> Clone for Stream<P> {
    fn clone(&self) -> Self {
        Self {
            provider: Arc::clone(&self.provider),
            path: self.path.clone(),
        }
    }
}

impl<P: StreamProvider> Stream<P> {
    /// Opens one validated path on its provider.
    pub fn new(provider: Arc<P>, path: impl AsRef<str>) -> Result<Self, StreamError> {
        Ok(Self { provider, path: StreamPath::new(path)? })
    }

    /// Permanent path.
    #[must_use]
    pub const fn path(&self) -> &StreamPath {
        &self.path
    }

    /// Current tail.
    pub async fn tail(&self) -> Result<u64, StreamError> {
        self.provider.tail(self.path.clone()).await
    }

    /// Exact retained replay window observed atomically by the provider.
    pub async fn bounds(&self) -> Result<StreamBounds, StreamError> {
        self.provider.bounds(self.path.clone()).await
    }

    /// Unconditionally appends one record.
    pub async fn append(&self, value: impl Into<OwnedBytes>) -> Result<AppendOutcome, StreamError> {
        self.append_batch(vec![value.into()], None, None).await
    }

    /// Appends one record only at the exact tail.
    pub async fn append_at(
        &self,
        value: impl Into<OwnedBytes>,
        if_tail: u64,
    ) -> Result<AppendOutcome, StreamError> {
        self.append_batch(vec![value.into()], Some(if_tail), None)
            .await
    }

    /// Atomically appends a contiguous batch.
    pub async fn append_batch(
        &self,
        records: Vec<OwnedBytes>,
        if_tail: Option<u64>,
        idempotency_key: Option<IdempotencyKey>,
    ) -> Result<AppendOutcome, StreamError> {
        self.provider
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
        self.provider
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
        self.provider
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
        self.provider.follow(self.path.clone(), from).await
    }
}

/// Gapless, paged replay of one stream up to its tail.
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
    IdempotencyKey(OwnedBytes::copy_from_slice(uuid::Uuid::new_v4().as_bytes()))
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

impl StreamError {
    /// Every failure, in canonical code order.
    pub const ALL: [Self; 14] = [
        Self::InvalidPath,
        Self::InvalidArgument,
        Self::LimitExceeded,
        Self::NotFound,
        Self::AlreadyExists,
        Self::PrefixNotRetained,
        Self::OutOfRange,
        Self::IdempotencyMismatch,
        Self::Capacity,
        Self::AccessDenied,
        Self::Unavailable,
        Self::HierarchyChanged,
        Self::DeadlineElapsed,
        Self::Unsupported,
    ];

    /// Stable `snake_case` code shared by the HTTP, gRPC, and WASM transports.
    #[must_use]
    pub const fn code(&self) -> &'static str {
        match self {
            Self::InvalidPath => "invalid_path",
            Self::InvalidArgument => "invalid_argument",
            Self::LimitExceeded => "limit_exceeded",
            Self::NotFound => "not_found",
            Self::AlreadyExists => "already_exists",
            Self::PrefixNotRetained => "prefix_not_retained",
            Self::OutOfRange => "out_of_range",
            Self::IdempotencyMismatch => "idempotency_mismatch",
            Self::Capacity => "capacity",
            Self::AccessDenied => "access_denied",
            Self::Unavailable => "unavailable",
            Self::HierarchyChanged => "hierarchy_changed",
            Self::DeadlineElapsed => "deadline_elapsed",
            Self::Unsupported => "unsupported",
        }
    }

    /// Parses a code produced by [`Self::code`].
    #[must_use]
    pub fn from_code(code: &str) -> Option<Self> {
        Self::ALL.into_iter().find(|error| error.code() == code)
    }
}

#[cfg(test)]
mod error_code_tests {
    use super::StreamError;

    #[test]
    fn wire_codes_are_pinned_and_round_trip() {
        let codes = StreamError::ALL.map(|error| error.code());
        assert_eq!(
            codes,
            [
                "invalid_path",
                "invalid_argument",
                "limit_exceeded",
                "not_found",
                "already_exists",
                "prefix_not_retained",
                "out_of_range",
                "idempotency_mismatch",
                "capacity",
                "access_denied",
                "unavailable",
                "hierarchy_changed",
                "deadline_elapsed",
                "unsupported"
            ]
        );
        for error in StreamError::ALL {
            assert_eq!(StreamError::from_code(error.code()), Some(error));
        }
        assert_eq!(StreamError::from_code("stream_not_found"), None);
    }
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
mod replay_tests {
    use super::*;
    use futures::StreamExt as _;

    struct Gapped {
        inner: MemoryStream,
        missing: u64,
    }

    #[cfg_attr(target_arch = "wasm32", async_trait(?Send))]
    #[cfg_attr(not(target_arch = "wasm32"), async_trait)]
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
        let stream = crate::Stream::new(Arc::new(provider), "replay")
            .unwrap();
        let mut next = 0;
        while next < records {
            let batch = (next..records.min(next + 500))
                .map(|sequence| OwnedBytes::from(sequence.to_be_bytes().to_vec()))
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
        let stream = crate::Stream::new(Arc::new(MemoryStream::new(MemoryLimits::default())), "absent")
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
