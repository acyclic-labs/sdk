//! Bounded crash-recoverable local Stream provider.

use std::fs::{File, OpenOptions};
use std::future::Future;
use std::io::{Read, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex, PoisonError};

use acyclic_native_runtime::{
    NativeByteReservation, NativeOperationKind, NativeOperationReservation, NativeReadMeter,
    OwnedNativeByteReservation, OwnedNativeOperationReservation, OwnershipAnchor, ReadAllocation,
};
use async_trait::async_trait;
#[cfg(test)]
use crate::OwnedBytes;
use fs2::FileExt as _;
use futures::{StreamExt as _, stream};
use sha2::{Digest as _, Sha256};
use thiserror::Error;
use tokio::sync::{OwnedRwLockWriteGuard, RwLock, mpsc, watch};
#[cfg(test)]
use tracing::Instrument as _;
use tracing::field::Empty;

use crate::journal::{Command, CommandRef, decode_command, journal_command, replay};
use crate::obs;
use crate::wire_codec::ObservedWire;
use crate::{
    AppendOutcome, AppendRequest, ChildStream, ChildrenPage, ChildrenPageRequest, ChildrenRequest,
    CommitInput, CommitOutcome, CommitRequest, CommittedEnvelope, ForkReceipt, ForkRequest, IdempotencyKey,
    IdempotencyObservation, MAX_COMMAND_BYTES, MemoryLimits, MemoryStream, ReadRequest,
    NativeProviderFuture, NativeRecordStream, NativeStreamFailure, NativeStreamOutput, NativeStreamResult, RecordStream,
    StreamBounds, StreamError, StreamPath, StreamProvider, SystemUnixMillisClock, UnixMillisClock,
};

const HEADER_MAGIC: &[u8; 24] = b"ACYCLIC-STREAM-LOCAL-V1\0";
/// The magic, the limits, then the journal's epoch.
const HEADER_BYTES: usize = HEADER_MAGIC.len() + LIMITS_BYTES + 8;
const LIMITS_BYTES: usize = 8 * 8;
const SNAPSHOT_MAGIC: &[u8; 24] = b"ACYCLIC-STREAM-SNAP-V01\0";
/// The magic, limits and journal epoch; state and checksum follow.
const SNAPSHOT_HEADER_BYTES: usize = SNAPSHOT_MAGIC.len() + LIMITS_BYTES + 8;
/// A frame's sampled Unix commit time, before its command.
const FRAME_TIME_BYTES: usize = 8;
const LOCK_FILE: &str = "stream.lock";
const JOURNAL_FILE: &str = "stream.journal";
const SNAPSHOT_FILE: &str = "stream.snapshot";
const SNAPSHOT_TEMPORARY: &str = "stream.snapshot.tmp";
const FRAME_CHECKSUM_BYTES: usize = 32;
const REPLAY_PIPELINE_COMMANDS: usize = 32;

/// Native errors retain their original I/O value; ordinary callers retain the existing semantic projection.
enum LocalCompletionError {
    Stream(StreamError),
    GuardedStream(NativeStreamFailure),
    Native(std::io::Error),
}

impl From<StreamError> for LocalCompletionError {
    fn from(error: StreamError) -> Self { Self::Stream(error) }
}

impl From<std::io::Error> for LocalCompletionError {
    fn from(error: std::io::Error) -> Self { Self::Native(error) }
}

impl LocalCompletionError {
    fn ordinary(self) -> StreamError {
        match self {
            Self::Stream(error) => error,
            Self::GuardedStream(error) => error.into_parts().0,
            Self::Native(_) => StreamError::Unavailable,
        }
    }
}

type LocalCompletion<T> = Result<T, LocalCompletionError>;

fn from_native<T>(result: NativeStreamResult<T>) -> LocalCompletion<NativeStreamOutput<T>> {
    result.map_err(LocalCompletionError::Native)?
        .map_err(LocalCompletionError::GuardedStream)
}

fn into_native<T>(
    result: LocalCompletion<NativeStreamOutput<T>>,
    meter: &Arc<dyn NativeReadMeter>,
) -> NativeStreamResult<T> {
    match result {
        Ok(value) => Ok(Ok(value)),
        Err(LocalCompletionError::Stream(error)) => Ok(Err(NativeStreamFailure::new(
            error, ReadAllocation::empty(meter),
        ))),
        Err(LocalCompletionError::GuardedStream(error)) => Ok(Err(error)),
        Err(LocalCompletionError::Native(error)) => Err(error),
    }
}

fn publication_error(error: LocalStreamError) -> LocalCompletionError {
    match error {
        LocalStreamError::Io(error) => LocalCompletionError::Native(error),
        LocalStreamError::InvalidLimits => LocalCompletionError::Stream(StreamError::Capacity),
        LocalStreamError::Replay(error) => LocalCompletionError::Stream(error),
        _ => LocalCompletionError::Stream(StreamError::Unavailable),
    }
}

fn finish_native<T>(
    span: &obs::OwnedSpan,
    result: NativeStreamResult<T>,
) -> NativeStreamResult<T> {
    match &result {
        Ok(Ok(_)) => { let _ = obs::finish(span, Ok::<(), StreamError>(())); }
        Ok(Err(error)) => obs::failed(span, error.source().code()),
        Err(_) => obs::failed(span, "native_work"),
    }
    result
}

pin_project_lite::pin_project! {
    /// Funds the SDK-owned submitted frame before executor boxing.
    struct FundedLocalTask<F> {
        #[pin]
        operation: F,
        _allocation: Option<ReadAllocation>,
    }
}

impl<F> FundedLocalTask<F> {
    fn new(operation: F, meter: Option<&Arc<dyn NativeReadMeter>>) -> std::io::Result<Self> {
        let allocation = meter.map(|meter| ReadAllocation::new(
            meter, std::mem::size_of::<Self>(),
        )).transpose()?;
        Ok(Self { operation, _allocation: allocation })
    }

    fn run<T>(self, execute: impl FnOnce(F) -> T) -> T { execute(self.operation) }
}

impl<F: Future> Future for FundedLocalTask<F> {
    type Output = F::Output;

    fn poll(
        self: std::pin::Pin<&mut Self>,
        context: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Self::Output> {
        self.project().operation.poll(context)
    }
}

#[cfg(test)]
static JOURNAL_OPEN_SUBMITTED: Mutex<Option<(PathBuf, tokio::sync::oneshot::Sender<()>)>> =
    Mutex::new(None);
#[cfg(test)]
type JournalPersistBlocker = (
    usize,
    std::sync::mpsc::SyncSender<()>,
    std::sync::mpsc::Receiver<()>,
);
#[cfg(test)]
static JOURNAL_PERSIST_BLOCKER: Mutex<Option<JournalPersistBlocker>> = Mutex::new(None);

async fn run_owned_initialization<T, F>(
    span: &obs::OwnedSpan,
    initialize: F,
) -> Result<T, LocalStreamError>
where
    T: Send + 'static,
    F: Future<Output = Result<T, LocalStreamError>> + Send + 'static,
{
    let operation = span.clone();
    tokio::spawn(async move { obs::finish(&operation, operation.scope(initialize).await) })
        .await
        .map_err(|_| {
            obs::failed(span, obs::ErrorKind::kind(&LocalStreamError::Executor));
            LocalStreamError::Executor
        })?
}

/// How the journal is made durable before a mutation becomes observable.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub enum LocalDurability {
    /// Every publication waits for the storage device to flush its cache (`F_FULLFSYNC` on Apple platforms, `fsync`/`fdatasync` elsewhere), so acknowledged frames survive power loss.
    #[default]
    FullFlush,
    /// Every publication is ordered behind earlier writes without waiting for the device cache.
    Barrier,
}

tokio::task_local! {
    /// Whether mutations made on this task leave their frames unflushed; see [`deferring_durability`].
    static DEFERRED: bool;
}

/// Runs `future` with every local-stream mutation it makes durable against a crash of this process only: each frame is written, and becomes visible, without waiting for the storage device, until [`LocalStream::flush`] or any mutation made outside such a scope flushes the journal, and with it every frame before.
pub async fn deferring_durability<F: Future>(future: F) -> F::Output {
    DEFERRED.scope(true, future).await
}

fn deferred() -> bool {
    DEFERRED.try_with(|deferred| *deferred).unwrap_or(false)
}

/// Explicit local retention and recovery bounds.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct LocalStreamLimits {
    /// Canonical in-memory semantic-state limits.
    pub memory: MemoryLimits,
    /// Maximum durable commands replayed at startup.
    pub journal_operations: u64,
    /// Maximum journal bytes, including framing.
    pub journal_bytes: u64,
    /// Journal synchronization policy. Not recorded in the durable header.
    pub durability: LocalDurability,
}

impl Default for LocalStreamLimits {
    fn default() -> Self {
        Self {
            memory: MemoryLimits::default(),
            journal_operations: 1_000_000,
            journal_bytes: 1024 * 1024 * 1024,
            durability: LocalDurability::FullFlush,
        }
    }
}

/// Local provider initialization or durable-publication failure.
#[derive(Debug, Error)]
pub enum LocalStreamError {
    /// Filesystem operation failed.
    #[error("local Stream I/O failed: {0}")]
    Io(#[from] std::io::Error),
    /// Another process owns the local provider root.
    #[error("local Stream root is already open")]
    AlreadyOpen,
    /// Stored bytes or configuration do not match the canonical local format.
    #[error("local Stream journal is corrupt or incompatible")]
    Corrupt,
    /// Configured bounds are zero or cannot represent the canonical format.
    #[error("local Stream limits are invalid")]
    InvalidLimits,
    /// Durable history cannot be reconstructed through the canonical state machine.
    #[error("local Stream replay failed: {0}")]
    Replay(StreamError),
    /// Native blocking execution could not complete.
    #[error("local Stream executor is unavailable")]
    Executor,
    /// A prepared first write completed only a prefix; no further syscall was accepted before external action.
    #[error("local Stream prepared frame is incomplete")]
    IncompleteFrame,
}

impl obs::ErrorKind for LocalStreamError {
    fn kind(&self) -> &'static str {
        match self {
            Self::Io(_) => "io",
            Self::AlreadyOpen => "already_open",
            Self::Corrupt => "corrupt",
            Self::InvalidLimits => "invalid_limits",
            Self::Replay(error) => error.code(),
            Self::Executor => "executor",
            Self::IncompleteFrame => "incomplete_frame",
        }
    }
}

/// Exclusive-process durable local provider backed by a snapshot and a checksummed command journal.
#[derive(Clone)]
pub struct LocalStream {
    inner: Arc<LocalInner>,
}

struct LocalInner {
    provider: MemoryStream,
    commit_clock: Arc<PinnedCommitClock>,
    journal: OwnedJournal,
    /// Held exclusively while a mutation applies and writes its frame, and shared while a read observes state; never across a device flush.
    visibility: Arc<RwLock<()>>,
    /// The journal's `synced` sequence, published under the journal lock so that callers can wait for their frame, and those before it, to flush.
    synced: watch::Sender<u64>,
    changed: watch::Sender<u64>,
    poisoned: AtomicBool,
    /// Prepared completion cannot allocate a new snapshot after external action.
    prepared_compaction: AtomicBool,
}

impl LocalInner {
    fn poison(&self) -> StreamError {
        self.poisoned.store(true, Ordering::Release);
        // Wake every caller waiting for a flush, so it sees the poison.
        self.synced.send_modify(|_| {});
        StreamError::Unavailable
    }

    /// Records that every frame through `target` is durable.
    fn publish_synced(&self, journal: &mut Journal, target: u64) {
        journal.synced = journal.synced.max(target);
        let synced = journal.synced;
        self.synced.send_modify(|published| *published = synced);
    }

    /// Flushes the journal through `target`, the latest frame written when this flush began, on a blocking thread.
    fn flush_through(
        &self,
        target: u64,
        file: &File,
        durability: LocalDurability,
        mut admission: Option<OwnedNativeOperationReservation>,
    ) -> LocalCompletion<()> {
        #[cfg(test)]
        {
            let journal_identity = Arc::as_ptr(&self.journal.journal) as usize;
            if let Ok(mut hook) = JOURNAL_PERSIST_BLOCKER.lock()
                && hook
                    .as_ref()
                    .is_some_and(|(expected_journal, _, _)| *expected_journal == journal_identity)
                && let Some((_, started, release)) = hook.take()
            {
                let _ = started.send(());
                let _ = release.recv();
            }
        }
        let (flushed, observed) = match &mut admission {
            Some(admission) => admission.attempt(|| sync_file_data(file, durability)),
            None => (sync_file_data(file, durability), Ok(())),
        };
        let Ok(mut journal) = self.journal.journal.lock() else {
            self.poison();
            return Err(StreamError::Unavailable.into());
        };
        journal.syncing = false;
        if flushed.is_ok() {
            self.publish_synced(&mut journal, target);
        } else {
            drop(journal);
            self.poison();
        }
        flushed?;
        observed?;
        Ok(())
    }
}

/// The one canonical flush worker's owned inputs and original native credit.
struct JournalFlushPlan {
    inner: Arc<LocalInner>,
    file: Arc<File>,
    durability: LocalDurability,
    admission: Option<OwnedNativeOperationReservation>,
    _allocation: Option<ReadAllocation>,
}

struct JournalFlushWork {
    plan: JournalFlushPlan,
    target: u64,
}

impl JournalFlushWork {
    fn run(self) -> LocalCompletion<()> {
        self.plan.inner.flush_through(
            self.target, &self.plan.file, self.plan.durability, self.plan.admission,
        )
    }
}

struct MutationCompletion<T> {
    result: LocalCompletion<()>,
    flush: Option<JournalFlushPlan>,
    // Keep SDK-owned request controls alive through accepted durability drain.
    _input: Option<MutationInput>,
    visibility: Option<CommitVisibility>,
    value: Option<T>,
    primary_failure: bool,
}

struct CommitCompletion<T> {
    value: T,
    error: Option<std::io::Error>,
    input: Option<MutationInput>,
    revision: Option<u64>,
    visibility: Option<CommitVisibility>,
    // A rejected original guard did not publish and must not write its frame.
    persist: bool,
    primary_failure: bool,
}

struct GuardedCommitOutput<E, F> {
    result: NativeStreamResult<CommitOutcome, E>,
    _before: Option<F>,
}

struct CommitVisibility {
    _visibility: OwnedRwLockWriteGuard<()>,
    clock: Arc<PinnedCommitClock>,
}

impl Drop for CommitVisibility {
    fn drop(&mut self) {
        self.clock.unpin();
    }
}

#[derive(Clone, Copy)]
enum MutationVisibility {
    BeforeMutation,
    FromCommit,
}

enum MutationInput {
    Append(AppendInput),
    Commit(CommitInput),
}

struct AppendInput {
    request: AppendRequest,
    _allocation: Option<ReadAllocation>,
}

impl AppendInput {
    fn capture(source: &AppendRequest, meter: &Arc<dyn NativeReadMeter>) -> std::io::Result<Self> {
        let mut funding = crate::wire_codec::WireFunding::new(Some(meter));
        let records = funding.records(&source.records)?;
        let captured = funding.finish(AppendRequest {
            path: source.path.clone(),
            records,
            if_tail: source.if_tail,
            idempotency_key: source.idempotency_key.clone(),
        });
        Ok(Self { request: captured.value, _allocation: captured.allocation })
    }
}

#[derive(Clone)]
struct OwnedJournal {
    journal: Arc<Mutex<Journal>>,
    _ownership_anchor: Option<OwnershipAnchor>,
}

/// The actual journal job packet exists before submission.
struct JournalPersistWork {
    frame: PreparedFrame,
    required: bool,
    admission: Option<AdmittedFrameWrite>,
    journal: OwnedJournal,
}

impl JournalPersistWork {
    fn run(mut self) -> (Result<(), LocalStreamError>, std::io::Result<()>) {
        match self.journal.journal.lock() {
            Ok(mut journal) => journal.append_observed(
                &self.frame, self.required, self.admission.as_mut(),
            ),
            Err(_) => (Err(LocalStreamError::Corrupt), Ok(())),
        }
    }
}

/// The SAME persist and flush jobs are accepted on one blocking submission before issuing the late-input packet.
struct PreparedLocalWork {
    persist: FundedLocalTask<JournalPersistWork>,
    input: CommitInput,
    prepared: crate::memory::NativeMemoryPreparedCommit,
    record: Arc<crate::NativeCommitRecordSlot>,
    runtime: tokio::runtime::Handle,
    stream: LocalStream,
    meter: Arc<dyn NativeReadMeter>,
    flush: JournalFlushPlan,
}

impl PreparedLocalWork {
    fn run(mut self) -> NativeStreamResult<CommitOutcome> {
        let body = match self.record.receive_blocking() {
            Ok(body) => body,
            Err(error) => return into_native(Err(error.into()), &self.meter),
        };
        let result: LocalCompletion<NativeStreamOutput<CommitOutcome>> = (|| {
            // Input was supplied before taking any provider lock.
            let [crate::CommitMutation::Append { records, .. }] = self.input.request.mutations.as_mut_slice()
                else { return Err(StreamError::InvalidArgument.into()); };
            let Some(record) = records.first_mut() else { return Err(StreamError::InvalidArgument.into()); };
            *record = body;
            let mut work = FrameWork { meter: Some(&self.meter), accepted: true, late: Ok(()) };
            let visibility = self.stream.inner.visibility.blocking_write();
            self.stream.check_available()?;
            let committed_at = self.stream.inner.commit_clock.source.now_unix_millis();
            let wire = crate::journal::journal_command(CommandRef::Commit(&self.input.request));
            let encoded_length = wire.encoded_len()?;
            keep_observation(&mut work.late, self.meter.record_examination(1, 0));
            let command_length = FRAME_TIME_BYTES.checked_add(encoded_length)
                .and_then(|length| u32::try_from(length).ok())
                .ok_or(LocalCompletionError::Stream(StreamError::Capacity))?;
            let frame_length = 4 + command_length as usize + FRAME_CHECKSUM_BYTES;
            self.persist.operation.frame.write_wire(
                &wire, committed_at, encoded_length, command_length.to_le_bytes(), frame_length, &mut work,
            ).map_err(publication_error)?;
            self.stream.inner.journal.journal.lock()
                .map_err(|_| LocalCompletionError::Stream(self.stream.inner.poison()))?
                .admit(&self.persist.operation.frame).map_err(publication_error)?;
            self.stream.inner.commit_clock.pin(committed_at);
            let completion = self.runtime.block_on(
                self.stream.inner.provider.native_finish_prepared_commit_borrowed_inner(
                    self.prepared, &mut self.input.request,
                ),
            );
            self.stream.inner.commit_clock.unpin();
            let completion = completion.map_err(LocalCompletionError::Native)?
                .map_err(LocalCompletionError::Stream)?;
            let (physical, observed) = self.persist.run(JournalPersistWork::run);
            if physical.is_err() { self.stream.inner.poison(); }
            else { self.stream.inner.changed.send_modify(|revision| *revision = revision.saturating_add(1)); }
            if physical.is_ok() && self.stream.inner.journal.compaction_due()
                .map_err(|_| LocalCompletionError::Stream(self.stream.inner.poison()))? {
                self.stream.inner.prepared_compaction.store(true, Ordering::Release);
            }
            keep_observation(&mut work.late, observed);
            drop(visibility);
            work.late?;
            if let Some(error) = completion.error { return Err(LocalCompletionError::Native(error)); }
            physical.map_err(publication_error)?;
            Ok(completion.outcome)
        })();
        let required = self.stream.inner.journal.required().unwrap_or(0);
        let flushed = JournalFlushWork { plan: self.flush, target: required }.run();
        into_native(match result {
            Err(error) => Err(error),
            Ok(value) => flushed.map(|()| value),
        }, &self.meter)
    }
}

impl OwnedJournal {

    /// The latest frame a caller that observed the current state must wait to flush.
    fn required(&self) -> Result<u64, StreamError> {
        Ok(self
            .journal
            .lock()
            .map_err(|_| StreamError::Unavailable)?
            .required)
    }

    fn compaction_due(&self) -> Result<bool, LocalStreamError> {
        Ok(self
            .journal
            .lock()
            .map_err(|_| LocalStreamError::Corrupt)?
            .compaction_due())
    }
}

/// Records the wall-clock sample in a frame and supplies that same sample on replay.
struct PinnedCommitClock {
    source: Arc<dyn UnixMillisClock>,
    pinned: Mutex<Option<u64>>,
}

impl PinnedCommitClock {
    fn pin(&self, at: u64) {
        *self.pinned.lock().unwrap_or_else(PoisonError::into_inner) = Some(at);
    }

    fn unpin(&self) {
        *self.pinned.lock().unwrap_or_else(PoisonError::into_inner) = None;
    }
}

impl UnixMillisClock for PinnedCommitClock {
    fn now_unix_millis(&self) -> u64 {
        self.pinned
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .unwrap_or_else(|| self.source.now_unix_millis())
    }
}

/// What recovery hands the state machine, in order.
enum Recovered {
    /// Authorizes disk repair only after every recovered semantic operation passed.
    Validate(tokio::sync::oneshot::Sender<bool>),
    /// The snapshot's state, first.
    State { encoded: Vec<u8> },
    /// Each command in the journal after it.
    Command {
        committed_at_millis: u64,
        command: Command,
    },
}

impl LocalStream {
    /// Opens or creates one exclusive durable provider below `root`.
    pub async fn open(
        root: impl AsRef<Path>,
        limits: LocalStreamLimits,
    ) -> Result<Self, LocalStreamError> {
        Self::open_with_clock_and_anchor(root, limits, Arc::new(SystemUnixMillisClock), None).await
    }

    /// Opens a provider while retaining an external ownership gate through every provider clone.
    #[doc(hidden)]
    pub async fn open_with_ownership_anchor(
        root: impl AsRef<Path>,
        limits: LocalStreamLimits,
        ownership_anchor: OwnershipAnchor,
    ) -> Result<Self, LocalStreamError> {
        Self::open_with_clock_and_anchor(
            root,
            limits,
            Arc::new(SystemUnixMillisClock),
            Some(ownership_anchor),
        )
        .await
    }

    /// Opens a provider with an injected trusted clock.
    pub async fn open_with_clock(
        root: impl AsRef<Path>,
        limits: LocalStreamLimits,
        clock: Arc<dyn UnixMillisClock>,
    ) -> Result<Self, LocalStreamError> {
        Self::open_with_clock_and_anchor(root, limits, clock, None).await
    }

    async fn open_with_clock_and_anchor(
        root: impl AsRef<Path>,
        limits: LocalStreamLimits,
        clock: Arc<dyn UnixMillisClock>,
        ownership_anchor: Option<OwnershipAnchor>,
    ) -> Result<Self, LocalStreamError> {
        let span = obs::OwnedSpan::new(tracing::info_span!(
            "acyclic.stream.open",
            outcome = Empty,
            error.kind = Empty
        ));
        span.scope(async {
            let root = root.as_ref().to_path_buf();
            // The worker retains the root gate until recovery ends.
            run_owned_initialization(&span, async move {
                Self::open_owned(root, limits, clock, ownership_anchor).await
            })
            .await
        })
        .await
    }

    async fn open_owned(
        root: PathBuf,
        limits: LocalStreamLimits,
        clock: Arc<dyn UnixMillisClock>,
        ownership_anchor: Option<OwnershipAnchor>,
    ) -> Result<Self, LocalStreamError> {
        validate_limits(limits)?;
        let (recovered, mut receiver) = mpsc::channel(REPLAY_PIPELINE_COMMANDS);
        #[cfg(test)]
        let submitted_root = root.clone();
        let span = obs::OwnedSpan::new(tracing::debug_span!(
            "acyclic.stream.recover",
            frames = Empty,
            bytes = Empty
        ));
        let open = tokio::task::spawn_blocking(move || {
            span.in_scope(|| Journal::open(&root, limits, &recovered, &span))
        });
        #[cfg(test)]
        {
            if let Ok(mut hook) = JOURNAL_OPEN_SUBMITTED.lock()
                && hook
                    .as_ref()
                    .is_some_and(|(expected_root, _)| expected_root == &submitted_root)
                && let Some((_, submitted)) = hook.take()
            {
                let _ = submitted.send(());
            }
        }
        let commit_clock = Arc::new(PinnedCommitClock {
            source: Arc::clone(&clock),
            pinned: Mutex::new(None),
        });
        let provider = MemoryStream::new_with_commit_clock(
            limits.memory,
            clock,
            Arc::clone(&commit_clock) as _,
        );
        let mut replay_error = None;
        while let Some(recovered) = receiver.recv().await {
            let replayed = match recovered {
                Recovered::Validate(complete) => {
                    let _ = complete.send(replay_error.is_none());
                    continue;
                }
                _ if replay_error.is_some() => continue,
                Recovered::State { encoded } => provider
                    .install_state(&encoded)
                    .await
                    .map_err(|_| LocalStreamError::Corrupt),
                Recovered::Command {
                    committed_at_millis,
                    command,
                } => {
                    commit_clock.pin(committed_at_millis);
                    replay(&provider, command)
                        .await
                        .map_err(LocalStreamError::Replay)
                }
            };
            if let Err(error) = replayed {
                replay_error = Some(error);
            }
        }
        let journal = open.await.map_err(|_| LocalStreamError::Executor)??;
        if let Some(error) = replay_error {
            return Err(error);
        }
        commit_clock.unpin();
        let (changed, _) = watch::channel(0_u64);
        Ok(Self {
            inner: Arc::new(LocalInner {
                provider,
                commit_clock,
                journal: OwnedJournal {
                    journal: Arc::new(Mutex::new(journal)),
                    _ownership_anchor: ownership_anchor,
                },
                visibility: Arc::new(RwLock::new(())),
                synced: watch::channel(0_u64).0,
                changed,
                poisoned: AtomicBool::new(false),
                prepared_compaction: AtomicBool::new(false),
            }),
        })
    }

    fn check_available(&self) -> Result<(), StreamError> {
        if self.inner.poisoned.load(Ordering::Acquire) {
            Err(StreamError::Unavailable)
        } else {
            Ok(())
        }
    }

    fn prepare(
        &self,
        command: CommandRef<'_>,
        meter: Option<&Arc<dyn NativeReadMeter>>,
        pin: bool,
    ) -> LocalCompletion<(PreparedFrame, u64)> {
        let committed_at_millis = self.inner.commit_clock.source.now_unix_millis();
        if let Some(meter) = meter {
            let length = match command {
                CommandRef::Append(request) =>
                    Some(crate::memory::append_encoded_len(request, &crate::memory::MemoryObserver { meter: Some(Arc::clone(meter)) })?),
                CommandRef::Commit(request) =>
                    Some(crate::memory::commit_encoded_len(request, &crate::memory::MemoryObserver { meter: Some(Arc::clone(meter)) })?),
                CommandRef::Fork(_) => None,
            };
            if let Some(length) = length {
                // One length-delimited JournalCommand operation wraps the request.
                let mut rest = length;
                let mut prefix = 1;
                while rest >= 128 { rest >>= 7; prefix += 1; }
                if length.checked_add(prefix + 1).is_none_or(|length| length > MAX_COMMAND_BYTES) {
                    return Err(StreamError::Capacity.into());
                }
            }
        }
        let frame = PreparedFrame::encode(command, committed_at_millis, meter)
            .map_err(publication_error)?;
        self.inner.journal.journal.lock()
            .map_err(|_| LocalCompletionError::Stream(StreamError::Unavailable))?
            .admit(&frame)
            .map_err(publication_error)?;
        if pin { self.inner.commit_clock.pin(committed_at_millis); }
        Ok((frame, committed_at_millis))
    }

    /// Flushes every frame written without waiting for the storage device (see [`deferring_durability`]), blocking the calling thread.
    pub fn flush(&self) -> Result<(), StreamError> {
        let span = obs::OwnedSpan::new(tracing::info_span!(
            "acyclic.stream.flush",
            batch_len = Empty,
            outcome = Empty,
            error.kind = Empty
        ));
        span.in_scope(|| obs::finish(&span, self.flush_all(&span)))
    }

    fn flush_all(&self, span: &tracing::Span) -> Result<(), StreamError> {
        let (target, file, durability) = {
            let journal = self
                .inner
                .journal
                .journal
                .lock()
                .map_err(|_| self.inner.poison())?;
            if journal.synced >= journal.appended {
                return Ok(());
            }
            span.record("batch_len", journal.appended - journal.synced);
            (
                journal.appended,
                Arc::clone(&journal.sync),
                journal.limits.durability,
            )
        };
        sync_file_data(&file, durability).map_err(|_| self.inner.poison())?;
        let mut journal = self
            .inner
            .journal
            .journal
            .lock()
            .map_err(|_| self.inner.poison())?;
        self.inner.publish_synced(&mut journal, target);
        Ok(())
    }

    /// Writes `frame`, in the order its mutation applied, without waiting for the device: [`Self::exclusive`] waits for the flush once it has released visibility, so concurrent mutations share one flush.
    async fn persist(&self, frame: PreparedFrame) -> Result<(), StreamError> {
        let plan = self.plan_persist(frame, None, None)
            .map_err(|_| self.inner.poison())?;
        self.persist_planned(plan, None).await.map_err(LocalCompletionError::ordinary)
    }

    fn plan_persist(
        &self,
        frame: PreparedFrame,
        admission: Option<AdmittedFrameWrite>,
        meter: Option<&Arc<dyn NativeReadMeter>>,
    ) -> std::io::Result<FundedLocalTask<JournalPersistWork>> {
        FundedLocalTask::new(JournalPersistWork {
            frame,
            required: !deferred(),
            admission,
            journal: self.inner.journal.clone(),
        }, meter)
    }

    async fn persist_planned(
        &self,
        plan: FundedLocalTask<JournalPersistWork>,
        meter: Option<&Arc<dyn NativeReadMeter>>,
    ) -> LocalCompletion<()>
    {
        let (physical, observed) = tokio::task::spawn_blocking(
            move || plan.run(JournalPersistWork::run),
        ).await
            .map_err(|_| LocalCompletionError::Stream(self.inner.poison()))?;
        if let Err(error) = physical {
            self.inner.poison();
            // A previously observed original cause remains the primary cause.
            observed?;
            return Err(publication_error(error));
        }
        self.inner.changed.send_modify(|revision| *revision = revision.saturating_add(1));
        let compacted = self.compact_if_due_observed(meter).await;
        observed?;
        compacted
    }

    /// Writes the whole state as the snapshot and starts the journal again, once the journal is half full.
    async fn compact_if_due_observed(
        &self,
        meter: Option<&Arc<dyn NativeReadMeter>>,
    ) -> LocalCompletion<()> {
        if !self.inner.journal.compaction_due()
            .map_err(|_| LocalCompletionError::Stream(self.inner.poison()))?
        {
            return Ok(());
        }
        let state = self.inner.provider.encode_state(meter).await?;
        let inner = Arc::clone(&self.inner);
        let observer = meter.cloned();
        let span = obs::OwnedSpan::new(tracing::debug_span!(
            "acyclic.stream.compact",
            bytes = state.value.len()
        ));
        let operation = FundedLocalTask::new(move || {
            span.in_scope(|| {
                let mut journal = inner.journal.journal.lock()
                    .map_err(|_| LocalCompletionError::Stream(inner.poison()))?;
                let (physical, observed, started) =
                    journal.compact_observed(&state.value, observer.as_ref());
                if physical.is_ok() {
                    let appended = journal.appended;
                    inner.publish_synced(&mut journal, appended);
                } else if started || observer.is_none() {
                    inner.poison();
                }
                observed?;
                physical.map_err(publication_error)
            })
        }, meter)?;
        tokio::task::spawn_blocking(move || operation.run(|operation| operation())).await
            .map_err(|_| LocalCompletionError::Stream(self.inner.poison()))?
    }

    fn plan_flush(
        &self,
        meter: Option<&Arc<dyn NativeReadMeter>>,
    ) -> LocalCompletion<JournalFlushPlan> {
        let journal = self.inner.journal.journal.lock()
            .map_err(|_| LocalCompletionError::Stream(self.inner.poison()))?;
        let admission = meter.map(|meter| OwnedNativeOperationReservation::new(
            Arc::clone(meter), NativeOperationKind::Durability, 1,
        )).transpose()?;
        let allocation = meter.map(|meter| ReadAllocation::new(
            meter, std::mem::size_of::<JournalFlushWork>(),
        )).transpose()?;
        Ok(JournalFlushPlan {
            inner: Arc::clone(&self.inner),
            file: Arc::clone(&journal.sync),
            durability: journal.limits.durability,
            admission,
            _allocation: allocation,
        })
    }

    async fn await_durable_observed(
        &self,
        sequence: u64,
        meter: Option<&Arc<dyn NativeReadMeter>>,
        mut planned: Option<JournalFlushPlan>,
    ) -> LocalCompletion<()> {
        let mut synced = self.inner.synced.subscribe();
        loop {
            let drain_poison = self.inner.poisoned.load(Ordering::Acquire) && planned.is_some();
            if !drain_poison {
                self.check_available()?;
                if *synced.borrow_and_update() >= sequence {
                    return Ok(());
                }
            }
            let lead = {
                let mut journal = self.inner.journal.journal.lock()
                    .map_err(|_| LocalCompletionError::Stream(self.inner.poison()))?;
                if journal.syncing || (!drain_poison && journal.synced >= sequence) {
                    None
                } else {
                    let plan = match planned.take() {
                        Some(plan) => plan,
                        None => {
                            drop(journal);
                            let plan = self.plan_flush(meter)?;
                            journal = self.inner.journal.journal.lock()
                                .map_err(|_| LocalCompletionError::Stream(self.inner.poison()))?;
                            if journal.syncing || journal.synced >= sequence {
                                planned = Some(plan);
                                continue;
                            }
                            plan
                        }
                    };
                    let target = journal.appended;
                    let before = journal.synced;
                    let bytes = journal.bytes;
                    journal.syncing = true;
                    Some((JournalFlushWork { plan, target }, before, bytes))
                }
            };
            match lead {
                Some((work, synced_before, journal_bytes)) => {
                    let target = work.target;
                    tokio::task::spawn_blocking(move || work.run()).await
                        .map_err(|_| LocalCompletionError::Stream(self.inner.poison()))??;
                    tracing::debug!(
                        fsyncs = 1_u64,
                        batch_len = target.saturating_sub(synced_before),
                        journal_bytes,
                        waiters = self.inner.synced.receiver_count(),
                        "acyclic.stream.fsync"
                    );
                    if drain_poison {
                        return Err(StreamError::Unavailable.into());
                    }
                }
                None => {
                    if let Some(meter) = meter {
                        if planned.is_none() { meter.check()?; }
                    }
                    synced.changed().await
                        .map_err(|_| LocalCompletionError::Stream(StreamError::Unavailable))?;
                }
            }
        }
    }

    /// Runs one mutation under exclusive visibility on its own task, so a cancelled caller can neither expose it before its frame is durable nor leave a failed persist unpoisoned.
    async fn exclusive<T, F>(
        &self,
        span: &obs::OwnedSpan,
        mutation: impl FnOnce(Self) -> F + Send + 'static,
    ) -> Result<T, StreamError>
    where
        T: Send + 'static,
        F: Future<Output = Result<T, StreamError>> + Send + 'static,
    {
        self.exclusive_observed(span, None, MutationVisibility::BeforeMutation, move |stream| async move {
            let (result, value) = match mutation(stream).await {
                Ok(value) => (Ok(()), Some(value)),
                Err(error) => (Err(LocalCompletionError::Stream(error)), None),
            };
            MutationCompletion {
                result,
                flush: None,
                _input: None,
                visibility: None,
                value,
                primary_failure: false,
            }
        }).await.map_err(LocalCompletionError::ordinary)
    }

    async fn exclusive_observed<T, F>(
        &self,
        span: &obs::OwnedSpan,
        meter: Option<Arc<dyn NativeReadMeter>>,
        visibility: MutationVisibility,
        mutation: impl FnOnce(Self) -> F + Send + 'static,
    ) -> LocalCompletion<T>
    where
        T: Send + 'static,
        F: Future<Output = MutationCompletion<T>> + Send + 'static,
    {
        self.check_available().inspect_err(|error| obs::failed(span, error.code()))?;
        if let Some(meter) = &meter { meter.check()?; }
        let stream = self.clone();
        let deferred = deferred();
        let operation = span.clone();
        let observer = meter.clone();
        let task = DEFERRED.scope(deferred, async move {
            let result = operation.scope(async {
                let (mut completion, required) = {
                    let _visibility = match visibility {
                        MutationVisibility::BeforeMutation =>
                            Some(Arc::clone(&stream.inner.visibility).write_owned().await),
                        MutationVisibility::FromCommit => None,
                    };
                    stream.check_available()?;
                    if let Some(meter) = &observer { meter.check()?; }
                    {
                        let _compaction_visibility = match visibility {
                            MutationVisibility::BeforeMutation => None,
                            MutationVisibility::FromCommit =>
                                Some(Arc::clone(&stream.inner.visibility).write_owned().await),
                        };
                        if stream.inner.prepared_compaction.load(Ordering::Acquire) {
                            stream.compact_if_due_observed(observer.as_ref()).await?;
                            stream.inner.prepared_compaction.store(false, Ordering::Release);
                        }
                    }
                    let mut completion = mutation(stream.clone()).await;
                    if matches!(visibility, MutationVisibility::BeforeMutation) {
                        stream.inner.commit_clock.unpin();
                    }
                    let required = stream.inner.journal.required();
                    drop(completion.visibility.take());
                    (completion, required)
                };
                let durable = match required {
                    Ok(required) => stream.await_durable_observed(
                        required, observer.as_ref(), completion.flush,
                    ).await,
                    Err(error) => {
                        if let Some(plan) = completion.flush {
                            let work = JournalFlushWork { plan, target: 0 };
                            let _ = tokio::task::spawn_blocking(move || work.run()).await;
                        }
                        Err(LocalCompletionError::Stream(error))
                    }
                };
                completion.result?;
                if !completion.primary_failure {
                    durable?;
                    if let Some(meter) = &observer { meter.check()?; }
                }
                Ok(completion.value.take().expect("successful mutation owns its value"))
            }).await;
            match &result {
                Ok(_) => { let _ = obs::finish(&operation, Ok::<(), StreamError>(())); }
                Err(LocalCompletionError::Stream(error)) => obs::failed(&operation, error.code()),
                Err(LocalCompletionError::GuardedStream(error)) =>
                    obs::failed(&operation, error.source().code()),
                Err(LocalCompletionError::Native(_)) => obs::failed(
                    &operation,
                    if observer.is_some() { "native_work" } else { StreamError::Unavailable.code() },
                ),
            }
            result
        });
        let task = FundedLocalTask::new(task, meter.as_ref())?;
        tokio::spawn(task).await.map_err(|_| {
            obs::failed(span, StreamError::Unavailable.code());
            LocalCompletionError::Stream(StreamError::Unavailable)
        })?
    }

    /// Runs one read under shared visibility, then returns its result once every frame it may have observed is durable.
    async fn visible<T>(
        &self,
        read: impl Future<Output = Result<T, StreamError>>,
    ) -> Result<T, StreamError> {
        self.visible_observed(async { read.await.map_err(LocalCompletionError::Stream) }, None)
            .await.map_err(LocalCompletionError::ordinary)
    }

    async fn visible_observed<T>(
        &self,
        read: impl Future<Output = LocalCompletion<T>>,
        meter: Option<&Arc<dyn NativeReadMeter>>,
    ) -> LocalCompletion<T> {
        self.check_available()?;
        if let Some(meter) = meter { meter.check()?; }
        let (result, required) = {
            let _visibility = self.inner.visibility.read().await;
            self.check_available()?;
            if let Some(meter) = meter { meter.check()?; }
            (read.await, self.inner.journal.required()?)
        };
        let durable = self.await_durable_observed(required, meter, None).await;
        // A produced original cause wins over a later drain observation.
        let result = result?;
        durable?;
        if let Some(meter) = meter { meter.check()?; }
        Ok(result)
    }

    async fn read_visible(&self, request: ReadRequest) -> Result<RecordStream, StreamError> {
        self.visible(self.inner.provider.read(request)).await
    }

    async fn native_visible<T>(
        &self,
        read: impl Future<Output = NativeStreamResult<T>>,
        meter: &Arc<dyn NativeReadMeter>,
    ) -> NativeStreamResult<T> {
        into_native(self.visible_observed(async { from_native(read.await) }, Some(meter)).await, meter)
    }

    /// The ordinary and native calls share frame preparation, visibility, persistence, compaction, clock pinning, and group completion.
    async fn mutation_with<T, F>(
        &self,
        input: MutationInput,
        deadline: Option<u64>,
        meter: Option<Arc<dyn NativeReadMeter>>,
        visibility: MutationVisibility,
        execute: impl FnOnce(MemoryStream, MutationInput, Option<u64>, u64, u64) -> F + Send + 'static,
    ) -> LocalCompletion<T>
    where
        T: Send + 'static,
        F: Future<Output = LocalCompletion<CommitCompletion<T>>> + Send + 'static,
    {
        let is_commit = matches!(&input, MutationInput::Commit(_));
        let span = obs::OwnedSpan::new(match &input {
            MutationInput::Commit(input) => tracing::info_span!(
                "acyclic.stream.commit", items = input.request.mutations.len(),
                outcome = Empty, error.kind = Empty,
            ),
            MutationInput::Append(input) => tracing::info_span!(
                "acyclic.stream.append", items = input.request.records.len(),
                bytes = input.request.records.iter().map(crate::OwnedBytes::len).sum::<usize>(),
                rev = Empty, outcome = Empty, error.kind = Empty,
            ),
        });
        let operation = span.clone();
        let observer = meter.clone();
        span.scope(self.exclusive_observed(&span, meter, visibility, move |stream| async move {
            let mut accepted_flush = None;
            let mut accepted_input = None;
            let mut accepted_visibility = None;
            let mut accepted_value = None;
            let mut primary_failure = false;
            let result = async {
                let command = match &input {
                    MutationInput::Commit(input) => CommandRef::Commit(&input.request),
                    MutationInput::Append(input) => CommandRef::Append(&input.request),
                };
                let (frame, committed_at) = stream.prepare(
                    command, observer.as_ref(),
                    matches!(visibility, MutationVisibility::BeforeMutation),
                )?;
                let frame_bytes = frame.bytes;
                let admission = observer.as_ref()
                    .map(|meter| AdmittedFrameWrite::new(meter, frame.encoded.len()))
                    .transpose()?;
                let mut flush = if observer.is_some() && !deferred() {
                    Some(stream.plan_flush(observer.as_ref())?)
                } else {
                    None
                };
                if is_commit { accepted_flush = flush.take(); }
                let persist = stream.plan_persist(frame, admission, observer.as_ref())?;
                let completion = execute(stream.inner.provider.clone(), input, deadline, committed_at, frame_bytes).await?;
                accepted_input = completion.input;
                accepted_visibility = completion.visibility;
                accepted_value = Some(completion.value);
                primary_failure = completion.primary_failure;
                let persisted = if completion.persist {
                    if !is_commit { accepted_flush = flush.take(); }
                    stream.persist_planned(persist, observer.as_ref()).await
                } else {
                    drop(persist);
                    Ok(())
                };
                if let Some(error) = completion.error {
                    return Err(LocalCompletionError::Native(error));
                }
                if primary_failure {
                    return Ok(());
                }
                persisted?;
                if let Some(revision) = completion.revision { operation.record("rev", revision); }
                Ok(())
            }.await;
            MutationCompletion {
                result, flush: accepted_flush,
                _input: accepted_input,
                visibility: accepted_visibility,
                value: accepted_value,
                primary_failure,
            }
        })).await
    }

    fn native_commit_with_deadline(
        &self,
        request: CommitRequest,
        deadline: Option<u64>,
        meter: Arc<dyn NativeReadMeter>,
    ) -> NativeProviderFuture<'_, CommitOutcome> {
        let before: Option<fn() -> std::future::Ready<Result<(), NativeStreamFailure>>> = None;
        self.native_commit_with_guard(request, deadline, meter, before)
    }

    fn native_commit_with_guard<E, F, Fut>(
        &self,
        request: CommitRequest,
        deadline: Option<u64>,
        meter: Arc<dyn NativeReadMeter>,
        before: Option<F>,
    ) -> NativeProviderFuture<'_, CommitOutcome, E>
    where
        E: From<NativeStreamFailure> + Send + 'static,
        F: FnMut() -> Fut + Clone + Send + 'static,
        Fut: Future<Output = Result<(), E>> + Send + 'static,
    {
        let observer = Arc::clone(&meter);
        let guard_stream = self.clone();
        NativeProviderFuture::new(&meter, async move {
            let input = CommitInput::capture(&request, &observer)?;
            // SDK descriptor custody is accepted before the worker detaches.
            drop(request);
            let execute_meter = Arc::clone(&observer);
            let failure_meter = Arc::clone(&observer);
            let visibility = if before.is_some() {
                MutationVisibility::FromCommit
            } else {
                MutationVisibility::BeforeMutation
            };
            let result = self.mutation_with(
                MutationInput::Commit(input), deadline, Some(observer), visibility,
                move |provider, input, deadline, committed_at, frame_bytes| async move {
                    let MutationInput::Commit(mut input) = input else { unreachable!() };
                    let mut before = before;
                    let guarded = before.is_some();
                    let mut acquire = || {
                        let mut original = before.as_mut().expect("guarded Commit has its original callback").clone();
                        let stream = guard_stream.clone();
                        let meter = Arc::clone(&execute_meter);
                        async move {
                            loop {
                                let available = Arc::clone(&stream.inner.visibility).write_owned().await;
                                drop(available);
                                original().await?;
                                let Ok(visibility) = Arc::clone(&stream.inner.visibility).try_write_owned() else {
                                    // No permission survives a contended wait.
                                    continue;
                                };
                                stream.check_available().map_err(|error| E::from(NativeStreamFailure::new(
                                    error, ReadAllocation::empty(&meter),
                                )))?;
                                let admitted = {
                                    match stream.inner.journal.journal.try_lock() {
                                        Ok(journal) => Some(journal.admit_bytes(frame_bytes)),
                                        Err(std::sync::TryLockError::Poisoned(_)) =>
                                            return Err(E::from(NativeStreamFailure::new(
                                                stream.inner.poison(), ReadAllocation::empty(&meter),
                                            ))),
                                        Err(std::sync::TryLockError::WouldBlock) => None,
                                    }
                                };
                                let Some(admitted) = admitted else {
                                    drop(visibility);
                                    {
                                        let _available = stream.inner.journal.journal.lock()
                                            .map_err(|_| E::from(NativeStreamFailure::new(
                                                stream.inner.poison(), ReadAllocation::empty(&meter),
                                            )))?;
                                    }
                                    continue;
                                };
                                if admitted.is_err() {
                                    return Err(E::from(NativeStreamFailure::new(
                                        StreamError::Capacity, ReadAllocation::empty(&meter),
                                    )));
                                }
                                stream.inner.commit_clock.pin(committed_at);
                                return Ok(CommitVisibility {
                                    _visibility: visibility, clock: Arc::clone(&stream.inner.commit_clock),
                                });
                            }
                        }
                    };
                    let completion = provider.native_commit_completion_guarded_borrowed_inner(
                        &mut input.request, deadline, Arc::clone(&execute_meter),
                        if guarded { Some(&mut acquire) } else { None },
                    ).await;
                    drop(acquire);
                    let (result, visibility, persist) = match completion {
                        Ok(Ok((completion, visibility))) => {
                            let result = match completion.error {
                                Some(error) => Err(error),
                                None => Ok(Ok(completion.outcome)),
                            };
                            (result, visibility, true)
                        }
                        Ok(Err(error)) => (Ok(Err(error)), None, false),
                        Err(error) => (Err(error), None, false),
                    };
                    let primary_failure = !matches!(&result, Ok(Ok(_)));
                    Ok(CommitCompletion {
                        value: GuardedCommitOutput { result, _before: before },
                        error: None, input: Some(MutationInput::Commit(input)), revision: None, visibility, persist, primary_failure,
                    })
                },
            ).await;
            match result {
                Ok(completion) => completion.result,
                Err(LocalCompletionError::Native(error)) => Err(error),
                Err(LocalCompletionError::Stream(error)) => Ok(Err(E::from(NativeStreamFailure::new(
                    error, ReadAllocation::empty(&failure_meter),
                )))),
                Err(LocalCompletionError::GuardedStream(error)) => Ok(Err(E::from(error))),
            }
        })
    }

}

#[async_trait]
impl StreamProvider for LocalStream {
    async fn inspect_idempotency(
        &self,
        idempotency_key: IdempotencyKey,
    ) -> Result<Option<IdempotencyObservation>, StreamError> {
        self.visible(self.inner.provider.inspect_idempotency(idempotency_key))
            .await
    }

    async fn tail(&self, path: StreamPath) -> Result<u64, StreamError> {
        self.visible(self.inner.provider.tail(path)).await
    }

    async fn bounds(&self, path: StreamPath) -> Result<StreamBounds, StreamError> {
        self.visible(self.inner.provider.bounds(path)).await
    }

    async fn append(&self, request: AppendRequest) -> Result<AppendOutcome, StreamError> {
        self.mutation_with(MutationInput::Append(AppendInput { request, _allocation: None }), None, None, MutationVisibility::BeforeMutation, |provider, input, _, _, _| async move {
            let MutationInput::Append(input) = input else { unreachable!() };
            let retain_conflict = input.request.idempotency_key.is_some();
            let value = provider.append(input.request).await?;
            let persist = matches!(value, AppendOutcome::Committed(_)) || retain_conflict;
            let revision = match &value {
                AppendOutcome::Committed(receipt) => Some(receipt.tail),
                _ => None,
            };
            Ok(CommitCompletion { value, error: None, persist, revision, input: None, visibility: None, primary_failure: false })
        }).await.map_err(LocalCompletionError::ordinary)
    }

    async fn fork(&self, request: ForkRequest) -> Result<ForkReceipt, StreamError> {
        let span = obs::OwnedSpan::new(tracing::info_span!(
            "acyclic.stream.fork",
            outcome = Empty,
            error.kind = Empty
        ));
        span.scope(async {
            self.exclusive(&span, |stream| async move {
                let frame = stream.prepare(CommandRef::Fork(&request), None, true).map(|(frame, _)| frame).map_err(LocalCompletionError::ordinary)?;
                let outcome = stream.inner.provider.fork(request).await?;
                stream.persist(frame).await?;
                Ok(outcome)
            })
            .await
        })
        .await
    }

    async fn read(&self, request: ReadRequest) -> Result<RecordStream, StreamError> {
        let span = obs::OwnedSpan::new(tracing::info_span!(
            "acyclic.stream.read",
            rev = request.from,
            items = request.limit,
            outcome = Empty,
            error.kind = Empty
        ));
        let mut observation = ReadObservation {
            span,
            finished: false,
        };
        let result = observation.span.scope(self.read_visible(request)).await;
        observation.finished = true;
        obs::finish(&observation.span, result)
    }

    async fn follow(&self, path: StreamPath, from: u64) -> Result<RecordStream, StreamError> {
        let span = obs::OwnedSpan::new(tracing::info_span!(
            "acyclic.stream.follow",
            rev = from,
            terminal = Empty,
            outcome = Empty,
            error.kind = Empty
        ));
        span.scope(async {
            let mut state = FollowState {
                provider: self.clone(),
                path,
                next: from,
                changed: self.inner.changed.subscribe(),
                done: false,
                span: span.clone(),
            };
            if let Err(error) = self.tail(state.path.clone()).await {
                state.failed(&error);
                return Err(error);
            }
            Ok(span
                .scope(stream::unfold(state, |mut state| async move {
                    if state.done {
                        return None;
                    }
                    loop {
                        match state
                            .provider
                            .read_visible(ReadRequest {
                                path: state.path.clone(),
                                from: state.next,
                                limit: 1,
                            })
                            .await
                        {
                            Ok(mut records) => match records.next().await {
                                Some(Ok(record)) => {
                                    state.next = record.sequence.saturating_add(1);
                                    return Some((Ok(record), state));
                                }
                                Some(Err(error)) => {
                                    state.failed(&error);
                                    return Some((Err(error), state));
                                }
                                None => {}
                            },
                            Err(error) => {
                                state.failed(&error);
                                return Some((Err(error), state));
                            }
                        }
                        if state.changed.changed().await.is_err() {
                            state.failed(&StreamError::Unavailable);
                            return Some((Err(StreamError::Unavailable), state));
                        }
                    }
                }))
                .boxed())
        })
        .await
    }

    async fn children(&self, request: ChildrenRequest) -> Result<ChildStream, StreamError> {
        self.visible(self.inner.provider.children(request)).await
    }

    async fn children_page(
        &self,
        request: ChildrenPageRequest,
    ) -> Result<ChildrenPage, StreamError> {
        self.visible(self.inner.provider.children_page(request))
            .await
    }

    async fn commit(&self, request: CommitRequest) -> Result<CommitOutcome, StreamError> {
        self.mutation_with(MutationInput::Commit(CommitInput { request, _allocation: None }), None, None, MutationVisibility::BeforeMutation, |provider, input, _, _, _| async move {
            let MutationInput::Commit(input) = input else { unreachable!() };
            let value = provider.commit(input.request).await?;
            Ok(CommitCompletion { value, error: None, input: None, revision: None, visibility: None, persist: true, primary_failure: false })
        }).await.map_err(LocalCompletionError::ordinary)
    }

    async fn commit_before(
        &self,
        request: CommitRequest,
        deadline_unix_millis: u64,
    ) -> Result<CommitOutcome, StreamError> {
        self.mutation_with(
            MutationInput::Commit(CommitInput { request, _allocation: None }), Some(deadline_unix_millis), None, MutationVisibility::BeforeMutation,
            |provider, input, deadline, _, _| async move {
                let MutationInput::Commit(input) = input else { unreachable!() };
                let value = provider.commit_before(input.request, deadline.expect("deadline is supplied")).await?;
                Ok(CommitCompletion { value, error: None, input: None, revision: None, visibility: None, persist: true, primary_failure: false })
            },
        ).await.map_err(LocalCompletionError::ordinary)
    }

    async fn read_commit(
        &self,
        commit_id: crate::CommitId,
    ) -> Result<CommittedEnvelope, StreamError> {
        self.visible(self.inner.provider.read_commit(commit_id))
            .await
    }

    fn native_inspect_idempotency(
        &self,
        key: IdempotencyKey,
        meter: Arc<dyn NativeReadMeter>,
    ) -> NativeProviderFuture<'_, Option<IdempotencyObservation>> {
        let observer = Arc::clone(&meter);
        NativeProviderFuture::new(&meter, async move {
            self.native_visible(
                self.inner.provider.native_inspect_idempotency(key, Arc::clone(&observer)),
                &observer,
            ).await
        })
    }

    fn native_append(
        &self,
        request: AppendRequest,
        meter: Arc<dyn NativeReadMeter>,
    ) -> NativeProviderFuture<'_, AppendOutcome> {
        let observer = Arc::clone(&meter);
        NativeProviderFuture::new(&meter, async move {
            let input = AppendInput::capture(&request, &observer)?;
            drop(request);
            let execute_meter = Arc::clone(&observer);
            let failure_meter = Arc::clone(&observer);
            let result = self.mutation_with(
                MutationInput::Append(input), None, Some(observer), MutationVisibility::BeforeMutation,
                move |provider, input, _, _, _| async move {
                    let MutationInput::Append(mut input) = input else { unreachable!() };
                    let retain_conflict = input.request.idempotency_key.is_some();
                    let completion = provider.native_append_completion_borrowed_inner(
                        &mut input.request, execute_meter,
                    ).await.map_err(LocalCompletionError::Native)?
                        .map_err(LocalCompletionError::Stream)?;
                    let persist = matches!(completion.outcome.value(), AppendOutcome::Committed(_))
                        || retain_conflict;
                    let revision = match completion.outcome.value() {
                        AppendOutcome::Committed(receipt) => Some(receipt.tail),
                        _ => None,
                    };
                    Ok(CommitCompletion {
                        value: completion.outcome, error: completion.error,
                        persist, revision, input: Some(MutationInput::Append(input)), visibility: None, primary_failure: false,
                    })
                },
            ).await;
            into_native(result, &failure_meter)
        })
    }

    fn native_tail(
        &self,
        path: StreamPath,
        meter: Arc<dyn NativeReadMeter>,
    ) -> NativeProviderFuture<'_, u64> {
        let observer = Arc::clone(&meter);
        NativeProviderFuture::new(&meter, async move {
            self.native_visible(
                self.inner.provider.native_tail(path, Arc::clone(&observer)), &observer,
            ).await
        })
    }

    fn native_bounds(
        &self,
        path: StreamPath,
        meter: Arc<dyn NativeReadMeter>,
    ) -> NativeProviderFuture<'_, StreamBounds> {
        let observer = Arc::clone(&meter);
        NativeProviderFuture::new(&meter, async move {
            self.native_visible(
                self.inner.provider.native_bounds(path, Arc::clone(&observer)), &observer,
            ).await
        })
    }

    fn native_read(
        &self,
        request: ReadRequest,
        meter: Arc<dyn NativeReadMeter>,
    ) -> NativeProviderFuture<'_, NativeRecordStream> {
        let observer = Arc::clone(&meter);
        NativeProviderFuture::new(&meter, async move {
            let span = obs::OwnedSpan::new(tracing::info_span!(
                "acyclic.stream.read",
                rev = request.from,
                items = request.limit,
                outcome = Empty,
                error.kind = Empty
            ));
            let mut observation = ReadObservation { span, finished: false };
            let result = observation.span.scope(self.native_visible(
                self.inner.provider.native_read(request, Arc::clone(&observer)), &observer,
            )).await;
            observation.finished = true;
            finish_native(&observation.span, result)
        })
    }

    fn native_read_commit(
        &self,
        commit_id: crate::CommitId,
        meter: Arc<dyn NativeReadMeter>,
    ) -> NativeProviderFuture<'_, CommittedEnvelope> {
        let observer = Arc::clone(&meter);
        NativeProviderFuture::new(&meter, async move {
            self.native_visible(
                self.inner.provider.native_read_commit(commit_id, Arc::clone(&observer)), &observer,
            ).await
        })
    }

    fn native_commit(
        &self,
        request: CommitRequest,
        meter: Arc<dyn NativeReadMeter>,
    ) -> NativeProviderFuture<'_, CommitOutcome> {
        self.native_commit_with_deadline(request, None, meter)
    }

    fn native_commit_guarded<E, F, Fut>(
        &self,
        request: CommitRequest,
        deadline_unix_millis: Option<u64>,
        meter: Arc<dyn NativeReadMeter>,
        before: F,
    ) -> NativeProviderFuture<'_, CommitOutcome, E>
    where
        E: From<NativeStreamFailure> + Send + 'static,
        F: FnMut() -> Fut + Clone + Send + 'static,
        Fut: Future<Output = Result<(), E>> + Send + 'static,
    {
        self.native_commit_with_guard(request, deadline_unix_millis, meter, Some(before))
    }

    fn native_prepare_commit(
        &self,
        request: CommitRequest,
        maximum_record_bytes: usize,
        meter: Arc<dyn NativeReadMeter>,
    ) -> NativeProviderFuture<'_, crate::PreparedNativeCommit> {
        let original = Arc::clone(&meter);
        NativeProviderFuture::new(&original, async move {
            let result: LocalCompletion<crate::PreparedNativeCommit> = async {
                self.check_available()?;
                meter.check()?;
                let mut input = CommitInput::capture(&request, &meter)?;
                drop(request);
                let visibility = self.inner.visibility.write().await;
                self.check_available()?;
                self.compact_if_due_observed(Some(&meter)).await?;
                self.inner.prepared_compaction.store(false, Ordering::Release);
                let prepared = self.inner.provider.native_prepare_commit_borrowed_inner(
                    &mut input.request, maximum_record_bytes, Arc::clone(&meter),
                ).await.map_err(LocalCompletionError::Native)?
                    .map_err(LocalCompletionError::Stream)?;
                let recovery = self.inner.provider.native_prepare_recovery_future(&prepared)
                    .map_err(|error| match error {
                        crate::memory::MemoryFailure::Observer(error) => LocalCompletionError::Native(error),
                        crate::memory::MemoryFailure::Provider(error) => LocalCompletionError::Stream(error),
                    })?;
                meter.admit_examination(1, 0)?;
                let maximum = crate::journal::journal_command(CommandRef::Commit(&input.request))
                    .encoded_len_for_record_maximum(maximum_record_bytes)?;
                meter.record_examination(1, 0)?;
                meter.check()?;
                if maximum > MAX_COMMAND_BYTES { return Err(StreamError::LimitExceeded.into()); }
                let maximum_frame = maximum.checked_add(4 + FRAME_TIME_BYTES + FRAME_CHECKSUM_BYTES)
                    .ok_or(LocalCompletionError::Stream(StreamError::Capacity))?;
                let mut frame = PreparedFrame::allocate_buffer(maximum_frame, Some(&meter))
                    .map_err(publication_error)?;
                frame.bytes = maximum_frame as u64;
                self.inner.journal.journal.lock()
                    .map_err(|_| LocalCompletionError::Stream(self.inner.poison()))?
                    .admit(&frame).map_err(publication_error)?;
                meter.admit_examination(1, 0)?;
                meter.admit_codec(
                    maximum as u64, maximum_frame as u64,
                    (b"acyclic-stream-local-frame-v1\0".len() + maximum_frame - FRAME_CHECKSUM_BYTES) as u64,
                )?;
                let admission = AdmittedFrameWrite::new(&meter, maximum_frame)?.close_plan();
                let flush = self.plan_flush(Some(&meter))?;
                let persist = self.plan_persist(frame, Some(admission), Some(&meter))?;
                let slot = crate::NativeCommitRecordSlot::new(&meter)?;
                let received = Arc::clone(&slot);
                let mut synced = self.inner.synced.subscribe();
                let recovering = self.clone();
                let recovery_meter = Arc::clone(&meter);
                let recovery = NativeProviderFuture::try_new(&meter, async move {
                    let visibility = recovering.inner.visibility.read().await;
                    if let Err(error) = recovering.check_available() {
                        return Ok(Err(NativeStreamFailure::new(error, ReadAllocation::empty(&recovery_meter))));
                    }
                    let result = recovery.await;
                    let required = match recovering.inner.journal.required() {
                        Ok(required) => required,
                        Err(error) => return Ok(Err(NativeStreamFailure::new(
                            error, ReadAllocation::empty(&recovery_meter),
                        ))),
                    };
                    drop(visibility);
                    loop {
                        if let Err(error) = recovering.check_available() {
                            return Ok(Err(NativeStreamFailure::new(
                                error, ReadAllocation::empty(&recovery_meter),
                            )));
                        }
                        if *synced.borrow_and_update() >= required { return result; }
                        if synced.changed().await.is_err() {
                            return Ok(Err(NativeStreamFailure::new(
                                StreamError::Unavailable, ReadAllocation::empty(&recovery_meter),
                            )));
                        }
                    }
                })?;
                let worker = FundedLocalTask::new(PreparedLocalWork {
                    persist, input, prepared, record: received,
                    runtime: tokio::runtime::Handle::current(), stream: self.clone(),
                    meter: Arc::clone(&meter), flush,
                }, Some(&meter))?;
                drop(visibility);
                let worker = tokio::task::spawn_blocking(move || worker.run(PreparedLocalWork::run));
                let finish_meter = Arc::clone(&meter);
                let future = match NativeProviderFuture::try_new(&meter, async move {
                    match worker.await {
                        Ok(result) => result,
                        Err(_) => Ok(Err(NativeStreamFailure::new(
                            StreamError::Unavailable, ReadAllocation::empty(&finish_meter),
                        ))),
                    }
                }) {
                    Ok(future) => future,
                    Err(error) => {
                        slot.close();
                        return Err(LocalCompletionError::Native(error));
                    }
                };
                Ok(crate::PreparedNativeCommit::new(future, recovery, slot, maximum_record_bytes))
            }.await;
            into_native(result.map(|prepared| NativeStreamOutput::new(
                prepared, ReadAllocation::empty(&meter),
            )), &meter)
        })
    }

    fn native_commit_before(
        &self,
        request: CommitRequest,
        deadline_unix_millis: u64,
        meter: Arc<dyn NativeReadMeter>,
    ) -> NativeProviderFuture<'_, CommitOutcome> {
        self.native_commit_with_deadline(request, Some(deadline_unix_millis), meter)
    }
}

/// Reads are caller-owned; cancellation while waiting for visibility is terminal.
struct ReadObservation {
    span: obs::OwnedSpan,
    finished: bool,
}
impl Drop for ReadObservation {
    fn drop(&mut self) {
        if !self.finished {
            obs::failed(&self.span, "cancelled");
        }
    }
}

struct FollowState {
    provider: LocalStream,
    path: StreamPath,
    next: u64,
    changed: watch::Receiver<u64>,
    done: bool,
    span: obs::OwnedSpan,
}
impl FollowState {
    fn failed(&mut self, error: &StreamError) {
        self.done = true;
        self.span.record("terminal", "error");
        obs::failed(&self.span, error.code());
    }
}
impl Drop for FollowState {
    fn drop(&mut self) {
        if !self.done {
            self.span.record("terminal", "dropped");
        }
    }
}

struct Journal {
    /// Held locked for as long as the provider is open.
    lock: File,
    file: File,
    /// Which snapshot the journal follows: the one naming this epoch.
    epoch: u64,
    operations: u64,
    bytes: u64,
    limits: LocalStreamLimits,
    /// A second handle on `file`, synchronized without holding the journal, so frames keep being written while earlier ones flush.
    sync: Arc<File>,
    /// How many frames were ever written, across every epoch: the sequence of the latest one.
    appended: u64,
    /// The latest frame written by a mutation that waits for the device.
    required: u64,
    /// Every frame through this sequence is durable.
    synced: u64,
    /// Whether a flush is under way.
    syncing: bool,
    root: PathBuf,
    torn_tail: Option<u64>,
}

impl Drop for Journal {
    fn drop(&mut self) {
        let _ = self.lock.unlock();
    }
}

struct PreparedFrame {
    encoded: Vec<u8>,
    bytes: u64,
    _allocation: Option<ReadAllocation>,
}

/// Only a closed prepared frame may omit fresh admission.
struct FrameWork<'a> {
    meter: Option<&'a Arc<dyn NativeReadMeter>>,
    accepted: bool,
    late: std::io::Result<()>,
}

impl FrameWork<'_> {
    fn admit(&self, encoded: u64, copied: u64, hashed: u64) -> std::io::Result<()> {
        if !self.accepted
            && let Some(meter) = self.meter {
            meter.admit_codec(encoded, copied, hashed)?;
        }
        Ok(())
    }

    fn complete(&mut self, encoded: u64, copied: u64, hashed: u64) -> std::io::Result<()> {
        if let Some(meter) = self.meter {
            let recorded = meter.record_codec(encoded, copied, hashed);
            if self.accepted {
                keep_observation(&mut self.late, recorded);
            } else {
                recorded?;
                meter.check()?;
            }
        }
        Ok(())
    }
}
impl PreparedFrame {
    fn encode<'a>(
        command: impl Into<CommandRef<'a>>,
        committed_at_millis: u64,
        meter: Option<&Arc<dyn NativeReadMeter>>,
    ) -> Result<Self, LocalStreamError> {
        if let Some(meter) = meter { meter.admit_examination(1, 0)?; }
        let journal = journal_command(command);
        if let Some(meter) = meter {
            meter.record_examination(1, 0)?;
            meter.check()?;
        }
        if let Some(meter) = meter {
            meter.admit_examination(1, 0)?;
        }
        let encoded_length = journal.encoded_len()?;
        if let Some(meter) = meter {
            meter.record_examination(1, 0)?;
            meter.check()?;
        }
        if encoded_length > MAX_COMMAND_BYTES {
            return Err(LocalStreamError::InvalidLimits);
        }
        let command_length = u32::try_from(FRAME_TIME_BYTES + encoded_length)
            .map_err(|_| LocalStreamError::InvalidLimits)?;
        let length = command_length.to_le_bytes();
        let command_length_usize =
            usize::try_from(command_length).map_err(|_| LocalStreamError::InvalidLimits)?;
        let capacity = 4_usize
            .checked_add(command_length_usize)
            .and_then(|value| value.checked_add(FRAME_CHECKSUM_BYTES))
            .ok_or(LocalStreamError::InvalidLimits)?;
        let mut frame = Self::allocate_buffer(capacity, meter)?;
        frame.write_wire(&journal, committed_at_millis, encoded_length, length, capacity,
            &mut FrameWork { meter, accepted: false, late: Ok(()) })?;
        Ok(frame)
    }

    /// Actual raw storage can be allocated independently of late scalar/body fill.
    fn allocate_buffer(
        capacity: usize,
        meter: Option<&Arc<dyn NativeReadMeter>>,
    ) -> Result<Self, LocalStreamError> {
        let mut allocation = meter.map(|meter| ReadAllocation::new(meter, capacity)).transpose()?;
        let encoded = frame_buffer(capacity, meter)?;
        if let Some(allocation) = &mut allocation {
            allocation.reconcile_capacity(encoded.capacity())?;
        }
        Ok(Self { encoded, bytes: 0, _allocation: allocation })
    }

    fn write_wire(
        &mut self,
        journal: &crate::wire_codec::CanonicalWire<'_>,
        committed_at_millis: u64,
        encoded_length: usize,
        length: [u8; 4],
        capacity: usize,
        work: &mut FrameWork<'_>,
    ) -> Result<(), LocalStreamError> {
        if capacity > self.encoded.capacity() { return Err(LocalStreamError::InvalidLimits); }
        self.encoded.clear();
        work.admit(encoded_length as u64, capacity as u64, 0)?;
        self.encoded.extend_from_slice(&length);
        self.encoded.extend_from_slice(&committed_at_millis.to_le_bytes());
        journal.encode(&mut self.encoded).map_err(|_| LocalStreamError::InvalidLimits)?;
        work.complete(encoded_length as u64, (capacity - FRAME_CHECKSUM_BYTES) as u64, 0)?;
        let command_bytes = self.encoded.get(4..).ok_or(LocalStreamError::InvalidLimits)?;
        let checksum = frame_checksum(&length, command_bytes, work)?;
        self.encoded.extend_from_slice(&checksum);
        work.complete(0, FRAME_CHECKSUM_BYTES as u64, 0)?;
        self.bytes = u64::try_from(capacity).map_err(|_| LocalStreamError::InvalidLimits)?;
        Ok(())
    }
}

/// Request-owned native allowance, admitted while the mutation can still decline without editing semantic state.
struct AdmittedFrameWrite {
    meter: Arc<dyn NativeReadMeter>,
    operations: Option<OwnedNativeOperationReservation>,
    bytes: OwnedNativeByteReservation,
    closed: bool,
}

enum FrameWriteFailure {
    Io(std::io::Error),
    Incomplete,
}

impl From<FrameWriteFailure> for LocalStreamError {
    fn from(error: FrameWriteFailure) -> Self {
        match error {
            FrameWriteFailure::Io(error) => Self::Io(error),
            FrameWriteFailure::Incomplete => Self::IncompleteFrame,
        }
    }
}

impl AdmittedFrameWrite {
    fn new(meter: &Arc<dyn NativeReadMeter>, length: usize) -> std::io::Result<Self> {
        let operations = OwnedNativeOperationReservation::new(
            Arc::clone(meter), NativeOperationKind::Write, 1,
        )?;
        let bytes = OwnedNativeByteReservation::new(
            Arc::clone(meter), NativeOperationKind::Write, length,
        )?;
        Ok(Self { meter: Arc::clone(meter), operations: Some(operations), bytes, closed: false })
    }

    fn close_plan(mut self) -> Self {
        self.closed = true;
        self
    }

    fn write_all(
        &mut self,
        file: &mut impl Write,
        mut source: &[u8],
    ) -> (Result<(), FrameWriteFailure>, std::io::Result<()>) {
        let mut observation = Ok(());
        let mut physical = Ok(());
        while !source.is_empty() {
            let mut operations = match self.operations.take() {
                Some(operations) => operations,
                None if self.closed => {
                    physical = Err(FrameWriteFailure::Incomplete);
                    break;
                }
                None => match OwnedNativeOperationReservation::new(
                    Arc::clone(&self.meter), NativeOperationKind::Write, 1,
                ) {
                    Ok(operations) => operations,
                    Err(error) => {
                        physical = Err(FrameWriteFailure::Io(error));
                        break;
                    }
                },
            };
            let (written, recorded) = operations.attempt(|| file.write(source));
            let completed = written.as_ref().copied().unwrap_or(0);
            let recorded_bytes = self.bytes.record_partial(completed);
            if observation.is_ok() {
                observation = recorded;
            }
            if observation.is_ok() {
                observation = recorded_bytes;
            }
            match written {
                Ok(0) => {
                    physical = Err(FrameWriteFailure::Io(std::io::ErrorKind::WriteZero.into()));
                    break;
                }
                Ok(count) => {
                    source = &source[count..];
                }
                Err(error) if !self.closed && error.kind() == std::io::ErrorKind::Interrupted => {}
                Err(error) => {
                    physical = Err(FrameWriteFailure::Io(error));
                    break;
                }
            }
        }
        let recorded = self.bytes.record(0);
        if observation.is_ok() {
            observation = recorded;
        }
        (physical, observation)
    }
}

fn attempt_control<T>(
    admission: &mut Option<OwnedNativeOperationReservation>,
    operation: impl FnOnce() -> T,
) -> (T, std::io::Result<()>) {
    match admission {
        Some(admission) => admission.attempt(operation),
        None => (operation(), Ok(())),
    }
}

fn keep_observation(first: &mut std::io::Result<()>, next: std::io::Result<()>) {
    if first.is_ok() { *first = next; }
}

struct ObservedPath {
    path: PathBuf,
    _allocation: Option<ReadAllocation>,
}

fn joined_path_observed(
    root: &Path,
    leaf: &str,
    meter: Option<&Arc<dyn NativeReadMeter>>,
) -> std::io::Result<ObservedPath> {
    let root_bytes = root.as_os_str().as_encoded_bytes().len();
    let capacity = root_bytes.checked_add(leaf.len()).and_then(|length| length.checked_add(1))
        .ok_or_else(|| std::io::Error::from(std::io::ErrorKind::InvalidInput))?;
    if let Some(meter) = meter { meter.admit_codec(0, capacity as u64, 0)?; }
    let mut allocation = meter.map(|meter| ReadAllocation::new(meter, capacity)).transpose()?;
    let mut path = PathBuf::new();
    if let Some(meter) = meter {
        path.try_reserve_exact(capacity).map_err(|error| meter.allocation_failed(error))?;
    } else {
        path.reserve_exact(capacity);
    }
    if let Some(allocation) = &mut allocation { allocation.reconcile_capacity(path.capacity())?; }
    path.push(root);
    let observed = meter.map_or(Ok(()), |meter| meter.record_codec(0, root_bytes as u64, 0));
    let retained = allocation.as_mut().map_or(Ok(()), |allocation| {
        allocation.reconcile_capacity(path.capacity())
    });
    observed?;
    retained?;
    if let Some(meter) = meter { meter.check()?; }
    if let Some(meter) = meter { meter.admit_codec(0, leaf.len() as u64, 0)?; }
    path.push(leaf);
    let observed = meter.map_or(Ok(()), |meter| meter.record_codec(0, leaf.len() as u64, 0));
    let capacity = allocation.as_mut().map_or(Ok(()), |allocation| {
        allocation.reconcile_capacity(path.capacity())
    });
    observed?;
    capacity?;
    if let Some(meter) = meter { meter.check()?; }
    Ok(ObservedPath { path, _allocation: allocation })
}

/// The temporary file's real close is accepted before acquiring the handle.
struct SnapshotFile {
    file: Option<File>,
    meter: Option<Arc<dyn NativeReadMeter>>,
    cleanup: Option<u64>,
}

impl SnapshotFile {
    fn create(
        path: &Path,
        meter: Option<&Arc<dyn NativeReadMeter>>,
        controls: &mut Option<OwnedNativeOperationReservation>,
        observed: &mut std::io::Result<()>,
        started: &mut bool,
    ) -> Result<Self, LocalStreamError> {
        let cleanup = meter.map(|meter| meter.reserve_cleanup_write()).transpose()?;
        let mut owned = Self { file: None, meter: meter.cloned(), cleanup };
        *started = true;
        let (file, recorded) = attempt_control(controls, || File::create(path));
        keep_observation(observed, recorded);
        owned.file = Some(file?);
        Ok(owned)
    }

    fn close(mut self) -> std::io::Result<()> {
        let observed = match (&self.meter, self.cleanup.take()) {
            (Some(meter), Some(cleanup)) => meter.record_cleanup_write(cleanup),
            _ => Ok(()),
        };
        drop(self.file.take());
        observed
    }
}

impl Drop for SnapshotFile {
    fn drop(&mut self) {
        if let (Some(meter), Some(cleanup)) = (&self.meter, self.cleanup.take()) {
            if self.file.is_some() {
                let _ = meter.record_cleanup_write(cleanup);
            } else {
                meter.cancel_cleanup_write(cleanup);
            }
        }
        drop(self.file.take());
    }
}

/// The blocking worker waits without releasing ownership; replay never authorizes truncation or restart after a rejected semantic prefix.
fn validate_recovery(recovered: &mpsc::Sender<Recovered>) -> Result<bool, LocalStreamError> {
    let (complete, validation) = tokio::sync::oneshot::channel();
    recovered
        .blocking_send(Recovered::Validate(complete))
        .map_err(|_| LocalStreamError::Executor)?;
    validation
        .blocking_recv()
        .map_err(|_| LocalStreamError::Executor)
}

impl Journal {
    fn open(
        root: &Path,
        limits: LocalStreamLimits,
        recovered: &mpsc::Sender<Recovered>,
        span: &tracing::Span,
    ) -> Result<Self, LocalStreamError> {
        let lock = lock_root(root, limits)?;
        let epoch = match read_snapshot(root, limits)? {
            Some(snapshot) => {
                recovered
                    .blocking_send(Recovered::State {
                        encoded: snapshot.state,
                    })
                    .map_err(|_| LocalStreamError::Executor)?;
                snapshot.epoch
            }
            None => 0,
        };
        let path = root.join(JOURNAL_FILE);
        let file = OpenOptions::new()
            .create(true)
            .truncate(false)
            .read(true)
            .write(true)
            .open(&path)?;
        let mut journal = Self {
            lock,
            sync: Arc::new(file.try_clone()?),
            file,
            epoch,
            operations: 0,
            bytes: 0,
            limits,
            appended: 0,
            required: 0,
            synced: 0,
            syncing: false,
            root: root.to_path_buf(),
            torn_tail: None,
        };
        let header_bytes =
            u64::try_from(HEADER_BYTES).map_err(|_| LocalStreamError::InvalidLimits)?;
        let length = journal.file.metadata()?.len();
        let mut header = [0_u8; HEADER_BYTES];
        let found = if length < header_bytes {
            None
        } else {
            journal
                .file
                .read_exact(&mut header)
                .map_err(|_| LocalStreamError::Corrupt)?;
            Some(decode_header(&header, limits))
        };
        match found {
            // An incomplete header cannot have acknowledged a frame.
            None => {
                journal.file.seek(SeekFrom::Start(0))?;
                let mut partial = Vec::new();
                journal.file.read_to_end(&mut partial)?;
                if !encode_header(limits, epoch)?.starts_with(&partial) {
                    return Err(LocalStreamError::Corrupt);
                }
                if !validate_recovery(recovered)? {
                    return Ok(journal);
                }
                journal.restart(epoch)?;
                sync_directory(&path, limits.durability)?;
                return Ok(journal);
            }
            Some(Some(found)) if found == epoch => {}
            Some(Some(found)) if found.checked_add(1) == Some(epoch) => {
                if !validate_recovery(recovered)? {
                    return Ok(journal);
                }
                journal.restart(epoch)?;
                return Ok(journal);
            }
            _ => return Err(LocalStreamError::Corrupt),
        }
        journal.replay(length, recovered, span)?;
        if !validate_recovery(recovered)? {
            return Ok(journal);
        }
        if let Some(frame_start) = journal.torn_tail {
            match acyclic_native_runtime::recover_log_tail(
                &mut journal.file,
                frame_start,
                maximum_frame_bytes(),
                native_durability(limits.durability),
                frame_is_valid,
            )? {
                acyclic_native_runtime::LogTail::Torn => {}
                acyclic_native_runtime::LogTail::Corrupt => return Err(LocalStreamError::Corrupt),
            }
        }
        sync_file_data(&journal.file, limits.durability)?;
        Ok(journal)
    }

    /// Hands every whole frame of a journal `length` long to `recovered`, cutting a torn one off the end.
    fn replay(
        &mut self,
        length: u64,
        recovered: &mpsc::Sender<Recovered>,
        span: &tracing::Span,
    ) -> Result<(), LocalStreamError> {
        let limits = self.limits;
        let mut operations = 0_u64;
        let mut valid_length =
            u64::try_from(HEADER_BYTES).map_err(|_| LocalStreamError::InvalidLimits)?;
        let total_length = length;
        while valid_length < total_length {
            let frame_start = valid_length;
            let Some(frame) = read_frame(&mut self.file, total_length - frame_start, None)? else {
                // Repair is deferred until semantic replay has accepted every prefix frame.
                self.torn_tail = Some(frame_start);
                break;
            };
            let (committed_at_millis, command) = frame
                .command
                .split_first_chunk::<8>()
                .ok_or(LocalStreamError::Corrupt)?;
            let command = decode_command(command).map_err(|_| LocalStreamError::Corrupt)?;
            operations = operations.checked_add(1).ok_or(LocalStreamError::Corrupt)?;
            if operations > limits.journal_operations {
                return Err(LocalStreamError::Corrupt);
            }
            recovered
                .blocking_send(Recovered::Command {
                    committed_at_millis: u64::from_le_bytes(*committed_at_millis),
                    command,
                })
                .map_err(|_| LocalStreamError::Executor)?;
            valid_length = valid_length
                .checked_add(frame_bytes(&frame.length))
                .ok_or(LocalStreamError::Corrupt)?;
        }
        if valid_length > limits.journal_bytes {
            return Err(LocalStreamError::InvalidLimits);
        }
        self.file.seek(SeekFrom::End(0))?;
        span.record("frames", operations);
        span.record("bytes", valid_length);
        self.operations = operations;
        self.bytes = valid_length;
        Ok(())
    }

    /// Empties the journal to follow the snapshot of `epoch`.
    fn restart(&mut self, epoch: u64) -> Result<(), LocalStreamError> {
        self.file.set_len(0)?;
        self.file.seek(SeekFrom::Start(0))?;
        self.file.write_all(&encode_header(self.limits, epoch)?)?;
        sync_file(&self.file, self.limits.durability)?;
        self.epoch = epoch;
        self.operations = 0;
        self.bytes = u64::try_from(HEADER_BYTES).map_err(|_| LocalStreamError::InvalidLimits)?;
        // The header was synchronized after the snapshot holding every frame.
        self.synced = self.appended;
        Ok(())
    }

    fn compaction_due(&self) -> bool {
        self.operations > 0
            && (self.operations >= self.limits.journal_operations / 2
                || self.bytes >= self.limits.journal_bytes / 2)
    }

    /// Makes `state`, which holds every frame so far, the snapshot the journal follows, then empties the journal.
    fn compact_observed(
        &mut self,
        state: &[u8],
        meter: Option<&Arc<dyn NativeReadMeter>>,
    ) -> (Result<(), LocalStreamError>, std::io::Result<()>, bool) {
        let mut observed = Ok(());
        let mut started = false;
        let physical = (|| {
            let epoch = self.epoch.checked_add(1).ok_or(LocalStreamError::Corrupt)?;
            let temporary = joined_path_observed(&self.root, SNAPSHOT_TEMPORARY, meter)?;
            let snapshot = joined_path_observed(&self.root, SNAPSHOT_FILE, meter)?;
            let encoded = encode_snapshot(self.limits, epoch, state, meter)?;
            let header = encode_header(self.limits, epoch)?;
            // Admit fixed boundaries before create; admit data retries separately.
            let mut controls = meter.map(|meter| OwnedNativeOperationReservation::new(
                Arc::clone(meter), NativeOperationKind::Write, 3,
            )).transpose()?;
            let mut seeks = meter.map(|meter| OwnedNativeOperationReservation::new(
                Arc::clone(meter), NativeOperationKind::Read, 1,
            )).transpose()?;
            let mut durability = meter.map(|meter| OwnedNativeOperationReservation::new(
                Arc::clone(meter), NativeOperationKind::Durability, 3,
            )).transpose()?;
            let mut snapshot_write = meter.map(|meter| AdmittedFrameWrite::new(meter, encoded.value.len()))
                .transpose()?;
            let mut header_write = meter.map(|meter| AdmittedFrameWrite::new(meter, header.len()))
                .transpose()?;
            let mut file = SnapshotFile::create(
                &temporary.path, meter, &mut controls, &mut observed, &mut started,
            )?;
            let handle = file.file.as_mut().expect("successful create retains its held file");
            let (written, recorded) = match &mut snapshot_write {
                Some(admission) => admission.write_all(handle, &encoded.value),
                None => (handle.write_all(&encoded.value).map_err(FrameWriteFailure::Io), Ok(())),
            };
            keep_observation(&mut observed, recorded);
            written?;
            let (synced, recorded) = attempt_control(&mut durability, || sync_file(handle, self.limits.durability));
            keep_observation(&mut observed, recorded);
            synced?;
            keep_observation(&mut observed, file.close());
            let (renamed, recorded) = attempt_control(&mut controls, || {
                std::fs::rename(&temporary.path, &snapshot.path)
            });
            keep_observation(&mut observed, recorded);
            renamed?;
            let (synced, recorded) = attempt_control(&mut durability, || {
                sync_directory(&snapshot.path, self.limits.durability)
            });
            keep_observation(&mut observed, recorded);
            synced?;
            let (truncated, recorded) = attempt_control(&mut controls, || self.file.set_len(0));
            keep_observation(&mut observed, recorded);
            truncated?;
            let (seeked, recorded) = attempt_control(&mut seeks, || self.file.seek(SeekFrom::Start(0)));
            keep_observation(&mut observed, recorded);
            seeked?;
            let (written, recorded) = match &mut header_write {
                Some(admission) => admission.write_all(&mut self.file, &header),
                None => (self.file.write_all(&header).map_err(FrameWriteFailure::Io), Ok(())),
            };
            keep_observation(&mut observed, recorded);
            written?;
            let (synced, recorded) = attempt_control(&mut durability, || sync_file(&self.file, self.limits.durability));
            keep_observation(&mut observed, recorded);
            synced?;
            self.epoch = epoch;
            self.operations = 0;
            self.bytes = HEADER_BYTES as u64;
            self.synced = self.appended;
            Ok(())
        })();
        (physical, observed, started)
    }

    fn admit(&self, frame: &PreparedFrame) -> Result<(), LocalStreamError> {
        self.admit_bytes(frame.bytes)
    }

    fn admit_bytes(&self, bytes: u64) -> Result<(), LocalStreamError> {
        if self.operations >= self.limits.journal_operations
            || self
                .bytes
                .checked_add(bytes)
                .is_none_or(|value| value > self.limits.journal_bytes)
        {
            return Err(LocalStreamError::InvalidLimits);
        }
        Ok(())
    }

    /// Uses the same cursor and counters.
    fn append_observed(
        &mut self,
        frame: &PreparedFrame,
        required: bool,
        admission: Option<&mut AdmittedFrameWrite>,
    ) -> (Result<(), LocalStreamError>, std::io::Result<()>) {
        let (written, observed) = match admission {
            Some(admission) => admission.write_all(&mut self.file, &frame.encoded),
            None => (self.file.write_all(&frame.encoded).map_err(FrameWriteFailure::Io), Ok(())),
        };
        if let Err(error) = written {
            return (Err(error.into()), observed);
        }
        tracing::trace!(
            bytes = frame.bytes,
            required,
            "acyclic.stream.journal_append"
        );
        self.appended += 1;
        if required {
            self.required = self.appended;
        }
        self.operations += 1;
        self.bytes += frame.bytes;
        (Ok(()), observed)
    }

    /// Whether frames were written since the journal last flushed.
    #[cfg(test)]
    fn unflushed(&self) -> bool {
        self.appended > self.synced
    }
}

/// Creates `root` if it is missing and takes its exclusive lock.
fn lock_root(root: &Path, limits: LocalStreamLimits) -> Result<File, LocalStreamError> {
    std::fs::create_dir_all(root)?;
    sync_directory(root, limits.durability)?;
    let lock = OpenOptions::new()
        .create(true)
        .truncate(false)
        .read(true)
        .write(true)
        .open(root.join(LOCK_FILE))?;
    lock.try_lock_exclusive().map_err(|error| {
        if acyclic_native_runtime::is_exclusive_lock_contention(&error) {
            LocalStreamError::AlreadyOpen
        } else {
            LocalStreamError::Io(error)
        }
    })?;
    Ok(lock)
}

/// The largest frame the journal holds: length prefix, commit timestamp, command and checksum.
fn maximum_frame_bytes() -> usize {
    4 + FRAME_TIME_BYTES + MAX_COMMAND_BYTES + FRAME_CHECKSUM_BYTES
}

/// The bytes of the frame whose length prefix is `length_bytes`.
fn frame_bytes(length_bytes: &[u8; 4]) -> u64 {
    4 + u64::from(u32::from_le_bytes(*length_bytes)) + FRAME_CHECKSUM_BYTES as u64
}

/// The commit timestamp and command length a frame's prefix declares, if the journal can hold it.
fn command_length(length_bytes: [u8; 4]) -> Option<usize> {
    usize::try_from(u32::from_le_bytes(length_bytes))
        .ok()
        .filter(|length| {
            (FRAME_TIME_BYTES + 1..=FRAME_TIME_BYTES + MAX_COMMAND_BYTES).contains(length)
        })
}

/// One whole journal frame whose checksum matched.
struct JournalFrame {
    length: [u8; 4],
    command: Vec<u8>,
    // The buffer is destroyed before its original invocation releases capacity.
    _allocation: Option<ReadAllocation>,
}

/// Reads the frame at the file's cursor, `remaining` bytes before its end, or `None` when it is invalid.
fn frame_buffer(
    capacity: usize,
    meter: Option<&Arc<dyn NativeReadMeter>>,
) -> std::io::Result<Vec<u8>> {
    let mut value = Vec::new();
    if let Some(meter) = meter {
        value.try_reserve_exact(capacity).map_err(|error| meter.allocation_failed(error))?;
    } else {
        value.reserve_exact(capacity);
    }
    Ok(value)
}

fn read_frame(
    file: &mut impl Read,
    remaining: u64,
    meter: Option<&Arc<dyn NativeReadMeter>>,
) -> Result<Option<JournalFrame>, LocalStreamError> {
    if let Some(meter) = meter {
        meter.check()?;
    }
    let mut length_bytes = [0_u8; 4];
    if remaining < 4 {
        return Ok(None);
    }
    read_exact_observed(file, &mut length_bytes, meter)?;
    let Some(length) = command_length(length_bytes) else {
        return Ok(None);
    };
    if remaining < frame_bytes(&length_bytes) {
        return Ok(None);
    }
    let mut allocation = meter.map(|meter| ReadAllocation::new(meter, length)).transpose()?;
    let mut encoded = frame_buffer(length, meter)?;
    encoded.resize(length, 0);
    if let Some(allocation) = &mut allocation {
        allocation.reconcile_capacity(encoded.capacity())?;
    }
    read_exact_observed(file, &mut encoded, meter)?;
    let mut checksum = [0_u8; FRAME_CHECKSUM_BYTES];
    read_exact_observed(file, &mut checksum, meter)?;
    if let Some(meter) = meter {
        meter.admit_examination(1, 0)?;
    }
    let valid = frame_checksum(&length_bytes, &encoded, &mut FrameWork { meter, accepted: false, late: Ok(()) })? == checksum;
    if let Some(meter) = meter {
        meter.record_examination(1, 0)?;
        meter.check()?;
    }
    Ok(valid.then_some(JournalFrame {
        length: length_bytes,
        command: encoded,
        _allocation: allocation,
    }))
}

/// Uses the ordinary cursor and records every completed prefix before checking late cancellation.
fn read_exact_observed(
    file: &mut impl Read,
    mut destination: &mut [u8],
    meter: Option<&Arc<dyn NativeReadMeter>>,
) -> std::io::Result<()> {
    while !destination.is_empty() {
        let mut operations = meter.map(|meter| NativeOperationReservation::new(
            meter.as_ref(), NativeOperationKind::Read, 1,
        )).transpose()?;
        let mut bytes = meter.map(|meter| NativeByteReservation::new(
            meter.as_ref(), NativeOperationKind::Read, destination.len(),
        )).transpose()?;
        let (result, recorded) = match &mut operations {
            Some(operations) => operations.attempt(|| file.read(destination)),
            None => (file.read(destination), Ok(())),
        };
        let completed = result.as_ref().copied().unwrap_or(0);
        let recorded_bytes = bytes.as_mut().map_or(Ok(()), |bytes| bytes.record(completed));
        match result {
            Err(error) if error.kind() == std::io::ErrorKind::Interrupted => {
                recorded?;
                recorded_bytes?;
                if let Some(meter) = meter {
                    meter.check()?;
                }
                continue;
            }
            Err(error) => return Err(error),
            Ok(0) => {
                recorded?;
                recorded_bytes?;
                return Err(std::io::ErrorKind::UnexpectedEof.into());
            }
            Ok(completed) => {
                recorded?;
                recorded_bytes?;
                if let Some(meter) = meter {
                    meter.check()?;
                }
                destination = &mut destination[completed..];
            }
        }
    }
    Ok(())
}

/// Whether a whole frame with a matching checksum starts `bytes`.
fn frame_is_valid(bytes: &[u8]) -> bool {
    let Some(length_bytes) = bytes.first_chunk::<4>() else {
        return false;
    };
    let Some(length) = command_length(*length_bytes) else {
        return false;
    };
    let Some(encoded) = bytes.get(4..4 + length) else {
        return false;
    };
    bytes
        .get(4 + length..4 + length + FRAME_CHECKSUM_BYTES)
        .is_some_and(|checksum| checksum == frame_checksum(length_bytes, encoded, &mut FrameWork { meter: None, accepted: false, late: Ok(()) }).expect("unmetered checksum cannot fail"))
}

fn sync_file(file: &File, durability: LocalDurability) -> std::io::Result<()> {
    acyclic_native_runtime::sync_file(file, native_durability(durability))
}

fn sync_file_data(file: &File, durability: LocalDurability) -> std::io::Result<()> {
    acyclic_native_runtime::sync_data(file, native_durability(durability))
}

fn native_durability(durability: LocalDurability) -> acyclic_native_runtime::Durability {
    match durability {
        LocalDurability::FullFlush => acyclic_native_runtime::Durability::Full,
        LocalDurability::Barrier => acyclic_native_runtime::Durability::Barrier,
    }
}

fn sync_directory(path: &Path, durability: LocalDurability) -> Result<(), LocalStreamError> {
    acyclic_native_runtime::sync_parent(path, native_durability(durability))?;
    Ok(())
}

fn validate_limits(limits: LocalStreamLimits) -> Result<(), LocalStreamError> {
    let memory = limits.memory;
    if memory.paths == 0
        || memory.path_bytes == 0
        || memory.records == 0
        || memory.payload_bytes == 0
        || memory.commits == 0
        || memory.idempotency_results == 0
        || limits.journal_operations == 0
        || limits.journal_bytes
            < u64::try_from(HEADER_BYTES).map_err(|_| LocalStreamError::InvalidLimits)?
    {
        Err(LocalStreamError::InvalidLimits)
    } else {
        Ok(())
    }
}

fn encode_limits(limits: LocalStreamLimits) -> Result<[u8; LIMITS_BYTES], LocalStreamError> {
    let memory = limits.memory;
    let values = [
        u64::try_from(memory.paths).map_err(|_| LocalStreamError::InvalidLimits)?,
        u64::try_from(memory.path_bytes).map_err(|_| LocalStreamError::InvalidLimits)?,
        u64::try_from(memory.records).map_err(|_| LocalStreamError::InvalidLimits)?,
        u64::try_from(memory.payload_bytes).map_err(|_| LocalStreamError::InvalidLimits)?,
        u64::try_from(memory.commits).map_err(|_| LocalStreamError::InvalidLimits)?,
        u64::try_from(memory.idempotency_results).map_err(|_| LocalStreamError::InvalidLimits)?,
        limits.journal_operations,
        limits.journal_bytes,
    ];
    let mut encoded = [0_u8; LIMITS_BYTES];
    for (destination, value) in encoded.chunks_exact_mut(8).zip(values) {
        destination.copy_from_slice(&value.to_le_bytes());
    }
    Ok(encoded)
}

fn encode_header(limits: LocalStreamLimits, epoch: u64) -> Result<[u8; HEADER_BYTES], LocalStreamError> {
    let mut encoded = [0_u8; HEADER_BYTES];
    let (magic, rest) = encoded.split_at_mut(HEADER_MAGIC.len());
    magic.copy_from_slice(HEADER_MAGIC);
    let (configuration, time) = rest.split_at_mut(LIMITS_BYTES);
    configuration.copy_from_slice(&encode_limits(limits)?);
    time.copy_from_slice(&epoch.to_le_bytes());
    Ok(encoded)
}

/// The epoch of a whole journal header written for `limits`.
fn decode_header(header: &[u8], limits: LocalStreamLimits) -> Option<u64> {
    let rest = header.strip_prefix(HEADER_MAGIC)?;
    let (found, epoch) = rest.split_at_checked(LIMITS_BYTES)?;
    (found == encode_limits(limits).ok()?).then_some(())?;
    Some(u64::from_le_bytes(epoch.try_into().ok()?))
}

struct Snapshot {
    epoch: u64,
    state: Vec<u8>,
}

fn snapshot_checksum(
    body: &[u8],
    meter: Option<&Arc<dyn NativeReadMeter>>,
) -> std::io::Result<[u8; 32]> {
    let domain = b"acyclic-stream-local-snapshot-v1\0";
    let hashed = domain.len() + body.len();
    if let Some(meter) = meter { meter.admit_codec(0, 0, hashed as u64)?; }
    let mut hasher = Sha256::new();
    hasher.update(domain);
    hasher.update(body);
    let checksum = hasher.finalize().into();
    if let Some(meter) = meter {
        meter.record_codec(0, 0, hashed as u64)?;
        meter.check()?;
    }
    Ok(checksum)
}

fn encode_snapshot(
    limits: LocalStreamLimits,
    epoch: u64,
    state: &[u8],
    meter: Option<&Arc<dyn NativeReadMeter>>,
) -> Result<ObservedWire<Vec<u8>>, LocalStreamError> {
    let capacity = SNAPSHOT_HEADER_BYTES.checked_add(state.len())
        .and_then(|length| length.checked_add(32)).ok_or(LocalStreamError::InvalidLimits)?;
    let mut allocation = meter.map(|meter| ReadAllocation::new(meter, capacity)).transpose()?;
    let mut encoded = frame_buffer(capacity, meter)?;
    if let Some(allocation) = &mut allocation { allocation.reconcile_capacity(encoded.capacity())?; }
    if let Some(meter) = meter { meter.admit_codec(capacity as u64, capacity as u64, 0)?; }
    encoded.extend_from_slice(SNAPSHOT_MAGIC);
    encoded.extend_from_slice(&encode_limits(limits)?);
    encoded.extend_from_slice(&epoch.to_le_bytes());
    encoded.extend_from_slice(state);
    if let Some(meter) = meter {
        meter.record_codec(encoded.len() as u64, encoded.len() as u64, 0)?;
        meter.check()?;
    }
    let checksum = snapshot_checksum(&encoded, meter)?;
    encoded.extend_from_slice(&checksum);
    if let Some(meter) = meter {
        meter.record_codec(32, 32, 0)?;
        meter.check()?;
    }
    Ok(ObservedWire { value: encoded, allocation })
}

/// The snapshot below `root`, if one was installed.
fn read_snapshot(
    root: &Path,
    limits: LocalStreamLimits,
) -> Result<Option<Snapshot>, LocalStreamError> {
    let encoded = match std::fs::read(root.join(SNAPSHOT_FILE)) {
        Ok(encoded) => encoded,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(None),
        Err(error) => return Err(error.into()),
    };
    let (body, checksum) = encoded
        .split_last_chunk::<32>()
        .ok_or(LocalStreamError::Corrupt)?;
    if snapshot_checksum(body, None)? != *checksum {
        return Err(LocalStreamError::Corrupt);
    }
    let rest = body
        .strip_prefix(SNAPSHOT_MAGIC)
        .ok_or(LocalStreamError::Corrupt)?;
    let (found, rest) = rest
        .split_at_checked(LIMITS_BYTES)
        .ok_or(LocalStreamError::Corrupt)?;
    if found != encode_limits(limits)? {
        return Err(LocalStreamError::Corrupt);
    }
    let (epoch, rest) = rest
        .split_first_chunk::<8>()
        .ok_or(LocalStreamError::Corrupt)?;
    let state = rest;
    Ok(Some(Snapshot {
        epoch: u64::from_le_bytes(*epoch),
        state: state.to_vec(),
    }))
}

fn frame_checksum(
    length: &[u8; 4],
    command: &[u8],
    work: &mut FrameWork<'_>,
) -> std::io::Result<[u8; 32]> {
    let domain = b"acyclic-stream-local-frame-v1\0";
    let hashed = domain.len() + length.len() + command.len();
    work.admit(0, 0, hashed as u64)?;
    let mut hasher = Sha256::new();
    hasher.update(domain);
    hasher.update(length);
    hasher.update(command);
    let checksum = hasher.finalize().into();
    work.complete(0, 0, hashed as u64)?;
    Ok(checksum)
}

#[cfg(test)]
#[allow(
    clippy::indexing_slicing,
    reason = "fixture byte offsets and captured field lookups intentionally fail the test if expected evidence is absent"
)]
mod tests {
    use super::*;

    #[derive(Debug)]
    struct NativeDenial(&'static str);

    impl std::fmt::Display for NativeDenial {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            f.write_str(self.0)
        }
    }
    impl std::error::Error for NativeDenial {}

    #[derive(Debug)]
    enum NativeGuardFailure {
        Provider(NativeStreamFailure),
        Io(std::io::Error),
        Owner(u64),
    }

    impl From<NativeStreamFailure> for NativeGuardFailure {
        fn from(error: NativeStreamFailure) -> Self { Self::Provider(error) }
    }

    impl std::fmt::Display for NativeGuardFailure {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            match self {
                Self::Provider(error) => std::fmt::Display::fmt(error, f),
                Self::Io(error) => std::fmt::Display::fmt(error, f),
                Self::Owner(identity) => write!(f, "original guard {identity}"),
            }
        }
    }

    impl std::error::Error for NativeGuardFailure {
        fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
            match self {
                Self::Provider(error) => Some(error),
                Self::Io(error) => Some(error),
                Self::Owner(_) => None,
            }
        }
    }

    struct GuardLeaseWitness(Arc<AtomicU64>);

    impl Drop for GuardLeaseWitness {
        fn drop(&mut self) { self.0.fetch_add(1, Ordering::SeqCst); }
    }

    #[derive(Default)]
    struct LocalNativeMeter {
        cancelled: AtomicBool,
        deny_allocation: AtomicBool,
        deny_durability: AtomicBool,
        cancel_on_write: AtomicBool,
        cancel_on_copy: AtomicBool,
        cancel_on_durability: AtomicBool,
        expire_after_hash: Mutex<Option<(Arc<TestClock>, u64)>>,
        memory: AtomicU64,
        allocations: AtomicU64,
        read_operations: AtomicU64,
        write_operations: AtomicU64,
        durability_operations: AtomicU64,
        read_bytes: AtomicU64,
        write_bytes: AtomicU64,
        copied: AtomicU64,
        hashed: AtomicU64,
        held_operations: [AtomicU64; 3],
        held_bytes: [AtomicU64; 2],
        held_cleanup: AtomicU64,
        cleanup_tokens: AtomicU64,
    }

    impl LocalNativeMeter {
        fn original(self: &Arc<Self>) -> Arc<dyn NativeReadMeter> { self.clone() }

        fn denial(reason: &'static str) -> std::io::Error {
            std::io::Error::other(NativeDenial(reason))
        }

        fn retain(&self, bytes: u64) {
            self.memory.fetch_add(bytes, Ordering::SeqCst);
        }

        fn kind(kind: NativeOperationKind) -> usize {
            match kind {
                NativeOperationKind::Read => 0,
                NativeOperationKind::Write => 1,
                NativeOperationKind::Durability => 2,
            }
        }

        fn physical_write(&self, bytes: u64) -> std::io::Result<()> {
            self.write_bytes.fetch_add(bytes, Ordering::SeqCst);
            if bytes != 0 && self.cancel_on_write.swap(false, Ordering::SeqCst) {
                self.cancelled.store(true, Ordering::SeqCst);
            }
            self.check()
        }

        fn assert_refunded(&self) {
            for held in &self.held_operations { assert_eq!(held.load(Ordering::SeqCst), 0); }
            for held in &self.held_bytes { assert_eq!(held.load(Ordering::SeqCst), 0); }
            assert_eq!(self.held_cleanup.load(Ordering::SeqCst), 0);
        }
    }

    impl NativeReadMeter for LocalNativeMeter {
        fn check(&self) -> std::io::Result<()> {
            if self.cancelled.load(Ordering::SeqCst) {
                return Err(Self::denial("cancelled"));
            }
            Ok(())
        }
        fn reserve_operations(&self, kind: NativeOperationKind, count: u64) -> std::io::Result<()> {
            self.check()?;
            if kind == NativeOperationKind::Durability && self.deny_durability.load(Ordering::SeqCst) {
                return Err(Self::denial("durability"));
            }
            self.held_operations[Self::kind(kind)].fetch_add(count, Ordering::SeqCst);
            Ok(())
        }
        fn cancel_reserved_operations(&self, kind: NativeOperationKind, count: u64) {
            self.held_operations[Self::kind(kind)].fetch_sub(count, Ordering::SeqCst);
        }
        fn record_reserved_operation(&self, kind: NativeOperationKind) -> std::io::Result<()> {
            self.held_operations[Self::kind(kind)].fetch_sub(1, Ordering::SeqCst);
            match kind {
                NativeOperationKind::Read => { self.read_operations.fetch_add(1, Ordering::SeqCst); }
                NativeOperationKind::Write => { self.write_operations.fetch_add(1, Ordering::SeqCst); }
                NativeOperationKind::Durability => {
                    self.durability_operations.fetch_add(1, Ordering::SeqCst);
                    if self.cancel_on_durability.swap(false, Ordering::SeqCst) {
                        self.cancelled.store(true, Ordering::SeqCst);
                    }
                }
            }
            self.check()
        }
        fn admit_write_bytes(&self, _: usize) -> std::io::Result<()> { self.check() }
        fn reserve_bytes(&self, kind: NativeOperationKind, maximum: u64) -> std::io::Result<()> {
            self.check()?;
            self.held_bytes[Self::kind(kind)].fetch_add(maximum, Ordering::SeqCst);
            Ok(())
        }
        fn cancel_reserved_bytes(&self, kind: NativeOperationKind, maximum: u64) {
            self.held_bytes[Self::kind(kind)].fetch_sub(maximum, Ordering::SeqCst);
        }
        fn record_reserved_bytes(&self, kind: NativeOperationKind, maximum: u64, actual: u64)
        -> std::io::Result<()> {
            self.held_bytes[Self::kind(kind)].fetch_sub(maximum, Ordering::SeqCst);
            match kind {
                NativeOperationKind::Read => self.record_read(actual as usize),
                NativeOperationKind::Write => self.physical_write(actual),
                NativeOperationKind::Durability => unreachable!("durability transfers no bytes"),
            }
        }
        fn allocate(&self, bytes: u64) -> std::io::Result<()> {
            self.check()?;
            if self.deny_allocation.load(Ordering::SeqCst) {
                return Err(Self::denial("allocation"));
            }
            self.allocations.fetch_add(1, Ordering::SeqCst);
            self.retain(bytes);
            Ok(())
        }
        fn reserve_allocation(&self, _: u64) -> std::io::Result<()> { Err(std::io::ErrorKind::Unsupported.into()) }
        fn cancel_reserved_allocation(&self, _: u64) {}
        fn record_reserved_allocation(&self, _: u64) -> std::io::Result<()> { Err(std::io::ErrorKind::Unsupported.into()) }
        fn allocation_failed(&self, error: std::collections::TryReserveError) -> std::io::Error {
            std::io::Error::new(std::io::ErrorKind::OutOfMemory, error)
        }
        fn allocate_refcount_header(&self) -> std::io::Result<()> { self.allocate(0) }
        fn adopt(&self, bytes: u64) -> std::io::Result<()> {
            self.check()?;
            self.retain(bytes);
            Ok(())
        }
        fn record_completed_adoption(&self, bytes: u64) -> (u64, std::io::Result<()>) {
            self.retain(bytes);
            (bytes, self.check())
        }
        fn admit_codec(&self, _: u64, _: u64, _: u64) -> std::io::Result<()> { self.check() }
        fn record_codec(&self, _: u64, copied: u64, hashed: u64) -> std::io::Result<()> {

            self.copied.fetch_add(copied, Ordering::SeqCst);
            self.hashed.fetch_add(hashed, Ordering::SeqCst);
            if copied != 0 && self.cancel_on_copy.swap(false, Ordering::SeqCst) {
                self.cancelled.store(true, Ordering::SeqCst);
            }
            if hashed != 0
                && let Some((clock, now)) = self.expire_after_hash.lock()
                    .expect("native clock seam is not poisoned").take()
            {
                clock.0.store(now, Ordering::SeqCst);
            }
            self.check()
        }
        fn examine(&self, items: u64, copied: u64) -> std::io::Result<()> {
            self.admit_examination(items, copied)?;
            self.record_examination(items, copied)
        }
        fn admit_examination(&self, _: u64, _: u64) -> std::io::Result<()> { self.check() }
        fn record_examination(&self, _: u64, copied: u64) -> std::io::Result<()> {

            self.copied.fetch_add(copied, Ordering::SeqCst);
            self.check()
        }
        fn release(&self, bytes: u64) { self.memory.fetch_sub(bytes, Ordering::SeqCst); }
        fn admit_bytes(&self, _: usize) -> std::io::Result<()> { self.check() }
        fn admit_read(&self, _: usize) -> std::io::Result<()> { self.admit_operations(1) }
        fn admit_operations(&self, _: u64) -> std::io::Result<()> { self.check() }
        fn record_operations(&self, count: u64) -> std::io::Result<()> {
            self.read_operations.fetch_add(count, Ordering::SeqCst);
            self.check()
        }
        fn record_read(&self, bytes: usize) -> std::io::Result<()> {
            self.read_bytes.fetch_add(bytes as u64, Ordering::SeqCst);
            self.check()
        }
        fn admit_write(&self, _: usize) -> std::io::Result<()> { self.admit_write_operations(1) }
        fn record_write(&self, bytes: usize) -> std::io::Result<()> { self.physical_write(bytes as u64) }
        fn admit_write_operations(&self, _: u64) -> std::io::Result<()> { self.check() }
        fn record_write_operations(&self, count: u64) -> std::io::Result<()> {
            self.write_operations.fetch_add(count, Ordering::SeqCst);
            self.check()
        }
        fn admit_durability(&self) -> std::io::Result<()> {
            self.check()?;
            self.durability_operations.fetch_add(1, Ordering::SeqCst);
            Ok(())
        }
        fn reserve_cleanup_write(&self) -> std::io::Result<u64> {
            self.check()?;
            self.held_cleanup.fetch_add(1, Ordering::SeqCst);
            Ok(self.cleanup_tokens.fetch_add(1, Ordering::SeqCst) + 1)
        }
        fn cancel_cleanup_write(&self, _: u64) { self.held_cleanup.fetch_sub(1, Ordering::SeqCst); }
        fn record_cleanup_write(&self, _: u64) -> std::io::Result<()> {
            self.held_cleanup.fetch_sub(1, Ordering::SeqCst);

            self.write_operations.fetch_add(1, Ordering::SeqCst);
            self.check()
        }
    }

    fn assert_original_denial(error: &std::io::Error, expected: &'static str) {
        let cause = error.get_ref().and_then(|cause| cause.downcast_ref::<NativeDenial>());
        assert_eq!(cause.map(|cause| cause.0), Some(expected));
    }

    #[tokio::test]
    async fn native_denial_precedes_read_capture() -> Result<(), Box<dyn std::error::Error>> {
        let directory = tempfile::tempdir()?;
        let provider = LocalStream::open(directory.path(), LocalStreamLimits::default()).await?;
        let meter = Arc::new(LocalNativeMeter::default());
        meter.deny_allocation.store(true, Ordering::SeqCst);
        let error = provider.native_read(ReadRequest {
            path: StreamPath::new("uncaptured")?, from: 0, limit: 1,
        }, meter.original()).await.err().ok_or("native read unexpectedly captured")?;
        assert_original_denial(&error, "allocation");
        assert_eq!(meter.allocations.load(Ordering::SeqCst), 0);
        assert_eq!(meter.memory.load(Ordering::SeqCst), 0);
        assert_eq!(meter.copied.load(Ordering::SeqCst), 0);
        assert_eq!(meter.read_operations.load(Ordering::SeqCst), 0);
        meter.assert_refunded();
        Ok(())
    }

    fn keyed_append(path: &str, tail: u64) -> Result<AppendRequest, StreamError> {
        Ok(AppendRequest {
            path: StreamPath::new(path)?,
            records: vec![OwnedBytes::from_static(b"native-append-record")],
            if_tail: Some(tail),
            idempotency_key: Some(IdempotencyKey::new(OwnedBytes::from(format!("{path}@{tail}")))?),
        })
    }

    #[tokio::test]
    async fn native_append_lost_ack_reopens_exact_append_identity()
    -> Result<(), Box<dyn std::error::Error>> {
        let directory = tempfile::tempdir()?;
        let limits = LocalStreamLimits::default();
        let provider = LocalStream::open(directory.path(), limits).await?;
        let request = keyed_append("native-append-lost-ack", 0)?;
        let key = request.idempotency_key.clone().ok_or("append fixture lacks a key")?;
        let meter = Arc::new(LocalNativeMeter::default());
        meter.cancel_on_write.store(true, Ordering::SeqCst);
        let error = provider.native_append(request.clone(), meter.original()).await.err()
            .ok_or("accepted cancelled append fabricated an acknowledgement")?;
        assert_original_denial(&error, "cancelled");
        let accepted = provider.inspect_idempotency(key.clone()).await?
            .ok_or("accepted append identity disappeared")?;
        let crate::IdempotencyOutcome::Append(outcome) = &accepted.outcome else {
            return Err("native append was rewritten into the Commit family".into());
        };
        assert!(matches!(outcome, AppendOutcome::Committed(_)));
        drop(provider);
        let reopened = LocalStream::open(directory.path(), limits).await?;
        assert_eq!(reopened.inspect_idempotency(key.clone()).await?, Some(accepted.clone()));
        assert_eq!(reopened.append(request.clone()).await?, *outcome);
        assert_eq!(reopened.tail(request.path.clone()).await?, 1);
        // Reusing the same bytes/key through Commit is a different operation.
        let wrong_family = CommitRequest {
            conditions: vec![crate::CommitCondition::Tail { path: request.path.clone(), expected: 1 }],
            mutations: vec![crate::CommitMutation::Append { path: request.path, records: request.records }],
            idempotency_key: key,
        };
        meter.cancelled.store(false, Ordering::SeqCst);
        let failure = reopened.native_commit(wrong_family, meter.original()).await?
            .err().ok_or("Commit incorrectly reused an Append identity")?;
        assert_eq!(failure.source(), &StreamError::IdempotencyMismatch);
        drop(failure);
        drop(reopened);
        meter.assert_refunded();
        assert_eq!(meter.memory.load(Ordering::SeqCst), 0);
        Ok(())
    }

    #[tokio::test]
    async fn native_append_keeps_keyed_conflict_and_does_not_journal_unkeyed_conflict()
    -> Result<(), Box<dyn std::error::Error>> {
        let directory = tempfile::tempdir()?;
        let limits = LocalStreamLimits::default();
        let provider = LocalStream::open(directory.path(), limits).await?;
        let path = StreamPath::new("native-append-conflict")?;
        provider.append(AppendRequest {
            path: path.clone(), records: vec![OwnedBytes::from_static(b"seed")],
            if_tail: Some(0), idempotency_key: None,
        }).await?;
        let request = keyed_append(path.as_str(), 0)?;
        let key = request.idempotency_key.clone().ok_or("append fixture lacks a key")?;
        let meter = Arc::new(LocalNativeMeter::default());
        let expected = AppendOutcome::TailConflict { actual_tail: 1 };
        let conflict = provider.native_append(request.clone(), meter.original()).await??;
        assert_eq!(conflict.value(), &expected);
        drop(conflict);
        let after_keyed = std::fs::metadata(directory.path().join(JOURNAL_FILE))?.len();
        let mut unkeyed = request.clone();
        unkeyed.idempotency_key = None;
        let conflict = provider.native_append(unkeyed, meter.original()).await??;
        assert_eq!(conflict.value(), &expected);
        drop(conflict);
        assert_eq!(std::fs::metadata(directory.path().join(JOURNAL_FILE))?.len(), after_keyed);
        let accepted = provider.inspect_idempotency(key.clone()).await?
            .ok_or("keyed conflict lost its original identity")?;
        assert_eq!(accepted.outcome, crate::IdempotencyOutcome::Append(expected.clone()));
        drop(provider);
        let reopened = LocalStream::open(directory.path(), limits).await?;
        assert_eq!(reopened.inspect_idempotency(key).await?, Some(accepted));
        assert_eq!(reopened.append(request).await?, expected);
        assert_eq!(reopened.tail(path).await?, 1);
        drop(reopened);
        meter.assert_refunded();
        assert_eq!(meter.memory.load(Ordering::SeqCst), 0);
        Ok(())
    }

    fn request_control_capacity(request: &CommitRequest) -> usize {
        request.conditions.capacity() * std::mem::size_of::<crate::CommitCondition>()
            + request.mutations.capacity() * std::mem::size_of::<crate::CommitMutation>()
            + request.mutations.iter().map(|mutation| {
                let records = match mutation {
                    crate::CommitMutation::Append { records, .. }
                    | crate::CommitMutation::Fork { records, .. } => records,
                };
                records.capacity() * std::mem::size_of::<OwnedBytes>()
            }).sum::<usize>()
    }

    #[tokio::test]
    async fn native_request_copy_cancellation_precedes_detached_publication()
    -> Result<(), Box<dyn std::error::Error>> {
        let directory = tempfile::tempdir()?;
        let provider = LocalStream::open(directory.path(), LocalStreamLimits::default()).await?;
        let request = keyed_commit("uncaptured-request", 0)?;
        let key = request.idempotency_key.clone();
        let meter = Arc::new(LocalNativeMeter::default());
        meter.cancel_on_copy.store(true, Ordering::SeqCst);
        let error = provider.native_commit(request, meter.original()).await.err()
            .ok_or("cancelled capture escaped into publication")?;
        assert_original_denial(&error, "cancelled");
        assert_eq!(meter.copied.load(Ordering::SeqCst),
            std::mem::size_of::<crate::CommitCondition>() as u64);
        assert_eq!(meter.hashed.load(Ordering::SeqCst), 0);
        assert_eq!(meter.write_operations.load(Ordering::SeqCst), 0);
        assert_eq!(meter.durability_operations.load(Ordering::SeqCst), 0);
        assert_eq!(meter.memory.load(Ordering::SeqCst), 0);
        assert!(provider.inspect_idempotency(key).await?.is_none());
        assert_eq!(std::fs::metadata(directory.path().join(JOURNAL_FILE))?.len(), HEADER_BYTES as u64);
        meter.assert_refunded();
        Ok(())
    }

    #[tokio::test]
    async fn native_durability_denial_precedes_semantic_cas()
    -> Result<(), Box<dyn std::error::Error>> {
        let directory = tempfile::tempdir()?;
        let provider = LocalStream::open(directory.path(), LocalStreamLimits::default()).await?;
        let meter = Arc::new(LocalNativeMeter::default());
        meter.deny_durability.store(true, Ordering::SeqCst);
        let request = keyed_commit("not-published", 0)?;
        let key = request.idempotency_key.clone();
        let error = provider.native_commit(request, meter.original()).await.err()
            .ok_or("native commit unexpectedly passed the durability gate")?;
        assert_original_denial(&error, "durability");
        assert!(provider.inspect_idempotency(key).await?.is_none());
        assert_eq!(provider.tail(StreamPath::new("not-published")?).await, Err(StreamError::NotFound));
        assert_eq!(std::fs::metadata(directory.path().join(JOURNAL_FILE))?.len(), HEADER_BYTES as u64);
        assert_eq!(meter.write_operations.load(Ordering::SeqCst), 0);
        assert_eq!(meter.durability_operations.load(Ordering::SeqCst), 0);
        assert_eq!(meter.memory.load(Ordering::SeqCst), 0);
        assert!(meter.hashed.load(Ordering::SeqCst) > 0);
        meter.assert_refunded();
        Ok(())
    }


    #[tokio::test]
    async fn borrowed_persisted_outcomes_match_unchanged_generated_schema()
    -> Result<(), Box<dyn std::error::Error>> {
        use prost::Message as _;
        use crate::wire_codec::CanonicalWire;
        let provider = MemoryStream::default();
        let append_key = IdempotencyKey::new(OwnedBytes::from_static(b"wire-append"))?;
        let fork_key = IdempotencyKey::new(OwnedBytes::from_static(b"wire-fork"))?;
        let commit_key = IdempotencyKey::new(OwnedBytes::from_static(b"wire-commit"))?;
        provider.append(AppendRequest {
            path: StreamPath::new("wire/source")?,
            records: vec![OwnedBytes::new(), OwnedBytes::from_static(b"body")],
            if_tail: Some(0), idempotency_key: Some(append_key.clone()),
        }).await?;
        provider.fork(ForkRequest {
            source: StreamPath::new("wire/source")?, destination: StreamPath::new("wire/fork")?,
            at_tail: Some(1), idempotency_key: Some(fork_key.clone()),
        }).await?;
        let outcome = provider.commit(CommitRequest {
            conditions: vec![
                crate::CommitCondition::Tail { path: StreamPath::new("wire/source")?, expected: 2 },
                crate::CommitCondition::Tail { path: StreamPath::new("wire/fork")?, expected: 1 },
                crate::CommitCondition::Absent { path: StreamPath::new("wire/new-fork")? },
            ],
            mutations: vec![
                crate::CommitMutation::Append { path: StreamPath::new("wire/fork")?,
                    records: vec![OwnedBytes::from_static(b"append")] },
                crate::CommitMutation::Fork {
                    source: StreamPath::new("wire/source")?, destination: StreamPath::new("wire/new-fork")?,
                    at_tail: 2, records: vec![OwnedBytes::from_static(b"forked")],
                },
            ], idempotency_key: commit_key.clone(),
        }).await?;
        let CommitOutcome::Committed(envelope) = outcome else { return Err("fixture conflict".into()); };
        assert_eq!(CanonicalWire::Envelope(&envelope).encode_to_vec()?,
            crate::wire_codec::envelope_to_wire(envelope.clone()).encode_to_vec());
        for key in [append_key, fork_key, commit_key] {
            let observation = provider.inspect_idempotency(key).await?.ok_or("missing wire identity")?;
            assert_eq!(CanonicalWire::Observation(
                &observation.idempotency_key.0, &observation.request_digest, &observation.outcome,
            ).encode_to_vec()?, crate::wire_codec::observation_wire(observation).encode_to_vec());
        }
        let key = IdempotencyKey::new(OwnedBytes::from_static(b"wire-conflicts"))?;
        let observation = IdempotencyObservation {
            idempotency_key: key, request_digest: [7; 32],
            outcome: crate::IdempotencyOutcome::Commit(CommitOutcome::Conflict(vec![
                crate::CommitConflict::Tail {
                    path: StreamPath::new("wire/source")?, expected: 0, actual: Some(0),
                },
                crate::CommitConflict::Tail {
                    path: StreamPath::new("wire/missing")?, expected: u64::MAX, actual: None,
                },
                crate::CommitConflict::Exists { path: StreamPath::new("wire/source")? },
            ])),
        };
        assert_eq!(CanonicalWire::Observation(
            &observation.idempotency_key.0, &observation.request_digest, &observation.outcome,
        ).encode_to_vec()?, crate::wire_codec::observation_wire(observation).encode_to_vec());
        Ok(())
    }

    async fn prepared_fixture(provider: &LocalStream) -> Result<CommitRequest, StreamError> {
        for path in ["prepared/owner", "prepared/target"] {
            provider.append(AppendRequest {
                path: StreamPath::new(path)?,
                records: vec![OwnedBytes::from_static(b"seed")],
                if_tail: Some(0), idempotency_key: None,
            }).await?;
        }
        Ok(CommitRequest {
            conditions: vec![
                crate::CommitCondition::Tail { path: StreamPath::new("prepared/owner")?, expected: 1 },
                crate::CommitCondition::Tail { path: StreamPath::new("prepared/target")?, expected: 1 },
            ],
            mutations: vec![crate::CommitMutation::Append {
                path: StreamPath::new("prepared/target")?, records: vec![OwnedBytes::new()],
            }],
            idempotency_key: IdempotencyKey::new(OwnedBytes::from_static(b"original-prepared-key"))?,
        })
    }

    #[tokio::test]
    async fn prepared_denial_precedes_worker_and_canonical_edit()
    -> Result<(), Box<dyn std::error::Error>> {
        let directory = tempfile::tempdir()?;
        let provider = LocalStream::open(directory.path(), LocalStreamLimits::default()).await?;
        let request = prepared_fixture(&provider).await?;
        let meter = Arc::new(LocalNativeMeter::default());
        meter.deny_allocation.store(true, Ordering::SeqCst);
        let error = match provider.native_prepare_commit(request, 64, meter.original()).await {
            Err(error) => error,
            _ => return Err("denied preparation produced an accepted packet".into()),
        };
        assert_original_denial(&error, "allocation");
        assert_eq!(provider.tail(StreamPath::new("prepared/target")?).await?, 1);
        assert_eq!(meter.write_operations.load(Ordering::SeqCst), 0);
        assert_eq!(meter.memory.load(Ordering::SeqCst), 0);
        meter.assert_refunded();
        Ok(())
    }

    #[tokio::test]
    async fn prepared_late_cancel_drains_original_frame_and_recovery_without_fresh_admission()
    -> Result<(), Box<dyn std::error::Error>> {
        let directory = tempfile::tempdir()?;
        let provider = LocalStream::open(directory.path(), LocalStreamLimits::default()).await?;
        let mut request = prepared_fixture(&provider).await?;
        let meter = Arc::new(LocalNativeMeter::default());
        let packet = provider.native_prepare_commit(request.clone(), 64, meter.original()).await??;
        let (mut packet, packet_guard) = packet.into_parts();
        let recovery = packet.take_recovery_future().ok_or("missing precreated recovery")?;
        assert!(packet.take_recovery_future().is_none());
        // A prepared worker waiting for its body holds no provider lock.
        assert_eq!(provider.tail(StreamPath::new("prepared/target")?).await?, 1);
        let allocations = meter.allocations.load(Ordering::SeqCst);
        meter.cancelled.store(true, Ordering::SeqCst);
        let error = packet.finish(OwnedBytes::from_static(b"actual-completion")).await
            .err().ok_or("cancelled completion invented an acknowledgement")?;
        assert_original_denial(&error, "cancelled");
        assert_eq!(meter.allocations.load(Ordering::SeqCst), allocations);
        assert!(meter.write_bytes.load(Ordering::SeqCst) > 0);
        let recovery_error = recovery.await.err().ok_or("late cancelled inspection fabricated success")?;
        assert_original_denial(&recovery_error, "cancelled");
        assert_eq!(meter.allocations.load(Ordering::SeqCst), allocations);
        drop(packet_guard);
        drop(provider);
        let reopened = LocalStream::open(directory.path(), LocalStreamLimits::default()).await?;
        let [crate::CommitMutation::Append { records, .. }] = request.mutations.as_mut_slice()
            else { return Err("fixture changed family".into()); };
        records[0] = OwnedBytes::from_static(b"actual-completion");
        let observed = reopened.inspect_idempotency(request.idempotency_key.clone()).await?
            .ok_or("original prepared identity disappeared on reopen")?;
        let crate::IdempotencyOutcome::Commit(expected) = observed.outcome
            else { return Err("prepared operation changed canonical family".into()); };
        assert_eq!(reopened.commit(request).await?, expected);
        meter.assert_refunded();
        Ok(())
    }

    #[test]
    fn prepared_partial_write_stops_at_exact_accepted_boundary()
    -> Result<(), Box<dyn std::error::Error>> {
        struct Partial { bytes: Vec<u8> }
        impl Write for Partial {
            fn write(&mut self, bytes: &[u8]) -> std::io::Result<usize> {
                let length = bytes.len().min(3);
                self.bytes.extend_from_slice(&bytes[..length]);
                Ok(length)
            }
            fn flush(&mut self) -> std::io::Result<()> { Ok(()) }
        }
        let meter = Arc::new(LocalNativeMeter::default());
        let mut plan = AdmittedFrameWrite::new(&meter.original(), 6)?.close_plan();
        let mut writer = Partial { bytes: Vec::new() };
        let (physical, observed) = plan.write_all(&mut writer, b"abcdef");
        assert!(matches!(physical, Err(FrameWriteFailure::Incomplete)));
        observed?;
        assert_eq!(writer.bytes, b"abc");
        assert_eq!(meter.write_bytes.load(Ordering::SeqCst), 3);
        drop(plan);
        meter.assert_refunded();
        Ok(())
    }

    #[tokio::test]
    async fn abandoned_prepared_packet_releases_worker_capacity_without_edit()
    -> Result<(), Box<dyn std::error::Error>> {
        let directory = tempfile::tempdir()?;
        let provider = LocalStream::open(directory.path(), LocalStreamLimits::default()).await?;
        let request = prepared_fixture(&provider).await?;
        let meter = Arc::new(LocalNativeMeter::default());
        let packet = provider.native_prepare_commit(request, 64, meter.original()).await??;
        let (packet, guard) = packet.into_parts();
        drop(packet);
        drop(guard);
        tokio::time::timeout(std::time::Duration::from_secs(5), async {
            while meter.memory.load(Ordering::SeqCst) != 0 { tokio::task::yield_now().await; }
        }).await?;
        assert_eq!(provider.tail(StreamPath::new("prepared/target")?).await?, 1);
        assert_eq!(meter.write_operations.load(Ordering::SeqCst), 0);
        meter.assert_refunded();
        Ok(())
    }

    #[tokio::test]
    async fn prepared_reply_drop_keeps_accepted_durability_and_input_custody()
    -> Result<(), Box<dyn std::error::Error>> {
        let _serial = PERSIST_BLOCKER_TESTS.lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let directory = tempfile::tempdir()?;
        let provider = LocalStream::open(directory.path(), LocalStreamLimits::default()).await?;
        let request = prepared_fixture(&provider).await?;
        let meter = Arc::new(LocalNativeMeter::default());
        let packet = provider.native_prepare_commit(request, 64, meter.original()).await??;
        let (packet, packet_guard) = packet.into_parts();
        let (started_send, started_receive) = std::sync::mpsc::sync_channel(1);
        let (release_send, release_receive) = std::sync::mpsc::sync_channel(1);
        *JOURNAL_PERSIST_BLOCKER.lock().map_err(|_| "poisoned fixture seam")? = Some((
            Arc::as_ptr(&provider.inner.journal.journal) as usize, started_send, release_receive,
        ));
        let future = packet.finish(OwnedBytes::from_static(b"accepted-without-reply"));
        drop(future);
        drop(packet_guard);
        tokio::task::spawn_blocking(move || started_receive.recv()).await??;
        assert!(meter.memory.load(Ordering::SeqCst) > 0);
        assert!(meter.write_bytes.load(Ordering::SeqCst) > 0);
        assert_eq!(meter.durability_operations.load(Ordering::SeqCst), 0);
        release_send.send(())?;
        assert_eq!(provider.tail(StreamPath::new("prepared/target")?).await?, 2);
        drop(provider);
        tokio::time::timeout(std::time::Duration::from_secs(5), async {
            while meter.memory.load(Ordering::SeqCst) != 0 { tokio::task::yield_now().await; }
        }).await?;
        meter.assert_refunded();
        Ok(())
    }

    #[tokio::test]
    async fn native_lost_ack_drains_flush_and_reopens_original_identity()
    -> Result<(), Box<dyn std::error::Error>> {
        let directory = tempfile::tempdir()?;
        let limits = LocalStreamLimits::default();
        let provider = LocalStream::open(directory.path(), limits).await?;
        let request = keyed_commit("lost-native-ack", 0)?;
        let key = request.idempotency_key.clone();
        let meter = Arc::new(LocalNativeMeter::default());
        meter.cancel_on_write.store(true, Ordering::SeqCst);
        let error = provider.native_commit(request.clone(), meter.original()).await.err()
            .ok_or("cancelled accepted mutation fabricated an acknowledgement")?;
        assert_original_denial(&error, "cancelled");
        assert!(std::fs::metadata(directory.path().join(JOURNAL_FILE))?.len() > HEADER_BYTES as u64);
        let accepted = provider.inspect_idempotency(key.clone()).await?
            .ok_or("accepted original identity disappeared")?;
        drop(provider);
        let reopened = LocalStream::open(directory.path(), limits).await?;
        assert_eq!(reopened.inspect_idempotency(key).await?, Some(accepted.clone()));
        assert_eq!(reopened.tail(StreamPath::new("lost-native-ack")?).await?, 1);
        let crate::IdempotencyOutcome::Commit(outcome) = &accepted.outcome else {
            return Err("reopened an unrelated operation family".into());
        };
        assert_eq!(&reopened.commit_before(request, 0).await?, outcome);
        drop(accepted);
        drop(error);
        meter.assert_refunded();
        assert_eq!(meter.memory.load(Ordering::SeqCst), 0);
        Ok(())
    }

    #[tokio::test]
    async fn native_compaction_drains_accepted_controls_and_actual_close()
    -> Result<(), Box<dyn std::error::Error>> {
        let directory = tempfile::tempdir()?;
        let limits = LocalStreamLimits { journal_operations: 2, ..LocalStreamLimits::default() };
        let provider = LocalStream::open(directory.path(), limits).await?;
        let request = keyed_commit("compact-native-ack", 0)?;
        let key = request.idempotency_key.clone();
        let meter = Arc::new(LocalNativeMeter::default());
        meter.cancel_on_durability.store(true, Ordering::SeqCst);
        let error = provider.native_commit(request.clone(), meter.original()).await.err()
            .ok_or("late cancelled compaction fabricated an acknowledgement")?;
        assert_original_denial(&error, "cancelled");
        assert_eq!(std::fs::metadata(directory.path().join(JOURNAL_FILE))?.len(), HEADER_BYTES as u64);
        assert!(directory.path().join(SNAPSHOT_FILE).is_file());
        assert!(!directory.path().join(SNAPSHOT_TEMPORARY).exists());
        let accepted = provider.inspect_idempotency(key.clone()).await?
            .ok_or("compaction erased original identity")?;
        drop(provider);
        let reopened = LocalStream::open(directory.path(), limits).await?;
        assert_eq!(reopened.inspect_idempotency(key).await?, Some(accepted.clone()));
        assert_eq!(reopened.tail(StreamPath::new("compact-native-ack")?).await?, 1);
        let crate::IdempotencyOutcome::Commit(outcome) = &accepted.outcome else {
            return Err("snapshot retained an unrelated operation".into());
        };
        assert_eq!(&reopened.commit_before(request, 0).await?, outcome);
        drop(accepted);
        drop(error);
        meter.assert_refunded();
        assert_eq!(meter.memory.load(Ordering::SeqCst), 0);
        Ok(())
    }

    #[tokio::test]
    async fn native_commit_before_checks_actual_cas_and_preserves_replay()
    -> Result<(), Box<dyn std::error::Error>> {
        let directory = tempfile::tempdir()?;
        let clock = Arc::new(TestClock::default());
        clock.0.store(99, Ordering::SeqCst);
        let provider = LocalStream::open_with_clock(
            directory.path(), LocalStreamLimits::default(), clock.clone(),
        ).await?;
        let request = keyed_commit("native-expired", 0)?;
        let journal_before = std::fs::read(directory.path().join(JOURNAL_FILE))?;
        let meter = Arc::new(LocalNativeMeter::default());
        *meter.expire_after_hash.lock().map_err(|_| "native clock seam was poisoned")? =
            Some((clock.clone(), 100));
        let expired = provider.native_commit_before(request.clone(), 100, meter.original()).await?;
        assert!(matches!(expired.as_ref().map_err(NativeStreamFailure::source), Err(StreamError::DeadlineElapsed)));
        assert_eq!(clock.0.load(Ordering::SeqCst), 100);
        assert_eq!(std::fs::read(directory.path().join(JOURNAL_FILE))?, journal_before);
        assert_eq!(meter.durability_operations.load(Ordering::SeqCst), 0);
        drop(expired);
        assert_eq!(meter.memory.load(Ordering::SeqCst), 0);
        meter.assert_refunded();
        let accepted = provider.commit(request.clone()).await?;
        let replay = provider.native_commit_before(request, 0, meter.original()).await??;
        assert_eq!(replay.value(), &accepted);
        drop(replay);
        assert_eq!(provider.tail(StreamPath::new("native-expired")?).await?, 1);
        meter.assert_refunded();
        Ok(())
    }

    #[test]
    fn cancelled_real_open_retains_ownership_until_journal_open_stops()
    -> Result<(), Box<dyn std::error::Error>> {
        use tracing::instrument::WithSubscriber as _;
        use tracing_subscriber::layer::SubscriberExt as _;
        let captured = obs::tests::Capture::default();
        let dispatch =
            tracing::Dispatch::new(tracing_subscriber::Registry::default().with(captured.clone()));
        let runtime = tokio::runtime::Builder::new_multi_thread()
            .worker_threads(1)
            .max_blocking_threads(1)
            .enable_all()
            .build()?;
        let (blocking_started_tx, blocking_started_rx) = std::sync::mpsc::sync_channel(0);
        let (release_blocker_tx, release_blocker_rx) = std::sync::mpsc::sync_channel(0);
        let blocker = runtime.spawn_blocking(move || {
            let _ = blocking_started_tx.send(());
            let _ = release_blocker_rx.recv();
        });
        blocking_started_rx.recv()?;

        runtime.block_on(async move {
            let directory = tempfile::tempdir()?;
            let lifecycle = Arc::new(tokio::sync::Mutex::new(()));
            let ownership = Arc::clone(&lifecycle).lock_owned().await;
            let anchor = OwnershipAnchor::new(ownership);
            let root = directory.path().to_path_buf();
            let (submitted_tx, submitted_rx) = tokio::sync::oneshot::channel();
            {
                let mut hook = JOURNAL_OPEN_SUBMITTED
                    .lock()
                    .map_err(|_| "journal-open test hook was poisoned")?;
                *hook = Some((root.clone(), submitted_tx));
            }
            let opening = tokio::spawn(
                async move {
                    LocalStream::open_with_ownership_anchor(
                        root,
                        LocalStreamLimits::default(),
                        anchor,
                    )
                    .await
                }
                .with_subscriber(dispatch),
            );

            submitted_rx.await?;
            opening.abort();
            let cancelled = opening.await;
            assert!(
                cancelled
                    .as_ref()
                    .is_err_and(tokio::task::JoinError::is_cancelled)
            );
            let ownership_retained = Arc::clone(&lifecycle).try_lock_owned().is_err();

            release_blocker_tx.send(())?;
            blocker.await?;
            tokio::task::spawn_blocking(|| ()).await?;
            tokio::time::timeout(
                std::time::Duration::from_secs(1),
                Arc::clone(&lifecycle).lock_owned(),
            )
            .await?;
            assert!(
                ownership_retained,
                "caller cancellation must not release ownership from active Journal::open"
            );
            let fields = captured.fields("acyclic.stream.open");
            assert_eq!(
                fields["outcome"], "ok",
                "detached work owns its successful completion"
            );
            assert!(!fields.contains_key("error.kind"));
            Ok::<_, Box<dyn std::error::Error>>(())
        })
    }

    #[test]
    fn cancelled_persist_retains_ownership_until_the_journal_task_stops()
    -> Result<(), Box<dyn std::error::Error>> {
        let _serial = PERSIST_BLOCKER_TESTS
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        use tracing::instrument::WithSubscriber as _;
        use tracing_subscriber::layer::SubscriberExt as _;
        let captured = obs::tests::Capture::default();
        let dispatch =
            tracing::Dispatch::new(tracing_subscriber::Registry::default().with(captured.clone()));
        let runtime = tokio::runtime::Builder::new_multi_thread()
            .worker_threads(1)
            .max_blocking_threads(1)
            .enable_all()
            .build()?;

        runtime.block_on(async move {
            let directory = tempfile::tempdir()?;
            let lifecycle = Arc::new(tokio::sync::Mutex::new(()));
            let ownership = Arc::clone(&lifecycle).lock_owned().await;
            let provider = LocalStream::open_with_ownership_anchor(
                directory.path(),
                LocalStreamLimits::default(),
                OwnershipAnchor::new(ownership),
            )
            .await?;

            let journal_identity = Arc::as_ptr(&provider.inner.journal.journal) as usize;
            let (started_tx, started_rx) = std::sync::mpsc::sync_channel(0);
            let (release_tx, release_rx) = std::sync::mpsc::sync_channel(0);
            {
                let mut hook = JOURNAL_PERSIST_BLOCKER
                    .lock()
                    .map_err(|_| "journal-persist test hook was poisoned")?;
                *hook = Some((journal_identity, started_tx, release_rx));
            }
            let appending = tokio::spawn(
                {
                    let provider = provider.clone();
                    async move {
                        provider
                            .append(AppendRequest {
                                path: StreamPath::new("cancelled-persist")?,
                                records: vec![OwnedBytes::from_static(b"record")],
                                if_tail: Some(0),
                                idempotency_key: None,
                            })
                            .await
                    }
                }
                .with_subscriber(dispatch),
            );

            started_rx.recv()?;
            appending.abort();
            assert!(appending.await.is_err_and(|error| error.is_cancelled()));
            drop(provider);
            assert!(
                Arc::clone(&lifecycle).try_lock_owned().is_err(),
                "queued journal persistence must retain local-root ownership after cancellation"
            );

            release_tx.send(())?;
            tokio::time::timeout(
                std::time::Duration::from_secs(1),
                Arc::clone(&lifecycle).lock_owned(),
            )
            .await?;
            let fields = captured.fields("acyclic.stream.append");
            assert_eq!(
                fields["outcome"], "ok",
                "detached work owns its successful completion"
            );
            assert!(!fields.contains_key("error.kind"));
            Ok::<_, Box<dyn std::error::Error>>(())
        })
    }

    static PERSIST_BLOCKER_TESTS: Mutex<()> = Mutex::new(());

    #[test]
    fn native_caller_abort_keeps_sdk_request_and_accepted_flush_owned()
    -> Result<(), Box<dyn std::error::Error>> {
        let _serial = PERSIST_BLOCKER_TESTS.lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let runtime = tokio::runtime::Builder::new_multi_thread()
            .worker_threads(2).enable_all().build()?;
        runtime.block_on(async {
            let directory = tempfile::tempdir()?;
            let provider = LocalStream::open(directory.path(), LocalStreamLimits::default()).await?;
            let request = keyed_commit("native-detached-input", 0)?;
            let key = request.idempotency_key.clone();
            let input_bytes = request_control_capacity(&request);
            let meter = Arc::new(LocalNativeMeter::default());
            let original = meter.original();
            let caller_controls = ReadAllocation::adopt(&original, input_bytes)?;
            let journal_identity = Arc::as_ptr(&provider.inner.journal.journal) as usize;
            let (started_tx, started_rx) = std::sync::mpsc::sync_channel(0);
            let (release_tx, release_rx) = std::sync::mpsc::sync_channel(0);
            *JOURNAL_PERSIST_BLOCKER.lock().map_err(|_| "journal-persist hook was poisoned")? =
                Some((journal_identity, started_tx, release_rx));
            let caller = tokio::spawn({
                let provider = provider.clone();
                async move {
                    let _caller_controls = caller_controls;
                    provider.native_commit(request, original).await
                }
            });
            tokio::task::spawn_blocking(move || {
                started_rx.recv_timeout(std::time::Duration::from_secs(5))
            }).await??;
            caller.abort();
            let cancelled = caller.await.is_err_and(|error| error.is_cancelled());
            let owned_during_drain = meter.memory.load(Ordering::SeqCst);
            // Always unblock accepted cleanup before asserting the control.
            release_tx.send(())?;
            assert!(cancelled);
            assert!(owned_during_drain >= input_bytes as u64);
            assert_eq!(provider.tail(StreamPath::new("native-detached-input")?).await?, 1);
            let accepted = provider.inspect_idempotency(key.clone()).await?
                .ok_or("detached native operation lost its identity")?;
            let terminal = Arc::downgrade(&provider.inner);
            drop(provider);
            tokio::time::timeout(std::time::Duration::from_secs(5), async {
                while terminal.upgrade().is_some() { tokio::task::yield_now().await; }
            }).await?;
                let reopened = LocalStream::open(directory.path(), LocalStreamLimits::default()).await?;
            assert_eq!(reopened.inspect_idempotency(key).await?, Some(accepted));
            meter.assert_refunded();
            assert_eq!(meter.memory.load(Ordering::SeqCst), 0);
            Ok::<_, Box<dyn std::error::Error>>(())
        })
    }

    #[test]
    fn a_cancelled_mutation_is_invisible_until_its_frame_is_durable()
    -> Result<(), Box<dyn std::error::Error>> {
        let _serial = PERSIST_BLOCKER_TESTS
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let runtime = tokio::runtime::Builder::new_multi_thread()
            .worker_threads(2)
            .enable_all()
            .build()?;
        runtime.block_on(async move {
            let directory = tempfile::tempdir()?;
            let provider =
                LocalStream::open(directory.path(), LocalStreamLimits::default()).await?;
            let journal_identity = Arc::as_ptr(&provider.inner.journal.journal) as usize;
            let (started_tx, started_rx) = std::sync::mpsc::sync_channel(0);
            let (release_tx, release_rx) = std::sync::mpsc::sync_channel(0);
            *JOURNAL_PERSIST_BLOCKER
                .lock()
                .map_err(|_| "journal-persist test hook was poisoned")? =
                Some((journal_identity, started_tx, release_rx));
            let path = StreamPath::new("cancelled-visibility")?;
            let appending = tokio::spawn({
                let (provider, path) = (provider.clone(), path.clone());
                async move {
                    provider
                        .append(AppendRequest {
                            path,
                            records: vec![OwnedBytes::from_static(b"record")],
                            if_tail: Some(0),
                            idempotency_key: None,
                        })
                        .await
                }
            });
            tokio::task::spawn_blocking(move || started_rx.recv()).await??;
            appending.abort();
            assert!(appending.await.is_err_and(|error| error.is_cancelled()));

            let reading = tokio::spawn({
                let (provider, path) = (provider.clone(), path.clone());
                async move { provider.tail(path).await }
            });
            tokio::time::sleep(std::time::Duration::from_millis(200)).await;
            assert!(!reading.is_finished(), "the unsynced record is not visible");
            release_tx.send(())?;
            assert_eq!(reading.await??, 1, "the persisted record is visible");
            Ok::<_, Box<dyn std::error::Error>>(())
        })
    }

    fn keyed_commit(path: &str, tail: u64) -> Result<CommitRequest, StreamError> {
        let path = StreamPath::new(path)?;
        Ok(CommitRequest {
            conditions: vec![if tail == 0 {
                crate::CommitCondition::Absent { path: path.clone() }
            } else {
                crate::CommitCondition::Tail {
                    path: path.clone(),
                    expected: tail,
                }
            }],
            mutations: vec![crate::CommitMutation::Append {
                path: path.clone(),
                records: vec![OwnedBytes::from(format!("{path}-{tail}"))],
            }],
            idempotency_key: IdempotencyKey::new(OwnedBytes::from(format!("{path}@{tail}")))?,
        })
    }

    #[test]
    fn mutations_on_other_paths_write_while_a_flush_is_under_way()
    -> Result<(), Box<dyn std::error::Error>> {
        let _serial = PERSIST_BLOCKER_TESTS
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let runtime = tokio::runtime::Builder::new_multi_thread()
            .worker_threads(2)
            .enable_all()
            .build()?;
        runtime.block_on(async move {
            let directory = tempfile::tempdir()?;
            let provider =
                LocalStream::open(directory.path(), LocalStreamLimits::default()).await?;
            let journal_identity = Arc::as_ptr(&provider.inner.journal.journal) as usize;
            let (started_tx, started_rx) = std::sync::mpsc::sync_channel(0);
            let (release_tx, release_rx) = std::sync::mpsc::sync_channel(0);
            *JOURNAL_PERSIST_BLOCKER
                .lock()
                .map_err(|_| "journal-persist test hook was poisoned")? =
                Some((journal_identity, started_tx, release_rx));
            let commit = |path: &'static str| {
                let provider = provider.clone();
                tokio::spawn(async move { provider.commit(keyed_commit(path, 0)?).await })
            };
            let first = commit("first");
            tokio::task::spawn_blocking(move || started_rx.recv()).await??;
            let second = commit("second");
            let appended = |provider: &LocalStream| {
                provider
                    .inner
                    .journal
                    .journal
                    .lock()
                    .map(|journal| journal.appended)
                    .unwrap_or_default()
            };
            tokio::time::timeout(std::time::Duration::from_secs(5), async {
                while appended(&provider) < 2 {
                    tokio::time::sleep(std::time::Duration::from_millis(5)).await;
                }
            })
            .await?;
            assert!(!first.is_finished() && !second.is_finished());
            release_tx.send(())?;
            for (outcome, path) in [(first.await??, "first"), (second.await??, "second")] {
                assert!(matches!(outcome, CommitOutcome::Committed(_)), "{path}");
                assert_eq!(provider.tail(StreamPath::new(path)?).await?, 1, "{path}");
            }
            Ok::<_, Box<dyn std::error::Error>>(())
        })
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn a_poisoned_store_fails_waits_for_frames_already_durable()
    -> Result<(), Box<dyn std::error::Error>> {
        let directory = tempfile::tempdir()?;
        let provider = LocalStream::open(directory.path(), LocalStreamLimits::default()).await?;
        provider.commit(keyed_commit("durable", 0)?).await?;
        let required = provider.inner.journal.required()?;
        assert_eq!(required, 1);
        provider.await_durable_observed(required, None, None).await
            .map_err(LocalCompletionError::ordinary)?;
        provider.inner.poison();
        assert_eq!(
            provider.await_durable_observed(required, None, None).await
                .map_err(LocalCompletionError::ordinary),
            Err(StreamError::Unavailable)
        );
        assert_eq!(
            provider.tail(StreamPath::new("durable")?).await,
            Err(StreamError::Unavailable)
        );
        Ok(())
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn concurrent_commits_on_distinct_paths_replay_after_reopen()
    -> Result<(), Box<dyn std::error::Error>> {
        const WRITERS: usize = 8;
        const COMMITS: u64 = 25;
        let directory = tempfile::tempdir()?;
        let provider = LocalStream::open(directory.path(), LocalStreamLimits::default()).await?;
        let mut writers = Vec::new();
        for writer in 0..WRITERS {
            let provider = provider.clone();
            writers.push(tokio::spawn(async move {
                let path = format!("agents/{writer}/memory");
                let mut outcomes = Vec::new();
                for tail in 0..COMMITS {
                    let outcome = provider.commit(keyed_commit(&path, tail)?).await?;
                    assert!(matches!(outcome, CommitOutcome::Committed(_)));
                    outcomes.push(outcome);
                }
                Ok::<_, StreamError>(outcomes)
            }));
        }
        // Cross-path atomic commits race the writers on paths of their own.
        let joint = {
            let provider = provider.clone();
            tokio::spawn(async move {
                let (left, right) = (
                    StreamPath::new("joint/left")?,
                    StreamPath::new("joint/right")?,
                );
                for tail in 0..COMMITS {
                    let condition = |path: &StreamPath| {
                        if tail == 0 {
                            crate::CommitCondition::Absent { path: path.clone() }
                        } else {
                            crate::CommitCondition::Tail {
                                path: path.clone(),
                                expected: tail,
                            }
                        }
                    };
                    let outcome = provider
                        .commit(CommitRequest {
                            conditions: vec![condition(&left), condition(&right)],
                            mutations: [&left, &right]
                                .into_iter()
                                .map(|path| crate::CommitMutation::Append {
                                    path: path.clone(),
                                    records: vec![OwnedBytes::from(tail.to_le_bytes().to_vec())],
                                })
                                .collect(),
                            idempotency_key: IdempotencyKey::new(OwnedBytes::from(format!(
                                "joint@{tail}"
                            )))?,
                        })
                        .await?;
                    assert!(matches!(outcome, CommitOutcome::Committed(_)));
                }
                Ok::<_, StreamError>(())
            })
        };
        let mut outcomes = Vec::new();
        for writer in writers {
            outcomes.push(writer.await??);
        }
        joint.await??;
        let frames = provider
            .inner
            .journal
            .journal
            .lock()
            .map_err(|_| "journal poisoned")?
            .appended;
        assert_eq!(frames, (WRITERS as u64 + 1) * COMMITS);
        // Nothing else flushes or closes the provider before it reopens.
        drop(provider);

        let reopened = LocalStream::open(directory.path(), LocalStreamLimits::default()).await?;
        for (writer, outcomes) in outcomes.iter().enumerate() {
            let path = format!("agents/{writer}/memory");
            assert_eq!(reopened.tail(StreamPath::new(&path)?).await?, COMMITS);
            let records = reopened
                .read(ReadRequest {
                    path: StreamPath::new(&path)?,
                    from: 0,
                    limit: 100,
                })
                .await?
                .collect::<Vec<_>>()
                .await
                .into_iter()
                .collect::<Result<Vec<_>, _>>()?;
            for (tail, record) in (0..).zip(&records) {
                assert_eq!(record.value, OwnedBytes::from(format!("{path}-{tail}")));
            }
            for (tail, outcome) in (0..).zip(outcomes) {
                assert_eq!(&reopened.commit(keyed_commit(&path, tail)?).await?, outcome);
            }
        }
        for path in ["joint/left", "joint/right"] {
            assert_eq!(reopened.tail(StreamPath::new(path)?).await?, COMMITS);
        }
        Ok(())
    }

    use crate::conformance;
    use std::sync::atomic::{AtomicU64, Ordering};

    #[derive(Default)]
    struct TestClock(AtomicU64);

    impl UnixMillisClock for TestClock {
        fn now_unix_millis(&self) -> u64 {
            self.0.load(Ordering::SeqCst)
        }
    }

    #[tokio::test]
    async fn every_partial_current_frame_recovers_only_the_accepted_prefix()
    -> Result<(), Box<dyn std::error::Error>> {
        let directory = tempfile::tempdir()?;
        let limits = LocalStreamLimits::default();
        let path = directory.path().join(JOURNAL_FILE);
        let command = Command::Append(AppendRequest {
            path: StreamPath::new("partial")?,
            records: vec![OwnedBytes::from_static(b"record")],
            if_tail: Some(0),
            idempotency_key: Some(IdempotencyKey::new(OwnedBytes::from_static(b"partial"))?),
        });
        let frame = PreparedFrame::encode(&command, 42, None)?;
        for cut in 0..=frame.encoded.len() {
            let mut bytes = encode_header(limits, 0)?.to_vec();
            bytes.extend_from_slice(&frame.encoded[..cut]);
            std::fs::write(&path, &bytes)?;
            let reopened = LocalStream::open(directory.path(), limits).await?;
            if cut == frame.encoded.len() {
                assert_eq!(reopened.tail(StreamPath::new("partial")?).await?, 1);
            } else {
                assert_eq!(
                    reopened.tail(StreamPath::new("partial")?).await,
                    Err(StreamError::NotFound)
                );
            }
            drop(reopened);
            assert_eq!(
                std::fs::metadata(&path)?.len(),
                u64::try_from(if cut == frame.encoded.len() {
                    bytes.len()
                } else {
                    HEADER_BYTES
                })?
            );
        }
        // A whole checksummed noncanonical command is never a torn append.
        let mut payload = 42_u64.to_le_bytes().to_vec();
        payload.extend_from_slice(&journal_command(&command).encode_to_vec()?);
        payload.extend_from_slice(&[0x78, 0]);
        let length = u32::try_from(payload.len())?.to_le_bytes();
        let mut bytes = encode_header(limits, 0)?.to_vec();
        bytes.extend_from_slice(&length);
        bytes.extend_from_slice(&payload);
        bytes.extend_from_slice(&frame_checksum(&length, &payload, &mut FrameWork { meter: None, accepted: false, late: Ok(()) })?);
        std::fs::write(&path, &bytes)?;
        assert!(matches!(
            LocalStream::open(directory.path(), limits).await,
            Err(LocalStreamError::Corrupt)
        ));
        assert_eq!(std::fs::read(&path)?, bytes);
        Ok(())
    }

    #[tokio::test]
    async fn invalid_headers_and_snapshot_state_fail_without_repairing_disk()
    -> Result<(), Box<dyn std::error::Error>> {
        let directory = tempfile::tempdir()?;
        let limits = LocalStreamLimits::default();
        let journal_path = directory.path().join(JOURNAL_FILE);
        let snapshot_path = directory.path().join(SNAPSHOT_FILE);
        let mut obsolete = encode_header(limits, 0)?;
        obsolete[22] = b'4';
        for header in [obsolete, [0; HEADER_BYTES], encode_header(limits, 2)?] {
            std::fs::write(&journal_path, &header)?;
            for _ in 0..2 {
                assert!(matches!(
                    LocalStream::open(directory.path(), limits).await,
                    Err(LocalStreamError::Corrupt)
                ));
                assert_eq!(std::fs::read(&journal_path)?, header);
            }
        }
        // Keep the checksum valid so rejection must enforce the snapshot version.
        let header = encode_header(limits, 0)?;
        std::fs::write(&journal_path, &header)?;
        let state = MemoryStream::new(limits.memory).encode_state(None).await.expect("unmetered snapshot encoding").value;
        for version in *b"024" {
            let mut obsolete = encode_snapshot(limits, 1, &state, None)?.value;
            obsolete[SNAPSHOT_MAGIC.len() - 2] = version;
            let checksum_at = obsolete.len() - 32;
            let checksum = snapshot_checksum(&obsolete[..checksum_at], None)?;
            obsolete[checksum_at..].copy_from_slice(&checksum);
            std::fs::write(&snapshot_path, &obsolete)?;
            for _ in 0..2 {
                assert!(matches!(
                    LocalStream::open(directory.path(), limits).await,
                    Err(LocalStreamError::Corrupt)
                ));
                assert_eq!(std::fs::read(&journal_path)?, header);
                assert_eq!(std::fs::read(&snapshot_path)?, obsolete);
            }
        }
        let header = encode_header(limits, 0)?;
        std::fs::write(&journal_path, &header)?;
        let invalid = encode_snapshot(limits, 1, b"invalid state", None)?.value;
        std::fs::write(&snapshot_path, &invalid)?;
        assert!(matches!(
            LocalStream::open(directory.path(), limits).await,
            Err(LocalStreamError::Corrupt)
        ));
        assert_eq!(std::fs::read(&journal_path)?, header);
        assert_eq!(std::fs::read(&snapshot_path)?, invalid);
        Ok(())
    }

    #[tokio::test]
    async fn recovery_rejects_receipt_overcapacity_without_rewriting_the_prefix()
    -> Result<(), Box<dyn std::error::Error>> {
        let directory = tempfile::tempdir()?;
        let limits = LocalStreamLimits {
            memory: MemoryLimits {
                commits: 1,
                idempotency_results: 1,
                ..MemoryLimits::default()
            },
            ..LocalStreamLimits::default()
        };
        let provider = LocalStream::open(directory.path(), limits).await?;
        let first = AppendRequest {
            path: StreamPath::new("capacity/replay")?,
            records: vec![OwnedBytes::from_static(b"first")],
            if_tail: Some(0),
            idempotency_key: Some(IdempotencyKey::new(OwnedBytes::from_static(b"first"))?),
        };
        let outcome = provider.append(first.clone()).await?;
        let state = provider.inner.provider.encode_state(None).await.expect("unmetered snapshot encoding").value;
        let (physical, observed, _) = provider
            .inner
            .journal
            .journal
            .lock()
            .map_err(|_| "poisoned")?
            .compact_observed(&state, None);
        physical?;
        observed?;
        drop(provider);
        let second = AppendRequest {
            idempotency_key: Some(IdempotencyKey::new(OwnedBytes::from_static(b"second"))?),
            if_tail: Some(1),
            ..first.clone()
        };
        let path = directory.path().join(JOURNAL_FILE);
        let before = std::fs::read(&path)?;
        let frame = PreparedFrame::encode(&Command::Append(second), 1, None)?;
        let mut damaged = before.clone();
        damaged.extend_from_slice(&frame.encoded);
        damaged.extend_from_slice(&[9, 8, 7]);
        std::fs::write(&path, &damaged)?;
        for _ in 0..2 {
            assert!(matches!(
                LocalStream::open(directory.path(), limits).await,
                Err(LocalStreamError::Replay(StreamError::Capacity))
            ));
            assert_eq!(std::fs::read(&path)?, damaged);
        }
        std::fs::write(&path, before)?;
        let reopened = LocalStream::open(directory.path(), limits).await?;
        assert_eq!(reopened.append(first).await?, outcome);
        Ok(())
    }

    #[tokio::test]
    async fn retry_outcomes_survive_clock_exhaustion_and_reopen()
    -> Result<(), Box<dyn std::error::Error>> {
        let directory = tempfile::tempdir()?;
        let clock = Arc::new(TestClock::default());
        let provider = LocalStream::open_with_clock(
            directory.path(),
            LocalStreamLimits::default(),
            clock.clone(),
        )
        .await?;
        let request = AppendRequest {
            path: StreamPath::new("retained/local")?,
            records: vec![OwnedBytes::from_static(b"once")],
            if_tail: None,
            idempotency_key: Some(IdempotencyKey::new(OwnedBytes::from_static(b"local-once"))?),
        };
        let first = provider.append(request.clone()).await?;
        clock.0.store(u64::MAX, Ordering::SeqCst);
        assert_eq!(provider.append(request.clone()).await?, first);
        drop(provider);
        let reopened = LocalStream::open(directory.path(), LocalStreamLimits::default()).await?;
        assert_eq!(reopened.append(request.clone()).await?, first);
        assert_eq!(reopened.tail(request.path.clone()).await?, 1);
        let mut changed = request;
        changed.path = StreamPath::new("retained/another")?;
        assert_eq!(
            reopened.append(changed).await,
            Err(StreamError::IdempotencyMismatch)
        );
        Ok(())
    }

    #[tokio::test]
    async fn journal_replay_preserves_monotonic_commit_times()
    -> Result<(), Box<dyn std::error::Error>> {
        let directory = tempfile::tempdir()?;
        let clock = Arc::new(TestClock::default());
        let path = StreamPath::new("accounting/local")?;
        let provider = LocalStream::open_with_clock(
            directory.path(),
            LocalStreamLimits::default(),
            clock.clone(),
        )
        .await?;
        for (time, body) in [(100, b"first".as_slice()), (90, b"rollback")] {
            clock.0.store(time, Ordering::SeqCst);
            provider
                .append(AppendRequest {
                    path: path.clone(),
                    records: vec![OwnedBytes::copy_from_slice(body)],
                    if_tail: None,
                    idempotency_key: None,
                })
                .await?;
        }
        drop(provider);
        clock.0.store(80, Ordering::SeqCst);
        let reopened =
            LocalStream::open_with_clock(directory.path(), LocalStreamLimits::default(), clock)
                .await?;
        reopened
            .append(AppendRequest {
                path: path.clone(),
                records: vec![OwnedBytes::from_static(b"after-restart")],
                if_tail: None,
                idempotency_key: None,
            })
            .await?;
        let records = reopened
            .read(ReadRequest {
                path,
                from: 0,
                limit: 3,
            })
            .await?
            .collect::<Vec<_>>()
            .await
            .into_iter()
            .collect::<Result<Vec<_>, _>>()?;
        assert_eq!(
            records
                .iter()
                .map(|record| record.committed_at_micros)
                .collect::<Vec<_>>(),
            vec![100_000, 100_000, 100_000]
        );
        Ok(())
    }

    #[cfg(not(target_vendor = "apple"))]
    #[tokio::test]
    async fn barrier_policy_rejects_targets_without_exact_barrier_semantics()
    -> Result<(), Box<dyn std::error::Error>> {
        let directory = tempfile::tempdir()?;
        let limits = LocalStreamLimits {
            durability: LocalDurability::Barrier,
            ..LocalStreamLimits::default()
        };
        assert!(matches!(
            LocalStream::open(directory.path(), limits).await,
            Err(LocalStreamError::Io(error))
                if error.kind() == std::io::ErrorKind::Unsupported
        ));
        Ok(())
    }

    #[tokio::test]
    async fn a_journal_torn_before_its_header_completed_is_created_again()
    -> Result<(), Box<dyn std::error::Error>> {
        let directory = tempfile::tempdir()?;
        let path = directory.path().join(JOURNAL_FILE);
        let limits = LocalStreamLimits::default();
        let header = encode_header(limits, 0)?;
        for cut in 0..header.len() {
            std::fs::write(&path, &header[..cut])?;
            let provider = LocalStream::open(directory.path(), limits).await?;
            drop(provider);
            assert_eq!(std::fs::read(&path)?, header);
        }
        let provider = LocalStream::open(directory.path(), limits).await?;
        conformance::verify(&provider)
            .await
            .map_err(std::io::Error::other)?;
        drop(provider);
        LocalStream::open(directory.path(), LocalStreamLimits::default()).await?;
        Ok(())
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn deferred_mutations_wait_for_the_next_flush() -> Result<(), Box<dyn std::error::Error>>
    {
        let directory = tempfile::tempdir()?;
        let provider = LocalStream::open(directory.path(), LocalStreamLimits::default()).await?;
        let unflushed = |provider: &LocalStream| -> Result<bool, Box<dyn std::error::Error>> {
            Ok(provider
                .inner
                .journal
                .journal
                .lock()
                .map_err(|_| "journal poisoned")?
                .unflushed())
        };
        let append = |provider: LocalStream, key: &'static [u8]| async move {
            provider
                .append(AppendRequest {
                    path: StreamPath::new("deferred")?,
                    records: vec![OwnedBytes::from_static(b"record")],
                    if_tail: None,
                    idempotency_key: Some(IdempotencyKey::new(OwnedBytes::from_static(key))?),
                })
                .await?;
            Ok::<_, Box<dyn std::error::Error>>(())
        };

        deferring_durability(append(provider.clone(), b"first")).await?;
        assert_eq!(provider.tail(StreamPath::new("deferred")?).await?, 1);
        assert!(unflushed(&provider)?);
        provider.flush()?;
        assert!(!unflushed(&provider)?);

        deferring_durability(append(provider.clone(), b"second")).await?;
        assert!(unflushed(&provider)?);
        append(provider.clone(), b"third").await?;
        assert!(
            !unflushed(&provider)?,
            "a flushed mutation flushes those before it"
        );
        assert_eq!(provider.tail(StreamPath::new("deferred")?).await?, 3);
        Ok(())
    }

    #[tokio::test]
    async fn filtered_local_operations_preserve_caller() -> Result<(), Box<dyn std::error::Error>> {
        use tracing_subscriber::layer::SubscriberExt as _;
        let captured = obs::tests::Capture::default();
        let subscriber = tracing_subscriber::Registry::default()
            .with(captured.clone())
            .with(tracing_subscriber::filter::filter_fn(|metadata| {
                !metadata.name().starts_with("acyclic.stream")
            }));
        let _default = tracing::subscriber::set_default(subscriber);
        let parent = tracing::info_span!(
            "caller",
            outcome = "caller",
            error.kind = "caller",
            frames = 77,
            bytes = 88,
            batch_len = 99,
            rev = 66
        );
        async {
            let directory = tempfile::tempdir()?;
            let provider =
                LocalStream::open(directory.path(), LocalStreamLimits::default()).await?;
            let request = AppendRequest {
                path: StreamPath::new("traced")?,
                records: vec![OwnedBytes::from_static(b"record")],
                if_tail: None,
                idempotency_key: None,
            };
            provider.append(request).await?;
            provider.flush()?;
            assert!(
                provider
                    .read(ReadRequest {
                        path: StreamPath::new("missing")?,
                        from: 0,
                        limit: 1
                    })
                    .await
                    .is_err()
            );
            drop(provider);
            let reopened =
                LocalStream::open(directory.path(), LocalStreamLimits::default()).await?;
            assert_eq!(reopened.tail(StreamPath::new("traced")?).await?, 1);
            Ok::<_, Box<dyn std::error::Error>>(())
        }
        .instrument(parent)
        .await?;
        let fields = captured.fields("caller");
        for (field, value) in [
            ("outcome", "caller"),
            ("error.kind", "caller"),
            ("frames", "77"),
            ("bytes", "88"),
            ("batch_len", "99"),
            ("rev", "66"),
        ] {
            assert_eq!(fields[field], value);
        }
        Ok(())
    }

    #[tokio::test]
    async fn local_follow_observes_lifetime_drop_and_late_error()
    -> Result<(), Box<dyn std::error::Error>> {
        use tracing_subscriber::layer::SubscriberExt as _;
        for late_error in [false, true] {
            let directory = tempfile::tempdir()?;
            let provider =
                LocalStream::open(directory.path(), LocalStreamLimits::default()).await?;
            let path = StreamPath::new("follow")?;
            provider
                .append(AppendRequest {
                    path: path.clone(),
                    records: vec![OwnedBytes::from_static(b"record")],
                    if_tail: None,
                    idempotency_key: None,
                })
                .await?;
            let captured = obs::tests::Capture::default();
            let mut records = tracing::subscriber::with_default(
                tracing_subscriber::Registry::default().with(captured.clone()),
                || {
                    let parent = obs::OwnedSpan::new(tracing::info_span!("origin_parent"));
                    parent.scope(async { provider.follow(path, 1).await })
                },
            )
            .await?;
            assert!(
                !captured
                    .fields("acyclic.stream.follow")
                    .contains_key("outcome")
            );
            assert_eq!(captured.closed("acyclic.stream.follow"), 0);
            let other = obs::tests::Capture::default();
            let _default = tracing::subscriber::set_default(
                tracing_subscriber::Registry::default().with(other.clone()),
            );
            let parent = tracing::info_span!("foreign_parent");
            let _entered = parent.enter();
            if late_error {
                provider.inner.poison();
                assert_eq!(
                    records.next().await.unwrap().unwrap_err(),
                    StreamError::Unavailable
                );
                assert!(records.next().await.is_none());
            } else {
                assert!(futures::poll!(records.next()).is_pending());
            }
            drop(records);
            let fields = captured.fields("acyclic.stream.follow");
            assert_eq!(
                fields["terminal"],
                if late_error { "error" } else { "dropped" }
            );
            if late_error {
                assert_eq!(fields["outcome"], "err");
                assert_eq!(fields["error.kind"], "unavailable");
            } else {
                assert!(!fields.contains_key("outcome"));
                assert!(!fields.contains_key("error.kind"));
            }
            assert_eq!(captured.closed("acyclic.stream.follow"), 1);
            assert_eq!(captured.closed("origin_parent"), 1);
            assert_eq!(other.closed("foreign_parent"), 0);
        }
        Ok(())
    }

    #[tokio::test]
    async fn cancelled_local_read_records_error_and_closes_origin_parent()
    -> Result<(), Box<dyn std::error::Error>> {
        use tracing_subscriber::layer::SubscriberExt as _;
        let directory = tempfile::tempdir()?;
        let provider = LocalStream::open(directory.path(), LocalStreamLimits::default()).await?;
        let visibility = provider.inner.visibility.write().await;
        let captured = obs::tests::Capture::default();
        let future = tracing::subscriber::with_default(
            tracing_subscriber::Registry::default().with(captured.clone()),
            || {
                let parent = obs::OwnedSpan::new(tracing::info_span!("origin_parent"));
                parent.scope(async {
                    provider
                        .read(ReadRequest {
                            path: StreamPath::new("pending")?,
                            from: 0,
                            limit: 1,
                        })
                        .await
                })
            },
        );
        let mut future = Box::pin(future);
        assert!(futures::poll!(future.as_mut()).is_pending());
        let other = obs::tests::Capture::default();
        let _default = tracing::subscriber::set_default(
            tracing_subscriber::Registry::default().with(other.clone()),
        );
        let parent = tracing::info_span!("foreign_parent");
        let _entered = parent.enter();
        drop(future);
        assert_eq!(
            captured.fields("acyclic.stream.read")["error.kind"],
            "cancelled"
        );
        assert_eq!(captured.closed("acyclic.stream.read"), 1);
        assert_eq!(captured.closed("origin_parent"), 1);
        assert_eq!(other.closed("foreign_parent"), 0);
        drop(visibility);
        Ok(())
    }


    #[tokio::test]
    async fn local_provider_reopens_and_passes_public_conformance()
    -> Result<(), Box<dyn std::error::Error>> {
        let directory = tempfile::tempdir()?;
        let provider = LocalStream::open(directory.path(), LocalStreamLimits::default()).await?;
        conformance::verify(&provider)
            .await
            .map_err(std::io::Error::other)?;
        drop(provider);
        let reopened = LocalStream::open(directory.path(), LocalStreamLimits::default()).await?;
        assert_eq!(
            reopened
                .tail(StreamPath::new("conformance/source")?)
                .await?,
            2
        );
        Ok(())
    }

    #[tokio::test]
    async fn deadline_is_evaluated_once_and_only_accepted_commands_are_replayed()
    -> Result<(), Box<dyn std::error::Error>> {
        let directory = tempfile::tempdir()?;
        let clock = Arc::new(TestClock::default());
        clock.0.store(10, Ordering::SeqCst);
        let provider = LocalStream::open_with_clock(
            directory.path(),
            LocalStreamLimits::default(),
            clock.clone(),
        )
        .await?;
        let accepted = CommitRequest {
            conditions: vec![crate::CommitCondition::Absent {
                path: StreamPath::new("deadline/accepted")?,
            }],
            mutations: vec![crate::CommitMutation::Append {
                path: StreamPath::new("deadline/accepted")?,
                records: vec![OwnedBytes::from_static(b"accepted")],
            }],
            idempotency_key: IdempotencyKey::new(OwnedBytes::from_static(b"accepted"))?,
        };
        assert!(matches!(
            provider.commit_before(accepted, 11).await?,
            CommitOutcome::Committed(_)
        ));
        let expired = CommitRequest {
            conditions: vec![crate::CommitCondition::Absent {
                path: StreamPath::new("deadline/expired")?,
            }],
            mutations: vec![crate::CommitMutation::Append {
                path: StreamPath::new("deadline/expired")?,
                records: vec![OwnedBytes::from_static(b"expired")],
            }],
            idempotency_key: IdempotencyKey::new(OwnedBytes::from_static(b"expired"))?,
        };
        assert_eq!(
            provider.commit_before(expired, 10).await,
            Err(StreamError::DeadlineElapsed)
        );
        drop(provider);

        clock.0.store(100, Ordering::SeqCst);
        let reopened =
            LocalStream::open_with_clock(directory.path(), LocalStreamLimits::default(), clock)
                .await?;
        assert_eq!(
            reopened.tail(StreamPath::new("deadline/accepted")?).await?,
            1
        );
        assert_eq!(
            reopened.tail(StreamPath::new("deadline/expired")?).await,
            Err(StreamError::NotFound)
        );
        Ok(())
    }

    #[tokio::test]
    async fn local_provider_excludes_a_second_process_owner_and_repairs_a_torn_tail()
    -> Result<(), Box<dyn std::error::Error>> {
        let directory = tempfile::tempdir()?;
        let provider = LocalStream::open(directory.path(), LocalStreamLimits::default()).await?;
        assert!(matches!(
            LocalStream::open(directory.path(), LocalStreamLimits::default()).await,
            Err(LocalStreamError::AlreadyOpen)
        ));
        provider
            .append(AppendRequest {
                path: StreamPath::new("durable")?,
                records: vec![OwnedBytes::from_static(b"one")],
                if_tail: Some(0),
                idempotency_key: Some(IdempotencyKey::new(OwnedBytes::from_static(b"append"))?),
            })
            .await?;
        drop(provider);
        let journal = directory.path().join("stream.journal");
        let valid_length = std::fs::metadata(&journal)?.len();
        OpenOptions::new()
            .append(true)
            .open(&journal)?
            .write_all(&[9, 8, 7])?;
        let reopened = LocalStream::open(directory.path(), LocalStreamLimits::default()).await?;
        assert_eq!(reopened.tail(StreamPath::new("durable")?).await?, 1);
        drop(reopened);
        assert_eq!(std::fs::metadata(journal)?.len(), valid_length);
        Ok(())
    }

    #[tokio::test]
    async fn reopen_streams_more_commands_than_the_pipeline_window()
    -> Result<(), Box<dyn std::error::Error>> {
        let directory = tempfile::tempdir()?;
        let provider = LocalStream::open(directory.path(), LocalStreamLimits::default()).await?;
        let path = StreamPath::new("bounded-replay")?;
        let count = u64::try_from(REPLAY_PIPELINE_COMMANDS * 4)?;
        for index in 0..count {
            provider
                .append(AppendRequest {
                    path: path.clone(),
                    records: vec![OwnedBytes::copy_from_slice(&index.to_le_bytes())],
                    if_tail: Some(index),
                    idempotency_key: None,
                })
                .await?;
        }
        drop(provider);

        let reopened = LocalStream::open(directory.path(), LocalStreamLimits::default()).await?;
        assert_eq!(reopened.tail(path).await?, count);
        Ok(())
    }

    #[tokio::test]
    async fn replay_failure_drains_the_bounded_pipeline_and_releases_the_root()
    -> Result<(), Box<dyn std::error::Error>> {
        let directory = tempfile::tempdir()?;
        let limits = LocalStreamLimits::default();
        let provider = LocalStream::open(directory.path(), limits).await?;
        drop(provider);

        let path = StreamPath::new("failed-replay")?;
        let invalid = PreparedFrame::encode(
            &Command::Fork(ForkRequest {
                source: StreamPath::new("missing-source")?,
                destination: path.clone(),
                at_tail: None,
                idempotency_key: None,
            }),
            0,
            None,
        )?;
        let valid = PreparedFrame::encode(
            &Command::Append(AppendRequest {
                path,
                records: vec![OwnedBytes::from_static(b"later")],
                if_tail: Some(0),
                idempotency_key: None,
            }),
            0,
            None,
        )?;
        let journal_path = directory.path().join("stream.journal");
        let mut journal = OpenOptions::new().append(true).open(&journal_path)?;
        journal.write_all(&invalid.encoded)?;
        for _ in 0..REPLAY_PIPELINE_COMMANDS * 4 {
            journal.write_all(&valid.encoded)?;
        }
        journal.write_all(&[9, 8, 7])?;
        journal.sync_all()?;
        drop(journal);
        let before = std::fs::read(&journal_path)?;

        for _ in 0..2 {
            let result = tokio::time::timeout(
                std::time::Duration::from_secs(5),
                LocalStream::open(directory.path(), limits),
            )
            .await?;
            assert!(matches!(
                result,
                Err(LocalStreamError::Replay(StreamError::NotFound))
            ));
            assert_eq!(std::fs::read(&journal_path)?, before);
        }
        Ok(())
    }

    #[tokio::test]
    async fn journal_capacity_rejects_before_mutating_visible_state()
    -> Result<(), Box<dyn std::error::Error>> {
        let directory = tempfile::tempdir()?;
        let limits = LocalStreamLimits {
            journal_bytes: u64::try_from(HEADER_BYTES).map_err(std::io::Error::other)?,
            ..LocalStreamLimits::default()
        };
        let provider = LocalStream::open(directory.path(), limits).await?;
        assert!(matches!(
            provider
                .append(AppendRequest {
                    path: StreamPath::new("capacity")?,
                    records: vec![OwnedBytes::from_static(b"must-not-appear")],
                    if_tail: Some(0),
                    idempotency_key: None,
                })
                .await,
            Err(StreamError::Capacity)
        ));
        assert!(matches!(
            provider.tail(StreamPath::new("capacity")?).await,
            Err(StreamError::NotFound)
        ));
        Ok(())
    }

    async fn append_one(
        provider: &LocalStream,
        path: &str,
        journal: &Path,
    ) -> Result<u64, Box<dyn std::error::Error>> {
        provider
            .append(AppendRequest {
                path: StreamPath::new(path)?,
                records: vec![OwnedBytes::from_static(b"body")],
                if_tail: Some(0),
                idempotency_key: None,
            })
            .await?;
        Ok(std::fs::metadata(journal)?.len())
    }

    async fn tail_of(
        directory: &Path,
        path: &str,
    ) -> Result<Option<u64>, Box<dyn std::error::Error>> {
        let provider = LocalStream::open(directory, LocalStreamLimits::default()).await?;
        Ok(provider.tail(StreamPath::new(path)?).await.ok())
    }

    #[tokio::test]
    async fn a_damaged_frame_before_an_intact_one_fails_closed()
    -> Result<(), Box<dyn std::error::Error>> {
        let directory = tempfile::tempdir()?;
        let journal = directory.path().join("stream.journal");
        let provider = LocalStream::open(directory.path(), LocalStreamLimits::default()).await?;
        append_one(&provider, "first", &journal).await?;
        append_one(&provider, "second", &journal).await?;
        drop(provider);

        let mut file = OpenOptions::new().read(true).write(true).open(&journal)?;
        let body_offset = u64::try_from(HEADER_BYTES + 5).map_err(std::io::Error::other)?;
        file.seek(SeekFrom::Start(body_offset))?;
        file.write_all(&[0xff])?;
        file.sync_all()?;
        drop(file);
        let damaged = std::fs::read(&journal)?;

        assert!(matches!(
            LocalStream::open(directory.path(), LocalStreamLimits::default()).await,
            Err(LocalStreamError::Corrupt)
        ));
        assert_eq!(
            std::fs::read(&journal)?,
            damaged,
            "recovery left it untouched"
        );
        Ok(())
    }

    #[tokio::test]
    async fn a_zero_filled_or_garbage_last_frame_is_a_torn_tail()
    -> Result<(), Box<dyn std::error::Error>> {
        for fill in [0x00_u8, 0x5a] {
            let directory = tempfile::tempdir()?;
            let journal = directory.path().join("stream.journal");
            let provider =
                LocalStream::open(directory.path(), LocalStreamLimits::default()).await?;
            let kept = append_one(&provider, "kept", &journal).await?;
            let torn = append_one(&provider, "torn", &journal).await?;
            drop(provider);

            let mut file = OpenOptions::new().read(true).write(true).open(&journal)?;
            file.seek(SeekFrom::Start(kept))?;
            file.write_all(&vec![fill; usize::try_from(torn - kept)?])?;
            file.sync_all()?;
            drop(file);

            assert_eq!(
                tail_of(directory.path(), "kept").await?,
                Some(1),
                "{fill:#x}"
            );
            assert_eq!(
                tail_of(directory.path(), "torn").await?.unwrap_or(0),
                0,
                "{fill:#x}"
            );
            assert_eq!(std::fs::metadata(&journal)?.len(), kept, "{fill:#x}");
        }
        Ok(())
    }

    fn compacting_limits() -> LocalStreamLimits {
        LocalStreamLimits {
            journal_operations: 8,
            ..LocalStreamLimits::default()
        }
    }

    async fn append_keyed(
        provider: &LocalStream,
        index: u64,
    ) -> Result<AppendOutcome, Box<dyn std::error::Error>> {
        Ok(provider
            .append(AppendRequest {
                path: StreamPath::new("compacted")?,
                records: vec![OwnedBytes::copy_from_slice(&index.to_le_bytes())],
                if_tail: Some(index),
                idempotency_key: Some(IdempotencyKey::new(OwnedBytes::copy_from_slice(&index.to_le_bytes(),))?),
            })
            .await?)
    }

    #[tokio::test]
    async fn a_half_full_journal_is_compacted_into_a_snapshot()
    -> Result<(), Box<dyn std::error::Error>> {
        let directory = tempfile::tempdir()?;
        let limits = compacting_limits();
        let provider = LocalStream::open(directory.path(), limits).await?;
        let mut outcomes = Vec::new();
        for index in 0..21 {
            outcomes.push(append_keyed(&provider, index).await?);
        }
        let journal = provider
            .inner
            .journal
            .journal
            .lock()
            .map_err(|_| "journal poisoned")?
            .operations;
        assert!(journal < 4, "the journal restarted: {journal} frames");
        drop(provider);
        assert!(directory.path().join(SNAPSHOT_FILE).exists());

        let reopened = LocalStream::open(directory.path(), limits).await?;
        assert_eq!(reopened.tail(StreamPath::new("compacted")?).await?, 21);
        let history = reopened
            .read(ReadRequest {
                path: StreamPath::new("compacted")?,
                from: 0,
                limit: 21,
            })
            .await?
            .collect::<Vec<_>>()
            .await
            .into_iter()
            .collect::<Result<Vec<_>, _>>()?;
        assert_eq!(history.len(), 21);
        for (index, record) in (0_u64..21).zip(&history) {
            assert_eq!(record.sequence, index);
            assert_eq!(record.value.as_ref(), index.to_le_bytes());
        }
        reopened
            .fork(ForkRequest {
                source: StreamPath::new("compacted")?,
                destination: StreamPath::new("compacted-fork")?,
                at_tail: Some(21),
                idempotency_key: None,
            })
            .await?;
        let fork_history = reopened
            .read(ReadRequest {
                path: StreamPath::new("compacted-fork")?,
                from: 0,
                limit: 21,
            })
            .await?
            .collect::<Vec<_>>()
            .await
            .into_iter()
            .collect::<Result<Vec<_>, _>>()?;
        assert_eq!(fork_history, history);
        for (index, outcome) in (0..).zip(&outcomes) {
            assert_eq!(&append_keyed(&reopened, index).await?, outcome, "{index}");
        }
        conformance::verify(&reopened)
            .await
            .map_err(std::io::Error::other)?;
        Ok(())
    }

    #[tokio::test]
    async fn a_journal_the_snapshot_already_holds_is_discarded()
    -> Result<(), Box<dyn std::error::Error>> {
        let directory = tempfile::tempdir()?;
        let limits = compacting_limits();
        let journal = directory.path().join(JOURNAL_FILE);
        let provider = LocalStream::open(directory.path(), limits).await?;
        for index in 0..3 {
            append_keyed(&provider, index).await?;
        }
        let before = std::fs::read(&journal)?;
        append_keyed(&provider, 3).await?;
        drop(provider);
        assert!(std::fs::metadata(&journal)?.len() < u64::try_from(before.len())?);
        std::fs::write(&journal, &before)?;
        std::fs::write(directory.path().join(SNAPSHOT_TEMPORARY), b"torn")?;

        let reopened = LocalStream::open(directory.path(), limits).await?;
        assert_eq!(reopened.tail(StreamPath::new("compacted")?).await?, 4);
        drop(reopened);
        assert_eq!(
            std::fs::read(directory.path().join(SNAPSHOT_TEMPORARY))?,
            b"torn"
        );
        assert_eq!(
            std::fs::metadata(&journal)?.len(),
            u64::try_from(HEADER_BYTES)?
        );
        Ok(())
    }

    #[tokio::test]
    async fn a_damaged_snapshot_fails_closed() -> Result<(), Box<dyn std::error::Error>> {
        let directory = tempfile::tempdir()?;
        let limits = compacting_limits();
        let provider = LocalStream::open(directory.path(), limits).await?;
        for index in 0..4 {
            append_keyed(&provider, index).await?;
        }
        drop(provider);
        let snapshot = directory.path().join(SNAPSHOT_FILE);
        let mut damaged = std::fs::read(&snapshot)?;
        let middle = damaged.len() / 2;
        *damaged.get_mut(middle).ok_or("snapshot is empty")? ^= 1;
        std::fs::write(&snapshot, damaged)?;
        assert!(matches!(
            LocalStream::open(directory.path(), limits).await,
            Err(LocalStreamError::Corrupt)
        ));
        Ok(())
    }
    #[tokio::test]
    async fn native_guard_denial_reads_same_provider_without_lock_and_preserves_typed_owner()
    -> Result<(), Box<dyn std::error::Error>> {
        let directory = tempfile::tempdir()?;
        let provider = LocalStream::open(directory.path(), LocalStreamLimits::default()).await?;
        provider.commit(keyed_commit("guard-readable", 0)?).await?;
        let before = std::fs::read(directory.path().join(JOURNAL_FILE))?;
        let request = keyed_commit("guard-denied", 0)?;
        let key = request.idempotency_key.clone();
        let meter = Arc::new(LocalNativeMeter::default());
        let callback_provider = provider.clone();
        let callback_meter = Arc::clone(&meter);
        let callback_path = StreamPath::new("guard-readable")?;
        let result = tokio::time::timeout(std::time::Duration::from_secs(5),
            provider.native_commit_guarded(request, None, meter.original(), move || {
                let provider = callback_provider.clone();
                let meter = Arc::clone(&callback_meter);
                let path = callback_path.clone();
                async move {
                    let tail = provider.native_tail(path, meter.original()).await
                        .map_err(NativeGuardFailure::Io)??;
                    assert_eq!(*tail.value(), 1);
                    drop(tail);
                    meter.cancelled.store(true, Ordering::SeqCst);
                    Err(NativeGuardFailure::Owner(0x51))
                }
            }),
        ).await??;
        assert!(matches!(result, Err(NativeGuardFailure::Owner(0x51))));
        assert!(provider.inspect_idempotency(key).await?.is_none());
        assert_eq!(std::fs::read(directory.path().join(JOURNAL_FILE))?, before);
        drop(provider);
        assert_eq!(meter.memory.load(Ordering::SeqCst), 0);
        meter.assert_refunded();
        Ok(())
    }

    #[tokio::test]
    async fn native_guard_rechecks_actual_canonical_drift_before_retaining_owner_identity()
    -> Result<(), Box<dyn std::error::Error>> {
        let directory = tempfile::tempdir()?;
        let limits = LocalStreamLimits::default();
        let provider = LocalStream::open(directory.path(), limits).await?;
        let request = keyed_commit("guard-drift-target", 0)?;
        let key = request.idempotency_key.clone();
        let meter = Arc::new(LocalNativeMeter::default());
        let calls = Arc::new(AtomicU64::new(0));
        let callback_provider = provider.clone();
        let callback_meter = Arc::clone(&meter);
        let callback_calls = Arc::clone(&calls);
        let result = provider.native_commit_guarded(request, None, meter.original(), move || {
            let provider = callback_provider.clone();
            let meter = Arc::clone(&callback_meter);
            let calls = Arc::clone(&callback_calls);
            async move {
                if calls.fetch_add(1, Ordering::SeqCst) == 0 {
                    let intervening = keyed_commit("guard-drift-other", 0)
                        .map_err(|error| NativeGuardFailure::Provider(NativeStreamFailure::new(
                            error, ReadAllocation::empty(&meter.original()),
                        )))?;
                    let result = provider.native_commit(intervening, meter.original()).await
                        .map_err(NativeGuardFailure::Io)??;
                    assert!(matches!(result.value(), CommitOutcome::Committed(_)));
                    drop(result);
                    Ok(())
                } else {
                    Err(NativeGuardFailure::Owner(0x52))
                }
            }
        }).await?;
        assert!(matches!(result, Err(NativeGuardFailure::Owner(0x52))));
        assert_eq!(calls.load(Ordering::SeqCst), 2);
        assert!(provider.inspect_idempotency(key.clone()).await?.is_none());
        assert_eq!(provider.tail(StreamPath::new("guard-drift-target")?).await, Err(StreamError::NotFound));
        assert_eq!(provider.tail(StreamPath::new("guard-drift-other")?).await?, 1);
        drop(provider);
        let reopened = LocalStream::open(directory.path(), limits).await?;
        assert!(reopened.inspect_idempotency(key).await?.is_none());
        assert_eq!(reopened.tail(StreamPath::new("guard-drift-other")?).await?, 1);
        drop(reopened);
        assert_eq!(meter.memory.load(Ordering::SeqCst), 0);
        meter.assert_refunded();
        Ok(())
    }

    #[test]
    fn native_guard_lease_survives_caller_abort_and_real_durability_terminal()
    -> Result<(), Box<dyn std::error::Error>> {
        let _serial = PERSIST_BLOCKER_TESTS.lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let runtime = tokio::runtime::Builder::new_multi_thread()
            .worker_threads(2).enable_all().build()?;
        runtime.block_on(async {
            let directory = tempfile::tempdir()?;
            let limits = LocalStreamLimits::default();
            let provider = LocalStream::open(directory.path(), limits).await?;
            let request = keyed_commit("guard-owned-drain", 0)?;
            let key = request.idempotency_key.clone();
            let meter = Arc::new(LocalNativeMeter::default());
            let released = Arc::new(AtomicU64::new(0));
            let lease = Arc::new(GuardLeaseWitness(Arc::clone(&released)));
            let weak = Arc::downgrade(&lease);
            let (started_tx, started_rx) = std::sync::mpsc::sync_channel(0);
            let (release_tx, release_rx) = std::sync::mpsc::sync_channel(0);
            *JOURNAL_PERSIST_BLOCKER.lock().map_err(|_| "journal-persist hook was poisoned")? =
                Some((Arc::as_ptr(&provider.inner.journal.journal) as usize, started_tx, release_rx));
            let caller = tokio::spawn({
                let provider = provider.clone();
                let meter = Arc::clone(&meter);
                async move {
                    provider.native_commit_guarded(request, None, meter.original(), move || {
                        let lease = Arc::clone(&lease);
                        async move {
                            let _original_lease = lease;
                            Ok::<(), NativeGuardFailure>(())
                        }
                    }).await
                }
            });
            tokio::task::spawn_blocking(move || {
                started_rx.recv_timeout(std::time::Duration::from_secs(5))
            }).await??;
            caller.abort();
            let cancelled = caller.await.is_err_and(|error| error.is_cancelled());
            let retained = weak.upgrade().is_some();
            let completed_bytes = meter.write_bytes.load(Ordering::SeqCst);
            let syncs_before_release = meter.durability_operations.load(Ordering::SeqCst);
            // Always release the accepted real cleanup before asserting.
            release_tx.send(())?;
            assert!(cancelled && retained);
            assert!(completed_bytes > 0);
            assert_eq!(syncs_before_release, 0);
            assert_eq!(provider.tail(StreamPath::new("guard-owned-drain")?).await?, 1);
            tokio::time::timeout(std::time::Duration::from_secs(5), async {
                while weak.upgrade().is_some() { tokio::task::yield_now().await; }
            }).await?;
            assert_eq!(released.load(Ordering::SeqCst), 1);
                let accepted = provider.inspect_idempotency(key.clone()).await?
                .ok_or("owned guarded commit lost its original identity")?;
            drop(provider);
            let reopened = LocalStream::open(directory.path(), limits).await?;
            assert_eq!(reopened.inspect_idempotency(key).await?, Some(accepted));
            drop(reopened);
            assert_eq!(meter.memory.load(Ordering::SeqCst), 0);
            meter.assert_refunded();
            Ok::<_, Box<dyn std::error::Error>>(())
        })
    }

}
