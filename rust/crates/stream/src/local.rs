//! Bounded crash-recoverable local Stream provider.

use std::fs::{File, OpenOptions};
use std::future::Future;
use std::io::{Read, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};

use acyclic_native_runtime::OwnershipAnchor;
use async_trait::async_trait;
use bytes::Bytes;
use fs2::FileExt as _;
use futures::{StreamExt as _, stream};
use prost::Message as _;
use sha2::{Digest as _, Sha256};
use thiserror::Error;
use tokio::sync::{RwLock, mpsc, watch};

use crate::wire_codec::{condition_from_wire, mutation_from_wire, optional_key, required_key};
use crate::{
    AppendOutcome, AppendRequest, ChildStream, ChildrenRequest, CommitOutcome, CommitRequest,
    CommittedEnvelope, DeleteReceipt, ForkReceipt, ForkRequest, IdempotencyKey,
    IdempotencyObservation, MAX_COMMAND_BYTES, MAX_ITEMS, MemoryLimits, MemoryStream, ReadRequest,
    RecordStream, StreamError, StreamPath, StreamProvider, SystemUnixMillisClock, TrimReceipt,
    UnixMillisClock,
};

const HEADER_MAGIC: &[u8; 24] = b"ACYCLIC-STREAM-LOCAL-V1\0";
const HEADER_BYTES: usize = HEADER_MAGIC.len() + 8 * 8;
const FRAME_CHECKSUM_BYTES: usize = 32;
const REPLAY_PIPELINE_COMMANDS: usize = 32;

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

async fn run_owned_initialization<T, F>(initialize: F) -> Result<T, LocalStreamError>
where
    T: Send + 'static,
    F: Future<Output = T> + Send + 'static,
{
    tokio::spawn(initialize)
        .await
        .map_err(|_| LocalStreamError::Executor)
}

/// How the journal is made durable before a mutation becomes observable.
///
/// A per-open policy, not part of the on-disk contract: a journal written under one policy
/// reopens under any other.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub enum LocalDurability {
    /// Every publication waits for the storage device to flush its cache (`F_FULLFSYNC` on
    /// Apple platforms, `fsync`/`fdatasync` elsewhere), so acknowledged frames survive power
    /// loss.
    #[default]
    FullFlush,
    /// Every publication is ordered behind earlier writes without waiting for the device
    /// cache. This requires Apple's `F_BARRIERFSYNC`; opening on a target without that exact
    /// primitive fails with an I/O error instead of substituting different semantics.
    Barrier,
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
}

/// Exclusive-process durable local provider backed by a checksummed command journal.
///
/// The journal is synchronized before a mutation becomes observable. Startup replays every
/// complete frame through the same bounded [`MemoryStream`] state machine used by conformance.
/// A torn final frame is removed; corruption in a complete frame fails closed.
#[derive(Clone)]
pub struct LocalStream {
    inner: Arc<LocalInner>,
}

struct LocalInner {
    provider: MemoryStream,
    journal: OwnedJournal,
    visibility: RwLock<()>,
    changed: watch::Sender<u64>,
    poisoned: AtomicBool,
}

#[derive(Clone)]
struct OwnedJournal {
    // Keep the anchor last: the final clone releases the journal lock before publishing release of
    // the shared local-root lifecycle. A blocking task therefore cannot outlive root ownership.
    journal: Arc<Mutex<Journal>>,
    _ownership_anchor: Option<OwnershipAnchor>,
}

impl OwnedJournal {
    fn append(&self, frame: &PreparedFrame) -> Result<(), LocalStreamError> {
        self.journal
            .lock()
            .map_err(|_| LocalStreamError::Corrupt)?
            .append(frame)
    }
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
        let root = root.as_ref().to_path_buf();
        // Dropping the caller must not release an external root gate while journal recovery is
        // still running on a blocking worker. The independently owned task retains every startup
        // input until the worker and bounded replay have both completed.
        run_owned_initialization(async move {
            Self::open_owned(root, limits, clock, ownership_anchor).await
        })
        .await?
    }

    async fn open_owned(
        root: PathBuf,
        limits: LocalStreamLimits,
        clock: Arc<dyn UnixMillisClock>,
        ownership_anchor: Option<OwnershipAnchor>,
    ) -> Result<Self, LocalStreamError> {
        validate_limits(limits)?;
        let (commands, mut receiver) = mpsc::channel(REPLAY_PIPELINE_COMMANDS);
        #[cfg(test)]
        let submitted_root = root.clone();
        let open = tokio::task::spawn_blocking(move || Journal::open(&root, limits, &commands));
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
        let provider = MemoryStream::new_with_clock(limits.memory, clock);
        let mut replay_error = None;
        while let Some(command) = receiver.recv().await {
            if replay_error.is_none()
                && let Err(error) = replay(&provider, command).await
            {
                replay_error = Some(error);
            }
        }
        let journal = open.await.map_err(|_| LocalStreamError::Executor)??;
        if let Some(error) = replay_error {
            return Err(LocalStreamError::Replay(error));
        }
        let (changed, _) = watch::channel(0_u64);
        Ok(Self {
            inner: Arc::new(LocalInner {
                provider,
                journal: OwnedJournal {
                    journal: Arc::new(Mutex::new(journal)),
                    _ownership_anchor: ownership_anchor,
                },
                visibility: RwLock::new(()),
                changed,
                poisoned: AtomicBool::new(false),
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

    fn prepare(&self, command: &Command) -> Result<PreparedFrame, StreamError> {
        let frame = PreparedFrame::encode(command).map_err(|error| match error {
            LocalStreamError::InvalidLimits => StreamError::Capacity,
            _ => StreamError::Unavailable,
        })?;
        self.inner
            .journal
            .journal
            .lock()
            .map_err(|_| StreamError::Unavailable)?
            .admit(&frame)
            .map_err(|error| match error {
                LocalStreamError::InvalidLimits => StreamError::Capacity,
                _ => StreamError::Unavailable,
            })?;
        Ok(frame)
    }

    async fn persist(&self, frame: PreparedFrame) -> Result<(), StreamError> {
        let journal = self.inner.journal.clone();
        #[cfg(test)]
        let journal_identity = Arc::as_ptr(&journal.journal) as usize;
        let persist = tokio::task::spawn_blocking(move || {
            #[cfg(test)]
            {
                if let Ok(mut hook) = JOURNAL_PERSIST_BLOCKER.lock()
                    && hook.as_ref().is_some_and(|(expected_journal, _, _)| {
                        *expected_journal == journal_identity
                    })
                    && let Some((_, started, release)) = hook.take()
                {
                    let _ = started.send(());
                    let _ = release.recv();
                }
            }
            journal.append(&frame)
        });
        let result = persist
            .await
            .map_err(|_| LocalStreamError::Executor)
            .and_then(|result| result);
        if result.is_err() {
            self.inner.poisoned.store(true, Ordering::Release);
            return Err(StreamError::Unavailable);
        }
        self.inner.changed.send_modify(|revision| {
            *revision = revision.saturating_add(1);
        });
        Ok(())
    }

    async fn read_visible(&self, request: ReadRequest) -> Result<RecordStream, StreamError> {
        self.check_available()?;
        let _visibility = self.inner.visibility.read().await;
        self.check_available()?;
        self.inner.provider.read(request).await
    }
}

#[async_trait]
impl StreamProvider for LocalStream {
    async fn inspect_idempotency(
        &self,
        idempotency_key: IdempotencyKey,
    ) -> Result<Option<IdempotencyObservation>, StreamError> {
        self.check_available()?;
        let _visibility = self.inner.visibility.read().await;
        self.check_available()?;
        self.inner
            .provider
            .inspect_idempotency(idempotency_key)
            .await
    }

    async fn tail(&self, path: StreamPath) -> Result<u64, StreamError> {
        self.check_available()?;
        let _visibility = self.inner.visibility.read().await;
        self.check_available()?;
        self.inner.provider.tail(path).await
    }

    async fn append(&self, request: AppendRequest) -> Result<AppendOutcome, StreamError> {
        self.check_available()?;
        let _visibility = self.inner.visibility.write().await;
        self.check_available()?;
        let command = Command::Append(request.clone());
        let frame = self.prepare(&command)?;
        let retain_conflict = request.idempotency_key.is_some();
        let outcome = self.inner.provider.append(request).await?;
        if matches!(outcome, AppendOutcome::Committed(_)) || retain_conflict {
            self.persist(frame).await?;
        }
        Ok(outcome)
    }

    async fn fork(&self, request: ForkRequest) -> Result<ForkReceipt, StreamError> {
        self.check_available()?;
        let _visibility = self.inner.visibility.write().await;
        self.check_available()?;
        let command = Command::Fork(request.clone());
        let frame = self.prepare(&command)?;
        let outcome = self.inner.provider.fork(request).await?;
        self.persist(frame).await?;
        Ok(outcome)
    }

    async fn trim(
        &self,
        path: StreamPath,
        before: u64,
        idempotency_key: IdempotencyKey,
    ) -> Result<TrimReceipt, StreamError> {
        self.check_available()?;
        let _visibility = self.inner.visibility.write().await;
        self.check_available()?;
        let command = Command::Trim {
            path: path.clone(),
            before,
            idempotency_key: idempotency_key.clone(),
        };
        let frame = self.prepare(&command)?;
        let outcome = self
            .inner
            .provider
            .trim(path, before, idempotency_key)
            .await?;
        self.persist(frame).await?;
        Ok(outcome)
    }

    async fn delete(
        &self,
        path: StreamPath,
        idempotency_key: IdempotencyKey,
    ) -> Result<DeleteReceipt, StreamError> {
        self.check_available()?;
        let _visibility = self.inner.visibility.write().await;
        self.check_available()?;
        let command = Command::Delete {
            path: path.clone(),
            idempotency_key: idempotency_key.clone(),
        };
        let frame = self.prepare(&command)?;
        let outcome = self.inner.provider.delete(path, idempotency_key).await?;
        self.persist(frame).await?;
        Ok(outcome)
    }

    async fn read(&self, request: ReadRequest) -> Result<RecordStream, StreamError> {
        self.read_visible(request).await
    }

    async fn follow(&self, path: StreamPath, from: u64) -> Result<RecordStream, StreamError> {
        self.tail(path.clone()).await?;
        let state = FollowState {
            provider: self.clone(),
            path,
            next: from,
            changed: self.inner.changed.subscribe(),
            done: false,
        };
        Ok(stream::unfold(state, |mut state| async move {
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
                            state.done = true;
                            return Some((Err(error), state));
                        }
                        None => {}
                    },
                    Err(error) => {
                        state.done = true;
                        return Some((Err(error), state));
                    }
                }
                if state.changed.changed().await.is_err() {
                    state.done = true;
                    return Some((Err(StreamError::Unavailable), state));
                }
            }
        })
        .boxed())
    }

    async fn children(&self, request: ChildrenRequest) -> Result<ChildStream, StreamError> {
        self.check_available()?;
        let _visibility = self.inner.visibility.read().await;
        self.check_available()?;
        self.inner.provider.children(request).await
    }

    async fn commit(&self, request: CommitRequest) -> Result<CommitOutcome, StreamError> {
        self.check_available()?;
        let _visibility = self.inner.visibility.write().await;
        self.check_available()?;
        let command = Command::Commit(request.clone());
        let frame = self.prepare(&command)?;
        let outcome = self.inner.provider.commit(request).await?;
        self.persist(frame).await?;
        Ok(outcome)
    }

    async fn commit_before(
        &self,
        request: CommitRequest,
        deadline_unix_millis: u64,
    ) -> Result<CommitOutcome, StreamError> {
        self.check_available()?;
        let _visibility = self.inner.visibility.write().await;
        self.check_available()?;
        let command = Command::Commit(request.clone());
        let frame = self.prepare(&command)?;
        let outcome = self
            .inner
            .provider
            .commit_before(request, deadline_unix_millis)
            .await?;
        self.persist(frame).await?;
        Ok(outcome)
    }

    async fn read_commit(
        &self,
        commit_id: crate::CommitId,
    ) -> Result<CommittedEnvelope, StreamError> {
        self.check_available()?;
        let _visibility = self.inner.visibility.read().await;
        self.check_available()?;
        self.inner.provider.read_commit(commit_id).await
    }
}

struct FollowState {
    provider: LocalStream,
    path: StreamPath,
    next: u64,
    changed: watch::Receiver<u64>,
    done: bool,
}

struct Journal {
    file: File,
    operations: u64,
    bytes: u64,
    limits: LocalStreamLimits,
    _root: PathBuf,
}

impl Drop for Journal {
    fn drop(&mut self) {
        let _ = self.file.unlock();
    }
}

struct PreparedFrame {
    encoded: Vec<u8>,
    bytes: u64,
}

impl PreparedFrame {
    fn encode(command: &Command) -> Result<Self, LocalStreamError> {
        let journal = journal_command(command);
        let command_length = journal.encoded_len();
        if command_length > MAX_COMMAND_BYTES {
            return Err(LocalStreamError::InvalidLimits);
        }
        let command_length =
            u32::try_from(command_length).map_err(|_| LocalStreamError::InvalidLimits)?;
        let length = command_length.to_le_bytes();
        let command_length_usize =
            usize::try_from(command_length).map_err(|_| LocalStreamError::InvalidLimits)?;
        let capacity = 4_usize
            .checked_add(command_length_usize)
            .and_then(|value| value.checked_add(FRAME_CHECKSUM_BYTES))
            .ok_or(LocalStreamError::InvalidLimits)?;
        let mut encoded = Vec::with_capacity(capacity);
        encoded.extend_from_slice(&length);
        journal
            .encode(&mut encoded)
            .map_err(|_| LocalStreamError::InvalidLimits)?;
        let command_bytes = encoded.get(4..).ok_or(LocalStreamError::InvalidLimits)?;
        let checksum = frame_checksum(&length, command_bytes);
        encoded.extend_from_slice(&checksum);
        Ok(Self {
            encoded,
            bytes: u64::try_from(capacity).map_err(|_| LocalStreamError::InvalidLimits)?,
        })
    }
}

impl Journal {
    fn open(
        root: &Path,
        limits: LocalStreamLimits,
        commands: &mpsc::Sender<Command>,
    ) -> Result<Self, LocalStreamError> {
        std::fs::create_dir_all(root)?;
        let path = root.join("stream.journal");
        let mut file = OpenOptions::new()
            .create(true)
            .truncate(false)
            .read(true)
            .write(true)
            .open(path)?;
        file.try_lock_exclusive().map_err(|error| {
            if acyclic_native_runtime::is_exclusive_lock_contention(&error) {
                LocalStreamError::AlreadyOpen
            } else {
                LocalStreamError::Io(error)
            }
        })?;
        let length = file.metadata()?.len();
        // Every command appends a frame after the header, so a journal shorter
        // than its header never acknowledged one: a crash tore its creation,
        // and it is created again.
        if length < u64::try_from(HEADER_BYTES).map_err(|_| LocalStreamError::InvalidLimits)? {
            let header = encode_header(limits)?;
            file.set_len(0)?;
            file.write_all(&header)?;
            sync_file(&file, limits.durability)?;
            sync_directory(root, limits.durability)?;
        } else {
            let mut header = vec![0_u8; HEADER_BYTES];
            file.read_exact(&mut header)
                .map_err(|_| LocalStreamError::Corrupt)?;
            if header != encode_header(limits)? {
                return Err(LocalStreamError::Corrupt);
            }
        }
        let mut operations = 0_u64;
        let mut valid_length =
            u64::try_from(HEADER_BYTES).map_err(|_| LocalStreamError::InvalidLimits)?;
        let total_length = file.metadata()?.len();
        file.seek(SeekFrom::Start(valid_length))?;
        while valid_length < total_length {
            let frame_start = valid_length;
            let Some(frame) = read_frame(&mut file, total_length - frame_start)? else {
                // An invalid frame is a torn append exactly when nothing valid
                // follows it; power loss can leave one zero-filled or garbage.
                match acyclic_native_runtime::recover_log_tail(
                    &mut file,
                    frame_start,
                    maximum_frame_bytes(),
                    native_durability(limits.durability),
                    frame_is_valid,
                )? {
                    acyclic_native_runtime::LogTail::Torn => break,
                    acyclic_native_runtime::LogTail::Corrupt => {
                        return Err(LocalStreamError::Corrupt);
                    }
                }
            };
            // An intact frame was written whole, so one that does not decode
            // may be a committed command: fail closed.
            let command = decode_command(&frame.command).map_err(|_| LocalStreamError::Corrupt)?;
            operations = operations.checked_add(1).ok_or(LocalStreamError::Corrupt)?;
            if operations > limits.journal_operations {
                return Err(LocalStreamError::Corrupt);
            }
            commands
                .blocking_send(command)
                .map_err(|_| LocalStreamError::Executor)?;
            valid_length = valid_length
                .checked_add(frame_bytes(&frame.length))
                .ok_or(LocalStreamError::Corrupt)?;
        }
        if valid_length > limits.journal_bytes {
            return Err(LocalStreamError::InvalidLimits);
        }
        file.seek(SeekFrom::End(0))?;
        Ok(Self {
            file,
            operations,
            bytes: valid_length,
            limits,
            _root: root.to_path_buf(),
        })
    }

    fn admit(&self, frame: &PreparedFrame) -> Result<(), LocalStreamError> {
        if self.operations >= self.limits.journal_operations
            || self
                .bytes
                .checked_add(frame.bytes)
                .is_none_or(|value| value > self.limits.journal_bytes)
        {
            return Err(LocalStreamError::InvalidLimits);
        }
        Ok(())
    }

    fn append(&mut self, frame: &PreparedFrame) -> Result<(), LocalStreamError> {
        self.file.write_all(&frame.encoded)?;
        sync_file_data(&self.file, self.limits.durability)?;
        self.operations += 1;
        self.bytes += frame.bytes;
        Ok(())
    }
}

/// The largest frame the journal holds: length prefix, command and checksum.
fn maximum_frame_bytes() -> usize {
    4 + MAX_COMMAND_BYTES + FRAME_CHECKSUM_BYTES
}

/// The bytes of the frame whose length prefix is `length_bytes`.
fn frame_bytes(length_bytes: &[u8; 4]) -> u64 {
    4 + u64::from(u32::from_le_bytes(*length_bytes)) + FRAME_CHECKSUM_BYTES as u64
}

/// The command length a frame's prefix declares, if the journal can hold it.
fn command_length(length_bytes: [u8; 4]) -> Option<usize> {
    usize::try_from(u32::from_le_bytes(length_bytes))
        .ok()
        .filter(|length| (1..=MAX_COMMAND_BYTES).contains(length))
}

/// One whole journal frame whose checksum matched.
struct JournalFrame {
    length: [u8; 4],
    command: Vec<u8>,
}

/// Reads the frame at the file's cursor, `remaining` bytes before its end,
/// or `None` when it is invalid.
fn read_frame(file: &mut File, remaining: u64) -> Result<Option<JournalFrame>, LocalStreamError> {
    let mut length_bytes = [0_u8; 4];
    if remaining < 4 {
        return Ok(None);
    }
    file.read_exact(&mut length_bytes)?;
    let Some(length) = command_length(length_bytes) else {
        return Ok(None);
    };
    if remaining < frame_bytes(&length_bytes) {
        return Ok(None);
    }
    let mut encoded = vec![0_u8; length];
    file.read_exact(&mut encoded)?;
    let mut checksum = [0_u8; FRAME_CHECKSUM_BYTES];
    file.read_exact(&mut checksum)?;
    Ok(
        (frame_checksum(&length_bytes, &encoded) == checksum).then_some(JournalFrame {
            length: length_bytes,
            command: encoded,
        }),
    )
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
        .is_some_and(|checksum| checksum == frame_checksum(length_bytes, encoded))
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

fn encode_header(limits: LocalStreamLimits) -> Result<Vec<u8>, LocalStreamError> {
    let mut encoded = Vec::with_capacity(HEADER_BYTES);
    encoded.extend_from_slice(HEADER_MAGIC);
    for value in [
        limits.memory.paths,
        limits.memory.path_bytes,
        limits.memory.records,
        limits.memory.payload_bytes,
        limits.memory.commits,
        limits.memory.idempotency_results,
    ] {
        encoded.extend_from_slice(
            &u64::try_from(value)
                .map_err(|_| LocalStreamError::InvalidLimits)?
                .to_le_bytes(),
        );
    }
    encoded.extend_from_slice(&limits.journal_operations.to_le_bytes());
    encoded.extend_from_slice(&limits.journal_bytes.to_le_bytes());
    Ok(encoded)
}

fn frame_checksum(length: &[u8; 4], command: &[u8]) -> [u8; 32] {
    let mut hasher = Sha256::new();
    hasher.update(b"acyclic-stream-local-frame-v1\0");
    hasher.update(length);
    hasher.update(command);
    hasher.finalize().into()
}

#[derive(Clone)]
enum Command {
    Append(AppendRequest),
    Fork(ForkRequest),
    Trim {
        path: StreamPath,
        before: u64,
        idempotency_key: IdempotencyKey,
    },
    Delete {
        path: StreamPath,
        idempotency_key: IdempotencyKey,
    },
    Commit(CommitRequest),
}

async fn replay(provider: &MemoryStream, command: Command) -> Result<(), StreamError> {
    match command {
        Command::Append(request) => provider.append(request).await.map(|_| ()),
        Command::Fork(request) => provider.fork(request).await.map(|_| ()),
        Command::Trim {
            path,
            before,
            idempotency_key,
        } => provider
            .trim(path, before, idempotency_key)
            .await
            .map(|_| ()),
        Command::Delete {
            path,
            idempotency_key,
        } => provider.delete(path, idempotency_key).await.map(|_| ()),
        Command::Commit(request) => provider.commit(request).await.map(|_| ()),
    }
}

fn journal_command(command: &Command) -> JournalCommand {
    let operation = match command {
        Command::Append(request) => journal_command::Operation::Append(wire_append(request)),
        Command::Fork(request) => journal_command::Operation::Fork(wire_fork(request)),
        Command::Trim {
            path,
            before,
            idempotency_key,
        } => journal_command::Operation::Trim(crate::wire::TrimRequest {
            path: path.to_string(),
            before: *before,
            idempotency_key: Some(Bytes::copy_from_slice(idempotency_key.as_bytes())),
        }),
        Command::Delete {
            path,
            idempotency_key,
        } => journal_command::Operation::Delete(crate::wire::DeleteRequest {
            path: path.to_string(),
            idempotency_key: Some(Bytes::copy_from_slice(idempotency_key.as_bytes())),
        }),
        Command::Commit(request) => journal_command::Operation::Commit(wire_commit(request)),
    };
    JournalCommand {
        operation: Some(operation),
    }
}

fn decode_command(encoded: &[u8]) -> Result<Command, StreamError> {
    let journal = JournalCommand::decode(encoded).map_err(|_| StreamError::InvalidArgument)?;
    match journal.operation.ok_or(StreamError::InvalidArgument)? {
        journal_command::Operation::Append(request) => domain_append(request).map(Command::Append),
        journal_command::Operation::Fork(request) => domain_fork(request).map(Command::Fork),
        journal_command::Operation::Trim(request) => Ok(Command::Trim {
            path: StreamPath::new(request.path)?,
            before: request.before,
            idempotency_key: required_key(request.idempotency_key)?,
        }),
        journal_command::Operation::Delete(request) => Ok(Command::Delete {
            path: StreamPath::new(request.path)?,
            idempotency_key: required_key(request.idempotency_key)?,
        }),
        journal_command::Operation::Commit(request) => domain_commit(request).map(Command::Commit),
    }
}

#[derive(Clone, PartialEq, prost::Message)]
struct JournalCommand {
    #[prost(oneof = "journal_command::Operation", tags = "1, 2, 3, 4, 5")]
    operation: Option<journal_command::Operation>,
}

mod journal_command {
    #[derive(Clone, PartialEq, prost::Oneof)]
    pub(super) enum Operation {
        #[prost(message, tag = "1")]
        Append(crate::wire::AppendRequest),
        #[prost(message, tag = "2")]
        Fork(crate::wire::ForkRequest),
        #[prost(message, tag = "3")]
        Trim(crate::wire::TrimRequest),
        #[prost(message, tag = "4")]
        Delete(crate::wire::DeleteRequest),
        #[prost(message, tag = "5")]
        Commit(crate::wire::CommitRequest),
    }
}

fn wire_append(request: &AppendRequest) -> crate::wire::AppendRequest {
    crate::wire::AppendRequest {
        path: request.path.to_string(),
        records: request.records.clone(),
        if_tail: request.if_tail,
        idempotency_key: request
            .idempotency_key
            .as_ref()
            .map(|key| Bytes::copy_from_slice(key.as_bytes())),
    }
}

fn domain_append(request: crate::wire::AppendRequest) -> Result<AppendRequest, StreamError> {
    Ok(AppendRequest {
        path: StreamPath::new(request.path)?,
        records: request.records,
        if_tail: request.if_tail,
        idempotency_key: optional_key(request.idempotency_key)?,
    })
}

fn wire_fork(request: &ForkRequest) -> crate::wire::ForkRequest {
    crate::wire::ForkRequest {
        source: request.source.to_string(),
        destination: request.destination.to_string(),
        at_tail: request.at_tail,
        idempotency_key: request
            .idempotency_key
            .as_ref()
            .map(|key| Bytes::copy_from_slice(key.as_bytes())),
    }
}

fn domain_fork(request: crate::wire::ForkRequest) -> Result<ForkRequest, StreamError> {
    Ok(ForkRequest {
        source: StreamPath::new(request.source)?,
        destination: StreamPath::new(request.destination)?,
        at_tail: request.at_tail,
        idempotency_key: optional_key(request.idempotency_key)?,
    })
}

fn wire_commit(request: &CommitRequest) -> crate::wire::CommitRequest {
    crate::wire::CommitRequest {
        conditions: request
            .conditions
            .iter()
            .cloned()
            .map(crate::wire_codec::condition_wire)
            .collect(),
        mutations: request
            .mutations
            .iter()
            .cloned()
            .map(crate::wire_codec::mutation_wire)
            .collect(),
        idempotency_key: Bytes::copy_from_slice(request.idempotency_key.as_bytes()),
        deadline_unix_millis: None,
    }
}

fn domain_commit(request: crate::wire::CommitRequest) -> Result<CommitRequest, StreamError> {
    if request.conditions.len() > MAX_ITEMS || request.mutations.len() > MAX_ITEMS {
        return Err(StreamError::LimitExceeded);
    }
    Ok(CommitRequest {
        conditions: request
            .conditions
            .into_iter()
            .map(condition_from_wire)
            .collect::<Result<_, _>>()?,
        mutations: request
            .mutations
            .into_iter()
            .map(mutation_from_wire)
            .collect::<Result<_, _>>()?,
        idempotency_key: IdempotencyKey::new(request.idempotency_key)?,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn cancelled_real_open_retains_ownership_until_journal_open_stops()
    -> Result<(), Box<dyn std::error::Error>> {
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
            let opening = tokio::spawn(async move {
                LocalStream::open_with_ownership_anchor(root, LocalStreamLimits::default(), anchor)
                    .await
            });

            // The only blocking worker is occupied, so the real Journal::open submitted by the
            // independently owned initialization cannot have completed when its caller is
            // cancelled.
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
            tokio::time::timeout(
                std::time::Duration::from_secs(1),
                Arc::clone(&lifecycle).lock_owned(),
            )
            .await?;
            assert!(
                ownership_retained,
                "caller cancellation must not release ownership from active Journal::open"
            );
            Ok::<_, Box<dyn std::error::Error>>(())
        })
    }

    #[test]
    fn cancelled_persist_retains_ownership_until_the_journal_task_stops()
    -> Result<(), Box<dyn std::error::Error>> {
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
            let appending = tokio::spawn({
                let provider = provider.clone();
                async move {
                    provider
                        .append(AppendRequest {
                            path: StreamPath::new("cancelled-persist")?,
                            records: vec![Bytes::from_static(b"record")],
                            if_tail: Some(0),
                            idempotency_key: None,
                        })
                        .await
                }
            });

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
            Ok::<_, Box<dyn std::error::Error>>(())
        })
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
        std::fs::write(
            directory.path().join("stream.journal"),
            HEADER_MAGIC.get(..3).ok_or("header magic")?,
        )?;
        let provider = LocalStream::open(directory.path(), LocalStreamLimits::default()).await?;
        conformance::verify(&provider)
            .await
            .map_err(std::io::Error::other)?;
        drop(provider);
        LocalStream::open(directory.path(), LocalStreamLimits::default()).await?;
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
                records: vec![Bytes::from_static(b"accepted")],
            }],
            idempotency_key: IdempotencyKey::new(Bytes::from_static(b"accepted"))?,
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
                records: vec![Bytes::from_static(b"expired")],
            }],
            idempotency_key: IdempotencyKey::new(Bytes::from_static(b"expired"))?,
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
                records: vec![Bytes::from_static(b"one")],
                if_tail: Some(0),
                idempotency_key: Some(IdempotencyKey::new(Bytes::from_static(b"append"))?),
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
                    records: vec![Bytes::copy_from_slice(&index.to_le_bytes())],
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
        let invalid = PreparedFrame::encode(&Command::Trim {
            path: path.clone(),
            before: 1,
            idempotency_key: IdempotencyKey::new(Bytes::from_static(b"invalid-trim"))?,
        })?;
        let valid = PreparedFrame::encode(&Command::Append(AppendRequest {
            path,
            records: vec![Bytes::from_static(b"later")],
            if_tail: Some(0),
            idempotency_key: None,
        }))?;
        let journal_path = directory.path().join("stream.journal");
        let mut journal = OpenOptions::new().append(true).open(journal_path)?;
        journal.write_all(&invalid.encoded)?;
        for _ in 0..REPLAY_PIPELINE_COMMANDS * 4 {
            journal.write_all(&valid.encoded)?;
        }
        journal.sync_all()?;
        drop(journal);

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
                    records: vec![Bytes::from_static(b"must-not-appear")],
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

    /// Appends one record to `path` and returns the journal's length after.
    async fn append_one(
        provider: &LocalStream,
        path: &str,
        journal: &Path,
    ) -> Result<u64, Box<dyn std::error::Error>> {
        provider
            .append(AppendRequest {
                path: StreamPath::new(path)?,
                records: vec![Bytes::from_static(b"body")],
                if_tail: Some(0),
                idempotency_key: None,
            })
            .await?;
        Ok(std::fs::metadata(journal)?.len())
    }

    /// The tail of `path` in the journal reopened, if the path exists.
    async fn tail_of(
        directory: &Path,
        path: &str,
    ) -> Result<Option<u64>, Box<dyn std::error::Error>> {
        let provider = LocalStream::open(directory, LocalStreamLimits::default()).await?;
        Ok(provider.tail(StreamPath::new(path)?).await.ok())
    }

    /// A damaged frame with an intact one after it was committed, so the
    /// journal fails closed and is left as it was.
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

    /// Power loss can leave the last append whole in length but zero-filled
    /// or garbage; nothing valid follows it, so it is a torn tail and the
    /// journal reopens with every frame before it.
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
}
